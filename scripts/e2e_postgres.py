#!/usr/bin/env python3
"""Disposable PostgreSQL/S3/HAProxy HA-runtime qualification with stock VM.

Requires VMTIER_TEST_POSTGRES_DSN (a disposable loopback PostgreSQL database),
psql, HAProxy, Moto and the four prebuilt VM/vmtier binaries. Only an isolated,
random schema is created/dropped. The outer HAProxy models an external stable
L4 load balancer; this test does not qualify that load balancer's own redundancy.
"""
import argparse
import concurrent.futures
import csv
import io
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from e2e import (ROOT, HTTP, Cluster, Processes, api, compare, fixture,
                 fixture_horizon, free_port, metrics, positive_int, remote_write, request)
from e2e_serve import ControlledSource, expressions, provenance, snapshot, start_tiered_select, wait_ready
from s3_compat_proxy import S3CompatibilityProxy
from blob_outage import assert_running_blob_failure


class ObservedS3Proxy(S3CompatibilityProxy):
    """Observe actual HTTP attempts, including SDK retries, at the bucket edge."""
    def __init__(self, endpoint):
        super().__init__(endpoint)
        self.attempt_times = []
        self.inflight = self.peak_inflight = 0
        original = self.server.RequestHandlerClass
        owner = self

        class Handler(original):
            def _observed(self):
                with owner.lock:
                    owner.attempt_times.append(time.monotonic())
                    owner.inflight += 1
                    owner.peak_inflight = max(owner.peak_inflight, owner.inflight)
                try:
                    self._forward()
                finally:
                    with owner.lock:
                        owner.inflight -= 1
            do_GET = do_PUT = do_HEAD = do_DELETE = do_POST = _observed

        self.server.RequestHandlerClass = Handler

    def admission_measurement(self):
        with self.lock:
            times, peak = list(self.attempt_times), self.peak_inflight
        left = maximum = 0
        for right, stamp in enumerate(times):
            while stamp - times[left] >= 1:
                left += 1
            maximum = max(maximum, right - left + 1)
        return {'observed_http_attempts': len(times), 'peak_proxy_inflight': peak,
                'max_attempts_in_sliding_second': maximum}


def executable(name, env_name, fallback):
    result = os.environ.get(env_name) or shutil.which(name)
    if not result and Path(fallback).is_file():
        result = fallback
    if not result:
        raise RuntimeError(f'Install {name} or set {env_name}')
    return result


def wait_until(predicate, label, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
        except ConnectionRefusedError:
            value = False
        if value:
            return value
        time.sleep(.1)
    raise AssertionError(f'Timed out waiting for {label}')


def recover_query(expected, fn, label, timeout=20):
    began, failures = time.monotonic(), 0
    while True:
        try:
            actual = fn()
        except urllib.error.HTTPError as error:
            # Only transport/query errors may be retried. A successful partial
            # or numerically incorrect result is always an immediate failure.
            if error.code < 500 or time.monotonic() - began >= timeout:
                raise
            failures += 1
            time.sleep(.1)
            continue
        compare(expected, actual, label)
        return {'seconds': time.monotonic() - began, 'http_failures_before_recovery': failures}


def write_proxy(path, listen, backends, health_ports=None):
    stats_port = free_port()
    lines = ['global', '  maxconn 256', '  nbthread 1', f'  stats socket ipv4@127.0.0.1:{stats_port} level user', 'defaults', '  mode tcp',
             '  timeout connect 1s', '  timeout client 60s', '  timeout server 60s',
             'frontend cold', f'  bind 127.0.0.1:{listen}', '  default_backend gateways',
             'backend gateways', '  balance first']
    if health_ports:
        lines += ['  option httpchk GET /ready', '  http-check expect status 200']
    for i, port in enumerate(backends):
        health = f' port {health_ports[i]}' if health_ports else ''
        lines.append(f'  server node{i} 127.0.0.1:{port} check{health} inter 100ms fall 2 rise 1 on-marked-down shutdown-sessions')
    path.write_text('\n'.join(lines) + '\n')
    return stats_port


def proxy_stats(port):
    with socket.create_connection(('127.0.0.1', port), timeout=3) as connection:
        connection.sendall(b'show stat\n')
        chunks = []
        while True:
            chunk = connection.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    return {row['svname']: row for row in csv.DictReader(io.StringIO(b''.join(chunks).decode().lstrip('# ')))
            if row['# pxname' if '# pxname' in row else 'pxname'] == 'gateways'}


def normalized_catalog(value):
    """Compare portable metadata; backend row IDs/schema/authority ID differ."""
    state = value['state']
    windows = []
    for window in sorted(value['windows'] or [], key=lambda row: row['generation']):
        refs = [{key: val for key, val in ref.items() if key != 'window_id'}
                for ref in sorted(value['packs'] or [], key=lambda row: row['ordinal'])
                if ref['window_id'] == window['id']]
        windows.append({**{key: val for key, val in window.items() if key != 'id'}, 'packs': refs})
    return {'vm_version': state['vm_version'], 'generation': state['generation'],
            'contract': json.loads(state['contract_json']), 'blob': json.loads(state['blob_json']),
            'coverage': sorted(value['coverage'], key=lambda row: (row['tenant'], row['source_id'])),
            'windows': windows}


def run(work, args):
    procs = Processes(work, args.gomaxprocs)
    source = proxy = None
    schema = 'vmtier_ha_' + uuid.uuid4().hex
    created = False
    report = {'architecture': 'PostgreSQL catalog; two fenced serve candidates; two HAProxy gateways behind a stable L4 fixture',
              'started_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'comparison': 'exact timestamps, labels and numeric strings; no tolerance or partial results',
              'limitations': ['single PostgreSQL server, no DB promotion in this harness',
                              'outer L4 fixture is single; production needs a redundant managed LB or fenced VIP',
                              'loopback Moto; no real Sakura/provider qualification'],
              'migrated_sqlite': args.migrate_sqlite, 'checks': [], 'runtime': procs.runtime}
    env = {'AWS_ACCESS_KEY_ID': 'vmtier-test', 'AWS_SECRET_ACCESS_KEY': 'vmtier-test-secret'}
    try:
        dsn = os.environ.get('VMTIER_TEST_POSTGRES_DSN', '')
        parsed = urllib.parse.urlsplit(dsn)
        if parsed.scheme not in ('postgres', 'postgresql') or parsed.hostname not in ('127.0.0.1', 'localhost', '::1'):
            raise RuntimeError('VMTIER_TEST_POSTGRES_DSN must be an explicit disposable loopback PostgreSQL URI')
        psql = executable('psql', 'PSQL', '/workspace/tools/postgres17/root/usr/lib/postgresql/17/bin/psql')
        haproxy = executable('haproxy', 'HAPROXY', '/workspace/tools/haproxy/root/usr/sbin/haproxy')
        moto = executable('moto_server', 'MOTO_SERVER', '/workspace/tools/moto-venv/bin/moto_server')
        query = urllib.parse.parse_qs(parsed.query)
        query['options'] = [('-csearch_path=' + schema)]
        # search_path is accepted by lib/pq but not libpq/psql as a URI keyword.
        query.pop('search_path', None)
        isolated = urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(query, doseq=True)))
        env['VMTIER_POSTGRES_DSN'] = isolated

        def sql(statement, isolated_schema=True):
            result = subprocess.run([psql, '-XAt', '-v', 'ON_ERROR_STOP=1', '-d', isolated if isolated_schema else dsn,
                                     '-c', statement], capture_output=True, text=True, timeout=15)
            if result.returncode:
                raise AssertionError('PostgreSQL fixture operation failed: ' + result.stderr[-3000:])
            return result.stdout.strip()

        sql('CREATE SCHEMA ' + schema, False)
        created = True
        binary = ROOT / '.bin/vmtier'
        report['provenance'] = provenance()
        report['provenance']['harness_sha256']['e2e_postgres.py'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        report['haproxy_version'] = subprocess.check_output([haproxy, '-v'], text=True).splitlines()[0]
        endpoint = f'http://127.0.0.1:{free_port()}'
        procs.start('moto-s3', [moto, '-H', '127.0.0.1', '-p', endpoint.rsplit(':', 1)[1]], endpoint, env)
        bucket = 'vmtier-ha-test'
        with HTTP.open(urllib.request.Request(endpoint + '/' + bucket, method='PUT', data=b'')) as response:
            response.read()
        proxy = ObservedS3Proxy(endpoint)
        store = {'type': 's3', 'endpoint': proxy.url, 'region': 'us-east-1', 'bucket': bucket,
                 'prefix': 'ha', 'requests_per_second': 80, 'max_concurrent_requests': 8, 'max_queued_requests': 64}
        ref, hot = Cluster(procs, 'reference', args.replicas), Cluster(procs, 'hot', args.replicas)
        ref.start('full')
        hot.start('full')
        horizon = fixture_horizon(int(time.time() * 1000))
        start, boundary, end = (horizon[key] for key in ('start_ms', 'boundary_ms', 'end_ms'))
        tenants = ['1:2', '7:9']
        for i, tenant in enumerate(tenants):
            data = fixture(start, boundary, end, i)
            if args.migrate_sqlite and i == 0:
                # A real source-verified empty window must survive migration.
                data = [(labels, [(stamp, value) for stamp, value in samples
                                  if stamp >= start + horizon['window_ms']]) for labels, samples in data]
            remote_write(ref.insert, tenant, data)
            remote_write(hot.insert, tenant, data)
        ref.flush()
        hot.flush()
        report['fixture'] = dict(horizon, tenants=tenants, vm_storage_replicas=args.replicas)
        source = ControlledSource(hot.select)
        cfg = {'store': store, 'catalog': {'type': 'postgres', 'dsn_env': 'VMTIER_POSTGRES_DSN', 'id': 'ha-e2e'},
               'tenants': tenants, 'sources': [{'id': 'hot', 'url': source.url}],
               'archive_age': f'{horizon["archive_age_ms"]}ms', 'hot_retention': '240h', 'lateness': '30m',
               'safety_margin': '30m', 'window': '1h', 'lateness_enforced': True, 'coverage_start_ms': start,
               'service': {'archive_interval': '100ms', 'archive_lease_ttl': '3s', 'max_replicas': 2}}
        config = work / 'config.json'
        config.write_text(json.dumps(cfg, indent=2) + '\n')
        imported = source_snapshot = None
        if args.migrate_sqlite:
            sqlite_dir = work / 'sqlite-source'
            sqlite_config = work / 'sqlite-source.json'
            sqlite_cfg = json.loads(json.dumps(cfg))
            sqlite_cfg['catalog'] = {'type': 'sqlite', 'path': str(sqlite_dir), 'id': 'ha-sqlite-source'}
            sqlite_cfg['service'] = {'archive_interval': '100ms'}
            sqlite_config.write_text(json.dumps(sqlite_cfg, indent=2) + '\n')
            procs.command_stats([binary, 'catalog-init', '-backend', 'sqlite', '-config', sqlite_config,
                                 '-path', sqlite_dir, '-id', 'ha-sqlite-source'], work / 'sqlite-init.log', env)
            sqlite_rpc, sqlite_http = free_port(), free_port()
            source.unavailable_tenants = {tenants[1]}
            source.unavailable = False
            sqlite_owner = procs.start('sqlite-source', [binary, 'serve', '-config', sqlite_config,
                                        '-listen', f'127.0.0.1:{sqlite_rpc}', '-http', f'127.0.0.1:{sqlite_http}'],
                                        f'http://127.0.0.1:{sqlite_http}/health', env)
            wait_until(lambda: snapshot(sqlite_dir / 'catalog.db')['state']['generation'] == 3,
                       'SQLite partial-prefix publication')
            procs.stop(sqlite_owner)
            assert sqlite_owner.returncode == 0
            source_snapshot = snapshot(sqlite_dir / 'catalog.db')
            source.unavailable = True
            puts_before = proxy.snapshot()['put_requests']
            procs.command_stats([binary, 'catalog-import-postgres', '-config', config, '-from', sqlite_dir,
                                 '-source-id', 'ha-sqlite-source'], work / 'sqlite-to-postgres-import.log', env)
            imported = json.loads(sql("SELECT json_build_object('state',(SELECT row_to_json(s) FROM catalog_state s),"
                                      "'coverage',(SELECT json_agg(c) FROM coverage c),"
                                      "'windows',(SELECT json_agg(w) FROM windows w),"
                                      "'packs',(SELECT json_agg(p) FROM packs p))"))
            compare(normalized_catalog(source_snapshot), normalized_catalog(imported), 'SQLite import portable metadata')
            assert imported['state']['catalog_id'] == 'ha-e2e' != source_snapshot['state']['catalog_id']
            empty = sum(window['pack_count'] == 0 for window in imported['windows'])
            assert empty == 1 and len(imported['packs']) == 2
            assert int(sql('SELECT count(*) FROM object_keys')) == 2 * len(imported['packs'])
            procs.command_stats([binary, 'verify', '-config', config], work / 'verify-imported-payloads.log', env)
            assert proxy.snapshot()['put_requests'] == puts_before
            assert snapshot(sqlite_dir / 'catalog.db') == source_snapshot
            report['migration'] = {'source_id': 'ha-sqlite-source', 'destination_id': 'ha-e2e',
                                   'preserved_generation': 3, 'preserved_windows': 3, 'preserved_packs': 2,
                                   'preserved_empty_windows': empty, 'additional_blob_puts': 0,
                                   'portable_metadata_equal': True, 'payload_verification': 'passed'}
            report['checks'].append('stopped SQLite owner imports into a new PostgreSQL identity with identical contract, locator, coverage, digests and refs; verified empty window and payloads preserved without blob PUTs')
        else:
            procs.command_stats([binary, 'catalog-init', '-backend', 'postgres', '-config', config, '-id', 'ha-e2e'],
                                work / 'catalog-init.log', env)
        rpc_ports, http_ports = [free_port(), free_port()], [free_port(), free_port()]
        services = []

        def start_service(i, suffix=''):
            return procs.start(f'serve-{i}{suffix}', [binary, 'serve', '-config', config, '-listen',
                               f'127.0.0.1:{rpc_ports[i]}', '-http', f'127.0.0.1:{http_ports[i]}'],
                               f'http://127.0.0.1:{http_ports[i]}/health', env)

        def leader():
            active = [i for i, process in enumerate(services) if process.poll() is None and
                      metrics(http_ports[i]).get('vmtier_archive_leader') == 1]
            return active[0] + 1 if len(active) == 1 else False

        def state():
            return json.loads(sql("SELECT json_build_object('generation',(SELECT generation FROM catalog_state),"
                                  "'coverage',(SELECT json_agg(c ORDER BY tenant,source_id) FROM coverage c),"
                                  "'windows',(SELECT count(*) FROM windows),'packs',(SELECT count(*) FROM packs))"))

        services.extend(start_service(i) for i in range(2))
        active = wait_until(leader, 'one archiver lease holder') - 1
        source.unavailable_tenants = {tenants[1]}
        source.unavailable = False
        wait_until(lambda: state()['generation'] == 3, 'healthy tenant coverage while other source is unavailable')
        before = state()
        initial_token = int(sql('SELECT token FROM archive_lease'))
        assert before['windows'] == 3 and before['packs'] == (2 if args.migrate_sqlite else 3)
        began = time.monotonic()
        services[active].kill()
        services[active].wait(timeout=5)
        source.unavailable_tenants.clear()
        successor = wait_until(leader, 'lease expiry and successor acquisition', 15) - 1
        assert successor != active
        successor_token = int(sql('SELECT token FROM archive_lease'))
        assert successor_token > initial_token
        wait_ready(services[successor], http_ports[successor], work)
        assert state()['generation'] == 6
        report['archiver_takeover'] = {'seconds_to_complete_remaining_windows': time.monotonic() - began,
                                       'generation_before': 3, 'generation_after': 6, 'lease_ttl_seconds': 3,
                                       'fencing_token_before': initial_token, 'fencing_token_after': successor_token}
        report['checks'].append('SIGKILL lease holder during partial coverage; standby acquires expired lease and commits remaining windows exactly once')
        services[active] = start_service(active, '-restarted')
        for i in range(2):
            wait_ready(services[i], http_ports[i], work)
        stable = state()
        assert stable['windows'] == 6 and stable['packs'] == (5 if args.migrate_sqlite else 6)
        assert all(row['end_ms'] == boundary for row in stable['coverage'])

        # Separate HAProxy processes give one logical Cold node to vmselect.
        edge_ports, edge_processes, edge_stats = [free_port(), free_port()], [], []
        for i, port in enumerate(edge_ports):
            path = work / f'haproxy-edge-{i}.cfg'
            edge_stats.append(write_proxy(path, port, rpc_ports, http_ports))
            edge_processes.append(procs.start(f'haproxy-edge-{i}', [haproxy, '-db', '-f', path]))
        endpoint_port = free_port()
        front = work / 'haproxy-l4-fixture.cfg'
        front_stats = write_proxy(front, endpoint_port, edge_ports)
        procs.start('haproxy-l4-fixture', [haproxy, '-db', '-f', front])
        wait_until(lambda: proxy_stats(front_stats)['node0']['status'] == 'UP' and
                   proxy_stats(edge_stats[0])['node0']['status'] == 'UP', 'preferred proxy and gateway health')
        cold_select = free_port()
        start_tiered_select(procs, 'cold-only-select', cold_select, [endpoint_port])
        baseline_query = {'query': 'cold_only', 'time': (boundary - 600000) / 1000, 'timeout': '3s'}
        expected = api(ref.select, tenants[0], 'query', baseline_query)
        recover_query(expected, lambda: api(cold_select, tenants[0], 'query', baseline_query), 'initial stable endpoint')
        # Prove both gateway processes can serve the same committed catalog.
        direct_selects = []
        for i, rpc in enumerate(rpc_ports):
            port = free_port()
            start_tiered_select(procs, f'direct-select-{i}', port, [rpc])
            direct_selects.append(port)
            compare(expected, api(port, tenants[0], 'query', baseline_query), f'direct gateway {i}')
        report['checks'].append('both read gateways return exact Cold results from the same catalog through independent RPC connections')

        # Read failover is independent of leader election: HAProxy initially
        # prefers node0. Kill that gateway while stock-vmselect has live pools.
        before_gateway_requests = metrics(http_ports[0]).get('vmtier_query_requests_total', 0)
        compare(expected, api(cold_select, tenants[0], 'query', baseline_query), 'preferred gateway before failure')
        assert metrics(http_ports[0]).get('vmtier_query_requests_total', 0) > before_gateway_requests
        services[0].kill()
        services[0].wait(timeout=5)
        report['gateway_failover'] = recover_query(expected, lambda: api(cold_select, tenants[0], 'query', baseline_query), 'gateway loss')
        wait_until(lambda: leader() == 2, 'surviving gateway holds archive lease', 15)
        assert state() == stable
        report['checks'].append('stock-vmselect reconnects to the surviving gateway at the unchanged Cold endpoint; coverage is unchanged')
        assert int(proxy_stats(front_stats)['node0']['scur']) > 0, 'proxy failure must interrupt a live client path'
        edge_processes[0].kill()
        edge_processes[0].wait(timeout=5)
        report['proxy_failover'] = recover_query(expected, lambda: api(cold_select, tenants[0], 'query', baseline_query), 'HAProxy instance loss')
        report['checks'].append('loss of one HAProxy instance recovers through the second proxy behind the stable L4 fixture')
        services[0] = start_service(0, '-reader-returned')
        wait_ready(services[0], http_ports[0], work)

        hot.stop()
        hot.start('recent-only')
        for i, tenant in enumerate(tenants):
            remote_write(hot.insert, tenant, fixture(start, boundary, end, i), start=boundary)
        hot.flush()
        top = free_port()
        start_tiered_select(procs, 'tiered-select', top, hot.storage_read + [endpoint_port])
        count = 0
        for tenant in tenants:
            for label, (lo, hi) in {'cold': (start + 600000, boundary - 600000), 'hot': (boundary + 600000, end),
                                    'boundary': (boundary - 900000, boundary + 900000)}.items():
                for expression in expressions(boundary):
                    for method, params in [('query_range', {'query': expression, 'start': lo / 1000, 'end': hi / 1000, 'step': '60s'}),
                                           ('query', {'query': expression, 'time': hi / 1000})]:
                        compare(api(ref.select, tenant, method, params), api(top, tenant, method, params),
                                f'{tenant} {label} {method} {expression}')
                        count += 1
            for path in ('labels', 'label/cold_label/values', 'label/__name__/values', 'series'):
                params = {'start': start / 1000, 'end': end / 1000, 'match[]': '{__name__!=""}'}
                expected_meta, actual = api(ref.select, tenant, path, params), api(top, tenant, path, params)
                if expected_meta and isinstance(expected_meta[0], str):
                    expected_meta, actual = sorted(expected_meta), sorted(actual)
                compare(expected_meta, actual, f'{tenant} metadata {path}')
        assert count == 360
        report['differential_checks'] = count
        report['checks'].append('360 exact Cold/Hot/boundary queries and both tenants metadata match full-data reference after gateway/proxy failover')

        for port in direct_selects:
            recover_query(expected, lambda port=port: api(port, tenants[0], 'query', baseline_query), 'direct RPC pool after restart')
        before_attempts = [metrics(port).get('vmtier_s3_attempts_total', 0) for port in http_ports]
        before_proxy = proxy.snapshot()
        proxy.unavailable.set()
        try:
            def failing_query(i):
                return assert_running_blob_failure(
                    lambda: api(direct_selects[i], tenants[0], 'query', dict(baseline_query, timeout='8s')),
                    services[i], lambda: metrics(http_ports[i]), 'VMTierInjectedBlobOutage')
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                outage_evidence = list(pool.map(failing_query, range(2)))
        finally:
            proxy.unavailable.clear()
        attempted = [metrics(port).get('vmtier_s3_attempts_total', 0) - before_attempts[i] for i, port in enumerate(http_ports)]
        failed_gets = proxy.snapshot()['unavailable_get_requests'] - before_proxy['unavailable_get_requests']
        assert all(value >= 2 for value in attempted), f'SDK retries not observed for both clients: {attempted}'
        assert sum(attempted) == failed_gets, (attempted, failed_gets)
        for port in direct_selects:
            compare(expected, api(port, tenants[0], 'query', baseline_query), 'S3 recovery')
        observation = proxy.admission_measurement()
        # Proxy handler completion can trail body delivery very briefly. Allow
        # two handlers of measurement skew. These are observed workload bounds;
        # config unit tests independently verify static per-process division.
        assert observation['peak_proxy_inflight'] <= store['max_concurrent_requests'] + 2, observation
        assert observation['max_attempts_in_sliding_second'] <= store['requests_per_second'] + store['max_concurrent_requests'] + 2, observation
        report['s3_budget'] = {'cluster': {k: store[k] for k in ('requests_per_second', 'max_concurrent_requests', 'max_queued_requests')},
                               'max_replicas': 2, 'per_process': {'requests_per_second': 40, 'max_concurrent_requests': 4, 'max_queued_requests': 32},
                               'outage_attempts_per_process_including_retries': attempted, 'proxy_failed_gets': failed_gets,
                               'outage_queries': outage_evidence,
                               'scope': 'observed workload bounds, not a saturation proof; static allocation is also covered by config tests',
                               'measurement_tolerance': '2 handler completions for concurrency; concurrency+2 arrivals for one-second transport jitter', **observation}
        report['checks'].append('both candidates S3 retries pass transport admission; summed admitted attempts equal observed failed GETs and aggregate observed rate/concurrency stay within stated bounds')
        assert state() == stable
        if args.migrate_sqlite:
            assert snapshot(sqlite_dir / 'catalog.db') == source_snapshot
            report['migration']['source_unchanged_after_postgres_advance'] = True
            report['migration']['post_cutover_differential_checks'] = count
            report['checks'].append('PostgreSQL catch-up and exact queries succeed after Hot old data removal; stopped SQLite source remains unchanged')
        report['catalog'] = stable
        report['s3_compatibility_proxy'] = proxy.snapshot()
        assert report['s3_compatibility_proxy']['conditional_requests_rejected'] == 0
        assert report['s3_compatibility_proxy']['bad_content_md5'] == 0
        assert int(time.time() * 1000) < horizon['next_eligible_at_ms']
        report['status'] = 'passed'
    except Exception as error:
        report['status'], report['failure'] = 'failed', str(error)
        raise
    finally:
        procs.close()
        if source:
            source.close()
        if proxy:
            proxy.close()
        if created:
            try:
                sql('DROP SCHEMA ' + schema + ' CASCADE', False)
            except Exception as error:
                report['cleanup_failure'] = str(error)
                report['status'] = 'failed'
        report['recorded_at_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (work / 'results.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
        print(json.dumps({k: v for k, v in report.items() if k not in ('runtime', 'provenance')}, indent=2))
        if 'cleanup_failure' in report and 'failure' not in report:
            raise RuntimeError('Test schema cleanup failed; see results.json')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workdir', type=Path)
    parser.add_argument('--migrate-sqlite', action='store_true', help='archive and stop SQLite, import metadata, then qualify the PostgreSQL cutover')
    parser.add_argument('--replicas', choices=(1, 2), default=1, type=int, help='Hot/reference VM storage replication')
    parser.add_argument('--gomaxprocs', type=positive_int)
    args = parser.parse_args()
    work = (args.workdir or Path(tempfile.mkdtemp(prefix='vmtier-postgres-e2e-'))).resolve()
    if work.exists() and any(work.iterdir()):
        parser.error('--workdir must be new or empty')
    work.mkdir(parents=True, exist_ok=True)
    print(f'Artifacts: {work}', flush=True)
    run(work, args)


if __name__ == '__main__':
    main()
