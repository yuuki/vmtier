#!/usr/bin/env python3
"""SQLite-backed serve integration against disposable, unmodified VM processes.

Reuses the MVP fixtures and protocol clients. All data, processes, and fault
injection are local to --workdir; no production VM or real S3 service is used.
"""
import argparse
import contextlib
import datetime
import hashlib
import http.server
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import sqlite3
import statistics
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from e2e import (ROOT, HTTP, Cluster, Processes, api, compare,
                 fixture, fixture_horizon, free_port, metrics, positive_int, remote_write, request,
                 start_select, test_s3_request, timed)
from s3_compat_proxy import S3CompatibilityProxy
from blob_outage import assert_running_blob_failure, unavailable_blobs


class ControlledSource:
    """Loopback failure injector for source outage and interrupted native export."""
    def __init__(self, hot_port):
        self.unavailable = True
        self.unavailable_tenants = set()
        self.pause = False
        self.started = threading.Event()
        self.release = threading.Event()
        self.failed = threading.Event()
        self.lock = threading.Lock()
        self.native_requests = 0
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    if owner.unavailable or any(self.path.startswith(f'/select/{tenant}/') for tenant in owner.unavailable_tenants):
                        owner.failed.set()
                        self.send_response(503)
                        self.end_headers()
                        self.wfile.write(b'source outage fixture')
                        return
                    with HTTP.open(f'http://127.0.0.1:{hot_port}' + self.path, timeout=30) as upstream:
                        body = upstream.read()
                        status = upstream.status
                        content_type = upstream.headers.get('Content-Type', '')
                    pause = False
                    if '/export/native?' in self.path:
                        with owner.lock:
                            owner.native_requests += 1
                            if owner.pause:
                                owner.pause = False
                                pause = True
                    self.send_response(status)
                    self.send_header('Content-Type', content_type)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    if pause:
                        self.wfile.write(body[:16])
                        self.wfile.flush()
                        owner.started.set()
                        owner.release.wait(30)
                        self.wfile.write(body[16:])
                    else:
                        self.wfile.write(body)
                except urllib.error.HTTPError as exc:
                    self.send_response(exc.code)
                    self.end_headers()
                    self.wfile.write(exc.read())
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def snapshot(path):
    # Test observation only: a read-only SQLite snapshot, never an ownership or
    # publication bypass. The real offline commands must reject the live owner.
    with contextlib.closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        state = dict(db.execute('SELECT * FROM catalog_state').fetchone())
        coverage = [dict(row) for row in db.execute('SELECT * FROM coverage ORDER BY tenant,source_id')]
        windows = [dict(row) for row in db.execute('SELECT * FROM windows ORDER BY tenant,source_id,start_ms')]
        packs = [dict(row) for row in db.execute('SELECT * FROM packs ORDER BY window_id,ordinal')]
        return {'state': state, 'coverage': coverage, 'windows': windows, 'packs': packs}


def assert_http_failure(fn, context, status=None):
    try:
        fn()
    except urllib.error.HTTPError as error:
        assert error.code >= 400
        if status is not None:
            assert error.code == status, f'{context}: expected HTTP {status}, got {error.code}'
    else:
        raise AssertionError(f'{context}: unexpected successful HTTP response')


def wait_ready(process, port, work, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f'serve exited {process.returncode}; see logs in {work}')
        try:
            request(f'http://127.0.0.1:{port}/ready', timeout=2)
            return
        except (OSError, urllib.error.URLError):
            time.sleep(.1)
    raise AssertionError(f'serve did not become ready; see logs in {work}')


def wait_query_reconnect(fn):
    # Stock vmselect caches failed dials and probes unavailable storage once per
    # second. Daemon readiness cannot clear the frontend's cached dial error.
    began = time.monotonic()
    retries = 0
    while True:
        try:
            return fn(), time.monotonic() - began, retries
        except urllib.error.HTTPError as error:
            body = error.read().decode()
            if (error.code != 503 or 'cannot obtain connection from a pool' not in body or
                    'connection refused' not in body or time.monotonic() - began >= 5):
                raise
            retries += 1
            time.sleep(.1)


def expressions(boundary):
    return [
        'requests_total', 'temperature', 'cold_only', 'hot_only', 'irregular', 'special', 'stale_series',
        'duplicates', 'churn', 'millisecond_boundary',
        'rate(requests_total[10m])', 'increase(requests_total[20m])',
        'avg_over_time(temperature[7m])', 'sum_over_time(irregular[10m])',
        'sum by (job) (rate(requests_total[10m]))',
        'temperature / on(instance,job,zone) factor',
        'temperature offset 2h', 'avg_over_time(rate(requests_total[3m])[10m:1m])',
        f'temperature @ {boundary / 1000}',
        '{__name__=~"temperature|cold_only",instance!="b"}',
        '{__name__="requests_total",zone=~"east|west"}',
        '{__name__="requests_total",zone!~"west"}',
        '{__name__="empty_label",maybe=""}', '{__name__="empty_label",absent=""}',
        '{__name__="empty_label",absent!="x"}', '{__name__="empty_label",absent=~".*"}',
        '{__name__="empty_label",absent!~"x"}',
        '{__name__=~"cold_only|temperature",job!~".*"}',
        '{__name__=~"cold_only|temperature",job!~"^.*$"}',
        '{__name__=~"cold_only|temperature",job!~"demo|"}',
    ]


def start_tiered_select(processes, name, port, nodes):
    # Metadata currently validates payload blocks. Allow the shared S3 request
    # budget to pace these reads; the stock default labels deadline is only 5s.
    return processes.start(name, [ROOT / '.bin/vmselect',
        f'-httpListenAddr=127.0.0.1:{port}',
        '-storageNode=' + ','.join(f'127.0.0.1:{p}' for p in nodes),
        '-replicationFactor=1', '-globalReplicationFactor=1', '-search.skipSlowReplicas=false',
        '-search.denyPartialResponse=true', '-search.disableCache',
        '-search.maxLabelsAPIDuration=30s', '-dedup.minScrapeInterval=1ms',
        '-memory.allowedBytes=128MiB', f'-cacheDataPath={processes.work / name}'],
        f'http://127.0.0.1:{port}/health')


def provenance():
    digest = hashlib.sha256()
    paths = sorted(list((ROOT / 'cmd').rglob('*.go')) + list((ROOT / 'internal').rglob('*.go')) +
                   [ROOT / 'go.mod', ROOT / 'go.sum'])
    for path in paths:
        digest.update(str(path.relative_to(ROOT)).encode() + b'\0' + path.read_bytes() + b'\0')
    return {
        'executor_hostname': socket.gethostname(),
        'binary_sha256': {name: hashlib.sha256((ROOT / '.bin' / name).read_bytes()).hexdigest()
                          for name in ('vmtier', 'vmstorage', 'vminsert', 'vmselect')},
        'harness_sha256': {name: hashlib.sha256((ROOT / 'scripts' / name).read_bytes()).hexdigest()
                          for name in ('e2e_serve.py', 'e2e.py', 's3_compat_proxy.py')},
        'go_source_tree_sha256': digest.hexdigest(),
        'go_source_tree_hash_scope': 'sorted cmd/**/*.go, internal/**/*.go, go.mod and go.sum; path NUL bytes NUL',
    }


def run(work, args):
    work = work.resolve()
    procs = Processes(work, args.gomaxprocs)
    source_proxy = s3_proxy = None
    report = {'backend': args.backend, 'replicas': args.replicas,
              'architecture': 'SQLite catalog with one serve process, scheduled archive and read gateway',
              'started_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'vm_revision': '9a009c491da4c17531893ab4acb6a5f1b5e0eaf2',
              'dedup_interval': '1ms', 'vm_result_cache': 'disabled',
              'tiered_vm_labels_api_max_duration': '30s',
              'comparison': 'exact timestamps, labels and numeric strings; no tolerance',
              'checks': [], 'query_timings_ms': {}, 'runtime': procs.runtime}
    env = {'AWS_ACCESS_KEY_ID': 'vmtier-test', 'AWS_SECRET_ACCESS_KEY': 'vmtier-test-secret'}
    try:
        for name in ('vmstorage', 'vminsert', 'vmselect', 'vmtier'):
            if not (ROOT / '.bin' / name).is_file():
                raise RuntimeError(f'Missing .bin/{name}; build it first')
        report['provenance'] = provenance()
        report['environment'] = {
            'os': platform.freedesktop_os_release().get('PRETTY_NAME', platform.system()),
            'machine': platform.machine(), 'python': platform.python_version(),
            'linux_kernel': platform.release(), 'logical_cpus_available': len(os.sched_getaffinity(0)),
            'measurement_scope': 'single development container, loopback; no real Sakura/AWS/MinIO service',
            'moto': None,
        }
        go = os.environ.get('GO_BIN') or shutil.which('go')
        if go:
            report['environment']['go'] = subprocess.check_output([go, 'version'], text=True).strip()
        if args.backend == 'local':
            store = {'type': 'local', 'path': str(work / 'objects')}
        else:
            moto = os.environ.get('MOTO_SERVER') or shutil.which('moto_server')
            if not moto and Path('/workspace/tools/moto-venv/bin/moto_server').exists():
                moto = '/workspace/tools/moto-venv/bin/moto_server'
            if not moto:
                raise RuntimeError('Install scripts/requirements-e2e.txt or set MOTO_SERVER')
            python = Path(moto).resolve().parent / 'python'
            if python.exists():
                report['environment']['moto'] = subprocess.check_output(
                    [python, '-c', 'from importlib.metadata import version; print(version("moto"))'], text=True).strip()
            endpoint = f'http://127.0.0.1:{free_port()}'
            s3_process = procs.start('moto-s3', [moto, '-H', '127.0.0.1', '-p', endpoint.rsplit(':', 1)[1]], endpoint, env)
            bucket = 'vmtier-serve-test'
            with HTTP.open(urllib.request.Request(endpoint + '/' + bucket, method='PUT', data=b'')) as response:
                response.read()
            s3_proxy = S3CompatibilityProxy(endpoint)
            store = {'type': 's3', 'endpoint': s3_proxy.url, 'region': 'us-east-1',
                     'bucket': bucket, 'prefix': 'serve', 'requests_per_second': 80,
                     'max_concurrent_requests': 8, 'max_queued_requests': 64}

        ref, hot = Cluster(procs, 'reference', args.replicas), Cluster(procs, 'hot', args.replicas)
        ref.start('full')
        hot.start('full')
        horizon = fixture_horizon(int(time.time() * 1000))
        start, boundary, end = (horizon[key] for key in ('start_ms', 'boundary_ms', 'end_ms'))
        age_ms, hour = horizon['archive_age_ms'], horizon['window_ms']
        tenants = ['1:2', '7:9']
        count = 0
        for i, tenant in enumerate(tenants):
            data = fixture(start, boundary, end, i)
            count += remote_write(ref.insert, tenant, data)
            remote_write(hot.insert, tenant, data)
        ref.flush()
        hot.flush()
        report['fixture'] = {**horizon, 'input_samples': count,
                             'tenants': tenants, 'series_per_tenant': 16, 'scrape_interval_ms': 30000,
                             'special_cases': ['irregular gaps', 'counter reset', 'stale marker', 'NaN',
                                               '+Inf', '-Inf', 'duplicate/conflicting timestamps',
                                               'millisecond boundary', 'empty/absent labels']}
        for tenant in tenants:
            params = {'query': 'cold_only', 'time': (boundary - 600000) / 1000}
            compare(api(ref.select, tenant, 'query', params), api(hot.select, tenant, 'query', params), 'initial data parity')
        report['checks'].append('reference and pre-archive Hot contain the same old data')
        source_proxy = ControlledSource(hot.select)
        catalog_dir = work / 'catalog'
        catalog_id = 'serve-e2e'
        database = catalog_dir / 'catalog.db'
        cfg = {'store': store, 'catalog': {'type': 'sqlite', 'path': str(catalog_dir), 'id': catalog_id},
               'tenants': tenants, 'sources': [{'id': 'hot', 'url': source_proxy.url}],
               'archive_age': f'{age_ms}ms', 'hot_retention': '240h', 'lateness': '30m',
               'safety_margin': '30m', 'window': '1h', 'lateness_enforced': True,
               'coverage_start_ms': start, 'service': {'archive_interval': '100ms'}}
        config_path = work / 'config.json'
        config_path.write_text(json.dumps(cfg, indent=2))
        binary = ROOT / '.bin/vmtier'
        rpc_port, http_port = free_port(), free_port()

        def serve_command(path=config_path, rpc=rpc_port, http=http_port):
            return [binary, 'serve', '-config', path, '-listen', f'127.0.0.1:{rpc}', '-http', f'127.0.0.1:{http}']

        def start_service(label, ready=True):
            process = procs.start(label, serve_command(), f'http://127.0.0.1:{http_port}/health', env)
            if ready:
                wait_ready(process, http_port, work)
            return process

        def stop_service(process):
            began = time.monotonic()
            procs.stop(process)
            elapsed = time.monotonic() - began
            assert process.returncode == 0, f'serve did not shut down cleanly: {process.returncode}'
            assert elapsed < 15, f'graceful shutdown exceeded budget: {elapsed}'
            return elapsed

        def object_url(key):
            return store['endpoint'] + '/' + store['bucket'] + '/' + store['prefix'] + '/' + key

        def object_get(key):
            return (Path(store['path']) / key).read_bytes() if args.backend == 'local' else test_s3_request(object_url(key))

        def object_set(key, data):
            if args.backend == 'local':
                path = Path(store['path']) / key
                path.unlink() if data is None else path.write_bytes(data)
            else:
                test_s3_request(object_url(key), 'DELETE' if data is None else 'PUT', data)
                if data is not None:
                    assert object_get(key) == data

        # Missing state must not be initialized implicitly by the daemon.
        procs.command_stats(serve_command(), work / 'serve-missing.log', env, False)
        assert not database.exists()
        procs.command_stats([binary, 'catalog-init', '-backend', 'sqlite', '-config', config_path,
                       '-path', catalog_dir, '-id', catalog_id], work / 'catalog-init.log', env)
        assert snapshot(database)['state']['generation'] == 0
        report['checks'].append('serve refuses missing state; SQLite initialization is explicit')
        process = start_service('serve-source-outage', ready=False)
        assert source_proxy.failed.wait(10), 'scheduler never attempted unavailable source'
        assert_http_failure(lambda: request(f'http://127.0.0.1:{http_port}/ready'), 'source outage readiness', status=503)
        assert snapshot(database)['state']['generation'] == 0
        cold_select = free_port()
        start_select(procs, 'cold-only-select', cold_select, [rpc_port])
        failure_query = {'query': 'cold_only', 'time': (boundary - 600000) / 1000, 'timeout': '3s'}
        assert_http_failure(lambda: api(cold_select, tenants[0], 'query', failure_query), 'unarchived required coverage')
        report['checks'].append('source outage leaves zero coverage and fails readiness and required Cold queries')

        # Stop during a native stream, before the first commit, then resume from disk.
        source_proxy.pause = True
        source_proxy.unavailable = False
        assert source_proxy.started.wait(10), 'scheduler never reached paused native stream'
        report['interrupted_shutdown_seconds'] = stop_service(process)
        assert snapshot(database)['state']['generation'] == 0
        source_proxy.release.set()
        source_proxy.unavailable_tenants = {tenants[0]}
        process = start_service('serve-resumed-independent-streams', ready=False)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            partial = snapshot(database)
            if next(row['end_ms'] for row in partial['coverage'] if row['tenant'] == tenants[1]) == boundary:
                break
            assert process.poll() is None, 'serve exited during independent-stream catch-up'
            time.sleep(.1)
        else:
            raise AssertionError('healthy tenant did not advance while another source stream failed')
        assert partial['state']['generation'] == 3
        assert next(row['end_ms'] for row in partial['coverage'] if row['tenant'] == tenants[0]) == start
        assert_http_failure(lambda: request(f'http://127.0.0.1:{http_port}/ready'), 'partial tenant coverage', status=503)
        assert_http_failure(lambda: api(cold_select, tenants[0], 'query', failure_query), 'failed tenant coverage')
        compare(api(ref.select, tenants[1], 'query', failure_query),
                api(cold_select, tenants[1], 'query', failure_query), 'independent healthy tenant')
        report['checks'].append('healthy tenant reaches coverage and remains queryable while another source stream fails')
        source_proxy.unavailable_tenants = set()
        wait_ready(process, http_port, work)
        stable = snapshot(database)
        assert stable['state']['generation'] == 6
        assert len(stable['windows']) == 6 and len(stable['packs']) == 6
        assert all(row['end_ms'] == boundary for row in stable['coverage'])
        assert all(row['end_ms'] <= boundary for row in stable['windows'])
        report['archive_metrics'] = metrics(http_port)
        assert report['archive_metrics']['vmtier_archive_committed_windows_total'] == 6
        assert report['archive_metrics']['vmtier_archive_samples_total'] > 0
        assert report['archive_metrics']['vmtier_archive_bytes_total'] > 0
        report['checks'].append('SIGTERM during native export publishes nothing; restart automatically archives six stream windows')
        report['catalog'] = {'schema': stable['state']['schema_version'], 'generation': 6,
                             'windows': len(stable['windows']), 'packs': len(stable['packs']),
                             'coverage': stable['coverage']}
        report['object_bytes'] = {'pack': sum(len(object_get(p['data_key'])) for p in stable['packs']),
                                  'index': sum(len(object_get(p['index_key'])) for p in stable['packs'])}
        try:
            object_get('catalog.json')
        except (FileNotFoundError, urllib.error.HTTPError) as error:
            if isinstance(error, urllib.error.HTTPError):
                assert error.code == 404
        else:
            raise AssertionError('S3/local blobs unexpectedly contain a legacy catalog.json')

        # The supported offline APIs and a second daemon must honor lifetime ownership.
        procs.command_stats([binary, 'catalog-backup', '-path', catalog_dir, '-id', catalog_id,
                       '-out', work / 'forbidden-live-backup'], work / 'backup-owned.log', env, False)
        procs.command_stats(serve_command(rpc=free_port(), http=free_port()), work / 'serve-second-owner.log', env, False)
        procs.command_stats([binary, 'verify', '-config', config_path], work / 'verify-owned.log', env, False)
        procs.command_stats([binary, 'inspect', '-config', config_path], work / 'inspect-owned.log', env, False)
        assert not (work / 'forbidden-live-backup').exists()
        assert snapshot(database) == stable
        report['checks'].append('live owner excludes offline inspect/verify/backup and a second serve process without changing catalog')

        params = {'query': 'cold_only', 'time': (boundary - 600000) / 1000}
        before = metrics(http_port)
        actual = api(cold_select, tenants[0], 'query', params)
        compare(api(ref.select, tenants[0], 'query', params), actual, 'SQLite native gateway spike')
        assert actual['result']
        range_key, byte_key = 'vmtier_object_range_requests_total', 'vmtier_object_download_bytes_total'
        assert metrics(http_port).get(range_key, 0) > before.get(range_key, 0)
        report['checks'].append('stock vmselect reads committed SQLite native packs through independent Cold RPC and Range GET')
        for tenant in tenants:
            raw = request(f'http://127.0.0.1:{cold_select}/select/{tenant}/prometheus/api/v1/export?' +
                          urllib.parse.urlencode({'match[]': '{__name__!=""}', 'start': boundary / 1000, 'end': end / 1000})).strip()
            assert not raw, f'Cold returned young samples: {raw[:500]}'
        report['checks'].append('Cold raw export excludes young samples at and after the exact boundary millisecond')

        hot.stop()
        hot.start('recent-only')
        for i, tenant in enumerate(tenants):
            remote_write(hot.insert, tenant, fixture(start, boundary, end, i), start=boundary)
        hot.flush()
        for tenant in tenants:
            raw = request(f'http://127.0.0.1:{hot.select}/select/{tenant}/prometheus/api/v1/export?' +
                          urllib.parse.urlencode({'match[]': '{__name__!=""}', 'start': start / 1000,
                                                  'end': (boundary - 1) / 1000})).strip()
            assert not raw, f'old data remained in rebuilt Hot: {raw[:500]}'
        report['checks'].append('fresh Hot contains recent data only; both tenants have empty old-range raw exports')
        exports = source_proxy.native_requests
        report['restart_shutdown_seconds'] = stop_service(process)

        # These changes remain syntactically valid but violate the pinned authority.
        variants = [('archive_age', dict(cfg, archive_age=f'{age_ms + 60000}ms')),
                    ('lateness', dict(cfg, lateness='45m')),
                    ('source', dict(cfg, sources=[{'id': 'hot', 'url': f'http://127.0.0.1:{free_port()}'}]))]
        changed_store = dict(store)
        changed_store['path' if args.backend == 'local' else 'prefix'] = str(work / 'wrong-objects') if args.backend == 'local' else 'wrong-prefix'
        variants.append(('blob-locator', dict(cfg, store=changed_store)))
        variants.append(('identity', dict(cfg, catalog=dict(cfg['catalog'], id='wrong-id'))))
        for label, changed in variants:
            path = work / f'changed-{label}.json'
            path.write_text(json.dumps(changed))
            procs.command_stats(serve_command(path), work / f'reject-{label}.log', env, False)
            assert snapshot(database) == stable
        process = start_service('serve-restarted-after-hot-removal')
        assert snapshot(database) == stable and source_proxy.native_requests == exports
        report['checks'].append('restart preserves coverage and avoids reexport; changed policy/source/blob locator/identity are rejected')
        top_select = free_port()
        start_tiered_select(procs, 'tiered-select', top_select, hot.storage_read + [rpc_port])
        count = 0
        for tenant in tenants:
            for label, (lo, hi) in {'cold': (start + 600000, boundary - 600000),
                                    'hot': (boundary + 600000, end),
                                    'boundary': (boundary - 900000, boundary + 900000)}.items():
                for expr in expressions(boundary):
                    params = {'query': expr, 'start': lo / 1000, 'end': hi / 1000, 'step': '60s'}
                    expected, ref_ms = timed(lambda: api(ref.select, tenant, 'query_range', params))
                    actual, tier_ms = timed(lambda: api(top_select, tenant, 'query_range', params))
                    compare(expected, actual, f'{tenant} {label} range {expr}')
                    report['query_timings_ms'].setdefault(label + '_reference', []).append(ref_ms)
                    report['query_timings_ms'].setdefault(label + '_tiered', []).append(tier_ms)
                    count += 1
                for expr in expressions(boundary):
                    params = {'query': expr, 'time': hi / 1000}
                    compare(api(ref.select, tenant, 'query', params), api(top_select, tenant, 'query', params),
                            f'{tenant} {label} instant {expr}')
                    count += 1
        assert count == 360
        report['differential_checks'] = count
        report['checks'].append('360 exact instant/range comparisons passed across Cold, Hot and the boundary')
        for tenant in tenants:
            for path in ['labels', 'label/cold_label/values', 'label/__name__/values', 'series']:
                params = {'start': start / 1000, 'end': end / 1000, 'match[]': '{__name__!=""}'}
                expected, actual = api(ref.select, tenant, path, params), api(top_select, tenant, path, params)
                if expected and isinstance(expected[0], str):
                    expected, actual = sorted(expected), sorted(actual)
                compare(expected, actual, f'{tenant} metadata {path}')
        report['checks'].append('Cold label names/values and series discovery match the reference for both tenants')
        report['final_metrics'] = metrics(http_port)
        if args.backend == 's3':
            assert report['final_metrics'].get('vmtier_s3_attempts_total', 0) > 0
            assert 'vmtier_s3_waited_requests_total' in report['final_metrics']
            assert report['final_metrics'].get('vmtier_s3_rejected_requests_total', 0) == 0
            report['checks'].append('shared S3 transport exposes admitted attempts and wait metrics without queue rejection')

        stop_service(process)
        cfg['cache_bytes'] = 32 << 20
        config_path.write_text(json.dumps(cfg, indent=2))
        process = start_service('serve-cache-enabled')
        cache_params = {'query': 'cold_only[30m]', 'time': (start + 7200000) / 1000}
        a = metrics(http_port)
        cold, cold_ms = timed(lambda: api(top_select, tenants[0], 'query', cache_params))
        b = metrics(http_port)
        warm, warm_ms = timed(lambda: api(top_select, tenants[0], 'query', cache_params))
        c = metrics(http_port)
        compare(cold, warm, 'immutable cache repeat')
        compare(api(ref.select, tenants[0], 'query', cache_params), cold, 'cache reference')
        assert b.get(range_key, 0) > a.get(range_key, 0)
        assert c.get(range_key, 0) == b.get(range_key, 0)
        report['cache_measurement'] = {'cold_latency_ms': cold_ms, 'warm_latency_ms': warm_ms,
                                       'cold_range_gets': b.get(range_key, 0) - a.get(range_key, 0),
                                       'warm_range_gets': c.get(range_key, 0) - b.get(range_key, 0),
                                       'warm_blob_bytes': c.get(byte_key, 0) - b.get(byte_key, 0),
                                       'cache_limit_bytes': cfg['cache_bytes'], 'vm_result_cache': 'disabled'}
        report['checks'].append('warm immutable cache returns equal data with no additional Range GETs')
        stop_service(process)
        assert_http_failure(lambda: api(top_select, tenants[0], 'query', dict(cache_params, timeout='3s')), 'stopped service')
        cfg['cache_bytes'] = 0
        config_path.write_text(json.dumps(cfg, indent=2))
        process = start_service('serve-fault-tests')
        recovered, elapsed, retries = wait_query_reconnect(lambda: api(top_select, tenants[0], 'query', cache_params))
        compare(cold, recovered, 'restart restores previously warm query')
        report['frontend_reconnect'] = {'seconds': elapsed, 'cached_dial_error_retries': retries,
                                        'cached_dial_error_retry_budget_seconds': 5}
        report['checks'].append('service outage propagates as query failure; restart restores reads from persistent SQLite state')

        first_window = next(w for w in stable['windows'] if w['tenant'] == tenants[0] and w['start_ms'] == start)
        damaged_key = next(p['data_key'] for p in stable['packs'] if p['window_id'] == first_window['id'])
        original = object_get(damaged_key)
        fail_params = {'query': 'count_over_time({__name__!=""}[30m]) keep_metric_names',
                       'time': (start + 1800000) / 1000, 'timeout': '3s'}
        for label, content in [('missing', None), ('truncated', original[:8]),
                               ('checksum', bytes([original[0] ^ 1]) + original[1:])]:
            object_set(damaged_key, content)
            try:
                assert_http_failure(lambda: api(top_select, tenants[0], 'query', fail_params), label)
            finally:
                object_set(damaged_key, original)
            compare(api(ref.select, tenants[0], 'query', fail_params), api(top_select, tenants[0], 'query', fail_params), label + ' recovery')
        assert snapshot(database) == stable
        report['checks'].append('missing/truncated/checksum-invalid payloads fail queries; restored bytes recover without catalog mutation')
        assert cfg['cache_bytes'] == 0
        outage_params = dict(fail_params, timeout='15s')
        expected = api(ref.select, tenants[0], 'query', outage_params)
        compare(expected, api(top_select, tenants[0], 'query', outage_params), 'blob outage baseline')
        request(f'http://127.0.0.1:{http_port}/health')
        request(f'http://127.0.0.1:{http_port}/ready')
        before_proxy = s3_proxy.snapshot() if s3_proxy else None
        with unavailable_blobs(store, work, s3_proxy) as marker:
            evidence = assert_running_blob_failure(
                lambda: api(top_select, tenants[0], 'query', outage_params),
                process, lambda: metrics(http_port), marker)
            request(f'http://127.0.0.1:{http_port}/health')
            request(f'http://127.0.0.1:{http_port}/ready')
            assert snapshot(database) == stable
        compare(expected, api(top_select, tenants[0], 'query', outage_params), 'blob outage recovery')
        assert snapshot(database) == stable and source_proxy.native_requests == exports
        if s3_proxy:
            evidence['proxy_failed_gets'] = s3_proxy.snapshot()['unavailable_get_requests'] - before_proxy['unavailable_get_requests']
            assert evidence['proxy_failed_gets'] > 0
        evidence.update(cache_bytes=0, process_pid=process.pid,
                        coverage_unchanged=True, healthy_and_ready_during_outage=True,
                        exact_baseline_and_recovery=True)
        report['blob_outage'] = evidence
        report['checks'].append('running service attempts blob GET and returns its error; health/coverage readiness stay successful and exact reads recover without publication changes')
        try:
            request(f'http://127.0.0.1:{cold_select}/delete/{tenants[0]}/prometheus/api/v1/admin/tsdb/delete_series?' +
                    urllib.parse.urlencode({'match[]': 'cold_only'}), data=b'')
        except urllib.error.HTTPError as error:
            message = error.read().decode().lower()
            assert 'unsupported' in message or 'not supported' in message, message
        else:
            raise AssertionError('Cold delete unexpectedly succeeded')
        report['checks'].append('Cold delete remains explicitly unsupported')
        stop_service(process)
        assert snapshot(database) == stable and source_proxy.native_requests == exports
        procs.command_stats([binary, 'catalog-backup', '-path', catalog_dir, '-id', catalog_id,
                       '-out', work / 'final-backup'], work / 'backup-after-stop.log', env)
        procs.command_stats([binary, 'verify', '-config', config_path], work / 'verify-after-stop.log', env)
        report['checks'].append('clean shutdown releases ownership for offline backup/verify and leaves completed windows unchanged')

        if s3_proxy:
            counters = s3_proxy.snapshot()
            assert counters['conditional_requests_rejected'] == 0
            assert counters['etag_headers_removed'] > 0 and counters['range_requests'] > 0
            assert counters['put_requests'] > 0 and counters['put_content_md5'] == counters['put_requests']
            assert counters['bad_content_md5'] == 0
            report['s3_compatibility_proxy'] = counters
            report['checks'].append('S3 traffic passes conditional-request rejection and ETag removal; every PUT has valid Content-MD5')
        for key, values in report['query_timings_ms'].items():
            ordered = sorted(values)
            report['query_timings_ms'][key] = {'count': len(values), 'median': statistics.median(values),
                                              'p95': ordered[min(len(ordered) - 1, int(.95 * len(ordered)))]}
        assert int(time.time() * 1000) < horizon['next_eligible_at_ms'], 'Fixture exceeded its 30-minute eligibility horizon'
        report['status'] = 'passed'
    except Exception as error:
        report['status'], report['failure'] = 'failed', str(error)
        raise
    finally:
        procs.close()
        if source_proxy:
            source_proxy.close()
        if s3_proxy:
            s3_proxy.close()
        if 'fixture' in report:
            report['fixture']['finished_at_ms'] = int(time.time() * 1000)
        report['recorded_at_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (work / 'results.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
        print(json.dumps({k: v for k, v in report.items() if k not in ('final_metrics', 'provenance')}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend', choices=['local', 's3'], default='local')
    parser.add_argument('--replicas', choices=[1, 2], type=int, default=1)
    parser.add_argument('--workdir', type=Path)
    parser.add_argument('--gomaxprocs', type=positive_int, help='GOMAXPROCS for workload children (default: outer GOMAXPROCS, else 2)')
    args = parser.parse_args()
    work = args.workdir or Path(tempfile.mkdtemp(prefix='vmtier-serve-e2e-'))
    if work.exists() and any(work.iterdir()):
        parser.error('--workdir must be new or empty; existing evidence is preserved')
    work.mkdir(parents=True, exist_ok=True)
    print(f'Artifacts: {work}', flush=True)
    run(work, args)


if __name__ == '__main__':
    main()
