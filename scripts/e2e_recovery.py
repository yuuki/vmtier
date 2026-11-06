#!/usr/bin/env python3
"""Offline cutover/recovery and process-crash checks against stock VM.

Only disposable loopback processes and local objects are used. The expired-source
case reconstructs an empty Hot fixture; it is not natural VM retention expiry.
"""
import argparse
import datetime
import hashlib
import http.server
import json
import platform
import signal
import threading
import time
import urllib.error
import urllib.parse
from pathlib import Path

from e2e import (ROOT, HTTP, Cluster, Processes, api, compare, fixture, free_port,
                 positive_int, remote_write, request, start_select)
from e2e_serve import expressions, provenance, snapshot, wait_ready


class RecoverySource:
    """Pause the independent raw oracle after native packs have been uploaded."""
    def __init__(self, hot_port):
        self.pause_raw = False
        self.started, self.release = threading.Event(), threading.Event()
        self.requests = []
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.append(self.path)
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                if owner.pause_raw and query.get('query', [''])[0].startswith('{__name__!=""}['):
                    owner.pause_raw = False
                    owner.started.set()
                    if not owner.release.wait(60):
                        return
                try:
                    with HTTP.open(f'http://127.0.0.1:{hot_port}' + self.path, timeout=30) as upstream:
                        body = upstream.read()
                        self.send_response(upstream.status)
                        self.send_header('Content-Type', upstream.headers.get('Content-Type', ''))
                        self.send_header('Content-Length', str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                except urllib.error.HTTPError as error:
                    self.send_response(error.code)
                    self.end_headers()
                    self.wfile.write(error.read())
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


def object_manifest(directory):
    # Record stat identity as well as content: import must not rewrite identical
    # bytes either. This is local-fixture evidence, not an S3 LIST recovery path.
    return {str(path.relative_to(directory)): {
        'bytes': path.stat().st_size, 'mtime_ns': path.stat().st_mtime_ns,
        'inode': path.stat().st_ino, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        for path in sorted(directory.rglob('*')) if path.is_file()}


def write_config(work, name, cfg):
    path = work / (name + '.json')
    path.write_text(json.dumps(cfg, indent=2) + '\n')
    return path


def coverage_failure(fn, expected_status=422):
    """Require the coverage error, not an unrelated connection/query failure."""
    try:
        fn()
    except urllib.error.HTTPError as error:
        body = error.read().decode()
        assert error.code == expected_status, (error.code, body)
        assert 'unarchived required interval' in body, body
        return {'http_status': error.code, 'body': body}
    raise AssertionError('missing suffix unexpectedly returned successfully')


def readiness_failure(port):
    try:
        request(f'http://127.0.0.1:{port}/ready')
    except urllib.error.HTTPError as error:
        body = error.read().decode()
        assert error.code == 503 and 'archive coverage' in body and 'required' in body, (error.code, body)
        return {'http_status': error.code, 'body': body}
    raise AssertionError('missing suffix unexpectedly became ready')


def run(work, args):
    work = work.resolve()
    procs = Processes(work, args.gomaxprocs)
    source = None
    report = {'started_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'status': 'running', 'backend': 'local', 'replicas': args.replicas,
              'vm_revision': '9a009c491da4c17531893ab4acb6a5f1b5e0eaf2',
              'comparison': 'exact labels, timestamps and numeric strings; no tolerance',
              'checks': [], 'runtime': procs.runtime, 'differential_checks': {},
              'limitations': ['loopback local filesystem; no real provider',
                              'SIGKILL is not physical power-loss durability',
                              'expired-source absence is reconstructed, not natural VM retention expiry']}
    binary = ROOT / '.bin/vmtier'

    def command(label, args):
        return procs.command_stats([binary, *args], work / (label + '.log'))

    def stop(process):
        procs.stop(process)
        assert process.returncode == 0, f'unclean shutdown: {process.returncode}'

    def start_serve(label, config, ready=True):
        rpc, http = free_port(), free_port()
        process = procs.start(label, [binary, 'serve', '-config', config,
            '-listen', f'127.0.0.1:{rpc}', '-http', f'127.0.0.1:{http}'],
            f'http://127.0.0.1:{http}/health')
        if ready:
            wait_ready(process, http, work)
        return process, rpc, http

    try:
        report['provenance'] = provenance()
        report['provenance']['harness_sha256']['e2e_recovery.py'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        report['environment'] = {'os': platform.platform(), 'python': platform.python_version(),
                                 'scope': 'disposable managed container; stock VM memory.allowedBytes=128MiB per process'}
        ref, hot = Cluster(procs, 'reference', args.replicas), Cluster(procs, 'hot', args.replicas)
        ref.start('full')
        hot.start('full')
        hour = 3600000
        floor = int(time.time() * 1000) // hour * hour
        start, boundary, end = floor - 6 * hour, floor - 3 * hour, floor - hour
        tenants = ['1:2', '7:9']
        samples = 0
        for i, tenant in enumerate(tenants):
            data = fixture(start, boundary, end, i)
            samples += remote_write(ref.insert, tenant, data)
            remote_write(hot.insert, tenant, data)
        ref.flush()
        hot.flush()
        source = RecoverySource(hot.select)
        # A real wall-clock boundary makes the old backup stale during this run.
        # No production clock override or mutable archive contract is involved.
        created = int(time.time() * 1000)
        next_eligible = created + args.rollover_delay * 1000
        age = next_eligible - boundary - hour
        report['fixture'] = {'tenants': tenants, 'series_per_tenant': 16, 'input_samples': samples,
                             'start_ms': start, 'initial_boundary_ms': boundary, 'end_ms': end,
                             'window_ms': hour, 'archive_age_ms': age,
                             'next_eligible_at_ms': next_eligible, 'configured_hot_retention_ms': 240 * hour,
                             'stock_hot_retention': '30d', 'dedup_interval': '1ms', 'result_cache': 'disabled'}
        objects = work / 'objects'
        json_dir, json_id = work / 'json-catalog', 'recovery-json-v2'
        catalog_port = free_port()
        catalog_url = f'http://127.0.0.1:{catalog_port}'
        command('json-init', ['catalog-init', '-path', json_dir, '-id', json_id])
        catalog_process = procs.start('json-authority', [binary, 'catalog', '-path', json_dir,
            '-id', json_id, '-http', f'127.0.0.1:{catalog_port}'], catalog_url + '/v1/catalog')
        cfg = {'store': {'type': 'local', 'path': str(objects)},
               'catalog': {'type': 'http', 'url': catalog_url, 'id': json_id},
               'tenants': tenants, 'sources': [{'id': 'hot', 'url': source.url}],
               'archive_age': f'{age}ms', 'hot_retention': '240h', 'lateness': '30m',
               'safety_margin': '30m', 'window': '1h', 'lateness_enforced': True,
               'coverage_start_ms': start, 'service': {'archive_interval': '100ms'}}
        json_config = write_config(work, 'json-config', cfg)
        command('json-archive', ['archive', '-config', json_config, '-start', str(start), '-end', str(boundary)])
        manifest = json.loads(request(catalog_url + '/v1/catalog'))
        old_rpc, old_http = free_port(), free_port()
        gateway = procs.start('json-gateway', [binary, 'gateway', '-config', json_config,
            '-listen', f'127.0.0.1:{old_rpc}', '-http', f'127.0.0.1:{old_http}'],
            f'http://127.0.0.1:{old_http}/ready')
        old_cold, old_top = free_port(), free_port()
        start_select(procs, 'json-cold-select', old_cold, [old_rpc])
        start_select(procs, 'json-tiered-select', old_top, hot.storage_read + [old_rpc])

        def compare_stage(label, cold, tiered, split):
            count = 0
            for tenant in tenants:
                for interval, port, lo, hi in [('cold', cold, start + 600000, split - 600000),
                                              ('boundary', tiered, split - 900000, split + 900000)]:
                    # The Cold-only frontend cannot answer an explicit @ at
                    # the first Hot millisecond; keep that expression inside
                    # the archived interval, while the tiered case spans it.
                    for expr in expressions(split - 600000 if interval == 'cold' else split):
                        params = {'query': expr, 'start': lo / 1000, 'end': hi / 1000, 'step': '60s'}
                        expected = api(ref.select, tenant, 'query_range', params)
                        compare(expected, api(port, tenant, 'query_range', params), f'{label} {tenant} {interval} {expr}')
                        count += 1
            report['differential_checks'][label] = count

        compare_stage('json-before-cutover', old_cold, old_top, boundary)
        before_objects = object_manifest(objects)
        stopped_at = time.monotonic()
        stop(gateway)
        stop(catalog_process)
        # The one-shot writer has completed and both old read/authority owners
        # are stopped before import. No other process ever writes this directory.
        imported_dir, imported_id = work / 'imported', 'recovery-imported-sqlite'
        command('catalog-import', ['catalog-import', '-config', json_config, '-from', json_dir,
                                 '-path', imported_dir, '-id', imported_id])
        imported = snapshot(imported_dir / 'catalog.db')
        assert len(imported['windows']) == len(manifest['entries']) == 6
        assert all(row['end_ms'] == boundary for row in imported['coverage'])
        assert {(row['data_key'], row['index_key']) for row in imported['packs']} == {
            (row['data_key'], row['index_key']) for row in manifest['entries']}
        assert object_manifest(objects) == before_objects
        service_cfg = dict(cfg, catalog={'type': 'sqlite', 'path': str(imported_dir), 'id': imported_id})
        imported_config = write_config(work, 'imported-config', service_cfg)
        process, rpc, http = start_serve('serve-imported', imported_config)
        cutover_downtime = time.monotonic() - stopped_at
        assert snapshot(imported_dir / 'catalog.db') == imported
        cold, top = free_port(), free_port()
        start_select(procs, 'imported-cold-select', cold, [rpc])
        start_select(procs, 'imported-tiered-select', top, hot.storage_read + [rpc])
        compare_stage('sqlite-after-cutover', cold, top, boundary)
        assert object_manifest(objects) == before_objects
        stop(process)
        backup_at = time.time()
        backup = work / 'backup'
        command('catalog-backup', ['catalog-backup', '-path', imported_dir, '-id', imported_id, '-out', backup])
        assert int(time.time() * 1000) < next_eligible, 'fixture rollover occurred before the initial backup; increase --rollover-delay'
        report['cutover'] = {'source_id': json_id, 'destination_id': imported_id,
                             'json_generation': manifest['generation'], 'sqlite_generation': imported['state']['generation'],
                             'coverage': imported['coverage'], 'unchanged_objects': before_objects,
                             'downtime_seconds': cutover_downtime,
                             'old_writer_completed_and_authority_stopped': True}
        report['checks'].append('JSON v2 writer/authority stopped; import retains coverage and every object byte/stat; Cold and boundary results match stock reference before/after cutover')

        process, _, http = start_serve('serve-original-advancing', imported_config)
        # Poll real time in short increments; the first eligible suffix is one
        # extra hour per tenant, while the archived contract remains unchanged.
        while int(time.time() * 1000) <= next_eligible:
            assert process.poll() is None
            time.sleep(.1)
        wait_ready(process, http, work)
        advanced = snapshot(imported_dir / 'catalog.db')
        assert advanced['state']['generation'] == 8
        assert all(row['end_ms'] == boundary + hour for row in advanced['coverage'])
        stop(process)
        old_owner_stopped = time.monotonic()
        restored_dir, restored_id = work / 'restored', 'recovery-restored-sqlite'
        command('catalog-restore', ['catalog-restore', '-backup', backup, '-path', restored_dir, '-id', restored_id])
        restored = snapshot(restored_dir / 'catalog.db')
        assert restored['state']['catalog_id'] == restored_id != imported_id
        assert restored['coverage'] == imported['coverage'] and restored['windows'] == imported['windows']
        assert restored['packs'] == imported['packs']
        restored_config = write_config(work, 'restored-config', dict(service_cfg,
            catalog={'type': 'sqlite', 'path': str(restored_dir), 'id': restored_id}))
        recovery_started = time.monotonic()
        source.pause_raw = True
        before_crash_objects = object_manifest(objects)
        process, rpc, http = start_serve('serve-restored-precommit', restored_config, ready=False)
        assert source.started.wait(15), 'raw verification did not pause'
        staged = object_manifest(objects)
        orphan_keys = sorted(set(staged) - set(before_crash_objects))
        assert len(orphan_keys) >= 2, 'no pack and index staged before raw verification'
        assert snapshot(restored_dir / 'catalog.db') == restored
        missing_ready_error = readiness_failure(http)
        missing_select = free_port()
        start_select(procs, 'missing-suffix-select', missing_select, [rpc])
        missing_suffix_error = coverage_failure(lambda: api(missing_select, tenants[0], 'query',
            {'query': 'temperature', 'time': (boundary + 600000) / 1000}))
        process.kill()
        process.wait(timeout=10)
        assert process.returncode == -signal.SIGKILL
        assert snapshot(restored_dir / 'catalog.db') == restored
        source.release.set()
        report['precommit_crash'] = {'signal': 'SIGKILL', 'exit_code': process.returncode,
            'phase': 'native packs uploaded/read back; independent raw verification response blocked',
            'generation_before': restored['state']['generation'], 'generation_after': snapshot(restored_dir / 'catalog.db')['state']['generation'],
            'unpublished_object_keys': orphan_keys, 'missing_suffix_error': missing_suffix_error,
            'readiness_error': missing_ready_error}
        process, rpc, http = start_serve('serve-restored-catchup', restored_config)
        recovered = snapshot(restored_dir / 'catalog.db')
        assert recovered['coverage'] == advanced['coverage']
        assert recovered['state']['generation'] == 8
        published = {row[field] for row in recovered['packs'] for field in ('data_key', 'index_key')}
        assert not published.intersection(orphan_keys)
        assert set(object_manifest(objects)).issuperset(orphan_keys), 'fixture must preserve ignored orphans'
        report['recovery'] = {'backup_age_seconds': time.time() - backup_at,
            'backup_generation': imported['state']['generation'], 'original_advanced_generation': advanced['state']['generation'],
            'restored_generation': recovered['state']['generation'], 'old_id': imported_id, 'new_id': restored_id,
            'catchup_seconds_including_crash': time.monotonic() - recovery_started,
            'downtime_seconds': time.monotonic() - old_owner_stopped,
            'missing_stream_windows': 2, 'missing_interval_ms_per_tenant': hour,
            'configured_retention_headroom_seconds_at_restart': (boundary + 240 * hour - int(time.time() * 1000)) / 1000,
            'old_owner_stopped_before_restore': True, 'coverage': recovered['coverage']}
        cold, top = free_port(), free_port()
        start_select(procs, 'restored-cold-select', cold, [rpc])
        start_select(procs, 'restored-tiered-select', top, hot.storage_read + [rpc])
        compare_stage('restored-after-catchup', cold, top, boundary + hour)
        requests_before = len(source.requests)
        process.kill()
        process.wait(timeout=10)
        assert process.returncode == -signal.SIGKILL
        process, rpc, http = start_serve('serve-committed-restart', restored_config)
        assert snapshot(restored_dir / 'catalog.db') == recovered
        assert len(source.requests) == requests_before
        cold, top = free_port(), free_port()
        start_select(procs, 'committed-cold-select', cold, [rpc])
        start_select(procs, 'committed-tiered-select', top, hot.storage_read + [rpc])
        compare_stage('committed-after-sigkill', cold, top, boundary + hour)
        assert len(source.requests) == requests_before
        stop(process)
        report['committed_crash'] = {'signal': 'SIGKILL', 'generation_after_restart': recovered['state']['generation'],
                                     'source_requests_after_restart': len(source.requests) - requests_before,
                                     'catalog_snapshot_unchanged': True}
        report['checks'].append('older offline backup restores under a new ID; readiness and missing-suffix queries fail until exact Hot-retained catch-up')
        report['checks'].append('SIGKILL after staging leaves orphan objects unpublished; retry ignores them; committed windows survive SIGKILL and are not re-exported')

        # Short-window negative fixture: real elapsed policy time, followed by
        # an explicitly reconstructed empty Hot. Stock VM cannot expire history
        # naturally on this timescale (its minimum retention is 24h).
        expired_start = int(time.time()) * 1000 - 5000
        expired_tenant = '55:8'
        remote_write(hot.insert, expired_tenant, [({'__name__': 'recovery_probe'}, [(expired_start + 100, 7.0)])])
        hot.flush()
        expired_json = work / 'expired-json'
        expired_catalog_port = free_port()
        expired_url = f'http://127.0.0.1:{expired_catalog_port}'
        command('expired-json-init', ['catalog-init', '-path', expired_json, '-id', 'expired-json'])
        expired_authority = procs.start('expired-authority', [binary, 'catalog', '-path', expired_json,
            '-id', 'expired-json', '-http', f'127.0.0.1:{expired_catalog_port}'], expired_url + '/v1/catalog')
        expired_cfg = dict(cfg, tenants=[expired_tenant], catalog={'type': 'http', 'url': expired_url, 'id': 'expired-json'},
            archive_age='2s', hot_retention='20s', lateness='0s', safety_margin='1s', window='1s', coverage_start_ms=expired_start)
        expired_config = write_config(work, 'expired-json-config', expired_cfg)
        command('expired-archive', ['archive', '-config', expired_config, '-start', str(expired_start), '-end', str(expired_start + 1000)])
        stop(expired_authority)
        expired_original = work / 'expired-original'
        command('expired-import', ['catalog-import', '-config', expired_config, '-from', expired_json,
                                  '-path', expired_original, '-id', 'expired-original'])
        expired_backup = work / 'expired-backup'
        expired_backup_at = time.time()
        command('expired-backup', ['catalog-backup', '-path', expired_original, '-id', 'expired-original', '-out', expired_backup])
        hot.stop()
        hot.start('empty-expiry-fixture')
        empty = request(f'http://127.0.0.1:{hot.select}/select/{expired_tenant}/prometheus/api/v1/export?' + urllib.parse.urlencode(
            {'match[]': '{__name__!=""}', 'start': expired_start / 1000, 'end': time.time()})).strip()
        assert not empty
        expiry_deadline = expired_start + 1000 + 20000 - 1000
        while int(time.time() * 1000) <= expiry_deadline:
            time.sleep(.1)
        expired_restored = work / 'expired-restored'
        command('expired-restore', ['catalog-restore', '-backup', expired_backup, '-path', expired_restored, '-id', 'expired-restored'])
        expired_service_config = write_config(work, 'expired-restored-config', dict(expired_cfg,
            catalog={'type': 'sqlite', 'path': str(expired_restored), 'id': 'expired-restored'}))
        before_expired = snapshot(expired_restored / 'catalog.db')
        source_requests = len(source.requests)
        process, rpc, http = start_serve('serve-expired-restored', expired_service_config, ready=False)
        deadline = time.monotonic() + 10
        marker = 'archive start is expired or inside the Hot retention safety margin'
        while marker not in (work / 'serve-expired-restored.log').read_text():
            assert process.poll() is None and time.monotonic() < deadline
            time.sleep(.1)
        expired_ready_error = readiness_failure(http)
        expired_select = free_port()
        start_select(procs, 'expired-cold-select', expired_select, [rpc])
        # Raw export preserves the requested millisecond interval. Recent
        # instant queries can be shifted by stock vmselect's latency offset.
        expired_suffix_error = coverage_failure(lambda: request(f'http://127.0.0.1:{expired_select}/select/{expired_tenant}/prometheus/api/v1/export?' +
            urllib.parse.urlencode({'match[]': 'recovery_probe', 'start': (expired_start + 1000) / 1000,
                                   'end': (expired_start + 2000) / 1000})), expected_status=400)
        assert snapshot(expired_restored / 'catalog.db') == before_expired
        assert len(source.requests) == source_requests
        stop(process)
        report['expired_backup'] = {'old_id': 'expired-original', 'new_id': 'expired-restored',
            'backup_age_seconds': time.time() - expired_backup_at,
            'coverage_end_ms': expired_start + 1000, 'retention_ms': 20000, 'safety_ms': 1000,
            'retention_safety_deadline_ms': expiry_deadline, 'observed_at_ms': int(time.time() * 1000),
            'headroom_after_safety_seconds': (expiry_deadline - int(time.time() * 1000)) / 1000,
            'generation_before': before_expired['state']['generation'],
            'generation_after': snapshot(expired_restored / 'catalog.db')['state']['generation'],
            'source_requests': len(source.requests) - source_requests, 'empty_hot_raw_export_bytes': len(empty),
            'refusal': marker, 'missing_suffix_error': expired_suffix_error, 'readiness_error': expired_ready_error,
            'source_absence_method': 'explicit empty Hot reconstruction; not natural retention expiry'}
        report['checks'].append('expired restored checkpoint refuses before source requests, stays unready, fails missing-suffix query and preserves coverage')
        report['status'] = 'passed'
    except Exception as error:
        report['status'], report['error'] = 'failed', repr(error)
        raise
    finally:
        procs.close()
        if source:
            source.close()
        report['finished_at_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (work / 'report.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workdir', type=Path, required=True)
    parser.add_argument('--gomaxprocs', type=positive_int)
    parser.add_argument('--replicas', type=int, choices=[1, 2], default=1)
    parser.add_argument('--rollover-delay', type=positive_int, default=60,
                        help='seconds until a real wall-clock archive eligibility transition')
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=False)
    run(args.workdir, args)
