#!/usr/bin/env python3
"""Measure a disposable stock-VM/SQLite workload; never contacts a real provider.

The default target has ten tenants and 10,000 active series per tenant for two
complete hours. Instance 00000 additionally has 24 hours of history. This does
NOT qualify 24 hours of the full cohort; use --dense-hours 24 for that workload.
Every successful query is compared exactly with a separately ingested reference.
Raw measurements, including a failed run, are preserved in the new --workdir.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import http.server
import json
import os
from pathlib import Path
import platform
import statistics
import threading
import time
import urllib.error
import urllib.parse

from e2e import (ROOT, HTTP, Cluster, Processes, api, compare, free_port, metrics,
                 positive_int, remote_write, request, start_select, timed)
from e2e_serve import provenance, snapshot, wait_ready, wait_query_reconnect

HOUR = 3600000
INTERVAL = 30000
IO_KEYS = ('vmtier_object_get_requests_total', 'vmtier_object_range_requests_total',
           'vmtier_object_download_bytes_total')


def workload_horizon(clock_ms, history_hours, dense_hours):
    boundary = clock_ms // HOUR * HOUR - 3 * HOUR
    return {'start_ms': boundary - history_hours * HOUR,
            'dense_start_ms': boundary - dense_hours * HOUR,
            'boundary_ms': boundary, 'created_at_ms': clock_ms,
            'archive_age_ms': clock_ms - boundary - HOUR // 2,
            'next_eligible_at_ms': clock_ms + HOUR // 2}


def batches(tenant_index, series_count, horizon, batch_size):
    """Bound Python ingestion memory; the first series carries narrow history."""
    for offset in range(0, series_count, batch_size):
        batch = []
        for index in range(offset, min(offset + batch_size, series_count)):
            start = horizon['start_ms'] if index == 0 else horizon['dense_start_ms']
            labels = {'__name__': 'qualify_load', 'instance': f'{index:05d}',
                      'group': str(index % 10), 'job': 'capacity'}
            samples = [(stamp, float(tenant_index * 1000000 + index * 100 +
                                     ((stamp - horizon['start_ms']) // INTERVAL) % 97))
                       for stamp in range(start, horizon['boundary_ms'], INTERVAL)]
            batch.append((labels, samples))
        yield batch


def process_memory(pid):
    result = {}
    try:
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            key, _, value = line.partition(':')
            if key in ('VmRSS', 'VmHWM'):
                result[key + '_bytes'] = int(value.split()[0]) * 1024
    except (FileNotFoundError, ProcessLookupError):
        pass
    return result


def hardware():
    result = {'kernel': platform.release(), 'machine': platform.machine(),
              'python': platform.python_version(), 'cpu_affinity_count': len(os.sched_getaffinity(0))}
    for name in ('cpu.max', 'memory.max', 'memory.swap.max'):
        path = Path('/sys/fs/cgroup') / name
        if path.exists():
            result['cgroup_' + name] = path.read_text().strip()
    for line in Path('/proc/cpuinfo').read_text().splitlines():
        if line.startswith('model name'):
            result['cpu_model'] = line.partition(':')[2].strip()
            break
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemTotal:'):
            result['host_mem_total_bytes'] = int(line.split()[1]) * 1024
    return result


class Sampler:
    """Synchronized sampled RSS sums, separate from non-additive kernel HWM."""
    def __init__(self, processes, work):
        self.processes, self.work = processes, work
        self.phase, self.http_port = 'setup', None
        self.stop = threading.Event()
        self.samples, self.per_process_peaks, self.metric_peaks = [], {}, {}
        self.minimum_retention_headroom_seconds = None
        self.began = time.monotonic()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        while not self.stop.is_set():
            sample = {'seconds': time.monotonic() - self.began, 'phase': self.phase,
                      'processes': {}, 'harness': process_memory(os.getpid())}
            for name, process, _ in list(self.processes.items):
                if process.poll() is None:
                    current = process_memory(process.pid)
                    sample['processes'][name] = current
                    peak = self.per_process_peaks.setdefault(name, {})
                    for key, value in current.items():
                        peak[key] = max(peak.get(key, 0), value)
            sample['combined_children_rss_bytes'] = sum(
                p.get('VmRSS_bytes', 0) for p in sample['processes'].values())
            sample['vmtier_rss_bytes'] = sum(
                p.get('VmRSS_bytes', 0) for name, p in sample['processes'].items()
                if name.startswith('serve-'))
            current = Path('/sys/fs/cgroup/memory.current')
            if current.exists():
                sample['cgroup_memory_current_bytes'] = int(current.read_text())
            # HTTP sampling adds observation overhead and is bounded separately
            # from the nominal 100 ms /proc sampling interval.
            if self.http_port and len(self.samples) % 3 == 0:
                try:
                    sample['metrics'] = {}
                    for line in request(f'http://127.0.0.1:{self.http_port}/metrics', timeout=1).decode().splitlines():
                        if line and not line.startswith('#'):
                            key, value = line.rsplit(None, 1)
                            try:
                                sample['metrics'][key] = float(value)
                            except ValueError:
                                pass
                    for key, value in sample['metrics'].items():
                        self.metric_peaks[key] = max(self.metric_peaks.get(key, value), value)
                        if key.startswith('vmtier_stream_retention_headroom_seconds{'):
                            old = self.minimum_retention_headroom_seconds
                            self.minimum_retention_headroom_seconds = value if old is None else min(old, value)
                except (OSError, urllib.error.URLError):
                    pass
            self.samples.append(sample)
            self.stop.wait(.1)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=35)
        if self.thread.is_alive():
            raise RuntimeError('memory sampler did not drain')
        path = self.work / 'memory-samples.json'
        path.write_text(json.dumps(self.samples, separators=(',', ':')) + '\n')
        phases = {}
        for sample in self.samples:
            phase = phases.setdefault(sample['phase'], {'samples': 0})
            phase['samples'] += 1
            for key in ('combined_children_rss_bytes', 'vmtier_rss_bytes',
                        'cgroup_memory_current_bytes'):
                phase[key + '_peak'] = max(phase.get(key + '_peak', 0), sample.get(key, 0))
        intervals = [b['seconds'] - a['seconds'] for a, b in zip(self.samples, self.samples[1:])]
        return {'nominal_proc_interval_seconds': .1, 'metrics_every_samples': 3,
                'metrics_timeout_seconds': 1,
                'observed_proc_interval_seconds': {
                    'median': statistics.median(intervals) if intervals else None,
                    'max': max(intervals) if intervals else None},
                'sample_count': len(self.samples), 'phases': phases,
                'per_process_peaks': self.per_process_peaks, 'metric_peaks': self.metric_peaks,
                'minimum_observed_retention_headroom_seconds': self.minimum_retention_headroom_seconds,
                'raw_samples_file': path.name,
                'raw_samples_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                'scope': 'RSS includes shared pages in each process; synchronized sums are not unique physical memory. VmHWM values are individual, never summed. cgroup includes other session work. Python proxy/ingestion harness RSS is separate. Sampling can miss shorter peaks.'}


class StreamingSource:
    """Loopback fault gate; streams Hot responses in bounded 64 KiB chunks."""
    def __init__(self, port):
        self.unavailable = True
        self.failures = 0
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if owner.unavailable:
                    owner.failures += 1
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(b'qualification source outage')
                    return
                try:
                    with HTTP.open(f'http://127.0.0.1:{port}' + self.path, timeout=120) as upstream:
                        self.send_response(upstream.status)
                        self.send_header('Content-Type', upstream.headers.get('Content-Type', ''))
                        self.end_headers()
                        while True:
                            part = upstream.read(65536)
                            if not part:
                                break
                            self.wfile.write(part)
                except urllib.error.HTTPError as error:
                    self.send_response(error.code)
                    self.end_headers()
                    self.wfile.write(error.read())
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *_):
                pass

        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def delta(before, after):
    return {key: after.get(key, 0) - before.get(key, 0) for key in IO_KEYS}


def distributions(records):
    result = {}
    for record in records:
        if 'latency_ms' in record:
            result.setdefault(record['kind'], []).append(record['latency_ms'])
    return {key: {'count': len(values), 'min': min(values),
                  'median': statistics.median(values),
                  'p95': sorted(values)[min(len(values) - 1, int(.95 * len(values)))],
                  'max': max(values)} for key, values in result.items()}


def run(work, args):
    work = work.resolve()
    procs = Processes(work, args.gomaxprocs)
    sampler = Sampler(procs, work)
    source = None
    report = {'started_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'hardware': hardware(), 'runtime': procs.runtime, 'backend': 'local',
              'vm_revision': '9a009c491da4c17531893ab4acb6a5f1b5e0eaf2',
              'comparison': 'exact timestamps, labels and numeric strings; no tolerance',
              'measurements': [], 'checks': [], 'scope':
              'Disposable loopback local filesystem. No real provider, natural retention, external ingress enforcement, or universal latency SLO.'}
    report_path = work / 'results.json'
    background_stop = threading.Event()
    background_thread = None
    background_errors = []
    try:
        report['provenance'] = provenance()
        report['provenance']['qualification_harness_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        ref, hot = Cluster(procs, 'reference', args.replicas), Cluster(procs, 'hot', args.replicas)
        ref.start('full')
        hot.start('full')
        horizon = workload_horizon(int(time.time() * 1000), args.history_hours, args.dense_hours)
        tenants = [f'{index + 1}:1' for index in range(args.tenants)]
        report['fixture'] = {**horizon, 'tenants': tenants,
                             'series_per_tenant': args.series, 'replicas': args.replicas,
                             'sample_interval_ms': INTERVAL, 'history_hours': args.history_hours,
                             'dense_hours': args.dense_hours,
                             'sparse_history_series_per_tenant': 1 if args.dense_hours < args.history_hours else 0,
                             'window_ms': HOUR, 'ingestion_batch_series': args.batch_series}
        sampler.phase = 'ingestion'
        began = time.monotonic()
        samples = 0
        for tenant_index, tenant in enumerate(tenants):
            for batch in batches(tenant_index, args.series, horizon, args.batch_series):
                samples += remote_write(ref.insert, tenant, batch)
                remote_write(hot.insert, tenant, batch)
            print(f'Ingested tenant {tenant}: {args.series} series', flush=True)
        ref.flush()
        hot.flush()
        report['fixture']['logical_samples'] = samples
        report['ingestion_seconds'] = time.monotonic() - began
        report['checks'].append('Identical bounded-batch logical input ingested into separate reference and Hot clusters')
        source = StreamingSource(hot.select)
        catalog_dir = work / 'catalog'
        database = catalog_dir / 'catalog.db'
        config_path = work / 'config.json'
        cfg = {'store': {'type': 'local', 'path': str(work / 'objects')},
               'catalog': {'type': 'sqlite', 'path': str(catalog_dir), 'id': 'workload-qualification'},
               'tenants': tenants, 'sources': [{'id': 'hot', 'url': source.url}],
               'archive_age': f"{horizon['archive_age_ms']}ms", 'hot_retention': '240h',
               'lateness': '30m', 'safety_margin': '30m', 'window': '1h',
               'lateness_enforced': True, 'coverage_start_ms': horizon['start_ms'],
               'service': {'archive_interval': '100ms', 'archive_timeout': '2m'},
               'cache_bytes': args.cache_mib << 20, 'max_query_bytes': args.query_mib << 20,
               'query_memory_bytes': args.shared_query_mib << 20}
        config_path.write_text(json.dumps(cfg, indent=2) + '\n')
        report['config'] = cfg
        report['effective_limits'] = {'rpc_concurrency': 8, 'rpc_duration_seconds': 30,
                                      'query_work_bytes': args.query_mib << 20,
                                      'query_memory_bytes': args.shared_query_mib << 20,
                                      'raw_cache_bytes': args.cache_mib << 20,
                                      'shared_read_flights': 16,
                                      'stock_process_memory_allowed_bytes': 128 << 20,
                                      'stock_result_cache': 'disabled', 'dedup_interval': '1ms'}
        binary = ROOT / '.bin/vmtier'
        procs.command_stats([binary, 'catalog-init', '-backend', 'sqlite', '-config', config_path,
                             '-path', catalog_dir, '-id', cfg['catalog']['id']], work / 'init.log')
        rpc_port, http_port, cold_port = free_port(), free_port(), free_port()

        def start_service(name):
            process = procs.start(name, [binary, 'serve', '-config', config_path,
                                         '-listen', f'127.0.0.1:{rpc_port}',
                                         '-http', f'127.0.0.1:{http_port}'],
                                  f'http://127.0.0.1:{http_port}/health')
            sampler.http_port = http_port
            return process

        process = start_service('serve-workload')
        start_select(procs, 'cold-select', cold_port, [rpc_port], '30s')
        sampler.phase = 'source-outage'
        deadline = time.monotonic() + 10
        while not source.failures and time.monotonic() < deadline:
            time.sleep(.05)
        assert source.failures, 'archive did not attempt the unavailable source'
        outage_started = time.monotonic()
        time.sleep(args.outage_seconds)
        before_outage = snapshot(database)
        assert before_outage['state']['generation'] == 0
        report['source_outage'] = {'seconds': time.monotonic() - outage_started,
                                   'failed_http_requests': source.failures,
                                   'generation_before_recovery': 0}

        dense = horizon['dense_start_ms']
        first_end = dense + HOUR
        broad = {'query': 'sum(qualify_load)', 'start': (dense + 300000) / 1000,
                 'end': (first_end - INTERVAL) / 1000, 'step': '60s', 'timeout': '30s'}
        narrow = dict(broad, query='qualify_load{instance="00000"}')
        expected_broad = api(ref.select, tenants[0], 'query_range', broad)
        expected_narrow = api(ref.select, tenants[0], 'query_range', narrow)
        assert expected_broad['result'] and expected_narrow['result']
        report['concurrent_archive_queries'] = {'successes': 0, 'admission_refusals': 0,
                                                 'started_generation': None,
                                                 'finished_generation': None,
                                                 'latencies_ms': []}

        def background_queries():
            try:
                while not background_stop.is_set():
                    state = snapshot(database)
                    coverage = next(row['end_ms'] for row in state['coverage'] if row['tenant'] == tenants[0])
                    if coverage >= first_end:
                        break
                    background_stop.wait(.01)
                if background_stop.is_set():
                    return
                observation = report['concurrent_archive_queries']
                observation['started_generation'] = state['state']['generation']
                while not background_stop.is_set():
                    for params, expected in ((broad, expected_broad), (narrow, expected_narrow)):
                        try:
                            result, elapsed = timed(lambda: api(cold_port, tenants[0], 'query_range', params))
                            compare(expected, result, 'concurrent archive query')
                            observation['successes'] += 1
                            observation['latencies_ms'].append(elapsed)
                        except urllib.error.HTTPError as error:
                            body = error.read().decode()
                            if 'Cold query memory admission limit exceeded' not in body:
                                raise AssertionError(body) from error
                            observation['admission_refusals'] += 1
                    background_stop.wait(.02)
                observation['finished_generation'] = snapshot(database)['state']['generation']
            except Exception as error:
                background_errors.append(str(error))

        sampler.phase = 'archive-catchup-with-queries'
        background_thread = threading.Thread(target=background_queries, daemon=True)
        background_thread.start()
        began = time.monotonic()
        source.unavailable = False
        wait_ready(process, http_port, work, timeout=args.archive_timeout_seconds)
        catchup_seconds = time.monotonic() - began
        background_stop.set()
        background_thread.join(timeout=35)
        assert not background_thread.is_alive(), 'background query did not drain'
        assert not background_errors, background_errors
        stable = snapshot(database)
        assert len(stable['windows']) == args.tenants * args.history_hours
        assert all(row['end_ms'] == horizon['boundary_ms'] for row in stable['coverage'])
        report['archive'] = {'catchup_seconds': catchup_seconds,
                             'logical_samples_per_second': samples / catchup_seconds,
                             'dense_payload_hours_per_wall_hour': args.dense_hours * 3600 / catchup_seconds,
                             'logical_windows_per_second': len(stable['windows']) / catchup_seconds,
                             'windows': len(stable['windows']), 'packs': len(stable['packs']),
                             'generation': stable['state']['generation'], 'coverage': stable['coverage'],
                             'metrics': metrics(http_port)}
        assert report['archive']['metrics']['vmtier_archive_samples_total'] == samples, (
            'Committed sample count differs from generated logical input', report['archive'], samples)
        physical_rows = 0
        object_sizes = {'data_bytes': 0, 'index_bytes': 0}
        for pack in stable['packs']:
            path = Path(cfg['store']['path']) / pack['index_key']
            index_bytes = path.read_bytes()
            index = json.loads(index_bytes)
            physical_rows += sum(block['rows'] for series in index['series'] for block in series['blocks'])
            object_sizes['index_bytes'] += len(index_bytes)
            object_sizes['data_bytes'] += (Path(cfg['store']['path']) / pack['data_key']).stat().st_size
        report['archive']['physical_rows_in_committed_indexes'] = physical_rows
        report['archive']['object_sizes'] = object_sizes
        report['archive']['initial_retention_safety_headroom_seconds'] = (
            horizon['start_ms'] + 240 * HOUR - 30 * 60000 - horizon['created_at_ms']) / 1000
        concurrency = report['concurrent_archive_queries']
        concurrency['commits_after_queries_started'] = (
            concurrency['finished_generation'] - concurrency['started_generation']
            if concurrency['started_generation'] is not None and concurrency['finished_generation'] is not None else 0)
        assert concurrency['successes'] > 0 and concurrency['commits_after_queries_started'] > 0, concurrency
        report['checks'].append('Source outage publishes nothing; subsequent archive catch-up makes commits while exact Cold queries run')

        # Restart clears raw cache without changing the authority or stored packs.
        sampler.phase = 'cache-cold-warm-pressure'
        procs.stop(process)
        assert process.returncode == 0
        process = start_service('serve-cold-cache')
        wait_ready(process, http_port, work)
        wait_query_reconnect(lambda: api(cold_port, tenants[0], 'query',
                                        {'query': 'qualify_load{instance="missing"}', 'time': (horizon['boundary_ms'] + HOUR) / 1000}))

        def measure(kind, tenant, path, params, expected=None, expected_count=None):
            if expected is None:
                expected = api(ref.select, tenant, path, params)
            before = metrics(http_port)
            actual, elapsed = timed(lambda: api(cold_port, tenant, path, params))
            after = metrics(http_port)
            compare(expected, actual, kind + ' ' + tenant)
            if expected_count is not None:
                assert len(actual['result']) == 1 and float(actual['result'][0]['value'][1]) == expected_count, (
                    f'{tenant}: expected {expected_count} samples in the complete dense hour', actual)
            if path == 'series':
                assert len(actual) == args.series, f'{tenant}: expected {args.series} active series, got {len(actual)}'
            record = {'kind': kind, 'tenant': tenant, 'path': path, 'params': params,
                      'latency_ms': elapsed, 'io_delta': delta(before, after), 'exact': True}
            if isinstance(actual, list):
                record['result_items'] = len(actual)
            if expected_count is not None:
                record['independent_expected_sample_count'] = expected_count
            report['measurements'].append(record)
            return record

        for tenant in tenants:
            measure('narrow_1h_cold', tenant, 'query_range', narrow)
            warm = measure('narrow_1h_warm', tenant, 'query_range', narrow)
            assert sum(warm['io_delta'].values()) == 0, warm
            measure('broad_1h', tenant, 'query_range', broad)
            measure('broad_complete_1h_sample_count', tenant, 'query',
                    {'query': 'sum(count_over_time(qualify_load[1h]))',
                     'time': (first_end - 1) / 1000, 'timeout': '30s'},
                    expected_count=args.series * (HOUR // INTERVAL))
            whole = dict(narrow, start=horizon['start_ms'] / 1000,
                         end=(horizon['boundary_ms'] - INTERVAL) / 1000, step='5m')
            measure('narrow_24h' if args.history_hours == 24 else 'narrow_history', tenant, 'query_range', whole)
            meta = {'match[]': 'qualify_load', 'start': dense / 1000,
                    'end': (first_end - 1) / 1000}
            measure('metadata_label_names', tenant, 'labels', meta)
            measure('metadata_label_values', tenant, 'label/instance/values', meta)
            measure('metadata_series', tenant, 'series', meta)
        revisit = measure('cache_pressure_revisit', tenants[0], 'query_range', broad)
        report['cache_pressure'] = {'limit_bytes': cfg['cache_bytes'],
                                    'revisit_io_delta': revisit['io_delta'],
                                    'metrics': metrics(http_port),
                                    'interpretation': 'Positive revisit GETs show entries displaced by later tenants; absent GETs mean this fixture did not pressure the configured cache.'}
        report['cache_pressure']['exercised'] = (
            report['cache_pressure']['metrics'].get('vmtier_cache_evictions_total', 0) > 0 and
            sum(revisit['io_delta'].values()) > 0)
        if args.require_cache_pressure:
            assert report['cache_pressure']['exercised'], 'fixture did not exercise cache pressure; lower --cache-mib or increase workload'

        sampler.phase = 'concurrent-query-admission'
        barrier = threading.Barrier(args.burst_queries)
        burst_before = metrics(http_port)

        def burst_query(index):
            barrier.wait(timeout=30)
            began = time.monotonic()
            try:
                actual = api(cold_port, tenants[0], 'query_range', broad)
                compare(expected_broad, actual, f'burst query {index}')
                return {'outcome': 'exact_success', 'milliseconds': (time.monotonic() - began) * 1000}
            except urllib.error.HTTPError as error:
                body = error.read().decode()
                if 'Cold query memory admission limit exceeded' not in body:
                    raise AssertionError('Unexpected burst refusal: ' + body) from error
                return {'outcome': 'memory_admission_refusal', 'http_status': error.code,
                        'milliseconds': (time.monotonic() - began) * 1000, 'message': body[:2000]}

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.burst_queries) as executor:
            burst = list(executor.map(burst_query, range(args.burst_queries)))
        report['admission_burst'] = {'requests': burst, 'before': burst_before,
                                     'after': metrics(http_port)}
        assert any(item['outcome'] == 'exact_success' for item in burst)
        assert any(item['outcome'] == 'memory_admission_refusal' for item in burst), 'burst did not exercise shared admission; increase workload or concurrency'
        admission_key = 'vmtier_query_limit_refusals_total{reason="memory_admission"}'
        assert report['admission_burst']['after'].get(admission_key, 0) > burst_before.get(admission_key, 0)
        assert metrics(http_port)['vmtier_query_memory_reserved_bytes'] == 0
        report['checks'].append('Concurrent requests either match the reference or explicitly refuse memory admission; all reservations drain')

        # A deliberately tiny allowance proves the whole request is refused,
        # independent of scheduling luck or frontend aggregation size.
        sampler.phase = 'explicit-read-budget-refusal'
        procs.stop(process)
        assert process.returncode == 0
        limited = dict(cfg, max_query_bytes=1024, query_memory_bytes=2048, cache_bytes=0)
        config_path.write_text(json.dumps(limited, indent=2) + '\n')
        process = start_service('serve-explicit-refusal')
        wait_ready(process, http_port, work)
        began = time.monotonic()
        while True:
            try:
                api(cold_port, tenants[0], 'query_range', broad)
            except urllib.error.HTTPError as error:
                body = error.read().decode()
                if 'connection refused' in body and time.monotonic() - began < 5:
                    time.sleep(.1)
                    continue
                assert 'limit exceeded' in body and 'query' in body.lower(), body
                report['explicit_refusal'] = {'max_query_bytes': 1024, 'http_status': error.code,
                                               'message': body, 'metrics': metrics(http_port)}
                break
            else:
                raise AssertionError('1 KiB read budget unexpectedly accepted the broad query')
        assert metrics(http_port)['vmtier_query_memory_reserved_bytes'] == 0
        assert snapshot(database) == stable
        report['checks'].append('1 KiB query allowance refuses the broad query without partial success or catalog change and releases its reservation')
        procs.stop(process)
        config_path.write_text(json.dumps(cfg, indent=2) + '\n')
        process = start_service('serve-recovered')
        wait_ready(process, http_port, work)
        actual, _, _ = wait_query_reconnect(lambda: api(cold_port, tenants[0], 'query_range', broad))
        compare(expected_broad, actual, 'budget restored exact recovery')
        report['final_metrics'] = metrics(http_port)
        report['latency_distributions_ms'] = distributions(report['measurements'])
        report['exact_comparisons'] = (len(report['measurements']) + concurrency['successes'] +
                                        sum(item['outcome'] == 'exact_success' for item in burst) + 1)
        assert int(time.time() * 1000) < horizon['next_eligible_at_ms'], 'run exceeded the 30-minute fixed archive horizon'
        report['status'] = 'passed'
    except Exception as error:
        report['status'], report['failure'] = 'failed', str(error)
        raise
    finally:
        background_stop.set()
        if background_thread:
            background_thread.join(timeout=35)
        sampler.http_port = None
        report['memory'] = sampler.close()
        procs.close()
        if source:
            source.close()
        report['finished_at_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
        print(json.dumps({'status': report.get('status'), 'failure': report.get('failure'),
                          'results': str(report_path), 'archive': report.get('archive', {}).get('catchup_seconds'),
                          'exact_comparisons': report.get('exact_comparisons')}, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workdir', type=Path, required=True)
    parser.add_argument('--tenants', type=positive_int, default=10)
    parser.add_argument('--series', type=positive_int, default=10000)
    parser.add_argument('--dense-hours', type=positive_int, default=2)
    parser.add_argument('--history-hours', type=positive_int, default=24)
    parser.add_argument('--replicas', choices=[1, 2], type=int, default=2)
    parser.add_argument('--batch-series', type=positive_int, default=100)
    parser.add_argument('--cache-mib', type=positive_int, default=16)
    parser.add_argument('--require-cache-pressure', action='store_true',
                        help='require measured evictions and blob I/O on the first tenant revisit')
    parser.add_argument('--query-mib', type=positive_int, default=128)
    parser.add_argument('--shared-query-mib', type=positive_int, default=256)
    parser.add_argument('--burst-queries', type=positive_int, default=6)
    parser.add_argument('--outage-seconds', type=positive_int, default=2)
    parser.add_argument('--archive-timeout-seconds', type=positive_int, default=1200)
    parser.add_argument('--gomaxprocs', type=positive_int)
    args = parser.parse_args()
    if not 2 <= args.dense_hours <= args.history_hours <= 168:
        parser.error('require 2 <= dense-hours <= history-hours <= 168')
    if args.shared_query_mib < args.query_mib:
        parser.error('shared-query-mib must cover query-mib')
    if args.workdir.exists() and any(args.workdir.iterdir()):
        parser.error('--workdir must be new or empty; existing evidence is preserved')
    args.workdir.mkdir(parents=True, exist_ok=True)
    run(args.workdir, args)


if __name__ == '__main__':
    main()
