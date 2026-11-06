#!/usr/bin/env python3
"""Disposable stock-VM integration/differential test. Python standard library only.

The harness creates its own data directories, launches its own processes, and never
connects to production VictoriaMetrics. Logs and measured results stay in --workdir.
"""
import argparse
import base64
import contextlib
import hashlib
import hmac
import datetime
import http.server
import json
import math
import os
from pathlib import Path
import platform
import resource
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from s3_compat_proxy import S3CompatibilityProxy
from blob_outage import assert_running_blob_failure, unavailable_blobs

ROOT = Path(__file__).resolve().parents[1]
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # loopback tests only
ALLOCATED_PORTS = set()
STALE = struct.unpack('<d', struct.pack('<Q', 0x7ff0000000000002))[0]


def request(url, data=None, headers=None, timeout=30):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    with HTTP.open(req, timeout=timeout) as r:
        return r.read()


def api(port, tenant, path, params=None):
    url = f'http://127.0.0.1:{port}/select/{tenant}/prometheus/api/v1/{path}'
    if params:
        url += '?' + urllib.parse.urlencode(params, doseq=True)
    value = json.loads(request(url))
    if value.get('status') != 'success' or value.get('isPartial'):
        raise AssertionError(f'Unexpected response: {value}')
    return value['data']


def test_s3_request(url, method='GET', data=None):
    # SigV4 with public, test-only credentials; production calls use the AWS Go SDK.
    parsed = urllib.parse.urlsplit(url)
    path = urllib.parse.quote(urllib.parse.unquote(parsed.path), safe='/~')
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    day = stamp[:8]
    digest = hashlib.sha256(data or b'').hexdigest()
    signed = 'host;x-amz-content-sha256;x-amz-date'
    canonical_headers = f'host:{parsed.netloc}\nx-amz-content-sha256:{digest}\nx-amz-date:{stamp}\n'
    canonical = f'{method}\n{path}\n{parsed.query}\n{canonical_headers}\n{signed}\n{digest}'
    scope = f'{day}/us-east-1/s3/aws4_request'
    to_sign = f'AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}'
    key = b'AWS4vmtier-test-secret'
    for value in [day, 'us-east-1', 's3', 'aws4_request']:
        key = hmac.new(key, value.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers = {'Content-Type': 'application/octet-stream', 'Content-MD5': base64.b64encode(hashlib.md5(data or b'').digest()).decode(), 'X-Amz-Date': stamp, 'X-Amz-Content-Sha256': digest,
               'Authorization': f'AWS4-HMAC-SHA256 Credential=vmtier-test/{scope}, SignedHeaders={signed}, Signature={signature}'}
    req = urllib.request.Request(url, method=method, data=data, headers=headers)
    with HTTP.open(req, timeout=30) as response:
        return response.read()


def free_port():
    while True:
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        if port not in ALLOCATED_PORTS:
            ALLOCATED_PORTS.add(port)
            return port


def varint(n):
    out = bytearray()
    while n >= 128:
        out.append((n & 127) | 128)
        n >>= 7
    out.append(n)
    return bytes(out)


def field(n, b):
    return varint(n << 3 | 2) + varint(len(b)) + b


def snappy_literal(raw):
    # Valid raw Snappy stream using literal-only encoding; compressor-independent.
    out = bytearray(varint(len(raw)))
    for i in range(0, len(raw), 65536):
        b = raw[i:i + 65536]
        n = len(b) - 1
        if n < 60:
            out.append(n << 2)
        else:
            width = (n.bit_length() + 7) // 8
            out.append((59 + width) << 2)
            out.extend(n.to_bytes(width, 'little'))
        out.extend(b)
    return bytes(out)


def remote_write(port, tenant, series, start=None):
    ts_messages = []
    count = 0
    for labels, samples in series:
        samples = [(t, v) for t, v in samples if start is None or t >= start]
        if not samples:
            continue
        msg = b''.join(field(1, field(1, k.encode()) + field(2, v.encode()))
                       for k, v in sorted(labels.items()))
        msg += b''.join(field(2, b'\x09' + struct.pack('<d', v) + b'\x10' + varint(t))
                        for t, v in samples)
        ts_messages.append(field(1, msg))
        count += len(samples)
    request(f'http://127.0.0.1:{port}/insert/{tenant}/prometheus/api/v1/write',
            snappy_literal(b''.join(ts_messages)),
            {'Content-Type': 'application/x-protobuf', 'Content-Encoding': 'snappy',
             'X-Prometheus-Remote-Write-Version': '0.1.0'})
    return count


def fixture(start, boundary, end, tenant_index):
    times = list(range(start, end + 1, 30000))
    scale = tenant_index + 1
    series = []
    for instance in ['a', 'b']:
        offset = (instance == 'b') * 17
        labels = {'job': 'demo', 'instance': instance, 'zone': 'east' if instance == 'a' else 'west', 'aaa': 'priority-order', 'z': 'tail'}
        series.append((dict(labels, __name__='requests_total'),
                       [(t, float((i if t < boundary + 60000 else i - 362) * scale + offset))
                        for i, t in enumerate(times)]))
        series.append((dict(labels, __name__='temperature'),
                       [(t, round(math.sin(i / 7) * 12.5 + scale * 20 + offset, 9))
                        for i, t in enumerate(times)]))
        series.append((dict(labels, __name__='factor'), [(t, float(scale)) for t in times]))
    series += [
        ({'__name__': 'irregular', 'job': 'demo'},
         [(t + (i % 3) * 1000, float(i % 11)) for i, t in enumerate(times) if i % 7 not in (2, 3)]),
        ({'__name__': 'cold_only', 'cold_label': 'archived', 'instance': 'a'},
         [(t, float(scale * 101)) for t in times if t < boundary]),
        ({'__name__': 'hot_only', 'hot_label': 'recent'},
         [(t, float(scale * 211)) for t in times if t >= boundary]),
        ({'__name__': 'churn', 'generation': 'old'},
         [(t, 10.0 * scale) for t in times if t < boundary]),
        ({'__name__': 'churn', 'generation': 'new'},
         [(t, 20.0 * scale) for t in times if t >= boundary]),
        ({'__name__': 'empty_label', 'maybe': ''}, [(t, float(scale)) for t in times]),
        ({'__name__': 'special'},
         [(t, math.inf if i % 9 == 0 else -math.inf if i % 9 == 1 else math.nan if i % 9 == 2 else 1.25 * scale)
          for i, t in enumerate(times)]),
        ({'__name__': 'stale_series'},
         [(t, STALE if i in (200, 360) else 3.0 * scale) for i, t in enumerate(times) if not 200 < i < 220]),
        ({'__name__': 'duplicates'},
         [(t, v) for t in times for v in (1.0 * scale, 1.0 * scale, 2.0 * scale)]),
        ({'__name__': 'millisecond_boundary'},
         [(boundary - 1, 11.0 * scale), (boundary, 12.0 * scale), (boundary + 1, 13.0 * scale)]),
    ]
    return series


def fixture_horizon(clock_ms):
    """Keep a fixed complete-window boundary for 30 minutes of real wall time."""
    hour = 3600000
    floor = clock_ms // hour * hour
    boundary = floor - 3 * hour
    return {'start_ms': floor - 6 * hour, 'boundary_ms': boundary,
            'end_ms': floor - hour, 'window_ms': hour,
            'created_at_ms': clock_ms, 'archive_age_ms': clock_ms - boundary - hour // 2,
            'next_eligible_at_ms': clock_ms + hour // 2}


def positive_int(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError('must be a positive integer')
    return value


class Processes:
    def __init__(self, work, gomaxprocs=None):
        self.work, self.items = work, []
        outer = os.environ.get('GOMAXPROCS')
        self.gomaxprocs = str(positive_int(gomaxprocs if gomaxprocs is not None else outer or 2))
        self.runtime = {'outer_GOMAXPROCS': outer, 'selected_GOMAXPROCS': self.gomaxprocs,
                        'selection': 'argument' if gomaxprocs is not None else 'environment' if outer else 'default',
                        'policy': 'CLI --gomaxprocs > outer GOMAXPROCS > 2; applied to all workload children',
                        'processes': []}

    def child_environment(self, env):
        # Test credentials may be supplied separately; runtime selection is explicit.
        return {**os.environ, **(env or {}), 'GOMAXPROCS': self.gomaxprocs}

    def record(self, name, command, kind):
        record = {'name': name, 'kind': kind, 'command': [str(x) for x in command],
                  'environment': {'GOMAXPROCS': self.gomaxprocs}}
        self.runtime['processes'].append(record)
        return record

    def command_stats(self, command, log_path, env=None, expect_success=True):
        record = self.record(log_path.stem, command, 'one-shot')
        return command_stats(command, log_path, self.child_environment(env), expect_success, record)

    def start(self, name, command, ready_url=None, env=None):
        log = open(self.work / f'{name}.log', 'wb')
        record = self.record(name, command, 'managed')
        try:
            p = subprocess.Popen([str(x) for x in command], stdout=log, stderr=subprocess.STDOUT,
                                 env=self.child_environment(env), cwd=ROOT)
        except Exception as error:
            log.close()
            record['launch_error'] = str(error)
            raise
        record['pid'] = p.pid
        self.items.append((name, p, log))
        if ready_url:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if p.poll() is not None:
                    raise RuntimeError(f'{name} exited {p.returncode}: {(self.work / (name + ".log")).read_text()[-6000:]}')
                try:
                    request(ready_url, timeout=1)
                    break
                except (OSError, urllib.error.URLError):
                    time.sleep(.1)
            else:
                raise RuntimeError(f'{name} not ready; see {self.work / (name + ".log")}')
        return p

    def stop(self, p):
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    def close(self):
        for name, p, log in reversed(self.items):
            self.stop(p)
            log.close()
            for record in self.runtime['processes']:
                if record.get('pid') == p.pid:
                    record['exit_code'] = p.returncode


class SourceProxy:
    """Loopback-only failure injector; forwards to one disposable Hot vmselect."""
    def __init__(self, hot_port):
        self.interrupt = False
        self.started, self.release = threading.Event(), threading.Event()
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    with HTTP.open(f'http://127.0.0.1:{hot_port}' + self.path, timeout=30) as r:
                        body, status, content_type = r.read(), r.status, r.headers.get('Content-Type', '')
                    self.send_response(status)
                    self.send_header('Content-Type', content_type)
                    self.end_headers()
                    if owner.interrupt and '/export/native?' in self.path:
                        owner.interrupt = False
                        self.wfile.write(body[:16])
                        self.wfile.flush()
                        owner.started.set()
                        owner.release.wait(15)
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


class Cluster:
    def __init__(self, processes, name, replicas):
        self.processes, self.name, self.replicas = processes, name, replicas
        self.storage_http = [free_port() for _ in range(replicas)]
        self.storage_write = [free_port() for _ in range(replicas)]
        self.storage_read = [free_port() for _ in range(replicas)]
        self.insert, self.select = free_port(), free_port()
        self.procs = []

    def start(self, generation):
        for i in range(self.replicas):
            name = f'{self.name}-storage-{i}-{generation}'
            self.procs.append(self.processes.start(name, [ROOT / '.bin/vmstorage',
                f'-httpListenAddr=127.0.0.1:{self.storage_http[i]}',
                f'-vminsertAddr=127.0.0.1:{self.storage_write[i]}',
                f'-vmselectAddr=127.0.0.1:{self.storage_read[i]}',
                f'-storageDataPath={self.processes.work / name}', '-retentionPeriod=30d',
                '-dedup.minScrapeInterval=1ms', '-inmemoryDataFlushInterval=1s',
                '-memory.allowedBytes=128MiB'], f'http://127.0.0.1:{self.storage_http[i]}/health'))
        self.procs.append(self.processes.start(f'{self.name}-insert-{generation}', [ROOT / '.bin/vminsert',
            f'-httpListenAddr=127.0.0.1:{self.insert}',
            '-storageNode=' + ','.join(f'127.0.0.1:{p}' for p in self.storage_write),
            f'-replicationFactor={self.replicas}', '-memory.allowedBytes=128MiB'],
            f'http://127.0.0.1:{self.insert}/health'))
        self.procs.append(start_select(self.processes, f'{self.name}-select-{generation}', self.select, self.storage_read))

    def flush(self):
        # vminsert's buffered write must arrive before forcing disk visibility.
        time.sleep(1.2)
        for port in self.storage_http:
            request(f'http://127.0.0.1:{port}/internal/force_flush')

    def stop(self):
        for p in reversed(self.procs):
            self.processes.stop(p)
        self.procs.clear()


def start_select(processes, name, port, nodes, labels_api_duration=None):
    extra = [] if labels_api_duration is None else [f'-search.maxLabelsAPIDuration={labels_api_duration}']
    return processes.start(name, [ROOT / '.bin/vmselect',
        f'-httpListenAddr=127.0.0.1:{port}',
        '-storageNode=' + ','.join(f'127.0.0.1:{p}' for p in nodes),
        '-replicationFactor=1', '-globalReplicationFactor=1', '-search.skipSlowReplicas=false', '-search.denyPartialResponse=true',
        '-search.disableCache', '-dedup.minScrapeInterval=1ms', '-memory.allowedBytes=128MiB',
        f'-cacheDataPath={processes.work / name}', *extra], f'http://127.0.0.1:{port}/health')


def normalize(value):
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        v = [normalize(x) for x in value]
        if v and isinstance(v[0], dict):
            v.sort(key=lambda x: json.dumps(x, sort_keys=True))
        return v
    return value


def compare(expected, actual, context):
    # Identical codec and stock evaluator: no float tolerance is justified.
    expected, actual = normalize(expected), normalize(actual)
    if expected != actual:
        raise AssertionError(f'{context}\nexpected={json.dumps(expected)[:5000]}\nactual={json.dumps(actual)[:5000]}')


def timed(fn):
    start = time.perf_counter()
    result = fn()
    return result, (time.perf_counter() - start) * 1000


def command_stats(command, log_path, env=None, expect_success=True, runtime_record=None):
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.perf_counter()
    try:
        p = subprocess.run([str(x) for x in command], capture_output=True, env=dict(os.environ, **(env or {})), timeout=60)
    except subprocess.TimeoutExpired as error:
        log_path.write_bytes((error.stdout or b'') + (error.stderr or b''))
        if runtime_record is not None:
            runtime_record['timed_out'] = True
        raise
    except OSError as error:
        if runtime_record is not None:
            runtime_record['launch_error'] = str(error)
        raise
    if runtime_record is not None:
        runtime_record['exit_code'] = p.returncode
    wall = time.perf_counter() - start
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    log_path.write_bytes(p.stdout + p.stderr)
    if expect_success and p.returncode:
        raise AssertionError(f'Command failed {command}: {(p.stdout + p.stderr).decode()[-7000:]}')
    if not expect_success and not p.returncode:
        raise AssertionError(f'Command unexpectedly succeeded {command}')
    return {'wall_seconds': wall, 'cpu_seconds': after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
            'max_rss_kib_children': after.ru_maxrss, 'exit_code': p.returncode}


def check_vm_only(work, replicas, gomaxprocs=None):
    procs = Processes(work, gomaxprocs)
    try:
        cluster = Cluster(procs, 'fixture-validation', replicas)
        cluster.start('full')
        now = int(time.time()) // 3600 * 3600000
        start, boundary, end = now - 6 * 3600000, now - 3 * 3600000, now - 3600000
        counts = {}
        for i, tenant in enumerate(['1:2', '7:9']):
            counts[tenant] = remote_write(cluster.insert, tenant, fixture(start, boundary, end, i))
        cluster.flush()
        result = api(cluster.select, '1:2', 'query', {'query': 'cold_only', 'time': (start + 3600000) / 1000})
        assert result['result'], result
        native = request(f'http://127.0.0.1:{cluster.select}/select/1:2/prometheus/api/v1/export/native?' +
                         urllib.parse.urlencode({'match[]': '{__name__!=""}', 'start': start / 1000,
                                                 'end': (boundary - 1) / 1000}))
        (work / 'spike.native').write_bytes(native)
        print(json.dumps({'fixture_counts': counts, 'native_bytes': len(native), 'data_start_ms': start,
                          'boundary_ms': boundary, 'data_end_ms': end, 'cold_query': result}, indent=2))
    finally:
        procs.close()
        (work / 'runtime.json').write_text(json.dumps(procs.runtime, indent=2) + '\n')


def metrics(port):
    result = {}
    for line in request(f'http://127.0.0.1:{port}/metrics').decode().splitlines():
        if line and not line.startswith('#'):
            key, val = line.rsplit(None, 1)
            try:
                result[key] = float(val)
            except ValueError:
                pass
    return result


def run(work, args):
    work = work.resolve()
    procs = Processes(work, args.gomaxprocs)
    reports = {'backend': args.backend, 'replicas': args.replicas,
               'architecture': 'v2: independent persistent catalog authority; ordinary PUT/GET/Range blobs',
               'started_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
               'vm_revision': '9a009c491da4c17531893ab4acb6a5f1b5e0eaf2',
               'dedup_interval': '1ms', 'vm_result_cache': 'disabled',
               'tiered_vm_labels_api_max_duration': '30s',
               'comparison': 'exact timestamps, labels and numeric strings; no tolerance',
               'checks': [], 'query_timings_ms': {}, 'runtime': procs.runtime}
    tenants = ['1:2', '7:9']
    source_proxy = None
    s3_proxy = None
    test_env = {'AWS_ACCESS_KEY_ID': 'vmtier-test', 'AWS_SECRET_ACCESS_KEY': 'vmtier-test-secret'}
    try:
        for binary in ['vmstorage', 'vminsert', 'vmselect', 'vmtier']:
            if not (ROOT / '.bin' / binary).exists():
                raise RuntimeError(f'Missing .bin/{binary}; build it first')
        source_hash = hashlib.sha256()
        source_paths = sorted(list((ROOT / 'cmd').rglob('*.go')) +
                              list((ROOT / 'internal').rglob('*.go')) +
                              [ROOT / 'go.mod', ROOT / 'go.sum'])
        for source in source_paths:
            source_hash.update(str(source.relative_to(ROOT)).encode() + b'\0' + source.read_bytes() + b'\0')
        reports['provenance'] = {
            'executor_hostname': socket.gethostname(),
            'binary_sha256': {name: hashlib.sha256((ROOT / '.bin' / name).read_bytes()).hexdigest()
                              for name in ('vmtier', 'vmstorage', 'vminsert', 'vmselect')},
            'harness_sha256': {name: hashlib.sha256((ROOT / 'scripts' / name).read_bytes()).hexdigest()
                               for name in ('e2e.py', 's3_compat_proxy.py')},
            'go_source_tree_sha256': source_hash.hexdigest(),
            'go_source_tree_hash_scope': 'sorted cmd/**/*.go, internal/**/*.go, go.mod and go.sum; path NUL bytes NUL',
        }
        reports['environment'] = {
            'os': platform.freedesktop_os_release().get('PRETTY_NAME', platform.system()),
            'machine': platform.machine(), 'linux_kernel': platform.release(),
            'python': platform.python_version(),
            'logical_cpus_available': len(os.sched_getaffinity(0)),
            'memory_total_gib': int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines()
                                         if line.startswith('MemTotal:'))) / (1024 ** 2),
            'measurement_scope': 'single managed development container, loopback; no real Sakura/AWS/MinIO service',
            'moto': None,
        }
        go_bin = os.environ.get('GO_BIN') or shutil.which('go')
        if go_bin:
            reports['environment']['go'] = subprocess.check_output([go_bin, 'version'], text=True).strip()
        horizon = fixture_horizon(int(time.time() * 1000))
        start, boundary, end = (horizon[key] for key in ('start_ms', 'boundary_ms', 'end_ms'))
        reports['fixture'] = {**horizon,
                              'scrape_interval_ms': 30000, 'tenants': tenants,
                              'series_per_tenant': 16, 'labels': 'fixed 2 instances, 2 zones; churn old/new',
                              'special_cases': ['irregular gaps', 'counter reset', 'stale marker', 'NaN',
                                                '+Inf', '-Inf', 'duplicate and conflicting timestamps',
                                                'millisecond boundary', 'empty/absent labels']}
        if args.backend == 'local':
            store = {'type': 'local', 'path': str(work / 'objects')}
        else:
            moto = os.environ.get('MOTO_SERVER') or shutil.which('moto_server')
            if not moto and Path('/workspace/tools/moto-venv/bin/moto_server').exists():
                moto = '/workspace/tools/moto-venv/bin/moto_server'
            if not moto:
                raise RuntimeError('Install scripts/requirements-e2e.txt; set MOTO_SERVER to its moto_server')
            moto_python = Path(moto).resolve().parent / 'python'
            if moto_python.exists():
                reports['environment']['moto'] = subprocess.check_output(
                    [moto_python, '-c', 'from importlib.metadata import version; print(version("moto"))'], text=True).strip()
            s3_port = free_port()
            endpoint = f'http://127.0.0.1:{s3_port}'
            s3_process = procs.start('moto-s3', [moto, '-H', '127.0.0.1', '-p', str(s3_port)], endpoint,
                                    test_env)
            bucket = 'vmtier-test'
            # Moto is a loopback test double; only this harness creates its test bucket.
            with HTTP.open(urllib.request.Request(endpoint + '/' + bucket, method='PUT', data=b'')) as r:
                r.read()
            s3_proxy = S3CompatibilityProxy(endpoint)
            store = {'type': 's3', 'endpoint': s3_proxy.url, 'region': 'us-east-1', 'bucket': bucket, 'prefix': 'e2e'}
        bin_path = ROOT / '.bin/vmtier'
        catalog_path = work / 'catalog-authority'
        catalog_port = free_port()
        catalog_url = f'http://127.0.0.1:{catalog_port}'
        catalog_id = 'vmtier-e2e-authority'
        procs.command_stats([bin_path, 'catalog-init', '-path', catalog_path, '-id', catalog_id],
                      work / 'catalog-init.log', test_env)

        def start_catalog(label):
            return procs.start(label, [bin_path, 'catalog', '-path', catalog_path, '-id', catalog_id,
                                      '-http', f'127.0.0.1:{catalog_port}'],
                               catalog_url + '/v1/catalog', test_env)

        def catalog_get():
            return request(catalog_url + '/v1/catalog')

        catalog_process = start_catalog('catalog-service')
        initial_manifest = json.loads(catalog_get())
        assert initial_manifest['generation'] == 0 and initial_manifest['id'] == catalog_id
        reports['catalog_authority'] = {'type': 'independent-http-service', 'id': catalog_id,
                                        's3_conditional_writes': False, 's3_etag_required': False}
        ref, hot = Cluster(procs, 'reference' , args.replicas), Cluster(procs, 'hot', args.replicas)
        ref.start('full')
        hot.start('full')
        sample_count = 0
        for i, tenant in enumerate(tenants):
            fixture_data = fixture(start, boundary, end, i)
            sample_count += remote_write(ref.insert, tenant, fixture_data)
            remote_write(hot.insert, tenant, fixture_data)
        ref.flush()
        hot.flush()
        reports['fixture']['input_samples'] = sample_count
        for tenant in tenants:
            compare(api(ref.select, tenant, 'query', {'query': 'cold_only', 'time': (boundary - 600000) / 1000}),
                    api(hot.select, tenant, 'query', {'query': 'cold_only', 'time': (boundary - 600000) / 1000}),
                    'identical initial ingestion')
        reports['checks'].append('reference and pre-archive Hot both contain identical old data')
        source_proxy = SourceProxy(hot.select)
        cfg = {'store': store, 'catalog': {'type': 'http', 'url': catalog_url, 'id': catalog_id}, 'tenants': tenants,
               'sources': [{'id': 'hot', 'url': source_proxy.url}],
               'archive_age': f"{horizon['archive_age_ms']}ms", 'hot_retention': '240h', 'lateness': '30m',
               'safety_margin': '30m', 'window': '1h', 'lateness_enforced': True,
               'coverage_start_ms': start}
        def object_url(key):
            return store['endpoint'] + '/' + store['bucket'] + '/' + store['prefix'] + '/' + key

        def object_get(key):
            return (Path(store['path']) / key).read_bytes() if args.backend == 'local' else test_s3_request(object_url(key))

        def object_set(key, data):
            # Deliberate corruption touches only this harness's temporary object store.
            if args.backend == 'local':
                path = Path(store['path']) / key
                if data is None:
                    path.unlink()
                else:
                    path.write_bytes(data)
            else:
                test_s3_request(object_url(key), 'DELETE' if data is None else 'PUT', data)
                if data is not None:
                    assert object_get(key) == data, 'Test S3 mutation failed to persist exact bytes'

        cfg_path = work / 'config.json'
        cfg_path.write_text(json.dumps(cfg, indent=2))
        bin_path = ROOT / '.bin/vmtier'

        def archive(config=cfg_path, first=start, last=boundary, label='archive', success=True):
            return procs.command_stats([bin_path, 'archive', '-config', config, '-start', str(first), '-end', str(last)],
                                 work / f'{label}.log', test_env, success)

        invalid_cfg = dict(cfg, lateness_enforced=False)
        (work / 'invalid.json').write_text(json.dumps(invalid_cfg))
        archive(work / 'invalid.json', label='invalid-lateness', success=False)
        reports['checks'].append('unenforced late-write contract rejected')
        missing_cfg = dict(cfg, sources=[{'id': 'hot', 'url': f'http://127.0.0.1:{free_port()}'}])
        (work / 'missing-source.json').write_text(json.dumps(missing_cfg))
        archive(work / 'missing-source.json', label='missing-source', success=False)
        reports['checks'].append('unreachable export source rejected before coverage commit')
        source_proxy.interrupt = True
        interrupted = procs.start('archive-interrupted', [bin_path, 'archive', '-config', cfg_path,
                                  '-start', str(start), '-end', str(boundary)], env=test_env)
        assert source_proxy.started.wait(10), 'Archive never reached injected native stream pause'
        interrupted.kill()
        interrupted.wait(timeout=10)
        source_proxy.release.set()
        catalog_before = json.loads(catalog_get())
        assert catalog_before['generation'] == 0 and not catalog_before['entries'], 'Interrupted export published coverage'
        reports['checks'].append('SIGKILL during native stream leaves no committed coverage; fresh archive resumes')
        reports['archive'] = archive()
        stats = json.loads((work / 'archive.log').read_text().splitlines()[0])
        reports['archive'].update(stats)
        reports['archive']['samples_per_second'] = stats['samples'] / reports['archive']['wall_seconds']
        manifest_bytes = catalog_get()
        manifest = json.loads(manifest_bytes)
        total_pack, total_index = 0, 0
        for entry in manifest['entries']:
            assert entry['end_ms'] <= boundary
            index_bytes = object_get(entry['index_key'])
            total_index += len(index_bytes)
            total_pack += len(object_get(entry['data_key']))
        reports['object_bytes'] = {'pack': total_pack, 'index': total_index, 'catalog_authority_manifest': len(manifest_bytes),
                                   'packs': len(manifest['entries'])}

        procs.command_stats([bin_path, 'verify', '-config', cfg_path], work / 'verify.log', test_env)
        reports['checks'].append('age-selected native export uploaded, verified and committed')
        reports['archive_repeat'] = archive(label='archive-repeat')
        assert catalog_get() == manifest_bytes, 'Replay changed committed catalog'
        procs.command_stats([bin_path, 'archive', '-config', cfg_path], work / 'archive-automatic.log', test_env)
        assert catalog_get() == manifest_bytes, 'Automatic scheduler re-exported committed coverage'
        procs.command_stats([bin_path, 'inspect', '-config', cfg_path], work / 'inspect.log', test_env)
        reports['checks'].append('same archive windows replay safely and verify succeeds')
        shifted_cfg = dict(cfg, coverage_start_ms=start + 3600000)
        (work / 'shifted-origin.json').write_text(json.dumps(shifted_cfg))
        archive(work / 'shifted-origin.json', first=start + 3600000, label='shifted-origin', success=False)
        assert catalog_get() == manifest_bytes, 'Mismatched coverage origin changed authority'
        reports['checks'].append('committed catalog rejects a changed coverage origin instead of masking holes')
        for field, changed in [('archive_age', '2h'), ('lateness', '45m')]:
            changed_path = work / f'changed-{field}.json'
            changed_path.write_text(json.dumps(dict(cfg, **{field: changed})))
            archive(changed_path, label=f'changed-{field}', success=False)
            assert catalog_get() == manifest_bytes, f'Changed {field} mutated committed authority'
        reports['checks'].append('committed catalog rejects changes to archive age and allowed lateness')
        # Catalog metadata must never be published into the blob backend.
        try:
            object_get('catalog.json')
        except (FileNotFoundError, urllib.error.HTTPError) as exc:
            if isinstance(exc, urllib.error.HTTPError) and exc.code != 404:
                raise
        else:
            raise AssertionError('Legacy catalog.json appeared in blob storage')
        gateway_port, gateway_http = free_port(), free_port()

        def start_gateway(label):
            return procs.start(label, [bin_path, 'gateway', '-config', cfg_path,
                                      '-listen', f'127.0.0.1:{gateway_port}', '-http', f'127.0.0.1:{gateway_http}'],
                               f'http://127.0.0.1:{gateway_http}/ready', test_env)

        gateway_process = start_gateway('gateway')
        cold_select = free_port()
        start_select(procs, 'cold-only-select', cold_select, [gateway_port])
        params = {'query': 'cold_only', 'time': (boundary - 600000) / 1000}
        before = metrics(gateway_http)
        result = api(cold_select, tenants[0], 'query', params)
        assert result['result'], 'Cold compatibility spike returned no data'
        compare(api(ref.select, tenants[0], 'query', params), result, 'native portable -> gateway wire -> stock vmselect')
        after = metrics(gateway_http)
        range_key = 'vmtier_object_range_requests_total'
        assert after.get(range_key, 0) > before.get(range_key, 0), f'Cold spike did not fetch block ranges: {after}'
        reports['checks'].append('native portable pack independently served to unmodified stock vmselect (query + Range GET)')
        params_range = {'query': 'cold_only', 'start': (start + 600000) / 1000,
                        'end': (boundary - 600000) / 1000, 'step': '60s'}
        compare(api(ref.select, tenants[0], 'query_range', params_range),
                api(cold_select, tenants[0], 'query_range', params_range), 'cold-only query_range spike')
        for tenant in tenants:
            young_raw = request(f'http://127.0.0.1:{cold_select}/select/{tenant}/prometheus/api/v1/export?' +
                                urllib.parse.urlencode({'match[]': '{__name__!=""}', 'start': boundary / 1000,
                                                        'end': end / 1000})).strip()
            assert not young_raw, f'Cold reader leaked samples beyond committed window: {young_raw[:500]}'
        reports['checks'].append('Cold-only raw export excludes all young samples, including the exact boundary millisecond')
        # A real fresh Hot storage directory proves old samples are no longer queryable.
        hot.stop()
        hot.start('recent-only')
        for i, tenant in enumerate(tenants):
            remote_write(hot.insert, tenant, fixture(start, boundary, end, i), start=boundary)
        hot.flush()
        for tenant in tenants:
            raw = request(f'http://127.0.0.1:{hot.select}/select/{tenant}/prometheus/api/v1/export?' +
                          urllib.parse.urlencode({'match[]': '{__name__!=""}', 'start': start / 1000,
                                                  'end': (boundary - 1) / 1000})).strip()
            assert not raw, f'Old samples remain in recreated Hot: {raw[:500]}'
        reports['checks'].append('fresh Hot storage contains recent samples only; raw old-range export is empty for both tenants')
        # Reconnect persisted catalog and objects after gateway restart.
        procs.stop(gateway_process)
        gateway_process = start_gateway('gateway-restarted')
        top_select = free_port()
        # Cold metadata validates full blocks; the default five-second label
        # deadline can expire on an uncached S3 scan without a result mismatch.
        start_select(procs, 'tiered-select', top_select, hot.storage_read + [gateway_port], labels_api_duration='30s')
        expressions = [
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
            '{__name__="empty_label",maybe=""}',
            '{__name__="empty_label",absent=""}',
            '{__name__="empty_label",absent!="x"}',
            '{__name__="empty_label",absent=~".*"}',
            '{__name__="empty_label",absent!~"x"}',
            '{__name__=~"cold_only|temperature",job!~".*"}',
            '{__name__=~"cold_only|temperature",job!~"^.*$"}',
            '{__name__=~"cold_only|temperature",job!~"demo|"}',
        ]
        ranges = {'cold': (start + 600000, boundary - 600000),
                  'hot': (boundary + 600000, end),
                  'boundary': (boundary - 900000, boundary + 900000)}
        diff_count = 0
        for tenant in tenants:
            for label, (a, b) in ranges.items():
                for expression in expressions:
                    params = {'query': expression, 'start': a / 1000, 'end': b / 1000, 'step': '60s'}
                    expected, ref_ms = timed(lambda: api(ref.select, tenant, 'query_range', params))
                    actual, tier_ms = timed(lambda: api(top_select, tenant, 'query_range', params))
                    compare(expected, actual, f'{tenant} {label} query_range {expression}')
                    reports['query_timings_ms'].setdefault(label + '_reference', []).append(ref_ms)
                    reports['query_timings_ms'].setdefault(label + '_tiered', []).append(tier_ms)
                    diff_count += 1
                for expression in expressions:
                    params = {'query': expression, 'time': b / 1000}
                    compare(api(ref.select, tenant, 'query', params), api(top_select, tenant, 'query', params),
                            f'{tenant} {label} query {expression}')
                    diff_count += 1
        reports['differential_checks'] = diff_count
        reports['checks'].append(f'{diff_count} exact differential instant/range queries passed across Cold, Hot and boundary')
        for tenant in tenants:
            for path in ['labels', 'label/cold_label/values', 'label/__name__/values', 'series']:
                params = {'start': start / 1000, 'end': end / 1000, 'match[]': '{__name__!=""}'}
                expected = api(ref.select, tenant, path, params)
                actual = api(top_select, tenant, path, params)
                if expected and isinstance(expected[0], str):
                    expected, actual = sorted(expected), sorted(actual)
                compare(expected, actual, f'{tenant} metadata {path}')
        reports['checks'].append('label names/values and series discovery include Cold; tenant-separated values match reference')
        reports['dedicated_latency_ms'] = {}
        for label, expr, lo, hi in [('hot_only', 'hot_only', boundary + 900000, end),
                                    ('cold_only', 'cold_only', start + 900000, boundary - 900000),
                                    ('cross_boundary', 'rate(requests_total[10m])', boundary - 900000, boundary + 900000)]:
            params = {'query': expr, 'start': lo / 1000, 'end': hi / 1000, 'step': '60s'}
            baseline, tiered = [], []
            for repetition in range(5):
                expected, ref_ms = timed(lambda: api(ref.select, tenants[0], 'query_range', params))
                actual, tier_ms = timed(lambda: api(top_select, tenants[0], 'query_range', params))
                compare(expected, actual, f'dedicated benchmark {label}')
                baseline.append(ref_ms)
                tiered.append(tier_ms)
            reports['dedicated_latency_ms'][label] = {'query': expr, 'repetitions': 5,
                'reference_median': statistics.median(baseline), 'tiered_median': statistics.median(tiered),
                'gateway_cache_bytes': 0, 'vm_result_cache': 'disabled', 'server_and_os': 'warm'}
        reports['final_metrics'] = metrics(gateway_http)
        # Measure immutable-object caching independently of vmselect result caching.
        procs.stop(gateway_process)
        cfg['cache_bytes'] = 32 << 20
        cfg_path.write_text(json.dumps(cfg, indent=2))
        gateway_process = start_gateway('gateway-cache-enabled')
        cache_params = {'query': 'cold_only[30m]', 'time': (start + 7200000) / 1000}
        cache_before = metrics(gateway_http)
        cold_result, cold_ms = timed(lambda: api(top_select, tenants[0], 'query', cache_params))
        cache_mid = metrics(gateway_http)
        warm_result, warm_ms = timed(lambda: api(top_select, tenants[0], 'query', cache_params))
        cache_after = metrics(gateway_http)
        compare(cold_result, warm_result, 'cached immutable blocks')
        compare(api(ref.select, tenants[0], 'query', cache_params), cold_result, 'cache oracle')
        byte_key = 'vmtier_object_download_bytes_total'
        first_ranges = cache_mid.get(range_key, 0) - cache_before.get(range_key, 0)
        second_ranges = cache_after.get(range_key, 0) - cache_mid.get(range_key, 0)
        assert first_ranges > 0 and second_ranges == 0, 'Cache failed to reuse block ranges'
        raw_samples = sum(len(series['values']) for series in cold_result['result'])
        assert raw_samples > 0
        cold_bytes = cache_mid.get(byte_key, 0) - cache_before.get(byte_key, 0)
        reports['cache_measurement'] = {
            'query': cache_params['query'], 'cold_latency_ms': cold_ms, 'warm_latency_ms': warm_ms,
            'cold_range_gets': first_ranges, 'warm_range_gets': second_ranges,
            'cold_blob_download_bytes_including_index': cold_bytes,
            'warm_blob_download_bytes': cache_after.get(byte_key, 0) - cache_mid.get(byte_key, 0),
            'returned_raw_samples': raw_samples,
            'blob_read_amplification_including_index': cold_bytes / (raw_samples * 16),
            'read_amplification_denominator': 'returned raw timestamp+float64 pairs, 16 bytes/sample',
            'cache_limit_bytes': cfg['cache_bytes'], 'vm_result_cache': 'disabled'}
        reports['checks'].append('immutable block cache reuses data with zero additional Range GETs; values unchanged')
        # A warm blob cache cannot replace the authoritative control-plane read.
        procs.stop(catalog_process)
        outage_params = dict(cache_params, timeout='3s')
        try:
            api(top_select, tenants[0], 'query', outage_params)
        except urllib.error.HTTPError as exc:
            assert exc.code >= 400
        else:
            raise AssertionError('Warm-cache query succeeded while catalog authority was unavailable')
        try:
            request(f'http://127.0.0.1:{gateway_http}/ready', timeout=10)
        except urllib.error.HTTPError as exc:
            assert exc.code >= 400
        else:
            raise AssertionError('Gateway readiness ignored unavailable catalog authority')
        catalog_process = start_catalog('catalog-service-restarted')
        assert catalog_get() == manifest_bytes, 'Catalog restart lost committed generation'
        compare(cold_result, api(top_select, tenants[0], 'query', cache_params), 'catalog restart with warm blob cache')
        reports['checks'].append('warm-cache queries and readiness fail on catalog outage; durable authority restart restores reads')
        procs.stop(gateway_process)
        cfg['cache_bytes'] = 0
        cfg_path.write_text(json.dumps(cfg, indent=2))
        gateway_process = start_gateway('gateway-fault-tests')
        damaged_key = manifest['entries'][0]['data_key']
        original = object_get(damaged_key)
        fail_params = {'query': 'count_over_time({__name__!=""}[30m]) keep_metric_names',
                       'time': (start + 1800000) / 1000, 'timeout': '3s'}
        compare(api(ref.select, tenants[0], 'query', fail_params),
                api(top_select, tenants[0], 'query', fail_params), 'healthy pre-fault baseline')
        for label, damaged in [('missing', None), ('truncated', original[:8]),
                                ('checksum', bytes([original[0] ^ 1]) + original[1:])]:
            object_set(damaged_key, damaged)
            try:
                try:
                    api(top_select, tenants[0], 'query', fail_params)
                except urllib.error.HTTPError as error:
                    assert error.code >= 400
                else:
                    raise AssertionError(f'{label} payload produced successful query')
                procs.command_stats([bin_path, 'verify', '-config', cfg_path], work / f'verify-{label}.log',
                              test_env, expect_success=False)
            finally:
                object_set(damaged_key, original)
        compare(api(ref.select, tenants[0], 'query', fail_params),
                api(top_select, tenants[0], 'query', fail_params), 'object restoration')
        reports['checks'].append('missing/truncated/checksum-invalid objects fail query and verify; restored objects recover')
        try:
            request(f'http://127.0.0.1:{cold_select}/delete/{tenants[0]}/prometheus/api/v1/admin/tsdb/delete_series?' +
                    urllib.parse.urlencode({'match[]': 'cold_only'}), data=b'')
        except urllib.error.HTTPError as exc:
            body = exc.read().decode().lower()
            assert 'unsupported' in body or 'not supported' in body, body
        else:
            raise AssertionError('Cold delete falsely reported success')
        reports['checks'].append('Cold delete API explicitly rejects unsupported mutation')
        procs.stop(catalog_process)
        unavailable_catalog_path = work / 'catalog-authority-held'
        catalog_path.rename(unavailable_catalog_path)
        procs.command_stats([bin_path, 'catalog', '-path', catalog_path, '-id', catalog_id,
                       '-http', f'127.0.0.1:{catalog_port}'], work / 'catalog-missing.log', test_env, expect_success=False)
        assert not catalog_path.exists(), 'Missing authority was silently initialized'
        unavailable_catalog_path.rename(catalog_path)
        catalog_process = start_catalog('catalog-service-restored')
        assert catalog_get() == manifest_bytes
        reports['checks'].append('catalog startup refuses missing authority state; no implicit empty genesis')
        # Establish a successful frontend connection, then interrupt only blob
        # access. No gateway restart, cached dial failure, or arbitrary sleep can
        # satisfy this assertion. Allow the bounded SDK retries to finish.
        assert cfg['cache_bytes'] == 0
        outage_params = dict(fail_params, timeout='15s')
        expected = api(ref.select, tenants[0], 'query', outage_params)
        compare(expected, api(top_select, tenants[0], 'query', outage_params), 'blob outage baseline')
        before_catalog = catalog_get()
        request(f'http://127.0.0.1:{gateway_http}/health')
        request(f'http://127.0.0.1:{gateway_http}/ready')
        before_proxy = s3_proxy.snapshot() if s3_proxy else None
        with unavailable_blobs(store, work, s3_proxy) as marker:
            evidence = assert_running_blob_failure(
                lambda: api(top_select, tenants[0], 'query', outage_params),
                gateway_process, lambda: metrics(gateway_http), marker)
            request(f'http://127.0.0.1:{gateway_http}/health')
            request(f'http://127.0.0.1:{gateway_http}/ready')
            assert catalog_get() == before_catalog
        compare(expected, api(top_select, tenants[0], 'query', outage_params), 'blob outage recovery')
        assert catalog_get() == before_catalog == manifest_bytes
        if s3_proxy:
            evidence['proxy_failed_gets'] = s3_proxy.snapshot()['unavailable_get_requests'] - before_proxy['unavailable_get_requests']
            assert evidence['proxy_failed_gets'] > 0
        evidence.update(cache_bytes=0, process_pid=gateway_process.pid,
                        coverage_unchanged=True, healthy_and_ready_during_outage=True,
                        exact_baseline_and_recovery=True)
        reports['blob_outage'] = evidence
        reports['checks'].append('running Cold gateway attempts blob GET and returns its error; health/coverage readiness stay successful and exact reads recover without publication changes')
        for key, values in list(reports['query_timings_ms'].items()):
            ordered = sorted(values)
            reports['query_timings_ms'][key] = {'count': len(values), 'median': statistics.median(values),
                                               'p95': ordered[min(len(ordered) - 1, int(.95 * len(ordered)))]}
        if s3_proxy:
            proxy_counts = s3_proxy.snapshot()
            assert proxy_counts['conditional_requests_rejected'] == 0, proxy_counts
            assert proxy_counts['etag_headers_removed'] > 0, proxy_counts
            assert proxy_counts['put_requests'] > 0, proxy_counts
            assert proxy_counts['put_content_md5'] == proxy_counts['put_requests'], proxy_counts
            assert proxy_counts['bad_content_md5'] == 0, proxy_counts
            reports['s3_compatibility_proxy'] = proxy_counts
            reports['checks'].append('all S3 traffic passed a conditional-request-rejecting, ETag-stripping proxy with valid Content-MD5 PUTs')
        assert int(time.time() * 1000) < horizon['next_eligible_at_ms'], 'Fixture exceeded its 30-minute eligibility horizon'
        reports['status'] = 'passed'
    except Exception as exc:
        reports['status'], reports['failure'] = 'failed', str(exc)
        raise
    finally:
        procs.close()
        if source_proxy:
            source_proxy.close()
        if s3_proxy:
            s3_proxy.close()
        if 'fixture' in reports:
            reports['fixture']['finished_at_ms'] = int(time.time() * 1000)
        reports['recorded_at_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (work / 'results.json').write_text(json.dumps(reports, indent=2, sort_keys=True))
        print(json.dumps({k: v for k, v in reports.items() if k not in ['final_metrics']}, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workdir', type=Path)
    parser.add_argument('--gomaxprocs', type=positive_int, help='GOMAXPROCS for workload children (default: outer GOMAXPROCS, else 2)')
    parser.add_argument('--backend', choices=['local', 's3'], default='local')
    parser.add_argument('--replicas', type=int, choices=[1, 2], default=1)
    parser.add_argument('--vm-only', action='store_true', help='Validate stock fixture ingestion/export before vmtier integration')
    args = parser.parse_args()
    work = args.workdir or Path(tempfile.mkdtemp(prefix='vmtier-e2e-'))
    if work.exists() and any(work.iterdir()):
        parser.error('--workdir must be a new or empty directory; previous evidence is preserved')
    work.mkdir(parents=True, exist_ok=True)
    print(f'Artifacts: {work}', flush=True)
    if args.vm_only:
        check_vm_only(work, args.replicas, args.gomaxprocs)
    else:
        run(work, args)


if __name__ == '__main__':
    main()
