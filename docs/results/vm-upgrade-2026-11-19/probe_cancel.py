#!/usr/bin/env python3
"""Probe how a vmtier gateway handles stock vmselect's query cancellation marker.

Stock vmselect v1.156.0 writes a length-prefixed "cancelRequestMarker" to a
storage connection when its query context is canceled while it still reads
MetricBlocks, then keeps the connection in its pool. A TCP proxy between the
frontend and the gateway delays the gateway's response so that the HTTP client
can disconnect first, which makes the frontend send the marker.
"""
import argparse
import json
from pathlib import Path
import queue
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

p = argparse.ArgumentParser()
p.add_argument('--repo', type=Path, required=True)
p.add_argument('--vmtier', type=Path, required=True, help='vmtier binary under test')
p.add_argument('--frontend', type=Path, required=True, help='stock vmselect binary')
p.add_argument('--archive-run', type=Path, required=True)
p.add_argument('--workdir', type=Path, required=True)
p.add_argument('--delay', type=float, default=1.5)
a = p.parse_args()
a.workdir.mkdir(parents=True, exist_ok=False)
sys.path.insert(0, str(a.repo / 'scripts'))
import e2e  # noqa: E402

MARKER = b'cancelRequestMarker'


class Proxy:
    """Forward frontend<->gateway bytes; optionally delay gateway->frontend."""

    def __init__(self, upstream_port):
        self.upstream_port = upstream_port
        self.delay = 0.0
        self.connections = 0
        self.client_bytes = bytearray()
        self.lock = threading.Lock()
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('127.0.0.1', 0))
        self.sock.listen(16)
        self.port = self.sock.getsockname()[1]
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while True:
            try:
                client, _ = self.sock.accept()
            except OSError:
                return
            upstream = socket.create_connection(('127.0.0.1', self.upstream_port))
            with self.lock:
                self.connections += 1
            pending = queue.Queue()
            threading.Thread(target=self.client_to_upstream, args=(client, upstream), daemon=True).start()
            threading.Thread(target=self.upstream_reader, args=(upstream, pending), daemon=True).start()
            threading.Thread(target=self.delayed_writer, args=(client, pending), daemon=True).start()

    def client_to_upstream(self, client, upstream):
        try:
            while data := client.recv(65536):
                with self.lock:
                    self.client_bytes += data
                upstream.sendall(data)
        except OSError:
            pass
        finally:
            for s in (client, upstream):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def upstream_reader(self, upstream, pending):
        try:
            while data := upstream.recv(65536):
                pending.put((time.monotonic() + self.delay, data))
        except OSError:
            pass
        pending.put(None)

    def delayed_writer(self, client, pending):
        try:
            while (item := pending.get()) is not None:
                due, data = item
                time.sleep(max(0.0, due - time.monotonic()))
                client.sendall(data)
        except OSError:
            pass
        finally:
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def markers(self):
        with self.lock:
            return bytes(self.client_bytes).count(MARKER)


def url(port, path, params):
    return (f'http://127.0.0.1:{port}/select/1:2/prometheus/api/v1/{path}?'
            + urllib.parse.urlencode(params))


def query(port, params):
    try:
        with urllib.request.urlopen(url(port, 'query_range', params), timeout=30) as r:
            body = json.loads(r.read())
        ok = body.get('status') == 'success' and not body.get('isPartial')
        return {'http_status': 200, 'ok': ok, 'series': len(body.get('data', {}).get('result', [])),
                'body': None if ok else body}
    except urllib.error.HTTPError as error:
        return {'http_status': error.code, 'ok': False, 'body': error.read().decode()[:2000]}


def canceled_query(port, params, timeout):
    try:
        with urllib.request.urlopen(url(port, 'query_range', params), timeout=timeout) as r:
            r.read()
        return 'completed before the client timeout'
    except (TimeoutError, socket.timeout, urllib.error.URLError) as error:
        return f'client disconnected: {error}'


def log_lines(path, needles):
    text = path.read_text(errors='replace') if path.exists() else ''
    return [line for line in text.splitlines() if any(n in line for n in needles)]


def frontend_metrics(port):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics', timeout=10) as r:
        text = r.read().decode()
    keep = {}
    for line in text.splitlines():
        if line.startswith('#'):
            continue
        name = line.split('{')[0].split(' ')[0]
        if name.startswith(('vm_rpc_', 'vm_tcpdialer_', 'vm_partial_results')) or 'Errors' in name:
            keep[line.rsplit(' ', 1)[0]] = float(line.rsplit(' ', 1)[1])
    return keep


procs = e2e.Processes(a.workdir, 2)
report = {'vmtier': str(a.vmtier),
          'vmtier_version': subprocess.run([str(a.vmtier), 'version'], capture_output=True, text=True).stdout.strip(),
          'frontend_version': subprocess.check_output([str(a.frontend), '-version'], text=True).strip(),
          'delay_seconds': a.delay, 'steps': []}
try:
    cfg = json.loads((a.archive_run / 'config.json').read_text())
    fixture = json.loads((a.archive_run / 'results.json').read_text())['fixture']
    cp, gp, hp, fp = e2e.free_port(), e2e.free_port(), e2e.free_port(), e2e.free_port()
    cfg['catalog']['url'] = f'http://127.0.0.1:{cp}'
    config = a.workdir / 'config.json'
    config.write_text(json.dumps(cfg))
    procs.start('catalog', [a.vmtier, 'catalog', '-path', a.archive_run / 'catalog-authority',
                            '-id', cfg['catalog']['id'], '-http', f'127.0.0.1:{cp}'],
                cfg['catalog']['url'] + '/v1/catalog')
    procs.start('gateway', [a.vmtier, 'gateway', '-config', config,
                            '-listen', f'127.0.0.1:{gp}', '-http', f'127.0.0.1:{hp}'],
                f'http://127.0.0.1:{hp}/ready')
    proxy = Proxy(gp)
    procs.start('frontend', [a.frontend, f'-httpListenAddr=127.0.0.1:{fp}', f'-storageNode=127.0.0.1:{proxy.port}',
                             '-replicationFactor=1', '-globalReplicationFactor=1', '-search.skipSlowReplicas=false',
                             '-search.denyPartialResponse=true', '-search.disableCache',
                             '-dedup.minScrapeInterval=1ms', '-memory.allowedBytes=128MiB',
                             f'-cacheDataPath={a.workdir / "frontend-cache"}'],
                f'http://127.0.0.1:{fp}/health')
    params = {'query': 'cold_only', 'start': (fixture['start_ms'] + 600000) / 1000,
              'end': (fixture['boundary_ms'] - 600000) / 1000, 'step': '60s'}

    def step(name, **extra):
        record = {'step': name, 'proxy_connections': proxy.connections,
                  'markers_sent_by_frontend': proxy.markers(), **extra}
        report['steps'].append(record)
        print(json.dumps(record), flush=True)

    baseline = query(fp, params)
    assert baseline['ok'] and baseline['series'] > 0, baseline
    metrics_before = frontend_metrics(fp)
    step('warm-up query', result=baseline)
    for pause in (1.0, 7.0):
        proxy.delay = a.delay
        outcome = canceled_query(fp, params, timeout=a.delay / 3)
        time.sleep(a.delay + 1.0)  # let the delayed response, the marker and the trace drain
        proxy.delay = 0.0
        step(f'canceled query, then wait {pause}s', client=outcome)
        time.sleep(pause)
        result = query(fp, params)
        step(f'next query after {pause}s', result=result, same_series_as_baseline=result.get('series') == baseline['series'])
    metrics_after = frontend_metrics(fp)
    report['frontend_metric_deltas'] = {k: metrics_after[k] - metrics_before.get(k, 0)
                                        for k in metrics_after if metrics_after[k] != metrics_before.get(k, 0)}
finally:
    procs.close()
needles = ['cancelRequestMarker', 'unsupported rpcName', 'connMonitor', 'cannot process vmselect request',
           'cannot read', 'error', 'warn', 'WARN', 'ERROR']
report['gateway_log'] = log_lines(a.workdir / 'gateway.log', needles)
report['frontend_log'] = log_lines(a.workdir / 'frontend.log', needles)
(a.workdir / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({k: report[k] for k in ['frontend_metric_deltas', 'gateway_log', 'frontend_log']}, indent=2))
