#!/usr/bin/env python3
"""Exercise conflicting replicas: numbers on one vmstorage, stale on another."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.parse

p = argparse.ArgumentParser()
p.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[3])
p.add_argument('--vm-bin', type=Path, required=True)
p.add_argument('--workdir', type=Path, required=True)
a = p.parse_args()
a.workdir.mkdir(parents=True, exist_ok=False)
sys.path.insert(0, str(a.repo / 'scripts'))
import e2e

facade = a.workdir / 'harness-root'
(facade / '.bin').mkdir(parents=True)
for name in ['vmstorage', 'vminsert', 'vmselect', 'vmtier']:
    source = a.repo / '.bin' if name == 'vmtier' else a.vm_bin
    (facade / '.bin' / name).symlink_to(source / name)
e2e.ROOT = facade
procs = e2e.Processes(a.workdir, 2)
report = {'fixture': 'Two vmstorage nodes: same metric and timestamps; values 1,2 on node 0, stale markers on node 1. Dedup 1ms everywhere.',
          'binary_sha256': {name: hashlib.sha256((facade / '.bin' / name).read_bytes()).hexdigest()
                            for name in ['vmstorage', 'vminsert', 'vmselect', 'vmtier']},
          'candidate_version': subprocess.check_output([str(a.vm_bin / 'vmselect'), '-version'], stderr=subprocess.STDOUT, text=True).strip()}
try:
    cluster = e2e.Cluster(procs, 'source', 2)
    cluster.start('full')
    horizon = e2e.fixture_horizon(int(time.time() * 1000))
    start, end = horizon['start_ms'], horizon['boundary_ms']
    report['start_ms'], report['end_ms'] = start, end
    for index, storage_port in enumerate(cluster.storage_write):
        port = e2e.free_port()
        procs.start(f'direct-insert-{index}', [a.vm_bin / 'vminsert', f'-httpListenAddr=127.0.0.1:{port}',
                    f'-storageNode=127.0.0.1:{storage_port}', '-replicationFactor=1', '-memory.allowedBytes=128MiB'],
                    f'http://127.0.0.1:{port}/health')
        samples = [(start + 30000, 1.0), (start + 60000, 2.0)] if index == 0 else [(start + 30000, e2e.STALE), (start + 60000, e2e.STALE)]
        e2e.remote_write(port, '1:2', [({'__name__': 'replica_conflict'}, samples)])
    cluster.flush()
    params = {'match[]': 'replica_conflict', 'start': start / 1000, 'end': (end - 1) / 1000}
    url = f'http://127.0.0.1:{cluster.select}/select/1:2/prometheus/api/v1/export/native?'
    native = e2e.request(url + urllib.parse.urlencode(params))
    (a.workdir / 'source-native.bin').write_bytes(native)
    report['native_sha256'] = hashlib.sha256(native).hexdigest()
    raw_params = {'query': 'replica_conflict[1h]', 'time': (start + 3600000 - 1) / 1000, 'nocache': '1'}
    report['source_raw'] = e2e.api(cluster.select, '1:2', 'query', raw_params)
    catalog_path = a.workdir / 'catalog.json'
    binary = a.repo / '.bin/vmtier'
    procs.command_stats([binary, 'catalog-init', '-path', catalog_path, '-id', 'collision-probe'], a.workdir / 'init.log')
    config = {'store': {'type': 'local', 'path': str(a.workdir / 'objects')},
              'catalog': {'type': 'local', 'path': str(catalog_path), 'id': 'collision-probe'},
              'tenants': ['1:2'], 'sources': [{'id': 'hot', 'url': f'http://127.0.0.1:{cluster.select}'}],
              'archive_age': f"{horizon['archive_age_ms']}ms", 'hot_retention': '240h', 'lateness': '30m', 'safety_margin': '30m',
              'window': '1h', 'lateness_enforced': True, 'coverage_start_ms': start}
    config_path = a.workdir / 'config.json'
    config_path.write_text(json.dumps(config))
    result = subprocess.run([str(binary), 'archive', '-config', str(config_path), '-start', str(start), '-end', str(end)],
                            capture_output=True, text=True, timeout=60, env=procs.child_environment(None))
    report['archive_exit_code'] = result.returncode
    report['archive_output'] = result.stdout + result.stderr
    (a.workdir / 'archive.log').write_text(report['archive_output'])
    report['catalog'] = json.loads((catalog_path / 'catalog.json').read_text())
    if result.returncode == 0:
        gp, hp, sp = e2e.free_port(), e2e.free_port(), e2e.free_port()
        procs.start('gateway', [binary, 'gateway', '-config', config_path, '-listen', f'127.0.0.1:{gp}',
                               '-http', f'127.0.0.1:{hp}'], f'http://127.0.0.1:{hp}/ready')
        e2e.start_select(procs, 'cold-select', sp, [gp])
        report['cold_raw'] = e2e.api(sp, '1:2', 'query', raw_params)
        e2e.compare(report['source_raw'], report['cold_raw'], 'conflicting replicas raw query')
        report['cold_matches_source'] = True
finally:
    procs.close()
    report['runtime'] = procs.runtime
    (a.workdir / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({k: v for k, v in report.items() if k not in ['runtime', 'catalog']}, indent=2))
