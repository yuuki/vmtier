#!/usr/bin/env python3
"""Compare old stock vmselect RPC clients against one current Cold gateway."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import urllib.error

p = argparse.ArgumentParser()
p.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[3])
p.add_argument('--archive-run', type=Path, required=True)
p.add_argument('--workdir', type=Path, required=True)
p.add_argument('vm_bins', nargs='+', type=Path)
a = p.parse_args()
a.workdir.mkdir(parents=True, exist_ok=False)
sys.path.insert(0, str(a.repo / 'scripts'))
import e2e

procs = e2e.Processes(a.workdir, 2)
report = {'scope': 'Cold frontend RPC only; current packs, no historical native export', 'candidates': []}
try:
    cfg = json.loads((a.archive_run / 'config.json').read_text())
    fixture = json.loads((a.archive_run / 'results.json').read_text())['fixture']
    cp, gp, hp = e2e.free_port(), e2e.free_port(), e2e.free_port()
    cfg['catalog']['url'] = f'http://127.0.0.1:{cp}'
    config = a.workdir / 'config.json'
    config.write_text(json.dumps(cfg))
    procs.start('catalog', [a.repo / '.bin/vmtier', 'catalog', '-path', a.archive_run / 'catalog-authority',
                          '-id', cfg['catalog']['id'], '-http', f'127.0.0.1:{cp}'], cfg['catalog']['url'] + '/v1/catalog')
    procs.start('gateway', [a.repo / '.bin/vmtier', 'gateway', '-config', config,
                          '-listen', f'127.0.0.1:{gp}', '-http', f'127.0.0.1:{hp}'], f'http://127.0.0.1:{hp}/ready')
    start, end = fixture['start_ms'] + 600000, fixture['boundary_ms'] - 600000
    tests = [('query', {'query': 'cold_only', 'time': end / 1000}),
             ('query_range', {'query': 'cold_only', 'start': start / 1000, 'end': end / 1000, 'step': '60s'}),
             ('series', {'match[]': 'cold_only', 'start': start / 1000, 'end': end / 1000}),
             ('labels', {'match[]': 'cold_only', 'start': start / 1000, 'end': end / 1000}),
             ('label/__name__/values', {'match[]': 'cold_only', 'start': start / 1000, 'end': end / 1000})]
    baseline = None
    for index, binary_dir in enumerate([a.repo / '.bin', *a.vm_bins]):
        port = e2e.free_port()
        binary = binary_dir / 'vmselect'
        record = {'binary': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                  'version': subprocess.check_output([str(binary), '-version'], stderr=subprocess.STDOUT, text=True).strip(),
                  'checks': []}
        proc = procs.start(f'frontend-{index}', [binary, f'-httpListenAddr=127.0.0.1:{port}',
                          f'-storageNode=127.0.0.1:{gp}', '-replicationFactor=1', '-search.denyPartialResponse=true',
                          '-search.disableCache', '-dedup.minScrapeInterval=1ms', '-memory.allowedBytes=128MiB'],
                          f'http://127.0.0.1:{port}/health')
        results = []
        for i, (path, params) in enumerate(tests):
            try:
                result = e2e.api(port, '1:2', path, params)
                if baseline is not None:
                    e2e.compare(baseline[i], result, path)
                results.append(result)
                record['checks'].append({'api': path, 'status': 'passed'})
            except urllib.error.HTTPError as error:
                record['checks'].append({'api': path, 'status': 'failed', 'http_status': error.code, 'body': error.read().decode()})
            except Exception as error:
                record['checks'].append({'api': path, 'status': 'failed', 'error': str(error)})
        if baseline is None:
            assert len(results) == len(tests)
            assert results[0]['result'], 'baseline must contain Cold data'
            baseline = results
        report['candidates'].append(record)
        procs.stop(proc)
finally:
    procs.close()
    report['runtime'] = procs.runtime
    (a.workdir / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report['candidates'], indent=2))
