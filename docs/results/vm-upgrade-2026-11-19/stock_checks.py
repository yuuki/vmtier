#!/usr/bin/env python3
"""One-node stock VM cluster: capture a native export fixture and check matcher assertions."""
import argparse
import json
from pathlib import Path
import subprocess
import time
import urllib.parse
import urllib.request

p = argparse.ArgumentParser()
p.add_argument('--vm-bin', type=Path, required=True)
p.add_argument('--workdir', type=Path, required=True)
p.add_argument('--metric', default='fixture_156')
p.add_argument('--values', default='[-1.5,4]')
p.add_argument('--day-start', type=int, default=1795046400, help='Unix seconds of the fixture day')
a = p.parse_args()
a.workdir.mkdir(parents=True, exist_ok=False)
base = 18600


def get(url, data=None):
    with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=30) as r:
        return r.read()


procs = []
try:
    def start(name, args):
        log = open(a.workdir / f'{name}.log', 'wb')
        procs.append(subprocess.Popen([str(a.vm_bin / name), *args], stdout=log, stderr=subprocess.STDOUT))

    start('vmstorage', [f'-storageDataPath={a.workdir}/data', f'-httpListenAddr=127.0.0.1:{base + 2}',
                        f'-vminsertAddr=127.0.0.1:{base + 10}', f'-vmselectAddr=127.0.0.1:{base + 11}',
                        '-retentionPeriod=100y'])
    start('vminsert', [f'-httpListenAddr=127.0.0.1:{base}', f'-storageNode=127.0.0.1:{base + 10}'])
    start('vmselect', [f'-httpListenAddr=127.0.0.1:{base + 1}', f'-storageNode=127.0.0.1:{base + 11}',
                       '-search.disableCache', f'-cacheDataPath={a.workdir}/cache'])
    for port in (base, base + 1, base + 2):
        for _ in range(300):
            try:
                get(f'http://127.0.0.1:{port}/health')
                break
            except OSError:
                time.sleep(0.1)
    def series(tenant, match):
        q = urllib.parse.urlencode({'match[]': match, 'start': a.day_start, 'end': a.day_start + 1})
        return json.loads(get(f'http://127.0.0.1:{base + 1}/select/{tenant}/prometheus/api/v1/series?{q}'))['data']

    ms = a.day_start * 1000
    fixture = {'metric': {'__name__': a.metric, 'job': 'codec'}, 'values': json.loads(a.values),
               'timestamps': [ms + 10, ms + 50]}
    lines = [json.dumps(fixture)]
    for _ in range(100):
        try:
            series('42:7', 'up')
            break
        except OSError:
            time.sleep(0.2)
    get(f'http://127.0.0.1:{base}/insert/42:7/prometheus/api/v1/import', '\n'.join(lines).encode())
    matcher_rows = [json.dumps({'metric': {'__name__': 'm', 'a': v}, 'values': [1], 'timestamps': [ms + 10]})
                    for v in ('0', 'ab', 'x')]
    get(f'http://127.0.0.1:{base}/insert/3:4/prometheus/api/v1/import', '\n'.join(matcher_rows).encode())

    for _ in range(100):
        try:
            get(f'http://127.0.0.1:{base + 2}/internal/force_flush')
            if series('42:7', a.metric) and len(series('3:4', 'm')) == 3:
                break
        except OSError:
            pass
        time.sleep(0.5)
    get(f'http://127.0.0.1:{base + 2}/internal/force_flush')
    q = urllib.parse.urlencode({'match[]': a.metric, 'start': f'{a.day_start}.0', 'end': f'{a.day_start}.099'})
    native = get(f'http://127.0.0.1:{base + 1}/select/42:7/prometheus/api/v1/export/native?{q}')
    (a.workdir / 'native.hex').write_text(native.hex() + '\n')
    matcher = {}
    for expr in ['0^', 'a^b', r'x\b', '^0$', r'a\b', r'a\B', '(?m)0$', '0$x']:
        got = {s['a'] for s in series('3:4', 'm{a=~' + json.dumps(expr) + '}')}
        matcher[expr] = ''.join('1' if v in got else '0' for v in ('0', 'ab', 'x'))
    version = subprocess.check_output([str(a.vm_bin / 'vmselect'), '-version'], text=True).strip()
    report = {'vmselect_version': version, 'import': fixture, 'native_bytes': len(native),
              'native_hex': native.hex(), 'matcher_results_over_0_ab_x': matcher}
    (a.workdir / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
finally:
    for proc in procs:
        proc.terminate()
        proc.wait(timeout=15)
