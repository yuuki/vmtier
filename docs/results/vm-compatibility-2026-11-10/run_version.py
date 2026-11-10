#!/usr/bin/env python3
"""Run the unmodified repository differential harness with isolated VM binaries."""
import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import traceback

p = argparse.ArgumentParser()
p.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[3])
p.add_argument('--vm-bin', type=Path, required=True)
p.add_argument('--workdir', type=Path, required=True)
p.add_argument('--collision', action='store_true')
p.add_argument('--legacy-flags', action='store_true', help='Omit four flags absent from older VM releases')
p.add_argument('--suite', choices=['json', 'sqlite'], default='json')
a = p.parse_args()
a.repo = a.repo.resolve()
a.vm_bin = a.vm_bin.resolve()
a.workdir = a.workdir.resolve()
a.workdir.mkdir(parents=True, exist_ok=False)
sys.path.insert(0, str(a.repo / 'scripts'))
import e2e

facade = a.workdir / 'harness-root'
(facade / '.bin').mkdir(parents=True)
for name in ['cmd', 'internal', 'go.mod', 'go.sum', 'scripts']:
    (facade / name).symlink_to(a.repo / name)
for name in ['vmstorage', 'vminsert', 'vmselect', 'vmtier']:
    source = a.repo / '.bin' if name == 'vmtier' else a.vm_bin
    (facade / '.bin' / name).symlink_to(source / name)
e2e.ROOT = facade
omitted_flags = []
if a.legacy_flags:
    original_start = e2e.Processes.start
    absent = {'-inmemoryDataFlushInterval', '-globalReplicationFactor',
              '-search.skipSlowReplicas', '-search.maxLabelsAPIDuration'}
    def legacy_start(self, name, command, ready_url=None, env=None):
        filtered = [arg for arg in command if str(arg).split('=')[0] not in absent]
        omitted_flags.extend(str(arg) for arg in command if arg not in filtered)
        return original_start(self, name, filtered, ready_url, env)
    e2e.Processes.start = legacy_start
versions = {name: subprocess.check_output([str(facade / '.bin' / name), '-version'],
                                        stderr=subprocess.STDOUT, text=True).strip()
            for name in ['vmstorage', 'vminsert', 'vmselect']}
if a.collision:
    original_fixture = e2e.fixture
    def collision_fixture(start, boundary, end, tenant_index):
        series = original_fixture(start, boundary, end, tenant_index)
        return [(labels, [(t, value) for t in range(start, end + 1, 30000)
                          for value in (1.0, e2e.STALE, 2.0)])
                if labels['__name__'] == 'duplicates' else (labels, samples)
                for labels, samples in series]
    e2e.fixture = collision_fixture
args = argparse.Namespace(backend='local', replicas=1, gomaxprocs=2,
                          labels_api_duration='5s', measure_warm_metadata=False, pack_max_blocks=4)
if a.suite == 'sqlite':
    import e2e_serve
    harness = e2e_serve
else:
    harness = e2e
failed = False
with (a.workdir / 'harness-output.log').open('w') as output:
    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
        try:
            harness.run(a.workdir, args)
        except Exception:
            failed = True
            traceback.print_exc()
result_path = a.workdir / 'results.json'
report = json.loads(result_path.read_text())
# The original harness hardcodes the supported VM revision. Never retain that
# field as the provenance of a candidate run. Actual binary identities follow.
report.pop('vm_revision', None)
report['candidate_binary_versions'] = versions
report['vmtier_codec_pin'] = 'v1.155.0-cluster/cab3d5fbeeedd5d0374c1dabde93939af4c2499c'
report['vmtier_git_commit'] = subprocess.check_output(
    ['git', '-C', str(a.repo), 'rev-parse', 'HEAD'], text=True).strip()
report['compatibility_driver_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
report['collision_fixture'] = a.collision
report['suite'] = a.suite
report['omitted_legacy_flags'] = sorted(set(omitted_flags))
report['scope'] = 'Experimental compatibility evidence; does not extend the supported version contract.'
result_path.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
print(json.dumps({k: report[k] for k in ['status', 'candidate_binary_versions', 'collision_fixture']}
                 | {'failure': report.get('failure'), 'differential_checks': report.get('differential_checks'),
                    'checks_completed': len(report['checks']), 'results': str(result_path)}, indent=2))
sys.exit(1 if failed else 0)
