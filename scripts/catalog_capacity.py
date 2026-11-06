#!/usr/bin/env python3
"""Publish and select a disposable historical SQLite catalog; no blob I/O."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]


def run_text(command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def optional_text(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go', default=os.environ.get('GO', 'go'))
    parser.add_argument('--gomaxprocs', type=int, default=2)
    parser.add_argument('--hours', type=int, default=8760)
    parser.add_argument('--tenants', type=int, default=10)
    parser.add_argument('--repeats', type=int, default=10)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workdir', type=Path)
    args = parser.parse_args()
    if not 24 <= args.hours <= 8760 or not 1 <= args.tenants <= 100 or not 1 <= args.repeats <= 100 or args.gomaxprocs < 1:
        parser.error('hours: 24..8760; tenants: 1..100; repeats: 1..100; gomaxprocs: positive')
    if args.output.exists():
        parser.error('output exists; use a new path to preserve earlier evidence')
    work = args.workdir.resolve() if args.workdir else Path(tempfile.mkdtemp(prefix='vmtier-catalog-capacity-'))
    work.mkdir(parents=True, exist_ok=True)
    if (work / 'state').exists():
        parser.error('workdir contains catalog state; use a fresh directory')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    binary = work / 'catalog-capacity.test'
    metrics = work / 'metrics.json'
    env = os.environ.copy()
    env.setdefault('GOCACHE', str(ROOT / '.cache' / 'go-build'))
    env.update(GOMAXPROCS=str(args.gomaxprocs), VMTIER_CATALOG_CAPACITY_OUTPUT=str(metrics),
               VMTIER_CATALOG_CAPACITY_HOURS=str(args.hours), VMTIER_CATALOG_CAPACITY_TENANTS=str(args.tenants),
               VMTIER_CATALOG_CAPACITY_REPEATS=str(args.repeats))
    digest = hashlib.sha256()
    for path in sorted([ROOT / 'go.mod', ROOT / 'go.sum', *ROOT.glob('internal/catalog/*.go'), Path(__file__) ]):
        digest.update(str(path.relative_to(ROOT)).encode() + b'\0' + path.read_bytes() + b'\0')
    cpu = next((line.split(':', 1)[1].strip() for line in (optional_text('/proc/cpuinfo') or '').splitlines() if line.startswith('model name')), None)
    report = {
        'schema_version': 1,
        'purpose': 'Historical catalog metadata qualification only; no archived payload, stock VM, or live provider workload.',
        'workdir': str(work),
        'provenance': {'git_head': run_text(['git', 'rev-parse', 'HEAD']),
                       'git_status': run_text(['git', 'status', '--short']),
                       'catalog_sources_and_harness_sha256': digest.hexdigest(),
                       'go_version': run_text([args.go, 'version']),
                       'go_environment': json.loads(run_text([args.go, 'env', '-json', 'CGO_ENABLED', 'GOARCH', 'GOOS', 'CC'])),
                       'compiler_version': run_text(['cc', '--version']).splitlines()[0]},
        'hardware': {'platform': platform.platform(), 'cpu_model': cpu, 'logical_cpus': os.cpu_count(),
                     'cpu_affinity_count': len(os.sched_getaffinity(0)),
                     'mem_total': next((line for line in (optional_text('/proc/meminfo') or '').splitlines() if line.startswith('MemTotal:')), None),
                     'cgroup_memory_max': optional_text('/sys/fs/cgroup/memory.max'),
                     'cgroup_cpu_max': optional_text('/sys/fs/cgroup/cpu.max')},
        'settings': {'GOMAXPROCS': str(args.gomaxprocs), 'GOCACHE': env['GOCACHE'], 'hours': args.hours, 'tenants': args.tenants, 'repeats': args.repeats},
    }
    build = [args.go, 'test', '-mod=vendor', '-p', str(args.gomaxprocs), '-c', '-o', str(binary), './internal/catalog']
    command = [str(binary), '-test.run=^TestSQLiteCatalogCapacityQualification$', '-test.count=1', '-test.timeout=10m', '-test.v']
    report['build_command'] = build
    report['command'] = command
    started = time.monotonic()
    exit_code = 1
    try:
        subprocess.run(build, cwd=ROOT, env=env, check=True)
        report['binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
        with (work / 'run.log').open('w') as log:
            process = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=660)
        exit_code = process.returncode
        if metrics.exists():
            report['measurement'] = json.loads(metrics.read_text())
        report['log'] = (work / 'run.log').read_text()
    finally:
        report['elapsed_seconds_including_build'] = time.monotonic() - started
        report['exit_code'] = exit_code
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
        print(f'evidence: {args.output}; workdir: {work}; exit: {exit_code}', flush=True)
    raise SystemExit(exit_code)


if __name__ == '__main__':
    main()
