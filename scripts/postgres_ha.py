#!/usr/bin/env python3
"""Qualify catalog failures against disposable synchronous PostgreSQL processes.

This explicitly fences/stops the primary and promotes its standby. It does not
test an automatic election controller, a real provider, or a production cluster.
"""
import argparse
import datetime
import hashlib
import json
import os
import shlex
from pathlib import Path
import socket
import subprocess
import time
import traceback


ROOT = Path(__file__).resolve().parent.parent


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def run(command, **kwargs):
    return subprocess.run([str(arg) for arg in command], check=True, **kwargs)


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds')


def output(command, **kwargs):
    return subprocess.check_output([str(arg) for arg in command], text=True,
                                   stderr=subprocess.STDOUT, **kwargs).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--postgres-bin', type=Path, required=True,
                        help='directory containing initdb, pg_ctl, psql and pg_basebackup')
    parser.add_argument('--go', default=os.environ.get('GO', 'go'))
    parser.add_argument('--race', action='store_true', help='build the fault test with Go race detection')
    parser.add_argument('--workdir', type=Path, default=ROOT / '.cache/postgres-ha')
    args = parser.parse_args()
    pg = args.postgres_bin.resolve()
    work = args.workdir.resolve()
    if work.exists() and any(work.iterdir()):
        parser.error('--workdir must be absent or empty; existing state is never removed')
    work.mkdir(parents=True, exist_ok=True)
    (work / 'owned-postgres-ha-fixture').write_text('vmtier disposable PostgreSQL HA fixture\n')
    primary, standby = work / 'primary', work / 'standby'
    sock = work / 'socket'
    environment = os.environ.copy()
    environment.setdefault('GOMAXPROCS', '3')
    environment.setdefault('GOCACHE', str(ROOT / '.cache/go-build'))
    test_binary = work / 'catalog.test'
    started_utc, started = utc_now(), time.monotonic()
    provenance = {'race': args.race}
    report, failure = {}, None
    stage = 'provenance'
    try:
        for executable in ('initdb', 'pg_ctl', 'pg_basebackup', 'psql', 'postgres'):
            if not (pg / executable).is_file():
                raise RuntimeError(f'missing PostgreSQL executable: {pg / executable}')
        provenance.update({
            'git_revision': output(['git', 'rev-parse', 'HEAD'], cwd=ROOT),
            'git_dirty': bool(output(['git', 'status', '--porcelain'], cwd=ROOT)),
            'go_version': output([args.go, 'version'], env=environment),
            'postgres_binary_version': output([pg / 'postgres', '--version']),
            'reproduce_command': shlex.join([
                'python3', 'scripts/postgres_ha.py', '--postgres-bin', str(pg),
                '--go', args.go, *(['--race'] if args.race else []),
                '--workdir', '<new-empty-workdir>']),
            'reproduce_workdir': 'Replace <new-empty-workdir> with an absent or empty directory.',
        })
        stage = 'build'
        with (work / 'build.log').open('w') as log:
            run([args.go, 'test', *(['-race'] if args.race else []),
                 '-mod=vendor', '-c', '-o', test_binary, './internal/catalog'],
                cwd=ROOT, env=environment, stdout=log, stderr=log)
        with test_binary.open('rb') as binary:
            provenance['test_binary_sha256'] = hashlib.file_digest(binary, 'sha256').hexdigest()
        stage = 'setup'
        primary_port, standby_port, replacement_port = free_port(), free_port(), free_port()
        while len({primary_port, standby_port, replacement_port}) != 3:
            replacement_port, standby_port = free_port(), free_port()
        sock.mkdir()
        primary_dsn = f'postgres://vmtier@127.0.0.1:{primary_port}/postgres?sslmode=disable&connect_timeout=2'
        standby_dsn = f'postgres://vmtier@127.0.0.1:{standby_port}/postgres?sslmode=disable&connect_timeout=2'
        with (work / 'setup.log').open('w') as log:
            run([pg / 'initdb', '-D', primary, '--auth=trust', '--username=vmtier',
                 '--encoding=UTF8', '--locale=C', '--no-instructions'], stdout=log, stderr=log)
            with (primary / 'postgresql.conf').open('a') as config:
                config.write(f"\nlisten_addresses='127.0.0.1'\nport={primary_port}\n"
                             f"unix_socket_directories='{sock}'\nmax_connections=40\n"
                             "wal_level=replica\nmax_wal_senders=10\n"
                             "synchronous_commit=on\nsynchronous_standby_names=''\n")
            run([pg / 'pg_ctl', '-D', primary, '-l', work / 'primary.log', '-w', 'start'],
                stdout=log, stderr=log)
            run([pg / 'pg_basebackup', '-d', primary_dsn + '&application_name=ha_standby',
                 '-D', standby, '-R', '-X', 'stream', '-c', 'fast'], stdout=log, stderr=log)
            with (standby / 'postgresql.conf').open('a') as config:
                config.write(f"\nport={standby_port}\n"
                             "synchronous_standby_names='FIRST 1 (ha_standby)'\n")
            run([pg / 'pg_ctl', '-D', standby, '-l', work / 'standby.log', '-w', 'start'],
                stdout=log, stderr=log)
            with (primary / 'postgresql.conf').open('a') as config:
                config.write("\nsynchronous_standby_names='FIRST 1 (ha_standby)'\n")
            run([pg / 'pg_ctl', '-D', primary, 'reload'], stdout=log, stderr=log)
            deadline = time.monotonic() + 20
            while True:
                value = subprocess.check_output(
                    [str(pg / 'psql'), primary_dsn, '-Atc',
                     "SELECT count(*) FROM pg_stat_replication WHERE application_name='ha_standby' AND sync_state='sync' AND state='streaming'"],
                    text=True).strip()
                if value == '1':
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError('synchronous standby did not become ready')
                time.sleep(.1)
        environment.update({
            'VMTIER_HA_PRIMARY_DSN': primary_dsn,
            'VMTIER_HA_STANDBY_DSN': standby_dsn,
            'VMTIER_HA_POSTGRES_BIN': str(pg),
            'VMTIER_HA_WORKDIR': str(work),
            'VMTIER_HA_REPLACEMENT_PORT': str(replacement_port),
        })
        stage = 'test'
        with (work / 'test.log').open('w') as log:
            result = subprocess.run(
                [str(test_binary), '-test.v', '-test.run=^TestPostgresHAFaultQualification$',
                 '-test.timeout=3m'], cwd=ROOT, env=environment,
                stdout=log, stderr=subprocess.STDOUT)
        print((work / 'test.log').read_text())
        if result.returncode:
            raise RuntimeError(f'fault qualification failed; inspect {work}')
        report = json.loads((work / 'results.json').read_text())
        if not report.get('passed'):
            raise RuntimeError('fault test did not produce a successful report')
    except BaseException as error:
        failure = {'stage': stage, 'type': type(error).__name__, 'message': str(error)}
        diagnostic = traceback.format_exc()
        if isinstance(error, subprocess.CalledProcessError) and error.output:
            diagnostic += '\nCommand output:\n' + str(error.output)
        (work / 'failure.log').write_text(diagnostic)
        raise
    finally:
        cleanup = []
        with (work / 'cleanup.log').open('w') as log:
            for name in ('primary', 'standby', 'replacement'):
                data = work / name
                if not (data / 'postmaster.pid').exists():
                    cleanup.append({'node': name, 'status': 'already_stopped_or_not_started'})
                    continue
                try:
                    stopped = subprocess.run(
                        [str(pg / 'pg_ctl'), '-D', str(data), '-m', 'immediate', '-w', 'stop'],
                        stdout=log, stderr=log, timeout=40)
                    cleanup.append({'node': name, 'status': 'stopped' if stopped.returncode == 0 else 'failed',
                                    'exit_code': stopped.returncode})
                except (OSError, subprocess.TimeoutExpired) as error:
                    cleanup.append({'node': name, 'status': 'failed', 'error': str(error)})
        # The Go test may have written partial checks before returning failure.
        # The workdir was required to be fresh, so this cannot replace prior runs.
        if not report and (work / 'results.json').is_file():
            try:
                report = json.loads((work / 'results.json').read_text())
            except (OSError, ValueError) as error:
                report = {'partial_report_error': str(error)}
        try:
            provenance['git_revision_end'] = output(['git', 'rev-parse', 'HEAD'], cwd=ROOT)
            provenance['git_dirty_end'] = bool(output(['git', 'status', '--porcelain'], cwd=ROOT))
        except (OSError, subprocess.CalledProcessError) as error:
            provenance['git_end_error'] = str(error)
        cleanup_failed = any(entry['status'] == 'failed' for entry in cleanup)
        report.update({
            'started_at_utc': started_utc,
            'finished_at_utc': utc_now(),
            'elapsed_seconds': round(time.monotonic() - started, 6),
            'provenance': provenance,
            'cleanup': cleanup,
            'diagnostic_files': sorted(path.name for path in work.glob('*.log')),
        })
        if failure is not None:
            report['failure'] = failure
            report['passed'] = False
        elif cleanup_failed:
            report['failure'] = {'stage': 'cleanup', 'message': 'one or more PostgreSQL processes could not be stopped'}
            report['passed'] = False
        else:
            report.setdefault('passed', False)
        (work / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report, indent=2))
        if cleanup_failed and failure is None:
            raise RuntimeError(f'PostgreSQL fixture cleanup failed; inspect {work / "cleanup.log"}')


if __name__ == '__main__':
    main()
