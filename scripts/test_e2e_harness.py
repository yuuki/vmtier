"""Behavioral checks for runtime provenance and real-clock fixture horizons."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from e2e import Processes, fixture_horizon


class RuntimeProvenanceTest(unittest.TestCase):
    def test_selected_runtime_reaches_managed_and_one_shot_children(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'GOMAXPROCS': '7'}):
            work = Path(directory)
            processes = Processes(work, gomaxprocs=3)
            command = [sys.executable, '-c', 'import os; print(os.environ["GOMAXPROCS"])']
            try:
                child = processes.start('daemon', command, env={'GOMAXPROCS': '99'})
                child.wait(timeout=10)
                processes.command_stats(command, work / 'cli.log')
                failing = command[:-1] + [command[-1] + '; raise SystemExit(4)']
                processes.command_stats(failing, work / 'failure.log', expect_success=False)
                with self.assertRaises(AssertionError):
                    processes.command_stats(failing, work / 'unexpected-failure.log')
            finally:
                processes.close()
            for filename in ('daemon.log', 'cli.log', 'failure.log', 'unexpected-failure.log'):
                self.assertEqual((work / filename).read_text().strip(), '3')
            # Check the same serializable record included in successful/failed reports.
            runtime = json.loads(json.dumps(processes.runtime))
            self.assertEqual(runtime['outer_GOMAXPROCS'], '7')
            self.assertEqual(runtime['selected_GOMAXPROCS'], '3')
            self.assertEqual(runtime['selection'], 'argument')
            self.assertEqual([p['exit_code'] for p in runtime['processes']], [0, 0, 4, 4])
            self.assertEqual([p['kind'] for p in runtime['processes']], ['managed', 'one-shot', 'one-shot', 'one-shot'])
            for process in runtime['processes']:
                self.assertEqual(process['environment'], {'GOMAXPROCS': '3'})

    def test_inherited_runtime_and_default_are_explicit(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'GOMAXPROCS': '5'}):
            processes = Processes(Path(directory))
            self.assertEqual(processes.runtime['selection'], 'environment')
            self.assertEqual(processes.child_environment(None)['GOMAXPROCS'], '5')
            del os.environ['GOMAXPROCS']
            processes = Processes(Path(directory))
            self.assertIsNone(processes.runtime['outer_GOMAXPROCS'])
            self.assertEqual(processes.runtime['selection'], 'default')
            self.assertEqual(processes.child_environment(None)['GOMAXPROCS'], '2')

    def test_launch_failure_keeps_provenance_without_environment_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            processes = Processes(Path(directory))
            with self.assertRaises(FileNotFoundError):
                processes.start('missing', ['/nonexistent-vmtier-test-command'], env={'AWS_SECRET_ACCESS_KEY': 'test-secret'})
            record = processes.runtime['processes'][0]
            self.assertIn('launch_error', record)
            self.assertNotIn('test-secret', json.dumps(processes.runtime))


class FixtureHorizonTest(unittest.TestCase):
    def test_hour_crossing_keeps_complete_window_boundary_for_full_horizon(self):
        hour = 3600000
        utc_hour = 1793966400000 // hour * hour
        for offset in (0, 1, hour // 2, hour - 60000, hour - 1):
            with self.subTest(offset=offset):
                now = utc_hour + offset
                fixture = fixture_horizon(now)
                age = fixture['archive_age_ms']
                boundary = fixture['boundary_ms']
                self.assertGreater(age, 30 * 60000)  # H > L
                self.assertEqual(boundary - fixture['start_ms'], 3 * hour)
                self.assertEqual(fixture['end_ms'] - fixture['start_ms'], 5 * hour)
                for elapsed in (0, 1, 60000, 15 * 60000, 30 * 60000 - 1):
                    self.assertEqual((now + elapsed - age) // hour * hour, boundary)
                # No clock override or relaxed coverage: a truly new interval
                # becomes required exactly at the recorded deadline.
                deadline = fixture['next_eligible_at_ms']
                self.assertEqual(deadline - now, 30 * 60000)
                self.assertEqual((deadline - age) // hour * hour, boundary + hour)


if __name__ == '__main__':
    unittest.main()
