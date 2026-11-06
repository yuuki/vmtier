"""Check evidence round trips, including failed and never-started suites."""

from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest


SCRIPT = Path(__file__).with_name('package_evidence.py')


class PackageEvidenceTest(unittest.TestCase):
    def run_packager(self, cache_dir):
        subprocess.run([sys.executable, str(SCRIPT), '--cache-dir', str(cache_dir)], check=True)
        return cache_dir / 'e2e-evidence.tar.gz'

    def test_complete_and_partial_evidence_round_trip(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache_dir = root / '.cache'
            files = {
                'ci-local/results.json': b'{"status":"passed"}\n',
                'ci-local/objects-offline/tenants/1:2/attempts/a.json': b'\x00\xffpayload\n',
                'ci-local/.hidden': b'hidden evidence',
                'ci-s3/vmselect.log': b'partial log before suite failure\n',
                'unrelated/not-evidence': b'not part of the artifact',
            }
            for name, contents in files.items():
                path = cache_dir / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(contents)
            # A suite may have made its work directory but produced no files.
            (cache_dir / 'ci-serve-local').mkdir()
            archive_path = self.run_packager(cache_dir)
            self.assertNotIn(':', archive_path.name)
            with tarfile.open(archive_path) as archive:
                names = archive.getnames()
                self.assertIn('ci-serve-local', names)
                self.assertFalse(any(name.startswith('unrelated') for name in names))
                archive.extractall(root / 'extracted', filter='data')
            for name, contents in files.items():
                self.assertEqual((cache_dir / name).read_bytes(), contents)
                if name.startswith('ci-'):
                    self.assertEqual((root / 'extracted' / name).read_bytes(), contents)
            self.assertFalse((cache_dir / 'ci-serve-s3').exists())

    def test_missing_work_directories_produce_uploadable_empty_archive(self):
        with tempfile.TemporaryDirectory() as temporary:
            cache_dir = Path(temporary) / 'missing-cache'
            archive_path = self.run_packager(cache_dir)
            self.assertGreater(archive_path.stat().st_size, 0)
            with tarfile.open(archive_path) as archive:
                self.assertEqual(archive.getnames(), [])
            # Repeated packaging must not include its own prior archive.
            self.run_packager(cache_dir)
            with tarfile.open(archive_path) as archive:
                self.assertEqual(archive.getnames(), [])


if __name__ == '__main__':
    unittest.main()
