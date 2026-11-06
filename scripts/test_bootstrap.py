"""Source-cache regressions for checksum-pinned stock VM builds."""
import hashlib
import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest


SOURCE_SCRIPT = Path(__file__).resolve().with_name("vm-source.sh")


class StockSourceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        deps = self.root / ".deps"
        deps.mkdir()
        self.archive = deps / "vm-fixture.tar.gz"
        with tarfile.open(self.archive, "w:gz") as archive:
            data = b"module fixture\n"
            entry = tarfile.TarInfo("VictoriaMetrics-fixture/go.mod")
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
        self.checksum = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        self.source = deps / "VictoriaMetrics-fixture"

    def prepare(self, override=None):
        env = os.environ.copy()
        env.pop("VM_SOURCE_DIR", None)
        if override is not None:
            env["VM_SOURCE_DIR"] = str(override)
        return subprocess.run(
            ["bash", "-c", '''
set -euo pipefail
repo_dir=$1
source "$2"
vm_revision=fixture
vm_archive_sha256=$3
vm_source_dir=${VM_SOURCE_DIR:-"$repo_dir/.deps/VictoriaMetrics-$vm_revision"}
prepare_vm_source
''', "test", str(self.root), str(SOURCE_SCRIPT), self.checksum],
            env=env, capture_output=True, text=True,
        )

    def test_replaces_unverified_generated_source(self):
        self.source.mkdir()
        (self.source / "go.mod").write_text("module stale\n")
        stale = self.source / "stale-file"
        stale.touch()
        result = self.prepare()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.source / "go.mod").read_text(), "module fixture\n")
        self.assertFalse(stale.exists())
        self.assertEqual(self.prepare().returncode, 0)

    def test_rejects_corrupt_cached_archive(self):
        self.archive.write_bytes(b"corrupt")
        result = self.prepare()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.source.exists())

    def test_does_not_relabel_or_delete_unverified_override(self):
        custom = self.root / "custom-source"
        custom.mkdir()
        (custom / "go.mod").write_text("module other\n")
        result = self.prepare(custom)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not a verified pinned source", result.stderr)
        self.assertEqual((custom / "go.mod").read_text(), "module other\n")

    def test_populates_and_reuses_new_override(self):
        custom = self.root / "custom-source"
        for _ in range(2):
            result = self.prepare(custom)
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((custom / "go.mod").read_text(), "module fixture\n")


if __name__ == "__main__":
    unittest.main()
