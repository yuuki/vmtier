"""Release packaging checks that run without Docker or a Go toolchain."""
import json
import struct
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release  # noqa: E402

GO_VERSION_M = """vmtier: go1.27.3
\tpath\tgithub.com/yuuki/vmtier/cmd/vmtier
\tmod\tgithub.com/yuuki/vmtier\t(devel)\t
\tdep\tgithub.com/VictoriaMetrics/VictoriaMetrics\tv1.155.0-cluster\t
\tdep\tgolang.org/x/sys\tv0.49.0\t
\tbuild\t-tags=netgo,osusergo,musl
\tbuild\tCGO_ENABLED=1
\tbuild\tGOOS=linux
"""


def elf(path, program_types):
    header = bytearray(64)
    header[:6] = b'\x7fELF\x02\x01'
    struct.pack_into('<H', header, 18, 62)
    struct.pack_into('<Q', header, 32, 64)
    struct.pack_into('<HH', header, 54, 56, len(program_types))
    table = b''.join(struct.pack('<I', kind) + bytes(52) for kind in program_types)
    Path(path).write_bytes(bytes(header) + table)


class VersionTest(unittest.TestCase):
    def test_accepts_release_and_prerelease_tags(self):
        for version in ('v0.1.0', 'v1.20.3', 'v0.1.0-rc.1', 'v0.0.0-dev.g29cb29ebcf2e'):
            self.assertEqual(release.validate_version(version), version)

    def test_rejects_ambiguous_versions(self):
        for version in ('0.1.0', 'v0.1', 'v01.0.0', 'v0.1.0+build.1', 'v0.1.0-rc.01', 'v0.1.0-'):
            with self.assertRaises(release.ReleaseError, msg=version):
                release.validate_version(version)

    def test_untagged_commit_gets_dev_version(self):
        commit = '29cb29ebcf2e0d97e325b498203d6951eb8a8e46'
        self.assertEqual(release.default_version(commit, []), 'v0.0.0-dev.g29cb29ebcf2e')
        self.assertEqual(release.default_version(commit, ['latest', 'v0.2.0']), 'v0.2.0')
        with self.assertRaises(release.ReleaseError):
            release.default_version(commit, ['v0.2.0', 'v0.2.1'])


class BuildRecordTest(unittest.TestCase):
    def test_module_cache_escaping_matches_go(self):
        self.assertEqual(release.escape_module_path('github.com/VictoriaMetrics/VictoriaMetrics'),
                         'github.com/!victoria!metrics/!victoria!metrics')

    def test_parses_linked_modules_and_rejects_replacements(self):
        go, main, deps, settings = release.parse_go_version_m(GO_VERSION_M)
        self.assertEqual((go, main), ('go1.27.3', 'github.com/yuuki/vmtier'))
        self.assertEqual([d['path'] for d in deps], ['github.com/VictoriaMetrics/VictoriaMetrics', 'golang.org/x/sys'])
        self.assertEqual(settings['-tags'], 'netgo,osusergo,musl')
        replaced = GO_VERSION_M + '\t=>\t../VictoriaMetrics\t(devel)\t\n'
        with self.assertRaises(release.ReleaseError):
            release.parse_go_version_m(replaced)

    def test_license_record_must_match_linked_set(self):
        _, _, deps, _ = release.parse_go_version_m(GO_VERSION_M)
        record = {'modules': {'github.com/VictoriaMetrics/VictoriaMetrics': 'Apache-2.0', 'golang.org/x/sys': 'BSD-3-Clause'}}
        release.check_license_record(deps, record)
        with self.assertRaisesRegex(release.ReleaseError, 'without a reviewed license: golang.org/x/sys'):
            release.check_license_record(deps, {'modules': {'github.com/VictoriaMetrics/VictoriaMetrics': 'Apache-2.0'}})
        with self.assertRaisesRegex(release.ReleaseError, 'not linked: example.com/old'):
            release.check_license_record(deps, {'modules': {**record['modules'], 'example.com/old': 'MIT'}})

    def test_committed_license_record_is_reviewed_spdx(self):
        record = json.loads((release.ROOT / 'release' / 'licenses.json').read_text())
        self.assertTrue(record['modules'])
        for module, expression in record['modules'].items():
            self.assertRegex(expression, r'^[A-Za-z0-9.()+ -]+$', module)
        notice = release.ROOT / record['toolchain']['musl']['notice']
        self.assertIn(record['toolchain']['musl']['version'], notice.name)
        self.assertIn('Permission is hereby granted', notice.read_text())

    def test_go_sum_hashes_skip_go_mod_entries(self):
        hashes = release.go_sum_hashes('a.example/m v1.0.0 h1:zip=\na.example/m v1.0.0/go.mod h1:mod=\n')
        self.assertEqual(hashes, {('a.example/m', 'v1.0.0'): 'h1:zip='})

    def test_toolchain_record_and_musl_version(self):
        record = release.parse_toolchain('alpine_release=3.24.4\napk=musl-1.2.6-r3\napk=musl-dev-1.2.6-r3\ntags=a,b\n')
        self.assertEqual(record['alpine_release'], '3.24.4')
        self.assertEqual(release.package_version(record['packages'], 'musl'), '1.2.6-r3')
        self.assertEqual(release.package_version(record['packages'], 'musl-dev'), '1.2.6-r3')
        with self.assertRaises(release.ReleaseError):
            release.package_version(record['packages'], 'gcc')


class FileTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)

    def test_notice_files_include_nested_licenses_but_not_hidden_trees(self):
        module = self.root / 'module'
        for name in ('LICENSE', 'NOTICE.txt', 'zstd/COPYING', '.github/workflows/license-check.yml', 'README.md'):
            (module / name).parent.mkdir(parents=True, exist_ok=True)
            (module / name).write_text(name)
        self.assertEqual([p.as_posix() for p in release.notice_files(module)], ['LICENSE', 'NOTICE.txt', 'zstd/COPYING'])
        (module / 'LICENSE').unlink()
        with self.assertRaisesRegex(release.ReleaseError, 'no root license'):
            release.notice_files(module)

    def test_elf_linking_detects_interpreter(self):
        elf(self.root / 'static', [1, 4])
        elf(self.root / 'dynamic', [6, 3, 1, 2])
        self.assertEqual(release.elf_linking(self.root / 'static'), ('amd64', 'static'))
        self.assertEqual(release.elf_linking(self.root / 'dynamic'), ('amd64', 'dynamic'))
        (self.root / 'script').write_text('#!/bin/sh\n')
        with self.assertRaises(release.ReleaseError):
            release.elf_linking(self.root / 'script')

    def test_archive_bytes_ignore_file_times_and_owners(self):
        outputs = []
        for run in ('a', 'b'):
            stage = self.root / f'stage-{run}'
            (stage / 'licenses').mkdir(parents=True)
            (stage / 'vmtier').write_bytes(b'binary')
            (stage / 'vmtier').chmod(0o700)
            (stage / 'licenses' / 'LICENSE').write_text('text')
            (stage / 'licenses' / 'LICENSE').chmod(0o600 if run == 'a' else 0o664)
            output = self.root / f'{run}.tar.gz'
            release.deterministic_tar_gz(stage, 'vmtier-v0.1.0-linux-amd64', output, 1794018042)
            outputs.append(output.read_bytes())
        self.assertEqual(outputs[0], outputs[1])
        with tarfile.open(self.root / 'a.tar.gz') as tar:
            members = {m.name: m for m in tar.getmembers()}
        self.assertEqual(list(members), ['vmtier-v0.1.0-linux-amd64', 'vmtier-v0.1.0-linux-amd64/licenses',
                                         'vmtier-v0.1.0-linux-amd64/licenses/LICENSE', 'vmtier-v0.1.0-linux-amd64/vmtier'])
        self.assertEqual(members['vmtier-v0.1.0-linux-amd64/vmtier'].mode, 0o755)
        self.assertEqual(members['vmtier-v0.1.0-linux-amd64/licenses/LICENSE'].mode, 0o644)
        self.assertTrue(all(m.mtime == 1794018042 and m.uid == 0 and m.uname == '' for m in members.values()))

    def test_checksums_round_trip_and_reject_paths(self):
        (self.root / 'a.tar.gz').write_bytes(b'a')
        release.write_checksums(self.root, ['a.tar.gz'])
        self.assertEqual(release.read_checksums(self.root),
                         {'a.tar.gz': 'ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb'})
        (self.root / 'SHA256SUMS').write_text('0' * 64 + '  ../escape\n')
        with self.assertRaises(release.ReleaseError):
            release.read_checksums(self.root)


class SBOMTest(unittest.TestCase):
    def test_cyclonedx_document_is_deterministic(self):
        build = {'version': 'v0.1.0', 'commit': 'c' * 40, 'source_date_epoch': 1794018042, 'platform': 'linux/amd64',
                 'binary': {'sha256': 'b' * 64}, 'go': {'GOVERSION': 'go1.27.3'},
                 'builder': {'packages': ['musl-1.2.6-r3'], 'alpine_release': '3.24.4'},
                 'modules': [{'path': 'golang.org/x/sys', 'version': 'v0.49.0', 'license': 'BSD-3-Clause', 'sum': 'h1:x='}]}
        licenses = {'toolchain': {'go': {'license': 'BSD-3-Clause'}, 'musl': {'license': 'MIT'}}}
        first, second = release.cyclonedx(build, licenses), release.cyclonedx(build, licenses)
        self.assertEqual(first, second)
        self.assertEqual(first['metadata']['timestamp'], '2026-11-07T02:20:42Z')
        refs = [c['bom-ref'] for c in first['components']]
        self.assertEqual(refs, sorted(refs))
        self.assertIn('pkg:golang/stdlib@1.27.3', refs)
        self.assertIn('pkg:apk/alpine/musl@1.2.6-r3', refs)
        self.assertEqual(first['dependencies'][0]['dependsOn'], refs)


if __name__ == '__main__':
    unittest.main()
