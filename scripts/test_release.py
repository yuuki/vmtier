"""Release packaging checks that run without Docker or a Go toolchain."""
import json
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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

    def test_promoted_candidate_builds_its_final_tag(self):
        commit = '29cb29ebcf2e0d97e325b498203d6951eb8a8e46'
        self.assertEqual(release.default_version(commit, ['v0.1.0-rc.1', 'v0.1.0', 'v0.1.0-rc.2']), 'v0.1.0')
        self.assertEqual(release.default_version(commit, ['v0.1.0-rc.10', 'v0.1.0-rc.9']), 'v0.1.0-rc.10')
        self.assertEqual(release.default_version(commit, ['v0.1.0-rc.1', 'v0.1.0-beta']), 'v0.1.0-rc.1')


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

    def test_committed_license_record_is_valid_spdx(self):
        record = json.loads((release.ROOT / 'release' / 'licenses.json').read_text())
        self.assertTrue(record['modules'])
        for entry in (*record['modules'].values(), *record['embedded'].values(), *record['toolchain'].values()):
            release.validate_spdx(entry['license'])
        for module, entry in record['modules'].items():
            self.assertRegex(entry['notices_sha256'], r'^[0-9a-f]{64}$', module)
        notice = release.ROOT / record['toolchain']['musl']['notice']
        self.assertIn(record['toolchain']['musl']['version'], notice.name)
        self.assertIn('Permission is hereby granted', notice.read_text())
        zstd = record['embedded']['zstd']['notice']
        self.assertIn(zstd['module_version'], zstd['path'])
        self.assertIn('Yuta Mori', (release.ROOT / zstd['path']).read_text())

    def test_spdx_validation_rejects_unknown_or_malformed_expressions(self):
        for good in ('MIT', 'MIT AND (BSD-3-Clause OR GPL-2.0-only)', 'GPL-3.0-or-later WITH GCC-exception-3.1'):
            self.assertEqual(release.validate_spdx(good), good)
        for bad in ('', 'MIT AND', '(MIT', 'MIT)', 'Apache 2.0', 'MIT WITH Classpath-exception-2.0', 'MIT and BSD-3-Clause'):
            with self.assertRaises(release.ReleaseError, msg=bad):
                release.validate_spdx(bad)

    def test_requirement_graph_uses_selected_versions_of_linked_modules(self):
        deps = [{'path': 'a.example/a', 'version': 'v1.1.0'}, {'path': 'b.example/b', 'version': 'v2.0.0'}]
        graph = ('github.com/yuuki/vmtier a.example/a@v1.1.0\n'
                 'a.example/a@v1.1.0 b.example/b@v1.9.0\n'
                 'a.example/a@v1.0.0 c.example/c@v1.0.0\n'
                 'a.example/a@v1.1.0 test.example/only@v1.0.0\n')
        self.assertEqual(release.module_requirements(graph, deps), {'a.example/a': ['b.example/b'], 'b.example/b': []})
        go_mod = 'module m\n\nrequire (\n\ta.example/a v1.1.0\n\tb.example/b v2.0.0 // indirect\n)\nrequire c.example/c v1.0.0\n'
        self.assertEqual(release.direct_requirements(go_mod), {'a.example/a', 'c.example/c'})

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

    def test_notice_digest_changes_when_license_text_changes(self):
        module = self.root / 'module'
        module.mkdir()
        (module / 'LICENSE').write_text('MIT terms')
        files = release.notice_files(module)
        before = release.notices_digest(module, files)
        (module / 'LICENSE').write_text('Business Source License')
        self.assertNotEqual(before, release.notices_digest(module, files))

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

    def make_dist(self, name):
        dist = self.root / name
        dist.mkdir()
        base = 'vmtier-v0.1.0-linux-amd64'
        for suffix in ('.tar.gz', '.build.json', '.cdx.json'):
            (dist / f'{base}{suffix}').write_bytes(suffix.encode())
        release.write_checksums(dist, [f'{base}{s}' for s in ('.tar.gz', '.build.json', '.cdx.json')])
        return dist, base

    def test_verify_requires_exact_files_and_matching_hashes(self):
        dist, base = self.make_dist('dist')
        self.assertEqual(release.verify_dist(dist), base)
        sums = release.sha256_file(dist / 'SHA256SUMS')
        self.assertEqual(release.verify_dist(dist, sums), base)
        with self.assertRaisesRegex(release.ReleaseError, 'reviewed checksum list'):
            release.verify_dist(dist, '0' * 64)
        (dist / 'extra.sh').write_text('#!/bin/sh\n')
        with self.assertRaisesRegex(release.ReleaseError, 'contains'):
            release.verify_dist(dist)
        (dist / 'extra.sh').unlink()
        (dist / f'{base}.tar.gz').write_bytes(b'tampered')
        with self.assertRaisesRegex(release.ReleaseError, 'checksum mismatch'):
            release.verify_dist(dist)
        (dist / 'SHA256SUMS').write_text('')
        with self.assertRaisesRegex(release.ReleaseError, 'one archive'):
            release.verify_dist(dist)

    def test_compare_hashes_files_rather_than_trusting_lists(self):
        first, base = self.make_dist('first')
        second = self.root / 'second'
        shutil.copytree(first, second)
        release.compare(mock.Mock(first=str(first), second=str(second)))
        (second / f'{base}.tar.gz').write_bytes(b'tampered')
        with self.assertRaisesRegex(release.ReleaseError, 'checksum mismatch'):
            release.compare(mock.Mock(first=str(first), second=str(second)))

    def git(self, repo, *args):
        return subprocess.run(['git', '-C', str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()

    def test_export_rejects_files_altered_by_local_git_settings(self):
        repo = self.root / 'repo'
        repo.mkdir()
        self.git(repo, 'init', '-q')
        (repo / 'go.mod').write_text('module m\n')
        (repo / 'NOTICE').write_text('notice\n')
        self.git(repo, 'add', '.')
        self.git(repo, '-c', 'user.name=t', '-c', 'user.email=t@example.com', 'commit', '-qm', 'c')
        commit = self.git(repo, 'rev-parse', 'HEAD')
        with mock.patch.object(release, 'ROOT', repo):
            release.export_commit(commit, self.root / 'clean')
            self.git(repo, 'config', 'core.autocrlf', 'true')
            (repo / '.git' / 'info').mkdir(exist_ok=True)
            (repo / '.git' / 'info' / 'attributes').write_text('NOTICE export-ignore\n')
            with self.assertRaisesRegex(release.ReleaseError, 'missing'):
                release.export_commit(commit, self.root / 'ignored')
            (repo / '.git' / 'info' / 'attributes').write_text('go.mod export-subst\n')
            (repo / 'go.mod').write_text('module m $Format:%H$\n')
            self.git(repo, 'add', 'go.mod')
            self.git(repo, '-c', 'user.name=t', '-c', 'user.email=t@example.com', 'commit', '-qm', 'subst')
            commit = self.git(repo, 'rev-parse', 'HEAD')
            with self.assertRaisesRegex(release.ReleaseError, 'differs from the committed blob'):
                release.export_commit(commit, self.root / 'substituted')

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
                 'builder': {'packages': ['musl-1.2.6-r3', 'libgcc-15.2.0-r5'], 'alpine_release': '3.24.4'},
                 'modules': [{'path': 'github.com/mattn/go-sqlite3', 'version': 'v1.14.53', 'license': 'MIT AND blessing',
                              'sum': 'h1:s=', 'direct': True, 'requires': []},
                             {'path': 'golang.org/x/sys', 'version': 'v0.49.0', 'license': 'BSD-3-Clause',
                              'sum': 'h1:x=', 'direct': False, 'requires': []}],
                 'embedded': {'sqlite': {'module': 'github.com/mattn/go-sqlite3', 'version': '3.54.1',
                                         'license': 'blessing', 'purl': 'pkg:generic/sqlite@3.54.1',
                                         'cpe': 'cpe:2.3:a:sqlite:sqlite:3.54.1:*:*:*:*:*:*:*'}}}
        licenses = {'toolchain': {'go': {'license': 'BSD-3-Clause AND MIT'}, 'musl': {'license': 'MIT'},
                                  'libgcc': {'license': 'GPL-3.0-or-later WITH GCC-exception-3.1'}}}
        first, second = release.cyclonedx(build, licenses), release.cyclonedx(build, licenses)
        self.assertEqual(first, second)
        self.assertEqual(first['metadata']['timestamp'], '2026-11-07T02:20:42Z')
        refs = [c['bom-ref'] for c in first['components']]
        self.assertEqual(refs, sorted(refs))
        self.assertIn('pkg:golang/stdlib@1.27.3', refs)
        self.assertIn('pkg:apk/alpine/musl@1.2.6-r3?arch=x86_64&distro=alpine-3.24.4', refs)
        self.assertIn('pkg:generic/sqlite@3.54.1', refs)
        graph = {d['ref']: d['dependsOn'] for d in first['dependencies']}
        root = graph['pkg:golang/github.com/yuuki/vmtier@v0.1.0']
        self.assertIn('pkg:golang/github.com/mattn/go-sqlite3@v1.14.53', root)
        self.assertNotIn('pkg:golang/golang.org/x/sys@v0.49.0', root)
        self.assertEqual(graph['pkg:golang/github.com/mattn/go-sqlite3@v1.14.53'], ['pkg:generic/sqlite@3.54.1'])
        self.assertEqual(set(graph) - {'pkg:golang/github.com/yuuki/vmtier@v0.1.0'}, set(refs))


if __name__ == '__main__':
    unittest.main()
