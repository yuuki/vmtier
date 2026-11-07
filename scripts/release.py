#!/usr/bin/env python3
"""Build, package, and check reproducible vmtier release artifacts.

`build` exports one commit with `git archive`, compiles it in the digest-pinned
container from release/Dockerfile, and writes a deterministic archive, CycloneDX
SBOM, build record, and SHA256SUMS. `verify` checks a release directory without
running anything from it, `compare` checks two independent builds, and `smoke`
also exercises the packaged binary. Only the standard library is required.
"""
import argparse
import gzip
import hashlib
import io
import json
import os
import re
import shlex
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE = 'github.com/yuuki/vmtier'
REPOSITORY = 'https://github.com/yuuki/vmtier'
PLATFORM = ('linux', 'amd64')
BUILD_SCHEMA = 'vmtier-build/v1'
SEMVER = re.compile(r'^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)'
                    r'(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?$')
DEV_VERSION = re.compile(r'^v0\.0\.0-dev\.g[0-9a-f]{12}$')
NOTICE_NAME = re.compile(r'^(licen[cs]e|copying|notice|patents)([.-][a-z0-9.-]*)?$', re.IGNORECASE)
ROOT_LICENSE = re.compile(r'^(licen[cs]e|copying)([.-][a-z0-9.-]*)?$', re.IGNORECASE)
TOP_LEVEL_FILES = ('LICENSE', 'NOTICE', 'THIRD_PARTY_NOTICES.md', 'README.md')
TOOLCHAIN_NOTICES = {'licenses/go/LICENSE': 'go-LICENSE', 'licenses/go/PATENTS': 'go-PATENTS',
                     'licenses/go/runtime-memmove_amd64.s-NOTICE': 'go-memmove_amd64-NOTICE'}
MUSL_NOTICE = 'licenses/musl/COPYRIGHT'
EXAMPLE = 'examples/serve-sakura.json'
REPRODUCIBILITY_SCOPE = (
    'Identical bytes require the same commit, release/Dockerfile base digest, the Alpine package '
    'versions listed under builder.packages, and the module versions in go.sum. Alpine mirrors keep '
    'only current package releases, so a later rebuild can need the recorded packages from an archive. '
    'Archive bytes also depend on the zlib used by the packaging Python; compare binary.sha256 when '
    'rebuilding elsewhere.')


class ReleaseError(Exception):
    pass


def run(cmd, cwd=None, env=None, capture=False):
    printable = ' '.join(shlex.quote(str(part)) for part in cmd)
    print(f'+ {printable}', file=sys.stderr, flush=True)
    result = subprocess.run([str(part) for part in cmd], cwd=cwd, env=env, check=False,
                            stdout=subprocess.PIPE if capture else None, text=True)
    if result.returncode != 0:
        raise ReleaseError(f'command failed with status {result.returncode}: {printable}')
    return result.stdout if capture else ''


def git(*args):
    return run(['git', *args], cwd=ROOT, capture=True).strip()


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def validate_version(version):
    if not SEMVER.match(version):
        raise ReleaseError(f'version {version!r} is not a v-prefixed SemVer without build metadata')
    return version


def semver_key(version):
    """Sort key implementing SemVer precedence for validated versions."""
    match = SEMVER.match(validate_version(version))
    core = tuple(int(match.group(i)) for i in (1, 2, 3))
    if match.group(4) is None:
        return core, (1,)
    ids = tuple((0, int(i), '') if i.isdigit() else (1, 0, i) for i in match.group(4).split('.'))
    return core, (0, ids)


def default_version(commit, tags):
    """The highest-precedence release tag on HEAD (a promoted candidate keeps its final tag)."""
    released = [tag for tag in tags if SEMVER.match(tag)]
    return max(released, key=semver_key) if released else f'v0.0.0-dev.g{commit[:12]}'


def escape_module_path(path):
    """Apply Go's module cache case encoding (golang.org/x/mod/module.EscapePath)."""
    return ''.join('!' + c.lower() if 'A' <= c <= 'Z' else c for c in path)


def parse_go_version_m(text):
    """Return (go version, main module path, linked deps, build settings) from `go version -m`."""
    lines = text.splitlines()
    if not lines or ': ' not in lines[0]:
        raise ReleaseError('unexpected go version -m output')
    go_version = lines[0].rsplit(': ', 1)[1].strip()
    main, deps, settings = None, [], {}
    for line in lines[1:]:
        fields = line.strip('\t').split('\t')
        kind = fields[0]
        if kind == 'mod':
            main = fields[1]
        elif kind == 'dep':
            deps.append({'path': fields[1], 'version': fields[2], 'sum': fields[3] if len(fields) > 3 else ''})
        elif kind == '=>':
            raise ReleaseError(f'module replacement is not allowed in release builds: {line.strip()}')
        elif kind == 'build':
            key, _, value = fields[1].partition('=')
            settings[key] = value
    if main is None:
        raise ReleaseError('go version -m did not report the main module')
    return go_version, main, deps, settings


def go_sum_hashes(text):
    """Module content hashes from go.sum; vendor builds do not stamp them into the binary."""
    hashes = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) == 3 and not fields[1].endswith('/go.mod'):
            hashes[(fields[0], fields[1])] = fields[2]
    return hashes


def parse_toolchain(text):
    record = {'packages': []}
    for line in text.splitlines():
        key, sep, value = line.partition('=')
        if not sep:
            continue
        if key == 'apk':
            record['packages'].append(value)
        else:
            record[key] = value
    return record


def package_version(packages, name):
    for package in packages:
        if package.startswith(name + '-') and re.match(r'\d', package[len(name) + 1:]):
            return package[len(name) + 1:]
    raise ReleaseError(f'builder package {name} was not recorded')


# SPDX identifiers that release/licenses.json may use. Extend deliberately after review.
SPDX_LICENSES = {'0BSD', 'Apache-2.0', 'BSD-2-Clause', 'BSD-3-Clause', 'GPL-2.0-only', 'GPL-3.0-or-later',
                 'ISC', 'MIT', 'MPL-2.0', 'blessing'}
SPDX_EXCEPTIONS = {'GCC-exception-3.1'}


def validate_spdx(expression):
    """Accept AND/OR/WITH expressions over SPDX_LICENSES, with balanced parentheses."""
    tokens = re.findall(r'\(|\)|[^\s()]+', expression)
    pos = 0

    def peek():
        return tokens[pos] if pos < len(tokens) else None

    def term():
        nonlocal pos
        token = peek()
        if token == '(':
            pos += 1
            expr()
            if peek() != ')':
                raise ReleaseError(f'unbalanced SPDX expression {expression!r}')
            pos += 1
        elif token in SPDX_LICENSES:
            pos += 1
            if peek() == 'WITH':
                pos += 1
                if peek() not in SPDX_EXCEPTIONS:
                    raise ReleaseError(f'unknown SPDX exception in {expression!r}')
                pos += 1
        else:
            raise ReleaseError(f'unknown or misplaced SPDX token {token!r} in {expression!r}')

    def expr():
        nonlocal pos
        term()
        while peek() in ('AND', 'OR'):
            pos += 1
            term()

    expr()
    if pos != len(tokens):
        raise ReleaseError(f'trailing tokens in SPDX expression {expression!r}')
    return expression


def notices_digest(module_dir, files):
    """Digest of a module's license files, so a version bump that relicenses is noticed."""
    digest = hashlib.sha256()
    for rel in sorted(files, key=lambda r: r.as_posix()):
        digest.update(f'{rel.as_posix()}\0{sha256_file(Path(module_dir) / rel)}\n'.encode())
    return digest.hexdigest()


def direct_requirements(go_mod):
    """Module paths required by go.mod without an // indirect comment."""
    direct, block = set(), False
    for line in go_mod.splitlines():
        line = line.strip()
        if line == 'require (':
            block = True
        elif block and line == ')':
            block = False
        elif (block or line.startswith('require ')) and '// indirect' not in line:
            fields = line.removeprefix('require ').split()
            if len(fields) >= 2:
                direct.add(fields[0])
    return direct


def module_requirements(graph, deps):
    """Requirement edges between linked modules at their selected versions, from `go mod graph`."""
    selected = {d['path']: d['version'] for d in deps}
    requires = {path: set() for path in selected}
    for line in graph.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        source, target = (field.rpartition('@') if '@' in field else (field, '', '') for field in fields)
        if source[0] in selected and source[2] == selected[source[0]] and target[0] in selected:
            requires[source[0]].add(target[0])
    return {path: sorted(targets) for path, targets in requires.items()}


def embedded_versions(module_dir_of):
    """Versions of C libraries compiled into linked Go modules."""
    sqlite = (module_dir_of('github.com/mattn/go-sqlite3') / 'sqlite3-binding.h').read_text()
    zstd = (module_dir_of('github.com/valyala/gozstd') / 'zstd' / 'lib' / 'zstd.h').read_text()
    parts = [re.search(rf'#define ZSTD_VERSION_{name}\s+(\d+)', zstd) for name in ('MAJOR', 'MINOR', 'RELEASE')]
    match = re.search(r'#define SQLITE_VERSION\s+"([0-9.]+)"', sqlite)
    if not match or not all(parts):
        raise ReleaseError('cannot determine embedded SQLite or zstd version')
    return {'sqlite': match.group(1), 'zstd': '.'.join(p.group(1) for p in parts)}


def check_license_record(deps, record):
    known = record['modules']
    linked = {dep['path'] for dep in deps}
    missing = sorted(linked - known.keys())
    stale = sorted(known.keys() - linked)
    problems = []
    if missing:
        problems.append('linked modules without a reviewed license: ' + ', '.join(missing))
    if stale:
        problems.append('license record lists modules that are not linked: ' + ', '.join(stale))
    if problems:
        raise ReleaseError('; '.join(problems) + '; update release/licenses.json after review')


def notice_files(module_dir):
    """License-like files of one module, excluding hidden trees such as .github."""
    found = []
    for path in sorted(Path(module_dir).rglob('*')):
        rel = path.relative_to(module_dir)
        if path.is_file() and NOTICE_NAME.match(path.name) and not any(p.startswith('.') for p in rel.parts):
            found.append(rel)
    if not any(len(rel.parts) == 1 and ROOT_LICENSE.match(rel.name) for rel in found):
        raise ReleaseError(f'no root license file in {module_dir}')
    return found


def elf_linking(path):
    """Classify an ELF64 little-endian executable as static or dynamic from its program headers."""
    with open(path, 'rb') as f:
        header = f.read(64)
        if len(header) < 64 or header[:4] != b'\x7fELF' or header[4] != 2 or header[5] != 1:
            raise ReleaseError(f'{path} is not a little-endian ELF64 file')
        machine, = struct.unpack_from('<H', header, 18)
        phoff, = struct.unpack_from('<Q', header, 32)
        phentsize, phnum = struct.unpack_from('<HH', header, 54)
        f.seek(phoff)
        table = f.read(phentsize * phnum)
    types = {struct.unpack_from('<I', table, i * phentsize)[0] for i in range(phnum)}
    linking = 'dynamic' if types & {2, 3} else 'static'  # PT_DYNAMIC, PT_INTERP
    return {62: 'amd64'}.get(machine, f'machine-{machine}'), linking


def cyclonedx(build, licenses):
    version = build['version']
    main_ref = f'pkg:golang/{MODULE}@{version}'
    ref = {m['path']: f"pkg:golang/{m['path']}@{m['version']}" for m in build['modules']}
    components = [{'type': 'library', 'bom-ref': ref[m['path']], 'name': m['path'],
                   'version': m['version'], 'purl': ref[m['path']],
                   'licenses': [{'expression': m['license']}],
                   'properties': [{'name': 'vmtier:go.sum', 'value': m['sum']}]} for m in build['modules']]
    edges = {ref[m['path']]: [ref[r] for r in m['requires']] for m in build['modules']}
    roots = [ref[m['path']] for m in build['modules'] if m['direct']]
    for name, c in sorted(build['embedded'].items()):
        components.append({'type': 'library', 'bom-ref': c['purl'], 'name': name, 'version': c['version'],
                           'purl': c['purl'], 'cpe': c['cpe'], 'licenses': [{'expression': c['license']}],
                           'description': f"C library compiled into {c['module']}"})
        edges[ref[c['module']]].append(c['purl'])
        edges[c['purl']] = []
    go = build['go']['GOVERSION']
    distro = f"alpine-{build['builder']['alpine_release']}"
    toolchain = [('stdlib', 'golang', f'pkg:golang/stdlib@{go.removeprefix("go")}', go, licenses['toolchain']['go'],
                  'Go runtime and standard library')]
    for name in ('musl', 'libgcc'):
        apk = package_version(build['builder']['packages'], name)
        toolchain.append((name, 'apk', f'pkg:apk/alpine/{name}@{apk}?arch=x86_64&distro={distro}', apk,
                          licenses['toolchain'][name], 'statically linked by the release builder'))
    for name, _, purl, component_version, record, description in toolchain:
        components.append({'type': 'library', 'bom-ref': purl, 'name': name, 'version': component_version,
                           'purl': purl, 'licenses': [{'expression': record['license']}], 'description': description})
        edges[purl] = []
        roots.append(purl)
    components.sort(key=lambda c: c['bom-ref'])
    serial = uuid.uuid5(uuid.NAMESPACE_URL, f"{REPOSITORY}/releases/{version}/{build['binary']['sha256']}")
    timestamp = datetime.fromtimestamp(build['source_date_epoch'], timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    return {
        'bomFormat': 'CycloneDX', 'specVersion': '1.6', 'serialNumber': f'urn:uuid:{serial}', 'version': 1,
        'metadata': {
            'timestamp': timestamp,
            'tools': {'components': [{'type': 'application', 'name': 'vmtier scripts/release.py',
                                      'version': build['commit']}]},
            'component': {
                'type': 'application', 'bom-ref': main_ref, 'name': 'vmtier', 'version': version,
                'purl': main_ref, 'licenses': [{'expression': 'Apache-2.0'}],
                'hashes': [{'alg': 'SHA-256', 'content': build['binary']['sha256']}],
                'externalReferences': [{'type': 'vcs', 'url': REPOSITORY}],
                'properties': [{'name': 'vmtier:commit', 'value': build['commit']},
                               {'name': 'vmtier:platform', 'value': build['platform']}],
            },
        },
        'components': components,
        'dependencies': [{'ref': main_ref, 'dependsOn': sorted(roots)}] +
                        [{'ref': r, 'dependsOn': sorted(edges[r])} for r in sorted(edges)],
    }


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def deterministic_tar_gz(source_dir, prefix, output, mtime):
    """Archive source_dir under prefix/ with sorted names, fixed owners, modes, and times."""
    source_dir = Path(source_dir)
    paths = sorted(source_dir.rglob('*'), key=lambda p: p.relative_to(source_dir).as_posix())
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w', format=tarfile.PAX_FORMAT) as tar:
        def add(info, data=None):
            info.mtime, info.uid, info.gid, info.uname, info.gname = mtime, 0, 0, '', ''
            tar.addfile(info, data)
        root = tarfile.TarInfo(prefix)
        root.type, root.mode = tarfile.DIRTYPE, 0o755
        add(root)
        for path in paths:
            info = tarfile.TarInfo(f'{prefix}/{path.relative_to(source_dir).as_posix()}')
            if path.is_symlink():
                raise ReleaseError(f'symlink in release payload: {path}')
            if path.is_dir():
                info.type, info.mode = tarfile.DIRTYPE, 0o755
                add(info)
            else:
                info.size = path.stat().st_size
                info.mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
                with open(path, 'rb') as f:
                    add(info, f)
    with open(output, 'wb') as f:
        with gzip.GzipFile(filename='', mode='wb', fileobj=f, compresslevel=9, mtime=0) as gz:
            gz.write(raw.getvalue())


def write_checksums(dist, names):
    lines = [f'{sha256_file(Path(dist) / name)}  {name}\n' for name in sorted(names)]
    (Path(dist) / 'SHA256SUMS').write_text(''.join(lines))


def read_checksums(dist):
    entries = {}
    for line in (Path(dist) / 'SHA256SUMS').read_text().splitlines():
        digest, _, name = line.partition('  ')
        if not re.fullmatch(r'[0-9a-f]{64}', digest) or not name or '/' in name:
            raise ReleaseError(f'malformed SHA256SUMS line: {line!r}')
        entries[name] = digest
    return entries


def artifact_base(version):
    return f'vmtier-{version}-{PLATFORM[0]}-{PLATFORM[1]}'


def docker_extra_args():
    # Environment-specific network or proxy settings; they must not change outputs.
    return shlex.split(os.environ.get('VMTIER_RELEASE_DOCKER_RUN_ARGS', ''))


def export_commit(commit, src):
    """Extract `git archive` of commit and require every file to match its blob in the commit."""
    # Local config or info/attributes can rewrite or drop exported files; detect that below.
    cmd = ['git', '-c', 'core.autocrlf=false', '-c', 'core.eol=lf', 'archive', '--format=tar', commit]
    with subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE) as proc:
        with tarfile.open(fileobj=proc.stdout, mode='r|') as tar:
            tar.extractall(src, filter='data')
    if proc.returncode != 0:
        raise ReleaseError('git archive failed')
    algorithm = git('rev-parse', '--show-object-format')
    expected = {}
    for entry in run(['git', 'ls-tree', '-r', '-z', commit], cwd=ROOT, capture=True).split('\0'):
        if entry:
            meta, _, path = entry.partition('\t')
            mode, kind, obj = meta.split()
            if kind != 'blob':
                raise ReleaseError(f'unsupported tree entry {kind} {path}')
            expected[path] = (mode, obj)
    found = {p.relative_to(src).as_posix() for p in Path(src).rglob('*') if p.is_file() or p.is_symlink()}
    if found != expected.keys():
        raise ReleaseError(f'exported tree differs from {commit[:12]}: missing {sorted(expected.keys() - found)}, '
                           f'extra {sorted(found - expected.keys())}')
    for path, (mode, obj) in expected.items():
        file = Path(src) / path
        data = os.readlink(file).encode() if mode == '120000' else file.read_bytes()
        if hashlib.new(algorithm, b'blob %d\0' % len(data) + data).hexdigest() != obj:
            raise ReleaseError(f'exported {path} differs from the committed blob; check git attributes and filters')


def build(args):
    commit = git('rev-parse', 'HEAD')
    tags = git('tag', '--points-at', 'HEAD').split()
    version = validate_version(args.version or default_version(commit, tags))
    if DEV_VERSION.match(version):
        if version != f'v0.0.0-dev.g{commit[:12]}':
            raise ReleaseError(f'{version} does not name HEAD {commit[:12]}')
    elif version not in tags:
        raise ReleaseError(f'{version} is not a tag on HEAD; tag the commit or omit --version for a dev build')
    # The packager itself runs from the working tree, so it must match the commit too.
    if git('status', '--porcelain', '--untracked-files=no'):
        raise ReleaseError('tracked files have uncommitted changes; commit or stash them first')
    source_date_epoch = int(git('log', '-1', '--format=%ct', 'HEAD'))
    dist = Path(args.output).resolve()
    if dist.exists() and any(dist.iterdir()):
        raise ReleaseError(f'output directory is not empty: {dist}')
    dist.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix='vmtier-release-', dir=args.workdir))
    try:
        src, out, modcache = work / 'src', work / 'out', work / 'gomodcache'
        for d in (src, out, modcache):
            d.mkdir()
        # Only committed files reach the builder; untracked or modified files cannot leak in.
        export_commit(commit, src)
        dockerfile = src / 'release' / 'Dockerfile'
        base_image = next(line.split()[1] for line in dockerfile.read_text().splitlines() if line.startswith('FROM '))
        if '@sha256:' not in base_image:
            raise ReleaseError('release/Dockerfile base image must be pinned by digest')
        image = args.builder_image
        if image is not None:
            # Not recorded: an equivalent image must produce identical outputs, and
            # builder.packages, cc, and ld show any difference.
            print(f'warning: using builder image {image} instead of release/Dockerfile', file=sys.stderr)
        else:
            image = f'vmtier-release-builder:{sha256_file(dockerfile)[:16]}'
            run(['docker', 'build', '--pull', '-t', image, '-f', dockerfile, src / 'release'])
        env = {'HOME': '/tmp', 'GOCACHE': '/tmp/go-build', 'GOMODCACHE': '/gomodcache',
               'VMTIER_VERSION': version, 'VMTIER_COMMIT': commit,
               'VMTIER_RELEASE_UNIT_TESTS': '1' if args.unit_tests else '0',
               'GOMAXPROCS': os.environ.get('GOMAXPROCS', '3')}
        cmd = ['docker', 'run', '--rm', '--user', f'{os.getuid()}:{os.getgid()}',
               '-v', f'{src}:/src', '-v', f'{out}:/out', '-v', f'{modcache}:/gomodcache', *docker_extra_args()]
        for key, value in env.items():
            cmd += ['-e', f'{key}={value}']
        run([*cmd, image, 'sh', '/src/release/build.sh'])
        package(version, commit, source_date_epoch, base_image, src, out, modcache, work, dist)
    finally:
        if args.keep_work:
            print(f'kept work directory {work}', file=sys.stderr)
        else:
            shutil.rmtree(work, ignore_errors=True)
    print(dist)


def package(version, commit, source_date_epoch, base_image, src, out, modcache, work, dist):
    licenses = json.loads((src / 'release' / 'licenses.json').read_text())
    for record in (*licenses['modules'].values(), *licenses['embedded'].values(), *licenses['toolchain'].values()):
        validate_spdx(record['license'])
    go_version, main_module, deps, settings = parse_go_version_m((out / 'go-version-m.txt').read_text())
    toolchain = parse_toolchain((out / 'toolchain.txt').read_text())
    go_env = json.loads((out / 'go-env.json').read_text())
    if main_module != MODULE or go_version != go_env['GOVERSION']:
        raise ReleaseError(f'unexpected binary identity {main_module} {go_version}')
    if (settings.get('GOOS'), settings.get('GOARCH'), settings.get('CGO_ENABLED')) != (*PLATFORM, '1'):
        raise ReleaseError(f'unexpected build settings {settings}')
    machine, linking = elf_linking(out / 'vmtier')
    if (machine, linking) != ('amd64', 'static'):
        raise ReleaseError(f'release binary is {machine}/{linking}, want amd64/static')
    check_license_record(deps, licenses)
    sums = go_sum_hashes((src / 'go.sum').read_text())
    for dep in deps:
        dep['sum'] = dep['sum'] or sums.get((dep['path'], dep['version']), '')
        if not dep['sum'].startswith('h1:'):
            raise ReleaseError(f"no go.sum module hash for {dep['path']} {dep['version']}")
    selected = {dep['path']: dep['version'] for dep in deps}

    def module_dir_of(path):
        return modcache / f'{escape_module_path(path)}@{escape_module_path(selected[path])}'

    versions = embedded_versions(module_dir_of)
    if versions.keys() != licenses['embedded'].keys():
        raise ReleaseError(f"embedded C libraries {sorted(versions)} differ from release/licenses.json")
    embedded = {}
    for name, record in licenses['embedded'].items():
        if record['module'] not in selected:
            raise ReleaseError(f"{name} is recorded in {record['module']}, which is not linked")
        notice = record.get('notice')
        if notice and selected[record['module']] != notice['module_version']:
            raise ReleaseError(f"{notice['path']} was reviewed for {record['module']} {notice['module_version']}, "
                               f"but {selected[record['module']]} is linked; refresh the notice after review")
        v = versions[name]
        embedded[name] = {'module': record['module'], 'version': v, 'license': record['license'],
                          'purl': record['purl'].format(version=v), 'cpe': record['cpe'].format(version=v)}
    musl = package_version(toolchain['packages'], 'musl')
    if musl.split('-r')[0] != licenses['toolchain']['musl']['version']:
        raise ReleaseError(f"builder musl {musl} differs from the recorded notice "
                           f"{licenses['toolchain']['musl']['notice']}; refresh it after review")

    base = artifact_base(version)
    stage = work / 'stage'
    stage.mkdir()
    shutil.copy2(out / 'vmtier', stage / 'vmtier')
    for name in TOP_LEVEL_FILES:
        shutil.copyfile(src / name, stage / name)
    (stage / 'examples').mkdir()
    shutil.copyfile(src / EXAMPLE, stage / EXAMPLE)
    shutil.copyfile(out / 'go-version-m.txt', stage / 'go-version-m.txt')
    for target, name in TOOLCHAIN_NOTICES.items():
        (stage / target).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(out / name, stage / target)
    (stage / MUSL_NOTICE).parent.mkdir(parents=True)
    shutil.copyfile(src / licenses['toolchain']['musl']['notice'], stage / MUSL_NOTICE)
    direct = direct_requirements((src / 'go.mod').read_text())
    requires = module_requirements((out / 'go-mod-graph.txt').read_text(), deps)
    modules, changed = [], {}
    for dep in deps:
        module_dir = module_dir_of(dep['path'])
        files = notice_files(module_dir)
        digest = notices_digest(module_dir, files)
        reviewed = licenses['modules'][dep['path']]
        if reviewed.get('notices_sha256') != digest:
            changed[dep['path']] = {'license': reviewed['license'], 'notices_sha256': digest}
        for rel in files:
            target = stage / 'licenses' / dep['path'] / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(module_dir / rel, target)
        notices = [f"licenses/{dep['path']}/{rel.as_posix()}" for rel in files]
        for record in licenses['embedded'].values():
            if record['module'] == dep['path'] and record.get('notice'):
                target = f"licenses/{dep['path']}/{Path(record['notice']['path']).name}"
                shutil.copyfile(src / record['notice']['path'], stage / target)
                notices.append(target)
        modules.append({**dep, 'license': reviewed['license'], 'notices': notices, 'notices_sha256': digest,
                        'direct': dep['path'] in direct, 'requires': requires[dep['path']]})
    if changed:
        raise ReleaseError('license files changed since review; read them, then update release/licenses.json: '
                           + json.dumps(changed, indent=2, sort_keys=True))

    build_record = {
        'schema': BUILD_SCHEMA, 'version': version, 'commit': commit, 'source_date_epoch': source_date_epoch,
        'repository': REPOSITORY, 'platform': '/'.join(PLATFORM),
        'binary': {'path': 'vmtier', 'sha256': sha256_file(out / 'vmtier'), 'size': (out / 'vmtier').stat().st_size,
                   'linking': linking},
        'builder': {'dockerfile': 'release/Dockerfile', 'dockerfile_sha256': sha256_file(src / 'release/Dockerfile'),
                    'script': 'release/build.sh', 'script_sha256': sha256_file(src / 'release/build.sh'),
                    'base_image': base_image,
                    'alpine_release': toolchain['alpine_release'], 'packages': toolchain['packages'],
                    'cc': toolchain['cc'], 'ld': toolchain['ld']},
        'go': go_env,
        'build': {'command': toolchain['command'], 'tags': toolchain['tags'].split(','), 'ldflags': toolchain['ldflags'],
                  'settings': settings},
        'modules': modules,
        'embedded': embedded,
        'notices': sorted([*TOOLCHAIN_NOTICES, MUSL_NOTICE]),
        'reproducibility': REPRODUCIBILITY_SCOPE,
    }
    sbom = cyclonedx(build_record, licenses)
    write_json(stage / 'BUILD.json', build_record)
    write_json(stage / 'sbom.cdx.json', sbom)

    deterministic_tar_gz(stage, base, dist / f'{base}.tar.gz', source_date_epoch)
    write_json(dist / f'{base}.build.json', build_record)
    write_json(dist / f'{base}.cdx.json', sbom)
    write_checksums(dist, [f'{base}.tar.gz', f'{base}.build.json', f'{base}.cdx.json'])


def verify_dist(dist, sums_sha256=None):
    """Check a release directory without executing anything from it; return its artifact base name."""
    dist = Path(dist)
    if sums_sha256 is not None and sha256_file(dist / 'SHA256SUMS') != sums_sha256:
        raise ReleaseError(f'{dist}/SHA256SUMS is not the reviewed checksum list {sums_sha256}')
    sums = read_checksums(dist)
    archives = [name for name in sums if name.endswith('.tar.gz')]
    if len(archives) != 1:
        raise ReleaseError(f'expected one archive in SHA256SUMS, found {archives}')
    base = archives[0].removesuffix('.tar.gz')
    expected = {f'{base}.tar.gz', f'{base}.build.json', f'{base}.cdx.json'}
    if sums.keys() != expected:
        raise ReleaseError(f'SHA256SUMS lists {sorted(sums)}, want {sorted(expected)}')
    present = {p.name for p in dist.iterdir()}
    if present != expected | {'SHA256SUMS'}:
        raise ReleaseError(f'{dist} contains {sorted(present)}, want exactly SHA256SUMS and {sorted(expected)}')
    for name, digest in sums.items():
        if sha256_file(dist / name) != digest:
            raise ReleaseError(f'checksum mismatch for {name}')
    return base


def verify(args):
    base = verify_dist(args.dist, args.sums_sha256)
    print(f'verified: {base} ({sha256_file(Path(args.dist) / "SHA256SUMS")})')


def compare(args):
    a, b = Path(args.first), Path(args.second)
    verify_dist(a)
    verify_dist(b)
    sums_a, sums_b = read_checksums(a), read_checksums(b)
    differences = [name for name in sorted(sums_a.keys() | sums_b.keys()) if sums_a.get(name) != sums_b.get(name)]
    for name in differences:
        print(f'differs: {name}: {sums_a.get(name, "missing")} != {sums_b.get(name, "missing")}', file=sys.stderr)
    if differences:
        builds = [json.loads(next(d.glob('*.build.json')).read_text()) for d in (a, b)]
        same_binary = builds[0]['binary']['sha256'] == builds[1]['binary']['sha256']
        print(f"binary sha256 {'matches' if same_binary else 'differs'}", file=sys.stderr)
        raise ReleaseError('builds are not reproducible')
    print(f'reproducible: {len(sums_a)} artifacts match')


def smoke(args):
    dist = Path(args.dist)
    base = verify_dist(dist)
    archives = [f'{base}.tar.gz']
    published = json.loads((dist / f'{base}.build.json').read_text())
    with tempfile.TemporaryDirectory(prefix='vmtier-smoke-') as tmp:
        tmp = Path(tmp)
        with tarfile.open(dist / archives[0]) as tar:
            names = tar.getnames()
            if any(name != base and not name.startswith(base + '/') for name in names):
                raise ReleaseError('archive entries escape the top-level directory')
            tar.extractall(tmp, filter='data')
        root = tmp / base
        build_record = json.loads((root / 'BUILD.json').read_text())
        if build_record != published:
            raise ReleaseError('packaged BUILD.json differs from the published build record')
        required = [*TOP_LEVEL_FILES, EXAMPLE, 'sbom.cdx.json', 'go-version-m.txt', *build_record['notices'],
                    *(n for m in build_record['modules'] for n in m['notices'])]
        missing = [name for name in required if not (root / name).is_file()]
        if missing:
            raise ReleaseError(f'archive is missing {missing}')
        binary = root / 'vmtier'
        if sha256_file(binary) != build_record['binary']['sha256']:
            raise ReleaseError('packaged binary differs from BUILD.json')
        if elf_linking(binary) != ('amd64', 'static'):
            raise ReleaseError('packaged binary is not a static amd64 executable')
        version_line = run([binary, 'version'], capture=True)
        expected = f"vmtier {build_record['version']} (commit {build_record['commit']}, {build_record['go']['GOVERSION']} linux/amd64);"
        if not version_line.startswith(expected):
            raise ReleaseError(f'unexpected version output {version_line!r}; want prefix {expected!r}')
        # Exercise the statically linked SQLite catalog without network access.
        catalog = tmp / 'catalog'
        config = {'store': {'type': 'local', 'path': str(tmp / 'objects')}, 'tenants': ['1:2'],
                  'catalog': {'type': 'sqlite', 'path': str(catalog), 'id': 'release-smoke'},
                  'sources': [{'id': 'hot', 'url': 'http://127.0.0.1:1'}], 'archive_age': '3h',
                  'hot_retention': '240h', 'lateness': '30m', 'safety_margin': '30m', 'window': '1h',
                  'lateness_enforced': True, 'coverage_start_ms': 3600000}
        write_json(tmp / 'config.json', config)
        run([binary, 'catalog-init', '-backend', 'sqlite', '-config', tmp / 'config.json', '-path', catalog,
             '-id', 'release-smoke'])
        status = json.loads(run([binary, 'catalog-inspect', '-path', catalog, '-id', 'release-smoke'], capture=True))
        if status.get('id') != 'release-smoke' or status.get('generation') != 0:
            raise ReleaseError(f'unexpected SQLite catalog status {status}')
        if args.install_to:
            target = Path(args.install_to)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(binary, target)
    print(f'smoke passed: {archives[0]} ({build_record["binary"]["sha256"]})')


def notes(args):
    dist = Path(args.dist)
    base = verify_dist(dist)
    build_record = json.loads((dist / f'{base}.build.json').read_text())
    sums = read_checksums(dist)
    checksum_lines = ''.join(f'{digest}  {name}\n' for name, digest in sorted(sums.items()))
    print(f"""## vmtier {build_record['version']}

Static `linux/amd64` build of commit `{build_record['commit']}` for
VictoriaMetrics OSS `v1.155.0-cluster` only. Read the
[support scope](https://github.com/yuuki/vmtier/blob/{build_record['commit']}/docs/releasing.md#support-scope)
before deploying: provider, retention, and ingress qualification are tracked separately.

### Verify

```sh
sha256sum --check SHA256SUMS
gh attestation verify {base}.tar.gz --repo yuuki/vmtier \\
  --signer-workflow yuuki/vmtier/.github/workflows/release.yml \\
  --source-ref refs/tags/{build_record['version']} --deny-self-hosted-runners
gh attestation verify {base}.tar.gz --repo yuuki/vmtier \\
  --signer-workflow yuuki/vmtier/.github/workflows/release.yml \\
  --source-ref refs/tags/{build_record['version']} --deny-self-hosted-runners \\
  --predicate-type https://cyclonedx.org/bom
```

Binary SHA-256: `{build_record['binary']['sha256']}`. The archive contains the
license files of every linked Go module, recorded notices embedded in bundled C
code, and the Go runtime and musl notices; the build recipe is in `{base}.build.json`.

```text
{checksum_lines}```""")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('build', help='build and package HEAD in the pinned container')
    p.add_argument('--version', help='release tag on HEAD; defaults to that tag or v0.0.0-dev.g<commit>')
    p.add_argument('--output', default=str(ROOT / 'dist'), help='empty output directory (default: dist)')
    p.add_argument('--workdir', help='parent for the temporary build directory')
    p.add_argument('--builder-image', help='use an existing builder image instead of building release/Dockerfile')
    p.add_argument('--unit-tests', action='store_true', help='also run go test in the builder with release tags')
    p.add_argument('--keep-work', action='store_true', help='keep the exported source and module cache')
    p.set_defaults(func=build)
    p = sub.add_parser('verify', help='check a dist directory by hashes only, without running anything')
    p.add_argument('dist')
    p.add_argument('--sums-sha256', help='require SHA256SUMS itself to have this SHA-256')
    p.set_defaults(func=verify)
    p = sub.add_parser('compare', help='require two verified dist directories to have identical checksums')
    p.add_argument('first')
    p.add_argument('second')
    p.set_defaults(func=compare)
    p = sub.add_parser('smoke', help='verify checksums and exercise the packaged binary')
    p.add_argument('dist')
    p.add_argument('--install-to', help='copy the verified binary here, e.g. .bin/vmtier for E2E suites')
    p.set_defaults(func=smoke)
    p = sub.add_parser('notes', help='print release notes for a dist directory')
    p.add_argument('dist')
    p.set_defaults(func=notes)
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except (ReleaseError, OSError, KeyError, json.JSONDecodeError) as e:
        print(f'release: {type(e).__name__ if not isinstance(e, ReleaseError) else "error"}: {e}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
