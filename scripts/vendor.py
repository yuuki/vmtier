"""Generate the offline vendor tree from pinned VM and database-driver source archives."""
from pathlib import Path
import hashlib, re, shutil, zipfile
root = Path(__file__).resolve().parent.parent
up = root / '.deps/victoriametrics'
vendor = root / 'vendor'
if not vendor.exists(): shutil.copytree(up / 'vendor', vendor)
previous_modules = (vendor/'modules.txt').read_text()
direct = set(re.findall(r'^\s*(\S+) v\S+', (root/'go.mod').read_text(), re.M))
lines = (up/'vendor/modules.txt').read_text().splitlines()
module = ''
out = []
for line in lines:
    if line.startswith('# '): module = line.split()[1]
    if line.startswith('## ') and module not in direct:
        line = line.replace('explicit; ', '').replace('explicit', '')
        if line == '## ': continue
    out.append(line)
vm = 'github.com/VictoriaMetrics/VictoriaMetrics'
out += ['# '+vm+' v1.128.0-cluster', '## explicit; go 1.25.3']
dest = vendor / vm
if not dest.exists():
    dest.mkdir(parents=True)
    shutil.copytree(up/'lib', dest/'lib')
    for name in ['LICENSE', 'NOTICE']:
        if (up/name).exists(): shutil.copy(up/name, dest/name)
for p in sorted((dest/'lib').rglob('*.go')):
    name = vm + '/' + str(p.parent.relative_to(dest))
    if not out or out[-1] != name: out.append(name)
# Additional drivers are pinned independently of the upstream VM vendor tree.
def add_driver(module, version, checksum, go_version, packages):
    name = module.rsplit('/', 1)[1]
    archive = root / '.deps' / f'{name}-{version}.zip'
    if hashlib.sha256(archive.read_bytes()).hexdigest() != checksum:
        raise SystemExit(f'{module} source archive checksum mismatch; rerun bootstrap with the pinned archive')
    pin = re.escape(f'{module} {version}')
    if not re.search(r'^\s*' + pin + r'\s*$', (root/'go.mod').read_text(), re.M):
        raise SystemExit(f'{module} version differs from the bootstrap pin')
    header = f'# {module} {version}'
    destination = vendor / module
    if not destination.exists() or header not in previous_modules.splitlines():
        prefix = f'{module}@{version}/'
        source_dir = root / '.deps' / f'{name}-{version}'
        if source_dir.exists(): shutil.rmtree(source_dir)
        source_dir.mkdir()
        with zipfile.ZipFile(archive) as source:
            for entry in source.infolist():
                if not entry.filename.startswith(prefix):
                    raise SystemExit(f'Unexpected path in {module} source archive')
                relative = Path(entry.filename[len(prefix):])
                if relative.is_absolute() or '..' in relative.parts:
                    raise SystemExit(f'Invalid path in {module} source archive')
                target = source_dir / relative
                if entry.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(source.read(entry))
        if destination.exists(): shutil.rmtree(destination)
        shutil.copytree(source_dir, destination)
    out.extend([header, f'## explicit; go {go_version}', *packages])

add_driver('github.com/mattn/go-sqlite3', 'v1.14.32',
           'c70101000256656bc0b7286807cf177852c753a001a3aa3c55d5373575fb81aa',
           '1.19', ['github.com/mattn/go-sqlite3'])
add_driver('github.com/lib/pq', 'v1.10.9',
           '6791a7af67f846a86335350012c4efadd0850096ebf22350b7b20e01e216e6f2',
           '1.13', ['github.com/lib/pq', 'github.com/lib/pq/hstore',
                    'github.com/lib/pq/oid', 'github.com/lib/pq/scram'])
(vendor/'modules.txt').write_text('\n'.join(out)+'\n')
