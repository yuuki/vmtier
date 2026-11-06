"""Generate the offline vendor tree from pinned VM and SQLite source archives."""
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
sqlite = 'github.com/mattn/go-sqlite3'
sqlite_version = 'v1.14.32'
sqlite_checksum = 'c70101000256656bc0b7286807cf177852c753a001a3aa3c55d5373575fb81aa'
archive = root / '.deps' / f'go-sqlite3-{sqlite_version}.zip'
if hashlib.sha256(archive.read_bytes()).hexdigest() != sqlite_checksum:
    raise SystemExit('SQLite source archive checksum mismatch; rerun bootstrap with the pinned archive')
if not re.search(r'^\s*github\.com/mattn/go-sqlite3 v1\.14\.32\s*$', (root/'go.mod').read_text(), re.M):
    raise SystemExit('SQLite module version differs from the bootstrap pin')
sqlite_header = f'# {sqlite} {sqlite_version}'
sqlite_dest = vendor / sqlite
if not sqlite_dest.exists() or sqlite_header not in previous_modules.splitlines():
    prefix = f'{sqlite}@{sqlite_version}/'
    sqlite_source = root / '.deps' / f'go-sqlite3-{sqlite_version}'
    if sqlite_source.exists(): shutil.rmtree(sqlite_source)
    sqlite_source.mkdir()
    with zipfile.ZipFile(archive) as source:
        for entry in source.infolist():
            if not entry.filename.startswith(prefix):
                raise SystemExit('Unexpected path in SQLite source archive')
            relative = Path(entry.filename[len(prefix):])
            if relative.is_absolute() or '..' in relative.parts:
                raise SystemExit('Invalid path in SQLite source archive')
            target = sqlite_source / relative
            if entry.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(source.read(entry))
    if sqlite_dest.exists(): shutil.rmtree(sqlite_dest)
    shutil.copytree(sqlite_source, sqlite_dest)
out += [sqlite_header, '## explicit; go 1.19', sqlite]
(vendor/'modules.txt').write_text('\n'.join(out)+'\n')
