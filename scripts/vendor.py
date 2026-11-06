"""Generate local offline vendor tree from checksum-pinned upstream archive."""
from pathlib import Path
import shutil, re
root = Path(__file__).resolve().parent.parent
up = root / '.deps/victoriametrics'
vendor = root / 'vendor'
if not vendor.exists(): shutil.copytree(up / 'vendor', vendor)
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
(vendor/'modules.txt').write_text('\n'.join(out)+'\n')
