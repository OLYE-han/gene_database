"""Move the currently deployed release on intel-30 without changing its content."""
from pathlib import Path
import hashlib
import json
import os
import shlex
import subprocess

root = Path('/sharenvme/xulihan_dhdgs/managed_cluster_20261019')
launcher = root / 'run_data.sh'
args = shlex.split(launcher.read_text().splitlines()[1])
assert args[0] == 'exec' and args[3] == 'data', args
python = args[1]
old_project = Path(args[2]).parents[1]
source = old_project.parent
destination = Path('/sharenvme/xulihan/gene_database')
assert source.parent.parent == root / 'releases'
assert source.is_dir() and not source.is_symlink()
assert not os.path.lexists(destination)
for proc in Path('/proc').glob('[0-9]*/cmdline'):
    try:
        command = proc.read_bytes().split(b'\0')
    except OSError:
        continue
    assert not (str(old_project / 'tools/cluster_service.py').encode() in command), 'Service is running'

def manifest(folder):
    return {str(p.relative_to(folder)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in folder.rglob('*') if p.is_file()}

original = manifest(source)
record = Path('/sharenvme/xulihan/relocation_20261019')
record.mkdir(parents=True, exist_ok=False)
(record / 'run_data.sh.before').write_bytes(launcher.read_bytes())
(record / 'manifest.before.json').write_text(json.dumps(original, indent=2))
assert source.stat().st_dev == destination.parent.stat().st_dev
source.rename(destination)
try:
    source.symlink_to(destination, target_is_directory=True)
except BaseException:
    destination.rename(source)
    raise
assert original == manifest(destination), 'File checksums changed'
args[2] = str(destination / 'DHDGS/tools/cluster_service.py')
launcher.write_text('#!/bin/sh\n' + shlex.join(args) + '\n')

check = '''import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from dhdgs import _native
from dhdgs.config import StrictLoader,validate
import yaml
p=yaml.load(Path(sys.argv[2]).read_text(),Loader=StrictLoader)
validate(p['dhdgs'])
n=next(n for n in p['dhdgs']['cluster']['nodes'] if n['id']==2)
m=json.loads((Path(n['index_directory'])/'manifest.json').read_text())
assert hasattr(_native,'NativeController')
assert Path(_native.__file__).resolve().is_relative_to(Path(sys.argv[1]).resolve())
print(json.dumps({'native':_native.__file__,'index':n['index_directory'],'shard_id':m['shard_id']}))
'''
probe = subprocess.check_output([python, '-c', check, str(destination / 'DHDGS/python'),
                                 str(root / 'cluster.yaml')], text=True, timeout=30)
subprocess.run([python, args[2], '--help'], check=True, timeout=30)
result = {'source': str(source), 'destination': str(destination),
          'destination_is_real_directory': not destination.is_symlink(),
          'old_path_is_compatibility_symlink': source.is_symlink(),
          'files_verified': len(original), 'all_file_hashes_unchanged': True,
          'launcher': str(launcher), 'python': python, 'probe': json.loads(probe)}
(record / 'verification.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
