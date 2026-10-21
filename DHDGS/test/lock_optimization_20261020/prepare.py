"""Build the frozen candidate per host; leave resident releases untouched."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
from pathlib import Path
import shlex
import subprocess
import tarfile

ROOT=Path(__file__).resolve().parent
previous=ROOT.parent/'adaptive_wait_20261020'
hosts=json.loads((previous/'hosts_v6.json').read_text())
for h in hosts:
    h['root']=str(ROOT/'data') if not h['ssh'] else str(Path(h['root']).parent/ROOT.name)
    h['project']=str(ROOT/'source') if not h['ssh'] else str(Path(h['root'])/'source')
(ROOT/'hosts.json').write_text(json.dumps(hosts,indent=2))
manifest=json.loads((ROOT/'source_hashes.json').read_text())
blob=io.BytesIO()
with tarfile.open(fileobj=blob,mode='w:gz') as tf:
    for rel in manifest:tf.add(ROOT/'source'/rel,arcname=rel)
script=r'''
import hashlib,io,json,os,subprocess,sys,tarfile
from pathlib import Path
h=json.loads(sys.argv[1]);p=Path(h['project']);root=Path(h['root'])
if h['ssh']:
 p.mkdir(parents=True,exist_ok=False)
 with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read()),mode='r:gz') as tf:
  for m in tf.getmembers():
   if not m.isfile() or '..' in Path(m.name).parts or Path(m.name).is_absolute():raise ValueError('Unsafe source path')
   dst=p/m.name;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(tf.extractfile(m).read())
 ref=Path(h['source_project']).parent/'Reference_Code'
 if not (ref/'RaBitQ-Library').is_dir():raise RuntimeError('Missing reference library')
 (root/'Reference_Code').symlink_to(ref,target_is_directory=True)
 with (root/'build.log').open('w') as log:
  compiler=['-DCMAKE_CXX_COMPILER=/usr/bin/g++'] if h['id']==2 else []
  subprocess.run(['cmake','-S',str(p),'-B',str(root/'build'),'-DCMAKE_BUILD_TYPE=Release','-DDHDGS_NATIVE_OPTIMIZATION=ON','-DPython_EXECUTABLE='+sys.executable,*compiler],stdout=log,stderr=subprocess.STDOUT,check=True)
  subprocess.run(['cmake','--build',str(root/'build'),'-j','2'],stdout=log,stderr=subprocess.STDOUT,check=True)
sys.path.insert(0,str(p/'python'))
from dhdgs import _native  # Verify the runtime linker, not only compilation.
files=[*sorted((p/'python/dhdgs').glob('*.py')),*sorted((p/'cpp').glob('*.*')),*sorted((p/'tools').glob('*.py')),p/'CMakeLists.txt',p/'configs/dhdgs.yaml']
print(json.dumps(dict(host=os.uname().nodename,affinity=sorted(os.sched_getaffinity(0)),hashes={str(f.relative_to(p)):hashlib.sha256(f.read_bytes()).hexdigest() for f in files},native_hashes={f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in (p/'python/dhdgs').glob('*.so')})))
'''

def build(h):
    argv=[h['python'],'-c',script,json.dumps(h)]
    if h['ssh']:argv=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10',h['ssh'],shlex.join(argv)]
    p=subprocess.run(argv,input=blob.getvalue(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=600)
    if p.returncode:raise RuntimeError(str(h['id'])+': '+p.stderr.decode()[-3000:])
    info=json.loads(p.stdout)
    for rel,expected in manifest.items():
        if info['hashes'].get(rel)!=expected:raise RuntimeError(f'Hash mismatch: node {h["id"]} / {rel}')
    print('BUILT',h['id'],info['host'],flush=True)
    return h['id'],info

with ThreadPoolExecutor(3) as pool:results=dict(pool.map(build,hosts))
(ROOT/'hardware_and_sources.json').write_text(json.dumps(results,indent=2))
