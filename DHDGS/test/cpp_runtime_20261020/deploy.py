"""Freeze and build the native runtime on isolated directories on each host."""
import hashlib,io,json,shlex,shutil,subprocess,tarfile
from pathlib import Path
import yaml
ROOT=Path(__file__).resolve().parent;PROJECT=ROOT.parents[1]
plan=yaml.safe_load((PROJECT/'test/control_capacity_20261020/candidate.yaml').read_text())
source=ROOT/'source';source.mkdir(exist_ok=True)
files=[PROJECT/'CMakeLists.txt',PROJECT/'configs/dhdgs.yaml',*sorted((PROJECT/'cpp').glob('*.cpp')),*sorted((PROJECT/'cpp').glob('*.hpp')),*sorted((PROJECT/'python/dhdgs').glob('*.py')),*sorted((PROJECT/'tools').glob('*.py'))]
hashes={}
for p in files:
 rel=p.relative_to(PROJECT);dest=source/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,dest);hashes[str(rel)]=hashlib.sha256(p.read_bytes()).hexdigest()
for p in (PROJECT/'python/dhdgs').glob('*.so'):shutil.copy2(p,source/'python/dhdgs'/p.name)
shutil.copytree(PROJECT/'python/dhdgs/.native_libs',source/'python/dhdgs/.native_libs',dirs_exist_ok=True)
(ROOT/'source_hashes.json').write_text(json.dumps(hashes,indent=2))
blob=io.BytesIO()
with tarfile.open(fileobj=blob,mode='w:gz') as archive:
 for rel in hashes:archive.add(source/rel,arcname=rel)
hosts={}
for h in plan['deployment']['hosts']:
 sid=h['id'];root=ROOT/'data' if sid==0 else Path(h['root']).parents[1]/ROOT.name
 project=source if sid==0 else root/'source'
 item={**h,'root':str(root),'project':str(project)};hosts[str(sid)]=item
 if sid==0:continue
 code=r'''
import io,json,os,subprocess,sys,tarfile
from pathlib import Path
p=json.loads(sys.argv[1]);root=Path(p['root']);target=Path(p['project']);target.mkdir(parents=True,exist_ok=True)
with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read()),mode='r:gz') as tf:
 for member in tf.getmembers():
  rel=Path(member.name)
  if not member.isfile() or '..' in rel.parts or rel.is_absolute():raise ValueError('Unsafe tar member')
  dest=target/rel;dest.parent.mkdir(parents=True,exist_ok=True);content=tf.extractfile(member).read()
  if not dest.exists() or dest.read_bytes()!=content:dest.write_bytes(content)
original=Path(p['python']).parents[2]
reference=root/'Reference_Code'
if not reference.exists():reference.symlink_to(original.parent/'Reference_Code',target_is_directory=True)
build=root/'build'
command=['cmake','-S',str(target),'-B',str(build),'-DCMAKE_BUILD_TYPE=Release','-DDHDGS_NATIVE_OPTIMIZATION=ON','-DCMAKE_CXX_COMPILER=/usr/bin/g++','-DPython_EXECUTABLE='+p['python']]
with (root/'configure.log').open('wb') as out:subprocess.run(command,stdout=out,stderr=out,check=True)
with (root/'build.log').open('wb') as out:subprocess.run(['cmake','--build',str(build),'-j','2'],stdout=out,stderr=out,check=True)
print(json.dumps({'project':str(target),'build':str(build)}))
'''
 argv=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8',h['ssh'],shlex.join([h['python'],'-c',code,json.dumps(item)])]
 try:
  run=subprocess.run(argv,input=blob.getvalue(),capture_output=True,timeout=600)
  (ROOT/f'deploy_node{sid}.log').write_bytes(run.stdout+run.stderr)
  print('build',sid,run.returncode,flush=True)
  if run.returncode:print(run.stderr.decode()[-1000:],flush=True)
 except subprocess.TimeoutExpired:
  print('deployment timeout',sid,flush=True)
(ROOT/'deployment.json').write_text(json.dumps(hosts,indent=2))
