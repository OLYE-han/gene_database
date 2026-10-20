"""Verify the copied shard before publishing it; reuse existing compiled releases."""
from pathlib import Path
import json,shlex,subprocess,sys
R=Path(__file__).resolve().parent
script=r'''
from pathlib import Path
import hashlib,json,sys
expected=json.load(sys.stdin)
r=Path('/sharenvme/xulihan_dhdgs/managed_35_30_20261019')
final=r/'index/shard_1';staged=r/'index/shard_1.incoming';source=final if final.exists() else staged
actual={}
for name,wanted in expected.items():
 p=source/name;sha=hashlib.sha256()
 with p.open('rb') as f:
  for block in iter(lambda:f.read(8<<20),b''):sha.update(block)
 value={'bytes':p.stat().st_size,'sha256':sha.hexdigest()}
 if value!=wanted:raise RuntimeError('Checksum mismatch '+name)
 actual[name]=value
if {p.name for p in source.iterdir() if p.is_file()}!=set(expected):raise RuntimeError('Unexpected files in copied shard')
if source==staged:staged.rename(final)
for i in (1,2):
 link=r/f'node{i}/releases';target=Path('/sharenvme/xulihan_dhdgs/managed_cluster_20261019/releases')
 if not link.exists():link.symlink_to(target,target_is_directory=True)
 if link.resolve()!=target.resolve():raise RuntimeError('Unexpected release directory')
print(json.dumps({'verified':True,'files':actual,'directory':str(final)}))
'''
cmd=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8','-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2','intel-30',shlex.join(['/usr/bin/python3','-c',script])]
result=subprocess.run(cmd,input=(R/'shard_1_manifest.json').read_bytes(),stdout=subprocess.PIPE,check=True,timeout=240)
(R/'shard_verification.json').write_bytes(result.stdout)
print('Copied shard verified by SHA256 and published',flush=True)
