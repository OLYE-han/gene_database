import concurrent.futures,hashlib,json,shlex,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent
hosts=json.loads((ROOT/'deployment.json').read_text());manifest=json.loads((ROOT/'source_hashes.json').read_text())
script='''import hashlib,json,sys,os,resource
from pathlib import Path
p=json.load(sys.stdin);root=Path(p['project'])
print(json.dumps(dict(mismatches=[r for r,h in p['manifest'].items() if hashlib.sha256((root/r).read_bytes()).hexdigest()!=h],fd_limit=resource.getrlimit(resource.RLIMIT_NOFILE),cpu_affinity=len(os.sched_getaffinity(0)),netdev=Path('/proc/net/dev').read_text())))
'''
def check(item):
 sid,h=item;argv=[h['python'],'-c',script]
 if h['ssh']:argv=['ssh','-T','-o','ControlMaster=auto','-o','ControlPersist=1800','-o','ControlPath=/tmp/dhdgs-route-%r@%h:%p','-o','BatchMode=yes','-o','ConnectTimeout=8',h['ssh'],shlex.join(argv)]
 p=subprocess.run(argv,input=json.dumps(dict(project=h['project'],manifest=manifest)),text=True,capture_output=True,check=True,timeout=180)
 return sid,json.loads(p.stdout)
with concurrent.futures.ThreadPoolExecutor(3) as pool:out=dict(pool.map(check,hosts.items()))
(ROOT/'environment.json').write_text(json.dumps(out,indent=2));print(out)
assert all(not h['mismatches'] for h in out.values())
