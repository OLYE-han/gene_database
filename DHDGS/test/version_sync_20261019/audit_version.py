"""Read-only source/version audit before redeploying a requested update."""
from pathlib import Path
import hashlib,json,shlex,subprocess,sys
from concurrent.futures import ThreadPoolExecutor
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from cluster_ctl import request
base=P/'test/managed_three_hosts_20261019'
plan=load_plan(base/'cluster.yaml')
manifest=json.loads((base/'payloads/05b5f3cf0079fd9ca3dd.json').read_text())
code="""
import json,hashlib,sys
from pathlib import Path
root=Path(sys.argv[1]);expected=json.load(sys.stdin);changed=[]
for name,digest in expected.items():
    path=root/name
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=digest:changed.append(name)
print(json.dumps({'checked':len(expected),'changed':changed,'root':str(root)}))
"""
def inspect(i):
    rec=json.loads((base/f'deployment_node{i}.json').read_text());h=rec['host']
    root=Path(rec['project']).parents[1]
    argv=[rec['python'],'-c',code,str(root)]
    cmd=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10',
         '-o','ServerAliveInterval=60','-o','ServerAliveCountMax=3',
         h['ssh'],shlex.join(argv)] if h['ssh'] else argv
    try:
        result=json.loads(subprocess.check_output(cmd,input=json.dumps(manifest).encode(),timeout=180))
        return dict(node=i,source_digest=rec['source_digest'],**result)
    except Exception as exc:return dict(node=i,error=str(exc))
with ThreadPoolExecutor(max_workers=3) as pool:nodes=list(pool.map(inspect,range(3)))
status=request(plan,'status')
(R/'status.json').write_text(json.dumps(status,indent=2))
result=dict(deployed_release='05b5f3cf0079fd9ca3dd',nodes=nodes,
            ready=status['ready'],settings=status['settings'])
(R/'version_audit.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
