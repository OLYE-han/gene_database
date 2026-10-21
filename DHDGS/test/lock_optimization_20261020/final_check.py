"""Read-only deployment, workload and source consistency verification."""
import hashlib
import json
from pathlib import Path
import socket
import sys
import yaml

ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parents[1]
sys.path[:0]=[str(PROJECT/'tools'),str(PROJECT/'python')]
from cluster_ctl import request

plan=yaml.safe_load((PROJECT/'test/managed_continuation_three_20261019/cluster.yaml').read_text())
state=request(plan,'status')
assert state['ready'] and not state['busy'] and not state['active_queries']
assert all(n['quiescent'] and n['accepting'] for n in state['nodes'])
cases=sorted(ROOT.glob('*/*/result.json'))
assert len(cases)==8
verified=[]
for file in cases:
    r=json.loads(file.read_text());case=file.parent
    c=yaml.safe_load((case/'plan.yaml').read_text())
    loaded=json.loads((case/'loaded_sources.json').read_text())
    assert r['failed_queries']==0 and all(r['audit'].values())
    assert r['recall_at_k_all_requests']>=.95
    assert r['configured_concurrency']==256 and r['threads']==32
    assert r['query_count']==(100000 if case.parent.name=='sustained' else 10000)
    assert '/source/python/' in loaded['dhdgs.peer']
    assert all(Path(h['project']).name=='source' for h in c['deployment']['hosts'])
    assert c['dhdgs']['peer']['worker_threads']==32 and c['dhdgs']['search']['max_expansions']==200
    verified.append(dict(case=str(case.relative_to(ROOT)),queries=r['query_count'],audit=True))
listeners=[]
for n in c['dhdgs']['cluster']['nodes']:
    with socket.socket() as s:
        s.settimeout(2);up=s.connect_ex((n['host'],n['port']))==0
    listeners.append(dict(node=n['id'],host=n['host'],port=n['port'],reachable=up))
assert not any(x['reachable'] for x in listeners)
manifest=json.loads((ROOT/'source_hashes.json').read_text())
for rel,digest in manifest.items():
    assert hashlib.sha256((PROJECT/rel).read_bytes()).hexdigest()==digest,rel
    assert hashlib.sha256((ROOT/'source'/rel).read_bytes()).hexdigest()==digest,rel
result=dict(production=state,experimental_listeners=listeners,cases=verified,source_consistent=True)
(ROOT/'final_cluster_check.json').write_text(json.dumps(result,indent=2))
print(json.dumps(dict(production_ready=True,experiments_stopped=True,queries=sum(r['queries'] for r in verified),source_consistent=True)))
