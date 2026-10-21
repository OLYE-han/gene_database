import gzip,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent;PROJECT=ROOT.parents[1]
d=json.loads((PROJECT/'test/concurrency_scale_20261020/three/00_c256/result.json').read_text());run=Path(d['directory']);best=None;delta=float('inf');count=0
with gzip.open(run/'queries.jsonl.gz','rt') as f:
 for line in f:
  q=json.loads(line)
  if q['stats'].get('peer_branch_rpc_count',0)<1 or len({b['node_id'] for b in q['branch_reports']})<2:continue
  count+=1;gap=abs(q['client_wall_ms']-d['latency_ms']['p99'])
  if gap<delta:best=q;delta=gap
assert best
with gzip.open(run/'nodes_after.json.gz','rt') as f:nodes=json.load(f)
branches=[]
for b in best['branch_reports']:
 life=nodes[str(b['node_id'])]['peer_lifecycle'];trace=next(t for t in life['execution_timestamps'] if t['branch_id']==b['branch_id']);events=[e for e in life['control_events'] if e['branch_id']==b['branch_id']]
 branches.append(dict(report=b,trace=trace,events=events))
out=dict(source=str(run),target_p99_ms=d['latency_ms']['p99'],distance_to_p99_ms=delta,cross_node_query_count=count,query=best,branches=branches)
(ROOT/'selected.json').write_text(json.dumps(out,indent=2))
print(json.dumps(dict(target=d['latency_ms']['p99'],query_id=best['query_id'],job=best['job'],latency=best['client_wall_ms'],peer_rpc=best['stats']['peer_branch_rpc_count'],branches=[dict(node=b['report']['node_id'],trace=b['trace'],events=b['events'],fork=b['report']['fork_events']) for b in branches]),indent=2))
