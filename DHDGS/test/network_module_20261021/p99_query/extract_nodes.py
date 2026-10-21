import gzip,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sel=json.loads((ROOT/'selection.json').read_text());qid=sel['request_id']
with gzip.open(Path(sel['run'])/'nodes_after.json.gz','rt') as f:nodes=json.load(f)
selected={}
for sid,node in nodes.items():
 life=node['peer_lifecycle']
 traces=[r for r in life.get('execution_timestamps',[]) if r['query_request_id']==qid]
 events=[r for r in life.get('control_events',[]) if r['query_request_id']==qid]
 if traces or events:selected[sid]=dict(execution_timestamps=traces,control_events=events,clock_domain=node.get('process',{}).get('clock_domain'))
(ROOT/'selected_node_events.json').write_text(json.dumps(selected,indent=2)+'\n')
print(json.dumps(selected))
