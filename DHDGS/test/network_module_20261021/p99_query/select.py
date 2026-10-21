import gzip,json
from pathlib import Path
import numpy as np
ROOT=Path(__file__).resolve().parent
CASE=ROOT.parent/'three/06_observe'
summary=json.loads((CASE/'result.json').read_text())
run=Path(summary['directory']);target=summary['latency_ms']['p99']
values=[];best=None;distance=float('inf')
with gzip.open(run/'queries.jsonl.gz','rt') as f:
 for line in f:
  q=json.loads(line);value=q['client_wall_ms'];values.append(value)
  delta=abs(value-target)
  if (delta,q['job'])<(distance,best['job'] if best else float('inf')):
   distance=delta;best=q
actual=float(np.percentile(values,99))
assert abs(actual-target)<1e-6,(actual,target)
(ROOT/'selected_query.json').write_text(json.dumps(best,ensure_ascii=False,indent=2)+'\n')
(ROOT/'selection.json').write_text(json.dumps(dict(case=str(CASE),run=str(run),source=str(run/'queries.jsonl.gz'),selection_rule='Minimum absolute distance to np.percentile(client_wall_ms,99), tie break by ascending job',query_count=len(values),p99_ms=actual,selected_latency_ms=best['client_wall_ms'],difference_ms=distance,job=best['job'],query_id=best['query_id'],request_id=best['request_id']),indent=2)+'\n')
print(json.dumps(dict(p99=actual,selected=best['client_wall_ms'],job=best['job'],query_id=best['query_id'],recall=best['recall'],branches=len(best['branch_reports']),head=best['stats']['native_head_timestamps'],entry=best['stats']['entry_rpc'],breakdown=best['stats'].get('latency_breakdown_ms'),branches_stats=[dict(node=b['node_id'],stats={k:v for k,v in b['stats'].items() if 'ms' in k or 'cpu' in k},native=b['native_timestamps'],peer=b['peer_transfer']) for b in best['branch_reports']]),ensure_ascii=False))
