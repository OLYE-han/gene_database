"""Independent native network load producer/consumer; never constructs an engine."""
import os
for k in ('OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS'):os.environ[k]='1'
import sys,json,argparse,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'python'))
import numpy as np,yaml
from dhdgs.index import native
from dhdgs.metrics import process_snapshot
p=argparse.ArgumentParser();p.add_argument('--plan',required=True);p.add_argument('--settings',required=True);p.add_argument('--output',required=True);p.add_argument('--count',type=int);p.add_argument('--concurrency',type=int,required=True);a=p.parse_args()
plan=yaml.safe_load(Path(a.plan).read_text());settings=yaml.safe_load(Path(a.settings).read_text());count=a.count or settings['experiment']['queries']
selection=json.loads(Path(plan['control']['query_selection']).read_text())['validation'];selection=(selection*((count+len(selection)-1)//len(selection)))[:count]
c=plan['dhdgs'];x=np.ascontiguousarray(np.load(c['query']['vectors'],mmap_mode='r')[selection],dtype=np.float64);ids=np.ascontiguousarray(np.load(c['query']['ids'],mmap_mode='r')[selection],dtype=np.uint64)
root=Path(a.output);root.mkdir(parents=True,exist_ok=False)
(root/'selection.json').write_text(json.dumps(dict(rows=selection,ids=ids.tolist())))
before=process_snapshot();result=native().run_network_load(x,ids,a.concurrency,settings['client'],str(root/'results'));after=process_snapshot()
(root/'manifest.json').write_text(json.dumps(dict(**result,process_before=before,process_after=after,settings=settings),indent=2));print(json.dumps(result),flush=True)
if result['error'] or result['received']!=count or sum(x['consumed'] for x in result['lanes'])!=count:raise SystemExit(1)
