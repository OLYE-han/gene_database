"""Controlled callback process-count scan against the same three idle data nodes."""
import os
for k in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[k]='1'
from pathlib import Path
import argparse,json,subprocess,sys
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from cluster_ctl import request
from dhdgs.managed_cluster import ClusterHead
from dhdgs.managed_node import save_json,revision
from full_truth import FullTruth

def child(case):
    detail=json.loads((case/'case.json').read_text());e=yaml.safe_load((R/'experiment.yaml').read_text())
    plan=load_plan(e['base_plan']);plan['control']['results_directory']=str(case)
    plan['dhdgs']['peer'].update(head_receiver_processes=detail['mode'],head_receiver_ipc='split',head_query_workers=2,head_entry_workers=e['entry_workers'],head_port=e['head_port'])
    (case/'plan.yaml').write_text(yaml.safe_dump(plan,sort_keys=False))
    truth=FullTruth(plan['control']['truth_plan'])
    head=ClusterHead(plan['dhdgs'],plan['control'],Path(plan['control']['token_file']).read_text().strip(),
        lambda row,k:list(map(int,truth.ids[row,:k])))
    try:
        states=head.status()['nodes']
        assert all(s.get('quiescent') and s.get('accepting') and s.get('revision')==revision(head.c) for s in states)
        head.ready=True
        result=head.benchmark({k:detail[k] for k in ('queries','concurrency','warmup')})
        result['receiver_processes']=detail['mode'];save_json(case/'result.json',result)
        print(json.dumps({k:result[k] for k in ('qps','latency_ms','recall_at_k_all_requests','failed_queries')}),flush=True)
        assert result['failed_queries']==0 and all(result['audit'].values())
    finally:head.close()

def suite():
    e=yaml.safe_load((R/'experiment.yaml').read_text());base=load_plan(e['base_plan'])
    initial=request(base,'status');save_json(R/'production_before.json',initial)
    assert initial['ready'] and not initial['busy'] and not initial['active_queries']
    for i,n in enumerate(e['order']):
        case=R/'cases'/f'{i:02d}_{n}';case.mkdir(parents=True,exist_ok=False)
        save_json(case/'case.json',dict(mode=n,**{k:e[k] for k in ('queries','concurrency','warmup')}))
        print('START',case.name,flush=True)
        with (case/'execution.log').open('w') as log:
            subprocess.run([sys.executable,__file__,'--child',str(case)],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
        s=json.loads((case/'result.json').read_text())
        print(json.dumps(dict(case=case.name,qps=s['qps'],p99=s['latency_ms']['p99'])),flush=True)
    save_json(R/'production_after.json',request(base,'status'))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--child',type=Path);args=parser.parse_args()
    if args.child:child(args.child)
    else:suite()
