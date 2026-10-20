"""Interleaved old head vs single-process pipeline, same three running data nodes."""
import os
for k in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[k]='1'
from pathlib import Path
import argparse,copy,hashlib,json,shutil,subprocess,sys
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from cluster_ctl import request
from dhdgs.managed_cluster import ClusterHead
from dhdgs.managed_node import save_json,revision
from full_truth import FullTruth

def child(case):
    global R
    R=case.parent.parent
    detail=json.loads((case/'case.json').read_text());e=yaml.safe_load((R/'experiment.yaml').read_text())
    plan=load_plan(e['base_plan']);plan['control']['results_directory']=str(case)
    c=plan['dhdgs'];c['peer']['head_port']=e['head_port']+32*detail.get('index',0);mode=detail['mode']
    c['runtime']['python_switch_interval_ms']=e.get('switch_ms',{}).get(mode,c['runtime']['python_switch_interval_ms'])
    if mode in ('direct','direct1') or mode.startswith(('channels','process','shared','inline')):
        defaults=yaml.safe_load((P/'configs/dhdgs.yaml').read_text())['peer']['head_direct']
        defaults.update(enabled=True,results_directory=str(case/'worker_results'))
        defaults.update(queued_reports=mode not in ('direct','direct1'))
        if 'completion_threads' in e:defaults['completion_threads']=e['completion_threads']
        if mode.startswith('channels'):defaults['result_channels']=int(mode[8:])
        if mode.startswith('process'):defaults.update(result_channels=int(mode[7:]),report_backend='process')
        if mode.startswith('shared'):defaults.update(result_channels=int(mode[6:]),report_backend='process',report_registration='shared')
        if mode.startswith('inline'):defaults.update(result_channels=int(mode[6:]),report_backend='process',report_registration='shared',completion_dispatch='inline')
        c['peer'].update(head_receiver_processes=0,head_query_workers=0,head_direct=defaults)
        if 'head_pipeline' in c['peer']:c['peer']['head_pipeline']['enabled']=False
    elif mode!='old':
        defaults=yaml.safe_load((P/'configs/dhdgs.yaml').read_text())['peer']['head_pipeline']
        c['peer'].update(head_receiver_processes=0,head_query_workers=0,head_pipeline=defaults)
        defaults.update(enabled=True,route_workers=int(mode[1:]),merge_workers=e['merge_workers'],dispatch_workers=e['dispatch_workers'],ack_inline=e.get('ack_modes',{}).get(mode,e.get('ack_inline',False)))
    (case/'plan.yaml').write_text(yaml.safe_dump(plan,sort_keys=False));load_plan(case/'plan.yaml')
    truth=FullTruth(plan['control']['truth_plan'])
    head=ClusterHead(c,plan['control'],Path(plan['control']['token_file']).read_text().strip(),lambda row,k:list(map(int,truth.ids[row,:k])))
    try:
        states=head.status()['nodes']
        assert all(s.get('quiescent') and s.get('accepting') and s.get('revision')==revision(head.c) for s in states)
        head.ready=True
        profilers=[];profile_rows=[]
        if e.get('profile'):
            import time,threading
            from unittest.mock import patch
            from dhdgs.pipeline_hub import PipelineHub
            from dhdgs.head_direct import DirectFlow
            from dhdgs.head_hub import HeadHub
            from dhdgs.head_receivers import ProcessHeadHub
            from dhdgs.peer import HeadSession
            from dhdgs.report_registry import ReportRegistry
            for cls,name in [(PipelineHub,'_finish'),(DirectFlow,'merge'),(HeadSession,'close'),(HeadHub,'unregister'),(ProcessHeadHub,'unregister'),(ReportRegistry,'remove')]:
                original=getattr(cls,name);label=cls.__name__+'.'+name
                def wrapper(*a,_fn=original,_label=label,**kw):
                    start=time.monotonic_ns();cpu=time.thread_time_ns()
                    try:return _fn(*a,**kw)
                    finally:profile_rows.append(dict(stage=_label,thread=threading.current_thread().name,wall_ms=(time.monotonic_ns()-start)/1e6,cpu_ms=(time.thread_time_ns()-cpu)/1e6))
                p=patch.object(cls,name,wrapper);p.start();profilers.append(p)
        result=head.benchmark({k:detail[k] for k in ('queries','concurrency','warmup')})
        for p in profilers:p.stop()
        if profile_rows:save_json(case/'profile.json',profile_rows)
        result['mode']=mode
        result['report_channels']=head.engine.head_hub.report_status()
        if mode!='old':result['pipeline_status']=head.engine.head_flow.ingress.status()
        save_json(case/'result.json',result)
        save_json(case/'final_status.json',head.status())
        print(json.dumps({k:result[k] for k in ('qps','latency_ms','recall_at_k_all_requests','failed_queries')}),flush=True)
        assert result['failed_queries']==0 and all(result['audit'].values())
    finally:head.close()

def suite():
    e=yaml.safe_load((R/'experiment.yaml').read_text());base=load_plan(e['base_plan'])
    initial=request(base,'status');save_json(R/'production_before.json',initial)
    assert initial['ready'] and not initial['busy'] and not initial['active_queries']
    manifest={}
    for path in [*sorted((P/'python/dhdgs').glob('*.py')),P/'tools/cluster_service.py',P/'configs/dhdgs.yaml']:
        rel=path.relative_to(P);dst=R/'snapshot'/rel;dst.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(path,dst);manifest[str(rel)]=hashlib.sha256(path.read_bytes()).hexdigest()
    save_json(R/'source_hashes.json',manifest)
    for i,mode in enumerate(e['order']):
        case=R/'cases'/f'{i:02d}_{mode}';case.mkdir(parents=True,exist_ok=False)
        save_json(case/'case.json',dict(index=i,mode=mode,**{k:e[k] for k in ('queries','concurrency','warmup')}))
        print('START',case.name,flush=True)
        with (case/'execution.log').open('w') as log:
            subprocess.run([sys.executable,__file__,'--child',str(case)],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
        s=json.loads((case/'result.json').read_text())
        print(json.dumps(dict(case=case.name,qps=s['qps'],p99=s['latency_ms']['p99'],recall=s['recall_at_k_all_requests'])),flush=True)
    save_json(R/'production_after.json',request(base,'status'))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--child',type=Path);parser.add_argument('--phase');args=parser.parse_args()
    if args.phase:R=R/args.phase
    if args.child:child(args.child)
    else:suite()
