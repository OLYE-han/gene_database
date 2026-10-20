"""Controlled head-only comparisons against the same live three-node index."""
import os
for key in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):
    os.environ[key]='1'
import argparse
import hashlib
import gzip
import json
from pathlib import Path
import shutil
import subprocess
import sys
import yaml

ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parents[1]

def archive_logs(case):
    """Lossless post-run compression; never touches a live worker log."""
    for path in (case/'worker_results').glob('*.jsonl'):
        target=path.with_suffix('.jsonl.gz')
        if target.exists():raise FileExistsError(target)
        temporary=target.with_suffix('.gz.tmp');original=hashlib.sha256()
        with path.open('rb') as source,gzip.open(temporary,'wb',compresslevel=1) as out:
            while block:=source.read(1024*1024):original.update(block);out.write(block)
        checked=hashlib.sha256()
        with gzip.open(temporary,'rb') as source:
            while block:=source.read(1024*1024):checked.update(block)
        if checked.digest()!=original.digest():raise RuntimeError('Compressed log checksum mismatch')
        temporary.replace(target);path.unlink()

def child(case):
    spec=json.loads((case/'spec.json').read_text())
    snapshot=Path(spec['snapshot'])
    sys.path[:0]=[str(snapshot/'python'),str(PROJECT/'tools')]
    import dhdgs
    assert Path(dhdgs.__file__).resolve().is_relative_to(snapshot.resolve())
    from cluster_service import load_plan
    from dhdgs.managed_cluster import ClusterHead
    from dhdgs.managed_node import revision,save_json
    from full_truth import FullTruth
    # cluster_service prepends its project Python directory. Spawned children
    # must inherit the frozen package first, as well as the already-imported parent.
    sys.path.remove(str(snapshot/'python'));sys.path.insert(0,str(snapshot/'python'))
    plan=load_plan(spec['base_plan'])
    plan['control']['results_directory']=str(case)
    if spec.get('repeat_selection'):
        selection=json.loads(Path(plan['control']['query_selection']).read_text())
        rows=selection['validation']
        selection['validation']=(rows*((spec['queries']+len(rows)-1)//len(rows)))[:spec['queries']]
        selection_path=case/'query_selection.json';selection_path.write_text(json.dumps(selection))
        plan['control'].update(query_selection=str(selection_path),max_queries=spec['queries'])
    plan['control']['max_client_threads']=max(plan['control']['max_client_threads'],spec['concurrency'])
    c=plan['dhdgs']
    defaults=yaml.safe_load((snapshot/'configs/dhdgs.yaml').read_text())['peer']['head_direct']
    defaults.update(enabled=True,queued_reports=True,report_backend='process',
                    report_registration='shared',result_channels=4,completion_dispatch='queued',
                    results_directory=str(case/'worker_results'))
    defaults.update(spec.get('direct',{}))
    c['peer'].update(head_port=spec['port'],head_receiver_processes=0,head_query_workers=0,head_direct=defaults)
    c['peer'].get('head_pipeline',{})['enabled']=False
    if spec.get('mode')=='legacy':
        c['peer']['head_direct']['enabled']=False
        c['peer'].update(head_receiver_processes=1,head_query_workers=2,head_entry_workers=4)
    if spec.get('mode')=='lanes':
        c['peer']['head_direct']['enabled']=False
        c['peer']['head_lanes']=yaml.safe_load((snapshot/'configs/dhdgs.yaml').read_text())['peer']['head_lanes']
        c['peer']['head_lanes'].update(enabled=True,**spec.get('lanes',{}))
        if 'results_directory' in c['peer']['head_lanes']:c['peer']['head_lanes']['results_directory']=str(case/'worker_results')
    if 'switch_ms' in spec:c['runtime']['python_switch_interval_ms']=spec['switch_ms']
    (case/'plan.yaml').write_text(yaml.safe_dump(plan,sort_keys=False))
    load_plan(case/'plan.yaml')
    truth=FullTruth(plan['control']['truth_plan'])
    head=ClusterHead(c,plan['control'],Path(plan['control']['token_file']).read_text().strip(),
                     lambda row,k:list(map(int,truth.ids[row,:k])))
    try:
        save_json(case/'loaded_sources.json',{k:v.__file__ for k,v in sys.modules.items()
            if k.startswith('dhdgs') and getattr(v,'__file__',None)})
        states=head.status()['nodes']
        assert all(s.get('quiescent') and s.get('accepting') and s.get('revision')==revision(head.c) for s in states)
        head.ready=True
        result=head.benchmark({k:spec[k] for k in ('queries','concurrency','warmup')})
        result['mode']=spec['name']
        save_json(case/'result.json',result)
        save_json(case/'final_status.json',head.status())
        print(json.dumps({k:result[k] for k in ('qps','latency_ms','recall_at_k_all_requests','failed_queries')}),flush=True)
        assert result['failed_queries']==0 and all(result['audit'].values())
    finally:head.close()

def snapshot(name):
    dst=ROOT/name
    dst.mkdir(exist_ok=False)
    manifest={}
    for path in [*sorted((PROJECT/'python/dhdgs').glob('*.py')),*sorted((PROJECT/'python/dhdgs').glob('*.so')),PROJECT/'configs/dhdgs.yaml']:
        rel=path.relative_to(PROJECT);target=dst/rel;target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(path,target);manifest[str(rel)]=hashlib.sha256(path.read_bytes()).hexdigest()
    (dst/'hashes.json').write_text(json.dumps(manifest,indent=2))

def suite(plan_path):
    e=yaml.safe_load(plan_path.read_text())
    sys.path[:0]=[str(PROJECT/'tools'),str(PROJECT/'python')]
    from cluster_service import load_plan
    from cluster_ctl import request
    base=load_plan(e['base_plan'])
    state=request(base,'status')
    assert state['ready'] and not state['busy'] and not state['active_queries']
    run=ROOT/e['directory'];run.mkdir(exist_ok=False)
    (run/'production_before.json').write_text(json.dumps(state,indent=2))
    for i,variant in enumerate(e['order']):
        case=run/f'{i:02d}_{variant}';case.mkdir()
        spec=dict(base_plan=e['base_plan'],name=variant,port=e['port']+32*i,
                  **{k:e[k] for k in ('queries','concurrency','warmup')})
        spec.update(e['variants'][variant])
        if e.get('repeat_selection'):spec['repeat_selection']=True
        spec['snapshot']=str((ROOT/spec['snapshot']).resolve())
        (case/'spec.json').write_text(json.dumps(spec,indent=2))
        print('START',case,flush=True)
        with (case/'execution.log').open('w') as out:
            subprocess.run([sys.executable,__file__,'--child',str(case)],stdout=out,stderr=subprocess.STDOUT,check=True,timeout=e.get('case_timeout_seconds',240))
        s=json.loads((case/'result.json').read_text())
        print(json.dumps(dict(case=case.name,qps=s['qps'],p99=s['latency_ms']['p99'],recall=s['recall_at_k_all_requests'])),flush=True)
        archive_logs(case)
    (run/'production_after.json').write_text(json.dumps(request(base,'status'),indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--child',type=Path);p.add_argument('--snapshot');p.add_argument('--plan',type=Path);p.add_argument('--archive',action='store_true')
    a=p.parse_args()
    if a.child:child(a.child)
    elif a.archive:
        for path in ROOT.glob('*/*/result.json'):archive_logs(path.parent)
    elif a.snapshot:snapshot(a.snapshot)
    else:suite(a.plan)
