"""Controlled topology x head-only connection-pool experiment.

No production source edits; remote topology reuses the idle managed data nodes.
All query work/versions/peer pools are identical. Only the head pool capacity is
intervened upon, after Engine construction; record this explicitly in each run.
"""
import os
for name in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[name]='1'
from pathlib import Path
import argparse,copy,json,subprocess,sys,time
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'python'),str(P/'tools')]
from cluster_service import load_plan
from cluster_ctl import request
from dhdgs.managed_cluster import ClusterHead,admin_rpc
from dhdgs.managed_node import save_json,revision
from dhdgs.metrics import process_snapshot


def configs():
    exp=yaml.safe_load((R/'experiment.yaml').read_text())
    base=load_plan((R/exp['base_plan']).resolve())
    return exp,base


def child(case):
    exp,base=configs();details=json.loads((case/'case.json').read_text());topology=details['topology']
    plan=load_plan(R/('local.yaml' if topology=='local' else 'remote.yaml'))
    c=plan['dhdgs'];settings=plan['control'];settings['results_directory']=str(case)
    token=Path(settings['token_file']).read_text().strip()
    from full_truth import FullTruth
    truth=FullTruth(settings['truth_plan'])
    def expected(row,k):return list(map(int,truth.ids[row,:k]))
    head=ClusterHead(c,settings,token,expected)
    try:
        states=head.status()['nodes']
        if not all(s.get('quiescent') and s.get('accepting') and s.get('revision')==revision(head.c) and s.get('epoch')==head.engine.manifest['epoch'] for s in states):
            raise RuntimeError('Nodes not idle/ready or different config/index')
        # Isolate head admission transport from data->data/control pools.
        head.engine.head_hub.pool.limit=details['head_connections']
        head.ready=True
        start=process_snapshot()
        result=head.benchmark(dict(queries=exp['queries'],concurrency=details['concurrency'],warmup=exp['warmup']))
        end=process_snapshot()
        result['experiment']=details
        result['head_process']={'before':start,'after':end,'cpu_seconds':end['cpu_seconds']-start['cpu_seconds'],
            'window_seconds':(end['monotonic_ns']-start['monotonic_ns'])/1e9,
            'note':'Includes warmup and metrics/serialization; not the QPS window.'}
        save_json(case/'result.json',result)
        print(json.dumps({k:result[k] for k in ('qps','latency_ms','recall_at_k_all_requests','failed_queries')}),flush=True)
        if result['failed_queries'] or not all(result['audit'].values()):raise RuntimeError('Query audit failed')
    finally:head.close()


def suite():
    exp,base=configs();original=request(base,'status');save_json(R/'production_before.json',original)
    if not original['ready'] or original['active_queries'] or original['busy']:raise RuntimeError('Production head is busy')
    if not all(n['worker_threads']==exp['worker_threads'] and n['settings']['search.max_expansions']==exp['budget'] for n in original['nodes']):raise RuntimeError('Unexpected production query configuration')
    remote=copy.deepcopy(base);remote['dhdgs']['peer']['head_port']=exp['head_callback_port']
    remote['control']['results_directory']=str(R)
    (R/'remote.yaml').write_text(yaml.safe_dump(remote,sort_keys=False))
    local=copy.deepcopy(remote)
    for i,n in enumerate(local['dhdgs']['cluster']['nodes']):
        n.update(host='192.168.33.35',port=exp['local_ports'][i],index_directory=str(P/f'test/seq2m_full_20261017/build/index/shard_{i}'))
        h=local['deployment']['hosts'][i]
        h.update(root=str(R),ssh=None,token_file=base['control']['token_file'],state_file=str(R/f'local_node{i}.json'))
    (R/'local.yaml').write_text(yaml.safe_dump(local,sort_keys=False))
    token=Path(base['control']['token_file']).read_text().strip();procs=[];logs=[];cleanup=[]
    try:
        for n in local['dhdgs']['cluster']['nodes']:
            f=(R/f"local_node{n['id']}.log").open('a');logs.append(f)
            procs.append(subprocess.Popen([sys.executable,str(P/'tools/cluster_service.py'),'data','--plan',str(R/'local.yaml'),'--node',str(n['id'])],stdout=f,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL))
        for n in local['dhdgs']['cluster']['nodes']:
            deadline=time.monotonic()+exp['startup_timeout_seconds']
            while True:
                if any(p.poll() is not None for p in procs):raise RuntimeError('Local node exited')
                try:
                    s=admin_rpc(n,token,'admin_status',timeout=1);break
                except OSError:
                    if time.monotonic()>deadline:raise
                    time.sleep(.2)
            admin_rpc(n,token,'admin_resume',revision=s['revision'])
        for repeat in range(exp['repetitions']):
            # Alternate topology and capacity order to expose drift.
            sizes=exp['head_pool_sizes'] if repeat%2==0 else list(reversed(exp['head_pool_sizes']))
            for size in sizes:
                for topology in (['remote','local'] if repeat%2==0 else ['local','remote']):
                    case=R/'cases'/f'{topology}_p{size}_c{exp["client_concurrency"]}_r{repeat}'
                    case.mkdir(parents=True,exist_ok=False)
                    save_json(case/'case.json',dict(topology=topology,head_connections=size,concurrency=exp['client_concurrency'],repeat=repeat))
                    print('START',case.name,flush=True)
                    with (case/'execution.log').open('w') as log:
                        subprocess.run([sys.executable,__file__,'--child',str(case)],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=exp['run_timeout_seconds'])
                    result=json.loads((case/'result.json').read_text())
                    print(json.dumps({'case':case.name,'qps':result['qps'],'p99_ms':result['latency_ms']['p99'],'recall':result['recall_at_k_all_requests']}),flush=True)
        save_json(R/'production_after.json',request(base,'status'))
    finally:
        for n in local['dhdgs']['cluster']['nodes']:
            try:admin_rpc(n,token,'admin_shutdown',timeout=5);cleanup.append({'node':n['id'],'stopped':True})
            except Exception as exc:cleanup.append({'node':n['id'],'error':str(exc)})
        for p in procs:
            try:p.wait(timeout=15)
            except subprocess.TimeoutExpired:p.terminate();p.wait(timeout=10)
        for f in logs:f.close()
        save_json(R/'local_cleanup.json',cleanup)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--child',type=Path);args=parser.parse_args()
    if args.child:child(args.child)
    else:suite()
