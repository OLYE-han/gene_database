"""Full-index local three-process native validation; no index rebuild or global cache drop."""
import argparse
import copy
import gzip
import hashlib
import json
from pathlib import Path
import socket
import statistics
import subprocess
import sys
import time
import traceback

ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parent.parent
sys.path[:0]=[str(PROJECT/'python'),str(PROJECT/'tools')]
import yaml
from threadpoolctl import threadpool_limits
from deploy_cluster import save,digest
from full_truth import FullTruth,select_queries
from dhdgs.config import load_config,snapshot
from dhdgs.cluster_test import run_local_cluster
from dhdgs.metrics import export_query_csv,export_resource_csvs


def run_point(plan,truth,rows,name,budget,frontier,workers,concurrency,model='native',backend='sync_pread'):
    out=ROOT/name
    if (out/'summary.json').exists():return json.loads((out/'summary.json').read_text())
    out.mkdir(parents=True,exist_ok=False)
    index=Path(plan['index_source']);(out/'index').symlink_to(index,target_is_directory=True)
    c=copy.deepcopy(plan['dhdgs']);c['paths']['output']=str(out)
    c['search'].update(max_expansions=budget,frontier_size=frontier)
    c['peer'].update(execution_model=model,worker_threads=workers,head_bind_host='127.0.0.1',head_advertise_host='127.0.0.1')
    c['storage']['io']['backend']=backend
    c['experiment'].update(benchmark_queries=len(rows),benchmark_concurrency=concurrency,benchmark_repetitions=1)
    sockets=[]
    try:
        for node in c['cluster']['nodes']:
            sock=socket.socket();sock.bind(('127.0.0.1',0));sockets.append(sock)
            node.update(host='127.0.0.1',port=sock.getsockname()[1],index_directory=str(index/f"shard_{node['id']}"))
        (out/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False));c=load_config(out/'experiment.yaml')
        run=out/'run';snapshot(c,run)
    finally:
        for sock in sockets:sock.close()
    save(ROOT/'status.json',dict(phase=name,started=time.time()))
    with threadpool_limits(limits=c['runtime']['blas_threads']):
        report=run_local_cluster(c,run,truth_provider=truth,query_rows=rows)
    report['ground_truth_source']=truth.provenance
    if 'exact_scan_seconds' in report:
        report['truth_preparation_seconds']=report.pop('exact_scan_seconds')
    archives=[]
    for file,body in [('report.json',json.dumps(report,allow_nan=False)),
                      ('queries.jsonl',''.join(json.dumps(q,allow_nan=False)+'\n' for q in report['queries']))]:
        raw=body.encode()
        with gzip.open(run/(file+'.gz'),'wb',compresslevel=1) as f:f.write(raw)
        archives.append(dict(file=file,uncompressed_sha256=hashlib.sha256(raw).hexdigest(),uncompressed_bytes=len(raw)))
    save(run/'archive.json',archives)
    export_query_csv(run/'queries.csv',report['queries']);export_resource_csvs(run,report)
    failures=report['failed_queries'];incomplete=sum(not q['stats'].get('metrics_complete',False) for q in report['queries'])
    errors=[]
    for q in report['queries']:
        if q['status']!='ok':continue
        s=q['stats']
        if s['expanded']>budget or s['node_revisit_count'] or s['router_expand_rpc_count']:errors.append(q['request_id'])
        if q['duplicate_result_ids']:errors.append(q['request_id'])
        for b in q['branch_reports']:
            if b['work_spent']+b['work_unused']+sum(ch['work_grant'] for ch in b['children'])!=b['work_grant']:errors.append(q['request_id'])
            if model=='native' and (not b['native_search_loop'] or b['compute_thread_migrations']):errors.append(q['request_id'])
            for ch in b['children']:
                if sum(d['remote'] and d['decision_id']==ch['decision_id'] and d['selected_node']==ch['node_id'] for d in b['decisions'])!=1:errors.append(q['request_id'])
    nodes=report['node_process_load']  # TCP: one shared-service snapshot per node, worker=None
    assert len(nodes)==len(c['cluster']['nodes'])
    complete=[q['stats'] for q in report['queries'] if q['status']=='ok' and q['stats'].get('metrics_complete')]
    keys=('server_thread_cpu_ms','pread_ms','rabitq_ms','exact_distance_ms','file_read_bytes',
          'server_to_server_branches','all_protocol_bytes','all_protocol_rpc_count','expanded','query_io_wait_ms')
    result=dict(name=name,scope='one physical host, three TCP data processes',model=model,backend=backend,
                budget=budget,frontier=frontier,threads=workers,concurrency=concurrency,queries=len(rows),
                qps=report['qps'],recall=report['recall_at_k'],latency_ms=report['latency_ms'],
                failures=failures,incomplete_metrics=incomplete,invariant_errors=len(errors),nodes=nodes,
                cpu_cores_sum=sum(n['mean_cpu_cores'] for n in nodes),
                physical_read_bytes=sum(n['process_deltas']['storage_read_bytes'] for n in nodes),
                means={k:statistics.mean(s.get(k,0) for s in complete) if complete else None for k in keys},
                query_timing_summary=report['query_timing_summary'])
    save(out/'summary.json',result);print(json.dumps({k:v for k,v in result.items() if k not in ('nodes','query_timing_summary')}),flush=True)
    if failures or incomplete or errors:raise RuntimeError(f'{name}: invalid measurements, see raw records')
    return result


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--phase',choices=['regression','experiment'],default='experiment');args=parser.parse_args()
    if args.phase=='regression':
        import os
        env=dict(os.environ,PYTHONPATH=str(PROJECT/'python'),DHDGS_TEST_URING='1')
        with (ROOT/'regression_final.log').open('w') as out:
            subprocess.run([sys.executable,'-m','unittest','discover','-s',str(PROJECT/'tests'),'-v'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True)
        return
    plan=yaml.safe_load((ROOT/'experiment.yaml').read_text())
    sources=[p for folder in ('cpp','python/dhdgs') for p in (PROJECT/folder).rglob('*') if p.suffix in ('.py','.cpp','.hpp')]
    hashes={str(p.relative_to(PROJECT)):digest(p) for p in sources}
    marker={'sources':hashes,'plan':digest(ROOT/'experiment.yaml'),'runner':digest(Path(__file__)),'native':digest(next((PROJECT/'python/dhdgs').glob('_native*.so')))}
    if (ROOT/'sources.json').exists():
        if json.loads((ROOT/'sources.json').read_text())!=marker:raise ValueError('Experiment sources changed')
    else:save(ROOT/'sources.json',marker)
    truth=FullTruth(plan['baseline_plan']);selection=select_queries(plan['baseline_plan'])
    save(ROOT/'query_selection.json',selection)
    cal=plan['calibration'];results=[]
    for budget in cal['budgets']:
        results.append(run_point(plan,truth,selection['calibration'],f'calibration/b{budget}',budget,cal['frontier_size'],cal['worker_threads'],cal['client_concurrency']))
    eligible=[r for r in results if r['recall']>=plan['dhdgs']['experiment']['target_recall']]
    if not eligible:raise RuntimeError('Calibration target not met; inspect before choosing a budget')
    chosen=min(eligible,key=lambda r:(r['latency_ms']['p99'],-r['qps']))
    save(ROOT/'selection.json',chosen)
    for mode in plan['validation']['modes']:
        results.append(run_point(plan,truth,selection['validation'],'validation/'+mode['name'],chosen['budget'],chosen['frontier'],mode['threads'],plan['validation']['client_concurrency'],mode['model'],mode['backend']))
        save(ROOT/'comparison.json',results)
    save(ROOT/'completed.json',{'points':len(results),'requests':sum(r['queries'] for r in results),
                                'all_valid':True,'all_validation_targets_met':all(r['recall']>=.95 for r in results if r['name'].startswith('validation/'))})
    save(ROOT/'status.json',{'phase':'complete','updated':time.time()})


if __name__=='__main__':
    try:main()
    except BaseException:
        save(ROOT/'failure.json',{'error':traceback.format_exc()})
        raise
