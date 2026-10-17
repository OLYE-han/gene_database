"""Fixed pilot smoke comparison; no parameter selection or performance claim."""
from pathlib import Path
import copy,hashlib,json,os,platform,socket,subprocess
import numpy as np
import yaml
from threadpoolctl import threadpool_limits
from dhdgs.config import load_config,snapshot
from dhdgs.cluster_test import run_local_cluster
from dhdgs.data import write_json
from dhdgs.metrics import export_query_csv,export_resource_csvs
import dhdgs.benchmark as benchmark

root=Path(__file__).resolve().parent
project=root.parent.parent
previous=project/'test/worker_threads_20261016T233213'
cache_path=previous/'exact_truth.json'
cache=json.loads(cache_path.read_text());additions={}
def write_progress(path, value):
    temp=path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(value,indent=2,allow_nan=False))
    temp.replace(path)

def cached_truth(dataset,indexed,query,k,metric,excluded_id,block_rows):
    if (dataset.identity()!=cache['dataset'] or hashlib.sha256(indexed.tobytes()).hexdigest()!=cache['indexed_ids_sha256']
            or k!=cache['top_k'] or metric!=cache['metric']):raise ValueError('Exact truth metadata mismatch')
    key=hashlib.sha256(np.asarray(query,dtype='<f4').tobytes()).hexdigest()+':'+str(excluded_id)
    if key not in cache['entries']:
        ids,distances=original_exact(dataset,indexed,query,k,metric,excluded_id,block_rows)
        cache['entries'][key]={'ids':ids,'squared_distances':distances}
        additions[key]=cache['entries'][key]
        write_progress(root/'exact_truth_additions.json',{'entries':additions,'indexed_ids_sha256':cache['indexed_ids_sha256'],
                   'dataset':cache['dataset'],'metric':metric,'top_k':k})
    row=cache['entries'][key]
    return row['ids'],row['squared_distances']
original_exact=benchmark.exact_topk
benchmark.exact_topk=cached_truth
base=load_config(previous/'base.yaml')
base={k:v for k,v in base.items() if not k.startswith('_')}
base['storage']['cache_bytes_per_shard']=0
base['peer']['worker_threads']=1
base['metrics']['detailed_trace']=True
base['experiment'].update(benchmark_queries=128,benchmark_repetitions=2,benchmark_concurrency=4,warmup_queries=1)
base['storage']['io'].update(worker_threads=2,max_inflight_per_device=8,
    max_inflight_bytes_per_device=8*12288,buffer_pool_bytes_per_device=4*1024*1024,
    max_inflight_per_query=4,max_submit_requests=8,fallback_backend='error')
index=project/'artifacts/metrics_tcp_20261016T062940/index'
sources=[p for folder in ('python/dhdgs','cpp') for p in (project/folder).rglob('*') if p.suffix in ('.py','.cpp','.so')]
sources += [project/'CMakeLists.txt',project/'configs/dhdgs.yaml']
hashes={str(p.relative_to(project)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
write_json(root/'provenance.json',{'source_sha256':hashes,'index_files_sha256':{
    str(p.relative_to(index)):hashlib.sha256(p.read_bytes()).hexdigest() for p in index.rglob('*') if p.is_file()},
    'truth_sha256':hashlib.sha256(cache_path.read_bytes()).hexdigest(),'truth_path':str(cache_path),
    'platform':platform.platform(),'uname':list(platform.uname()),
    'device':os.stat(index/'shard_0/nodes.bin').st_dev,
    'note':'Application cache disabled; OS page cache left intact. Closed-loop single-host test, not cold SSD or production p99 validation.'})
with (root/'hardware.txt').open('x') as log:
    for cmd in (['lscpu'],['lsblk','-o','NAME,TYPE,SIZE,ROTA,MOUNTPOINTS'],['findmnt','-T',str(index)]):
        log.write(' '.join(cmd)+'\n');log.flush();subprocess.run(cmd,stdout=log,stderr=log,check=False)
summaries=[]
for name,model,backend in [('blocking','blocking','io_uring'),('thread_pool','resumable','thread_pool'),('io_uring','resumable','io_uring')]:
    directory=root/name;directory.mkdir()
    (directory/'index').symlink_to(index,target_is_directory=True)
    c=copy.deepcopy(base);c['paths']['output']=str(directory)
    c['peer']['execution_model']=model;c['storage']['io']['backend']=backend
    sockets=[socket.socket() for _ in c['cluster']['nodes']]
    try:
        for node,sock in zip(c['cluster']['nodes'],sockets):
            sock.bind(('127.0.0.1',0));node['port']=sock.getsockname()[1]
    finally:
        for sock in sockets:sock.close()
    config_path=directory/'experiment.yaml';config_path.write_text(yaml.safe_dump(c,sort_keys=False))
    c=load_config(config_path);run=directory/'run';snapshot(c,run)
    with threadpool_limits(limits=c['runtime']['blas_threads']):report=run_local_cluster(c,run)
    report['exact_truth_preparation_seconds']=report.pop('exact_scan_seconds')
    report['new_exact_truth_entries']=len(additions)
    write_json(run/'report.json',report)
    export_query_csv(run/'queries.csv',report['queries']);export_resource_csvs(run,report)
    with (run/'queries.jsonl').open('x') as f:
        for row in report['queries']:f.write(json.dumps(row,allow_nan=False)+'\n')
    rows=report['queries'];stats=[r['stats'] for r in rows]
    complete=[s for s in stats if s.get('metrics_complete')]
    summary={'mode':name,'requests':len(rows),'failed':report['failed_queries'],
        'recall':report['recall_at_k'],'qps':report['qps'],'latency_ms':report['latency_ms'],
        'incomplete_metrics':len(stats)-len(complete),
        'physical_read_bytes':sum(n['process_deltas']['storage_read_bytes'] or 0 for n in report['node_process_load']),
        'mean_complete_counters':{k:float(np.mean([s[k] for s in complete])) if complete else None for k in
            ('file_read_bytes','pread_calls','async_read_requests','server_thread_cpu_ms','io_batch_wall_ms',
             'query_io_wait_ms','query_ready_queue_ms','expanded','peer_branch_rpc_count','wire_bytes','control_rpc_count')},
        'node_revisits':sum(s['node_revisit_count'] for s in stats),
        'router_expand_rpc_count':sum(s['router_expand_rpc_count'] for s in stats),
        'stopped':all(s['stopped'] for s in report['local_cluster']['services'])}
    for row in rows:
        if row['status']!='ok':continue
        assert row['stats']['expanded']<=c['search']['max_expansions']
        assert row['stats']['node_revisit_count']==0
        for branch in row['branch_reports']:
            assert branch['work_spent']+branch['work_unused']+sum(ch['work_grant'] for ch in branch['children'])==branch['work_grant']
            for child in branch['children']:
                assert any(d['decision_id']==child['decision_id'] and d['remote'] and d['selected_node']==child['node_id'] for d in branch['decisions'])
    summaries.append(summary);write_progress(root/'comparison.json',summaries)
    print(json.dumps(summary),flush=True)
# Refuse mixed-source comparisons.
assert hashes=={str(p.relative_to(project)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
