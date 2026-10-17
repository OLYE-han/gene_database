"""Archive one ordinary TCP benchmark, reusing verified exact truth outside timing."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,sys,uuid
import numpy as np
from threadpoolctl import threadpool_limits
from dhdgs.config import load_config,snapshot
from dhdgs.data import write_json
import dhdgs.benchmark as benchmark
from dhdgs.cluster_test import run_local_cluster
from dhdgs.metrics import export_query_csv,export_resource_csvs

root=Path(__file__).resolve().parent
c=load_config(sys.argv[1])
cache_path=root/'exact_truth.json'
cache=json.loads(cache_path.read_text())
def cached_truth(dataset,indexed,query,k,metric,excluded_id,block_rows):
    if (dataset.identity()!=cache['dataset'] or hashlib.sha256(indexed.tobytes()).hexdigest()!=cache['indexed_ids_sha256']
            or k!=cache['top_k'] or metric!=cache['metric']):
        raise ValueError('Exact truth cache metadata mismatch')
    key=hashlib.sha256(np.asarray(query,dtype='<f4').tobytes()).hexdigest()+':'+str(excluded_id)
    row=cache['entries'][key]
    return row['ids'],row['squared_distances']
benchmark.exact_topk=cached_truth
stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
directory=Path(c['paths']['output'])/'runs'/f'tcp_{stamp}_{uuid.uuid4().hex[:8]}'
snapshot(c,directory)
with threadpool_limits(limits=c['runtime']['blas_threads']):
    report=run_local_cluster(c,directory)
report['exact_truth_lookup_seconds']=report.pop('exact_scan_seconds')
report['exact_truth_cache']={'path':str(cache_path),'sha256':hashlib.sha256(cache_path.read_bytes()).hexdigest(),
                            'precompute_seconds':cache['precompute_seconds'],'indexed_rows':cache['indexed_rows']}
write_json(directory/'report.json',report)
export_query_csv(directory/'queries.csv',report['queries'])
export_resource_csvs(directory,report)
with (directory/'queries.jsonl').open('x') as f:
    for q in report['queries']:f.write(json.dumps(q,allow_nan=False)+'\n')
print(json.dumps({'report':str(directory/'report.json'),'recall':report['recall_at_k'],
                  'mean_ms':report['latency_ms'].get('mean'),'qps':report['qps'],'failed':report['failed_queries']}),flush=True)
# Failed requests remain in reports; the runner excludes these points from selection.
