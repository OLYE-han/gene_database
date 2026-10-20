"""Measure actual parallel CPU use without head/TCP throughput limiting arrival."""
import copy
import json
import os
from pathlib import Path
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

ROOT=Path(__file__).resolve().parent;PROJECT=ROOT.parent.parent
sys.path[:0]=[str(PROJECT/'python'),str(PROJECT/'tools')]
import numpy as np
import yaml
from threadpoolctl import threadpool_limits
from deploy_cluster import save,digest
from dhdgs.index import Shard
from dhdgs.native_search import prepare_shard,run_native
from dhdgs.metrics import process_snapshot


def main():
    p=yaml.safe_load((ROOT/'cpu_diagnostic.yaml').read_text())
    plan=yaml.safe_load((ROOT/p['experiment_plan']).read_text())
    selected=json.loads((ROOT/p['budget_selection']).read_text())
    c=copy.deepcopy(plan['dhdgs']);c['search'].update(max_expansions=selected['budget'],frontier_size=selected['frontier'],fallback_enabled=False)
    c['metrics']['detailed_trace']=p['trace'];c['peer']['execution_model']='native'
    sid=p['shard_id'];shard=Shard(Path(plan['index_source'])/f'shard_{sid}',0)
    # The sidecar supplies the immutable owner bank directly; no Python owner
    # dictionary is needed by the native hot loop for this diagnostic.
    prepare_shard(shard,{})
    vectors=np.load(c['query']['vectors'],mmap_mode='r');ids=np.load(c['query']['ids'],mmap_mode='r')
    selection=np.arange(p['query_start_row'],p['query_start_row']+p['query_count'])
    q=np.array(vectors[selection]);qids=np.array(ids[selection]);seed=int(shard.ids[0])
    def query(i):
        i%=len(q)
        state={'query_request_id':str(i),'branch_id':str(i),'parent_branch_id':None,
               'target_node':sid,'entry_node':sid,'visited_nodes':[sid],'epoch':shard.meta['epoch'],
               'query_f32':q[i].tobytes(),'query_id':int(qids[i]),'work_grant':selected['budget'],
               'branch_slots':0,'seeds':[[seed,0.,0.,[sid],None,0,0,seed,0.]],'seen':[seed],
               'remote_frontier':[],'fallback_entries':[],'fallback_remaining':0,
               'scale':max(1.,float(np.dot(q[i],q[i]))),'decision_seed':c['seed']}
        tid=threading.get_native_id()
        result=run_native(state,shard,{},c,lambda *_:(_ for _ in ()).throw(AssertionError('Unexpected remote')),lambda:False)
        assert result['compute_thread_id']==tid and result['compute_thread_migrations']==0
        return (result['stats']['server_thread_cpu_ms'],result['work_spent'],tid)
    results=[]
    try:
        with threadpool_limits(limits=1):
            for i in range(p['warmup_queries']):query(i)
            for threads in p['threads']:
                with ThreadPoolExecutor(max_workers=threads) as pool:
                    before=process_snapshot();start=time.perf_counter()
                    rows=list(pool.map(query,range(p['query_count']*p['repetitions'])))
                    wall=time.perf_counter()-start;after=process_snapshot()
                result={'threads':threads,'queries':len(rows),'wall_seconds':wall,'qps':len(rows)/wall,
                        'process_cpu_cores':(after['cpu_seconds']-before['cpu_seconds'])/wall,
                        'search_thread_cpu_cores':sum(r[0] for r in rows)/1000/wall,
                        'compute_thread_ids':sorted({r[2] for r in rows}),
                        'expanded':sum(r[1] for r in rows),
                        'physical_read_bytes':after['storage_read_bytes']-before['storage_read_bytes']}
                results.append(result);print(json.dumps(result),flush=True)
        save(ROOT/'cpu_diagnostic.json',{'scope':'single data-node fixed local entry; no router/TCP/remote branches; not a recall or end-to-end QPS benchmark',
            'budget':selected['budget'],'frontier':selected['frontier'],'settings':p,'results':results,
            'native_sha256':digest(next((PROJECT/'python/dhdgs').glob('_native*.so'))),
            'config_sha256':digest(ROOT/'cpu_diagnostic.yaml'),'available_cpu_affinity_count':len(os.sched_getaffinity(0))})
    finally:shard.close()


if __name__=='__main__':main()
