"""Validate the requested persistent Intel-35 + Intel-30 topology via the head.

Services are left running with restored budget 200 and 32 workers per node.
"""
from pathlib import Path
import json,sys
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path.insert(0,str(P/'tools'));sys.path.insert(0,str(P/'python'))
from cluster_service import load_plan
from cluster_ctl import request
from dhdgs.managed_node import save_json
plan=load_plan(R/'cluster.yaml')
initial=request(plan,'status');assert initial['ready']
save_json(R/'initial_status.json',initial)
a=initial['nodes'];assert a[0]['hostname']!=a[1]['hostname']==a[2]['hostname']
assert a[1]['pid']!=a[2]['pid'] and all(n['worker_threads']==32 for n in a)
results={}
try:
    baseline=request(plan,'benchmark',dict(queries=2000,concurrency=128,warmup=32))
    save_json(R/'baseline.json',baseline);results['baseline']=baseline['directory']
    print(json.dumps({k:baseline[k] for k in ('directory','qps','successful_queries','failed_queries','latency_ms','recall_at_k_all_requests')}),flush=True)
    changed=request(plan,'configure',{'search.max_expansions':160,'scheduler.remote_weight':.5,'peer.worker_threads':32})
    save_json(R/'changed_status.json',changed)
    for old,new in zip(a,changed['nodes']):
        assert (old['pid'],old['index_fd'])==(new['pid'],new['index_fd'])
        assert new['settings']['search.max_expansions']==160 and new['settings']['scheduler.remote_weight']==.5
    follow=request(plan,'benchmark',dict(queries=2000,concurrency=128,warmup=32))
    save_json(R/'changed_benchmark.json',follow);results['changed']=follow['directory']
    print(json.dumps({k:follow[k] for k in ('directory','qps','successful_queries','failed_queries','latency_ms','recall_at_k_all_requests')}),flush=True)
finally:
    request(plan,'configure',{'search.max_expansions':200,'scheduler.remote_weight':1.,'peer.worker_threads':32})
final=request(plan,'status');save_json(R/'final_status.json',final)
for old,new in zip(a,final['nodes']):assert (old['pid'],old['index_fd'])==(new['pid'],new['index_fd'])
collected=request(plan,'collect');save_json(R/'collection.json',collected)
save_json(R/'verification.json',dict(topology='Intel-35: head + data0; Intel-30: data1 + data2',worker_threads_per_node=32,client_concurrency=128,same_pid_and_fd=True,restored=True,services_left_running=True,**results))
assert all(v['successful_queries']==2000 and v['failed_queries']==0 and all(v['audit'].values()) for v in (baseline,follow))
