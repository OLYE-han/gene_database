"""Head-local timeline dissection; never subtract cross-host clocks."""
from pathlib import Path
import csv,gzip,json
import numpy as np
R=Path(__file__).resolve().parent

def dist(v):return {'mean':float(np.mean(v)),'p50':float(np.percentile(v,50)),'p99':float(np.percentile(v,99)),'max':float(max(v))}
rows=[]
for case in sorted((R/'cases').glob('*')):
    if not (case/'result.json').exists():continue
    s=json.loads((case/'result.json').read_text());source=Path(s['directory'])
    with gzip.open(source/'queries.jsonl.gz','rt') as f:qs=[json.loads(line) for line in f]
    ok=[q for q in qs if q['status']=='ok'];detail={'case':case.name,**s['experiment']}
    stages={k:[] for k in ('routing','entry_pool','entry_socket','report_after_entry_ack','head_merge_return','head_other','entry_return','data_ready','data_compute','data_pread')}
    bynode={sid:[] for sid in (0,1,2)}
    for q in ok:
        st=q['stats'];t=st['head_timestamps'];rpc=st['entry_rpc']
        stages['routing'].append((t['routing_done_ns']-t['search_enter_ns'])/1e6)
        stages['entry_pool'].append(rpc['pool_wait_ms'])
        stages['entry_socket'].append(rpc['wall_ms']-rpc['pool_wait_ms'])
        stages['report_after_entry_ack'].append((t['all_reports_received_ns']-t['entry_submit_return_ns'])/1e6)
        stages['head_merge_return'].append((t['search_return_ns']-t['all_reports_received_ns'])/1e6)
        stages['head_other'].append(q['client_wall_ms']-sum(stages[k][-1] for k in ('routing','entry_pool','entry_socket','report_after_entry_ack','head_merge_return')))
        stages['entry_return'].append((t['entry_submit_return_ns']-rpc['response_received_ns'])/1e6)
        stages['data_ready'].append(st['query_ready_queue_ms']);stages['data_compute'].append(st['query_compute_slice_wall_ms']);stages['data_pread'].append(st['pread_ms'])
        root=next(b for b in q['branch_reports'] if b['parent_branch_id'] is None)
        bynode[root['node_id']].append(q)
    detail.update(qps=s['qps'],mean_ms=s['latency_ms']['mean'],p99_ms=s['latency_ms']['p99'],recall=s['recall_at_k_all_requests'],failed=s['failed_queries'],head_cpu_window=s['head_process'])
    detail['stages_ms']={k:dist(v) for k,v in stages.items()}
    detail['entry_pool_fraction']=sum(stages['entry_pool'])/sum(q['client_wall_ms'] for q in ok)
    detail['physical_read_bytes']=[n['process_deltas']['storage_read_bytes'] for n in s['node_process_load']]
    detail['entry_by_node']={sid:{'count':len(q),'latency_ms':dist([x['client_wall_ms'] for x in q]),'pool_ms':dist([x['stats']['entry_rpc']['pool_wait_ms'] for x in q]),'rpc_response_ms':dist([x['stats']['entry_rpc']['response_wait_ms'] for x in q])} for sid,q in bynode.items()}
    with gzip.open(source/'nodes_after.json.gz','rt') as f:nodes=json.load(f)
    qids={q['request_id'] for q in qs};control={}
    for sid,node in nodes.items():
        life=node['peer_lifecycle'];tr=[t for t in life['execution_timestamps'] if t['query_request_id'] in qids]
        ev=[e for e in life['control_events'] if e['query_request_id'] in qids and e['kind']=='result' and e.get('succeeded')]
        control[sid]={k:dist([e['rpc_counts'][k] for e in ev]) for k in ('pool_wait_ms','send_ms','response_wait_ms')}
        selected=[(t['control_result_selected_ns']-t['native_report_ready_ns'])/1e6 for t in tr if 'control_result_selected_ns' in t and 'native_report_ready_ns' in t]
        control[sid]['report_ready_to_selected_ms']=dist(selected)
    detail['result_channel_ms']=control
    rows.append(detail);(case/'analysis.json').write_text(json.dumps(detail,indent=2))
(R/'analysis.json').write_text(json.dumps(rows,indent=2))
if rows:
    with (R/'comparison.csv').open('w') as f:
        keys=['case','topology','head_connections','concurrency','repeat','qps','mean_ms','p99_ms','recall','failed','entry_pool_fraction']
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows({k:r[k] for k in keys} for r in rows)
for r in rows:print(r['case'],round(r['qps'],1),round(r['p99_ms'],1),{k:round(v['mean'],2) for k,v in r['stages_ms'].items()})
