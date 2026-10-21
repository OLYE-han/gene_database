"""Lock/IPC/GC attribution after all timed workloads have stopped."""
import csv
import gzip
import importlib.util
import json
from pathlib import Path

import numpy as np

ROOT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('phases',ROOT.parent/'control_capacity_20261020/analyze.py')
phases=importlib.util.module_from_spec(spec);spec.loader.exec_module(phases)
query_rows=[];node_rows=[];examples=[]


def summary(values):
    return dict(mean=float(np.mean(values)),p99=float(np.percentile(values,99)),max=float(max(values))) if values else None


for path in sorted(ROOT.glob('*/*/result.json')):
    case=str(path.parent.relative_to(ROOT));result=json.loads(path.read_text());run=Path(result['directory'])
    gc=json.loads((run/'head_gc.json').read_text())
    values={k:[] for k in ['emit_prepare','encode','write_lock','send_receive','decode','output_ipc']}
    formal=set();worst=None;count=0
    with gzip.open(run/'queries.jsonl.gz','rt') as f:
        for line in f:
            q=json.loads(line);formal.add(q['request_id']);count+=1;t=q['stats']['head_lane']
            for name,a,b in [('emit_prepare','worker_finished_ns','ipc_pack_enter_ns'),
                             ('encode','ipc_pack_enter_ns','ipc_pack_done_ns'),
                             ('write_lock','ipc_write_lock_enter_ns','ipc_write_lock_acquired_ns'),
                             ('send_receive','ipc_write_lock_acquired_ns','ipc_receive_done_ns'),
                             ('decode','ipc_receive_done_ns','ipc_decode_done_ns'),
                             ('output_ipc','worker_finished_ns','parent_received_ns')]:
                assert t[b]>=t[a],(case,name,q['request_id'])
                values[name].append((t[b]-t[a])/1e6)
            if worst is None or q['client_wall_ms']>worst['client_wall_ms']:worst=q
    row=dict(case=case,queries=count,qps=result['qps'],p99_ms=result['latency_ms']['p99'])
    row['ipc']={k:summary(v) for k,v in values.items()}
    row['parent_gc_wall_ms_delta']=gc['after']['wall_ms']-gc['before']['wall_ms']
    row['parent_gc_cpu_ms_delta']=gc['after']['cpu_ms']-gc['before']['cpu_ms']
    t=worst['stats']['head_lane'];start=t['worker_finished_ns'];end=t['parent_received_ns']
    overlaps=[dict(**e,overlap_ms=max(0,min(e['end_ns'],end)-max(e['start_ns'],start))/1e6)
              for e in gc['after']['events'] if e['start_ns']<end and e['end_ns']>start]
    row['slowest_query']=dict(query_id=worst['query_id'],request_id=worst['request_id'],
                             client_ms=worst['client_wall_ms'],output_ipc_ms=(end-start)/1e6,
                             ipc_gc_overlaps=overlaps)
    query_rows.append(row)
    selected=path.parent/'slowest_query.json';selected.write_text(json.dumps(worst,indent=2))
    (path.parent/'slowest_head_timeline.json').write_text(json.dumps(phases.timeline(worst),indent=2))
    nodes=json.load(gzip.open(run/'nodes_after.json.gz','rt'))
    for sid,node in nodes.items():
        traces=[t for t in node['peer_lifecycle']['execution_timestamps'] if t['query_request_id'] in formal]
        nr=dict(case=case,node=sid,traces=len(traces))
        for name,a,b in [('reserve_wait','accept_state_lock_enter_ns','accept_state_lock_acquired_ns'),
                         ('reserve_hold','accept_state_lock_acquired_ns','accept_state_lock_released_ns'),
                         ('commit_wait','accept_commit_lock_enter_ns','accept_commit_lock_acquired_ns'),
                         ('commit_hold','accept_commit_lock_acquired_ns','accept_commit_lock_released_ns'),
                         ('native_register_python','accept_native_register_enter_ns','accept_native_register_return_ns'),
                         ('native_register_cpp','native_register_start_ns','native_register_end_ns'),
                         ('accept','receiver_accept_enter_ns','receiver_accept_return_ns'),
                         ('admission','enqueued_ns','dequeued_ns'),
                         ('submit_lock','submit_state_lock_enter_ns','submit_state_lock_acquired_ns'),
                         ('executor','executor_enqueue_ns','executor_dequeue_ns'),
                         ('worker_lock','worker_enter_ns','worker_state_lock_acquired_ns'),
                         ('native','native_search_start_ns','native_loop_done_ns')]:
            nr[name]=summary([(t[b]-t[a])/1e6 for t in traces if a in t and b in t])
        for name in ['native_register_state_wait_ns','native_register_queue_wait_ns','native_register_lock_held_ns']:
            nr[name]=summary([t[name]/1e6 for t in traces if name in t])
        nr['search_workers']=node['peer_lifecycle']['compute_queue']['workers'];assert nr['search_workers']==32
        nr['slowest_query_traces']=[t for t in traces if t['query_request_id']==worst['request_id']]
        profile=node.get('gc_profile') or {}
        nr['gc_wall_ms']=profile.get('wall_ms')
        nr['gc_cpu_ms']=profile.get('cpu_ms')
        nr['longest_lock_waits']=[]
        for a,b in [('accept_state_lock_enter_ns','accept_state_lock_acquired_ns'),
                    ('submit_state_lock_enter_ns','submit_state_lock_acquired_ns'),
                    ('worker_enter_ns','worker_state_lock_acquired_ns')]:
            eligible=[t for t in traces if a in t and b in t]
            if not eligible:continue
            tr=max(eligible,key=lambda t:t[b]-t[a]);left,right=tr[a],tr[b]
            events=[dict(**e,overlap_ms=max(0,min(e['end_ns'],right)-max(e['start_ns'],left))/1e6)
                    for e in profile.get('events',[]) if e['start_ns']<right and e['end_ns']>left]
            nr['longest_lock_waits'].append(dict(stage=a,query_request_id=tr['query_request_id'],
                start_ns=left,end_ns=right,wall_ms=(right-left)/1e6,gc_overlaps=events))
        node_rows.append(nr)
(ROOT/'ipc_gc.json').write_text(json.dumps(query_rows,indent=2))
(ROOT/'lock_metrics.json').write_text(json.dumps(node_rows,indent=2))
flat=[]
for row in node_rows:
    f={k:row[k] for k in ['case','node','traces','search_workers']}
    for k,v in row.items():
        if isinstance(v,dict):f.update({k+'_'+s:v[s] for s in ['mean','p99','max']})
    flat.append(f)
with (ROOT/'lock_metrics.csv').open('w') as f:
    w=csv.DictWriter(f,fieldnames=list(dict.fromkeys(k for r in flat for k in r)));w.writeheader();w.writerows(flat)
print(json.dumps(query_rows,indent=2))
