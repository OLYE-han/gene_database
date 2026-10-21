"""Select the actual query nearest a run's overall p99; use only local clocks."""
import argparse,csv,gzip,json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

parser=argparse.ArgumentParser()
parser.add_argument('--case',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
a=parser.parse_args();a.output.mkdir(parents=True,exist_ok=True)
summary=json.loads((a.case/'result.json').read_text());run=Path(summary['directory']);target=summary['latency_ms']['p99']
q=None;gap=float('inf');count=0
with gzip.open(run/'queries.jsonl.gz','rt') as f:
    for line in f:
        candidate=json.loads(line);count+=1;distance=abs(candidate['client_wall_ms']-target)
        if distance<gap:q=candidate;gap=distance
assert q is not None and q['status']=='ok'
with gzip.open(run/'nodes_after.json.gz','rt') as f:nodes=json.load(f)
branches=[]
for report in sorted(q['branch_reports'],key=lambda b:(b['parent_branch_id'] is not None,b['node_id'])):
    life=nodes[str(report['node_id'])]['peer_lifecycle']
    traces=[t for t in life['execution_timestamps'] if t['branch_id']==report['branch_id'] and t['query_request_id']==q['request_id']]
    assert len(traces)==1
    events=[e for e in life['control_events'] if e['branch_id']==report['branch_id'] and e['query_request_id']==q['request_id']]
    branches.append(dict(report=report,trace=traces[0],events=events))
selected=dict(source_case=str(a.case.resolve()),source_run=str(run),selection='closest latency to overall per-run p99; no cross-node filter',query_count=count,target_p99_ms=target,distance_to_p99_ms=gap,query=q,branches=branches)
(a.output/'selected.json').write_text(json.dumps(selected,indent=2))
t=q['stats']['native_head_timestamps'];origin=q['client_start_ns']
points=[('API submission',origin),('Route queue',t['search_enter_ns']),('HNSW routing',t['route_started_ns']),('Entry dispatch incl. queue',t['routing_done_ns']),('Data / RPC / control',t['entry_send_done_ns']),('Report validation / completion gating',t['last_report_received_ns']),('Merge queue',t['merge_enqueued_ns']),('Merge',t['merge_started_ns']),('API result handoff',t['native_ready_ns']),('Done',q['api_future_done_ns'])]
head=[]
for (label,start),(_,end) in zip(points,points[1:]):
    assert end>=start,(label,start,end)
    head.append(dict(label=label,start_ms=(start-origin)/1e6,ms=(end-start)/1e6,fraction=(end-start)/(q['api_future_done_ns']-origin)))
assert abs(sum(s['ms'] for s in head)-q['client_wall_ms'])<1e-5
fig,axs=plt.subplots(1+len(branches),1,figsize=(17,4+4.4*len(branches)),layout='constrained',gridspec_kw={'height_ratios':[1.1]+[2]*len(branches)})
axes=list(axs) if hasattr(axs,'__iter__') else [axs];colors=plt.get_cmap('tab20').colors
for i,s in enumerate(head):
    axes[0].barh(0,s['ms'],left=s['start_ms'],height=.5,color=colors[i],label=f"{s['label']}: {s['ms']:.3f} ms ({s['fraction']*100:.1f}%)")
    if s['ms']>4:axes[0].text(s['start_ms']+s['ms']/2,0,f"{s['ms']:.2f}",ha='center',va='center',fontsize=9)
axes[0].set_yticks([0],['Head / API']);axes[0].set_title('Intel-35 head clock: mutually exclusive end-to-end stages')
axes[0].set_xlabel('ms from client submission');axes[0].legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8)
records=[];hosts={0:'Intel-35',1:'Intel-32',2:'Intel-30'}
for ax,b in zip(axes[1:],branches):
    t=b['trace'];origin=t['receiver_message_ready_ns'];events=[e for e in b['events'] if e['kind']=='result' and e['succeeded']];assert events
    ev=min(events,key=lambda e:e['sender_rpc_return_ns']);stages=[]
    def add(lane,label,start,end):
        assert end>=start,(label,start,end)
        stages.append(dict(lane=lane,label=label,start_ms=(start-origin)/1e6,ms=(end-start)/1e6))
    add(0,'Receive queue / validation',origin,t['accepted_ns'])
    add(0,'Admission + registration',t['accepted_ns'],t['enqueued_ns'])
    add(0,'Compute queue + setup',t['enqueued_ns'],t['native_search_start_ns'])
    add(1,'Search loop',t['native_search_start_ns'],t['native_loop_done_ns'])
    add(1,'Report build / worker release',t['native_loop_done_ns'],t['compute_released_ns'])
    add(2,'Completion command wait',t['local_done_enqueued_ns'],t['local_done_dequeued_ns'])
    add(2,'Finalize incl. pending transfer',t['local_done_dequeued_ns'],t['report_ready_ns'])
    # message_ready_ns is recorded AFTER async submit, so it is not a queue boundary.
    wire_queued_ns=ev['sender_enter_ns']-round(ev['rpc_counts']['pool_wait_ms']*1e6)
    send_done_ns=ev['sender_enter_ns']+round(ev['rpc_counts']['send_ms']*1e6)
    add(2,'Schedule + encode / submit',t['report_ready_ns'],wire_queued_ns)
    add(2,'Result outbound queue / event loop',wire_queued_ns,ev['sender_enter_ns'])
    add(2,'Result send incl. connect',ev['sender_enter_ns'],send_done_ns)
    add(2,'Result response / ACK wait',send_done_ns,ev['sender_rpc_return_ns'])
    if 'transfer_worker_enter_ns' in t:add(3,'Cross-node submit to ACK callback',t['transfer_worker_enter_ns'],t['transfer_worker_return_ns'])
    for i,s in enumerate(stages):
        ax.barh(s['lane'],s['ms'],left=s['start_ms'],height=.5,color=colors[i],label=f"{s['label']}: {s['ms']:.3f} ms")
        if s['ms']>4:ax.text(s['start_ms']+s['ms']/2,s['lane'],f"{s['ms']:.2f}",ha='center',va='center',fontsize=9)
    release=(t['compute_released_ns']-origin)/1e6;ax.axvline(release,color='black',ls='--',label=f'Compute worker released: {release:.3f} ms')
    role='PARENT' if b['report']['parent_branch_id'] is None else 'CHILD'
    ax.set_yticks([0,1,2,3],['Receive / ready','Compute worker','Completion / report','Cross-node RPC']);ax.invert_yaxis()
    ax.set_title(f"{hosts[b['report']['node_id']]} {role}: local clock, independent origin; lanes overlap")
    ax.set_xlabel('ms from THIS node receiving the complete request frame (not aligned to other panels)')
    ax.legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8)
    records.append(dict(node_id=b['report']['node_id'],role=role,worker_release_ms=release,stages=stages))
for ax in axes:ax.grid(axis='x',alpha=.2)
fig.suptitle(f"{a.case.name} | query {q['query_id']} / job {q['job']}\nRun p99 {target:.6f} ms; selected query {q['client_wall_ms']:.6f} ms; Recall@10 {q['recall']:.2f}",fontsize=13)
fig.savefig(a.output/'timeline.png',dpi=170);fig.savefig(a.output/'timeline.svg');plt.close(fig)
(a.output/'stages.json').write_text(json.dumps(dict(head=head,branches=records),indent=2))
with (a.output/'stages.csv').open('w') as f:
    writer=csv.writer(f);writer.writerow(['clock','lane','stage','start_ms','duration_ms'])
    for s in head:writer.writerow(['head',0,s['label'],s['start_ms'],s['ms']])
    for b in records:
        for s in b['stages']:writer.writerow([f"node_{b['node_id']}",s['lane'],s['label'],s['start_ms'],s['ms']])
lines=['# p99真实查询时序分解','',f"来源：{a.case.name}，{count}条查询。选择全体查询中服务延迟最接近本轮p99的记录；不筛选跨节点条件，也不使用多轮p99均值选择样本。",'',f"本轮p99为{target:.9f}ms；query_id={q['query_id']}、job={q['job']}，延迟{q['client_wall_ms']:.9f}ms，相差{gap:.9f}ms，Recall@10={q['recall']:.2f}。",'','![时序图](timeline.png)','','顶层面板使用同一主机时钟，阶段互斥且总和等于端到端服务延迟。每个数据节点单独以收到完整报文为零点，不能跨主机直接对齐时间轴或相加。节点内计算、控制、派发存在重叠；虚线表示计算线程释放时刻，之后的控制/网络等待不占用该计算线程。结果ACK时刻与用户Future完成时刻也不同。','','|顶层阶段|耗时ms|占服务延迟|','|---|---:|---:|']
for s in head:lines.append(f"|{s['label']}|{s['ms']:.6f}|{s['fraction']*100:.2f}%|")
for b in records:
    lines += ['',f"## {hosts[b['node_id']]} / {b['role']}",'','|阶段|本节点相对起点ms|耗时ms|','|---|---:|---:|']
    for s in b['stages']:lines.append(f"|{s['label']}|{s['start_ms']:.6f}|{s['ms']:.6f}|")
lines += ['','结果RPC排队起点由 sender_enter_ns - round(pool_wait_ms * 1e6) 还原；pool_wait_ms来自底层同一主机时钟的queued_ns与send_start_ns之差。旧message_ready_ns是在异步提交成功后记录，网络线程可能已开始发送，不能用它直接计算发送前队列。还原数值保存在stages.json/CSV，未改写原始时间戳。','','接收队列/验证、完成控制和发送前等待均为观测区间，可能包含OS调度及状态锁等待，不能据图将其全部认定为锁、CPU计算或纯链路传输。单条p99附近查询用于定位现象，不代表所有尾延迟样本。selected.json保留完整源记录，stages.json/CSV保存精确阶段数值。','']
(a.output/'REPORT.md').write_text('\n'.join(lines))
print(json.dumps(dict(case=a.case.name,query_id=q['query_id'],job=q['job'],target_p99_ms=target,latency_ms=q['client_wall_ms'],branches=[b['node_id'] for b in records],head=head,branch_stages=records),indent=2))
