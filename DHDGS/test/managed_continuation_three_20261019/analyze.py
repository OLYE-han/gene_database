"""Export repeated metrics and a query timeline without cross-host subtraction."""
from pathlib import Path
import csv,gzip,json
import numpy as np
import yaml
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
R=Path(__file__).resolve().parent
e=yaml.safe_load((R/'experiment.yaml').read_text())
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'axes.unicode_minus':False,'svg.fonttype':'path'})
def read_gz(p):
    with gzip.open(p,'rt') as f:return json.load(f)
def write_csv(p,rows):
    with p.open('w') as f:
        w=csv.DictWriter(f,list(rows[0]));w.writeheader();w.writerows(rows)
def save(p,v):p.write_text(json.dumps(v,ensure_ascii=False,indent=2))
summaries=[];allqueries=[];stage_means=[];node_load=[];selected=None;nodes=None;entry_rows=[]
for repeat in range(e['repetitions']):
    s=json.loads((R/f'run_{repeat}.json').read_text());d=Path(s['directory'])
    with gzip.open(d/'queries.jsonl.gz','rt') as f:qs=[json.loads(line) for line in f]
    assert len(qs)==e['queries'] and all(q['status']=='ok' for q in qs)
    summaries.append(s);allqueries.extend(qs)
    if repeat==0:
        selected=next(q for q in qs if q['query_id']==e['trace_query_id'])
        nodes=read_gz(d/'nodes_after.json.gz')
    stage={k:[] for k in ('routing','entry_pool','entry_rpc_after_pool','entry_return_scheduling','after_entry_ack','head_merge_return','head_other')}
    for q in qs:
        t=q['stats']['head_timestamps'];rpc=q['stats']['entry_rpc']
        entry_rows.append(dict(repeat=repeat,query_id=q['query_id'],request_id=q['request_id'],
            pool_wait_ms=rpc['pool_wait_ms'],release_ms=rpc['pool_release_ms'],
            release_lock_ms=rpc.get('pool_release_lock_wait_ms',0.),
            after_release_return_ms=(t['entry_submit_return_ns']-rpc['rpc_release_done_ns'])/1e6))
        vals=dict(routing=(t['routing_done_ns']-t['search_enter_ns'])/1e6,
            entry_pool=rpc['pool_wait_ms'],entry_rpc_after_pool=rpc['wall_ms']-rpc['pool_wait_ms'],
            entry_return_scheduling=(t['entry_submit_return_ns']-rpc['response_received_ns'])/1e6,
            after_entry_ack=(t['all_reports_received_ns']-t['entry_submit_return_ns'])/1e6,
            head_merge_return=(t['search_return_ns']-t['all_reports_received_ns'])/1e6)
        vals['head_other']=q['client_wall_ms']-sum(vals.values())
        for k,v in vals.items():stage[k].append(v)
    stage_means.append({'repeat':repeat,**{k:float(np.mean(v)) for k,v in stage.items()}})
    for n in s['node_process_load']:
        node_load.append(dict(repeat=repeat,node=n['shard_id'],mean_cpu_cores=n['mean_cpu_cores'],
            physical_read_bytes=n['process_deltas']['storage_read_bytes'],
            physical_write_bytes=n['process_deltas']['storage_write_bytes'],
            snapshot_window_seconds=n['process_window_seconds'],process=json.dumps(n['process'])))
write_csv(R/'head_stage_means.csv',stage_means);write_csv(R/'node_load.csv',node_load)
write_csv(R/'entry_transport.csv',entry_rows)
comparison=[]
for i,s in enumerate(summaries):
    comparison.append(dict(repeat=i,qps=s['qps'],mean_ms=s['latency_ms']['mean'],p50_ms=s['latency_ms']['p50'],
        p95_ms=s['latency_ms']['p95'],p99_ms=s['latency_ms']['p99'],max_ms=s['latency_ms']['max'],
        recall=s['recall_at_k_all_requests'],failures=s['failed_queries'],peak_inflight=s['actual_peak_inflight']))
write_csv(R/'comparison.csv',comparison)
numeric=sorted({k for q in allqueries for k,v in q['stats'].items() if type(v) in (int,float)})
metrics={k:{'mean':float(np.mean(v)),'p99':float(np.percentile(v,99)),'max':float(max(v))}
         for k in numeric if (v:=[q['stats'][k] for q in allqueries if type(q['stats'].get(k)) in (int,float)])}
save(R/'metric_summary.json',metrics)
save(R/'selected_query.json',selected)
events={sid:{key:[t for t in n['peer_lifecycle'][key] if t['query_request_id']==selected['request_id']]
             for key in ('execution_timestamps','control_events')} for sid,n in nodes.items()}
save(R/'selected_events.json',events)

q=selected;rpc=q['stats']['entry_rpc'];head=q['stats']['head_timestamps'];rows=[]
def add(domain,lane,name,a,b,origin):
    if b<a:raise ValueError((domain,lane,name,a,b))
    rows.append(dict(clock_domain=domain,lane=lane,stage=name,start_ms=(a-origin)/1e6,
        end_ms=(b-origin)/1e6,duration_ms=(b-a)/1e6))
t0=q['client_start_ns'];end=q['client_end_ns']
points=[t0,rpc['rpc_start_ns'],rpc['pool_acquired_ns'],rpc['send_done_ns'],
        rpc['response_received_ns'],rpc['rpc_finally_enter_ns'],rpc['rpc_release_done_ns'],head['entry_submit_return_ns'],
        max(head['entry_submit_return_ns'],head['all_reports_received_ns']),end]
labels=['顶层排队、路由及入口准备','入口连接池等待','入口建连/发送','接纳ACK等待','ACK响应处理','归还入口连接','提交调用返回','返回后等齐报告','最终合并与返回']
for i,label in enumerate(labels):add('head','端到端',label,points[i],points[i+1],t0)
assert abs(sum(r['duration_ms'] for r in rows)-q['client_wall_ms'])<1e-6
active=[(sid,ev) for sid,ev in events.items() if ev['execution_timestamps']]
compute=[]
for sid,ev in active:
    for t in ev['execution_timestamps']:
        origin=t['accepted_ns'];domain=f'node-{sid}'
        pairs=[('接纳队列','accepted_ns','dequeued_ns'),('提交计算队列','dequeued_ns','executor_enqueue_ns'),
          ('计算队列','executor_enqueue_ns','executor_dequeue_ns'),('进入原生搜索','executor_dequeue_ns','native_search_start_ns'),
          ('原生搜索循环','native_search_start_ns','native_loop_done_ns'),
          ('报告构造与交接','native_loop_done_ns','native_gil_wait_start_ns'),
          ('重获GIL','native_gil_wait_start_ns','native_gil_reacquired_ns'),
          ('返回及释放线程','native_gil_reacquired_ns','worker_available_ns')]
        for name,a,b in pairs:
            if a in t and b in t:add(domain,'计算线程',name,t[a],t[b],origin)
        for name,a,b in [('完成事件排队','local_done_enqueued_ns','local_done_dequeued_ns'),
            ('报告收尾','local_done_dequeued_ns','report_ready_ns')]:
            if a in t and b in t:add(domain,'报告控制',name,t[a],t[b],origin)
        for c in ev['control_events']:
            if c['branch_id']!=t['branch_id'] or c['kind']!='result':continue
            start=max(t['report_ready_ns'],c['sender_enter_ns'])
            add(domain,'报告控制','报告就绪至发送',t['report_ready_ns'],start,origin)
            add(domain,'报告控制','报告发送至ACK',start,c['sender_rpc_return_ns'],origin)
        for name,a,b in [('远程派发队列','transfer_enqueue_enter_ns','transfer_worker_enter_ns'),
                         ('远程派发RPC','transfer_worker_enter_ns','transfer_worker_return_ns')]:
            if a in t and b in t:add(domain,'派发线程',name,t[a],t[b],origin)
        b=next(b for b in q['branch_reports'] if b['branch_id']==t['branch_id'])
        batches=b.get('local_batch_trace',[])
        sums={k:sum(z['stats'].get(k,0) for z in batches) for k in ('pread_ms','exact_distance_ms','rabitq_ms')}
        elapsed=(t['native_loop_done_ns']-t['native_search_start_ns'])/1e6
        compute.append(dict(node=sid,loop_wall_ms=elapsed,**sums,other_wall_ms=elapsed-sum(sums.values()),
            thread_released_after_accept_ms=(t['worker_available_ns']-origin)/1e6))
write_csv(R/'selected_timeline.csv',rows);save(R/'selected_compute.json',compute)
stage_names=list(dict.fromkeys(r['stage'] for r in rows));palette=plt.get_cmap('tab20')
colors={name:palette(i%20) for i,name in enumerate(stage_names)}
fig,axes=plt.subplots(1+len(active),1,figsize=(16,3.8+3*len(active)),layout='constrained')
for ax,domain in zip(np.atleast_1d(axes),['head']+[f'node-{sid}' for sid,_ in active]):
    rr=[r for r in rows if r['clock_domain']==domain];lanes=list(dict.fromkeys(r['lane'] for r in rr))
    for r in rr:
        ax.barh(lanes.index(r['lane']),r['duration_ms'],left=r['start_ms'],height=.5,color=colors[r['stage']])
        total=max(v['end_ms'] for v in rr)
        if r['duration_ms']>total*.08:
            ax.text((r['start_ms']+r['end_ms'])/2,lanes.index(r['lane']),f"{r['duration_ms']:.2f} ms",ha='center',va='center',fontsize=9)
    ax.set_yticks(range(len(lanes)),lanes);ax.invert_yaxis();ax.grid(axis='x',alpha=.2)
    ax.set_xlabel('距客户端提交 / ms' if domain=='head' else '距本节点接纳 / ms（独立原点，不与其他主机对齐）')
    ax.set_title('Intel-35 顶层：互斥时间分解' if domain=='head' else f"数据节点{int(domain.split('-')[1])+1}：本机计算、报告与派发可重叠")
    names=list(dict.fromkeys(r['stage'] for r in rr))
    ax.legend(handles=[plt.Rectangle((0,0),1,1,color=colors[n]) for n in names],labels=names,
        loc='upper left',bbox_to_anchor=(0,-.34),ncols=4,fontsize=8)
fig.suptitle(f"三物理机 · 查询{q['query_id']} · API {q['client_wall_ms']:.2f} ms · Recall@10 {q['recall']:.0%}\n各主机独立时钟；分支轨道不可相加为端到端耗时",fontsize=14)
for ext in ('png','svg'):fig.savefig(R/f'query_timeline.{ext}',dpi=160,bbox_inches='tight')
plt.close(fig)
fig,ax=plt.subplots(figsize=(11,4),layout='constrained')
names={'pread_ms':'pread（逻辑读取）','exact_distance_ms':'精确距离','rabitq_ms':'RaBitQ','other_wall_ms':'循环其余时间（含调度）'}
for i,c in enumerate(compute):
    left=0
    for j,(key,name) in enumerate(names.items()):
        ax.barh(i,c[key],left=left,label=name if i==0 else None,color=palette(j*2))
        if c[key]>max(z['loop_wall_ms'] for z in compute)*.08:ax.text(left+c[key]/2,i,f"{c[key]:.2f} ms",ha='center',va='center',fontsize=9)
        left+=c[key]
ax.set_yticks(range(len(compute)),[f"节点{int(c['node'])+1}" for c in compute]);ax.invert_yaxis()
ax.set_xlabel('节点原生搜索循环内墙钟时间 / ms');ax.legend(loc='upper left',bbox_to_anchor=(0,-.2),ncols=2)
ax.set_title(f"查询{q['query_id']}：计算区间拆分（不同分支可并行；pread不等于物理SSD时间）")
for ext in ('png','svg'):fig.savefig(R/f'query_compute.{ext}',dpi=160,bbox_inches='tight')
save(R/'analysis.json',dict(comparison=comparison,median_qps=float(np.median([s['qps'] for s in summaries])),
    median_p99_ms=float(np.median([s['latency_ms']['p99'] for s in summaries])),
    head_stage_means=stage_means,selected_query_id=q['query_id'],selected_latency_ms=q['client_wall_ms'],
    selected_recall=q['recall'],selected_entry_node=next(b['node_id'] for b in q['branch_reports'] if b['parent_branch_id'] is None),
    selected_nodes=[b['node_id'] for b in q['branch_reports']],selected_head_stages=[r for r in rows if r['clock_domain']=='head']))
print(json.dumps(comparison,indent=2));print('Selected query',q['query_id'],q['client_wall_ms'])

# Head and receiver share a verified host/time namespace; other hosts never align.
m=q['stats']['head_receiver'];save(R/'selected_receiver.json',m)
receiver_rows=[]
for name,a,b in [('末次报告处理及ACK','last_callback_dispatch_ns','terminal_publish_ns'),('终态IPC传输与主进程调度','terminal_publish_ns','terminal_parent_received_ns'),('主进程应用状态','terminal_parent_received_ns','terminal_parent_applied_ns')]:
    receiver_rows.append(dict(stage=name,start_ms=(m[a]-t0)/1e6,duration_ms=(m[b]-m[a])/1e6))
receiver_rows.append(dict(stage='唤醒查询至wait返回',start_ms=(m['terminal_parent_applied_ns']-t0)/1e6,duration_ms=(head['all_reports_received_ns']-m['terminal_parent_applied_ns'])/1e6))
write_csv(R/'selected_receiver_timeline.csv',receiver_rows)
fig,ax=plt.subplots(figsize=(14,4),layout='constrained')
for r in rows:
    if r['clock_domain']=='head':
        ax.barh(0,r['duration_ms'],left=r['start_ms'],color='#cccccc',edgecolor='white',height=.5)
        if r['duration_ms']>q['client_wall_ms']*.1:ax.text(r['start_ms']+r['duration_ms']/2,0,f"{r['stage']}\n{r['duration_ms']:.1f} ms",ha='center',va='center',fontsize=9)
for i,r in enumerate(receiver_rows):
    ax.barh(1,r['duration_ms'],left=r['start_ms'],label=r['stage'],color=palette(i*2),height=.5)
    if r['duration_ms']>q['client_wall_ms']*.03:ax.text(r['start_ms']+r['duration_ms']/2,1,f"{r['duration_ms']:.2f} ms",ha='center',va='center')
ax.set_yticks([0,1],['顶层端到端','末次报告与IPC']);ax.invert_yaxis();ax.set_xlabel('距客户端提交 / ms（同一Intel-35时钟域）');ax.grid(axis='x',alpha=.2)
ax.set_title(f"查询{q['query_id']}：API {q['client_wall_ms']:.2f} ms；接收进程注册就绪 {m['registration_ready_ms']:.2f} ms（无需等待注册ACK）\n下轨为上轨中重叠的细分，不能相加；网络报告到达前等待不由下轨测量")
ax.legend(loc='upper left',bbox_to_anchor=(0,-.3),ncols=2)
for ext in ('png','svg'):fig.savefig(R/f'query_receiver.{ext}',dpi=160,bbox_inches='tight')
rx_stats=[]
for repeat,s in enumerate(summaries):
    with gzip.open(Path(s['directory'])/'queries.jsonl.gz','rt') as f:queries=[json.loads(line) for line in f]
    values={k:[] for k in ('registration','ipc','apply','query_resume')}
    for query in queries:
        m=query['stats']['head_receiver'];t=query['stats']['head_timestamps']
        values['registration'].append(m['registration_ready_ms'])
        values['ipc'].append((m['terminal_parent_received_ns']-m['terminal_publish_ns'])/1e6)
        values['apply'].append((m['terminal_parent_applied_ns']-m['terminal_parent_received_ns'])/1e6)
        values['query_resume'].append((t['all_reports_received_ns']-max(t['entry_submit_return_ns'],m['terminal_parent_applied_ns']))/1e6)
    rx_stats.append(dict(repeat=repeat,api_over100=sum(query['client_wall_ms']>100 for query in queries),**{k+'_mean_ms':float(np.mean(v)) for k,v in values.items()},**{k+'_p99_ms':float(np.percentile(v,99)) for k,v in values.items()}))
write_csv(R/'receiver_stage_means.csv',rx_stats)
save(R/'head_process_load.json',[dict(repeat=i,processes=s['head_process_load']) for i,s in enumerate(summaries)])

# Explicit nonoverlapping dependency chain for continuation mode.
m=q['stats']['head_receiver'];t=q['stats']['head_timestamps'];origin=q['client_start_ns']
points=[origin,t['head_stage_start_ns'],t['routing_done_ns'],m['register_parent_enqueued_ns'],m['registered_ns'],t['entry_submit_enter_ns'],t['entry_submit_return_ns'],m['query_ready_ns'],m['head_stage_resume_ns'],t['search_return_ns'],q['client_end_ns']]
names=['顶层首次就绪队列','HNSW路由及入口估距','准备初始请求','IPC命令排队/传输及注册','接收进程初始派发队列','入口提交RPC','等待入口确认交付及全部报告','顶层合并就绪队列','合并与生命周期清理','唤醒API调用线程及返回']
flow_rows=[]
for i,name in enumerate(names):
    assert points[i+1]>=points[i],(name,points[i:i+2])
    flow_rows.append(dict(stage=name,start_ms=(points[i]-origin)/1e6,duration_ms=(points[i+1]-points[i])/1e6))
assert abs(sum(r['duration_ms'] for r in flow_rows)-q['client_wall_ms'])<1e-6
write_csv(R/'selected_flow_timeline.csv',flow_rows)
fig,ax=plt.subplots(figsize=(16,4),layout='constrained')
for i,r in enumerate(flow_rows):
    ax.barh(0,r['duration_ms'],left=r['start_ms'],label=r['stage'],color=palette(i),height=.5)
    if r['duration_ms']>q['client_wall_ms']*.055:ax.text(r['start_ms']+r['duration_ms']/2,0,f"{r['duration_ms']:.2f} ms",ha='center',va='center',fontsize=9)
ax.set_yticks([0],['完整API']);ax.set_xlabel('距客户端提交 / ms（同一Intel-35时钟域；所有阶段互斥）');ax.grid(axis='x',alpha=.2)
ax.set_title(f"查询{q['query_id']}：API {q['client_wall_ms']:.2f} ms · Recall@10 {q['recall']:.0%}\n2个顶层续执行线程，128在途；初次和合并排队均计入总时延")
ax.legend(loc='upper left',bbox_to_anchor=(0,-.3),ncols=3,fontsize=9)
for ext in ('png','svg'):fig.savefig(R/f'query_flow_timeline.{ext}',dpi=160,bbox_inches='tight')
