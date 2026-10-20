"""Same-load medians, full query metrics and non-overlapping head timeline."""
from pathlib import Path
import argparse,csv,gzip,json
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parent);args=parser.parse_args();R=args.root
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'axes.unicode_minus':False,'svg.fonttype':'path'})
def save(name,value):(R/name).write_text(json.dumps(value,ensure_ascii=False,indent=2))
def csvwrite(name,rows):
    with (R/name).open('w') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
def dist(v):return dict(mean=float(np.mean(v)),p50=float(np.percentile(v,50)),p99=float(np.percentile(v,99)),max=float(max(v)))
cases=[];by_mode={};rows=[];loads=[];controls=[];query_timings=[]
for p in sorted((R/'cases').glob('*/result.json')):
    s=json.loads(p.read_text());d=Path(s['directory'])
    with gzip.open(d/'queries.jsonl.gz','rt') as f:qs=[json.loads(line) for line in f]
    assert len(qs)==2000 and all(q['status']=='ok' for q in qs) and all(s['audit'].values())
    case=dict(name=p.parent.name,summary=s,queries=qs);cases.append(case);by_mode.setdefault(s['mode'],[]).append(case)
    with gzip.open(d/'nodes_before.json.gz','rt') as f:before=json.load(f)
    with gzip.open(d/'nodes_after.json.gz','rt') as f:after=json.load(f)
    ids={q['request_id'] for q in qs}
    for sid,node in after.items():
        life=node['peer_lifecycle'];oldlife=before[sid]['peer_lifecycle']
        failed=[ev for ev in life['control_events'] if ev['query_request_id'] in ids and ev.get('succeeded') is False]
        stamps={ev['branch_id']:ev for ev in life['execution_timestamps'] if ev['query_request_id'] in ids}
        events=[ev for ev in life['control_events'] if ev['query_request_id'] in ids and ev['kind']=='result']
        waits=[(ev['sender_enter_ns']-stamps[ev['branch_id']]['report_ready_ns'])/1e6 for ev in events]
        rtts=[(ev['sender_rpc_return_ns']-ev['sender_enter_ns'])/1e6 for ev in events]
        controls.append(dict(case=p.parent.name,node=sid,control_error_delta=life['control_totals']['errors']-oldlife['control_totals']['errors'],
            failed_query_control_attempts=len(failed),report_queue_mean_ms=float(np.mean(waits)),report_rpc_mean_ms=float(np.mean(rtts))))
    for qv in qs:
        st=qv['stats'];query_timings.append(dict(case=p.parent.name,query_id=qv['query_id'],request_id=qv['request_id'],
            client_wall_ms=qv['client_wall_ms'],**{k:st.get(k) for k in ('head_receive_queue_ms','head_route_queue_ms','head_dispatch_queue_ms','head_merge_queue_ms','head_stage_queue_ms','head_stage_resume_queue_ms')}))
    rows.append(dict(case=p.parent.name,mode=s['mode'],qps=s['qps'],mean_ms=s['latency_ms']['mean'],
        p99_ms=s['latency_ms']['p99'],recall=s['recall_at_k_all_requests'],failures=s['failed_queries'],
        peak_inflight=s['actual_peak_inflight'],over100=sum(q['client_wall_ms']>100 for q in qs)))
    for n in s['node_process_load']:
        loads.append(dict(case=p.parent.name,role=f"data-{n['shard_id']}",cpu_cores=n['mean_cpu_cores'],
            rss_bytes=n['process']['rss_bytes'],physical_read_bytes=n['process_deltas']['storage_read_bytes']))
    for n in s['head_process_load']:
        loads.append(dict(case=p.parent.name,role=n['role'],cpu_cores=n['mean_cpu_cores'],
            rss_bytes=n['process']['rss_bytes'],physical_read_bytes=n['process']['storage_read_bytes']))
csvwrite('comparison.csv',rows);csvwrite('process_load.csv',loads)
csvwrite('control_transport.csv',controls);csvwrite('query_head_timings.csv',query_timings)
summary={};metrics={}
for mode,cc in by_mode.items():
    qq=[q for c in cc for q in c['queries']]
    keys=sorted({k for q in qq for k,v in q['stats'].items() if type(v) in (int,float)})
    metrics[mode]={k:dist([q['stats'][k] for q in qq if type(q['stats'].get(k)) in (int,float)]) for k in keys}
    summary[mode]=dict(rounds=len(cc),queries=len(qq),median_qps=float(np.median([c['summary']['qps'] for c in cc])),
        median_p99_ms=float(np.median([c['summary']['latency_ms']['p99'] for c in cc])),
        median_mean_ms=float(np.median([c['summary']['latency_ms']['mean'] for c in cc])),
        recall_mean=float(np.mean([q['recall'] for q in qq])),api_over100=sum(q['client_wall_ms']>100 for q in qq))
save('metrics.json',metrics);save('summary.json',summary)
chosen=min((m for m in summary if m!='old'),key=lambda m:summary[m]['median_p99_ms'])
case=by_mode[chosen][0];q=next(q for q in case['queries'] if q['query_id']==734744)
save('selected_query.json',q);save('selection.json',dict(mode=chosen,case=case['name'],query_id=q['query_id']))
t=q['stats']['head_timestamps'];p=q['stats']['head_pipeline'];m=q['stats']['head_receiver']
points=[q['client_start_ns'],p['receive_enqueued_ns'],p['receive_started_ns'],p['route_enqueued_ns'],p['route_started_ns'],
    p['dispatch_enqueued_ns'],p['dispatch_started_ns'],t['entry_submit_enter_ns'],t['entry_submit_return_ns'],
    m['query_ready_ns'],p['merge_enqueued_ns'],p['merge_started_ns'],t['search_return_ns'],q['client_end_ns']]
labels=['API提交','接收队列','接收入队操作','HNSW路由队列','路由计算及初始请求准备','派发队列','创建查询状态',
    '入口派发RPC','等待完整分支报告','合并入队操作','top-k合并队列','结果合并及清理','返回调用者']
timeline=[]
for i,name in enumerate(labels):
    assert points[i+1]>=points[i],(name,points[i],points[i+1])
    timeline.append(dict(stage=name,start_ms=(points[i]-points[0])/1e6,duration_ms=(points[i+1]-points[i])/1e6))
assert abs(sum(r['duration_ms'] for r in timeline)-q['client_wall_ms'])<1e-6
csvwrite('selected_timeline.csv',timeline)
fig,ax=plt.subplots(figsize=(15,4),layout='constrained');colors=plt.get_cmap('tab20')
for i,row in enumerate(timeline):
    ax.barh(0,row['duration_ms'],left=row['start_ms'],color=colors(i),label=row['stage'])
    if row['duration_ms']>q['client_wall_ms']*.045:ax.text(row['start_ms']+row['duration_ms']/2,0,f"{row['duration_ms']:.2f} ms",ha='center',va='center',fontsize=9)
ax.set_yticks([0],['完整API']);ax.set_xlabel('距客户端提交 / ms（同一Intel-35单调时钟，全部区间互斥）')
ax.set_title(f"单进程流水线 {chosen} · 固定查询734744 · {q['client_wall_ms']:.2f}ms · Recall@10 {q['recall']:.0%}")
ax.legend(loc='upper left',bbox_to_anchor=(0,-.25),ncols=4,fontsize=9);ax.grid(axis='x',alpha=.2)
for ext in ('png','svg'):fig.savefig(R/f'query_timeline.{ext}',dpi=150,bbox_inches='tight')
plt.close(fig)
fig,axes=plt.subplots(1,2,figsize=(11,4),layout='constrained')
names=list(summary)
for ax,key,title in zip(axes,['median_qps','median_p99_ms'],['QPS中位数（越高越好）','p99中位数 / ms（越低越好）']):
    vals=[summary[n][key] for n in names];bars=ax.bar(names,vals);ax.bar_label(bars,fmt='%.1f');ax.set_title(title);ax.set_ylim(0,max(vals)*1.2)
for ext in ('png','svg'):fig.savefig(R/f'comparison.{ext}',dpi=150,bbox_inches='tight')
plt.close(fig)
# Remote clocks remain independent; use only intervals on each data host.
with gzip.open(Path(case['summary']['directory'])/'nodes_after.json.gz','rt') as f:nodes=json.load(f)
events={sid:{k:[v for v in n['peer_lifecycle'][k] if v['query_request_id']==q['request_id']]
    for k in ('execution_timestamps','control_events')} for sid,n in nodes.items()}
save('selected_node_events.json',events)
data_rows=[]
for sid,ev in events.items():
    for stamp in ev['execution_timestamps']:
        origin=stamp['accepted_ns']
        for name,a,b in [('接纳队列','accepted_ns','dequeued_ns'),('计算队列','executor_enqueue_ns','executor_dequeue_ns'),
            ('原生搜索','native_search_start_ns','native_loop_done_ns'),('返回并释放计算线程','native_loop_done_ns','worker_available_ns'),
            ('完成事件队列','local_done_enqueued_ns','local_done_dequeued_ns'),('报告构造','local_done_dequeued_ns','report_ready_ns')]:
            if a in stamp and b in stamp:data_rows.append(dict(node=sid,stage=name,start_ms=(stamp[a]-origin)/1e6,duration_ms=(stamp[b]-stamp[a])/1e6))
        for evv in ev['control_events']:
            if evv['branch_id']!=stamp['branch_id'] or evv['kind']!='result':continue
            start=max(stamp['report_ready_ns'],evv['sender_enter_ns'])
            for name,a,b in [('报告就绪至发送',stamp['report_ready_ns'],start),('报告发送至ACK',start,evv['sender_rpc_return_ns'])]:
                data_rows.append(dict(node=sid,stage=name,start_ms=(a-origin)/1e6,duration_ms=(b-a)/1e6))
csvwrite('selected_node_timeline.csv',data_rows)
nodes_shown=list(dict.fromkeys(r['node'] for r in data_rows))
fig,axes=plt.subplots(len(nodes_shown),1,figsize=(13,4*len(nodes_shown)),layout='constrained')
for ax,sid in zip(np.atleast_1d(axes),nodes_shown):
    rr=[r for r in data_rows if r['node']==sid]
    for i,row in enumerate(rr):
        ax.barh(i,row['duration_ms'],left=row['start_ms'],color=colors(i))
        ax.text(row['start_ms']+row['duration_ms'],i,f" {row['duration_ms']:.2f} ms",va='center',fontsize=9)
    ax.set_yticks(range(len(rr)),[r['stage'] for r in rr]);ax.invert_yaxis();ax.grid(axis='x',alpha=.2)
    ax.set_title(f'数据节点{sid}：距本节点接纳时刻 / ms（独立原点，不与其他主机对齐）')
    ax.margins(x=.15)
for ext in ('png','svg'):fig.savefig(R/f'query_nodes.{ext}',dpi=150,bbox_inches='tight')
plt.close(fig)
print(json.dumps(summary,indent=2));print('selected',chosen,q['client_wall_ms'])
