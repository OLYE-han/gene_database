"""One query timeline, preserving overlap between branch and sender lanes."""
from pathlib import Path
import argparse,json,csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
R=Path(__file__).resolve().parent
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'font.weight':500,'axes.titleweight':500,'axes.labelweight':500,'axes.unicode_minus':False,'svg.fonttype':'path'})
p=argparse.ArgumentParser();p.add_argument('group');p.add_argument('--run');a=p.parse_args()
d=R/'analysis'/a.group/(a.run or sorted((R/'analysis'/a.group).iterdir())[-1].name)
q=json.loads((d/'selected_query.json').read_text());ev=json.loads((d/'selected_events.json').read_text())
t0=q['client_start_ns'];end=q['client_end_ns'];rows=[]
colors={'入口到接纳':'#a6bddb','接纳至工作线程':'#c7b2d6','进入原生搜索':'#d9d9d9','原生搜索区间':'#36a59b','搜索后收尾':'#e8ad5b','报告就绪后待发送':'#dd7e7e','结果发送与确认':'#5673a7','远程派发队列':'#d2aecc','远程派发RPC':'#9a6eaa'}
colors['原生报告构造及交接']='#96b3a4'
colors.update({'循环结束后重获GIL':'#b64069','报告物化及返回':'#96b3a4'})
colors.update({'HNSW路由及入口准备':'#719abe','连接池借用':'#c5a3c5','建连与发送':'#668daf',
               '入口ACK等待':'#ddbb80','等待分支结果':'#c8c8c8','最终合并与返回':'#75a8ad',
               '接纳队列':'#c7b2d6','计算队列':'#b18ec0','启动及入队':'#d9d9d9',
               '完成事件待消费':'#e8ad5b','完成事件状态更新':'#c08e53','异步报告收尾':'#ddb568',
               '线程释放':'#b5b5b5'})
def add(lane,kind,start,finish):
    if finish>start:rows.append(dict(lane=lane,stage=kind,start_ms=(start-t0)/1e6,end_ms=(finish-t0)/1e6,duration_ms=(finish-start)/1e6))
root=min(ev['startup'],key=lambda t:t['accepted_ns'])
head=q['stats'].get('head_timestamps',{});rpc=q['stats'].get('entry_rpc',{})
if 'routing_done_ns' in head and 'rpc_start_ns' in rpc:
    add('入口 / 客户端','HNSW路由及入口准备',t0,rpc['rpc_start_ns'])
    add('入口 / 客户端','连接池借用',rpc['rpc_start_ns'],rpc['pool_acquired_ns'])
    add('入口 / 客户端','建连与发送',rpc['pool_acquired_ns'],rpc['send_done_ns'])
    add('入口 / 客户端','入口ACK等待',rpc['send_done_ns'],rpc['response_received_ns'])
    add('入口 / 客户端','等待分支结果',rpc['response_received_ns'],head['all_reports_received_ns'])
    add('入口 / 客户端','最终合并与返回',head['all_reports_received_ns'],end)
else:add('客户端','入口到接纳',t0,root['accepted_ns'])
for t in ev['startup']:
    lane=f"节点{t['node_id']} 计算任务"
    native_end='native_loop_done_ns' if 'native_loop_done_ns' in t else 'native_return_ns'
    if 'executor_enqueue_ns' in t:
        dequeued='dequeued_ns' if 'dequeued_ns' in t else 'accepted_ns'
        for kind,x,y in [('接纳队列','accepted_ns',dequeued),('启动及入队',dequeued,'executor_enqueue_ns'),
                          ('计算队列','executor_enqueue_ns','executor_dequeue_ns')]:add(lane,kind,t[x],t[y])
    else:add(lane,'接纳至工作线程',t['accepted_ns'],t['worker_enter_ns'])
    for kind,x,y in [('进入原生搜索','worker_enter_ns','native_search_start_ns'),('原生搜索区间','native_search_start_ns',native_end)]:add(lane,kind,t[x],t[y])
    if 'native_loop_done_ns' in t:
        gil_start=t.get('native_gil_wait_start_ns',t['native_loop_done_ns'])
        add(lane,'原生报告构造及交接',t['native_loop_done_ns'],gil_start)
        add(lane,'循环结束后重获GIL',gil_start,t['native_gil_reacquired_ns'])
        add(lane,'报告物化及返回',t['native_gil_reacquired_ns'],t['native_return_ns'])
    if 'worker_available_ns' in t:add(lane,'线程释放',t['native_return_ns'],t['worker_available_ns'])
    lane=f"节点{t['node_id']} 报告控制"
    if 'local_done_dequeued_ns' in t:
        add(lane,'完成事件待消费',t['local_done_enqueued_ns'],t['local_done_dequeued_ns'])
        add(lane,'完成事件状态更新',t['local_done_dequeued_ns'],t['local_done_consumed_ns'])
        add(lane,'异步报告收尾',t['local_done_consumed_ns'],t['report_ready_ns'])
    else:add(lane,'异步报告收尾',t['native_return_ns'],t['report_ready_ns'])
    results=[e for e in ev['control'] if e['branch_id']==t['branch_id'] and e['kind']=='result']
    e=min(results,key=lambda e:e['message_ready_ns'])
    # A queued heartbeat can be upgraded to result after sender entry. Avoid
    # asserting sender_entry always follows report readiness on each branch.
    add(lane,'报告就绪后待发送',t['report_ready_ns'],max(t['report_ready_ns'],e['sender_enter_ns']))
    add(lane,'结果发送与确认',max(t['report_ready_ns'],e['sender_enter_ns']),e['sender_rpc_return_ns'])
    if 'transfer_worker_enter_ns' in t:
        lane=f"节点{t['node_id']} 派发线程"
        add(lane,'远程派发队列',t['transfer_enqueue_enter_ns'],t['transfer_worker_enter_ns'])
        add(lane,'远程派发RPC',t['transfer_worker_enter_ns'],t['transfer_worker_return_ns'])
lanes=list(dict.fromkeys(r['lane'] for r in rows))
fig,(ax,bx)=plt.subplots(2,1,figsize=(16,10),gridspec_kw={'height_ratios':[4,1]},layout='constrained')
used=set()
for r in rows:
    ax.barh(lanes.index(r['lane']),r['duration_ms'],left=r['start_ms'],height=.55,color=colors[r['stage']],label=r['stage'] if r['stage'] not in used else None)
    used.add(r['stage'])
    if r['duration_ms']>60:ax.text((r['start_ms']+r['end_ms'])/2,lanes.index(r['lane']),f"{r['duration_ms']:.1f}",ha='center',va='center',fontsize=8)
ax.axvline((end-t0)/1e6,color='black',ls='--',label='API 返回')
for i,t in enumerate(ev['startup']):
    if 'worker_available_ns' in t:
        ax.scatter([(t['worker_available_ns']-t0)/1e6],[lanes.index(f"节点{t['node_id']} 计算任务")],
                   marker='D',s=35,color='black',zorder=5,label='计算线程可接下一任务' if i==0 else None)
ax.set_yticks(range(len(lanes)),lanes);ax.invert_yaxis();ax.set_xlabel('距离客户端提交 / ms');ax.grid(axis='x',alpha=.2)
ax.legend(loc='upper left',bbox_to_anchor=(0,-.12),ncols=5,fontsize=8)
ready=max(t['report_ready_ns'] for t in ev['startup']);sent=max(ready,max(e['sender_enter_ns'] for e in ev['control'] if e['kind']=='result'))
points=[t0,root['accepted_ns'],ready,sent,end]
labels=['入口到接纳','接纳至全部报告就绪','报告齐备至最后发送','最后发送至API返回'];palette=['#a6bddb','#36a59b','#dd7e7e','#5673a7']
for i,label in enumerate(labels):
    width=(points[i+1]-points[i])/1e6
    bx.barh(0,width,left=(points[i]-t0)/1e6,color=palette[i],label=f'{label} {width:.2f} ms')
    if width>80:bx.text(((points[i]+points[i+1])/2-t0)/1e6,0,f'{width:.1f} ms\n{width/((end-t0)/1e6):.1%}',ha='center',va='center',fontsize=9)
bx.set_yticks([]);bx.set_xlabel('端到端边界分组 / ms（含工作与等待，仅下图可相加）');bx.legend(loc='upper left',bbox_to_anchor=(0,-.45),ncols=2,fontsize=8)
fig.suptitle(f"固定查询 {q['query_id']} · {a.group} · {d.name}\nAPI延迟 {(end-t0)/1e6:.2f} ms · Recall@10 {q['recall']:.0%}；各分支/派发轨道并行，不能累计成总延迟")
for ext in ('png','svg'):fig.savefig(d/f'query_timeline.{ext}',dpi=150)
with (d/'query_timeline.csv').open('w') as f:
    w=csv.DictWriter(f,list(rows[0]));w.writeheader();w.writerows(rows)
print(d/'query_timeline.png')
