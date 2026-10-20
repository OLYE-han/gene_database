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
def add(lane,kind,start,finish):
    if finish>start:rows.append(dict(lane=lane,stage=kind,start_ms=(start-t0)/1e6,end_ms=(finish-t0)/1e6,duration_ms=(finish-start)/1e6))
root=min(ev['startup'],key=lambda t:t['accepted_ns']);add('客户端','入口到接纳',t0,root['accepted_ns'])
for t in ev['startup']:
    lane=f"数据节点{t['node_id']} 分支"
    for kind,x,y in [('接纳至工作线程','accepted_ns','worker_enter_ns'),('进入原生搜索','worker_enter_ns','native_search_start_ns'),('原生搜索区间','native_search_start_ns','native_return_ns'),('搜索后收尾','native_return_ns','report_ready_ns')]:add(lane,kind,t[x],t[y])
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
fig,(ax,bx)=plt.subplots(2,1,figsize=(14,8),gridspec_kw={'height_ratios':[3,1]},layout='constrained')
used=set()
for r in rows:
    ax.barh(lanes.index(r['lane']),r['duration_ms'],left=r['start_ms'],height=.55,color=colors[r['stage']],label=r['stage'] if r['stage'] not in used else None)
    used.add(r['stage'])
    if r['duration_ms']>60:ax.text((r['start_ms']+r['end_ms'])/2,lanes.index(r['lane']),f"{r['duration_ms']:.1f}",ha='center',va='center',fontsize=8)
ax.axvline((end-t0)/1e6,color='black',ls='--',label='API 返回')
ax.set_yticks(range(len(lanes)),lanes);ax.invert_yaxis();ax.set_xlabel('距离客户端提交 / ms');ax.grid(axis='x',alpha=.2)
ax.legend(loc='upper left',bbox_to_anchor=(0,-.15),ncols=4,fontsize=8)
ready=max(t['report_ready_ns'] for t in ev['startup']);sent=max(e['sender_enter_ns'] for e in ev['control'] if e['kind']=='result')
points=[t0,root['accepted_ns'],ready,sent,end]
labels=['入口到接纳','接纳至全部报告就绪','报告齐备至最后发送','最后发送至API返回'];palette=['#a6bddb','#36a59b','#dd7e7e','#5673a7']
for i,label in enumerate(labels):
    width=(points[i+1]-points[i])/1e6
    bx.barh(0,width,left=(points[i]-t0)/1e6,color=palette[i],label=f'{label} {width:.2f} ms')
    if width>80:bx.text(((points[i]+points[i+1])/2-t0)/1e6,0,f'{width:.1f} ms\n{width/((end-t0)/1e6):.1%}',ha='center',va='center',fontsize=9)
bx.set_yticks([]);bx.set_xlabel('互斥关键路径分组 / ms（仅下图可相加）');bx.legend(loc='upper left',bbox_to_anchor=(0,-.45),ncols=2,fontsize=8)
fig.suptitle(f"固定查询 {q['query_id']} · {a.group} · {d.name}\nAPI延迟 {(end-t0)/1e6:.2f} ms · Recall@10 {q['recall']:.0%}；各分支/派发轨道并行，不能累计成总延迟")
for ext in ('png','svg'):fig.savefig(d/f'query_timeline.{ext}',dpi=150)
with (d/'query_timeline.csv').open('w') as f:
    w=csv.DictWriter(f,list(rows[0]));w.writeheader();w.writerows(rows)
print(d/'query_timeline.png')
