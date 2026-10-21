import csv,json,hashlib
from pathlib import Path
import numpy as np
import yaml
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties,fontManager

ROOT=Path(__file__).resolve().parent
fontManager.addfont('/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':'WenQuanYi Zen Hei','axes.unicode_minus':False,'font.size':10,'svg.fonttype':'none'})
q=json.loads((ROOT/'selected_query.json').read_text());selection=json.loads((ROOT/'selection.json').read_text())
node=json.loads((ROOT/'selected_node_events.json').read_text())
t=q['stats']['native_head_timestamps'];start=q['client_start_ns'];end=q['client_end_ns'];total=q['client_wall_ms']
case=Path(selection['case']);config=yaml.safe_load((case/'plan.yaml').read_text())['dhdgs'];policy=config['native_runtime']['network_module']
assert policy['enabled'] and policy['send_max_reactors']>policy['send_min_reactors']
head_defs=[
 ('API提交','client_start','search_enter_ns'),
 ('路由排队','search_enter_ns','route_started_ns'),
 ('HNSW / 样本RaBitQ','route_started_ns','routing_done_ns'),
 ('入口派发至发送完成','routing_done_ns','entry_send_done_ns'),
 ('发送完成至最后报告到达','entry_send_done_ns','last_report_received_ns'),
 ('顶层报告处理区间','last_report_received_ns','all_reports_received_ns'),
 ('报告完整至ACK发送完成','all_reports_received_ns','last_report_ack_sent_ns'),
 ('ACK完成至合并入队','last_report_ack_sent_ns','merge_enqueued_ns'),
 ('合并排队','merge_enqueued_ns','merge_started_ns'),
 ('合并及原生完成','merge_started_ns','native_ready_ns'),
 ('API结果交付','native_ready_ns','client_end')]
alltimes={**t,'client_start':start,'client_end':end}
rows=[]
for label,a,b in head_defs:
 dur=(alltimes[b]-alltimes[a])/1e6
 assert dur>=0,(label,dur)
 rows.append(dict(stage=label,start_ms=(alltimes[a]-start)/1e6,end_ms=(alltimes[b]-start)/1e6,duration_ms=dur,percent=dur/total*100))
assert abs(sum(r['duration_ms'] for r in rows)-total)<1e-8
with (ROOT/'head_stages.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
# Independent data-node monotonic clock. Never align it to the head clock.
b=q['branch_reports'][0];assert len(q['branch_reports'])==1
sid=str(b['node_id']);trace=node[sid]['execution_timestamps'][0];event=[e for e in node[sid]['control_events'] if e['kind']=='result'][0]
base=trace['receiver_message_ready_ns']
data_defs=[('接收帧至接纳','receiver_message_ready_ns','accepted_ns'),('计算就绪排队','accepted_ns','worker_enter_ns'),('worker进入原生','worker_enter_ns','native_entry_ns'),('原生搜索入口至循环结束','native_entry_ns','native_loop_done_ns'),('构造完成报告','native_loop_done_ns','local_done_enqueued_ns'),('完成事件排队','local_done_enqueued_ns','local_done_dequeued_ns'),('完成事件处理','local_done_dequeued_ns','report_ready_ns'),('报告就绪至控制选择','report_ready_ns','control_result_selected_ns')]
data_rows=[dict(stage=l,start_ms=(trace[a]-base)/1e6,end_ms=(trace[z]-base)/1e6,duration_ms=(trace[z]-trace[a])/1e6) for l,a,z in data_defs]
assert all(r['duration_ms']>=0 for r in data_rows)
with (ROOT/'node_stages.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=list(data_rows[0]));w.writeheader();w.writerows(data_rows)
s=b['stats'];search_parts={'pread逻辑读取':s['pread_ms'],'RaBitQ准备':s.get('rabitq_prepare_ms',0),'RaBitQ估距':s['rabitq_ms'],'精确距离':s['exact_distance_ms']}
search_parts['其他搜索区间']=s['query_compute_slice_wall_ms']-sum(search_parts.values())
assert search_parts['其他搜索区间']>=0
rpc={'入口RPC':q['stats']['entry_rpc'],'结果回传RPC':event['rpc_counts']}
report={**selection,'recall':q['recall'],'node_id':b['node_id'],'peer_rpc_count':b['peer_transfer']['peer_rpc_count'],'search_wall_ms':s['query_compute_slice_wall_ms'],'search_cpu_ms':s['server_thread_cpu_ms'],'head_stages':rows,'node_stages':data_rows,'search_parts_ms':search_parts,'rpc_metrics':rpc,'network_policy':policy,'entry_ack_at_head_ms':(t['entry_submit_return_ns']-start)/1e6,'compute_release_after_local_done_ms':(trace['compute_released_ns']-trace['local_done_enqueued_ns'])/1e6,'source_sha256':hashlib.sha256((ROOT/'selected_query.json').read_bytes()).hexdigest()}
(ROOT/'analysis.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')

colors=['#7198c5','#6685b3','#4f79a6','#bd8c50','#80b4bd','#a786b8','#d08b62','#999999','#d3b14c','#53a485','#7d8fa4']
fig=plt.figure(figsize=(15,14),layout='constrained');gs=fig.add_gridspec(3,2,height_ratios=[2.3,1.6,1.2])
ax=fig.add_subplot(gs[0,:])
for i,(r,c) in enumerate(zip(rows,colors)):
 ax.barh(i,r['duration_ms'],left=r['start_ms'],height=.62,color=c)
 ax.text(total+1,i,f"{r['duration_ms']:.3f} ms  ({r['percent']:.1f}%)",va='center',fontsize=10)
ax.set_yticks(range(len(rows)),[r['stage'] for r in rows]);ax.invert_yaxis();ax.set_xlim(0,total*1.22);ax.set_xlabel('距客户端实际提交 / ms（全部使用顶层主机时钟）');ax.grid(axis='x',alpha=.15);ax.set_axisbelow(True)
ax.set_title(f"A. 真实端到端时间轴：{total:.3f} ms；所有阶段互斥，可相加",loc='left',fontweight='bold')
ack=report['entry_ack_at_head_ms'];ax.axvline(ack,ls='--',color='#5b6470',alpha=.6);ax.text(ack+.4,2.7,f'入口ACK收到 {ack:.2f} ms',fontsize=9,color='#525b67')

ax=fig.add_subplot(gs[1,0]);dc=['#83a7c2','#d0af5c','#a5b4c5','#58a486','#7594ab','#c99667','#a68bb8','#b68a9f']
for i,(r,c) in enumerate(zip(data_rows,dc)):
 ax.barh(i,r['duration_ms'],left=r['start_ms'],height=.65,color=c)
 ax.text(max(x['end_ms'] for x in data_rows)+.25,i,f"{r['duration_ms']:.3f}",va='center',fontsize=9)
ax.set_yticks(range(len(data_rows)),[r['stage'] for r in data_rows]);ax.invert_yaxis();ax.set_xlim(0,max(r['end_ms'] for r in data_rows)*1.20);ax.set_xlabel('距Intel-32收到完整帧 / ms（独立时钟）');ax.grid(axis='x',alpha=.15);ax.set_axisbelow(True);ax.set_title('B. 数据分支本机时间轴：不能与A对齐',loc='left',fontweight='bold')

ax=fig.add_subplot(gs[1,1]);labels=list(search_parts);vals=list(search_parts.values());cs=['#7198c5','#9ca3bb','#7ca982','#d5a25e','#a291b3']
ax.barh(range(len(vals)),vals,color=cs)
for i,v in enumerate(vals):ax.text(v+.03,i,f'{v:.3f} ms  ({v/s["query_compute_slice_wall_ms"]*100:.1f}%)',va='center',fontsize=9)
ax.set_yticks(range(len(vals)),labels);ax.invert_yaxis();ax.set_xlim(0,max(vals)*1.62);ax.set_xlabel('搜索执行耗时贡献 / ms');ax.set_title(f'C. 搜索执行 {s["query_compute_slice_wall_ms"]:.3f} ms 的构成',loc='left',fontweight='bold');ax.grid(axis='x',alpha=.15);ax.set_axisbelow(True)

ax=fig.add_subplot(gs[2,:]);rc=['#b69a66','#65a48e','#839fc0']
for i,(name,m) in enumerate(rpc.items()):
 left=0
 for k,c in zip(['pool_wait_ms','send_ms','response_wait_ms'],rc):
  v=m[k];ax.barh(i,v,left=left,height=.46,color=c);left+=v
 ax.text(left+.6,i,f"队列 {m['pool_wait_ms']:.3f} / 发送 {m['send_ms']:.3f} / 等ACK {m['response_wait_ms']:.3f} ms",va='center',fontsize=10)
ax.set_yticks([0,1],list(rpc));ax.invert_yaxis();ax.set_xlim(0,80);ax.set_xlabel('各RPC从提交起计时 / ms（独立诊断；与A、B存在重叠，不可再相加）');ax.set_title('D. 网络模块记录：发送前排队 / 发送（含连接） / 响应等待',loc='left',fontweight='bold');ax.grid(axis='x',alpha=.15);ax.set_axisbelow(True)
fig.suptitle(f"动态网络模块最新测试 06_observe：近p99查询\nquery_id={q['query_id']}，job={q['job']}，Recall@10={q['recall']:.1f}；全轮p99={selection['p99_ms']:.3f} ms",fontsize=15,fontweight='bold')
fig.savefig(ROOT/'p99_latency.png',dpi=180);fig.savefig(ROOT/'p99_latency.svg');plt.close(fig)
print(json.dumps({'head_stages':rows,'data_elapsed_ms':max(r['end_ms'] for r in data_rows),'entry_ack_at_head_ms':ack,'compute_release_after_local_done_ms':report['compute_release_after_local_done_ms'],'search_parts_ms':search_parts},ensure_ascii=False,indent=2))
