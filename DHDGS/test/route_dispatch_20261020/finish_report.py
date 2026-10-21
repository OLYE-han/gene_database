import json,statistics
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parent;c=json.loads((ROOT/'comparison.json').read_text());rows=c['runs'];groups={}
for r in rows:groups.setdefault((r['spec']['route_threads'],r['spec']['head_connections']),[]).append(r)
order=[(4,32),(8,32),(16,32),(16,64),(16,128)]
def mean(g,key):return statistics.mean(r['metrics'][key]['mean'] for r in groups[g])
fig,axes=plt.subplots(2,1,figsize=(12,8),layout='constrained')
labels=['Route queue','Routing','Data + control','Merge queue','Merge','Result handoff'];keys=['route_queue','routing','data_control','merge_queue','merge','handoff'];colors=['#bd77a8','#4584b9','#e2a247','#aa5353','#52a079','#647a84']
for j,g in enumerate(order):
 left=0
 for i,k in enumerate(keys):
  v=mean(g,k);axes[0].barh(j,v,left=left,color=colors[i],label=labels[i] if j==0 else None)
  if v>2:axes[0].text(left+v/2,j,f'{v:.2f}',ha='center',va='center')
  left+=v
axes[0].set_yticks(range(5),[f'route {r}, conn {n}' for r,n in order]);axes[0].invert_yaxis();axes[0].legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=9);axes[0].set_xlabel('Mean per-query stage duration, ms (two runs; API submission excluded)');axes[0].set_title('Route queue reduction does not imply end-to-end throughput gain')
for i,(k,label) in enumerate([('entry_pool_wait_ms','Before send / pool + scheduling'),('entry_send_ms','Send incl. connect'),('entry_response_wait_ms','Response wait')]):
 axes[1].bar([j+(i-1)*.25 for j in range(5)],[mean(g,k) for g in order],width=.25,label=label)
axes[1].set_xticks(range(5),[f'route {r}\nconn {n}' for r,n in order]);axes[1].set_ylabel('Mean entry RPC duration / ms');axes[1].legend();axes[1].set_title('Entry RPC components overlap data execution; do not add to top chart')
fig.savefig(ROOT/'waiting_breakdown.png',dpi=180);fig.savefig(ROOT/'waiting_breakdown.svg');plt.close(fig)
lines=['','## 分组结论','','每组两轮算术平均；p99列是每轮p99的均值，不是合并样本p99。','','|路由线程|顶层连接/目标|QPS|p99 ms|路由排队ms|发送前排队ms|入口响应等待ms|','|---|---:|---:|---:|---:|---:|---:|']
for g in order:
 a=c['averages'][f'r{g[0]}_c{g[1]}'];lines.append(f"|{g[0]}|{g[1]}|{a['qps']:.2f}|{a['p99']:.2f}|{mean(g,'route_queue'):.3f}|{mean(g,'entry_pool_wait_ms'):.3f}|{mean(g,'entry_response_wait_ms'):.3f}|")
a=c['averages'];route_qps=(a['r16_c32']['qps']/a['r4_c32']['qps']-1)*100
lines += ['',f'路由线程4→16，QPS变化{route_qps:+.2f}%。路由队列确实缩短，但数据与控制区间增加，整体没有稳定吞吐增益。不能将路由排队长等同于路由计算是唯一瓶颈。','']
for conn in [64,128]:
 b=a[f'r16_c{conn}'];base=a['r16_c32'];lines.append(f"顶层连接32→{conn}：QPS {(b['qps']/base['qps']-1)*100:+.2f}%，每轮p99均值 {(b['p99']/base['p99']-1)*100:+.2f}%。")
lines+=['','![等待构成](waiting_breakdown.png)','','## 数据与控制区间的进一步观测','','以下仍为每查询均值，单位ms；分支累计时长可能重叠，不可直接从端到端时长相减计算网络时间。','','|路由/连接|数据与控制|分支累计总时长|分支计算就绪|分支搜索执行|分支派发收尾|最后结果收到至合并入队|结果全部处理后等入口ACK|','|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for g in order:lines.append('|'+str(g)+'|'+'|'.join(f'{mean(g,k):.3f}' for k in ['data_control','query_branch_wall_ms','query_ready_queue_ms','search_wall','query_transfer_finalize_wait_ms','report_to_merge','ack_after_reports'])+'|')
lines+=['','实际代码中的routes线程在异步提交入口RPC后返回，没有等待ACK；入口网络客户端由单个事件循环处理。pool_wait_ms覆盖客户端incoming/waiting队列与连接分配前调度间隔，不能精确区分连接占用和事件循环调度。response_wait_ms覆盖发送结束至响应处理结束，不能精确区分远端接收排队、ACK发送和本地事件循环处理。当前数据不能证明其中任何一项是唯一瓶颈。','','下一步应在上述路径加入分段时间戳与队列/连接占用采样：客户端入队→事件循环取走→获得连接→发送完成；数据端报文收齐→处理开始→ACK入发送队列→实际发出；结果报告同样记录。各段均用本机单调时钟，跨机器不直接相减。随后才有依据对单事件循环增加分片或资源。当前实验只调整已有配置，没有引入未经对照的网络实现改动。','','测试服务已关闭，原有三个数据节点及127.0.0.1:22091管理端口仍可达；十轮源码hash与查询选择一致。']
with (ROOT/'REPORT.md').open('a') as f:f.write('\n'.join(lines)+'\n')
print('\n'.join(lines[:20]))
