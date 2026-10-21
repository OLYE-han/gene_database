import json,statistics
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parent;c=json.loads((ROOT/'comparison.json').read_text());rows=c['runs'];order=[256,512,1024,2048];groups={n:[r for r in rows if r['spec']['concurrency']==n] for n in order}
def avg(n,key):return statistics.mean(r[key] for r in groups[n])
def metric(n,key):return statistics.mean(r['metrics'][key]['mean'] for r in groups[n])
fig,ax=plt.subplots(1,2,figsize=(12,4.5),layout='constrained')
for a,key,title in zip(ax,['qps','p99'],['Throughput / QPS','Per-run p99 / ms']):
 for run in range(2):a.plot(order,[groups[n][run][key] for n in order],marker='o',label=f'Run {run+1}')
 a.set_xscale('log',base=2);a.set_xticks(order,[str(n) for n in order]);a.set_xlabel('Client concurrency');a.set_ylabel(title);a.legend();a.grid(alpha=.2)
fig.suptitle('Three machines | 64 search threads/node | route 16 / connections 32 | budget 200')
fig.savefig(ROOT/'concurrency_qps_p99.png',dpi=180);fig.savefig(ROOT/'concurrency_qps_p99.svg');plt.close(fig)
fig,ax=plt.subplots(figsize=(12,5),layout='constrained')
keys=['route_queue','routing','data_control','merge_queue','merge','handoff'];labels=['Route queue','HNSW routing','Data + network/control','Merge queue','Merge','Result handoff'];colors=['#ae77b0','#488abc','#e2a342','#bd6565','#57a07b','#66808c']
for j,n in enumerate(order):
 left=0
 for i,k in enumerate(keys):
  v=metric(n,k);ax.barh(j,v,left=left,color=colors[i],label=labels[i] if j==0 else None)
  if v>8:ax.text(left+v/2,j,f'{v:.1f}',ha='center',va='center')
  left+=v
ax.set_yticks(range(4),[str(n) for n in order]);ax.invert_yaxis();ax.set_ylabel('Client concurrency');ax.set_xlabel('Mean stage duration / ms (two runs; excludes API submission)');ax.legend(loc='upper left',bbox_to_anchor=(1,1));ax.set_title('Where latency grows as concurrency increases')
fig.savefig(ROOT/'waiting_breakdown.png',dpi=180);fig.savefig(ROOT/'waiting_breakdown.svg');plt.close(fig)
lines=['','## 两轮汇总','','每组两轮算术平均；p99为每轮p99的均值，不是合并样本p99。','','|客户端并发|QPS|相对256|p99 ms|平均延迟ms|Recall@10|','|---:|---:|---:|---:|---:|---:|']
for n in order:lines.append(f"|{n}|{avg(n,'qps'):.2f}|{(avg(n,'qps')/avg(256,'qps')-1)*100:+.2f}%|{avg(n,'p99'):.2f}|{avg(n,'mean'):.2f}|{avg(n,'recall'):.5f}|")
lines+=['','![并发与QPS/p99](concurrency_qps_p99.png)','','## 等待变化','','下列为均值，ms；分支累计值与顶层时间可能重叠，不可相加。','','|并发|路由排队|入口发送前排队|入口响应等待|数据与控制|计算就绪累计|搜索执行累计|搜索CPU累计|最后结果收到至合并入队|','|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
for n in order:lines.append('|'+str(n)+'|'+'|'.join(f'{metric(n,k):.3f}' for k in ['route_queue','entry_pool_wait_ms','entry_response_wait_ms','data_control','query_ready_queue_ms','search_wall','server_thread_cpu_ms','report_to_merge'])+'|')
lines+=['','![排队构成](waiting_breakdown.png)','','本次结果用于比较给定负载下的有限请求测试，不是稳态容量证明。每轮仅10000请求；2048并发约对应5批在途窗口，启动和排空效应较明显。没有通过增加预算或降低召回来换取吞吐。','','新增原始分片与report_context已做可逆gzip压缩，SHA256与原名映射见各轮archive_manifest.json。需要重跑原始离线处理时先用gzip -dk恢复分片与report_context原名；已生成的报告和queries.jsonl.gz可直接读取。历史实验文件未改动。','','八轮结束后独立测试服务关闭，常驻服务保持可达；各轮源码hash和查询选择相同，有效顶层配置均为16路由线程/32连接，实际数据工作线程64，实际峰值在途分别达到256/512/1024/2048。']
with (ROOT/'REPORT.md').open('a') as f:f.write('\n'.join(lines)+'\n')
print('\n'.join(lines[:12]))
