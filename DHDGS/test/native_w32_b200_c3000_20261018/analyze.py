"""Audit the overload run and retain failed requests in all-request metrics."""
from pathlib import Path
from collections import Counter
import csv,gzip,hashlib,json,math,os,sys
import numpy as np
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'python'),str(P/'tools')]
os.environ.setdefault('MPLCONFIGDIR',str(R/'.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'font.weight':500,'axes.labelweight':500,'axes.titleweight':500,'axes.unicode_minus':False,'svg.fonttype':'path'})

def save(p,x):p.write_text(json.dumps(x,ensure_ascii=False,indent=2,allow_nan=False))
def table(h,rows):return ['| '+' | '.join(h)+' |','|'+'|'.join(['---']*len(h))+'|']+['| '+' | '.join(map(str,x))+' |' for x in rows]
def main():
 runs=list(R.glob('runs/*/report.json.gz'));assert len(runs)==1
 run=runs[0].parent
 with gzip.open(runs[0],'rt') as f:r=json.load(f)
 qs=r['queries'];ok=[q for q in qs if q['status']=='ok'];bad=[q for q in qs if q['status']!='ok'];complete=[q for q in ok if q['stats']['metrics_complete']]
 assert len(qs)==6144 and len(ok)+len(bad)==6144
 selected=json.loads((R/'query_selection.json').read_text())['validation'];assert Counter(q['query_row'] for q in qs)==Counter({x:6 for x in selected})
 assert len({q['request_id'] for q in qs})==6144
 with gzip.open(run/'queries.jsonl.gz','rt') as f:assert [json.loads(x) for x in f]==qs
 for name,digest in json.loads((R/'source_manifest.json').read_text()).items():assert hashlib.sha256((P/name).read_bytes()).hexdigest()==digest,name
 for q in qs:
  assert q['recall']==len(set(q['ids'])&set(q['truth_ids']))/10
  assert math.isclose(q['client_wall_ms'],(q['client_end_ns']-q['client_start_ns'])/1e6,abs_tol=1e-6)
 for q in ok:
  s=q['stats'];assert s['expanded']<=200 and s['node_revisit_count']==s['router_expand_rpc_count']==0
  assert math.isclose(sum(s['latency_breakdown_ms'].values()),s['latency_ms'],abs_tol=1e-6)
  for b in q['branch_reports']:
   assert b['native_search_loop'] and b['compute_thread_migrations']==0
   assert b['work_grant']==b['work_spent']+b['work_unused']+sum(ch['work_grant'] for ch in b['children'])
   for ch in b['children']:assert sum(d['remote'] and d['decision_id']==ch['decision_id'] and d['selected_node']==ch['node_id'] for d in b['decisions'])==1
 from dhdgs.data import Dataset,load_complete_npy
 import yaml
 c=yaml.safe_load((R/'experiment.yaml').read_text());dataset=Dataset(c['paths']['dataset']);vectors=load_complete_npy(Path(c['query']['vectors']))
 for q in ok[:32]:
  delta=dataset.take(np.array(q['ids'],dtype='u8')).astype('f8')-vectors[q['query_row']].astype('f8')
  np.testing.assert_allclose(q['squared_distances'],np.einsum('ij,ij->i',delta,delta),rtol=1e-12,atol=1e-10)
 services=json.loads((run/'services.json').read_text());clients=json.loads((run/'clients_lifecycle.json').read_text())
 assert services['test_completed'] and all(s['stopped'] and s['exit_code'] in (0,-15) for s in services['services'])
 assert len(clients)==30 and all(p['stopped'] and p['exit_code']==0 for p in clients)
 gate=json.loads((run/'START.json').read_text());assert gate['ready_threads']==3000
 events=sorted([(q['client_start_ns'],1) for q in qs]+[(q['client_end_ns'],-1) for q in qs]);start=gate['monotonic_ns'];end=max(q['client_end_ns'] for q in qs)
 t=[0.];active=[0];cur=peak=0;above2500_ns=0;at3000_ns=0;last=start;area=0
 for ts,d in events:
  if cur>=2500:above2500_ns+=ts-last
  if cur==3000:at3000_ns+=ts-last
  area+=cur*(ts-last);cur+=d;peak=max(peak,cur);t.append((ts-start)/1e9);active.append(cur);last=ts
 assert cur==0 and peak==r['actual_peak_inflight']==3000
 assert math.isclose(area/(end-start),r['mean_inflight'],abs_tol=1e-8)
 seconds=r['measured_wall_seconds'];assert math.isclose(seconds,(end-start)/1e9,abs_tol=1e-8)
 assert math.isclose(r['qps'],len(ok)/seconds,abs_tol=1e-8)
 with (run/'client_timing.csv').open('w') as f:
  w=csv.DictWriter(f,['job','request_id','query_row','repetition','worker_process','worker_thread','status','client_start_ns','client_end_ns','client_wall_ms']);w.writeheader()
  for q in qs:w.writerow({k:q[k] for k in w.fieldnames})
 with gzip.open(run/'resource_samples.json.gz','rt') as f:resources=json.load(f)
 sampled_client_threads=max(sum(p.get('threads') or 0 for p in x['clients']) for x in resources)
 sampled_client_rss=max(sum(p.get('rss_bytes') or 0 for p in x['clients']) for x in resources)/2**30
 control_errors=Counter((e['kind'],e['error_type'],e['error']) for q in ok for e in q['stats']['control_error_details'])
 valid_qps=len(complete)/seconds
 verified={'passed_record_audit':True,'system_passed_load_test':False,'requests':6144,'successful_requests':len(ok),'failed_requests':len(bad),'complete_metric_successes':len(complete),'sources_verified':True,'query_rows_verified':True,'all_request_recall_verified':True,'raw_records_equal':True,'success_budget_and_native_thread_invariants_verified':True,'exact_distance_checks':320,'all_services_and_clients_stopped':True,'actual_peak_inflight':peak,'seconds_inflight_ge2500':above2500_ns/1e9,'seconds_inflight_eq3000':at3000_ns/1e9,'complete_metric_success_qps':valid_qps,'sampled_client_peak_threads':sampled_client_threads,'sampled_client_peak_rss_gib':sampled_client_rss,'control_errors_on_successes':[{'kind':k[0],'type':k[1],'error':k[2],'count':v} for k,v in control_errors.items()]}
 save(R/'verification.json',verified)
 fig,axes=plt.subplots(2,1,figsize=(12,8),layout='constrained')
 ax=axes[0];ax.step(t,active,where='post',color='#276e9e');ax.axhline(3000,ls='--',color='#c14747');ax.set_ylim(0,3300);ax.set_xlabel('自压测放行起 / 秒');ax.set_ylabel('客户端实际在途请求数');ax.set_title(f'并发峰值 {peak}，平均 {r["mean_inflight"]:.1f}；高并发突发负载，非稳态 3000 并发');ax.grid(alpha=.2)
 labels=['成功且指标完整','成功但指标不完整','容量拒绝（含子分支）','请求超时'];values=[len(complete),len(ok)-len(complete),sum('capacity exceeded' in q['error'] for q in bad),sum(q['error_type']=='TimeoutError' for q in bad)]
 ax=axes[1];ax.barh(labels,values,color=['#409568','#d9a649','#c76a45','#954c69']);ax.invert_yaxis();ax.set_xlim(0,max(values)*1.25)
 for i,v in enumerate(values):ax.text(v+25,i,f'{v:,} ({v/len(qs):.2%})',va='center')
 ax.set_xlabel('请求数 / 共 6,144 次');ax.set_title('成功 QPS 与拒绝/超时必须分开计算');ax.grid(axis='x',alpha=.2);ax.set_axisbelow(True)
 fig.suptitle('DHDGS · 32 线程/节点 · 预算 200 · 客户端并发 3000',fontsize=17)
 for ext in ('png','svg'):fig.savefig(R/f'load_result.{ext}',dpi=160,bbox_inches='tight')
 plt.close(fig)
 rel=str(run.relative_to(R));base=json.loads((P/'test/native_w32_b200_20261018/summary.json').read_text());lat=r['latency_ms']
 lines=['# 3000 客户端并发：32 线程、预算 200 压测','',
 f"本次达到真实客户端在途请求峰值 **3000**。共 6,144 次请求，成功 **{len(ok)}**、失败 **{len(bad)}（{len(bad)/len(qs):.2%}）**。成功 QPS **{r['qps']:.2f}**，成功请求平均延迟 **{lat['mean']/1000:.3f} 秒**、p99 **{lat['p99']/1000:.3f} 秒**。当前配置不能稳定承受该压力。",'',
 '![实际并发及请求结果](load_result.png)','',
 '## 配置、范围与并发定义','',
 '完整 2M × 2560 索引，Intel-35 本机三个独立 TCP 数据进程；每节点 32 个 C++ 计算工作线程，全局扩展预算 200、frontier 64、top-k 10、原始 L2、RaBitQ、sync_pread、应用缓存 0、详细 trace 开启。服务端参数与上一轮一致：max_active_queries=128/节点、listen_backlog=128、handoff_timeout=5秒、control_timeout=1秒、heartbeat_interval=250ms、lease_timeout=5秒。未调大接纳限制或修改生产搜索代码。','',
 '旧驱动逐客户端完整复制 Engine 会创建 3000 份路由/位置字典。此次使用实验目录中的独立驱动，30 个客户端进程 × 每进程 100 个工作线程；每进程共享一份只读 Engine，查询状态仍逐请求独立，正常调用原有 Engine.search。原生 HNSW 固定 ef 后可并发只读搜索，RaBitQ score 使用局部查询状态。每个进程预热 2 次（共 60 次，不计时），所有 3000 个工作线程就绪后统一放行。','',
 '沿用前轮冻结的 1024 条验证 query row，每条重复 6 次，共 6144 请求；每线程最多一个查询在途。任务按 job % 30 分配到进程，各进程线程从本地任务队列领取；末尾部分进程先耗尽任务，因此并发会下降。既有全量真值按请求 query ID / row 核验，未重做全库暴力扫描。','',
 f"计时窗口 **{seconds:.3f} 秒**，包含放行后请求调度及最后一次搜索调用返回，排除引擎加载、预热、结果压缩收尾和真值核验。实际在途按 Engine.search 调用进入至返回（含失败清理）的单调时钟区间重建：峰值 {peak}、均值 {r['mean_inflight']:.2f}；≥2500 请求持续 {above2500_ns/1e9:.3f} 秒，恰为 3000 持续 {at3000_ns/1e9:.3f} 秒。客户端在途不等于服务端已接纳分支数或同时占用的 CPU 数。",'',
 '这是一次有限请求集的高并发突发/过载测试，不能视为长期保持 3000 并发的稳态吞吐。压测器/入口处理与数据节点在同一物理机器上运行，争用同一主机资源。与前轮相比，客户端从单进程 8 份 Engine 改成 30 进程共享元数据，预热和请求总数也变化，不能把差异全部归因于并发数。','',
 '## 吞吐、延迟与召回','']
 lines+=table(['指标','结果'],[['成功请求数',len(ok)],['失败请求数',len(bad)],['成功率',f'{len(ok)/len(qs):.2%}'],['成功 QPS',f"{r['qps']:.3f}"],['全部完成 QPS（含拒绝/超时）',f"{r['completed_qps']:.3f}"],['成功且指标完整的请求数',len(complete)],['成功且指标完整的 QPS',f'{valid_qps:.3f}'],['成功请求平均 Recall@10',f"{r['recall_at_k_successes']:.4%}"],['所有请求平均 Recall@10（失败记 0）',f"{r['recall_at_k_all_requests']:.4%}"]])
 lines+=['','“成功且指标完整”是更严格的统计口径，不要求单条召回达标。失败请求不从总分母中删除；成功请求子集可能存在选择偏差，其较高召回不能解释为并发提升了质量。','']
 lines+=table(['延迟 / ms','成功请求','失败请求','全部请求'],[[k,*[f'{r[field][k]:.3f}' for field in ('latency_ms','failure_latency_ms','all_latency_ms')]] for k in ('mean','p50','p90','p95','p99','max')])
 lines+=['','延迟以客户端外层完整调用计时，包含异常后的清理。原始失败 stats.latency_ms 可能在异常清理前取值，因此本报告失败延迟和总体延迟使用 client_wall_ms；原始值仍完整保留。','',
 '## 失败与指标不完整','']
 lines+=table(['错误','次数'],list(r['error_types'].items()))
 lines+=['','容量拒绝共 3553 次：3073 次直接返回容量错误，480 次经分支执行异常向上返回。另有 863 次请求超时；日志只记录 timed out，不能把它们全部归为某一种 socket 阶段。','',
 f"1,728 次成功请求中，**929 次指标不完整**：其原因为控制 RPC 失败或分支传输重试。成功请求观察到的控制超时合计：`{dict((k[0],v) for k,v in control_errors.items())}`；有 {sum(q['stats']['rpc_retries']>0 for q in ok)} 个成功请求发生分支提交重试。返回 top-k 成功不代表控制通信没有异常。",'',
 '容量检查使用尚未 ack 的分支数，包含排队、执行中以及完成但结果尚未确认的分支，不仅是正在计算的线程。因此 32 个线程未全部占满时，也可能先耗尽 128 个接纳名额。服务端当前的控制线程逐条发送心跳/结果并等待响应，高压下的回传延迟、客户端控制连接超时可能让名额释放滞后。这是代码路径支持的机制解释；本轮没有独立分离其耗时或证实它是唯一瓶颈。','',
 '## 成功请求各阶段延迟','',
 '以下 1728 条成功请求都有入口端阶段计时。占比采用阶段总耗时 / 成功请求内部总耗时；控制指标不完整并不取消入口端墙钟记录。','']
 labels={'routing_wall_ms':'HNSW 路由与入口','peer_execution_and_control_wait_ms':'数据执行与控制等待','head_merge_and_lifecycle_ms':'结果合并与生命周期清理'}
 lines+=table(['阶段','平均 ms','占成功请求内部总延迟'],[[labels[k],f"{v['mean_ms']:.3f}",f"{v['fraction_of_total_latency']:.2%}"] for k,v in r['query_timing_summary']['wall_stages'].items()])
 internal=r['query_timing_summary']['mean_latency_ms_for_wall_samples'];lines+=['',f"内部阶段合计平均 {internal:.3f} ms；外层调用平均 {lat['mean']:.3f} ms，平均差值 {lat['mean']-internal:.3f} ms 为计时边界/返回开销。数据执行与控制等待占比约 89.21%，不应解读为纯 CPU 计算时间或纯网络耗时。",'',
 '下表只使用 **799 条指标完整的成功请求**，是跨分支累计工作时间，分支可并行，不能与上表相加，也不能外推全部 6144 请求。','']
 work=r['query_timing_summary']['cumulative_work']
 lines+=table(['累计工作区间','平均 ms / 完整请求'],[[name,f"{work[k]['mean_ms']:.3f}"] for k,name in [('query_ready_queue_ms','分支就绪排队'),('query_compute_slice_wall_ms','分支计算区间（含同步读取）'),('query_transfer_finalize_wait_ms','分支派发收尾等待'),('pread_ms','pread 调用'),('exact_distance_ms','精确距离代码'),('rabitq_ms','RaBitQ 估距代码'),('server_thread_cpu_ms','搜索线程 CPU')]])
 lines+=['','## 网络、磁盘与节点负载','',
 '失败请求缺少完整分支计数，部分成功请求控制计数也不完整，因此不能给出可信的“全部请求网络总字节数”。下面报告完整成功子集的均值，并保留所有原始数据。','']
 metricnames=[('all_protocol_rpc_count','全部协议 RPC'),('all_protocol_bytes','应用层协议字节'),('server_to_server_branches','跨节点扩展次数'),('max_expanded_depth','最大已扩展图深度'),('expanded','扩展顶点数'),('pread_calls','pread 次数'),('file_read_bytes','逻辑读取字节')]
 lines+=table(['完整成功子集指标','平均 / query'],[[label,f"{np.mean([q['stats'][k] for q in complete]):.3f}"] for k,label in metricnames])
 lines+=['','全部协议 RPC 含入口提交、数据节点之间派发、结果和心跳/租约/清理；不是图搜索跨机次数。应用协议字节不含 TCP/IP 头。','']
 nodes=r['node_process_load']
 lines+=table(['节点','平均 CPU 核当量','结束 RSS / GiB','逻辑读取 GiB*','物理读取 B','未 ack 分支**'],[[n['shard_id'],f"{n['mean_cpu_cores']:.3f}",f"{n['process']['rss_bytes']/2**30:.3f}",f"{n['counter_deltas']['file_read_bytes']/2**30:.3f}",n['process_deltas']['storage_read_bytes'],n['peer_lifecycle_after']['unacknowledged']] for n in nodes])
 lines+=['',f"数据进程合计平均约 {sum(n['mean_cpu_cores'] for n in nodes):.3f} 核当量，资源快照窗口约 {nodes[0]['process_window_seconds']:.3f} 秒，稍长于查询计时窗口。*进程计数包含失败/取消查询实际发生的工作，不能只与成功请求累加值比较。**停止服务前快照仍有未 ack 的终态分支，均无 queued/running 分支；随后终止测试服务清理。",'',
 f"客户端采样峰值线程总数 {sampled_client_threads}（除 3000 工作线程外，还包括逐查询回调监听/处理等线程）；采样 RSS 合计峰值 {sampled_client_rss:.3f} GiB。这些是 0.5 秒间隔采样得到的峰值下界，未作为服务端 CPU 指标。",'',
 '三个数据进程物理读取增量均为 0 B，仍是 Linux 热页缓存测试。数据量和索引未变，未测冷 SSD，也未启用异步跨查询 SSD 聚合。','',
 '## 与并发 8 的前轮对照','']
 lines+=table(['指标','并发 8：前轮','并发 3000：本轮'],[['请求总数',1024,6144],['失败率','0%',f'{len(bad)/len(qs):.2%}'],['成功 QPS',f"{base['qps']:.2f}",f"{r['qps']:.2f}"],['成功平均延迟 ms',f"{base['latency_ms']['mean']:.2f}",f"{lat['mean']:.2f}"],['成功 p99 ms',f"{base['latency_ms']['p99']:.2f}",f"{lat['p99']:.2f}"],['全请求平均 Recall@10',f"{base['recall_at_k']:.4%}",f"{r['recall_at_k_all_requests']:.4%}"]])
 lines+=['','成功 QPS 增加伴随着大量失败和秒级尾延迟，不满足稳定检索目标。下一步适合先确认目标负载下的接纳/排队策略，以及逐查询监听线程和串行控制回传路径的扩展能力；不能仅把 max_active_queries 调大就认定问题已解决。保留所有默认服务端参数，未擅自修改超时、接纳上限或搜索逻辑。','',
 '## 审计与文件','',
 '原始记录完整性审计通过，不等于本次系统压力测试通过。核验了 6144 个唯一请求 ID、每个冻结查询恰好重复六次、召回重算、逐请求外层计时、JSONL/总报告一致和源码哈希。对所有成功请求核验全局预算 ≤200、预算守恒、扩展批准、无节点回访、C++ 固定计算线程；独立复算 32 条成功请求的 320 个返回距离。失败请求无完整图状态，不声称其预算或路径已逐条核验。','',
 '30 个客户端进程均正常退出，三个本轮启动的数据服务均已停止（正常 SIGTERM，退出码 -15）。复用历史索引，未重新构建；前轮已记录构建约 2510.98 秒。','',
 '- [实验参数](experiment.yaml)；[压测器参数](load_plan.yaml)；[独立驱动](run_load.py)。',
 f'- [汇总原始值]({rel}/summary.json)；[逐查询指标 CSV]({rel}/queries.csv)；[外层延迟/并发时序 CSV]({rel}/client_timing.csv)。',
 f'- [逐查询 JSONL]({rel}/queries.jsonl.gz)；[完整报告数据]({rel}/report.json.gz)；[资源采样]({rel}/resource_samples.json.gz)。',
 f'- [阶段均值]({rel}/query_stage_means.csv)；[逐 query 阶段均值]({rel}/per_query_stage_means.csv)。后者按 row 汇总六次请求，阶段均值仅使用成功样本，失败数单列。',
 '- [审计结果](verification.json)；[图表 SVG](load_result.svg)。','',
 '工作区根目录复现：先运行本目录 prepare.py，再运行 run_load.py（需要本机 TCP 权限）；运行 analyze.py 可重做本轮审计/图表。若要新增独立轮次，复制实验目录脚本并更新 paths.output；不要覆盖本次参数和原始记录。','']
 (R/'REPORT.md').write_text('\n'.join(lines))
 save(R/'artifact_manifest.json',{str(p.relative_to(R)):hashlib.sha256(p.read_bytes()).hexdigest() for p in R.rglob('*') if p.is_file() and not p.is_symlink() and '.mplconfig' not in p.parts and '__pycache__' not in p.parts and p.name not in ('artifact_manifest.json','analysis.log')})
 print(json.dumps(verified,ensure_ascii=False))
if __name__=='__main__':main()
