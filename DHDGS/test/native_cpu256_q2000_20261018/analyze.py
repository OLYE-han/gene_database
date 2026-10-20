"""Audit 2000 queries and distinguish configured hardware threads from CPU use."""
from pathlib import Path
from collections import Counter
import csv,gzip,hashlib,json,math,os,sys
import numpy as np
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1];sys.path[:0]=[str(P/'python'),str(P/'tools')]
os.environ.setdefault('MPLCONFIGDIR',str(R/'.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'font.weight':500,'axes.titleweight':500,'axes.labelweight':500,'axes.unicode_minus':False,'svg.fonttype':'path'})

def save(p,d):p.write_text(json.dumps(d,ensure_ascii=False,indent=2,allow_nan=False))
def table(h,rows):return ['| '+' | '.join(h)+' |','|'+'|'.join(['---']*len(h))+'|']+['| '+' | '.join(map(str,row))+' |' for row in rows]
def main():
 files=list(R.glob('runs/*/report.json.gz'));assert len(files)==1;run=files[0].parent
 with gzip.open(files[0],'rt') as f:r=json.load(f)
 qs=r['queries'];ok=[q for q in qs if q['status']=='ok'];bad=[q for q in qs if q['status']!='ok'];full=[q for q in ok if q['stats']['metrics_complete']]
 assert len(qs)==2000 and len({q['request_id'] for q in qs})==2000
 selection=json.loads((R/'query_selection.json').read_text());assert sorted(q['query_row'] for q in qs)==selection['validation']
 assert len(set(selection['validation']))==2000 and not set(selection['validation'])&set(selection['calibration_excluded'])
 for name,sha in json.loads((R/'source_manifest.json').read_text()).items():assert hashlib.sha256((P/name).read_bytes()).hexdigest()==sha,name
 with gzip.open(run/'queries.jsonl.gz','rt') as f:assert [json.loads(x) for x in f]==qs
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
 c=yaml.safe_load((R/'experiment.yaml').read_text());dataset=Dataset(c['paths']['dataset']);qv=load_complete_npy(Path(c['query']['vectors']))
 for q in ok[:32]:
  diff=dataset.take(np.array(q['ids'],dtype='u8')).astype('f8')-qv[q['query_row']].astype('f8')
  np.testing.assert_allclose(q['squared_distances'],np.einsum('ij,ij->i',diff,diff),rtol=1e-12,atol=1e-10)
 services=json.loads((run/'services.json').read_text());clients=json.loads((run/'clients_lifecycle.json').read_text())
 assert services['test_completed'] and all(x['stopped'] and x['exit_code'] in (0,-15) for x in services['services'])
 assert len(clients)==20 and all(x['stopped'] and x['exit_code']==0 for x in clients)
 start=json.loads((run/'START.json').read_text());assert start['ready_threads']==2000
 events=sorted([(q['client_start_ns'],1) for q in qs]+[(q['client_end_ns'],-1) for q in qs]);cur=peak=0;t=[0];inflight=[0]
 for stamp,d in events:cur+=d;peak=max(peak,cur);t.append((stamp-start['monotonic_ns'])/1e9);inflight.append(cur)
 assert cur==0 and peak==r['actual_peak_inflight']
 before=json.loads((run/'cpu_before.json').read_text());after=json.loads((run/'cpu_after.json').read_text())
 per_cpu=[]
 for k,v in before['cpu'].items():
  if k=='cpu':continue
  b=after['cpu'][k];total=b['total']-v['total'];busy=b['busy']-v['busy'];per_cpu.append({'cpu':int(k[3:]),'busy_percent':100*busy/total if total else 0})
 per_cpu.sort(key=lambda x:x['cpu']);assert len(per_cpu)==256
 with (R/'logical_cpu_utilization.csv').open('w') as f:
  w=csv.DictWriter(f,['cpu','busy_percent']);w.writeheader();w.writerows(per_cpu)
 with gzip.open(run/'resource_samples.json.gz','rt') as f:samples=json.load(f)
 window_start=start['monotonic_ns'];window_end=max(q['client_end_ns'] for q in qs)
 samples=[s for s in samples if window_start<=s['monotonic_ns']<=window_end]
 plotrows=[]
 for a,b in zip(samples,samples[1:]):
  dt=(b['monotonic_ns']-a['monotonic_ns'])/1e9
  if dt<=0:continue
  old={p['pid']:p for p in a['nodes']};cores=sum(p['cpu_seconds']-old[p['pid']]['cpu_seconds'] for p in b['nodes'] if p.get('cpu_seconds') is not None and p['pid'] in old and old[p['pid']].get('cpu_seconds') is not None)/dt
  ac=a['host_cpu']['cpu']['cpu'];bc=b['host_cpu']['cpu']['cpu'];total=bc['total']-ac['total'];busy=bc['busy']-ac['busy']
  plotrows.append({'time_seconds':(b['monotonic_ns']-window_start)/1e9,'data_process_cpu_cores':cores,'host_busy_percent':100*busy/total if total else 0})
 assert plotrows
 with (R/'cpu_timeseries.csv').open('w') as f:
  w=csv.DictWriter(f,list(plotrows[0]));w.writeheader();w.writerows(plotrows)
 nodes=r['node_process_load'];mean_cores=sum(n['mean_cpu_cores'] for n in nodes);peak_cores=max(x['data_process_cpu_cores'] for x in plotrows)
 threads={str(n['shard_id']):sorted({b['compute_thread_id'] for q in ok for b in q['branch_reports'] if b['node_id']==n['shard_id']}) for n in nodes}
 usage={'hardware_threads':256,'physical_cores':128,'configured_workers_per_node':256,'configured_workers_total':768,'mean_data_process_cpu_cores':mean_cores,'mean_data_process_percent_of_256':mean_cores/256*100,'sampled_peak_data_process_cpu_cores':peak_cores,'host_logical_cpu_mean_busy_percent':float(np.mean([x['busy_percent'] for x in per_cpu])),'logical_cpus_over_80_percent_full_window':sum(x['busy_percent']>=80 for x in per_cpu),'successful_query_compute_thread_ids_by_node':threads,'cpu_window_seconds':(after['monotonic_ns']-before['monotonic_ns'])/1e9}
 save(R/'cpu_summary.json',usage)
 verification={'records_verified':True,'requests':2000,'successes':len(ok),'failures':len(bad),'complete_metric_successes':len(full),'source_hashes_verified':True,'independent_exact_distance_checks':10*min(32,len(ok)),'successful_request_budget_dispatch_thread_invariants_verified':True,'all_test_processes_stopped':True,'actual_peak_inflight':peak,'cpu_saturation_achieved':mean_cores>=256*.9}
 save(R/'verification.json',verification)
 fig,axes=plt.subplots(3,1,figsize=(12,11),layout='constrained')
 ax=axes[0];ax.step(t,inflight,where='post',color='#237aa8');ax.set_ylabel('客户端在途请求');ax.set_title(f'2000 条不同查询，峰值在途 {peak}');ax.grid(alpha=.2)
 ax=axes[1];ax.plot([x['time_seconds'] for x in plotrows],[x['data_process_cpu_cores'] for x in plotrows],label='三个数据进程合计',color='#2e9871');ax.axhline(mean_cores,color='#b27628',ls='--',label=f'快照窗口平均 {mean_cores:.2f} 核当量');ax.set_ylabel('CPU 核当量');ax.set_title('数据进程实际用核量（纵轴按观测值缩放）');ax.legend();ax.grid(alpha=.2)
 ax=axes[2];ax.plot([x['time_seconds'] for x in plotrows],[x['host_busy_percent'] for x in plotrows],color='#8060a3');ax.set_ylim(0,100);ax.set_ylabel('整机 CPU 忙碌率 / %');ax.set_xlabel('自统一放行起 / 秒');ax.set_title('整机含压测客户端与其他任务，不能全部归因于数据节点');ax.grid(alpha=.2)
 fig.suptitle('每节点线程池上限 256 ≠ 实际占满 256 个硬件线程',fontsize=17)
 for ext in ('png','svg'):fig.savefig(R/f'cpu_load.{ext}',dpi=160,bbox_inches='tight')
 plt.close(fig)
 rel=str(run.relative_to(R));lat=r['latency_ms']
 lines=['# 256 线程池上限、2000 条查询测试','',f"完成 2000 条不同查询测试，实际客户端并发峰值 **{peak}**。三个数据进程合计平均使用 **{mean_cores:.3f} CPU 核当量**，相当于整机 256 个硬件线程的 **{mean_cores/256:.2%}**；采样峰值约 **{peak_cores:.3f} 核当量**。**本次没有实现 CPU 满载，不能把线程池配置成 256 称为跑满。**",'',
 '![实际 CPU 利用率与在途查询](cpu_load.png)','',
 '## 配置与测试范围','',
 '2026-10-18，Intel-35 本机三个 TCP 数据服务；整机 4 路 Xeon Gold 6448H、128 个物理核、256 个硬件线程，所有测试进程可调度到 CPU 0–255。未绑定 CPU、未做 NUMA 内存绑定。三个进程共享这台机器，不是三个各有 256 硬件线程的物理节点。','',
 '每个数据节点 peer.worker_threads=256，三进程线程池上限合计 768。线程池按需创建线程，768 不是实际运行线程数或硬件核数；该设置允许某个较忙分片使用较多线程，整体仍由 OS 在 256 个硬件线程上调度。查询在每个分支内由固定计算线程执行，不让多个线程共同计算一个分支。未增加无关的忙循环或后台负载来制造满载。','',
 '完整 200 万条 × 2560 维索引，未归一化 L2、1-bit RaBitQ、top-k 10、全局扩展预算 200、frontier 64、local_beam 4、sync_pread、应用缓存 0、详细 trace 开启。索引复用，未重新构建。','',
 '20 个客户端进程 × 100 个查询工作线程，共 2000 个并发槽；每进程共享只读路由元数据，各预热 2 条，共 40 次预热不计入测试。2000 条不同查询只执行一次：保留此前 1024 条验证查询，再用固定种子 202610182000 从剩余查询补充 976 条，排除此前 128 条校准查询。总查询数有限，因此并发只在开始阶段接近满额，随后下降，不是稳态满载实验。','',
 '为了避免上轮容量拒绝遮蔽计算表现，本次实验 YAML 调整如下；生产代码与默认配置未改。','']
 lines+=table(['参数','上轮','本轮'],[['peer.worker_threads',32,256],['peer.max_active_queries',128,2048],['network.listen_backlog',128,4096],['peer.control_timeout_seconds',1,5],['peer.handoff_timeout_seconds',5,30],['peer.lease_timeout_ms',5000,30000],['peer.heartbeat_interval_ms',250,1000],['客户端并发',3000,2000],['查询总数',6144,2000]])
 lines+=['','控制超时和租约放宽是为减小高并发下的过早失败，不能视为性能优化收益。本轮改变多个参数，不能单独用来证明增加工作线程的因果效果。','',
 '## 查询结果','']
 lines+=table(['指标','结果'],[['成功 / 失败',f'{len(ok)} / {len(bad)}'],['成功 QPS',f"{r['qps']:.3f}"],['全部完成 QPS（含失败）',f"{r['completed_qps']:.3f}"],['计时窗口 / 秒',f"{r['measured_wall_seconds']:.3f}"],['成功平均延迟 / ms',f"{lat.get('mean',0):.3f}"],['成功 p50 / ms',f"{lat.get('p50',0):.3f}"],['成功 p95 / ms',f"{lat.get('p95',0):.3f}"],['成功 p99 / ms',f"{lat.get('p99',0):.3f}"],['成功最大延迟 / ms',f"{lat.get('max',0):.3f}"],['成功请求平均 Recall@10',f"{r['recall_at_k_successes']:.4%}"],['全部请求平均 Recall@10（失败记0）',f"{r['recall_at_k_all_requests']:.4%}"],['成功且指标完整',len(full)],['成功但指标不完整',len(ok)-len(full)]])
 lines+=['',f"错误分类：`{r['error_types']}`。外层延迟包含调用返回前的失败清理；原始失败内部 stats.latency_ms 可能在清理前记录，因此保留两者，避免低估失败等待。",'',
 '## CPU 利用率、工作线程与节点负载','']
 lines+=table(['节点','平均 CPU 核当量','快照窗口 / 秒','成功分支使用过的计算线程数*','结束 RSS / GiB','物理读取 B'],[[n['shard_id'],f"{n['mean_cpu_cores']:.3f}",f"{n['process_window_seconds']:.3f}",len(threads[str(n['shard_id'])]),f"{n['process']['rss_bytes']/2**30:.3f}",n['process_deltas']['storage_read_bytes']] for n in nodes])
 lines+=['','*计算线程数按成功分支的原生 compute_thread_id 去重，表示整个窗口用过多少线程，不表示同时忙碌数量；失败分支可能缺少对应记录。线程是否一直占用 CPU 应查看 CPU 时间增量，不能用 OS 线程数量或在途查询数代替。','',
 f"CPU 核当量定义为进程 user+system CPU 秒 / 快照窗口秒；一个硬件线程持续执行约为 1 核当量。占全机比例按 256 个逻辑 CPU 归一化，超线程共享物理核心资源，不代表线性算力倍数。每 0.1 秒左右采样数据进程和整机计数，实际间隔受调度影响；峰值基于实际采样间隔计算。",'',
 f"整机各逻辑 CPU 窗口平均忙碌率的均值为 {usage['host_logical_cpu_mean_busy_percent']:.2f}%，其中窗口平均忙碌率 ≥80% 的逻辑 CPU 有 {usage['logical_cpus_over_80_percent_full_window']} 个。整机统计还包含压测器和其他用户进程，不归因于 DHDGS 数据服务。详见 [逐逻辑 CPU CSV](logical_cpu_utilization.csv)。",'',
 '## 时间消耗与未跑满的原因','']
 labels={'routing_wall_ms':'路由与入口','peer_execution_and_control_wait_ms':'数据执行与控制等待','head_merge_and_lifecycle_ms':'结果合并与生命周期清理'}
 lines+=table(['成功请求阶段','平均 ms','占内部端到端时间'],[[labels[k],f"{v['mean_ms']:.3f}",f"{v['fraction_of_total_latency']:.2%}"] for k,v in r['query_timing_summary']['wall_stages'].items()])
 lines+=['','以下均值仅来自指标完整的成功请求，是跨分支累加的时间，不可与上表相加：','']
 work=r['query_timing_summary']['cumulative_work']
 lines+=table(['累计时间','平均 ms'],[[label,'无样本' if work[k]['mean_ms'] is None else f"{work[k]['mean_ms']:.3f}"] for k,label in [('query_ready_queue_ms','就绪排队'),('query_compute_slice_wall_ms','分支执行区间'),('query_transfer_finalize_wait_ms','派发收尾等待'),('server_thread_cpu_ms','搜索线程 CPU'),('pread_ms','文件读取调用'),('exact_distance_ms','精确距离'),('rabitq_ms','RaBitQ 估距')]])
 lines+=['','本轮验证的是“放大线程池和接纳上限后是否自然用满 CPU”。结果表明当前完整检索路径仍不足以持续供给 256 个硬件线程：2000 条查询中的相当一部分时间消耗在查询生命周期、控制回传及等待，而不是搜索线程持续计算。每查询预算只有 200，单个 C++ 搜索分支的计算任务较短。','',
 '本轮还有一个明确的独立并发限制：search.max_inflight 仍为 3，PeerRuntime 用它配置每节点的跨节点派发线程池。计算工作线程完成原生搜索后，会等待自己提交的派发 future 完成；这部分属于 query_transfer_finalize_wait_ms，包含派发排队及执行完成等待，不是 CPU 计算。本轮该项每查询跨分支累计平均约 868.658 ms，明显高于分支执行区间的 14.326 ms。此处是值得优先隔离验证的等待点；未实测调大派发池的收益，不能直接宣称已确定唯一瓶颈。','',
 '从现有代码看，每查询创建回调监听线程、服务端逐条发送控制/结果消息、Python 入口和协议处理仍是潜在限制；放大数据线程池不会自动并行化这些路径。该机制解释不能代替各瓶颈的独立剖析，本次没有将全部等待武断归为 GIL 或网络。要进一步实现真实 CPU 满载，应先优化并验证派发/入口/控制路径的并发处理，再以持续负载观察吞吐和尾延迟，而不是继续单纯增加线程数。','',
 '## 网络、磁盘和搜索工作量','',
 '完整成功请求子集均值如下；失败或控制计数不完整请求不外推网络总量。','']
 keys=[('all_protocol_rpc_count','全部协议 RPC 次数'),('all_protocol_bytes','应用协议字节'),('server_to_server_branches','跨节点扩展次数'),('max_expanded_depth','最大已扩展图深度'),('expanded','扩展顶点数'),('file_read_bytes','逻辑读取字节')]
 lines+=table(['指标','平均 / 完整成功请求'],[[label,'无样本' if not full else f"{np.mean([q['stats'][k] for q in full]):.3f}"] for k,label in keys])
 lines+=['',f"三个数据进程物理读取增量合计 {sum(n['process_deltas']['storage_read_bytes'] for n in nodes)} B；buffered pread 保留页缓存，不能把本轮当作冷 SSD 测试。逻辑读取包含失败查询也可能已经执行的工作，和返回成功请求累加量不必相等。",'',
 '## 审计、记录与复现','',
 '核验全部 2000 个查询身份唯一、冻结选择与排除校准集合、召回重算、原始记录一致性和源代码哈希。对成功查询核验预算 ≤200、父子预算守恒、远端扩展批准、无数据节点回访、固定原生计算线程，并独立复算 32 条成功查询的 320 个距离。失败查询没有完整分支状态，不声称其全部内部过程已审计。20 个客户端正常退出，三个测试服务均正常 SIGTERM 停止。','',
 '- [实验 YAML](experiment.yaml)；[驱动参数](load_plan.yaml)；[CPU 汇总](cpu_summary.json)；[核验记录](verification.json)。',
 f'- [查询明细]({rel}/queries.csv)；[原始逐查询记录]({rel}/queries.jsonl.gz)；[完整报告数据]({rel}/report.json.gz)。',
 f'- [资源采样]({rel}/resource_samples.json.gz)；[逐查询阶段耗时]({rel}/per_query_stage_means.csv)。',
 '- [CPU 时间序列](cpu_timeseries.csv)；[图表 SVG](cpu_load.svg)。',
 '- 本目录 run_load.py 执行测试，analyze.py 重做记录审计与图表。所有实验参数放在 YAML 中；测试需要本机 TCP 权限，索引通过本目录 index 符号链接复用。', '']
 (R/'REPORT.md').write_text('\n'.join(lines))
 save(R/'artifact_manifest.json',{str(p.relative_to(R)):hashlib.sha256(p.read_bytes()).hexdigest() for p in R.rglob('*') if p.is_file() and not p.is_symlink() and '.mplconfig' not in p.parts and '__pycache__' not in p.parts and p.name not in ('artifact_manifest.json','analysis.log')})
 print(json.dumps({'verification':verification,'cpu':{k:v for k,v in usage.items() if k!='successful_query_compute_thread_ids_by_node'}}))
if __name__=='__main__':main()
