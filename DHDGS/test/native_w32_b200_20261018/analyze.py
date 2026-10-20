"""Audit one frozen experiment; produce non-overlapping latency charts and report."""
from pathlib import Path
import csv,gzip,hashlib,json,math,os,sys
from collections import Counter
import numpy as np
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'python'),str(P/'tools')]
os.environ.setdefault('MPLCONFIGDIR',str(R/'.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'axes.unicode_minus':False,'font.size':11,'font.weight':500,'axes.titleweight':500,'axes.labelweight':500,'svg.fonttype':'path'})

def save(p,x):p.write_text(json.dumps(x,ensure_ascii=False,indent=2,allow_nan=False))
def dist(a):
 a=np.asarray(a,dtype=float)
 return dict(mean=float(a.mean()),p50=float(np.percentile(a,50)),p95=float(np.percentile(a,95)),p99=float(np.percentile(a,99)),max=float(a.max()),total=float(a.sum()))
def table(headers,rows):return ['| '+' | '.join(headers)+' |','|'+'|'.join(['---']*len(headers))+'|']+['| '+' | '.join(map(str,row))+' |' for row in rows]

def main():
 runs=sorted(R.glob('runs/*/summary.json'));assert len(runs)==1,runs
 run=runs[0].parent
 with gzip.open(run/'report.json.gz','rt') as f:r=json.load(f)
 qs=r['queries'];c=yaml.safe_load((R/'experiment.yaml').read_text())
 assert len(qs)==1024
 selection=json.loads((R/'frozen_query_selection.json').read_text())
 assert sorted(q['query_row'] for q in qs)==selection['validation']
 with gzip.open(run/'queries.jsonl.gz','rt') as f:assert [json.loads(x) for x in f]==qs
 for name,sha in json.loads((R/'source_manifest.json').read_text()).items():assert hashlib.sha256((P/name).read_bytes()).hexdigest()==sha,name
 for q in qs:
  s=q['stats'];assert q['status']=='ok' and s['metrics_complete']
  assert q['recall']==len(set(q['ids'])&set(q['truth_ids']))/10
  assert s['expanded']<=200 and s['node_revisit_count']==s['router_expand_rpc_count']==s['control_errors']==0
  assert math.isclose(sum(s['latency_breakdown_ms'].values()),s['latency_ms'],abs_tol=1e-6)
  bs=q['branch_reports'];assert len(set(b['node_id'] for b in bs))==len(bs)
  for b in bs:
   assert b['native_search_loop'] and b['compute_thread_migrations']==0
   assert b['work_grant']==b['work_spent']+b['work_unused']+sum(ch['work_grant'] for ch in b['children'])
   for ch in b['children']:
    assert sum(d['remote'] and d['decision_id']==ch['decision_id'] and d['selected_node']==ch['node_id'] for d in b['decisions'])==1
 services=json.loads((run/'services.json').read_text());assert services['test_completed']
 assert all(s['stopped'] and s['exit_code'] in (0,-15) for s in services['services'])
 from dhdgs.data import Dataset,load_complete_npy
 dataset=Dataset(c['paths']['dataset']);vectors=load_complete_npy(Path(c['query']['vectors']))
 for q in sorted(qs,key=lambda x:x['query_row'])[:32]:
  delta=dataset.take(np.array(q['ids'],dtype='u8')).astype('f8')-vectors[q['query_row']].astype('f8')
  np.testing.assert_allclose(q['squared_distances'],np.einsum('ij,ij->i',delta,delta),rtol=1e-12,atol=1e-10)
 keys=['expanded','unique_expanded','duplicate_expansions','max_expanded_depth','max_cross_shard_depth','server_to_server_branches','all_protocol_rpc_count','all_protocol_bytes','control_rpc_count','control_wire_bytes','router_submit_rpc_count','peer_branch_rpc_count','peer_request_bytes','peer_response_bytes','file_read_bytes','pread_calls','rabitq_evaluations','exact_distance_evaluations','fallback_entries','pread_ms','rabitq_ms','exact_distance_ms','service_ms','server_thread_cpu_ms','query_ready_queue_ms','query_compute_slice_wall_ms','query_io_wait_ms','query_transfer_finalize_wait_ms','query_state_other_ms']
 metrics={k:dist([q['stats'][k] for q in qs]) for k in keys}
 nodes=r['node_process_load'];assert len(nodes)==3
 summary={'run':str(run.relative_to(R)), 'query_count':len(qs),'qps':r['qps'],'recall_at_k':r['recall_at_k'],'latency_ms':r['latency_ms'],'recall_distribution':dict(sorted(Counter(str(q['recall']) for q in qs).items())), 'metric_distributions':metrics,'wall_stages':r['query_timing_summary']['wall_stages'], 'node_branch_count':dict(Counter(b['node_id'] for q in qs for b in q['branch_reports'])), 'branch_count_distribution':dict(Counter(len(q['branch_reports']) for q in qs)), 'physical_read_bytes':sum(n['process_deltas']['storage_read_bytes'] for n in nodes),'sum_mean_cpu_cores':sum(n['mean_cpu_cores'] for n in nodes),'local_cluster':r['local_cluster']}
 save(R/'summary.json',summary)
 save(R/'verification.json',{'passed':True,'measured_queries':1024,'exact_distance_checks':320,'frozen_rows_verified':True,'recall_recomputed':True,'jsonl_report_identical':True,'source_hashes_verified':True,'global_budget_verified':True,'budget_conservation_verified':True,'dispatch_decision_verified':True,'node_revisits':0,'thread_migrations':0,'router_expansion_rpcs':0,'services_stopped':True})
 plan=json.loads((R/'random_selection_plan.json').read_text());q=sorted(qs,key=lambda x:x['query_row'])[plan['selected_position']]
 plot=R/'query_latency';plot.mkdir(exist_ok=True);save(plot/'selected_query.json',q)
 save(plot/'selection.json',{**plan,'query_id':q['query_id'],'query_row':q['query_row'],'request_id':q['request_id'],'filters':'none; all 1024 measured requests eligible'})
 breakdown=plot_query(q,plot)
 report(r,summary,nodes,q,breakdown,run)
 files=[p for p in R.rglob('*') if p.is_file() and not p.is_symlink() and '.mplconfig' not in p.parts and p.name not in ('artifact_manifest.json','analysis.log')]
 save(R/'artifact_manifest.json',{str(p.relative_to(R)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
 print(json.dumps({'qps':r['qps'],'latency_ms':r['latency_ms'],'recall':r['recall_at_k'],'random_query_id':q['query_id'],'query_latency_ms':q['stats']['latency_ms']}))

def plot_query(q,plot):
 s=q['stats'];p=s['latency_breakdown_ms'];bs=q['branch_reports'];lat=s['latency_ms']
 top=[('routing','HNSW 路由与入口处理',p['routing_wall_ms']),('execution','数据执行与控制等待',p['peer_execution_and_control_wait_ms']),('merge','合并与生命周期清理',p['head_merge_and_lifecycle_ms'])]
 colors={'routing':'#64748b','execution':'#3d7ca6','merge':'#83939e','outside':'#b69a72','pread':'#9562b6','exact_distance':'#189b97','rabitq':'#519641','service_other':'#4079b4','branch_other':'#db8c37'}
 branchparts=[];intervals=[];domains=set()
 for i,b in enumerate(bs):
  totals=dict(pread=0.,exact_distance=0.,rabitq=0.);service=0;prev=None
  for batch in b['local_batch_trace']:
   t=batch['service_trace'];a,z=t['start_ns'],t['end_ns'];domains.add(t['clock_domain'])
   assert a<=z and (prev is None or prev<=a)
   if prev is not None and prev<a:intervals.append((i,'branch_other',prev,a))
   cursor=a
   for key,x,y in sorted(t['spans'],key=lambda v:v[1]):
    assert cursor<=x<=y<=z
    if cursor<x:intervals.append((i,'service_other',cursor,x))
    intervals.append((i,key,x,y));totals[key]+=(y-x)/1e6;cursor=y
   if cursor<z:intervals.append((i,'service_other',cursor,z))
   service+=(z-a)/1e6;prev=z
  for k,v in totals.items():assert math.isclose(v,b['stats'][k+'_ms'],abs_tol=1e-6)
  assert math.isclose(service,b['stats']['service_ms'],abs_tol=1e-6)
  wall=b['stats']['branch_wall_ms']
  parts=[('pread','pread 文件读取调用',totals['pread']),('exact_distance','精确距离代码区间',totals['exact_distance']),('rabitq','RaBitQ 估距代码区间',totals['rabitq']),('service_other','解码 / 邻居处理等批次内区间',service-sum(totals.values())),('branch_other','查询准备 / 候选维护 / 批次衔接',wall-service)]
  assert min(x[2] for x in parts)>=-1e-6
  branchparts.append({'node_id':b['node_id'],'branch_wall_ms':wall,'parts':[{'key':k,'label':l,'ms':v} for k,l,v in parts]})
 assert len(domains)==1
 if len(bs)==1:
  residual=p['peer_execution_and_control_wait_ms']-branchparts[0]['branch_wall_ms'];assert residual>=0
  rows=[top[0],('outside','提交 / 排队 / 回传 / 控制（合并残差）',residual)]+[(r['key'],r['label'],r['ms']) for r in branchparts[0]['parts']]+[top[2]]
 else:rows=top
 data=[dict(key=k,label=l,ms=v,percent=v/lat*100) for k,l,v in rows]
 assert math.isclose(sum(r['ms'] for r in data),lat,abs_tol=1e-6)
 save(plot/'breakdown.json',{'latency_ms':lat,'end_to_end_parts':data,'branches':branchparts,'timeline_clock_domain':next(iter(domains)),'pure_network_ms':None,'pure_ssd_ms':None,'method':'Disjoint wall intervals; concurrent branches never added into end-to-end fractions; elapsed code spans include scheduling waits.'})
 with (plot/'breakdown.csv').open('w') as f:
  w=csv.DictWriter(f,['key','label','ms','percent']);w.writeheader();w.writerows(data)
 fig,axes=plt.subplots(3,1,figsize=(14,11),gridspec_kw={'height_ratios':[1,3,2]},layout='constrained')
 ax=axes[0];ax.set_axis_off()
 ax.set_title(f'随机查询 ID {q["query_id"]} · 端到端 {lat:.2f} ms · Recall@10 {q["recall"]:.1%}',loc='left',fontsize=18,pad=24)
 for i,(k,l,v) in enumerate(top):
  ax.text(.16+i*.34,.5,f'{l}\n{v:.3f} ms · {v/lat*100:.2f}%',ha='center',va='center',bbox=dict(boxstyle='round,pad=.7',fc='#edf3f8',ec='#aec1d1'))
 ax.text(0,-.2,f'完整 2M 索引 · 本机三节点 · 32 线程/节点 · 预算 200 · 客户端并发 8 · 实际访问 {len(bs)} 个节点',fontsize=11)
 ax=axes[1];y=np.arange(len(data));ax.barh(y,[r['percent'] for r in data],color=[colors[r['key']] for r in data],height=.62)
 ax.set_yticks(y,[r['label'] for r in data]);ax.invert_yaxis();ax.set_xlim(0,max(r['percent'] for r in data)*1.45+3)
 for i,r in enumerate(data):ax.text(r['percent']+.6,i,f'{r["ms"]:.3f} ms ({r["percent"]:.2f}%)',va='center')
 ax.set_xlabel('占端到端延迟的百分比；各项互斥，相加为 100%');ax.grid(axis='x',alpha=.2);ax.set_axisbelow(True)
 ax=axes[2]
 for i,b in enumerate(branchparts):
  left=0
  for part in b['parts']:
   ax.barh(i,part['ms'],left=left,color=colors[part['key']],height=.5,label=part['label'] if i==0 else None)
   if part['ms']>.6:ax.text(left+part['ms']/2,i,f"{part['ms']:.2f}",ha='center',va='center',color='white',fontsize=10)
   left+=part['ms']
  ax.text(left+.1,i,f'{left:.2f} ms',va='center')
 ax.set_yticks(range(len(branchparts)),[f"节点 {b['node_id']}" for b in branchparts]);ax.invert_yaxis()
 ax.set_xlim(0,max(b['branch_wall_ms'] for b in branchparts)*1.2)
 ax.set_title('各分支 C++ 搜索循环细分（独立口径；不可与上图相加）',loc='left')
 ax.set_xlabel('分支内墙钟时间 / ms（包含等待）');ax.legend(loc='upper center',bbox_to_anchor=(.5,-.3),ncol=3,fontsize=9)
 fig.supxlabel('代码区间包含等待；提交/回传残差不等于纯网络时间，pread 不等于物理 SSD 时间。',fontsize=11)
 for ext in ['png','svg']:fig.savefig(plot/f'latency_breakdown.{ext}',dpi=160,bbox_inches='tight')
 plt.close(fig)
 origin=min(x for _,_,x,_ in intervals);end=max(z for _,_,_,z in intervals)
 fig,axes=plt.subplots(len(bs),1,figsize=(14,3*len(bs)+2),squeeze=False,layout='constrained')
 order=['branch_other','service_other','pread','exact_distance','rabitq'];labels=['查询控制 / 批次间','批次内其他','pread','精确距离','RaBitQ']
 for i,b in enumerate(bs):
  ax=axes[i,0]
  for j,k in enumerate(order):ax.broken_barh([((a-origin)/1e6,(z-a)/1e6) for bi,key,a,z in intervals if bi==i and key==k],(j-.35,.7),facecolors=colors[k])
  ax.set_yticks(range(5),labels);ax.invert_yaxis();ax.set_xlim(0,(end-origin)/1e6);ax.set_title(f'数据节点 {b["node_id"]} · {len(b["local_batch_trace"])} 个扩展批次',loc='left');ax.grid(axis='x',alpha=.2);ax.set_xlabel('相对首个扩展批次开始的本机单调时钟 / ms')
 fig.suptitle(f'随机查询 {q["query_id"]}：实测批次时间轴',fontsize=17)
 fig.supxlabel('共享同一时钟；未覆盖首批前/末批后的代码和入口处理。颜色表示代码区间墙钟跨度。',fontsize=10)
 for ext in ['png','svg']:fig.savefig(plot/f'local_timeline.{ext}',dpi=160,bbox_inches='tight')
 plt.close(fig)
 return data

# The report is generated from the same audited raw observations as the plots.
def report(r,s,nodes,q,breakdown,run):
 m=s['metric_distributions'];rel=str(run.relative_to(R));lat=r['latency_ms'];qs=r['queries'];nq=len(qs)
 lines=['# DHDGS：32 线程、搜索预算 200 测试报告','',
 f"2026-10-18，完整 200 万条索引、1,024 条固定验证查询。全部成功，平均 Recall@10 **{r['recall_at_k']:.4%}**、QPS **{r['qps']:.2f}**、平均延迟 **{lat['mean']:.2f} ms**、p99 **{lat['p99']:.2f} ms**。平均召回达到 95% 目标，但并非每个查询都达到该目标。",'',
 '## 配置与实验范围','',
 'Intel-35 本机三个独立 TCP 数据进程（127.0.0.1:19731–19733），不是三台物理服务器。每节点最多 32 个计算工作线程，客户端并发 8；每个查询在每个访问节点上由一个固定 C++ 计算线程执行，跨节点分支可并行。全局顶点扩展预算 200，在父子分支间分配，不是每个节点分别扩展 200。','',
 '完整索引：2,000,000 × 2560 float32，未归一化的 L2；1-bit RaBitQ，top-k=10、排除自身、frontier=64、local_beam=4、remote_weight=1、child_budget_fraction=0.5、备用入口开启（最多 2）。本次仅改变实验 YAML，不修改搜索实现或项目默认值。','',
 '同步 buffered pread；应用缓存为 0，Linux 页缓存保留。详细 trace 开启，BLAS 线程 1。未启用 io_uring，也未在本轮测试跨查询异步读取聚合。','',
 f"CPU：4 × Xeon Gold 6448H，128 物理核 / 256 逻辑 CPU；可见内存约 2.1 TiB。索引所在设备 /dev/nvme2n1，NVMe 3.5 TiB，运行前可用约 19 GiB；复用索引符号链接，无额外复制。完整信息见 [environment.txt](environment.txt)。",'',
 f"冻结验证集来自 200K 查询，与此前 128 条校准查询不重叠；真值通过 FullTruth 验证完整数据身份、查询和 L2 定义。每个客户端先预热 {r['warmup_queries_per_worker']} 条，共 16 次预热不计入结果；正式每条一次，共 1,024 次，计时窗口 **{r['measured_wall_seconds']:.3f} 秒**。QPS=成功请求数/整个计时窗口。该窗口较短、无重复轮次，p99 和吞吐仅作为一次实测，不给统计显著性结论。",'',
 '## 总体延迟与召回','']
 lines+=table(['指标','结果'],[['QPS',f"{r['qps']:.3f}"],['成功 / 失败',f"{r['successful_queries']} / {r['failed_queries']}"],['平均 Recall@10',f"{r['recall_at_k']:.6f}"],*[[f'延迟 {k} / ms',f'{lat[k]:.3f}'] for k in ('mean','p50','p90','p95','p99','max')],['单查询达到 Recall@10 ≥ 0.95 的比例',f"{r['queries_meeting_recall_fraction']:.4%}"]])
 lines+=['','Recall@10 以 0.1 为步长，因此单查询达到 0.95 实际要求 1.0。平均值不会反映全部困难查询；本轮存在 8 条 Recall@10=0 的查询，后续应单独分析这些查询的路由和搜索路径。','']
 lines+=table(['单查询 Recall@10','查询数'],list(s['recall_distribution'].items()))
 lines+=['','## 查询各阶段平均耗时','', '下面采用“所有查询该阶段耗时之和 / 所有查询端到端耗时之和”计算占比，三阶段互斥、合计 100%。','']
 labels={'routing_wall_ms':'HNSW 路由与入口处理','peer_execution_and_control_wait_ms':'数据节点执行与控制等待','head_merge_and_lifecycle_ms':'顶层结果合并与生命周期清理'}
 lines+=table(['阶段','平均 ms / query','占总延迟'],[[labels[k],f"{s['wall_stages'][k]['mean_ms']:.4f}",f"{s['wall_stages'][k]['fraction_of_total_latency']:.2%}"] for k in labels])
 lines+=['','以下是每个查询跨分支累计的工作时间。分支可能并行，这些值与上表口径不同，不能直接除以端到端延迟作为互斥的占比；CPU 时间也不能与墙钟代码区间相加。','']
 work=[('pread_ms','pread 调用墙钟'),('exact_distance_ms','精确距离区间'),('rabitq_ms','RaBitQ 候选估距区间'),('service_ms','扩展批次 service 总时间（包含前三项）'),('server_thread_cpu_ms','数据搜索线程累计 CPU'),('query_ready_queue_ms','分支就绪队列等待'),('query_compute_slice_wall_ms','计算执行区间墙钟（包含同步读取）'),('query_transfer_finalize_wait_ms','分支传输 / 最终处理等待'),('query_state_other_ms','分支状态其他区间')]
 lines+=table(['累计时间口径','平均 ms / query'],[[label,f"{m[k]['mean']:.4f}"] for k,label in work])
 lines+=['','`query_io_wait_ms=0` 表示本轮未走异步挂起状态；同步 pread 时间计入执行区间，并非没有读文件。RaBitQ 查询准备属于分支准备区间，不包含在候选估距统计中。纯网络等待、物理 SSD 等待、GIL 等待没有独立可靠计时，报告不为它们捏造占比。','',
 '## 网络、磁盘、搜索深度与计算量','']
 met=[('all_protocol_rpc_count','全部协议 RPC 次数'),('router_submit_rpc_count','路由器初始提交 RPC'),('peer_branch_rpc_count','数据节点间分支提交 RPC'),('control_rpc_count','控制 / 结果 RPC'),('server_to_server_branches','实际跨节点搜索扩展次数'),('all_protocol_bytes','全部应用协议字节'),('control_wire_bytes','控制 / 结果协议字节'),('file_read_bytes','逻辑文件读取字节'),('pread_calls','pread 调用次数'),('expanded','扩展顶点数'),('unique_expanded','去重扩展顶点数'),('duplicate_expansions','副本重复扩展次数'),('max_expanded_depth','单查询最大已扩展图深度'),('max_cross_shard_depth','单查询最大跨节点深度'),('rabitq_evaluations','RaBitQ 估距次数'),('exact_distance_evaluations','精确距离次数')]
 lines+=table(['指标','平均 / query','p99','最大'],[[label,*[f'{m[k][stat]:.3f}' for stat in ('mean','p99','max')]] for k,label in met])
 control=Counter()
 for row in qs:
  for kind,values in row['stats']['control_by_kind'].items():control[kind]+=values['rpc_count']
 lines+=['',f"全部应用协议流量合计 {m['all_protocol_bytes']['total']/2**20:.3f} MiB；不包含 TCP/IP 头或 TCP 重传，是本机 loopback 上的应用层计量。控制 RPC 分类总数：`{dict(control)}`。result 包含 top-k 和详细统计/trace，因此详细追踪会增加回传字节和序列化成本。heartbeat/lease/cancel 不是重新执行图搜索。",'',
 f"354/1024（{354/1024:.2%}）查询访问了多个节点；实际跨节点扩展共 {int(m['server_to_server_branches']['total'])} 次。分支数分布：`{s['branch_count_distribution']}`。路由器中间扩展 RPC 为 0，数据节点回访为 0。跨分片副本仍会导致相同向量被不同节点扩展，与节点回访是两回事。备用入口触发查询占比 {r['fallback_query_fraction']:.2%}。",'',
 f"本轮逻辑读取总计 {m['file_read_bytes']['total']/2**30:.5f} GiB，每查询 200 次 pread × 12 KiB = 2.34375 MiB。每条记录含向量和邻接信息；本次路径逐记录同步读，没有跨查询异步聚合。三个服务在计量窗口内的 `/proc` 物理读取增量之和为 **{s['physical_read_bytes']} B**。这说明读请求由热页缓存满足，本次无法评价真实 SSD 随机读延迟、IOPS 或冷缓存性能。",'',
 '图深度是搜索发现链上的最大深度，不等于扩展顶点总数，也不等于串行网络往返次数。','', '## 各数据节点负载','']
 rows=[]
 for n in nodes:
  sid=n['shard_id'];tot=r['per_node_query_totals'][str(sid)]
  rows.append([sid,s['node_branch_count'].get(sid,0),int(tot['expanded']),f"{n['mean_cpu_cores']:.3f}",f"{n['process']['rss_bytes']/2**30:.3f}",n['peak_active_since_start'],n['process_deltas']['storage_read_bytes']])
 lines+=table(['节点','执行分支数','扩展顶点总数','平均 CPU 核当量','结束时 RSS / GiB','峰值活动分支*','物理读取 B'],rows)
 lines+=['',f"三个数据进程平均 CPU 核当量合计 **{s['sum_mean_cpu_cores']:.3f}**，不包含客户端/入口进程。扩展负载最大值/均值 {r['expansion_imbalance']['max_to_mean']:.3f}，变异系数 {r['expansion_imbalance']['coefficient_of_variation']:.3f}。RSS 不包含全部共享页缓存，不能把 RSS 当作整个系统的内存占用。",'',
 '峰值活动分支从服务启动开始统计，可能包含预热，且不等于同一时刻真正使用 CPU 的线程数。客户端最多 8 个查询在途，每查询每节点最多一个活动分支；因此本轮无法让单节点的 32 个工作线程同时忙于 32 条查询。这是 32 线程配置的并发 8 测试，不是 32 线程饱和吞吐测试。','',
 '## 构建、启动与正确性','',
 f"本次复用现有 26.42 GiB 索引，未重新构建。原构建记录为 {r['build_seconds']:.3f} 秒（约 {r['build_seconds']/60:.2f} 分钟），build_stages.csv 是历史构建指标。此次三个服务启动耗时 {r['local_cluster']['startup_seconds']:.3f} 秒；客户端各自加载路由/元数据约 {min(r['engine_load_seconds']):.2f}–{max(r['engine_load_seconds']):.2f} 秒。它们均不计入查询 QPS 窗口。",'',
 '核验通过：1,024 个冻结 query row；逐查询召回重算；JSONL 与总报告一致；全部预算 ≤200；父子预算守恒；每次子分支有扩展函数批准；无节点回访、无计算线程迁移、无控制错误；源码与原生模块哈希未变。另使用原始 float32 向量转 float64，独立复算 32 条查询的 320 个返回距离。三个测试服务均已停止，退出码 -15 是测试框架的正常 SIGTERM 清理。详见 [verification.json](verification.json)。','',
 '初次在沙箱启动被禁止 TCP socket，未执行查询；失败尝试的日志保留在 execution.log 和较早的 runs 目录中。授权后的正式运行完整成功。此次仅参数测试，不重复运行此前已通过的 79 项实现回归。','',
 '## 随机查询：阶段耗时与时间轴','',
 f"测试前固定种子 2026101832200，从按 query_row 排序的全部 1,024 条请求中均匀抽取，不按召回、延迟或分支数筛选。选中 query ID **{q['query_id']}**（row {q['query_row']}），Recall@10={q['recall']:.1f}，延迟 **{q['stats']['latency_ms']:.3f} ms**。",'',
 '![随机查询阶段耗时](query_latency/latency_breakdown.png)','']
 lines+=table(['端到端阶段','ms','占比'],[[x['label'],f"{x['ms']:.4f}",f"{x['percent']:.2f}%"] for x in breakdown])
 bdata=json.loads((R/'query_latency/breakdown.json').read_text())['branches']
 lines+=['','该查询由节点 0 进入，完成 37 次本地扩展后批准节点 1 分支，将 81 个剩余扩展预算交给节点 1；节点 0 继续本地执行。最终节点 0 扩展 119 个、节点 1 扩展 81 个，合计 200；重复副本扩展 5 次。两个分支分别耗时如下，属于分支内部口径，不可相加当作端到端延迟。','']
 lines+=table(['C++ 循环区间','节点 0 / ms','节点 1 / ms'],[[bdata[0]['parts'][i]['label'],f"{bdata[0]['parts'][i]['ms']:.4f}",f"{bdata[1]['parts'][i]['ms']:.4f}"] for i in range(5)]+[['循环总墙钟',*[f"{b['branch_wall_ms']:.4f}" for b in bdata]]])
 lines+=['','![随机查询扩展时间轴](query_latency/local_timeline.png)','',
 '时间轴根据实际 trace 绘制，已核验所有进程共享单调时钟域、单分支批次互斥以及细分区间与累计字段一致。横轴以首批次开始为零，未包含首批前/末批后或入口端代码；不能据此把未显示区间统称为网络等待。分支 CPU 字段还包括 Python 边界和报告处理，与 C++ 循环墙钟范围不同，不直接计算二者比值。','',
 f"这条查询共 {q['stats']['all_protocol_rpc_count']} 次协议 RPC：1 次初始提交、1 次数据节点间派发、2 次结果回传、1 次 heartbeat、1 次 lease、2 次清理 cancel。共 {q['stats']['all_protocol_bytes']:,} 应用协议字节；pread 累计 {q['stats']['pread_ms']:.4f} ms。",'',
 '## 与上一轮结果的关系','']
 prev=json.loads((P/'test/native_search_20261018/comparison.json').read_text())
 baseline=next(x for x in prev if x['name'].startswith('validation/') and x['model']=='native' and x['threads']==8 and x['backend']=='sync_pread')
 lines+=table(['配置','QPS','平均 ms','p99 ms','Recall@10'],[['前轮 8线程 / 预算192',f"{baseline['qps']:.2f}",f"{baseline['latency_ms']['mean']:.2f}",f"{baseline['latency_ms']['p99']:.2f}",f"{baseline['recall']:.4%}"],['本轮 32线程 / 预算200',f"{r['qps']:.2f}",f"{lat['mean']:.2f}",f"{lat['p99']:.2f}",f"{r['recall_at_k']:.4%}"]])
 lines+=['',f"观察值：QPS 变化 {(r['qps']/baseline['qps']-1):+.2%}、p99 变化 {(lat['p99']/baseline['latency_ms']['p99']-1):+.2%}。两轮同时改变线程数和预算，预热条件/运行时间也不同，且均为短窗口单次测量；不能据此认定 32 线程造成退化。本轮没有展示出超过前轮的性能收益。",'',
 '按你优先最低 p99 的标准，目前不能据此推荐把默认工作线程提升到 32。下一步若要隔离原因，应固定预算 200，比较 8/16/32 工作线程，在客户端并发 8 和 32 下重复较长窗口；另把 trace 开关作为独立变量，核验结果回传和生命周期耗时。真实 SSD 性能应另做明确缓存条件的独立实验；本次未自动清空系统页缓存。','',
 '## 文件与复现','',
 '- [实验 YAML](experiment.yaml)：全部运行参数。',
 f'- [逐查询 CSV]({rel}/queries.csv)：每条请求的延迟、召回和开销。',
 f'- [逐查询阶段均值]({rel}/per_query_stage_means.csv)；[全体阶段均值]({rel}/query_stage_means.csv)。本轮每条一次，逐查询均值等于本次观测。',
 f'- [节点资源]({rel}/process_load.csv)；[原始完整报告]({rel}/report.json.gz)；[逐查询原始记录]({rel}/queries.jsonl.gz)。',
 '- [随机选择记录](query_latency/selection.json)；[原始随机查询](query_latency/selected_query.json)；[图表数值](query_latency/breakdown.json)。',
 '- 图表同时保留 PNG 和 SVG；[耗时图 SVG](query_latency/latency_breakdown.svg)、[时间轴 SVG](query_latency/local_timeline.svg)。',
 '- source_manifest.json 保存运行前源码/原生模块/配置哈希，artifact_manifest.json 保存最终实验产物哈希。大体积原始记录仅保留本机。','',
 '从工作区根目录运行（需要允许本机 TCP socket）：','',
 '```sh',
 'gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/native_search_20261018/run_config.py --config gene_database/DHDGS/test/native_w32_b200_20261018/experiment.yaml',
 'gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/native_w32_b200_20261018/analyze.py',
 '```','',
 '重新测试请先复制 YAML 并更换 paths.output，防止把多个轮次混入本报告；analyze.py 对本实验目录要求只有一个成功轮次。只重画本次图表和重做审计，直接运行第二条命令。','']
 (R/'REPORT.md').write_text('\n'.join(lines))

if __name__=='__main__':main()
