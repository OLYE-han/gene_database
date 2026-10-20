"""Build the report from audited final runs, keeping two-run ranges visible."""
from pathlib import Path
import json
R=Path(__file__).resolve().parent
rows=json.loads((R/'comparison.json').read_text());audit=json.loads((R/'all_runs_verification.json').read_text())
assert audit['all_passed']
groups={c:[r for r in rows if r['concurrency']==c] for c in sorted({r['concurrency'] for r in rows})}
def span(rs,key,digits=1):
 v=[r[key] for r in rs];fmt=lambda x:f'{x:.{digits}f}'
 return fmt(min(v)) if len(v)==1 else fmt(min(v))+'–'+fmt(max(v))
lines=['# 同时在途并发数与就绪等待实验','',
'所有正式运行采用修正后的客户端驱动；生产搜索代码未修改，启用上一轮已验证的C++完成/报告控制路径。降低并发上限能降低系统内排队，但整批2000条需求若同时产生，尚未提交的查询仍会在客户端等待。','',
'## 条件与口径','',
'- 完整seq_2M已有索引，本机三个TCP数据进程，热页缓存、sync_pread。每节点32计算线程，搜索预算200，top-k=10。','- 各档使用相同2000条验证查询，40条总预热查询不计入结果。固定20个客户端进程，将指定数量的工作线程均分到各进程；并发小于20时部分进程只预热、不执行计时查询。','- 各线程预先获得互不重叠的查询序号，当前请求完成后才提交下一条；没有跨进程取任务锁。所有查询总数均为2000。跨线程不做工作窃取，因此尾部可能出现线程提前结束，实际峰值和平均在途同时记录。','- 并发上限不是固定QPS开放到达模型。本实验测量的是有积压任务时的有界在途服务曲线，不能外推为任意到达率下的延迟SLA。','- 先2000→1，再1→2000反向重复，保持源码/原生模块/各组参数冻结；全体生命周期追踪，仅查询734744保留详细轨迹。两轮范围不等同于置信区间。','- **提交后API延迟**：调用search到返回，不包含下一条查询等待获得线程的时间。**客户端积压**：假定2000条需求在START同时产生，从START到真正调用search。**需求到完成**：从START到该query返回；逐query等于前两者之和。','- **分支就绪等待**：数据节点accepted→worker_enter，包含接纳、提交及计算队列。**query就绪并集**：同query各分支上述区间取并集。**已观测等待并集**沿用此前分析，包含入口连接池及已测队列，扣除与同query路由/原生搜索重叠部分；不是完整纯off-CPU剖析。','',
'## 吞吐与延迟','',
'下表为两轮各自统计量的最小–最大范围，不合并样本后重新计算分位数。','',
'| 并发上限 | 实际峰值 | QPS | API平均 ms | API p99 ms | 分支就绪 p99 ms | query就绪并集 p99 ms | 平均Recall@10 |',
'|---:|---:|---:|---:|---:|---:|---:|---:|']
for c,rs in groups.items():
 lines.append(f"| {c} | {span(rs,'actual_peak',0)} | {span(rs,'qps')} | {span(rs,'latency_ms_mean')} | {span(rs,'latency_ms_p99')} | {span(rs,'accepted_to_worker_ms_p99')} | {span(rs,'server_ready_union_ms_p99')} | {min(r['recall'] for r in rs):.3%}–{max(r['recall'] for r in rs):.3%} |")
lines+=['','![并发扫描曲线](concurrency_sweep.png)','','## 检查100ms目标及客户端等待','',
'这里用已观测等待并集检查目标，同时列出最大值和超过100ms的数量；p99小于100ms不代表每条都满足。未观测到的等待不能被默认为零。','',
'| 并发 | 已观测等待 p99 ms | 最大值 ms | 超100ms条数/4000 | 提交前客户端等待 p99 ms | 需求到完成 p99 ms | 整批完成秒 |',
'|---:|---:|---:|---:|---:|---:|---:|']
for c,rs in groups.items():
 lines.append(f"| {c} | {span(rs,'query_exposed_queue_union_ms_p99')} | {span(rs,'query_exposed_queue_union_ms_max')} | {sum(r['exposed_wait_over_100ms'] for r in rs)}/{sum(r['queries'] for r in rs)} | {span(rs,'client_backlog_ms_p99')} | {span(rs,'offered_to_complete_ms_p99')} | {span(rs,'batch_seconds',3)} |")
strict=[c for c,rs in groups.items() if all(r['query_exposed_queue_union_ms_max']<100 for r in rs)]
api=[c for c,rs in groups.items() if all(r['latency_ms_max']<100 for r in rs)]
lines+=['',f'两轮已观测等待最大值均低于100ms的测试档位：{strict}。两轮提交后完整API最大值均低于100ms的档位：{api}。这是本次样本结果，不是未来所有请求的硬上界。','',
'## 其他指标','',
'| 并发 | 平均协议RPC/query | 平均协议字节/query | 累计搜索线程CPU ms/query | 文件读取字节/query | 物理读取增量 | 节点平均用核数（0/1/2，两轮均值） |',
'|---:|---:|---:|---:|---:|---:|---|']
for c,rs in groups.items():
 cpu='/'.join(f"{sum(r['node_cpu_cores'][i] for r in rs)/len(rs):.2f}" for i in range(3))
 lines.append(f"| {c} | {span(rs,'mean_all_protocol_rpc_count',3)} | {span(rs,'mean_all_protocol_bytes',0)} | {span(rs,'mean_server_thread_cpu_ms',2)} | {span(rs,'mean_file_read_bytes',0)} | {sum(r['physical_read_bytes'] for r in rs)} | {cpu} |")
lines+=['','CPU用核数使用节点metrics采样窗口，包含测量开始/结束取样开销，不能当作精确搜索区间利用率。pread文件字节不等于SSD物理字节；本轮物理读取为0，结论仅适用于热页缓存本机测试。逐query距离计算、读调用、跳数、跨服务器分支数等仍完整记录。','',
'## 正确性与实验完整性','',
f"正式{audit['run_count']}轮、{audit['queries']}条计时请求通过审计。每条成功、指标完整、预算≤200，预算守恒，数据节点不回访，每个远端分支对应批准决策，单分支计算线程不迁移。接纳/计算/发送队列排空，无容量拒绝或原生控制事件错误，所有本轮服务及客户端正常退出。平均召回均≥.95；不声称每条query召回≥.95。",'',
'初始驱动使用跨进程共享计数器领取任务，在2000并发配置下产生p99约1796ms的取任务锁等待，实际峰值只有197。该9轮探测已移到各组harness_probe_runs，元数据及初始分析在[harness_probe](harness_probe/README.md)，**完全排除于正式对比**。此问题属于测试端，生产系统未作修改。修正驱动采用预分配序号，九档的2000条覆盖、无重复/遗漏已静态核验；正式审计再次按实际输出核验。','',
'## 文件与复验','',
'- [机器可读比较CSV](comparison.csv)、[JSON](comparison.json)：包含各阶段mean/p99/max、RPC、字节、计算、负载与等待超限数。','- [正式运行清单](final_runs.json)、[审计结果](all_runs_verification.json)、[源码与参数哈希](sources.json)。','- 各c*/experiment.yaml为完整系统参数，load_plan.yaml记录客户端线程分布；输出在c*/runs/时间戳，包含summary、queries、nodes_before/after、资源采样、完整报告和服务清理记录。','- analysis/c*/时间戳/query_latency_and_backlog.csv记录每query的客户端积压、提交后API和需求到完成；query_wait_union.csv记录已观测等待并集。','',
'项目根目录复验（使用相同源码，依次运行，不能重叠占用端口20561–20563）：','',
'```sh','.venv/bin/python test/concurrency_20261018/run_suite.py --groups c2000 c1000 c512 c256 c128 c64 c32 c16 c1 c1 c16 c32 c64 c128 c256 c512 c1000 c2000','.venv/bin/python test/concurrency_20261018/analyze.py --plots','.venv/bin/python test/concurrency_20261018/audit_all.py','.venv/bin/python test/concurrency_20261018/plot_query.py c32','.venv/bin/python test/concurrency_20261018/plot_query.py c64','.venv/bin/python test/concurrency_20261018/plot_query.py c2000','.venv/bin/python test/concurrency_20261018/make_report.py','```','',
'run_suite在每组运行前校验冻结哈希；analyze会重新收集正式runs目录，包含新增复验。需要保存旧报告时请新建实验目录。没有修改生产默认并发，测试推荐值见结论补充。','']
baseline=sum(r['qps'] for r in groups[2000])/len(groups[2000])
recommend=[]
for c in (32,64):
 rs=groups[c];ratio=(sum(r['qps'] for r in rs)/len(rs))/baseline
 recommend.append(f'并发{c}保留并发2000约{ratio:.1%}的吞吐；API p99 {span(rs,"latency_ms_p99",2)}ms，已观测等待最大值{max(r["query_exposed_queue_union_ms_max"] for r in rs):.2f}ms。')
lines[2:2]=['**结论：降低同时在途并发确实降低就绪等待。** '+ ' '.join(recommend), '',
 '如果以本次已观测等待均低于100ms并尽量保持吞吐为标准，64是已测试档位中最高的合格并发；128已有超限。若更重视尾延迟和余量，32更稳妥；16的API p99更低但吞吐约1100QPS；绝对最低API p99出现在并发1，但吞吐仅约81QPS。未扫描65–127，不能称64为精确最大可用并发，也没有修改生产默认值。','']
lines+=['## 固定查询的提交后时序','', '沿用查询734744，未按结果选择查询。图以真正提交为零点，提交前客户端积压单独标注；各分支并行，不能把轨道总时长相加。','']
for c in (32,64,2000):
 row=groups[c][-1];path=f'analysis/{row["group"]}/{row["run"]}'
 lines.append(f'- [并发{c}时序图]({path}/query_timeline.png) · [时间段CSV]({path}/query_timeline.csv) · [原始查询]({path}/selected_query.json)')
lines+=['',f'![并发32查询时序](analysis/{groups[32][-1]["group"]}/{groups[32][-1]["run"]}/query_timeline.png)','']
(R/'REPORT.md').write_text('\n'.join(lines))
print(R/'REPORT.md')
