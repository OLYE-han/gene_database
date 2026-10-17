"""Create a reviewable report and complete YAML candidates from measured results."""
from pathlib import Path
import csv,json,hashlib,yaml
root=Path(__file__).resolve().parent
points=json.loads((root/'points_summary.json').read_text())
comparisons=json.loads((root/'paired_comparisons.json').read_text())
selection=json.loads((root/'selection_before_validation.json').read_text())['selection']
bykey={(p['stage'],p['concurrency'],p['threads']):p for p in points}
lines=['# 工作线程数实验：p99 优先','',
'2026-10-16，单机三数据进程、TCP 回环、4096×2560 不可变索引、L2、1 bit RaBitQ。所有结论限于当前 Python/C++ 原型、缓存友好数据规模和已测负载。没有修改产品代码或默认配置。','',
'## 结论与选择方法','',
'用户明确要求优先最低 p99，再比较 QPS。扫描每节点工作线程数 1/2/4/8/16/32 与客户端并发 1/2/4/8/16/32 的 36 个组合，两轮随机顺序校准；每个固定客户端并发分别选择无失败、每轮平均 Recall@10≥0.99 的最低合并 p99 配置，完全相同的 p99 才用 QPS、线程数依次打破并列。冻结选择后用新查询独立验证，再按事先记录的方案延长请求窗口；没有从验证结果重新挑选其他线程数。','',
'校准在并发 2/4/8/16/32 都选中 1 线程，在并发 1 选中 32 线程。后者只是在少量尾部样本上的选择：单客户端同一节点最多运行一个 query，线程池不会因容量为 32 就有 32 个搜索任务；必须根据复验判断，不能解释为 32 核加速。','',
'## 较长窗口复验结果','',
'每个配置两次独立服务启动，每次 256 个 query 各重复 4 次，合计 2048 个请求。QPS=两轮成功请求总数/两轮计时总时长；p99 对两轮请求合并计算，不是两轮 p99 的平均值。下表的线程数为校准时冻结的候选，另一列为默认线程数 4 的对照；搜索预算均为 64、frontier=32，并非默认搜索预算 256。','',
'| 客户端并发 | 冻结候选线程 | 候选 p99 ms | 4线程 p99 ms | p99 降幅 | 候选 QPS | 4线程 QPS | 候选平均 Recall@10 |',
'|---|---|---|---|---|---|---|---|']
for x in comparisons:
 if x['stage']=='confirmation':
  lines.append(f"| {x['concurrency']} | {x['chosen_threads']} | {x['chosen_p99_ms']:.2f} | {x['baseline_p99_ms']:.2f} | {x['p99_reduction_percent']:.1f}% | {x['chosen_qps']:.2f} | {x['baseline_qps']:.2f} | {x['chosen_recall']:.6f} |")
tail=bykey['confirmation',32,1]
lines+=['',f"并发 32、1 线程的两次独立复验 p99 分别落在 {tail['trial_p99_min']:.2f}–{tail['trial_p99_max']:.2f} ms，波动明显。合并 p99 不能代替逐轮稳定性：其 {tail['requests']} 次请求中有 {tail['over_1000ms']} 次超过 1 秒，其中 {tail['first_wave_over_1000ms']} 次位于各轮最初同时发出的查询。保留这些请求，不把合并值作为尾延迟上限承诺。"]
lines+=['','## 不确定性与建议','',
'按 query_row 成组 bootstrap 2000 次，重复请求随其 query 整组重采样；延迟差使用相同 query 索引配对重采样。下面区间描述这批查询的变动，不覆盖所有跨运行环境变化、时间相关性、非独占主机干扰或挑选最优值带来的全部不确定性；只有两次独立服务重启，仍不能当作生产稳定性保证。','',
'| 客户端并发 | 候选减4线程 p99 的95%区间 ms | 候选 Recall 的95%区间 | 建议 |','|---|---|---|---|']
recommended=[]
for x in comparisons:
 if x['stage']!='confirmation':continue
 c=x['concurrency'];w=x['chosen_threads'];p=bykey['confirmation',c,w]
 evidence=(x['descriptive_ci_supports_lower_p99'] and p['failed']==0 and p['trial_recall_min']>=.99)
 chosen=w if evidence else 4
 reason='本范围内候选优于4线程，作为实验推荐' if evidence else '未形成稳定改善证据，保留4线程基线'
 lines.append(f"| {c} | [{x['p99_delta_ci_low']:.2f}, {x['p99_delta_ci_high']:.2f}] | [{p['recall_ci_low']:.6f}, {p['recall_ci_high']:.6f}] | {reason} |")
 cfg=yaml.safe_load((root/'confirmation'/f'w{chosen}_c{c}_trial0/config.yaml').read_text())
 out=root/'recommended'/f'c{c}_w{chosen}';out.mkdir(parents=True,exist_ok=True)
 if not (out/'index').exists():(out/'index').symlink_to(yaml.safe_load((root/'plan.yaml').read_text())['index'],target_is_directory=True)
 cfg['paths']['output']=str(out)
 (out/'config.yaml').write_text(yaml.safe_dump(cfg,sort_keys=False))
 recommended.append({'concurrency':c,'threads':chosen,'config':str(out/'config.yaml'),'reason':reason,
                     'observed_mean_recall_met':p['trial_recall_min']>=.99,
                     'recall_lower_bound_met':p['recall_ci_low']>=.99})
lines+=['','推荐仅是当前原型的实验工作点，生产默认 YAML 保持不变。召回均值达标与置信区间下限达标是不同结论：重复同一批 query 增加计时观测，不能把 256 个独立 query 变为 2048 个独立质量样本。必须保留上述质量不确定性。','',
'## 完整校准网格','',
'下面每格为 p99 ms / QPS；每格 128 条 query×两次独立重启。结果用于挑选候选，不能单独作为最终性能结论。','',
'| 每节点线程数 | 并发1 | 并发2 | 并发4 | 并发8 | 并发16 | 并发32 |','|---|---|---|---|---|---|---|']
for w in [1,2,4,8,16,32]:
 values=[f"{bykey['calibration',c,w]['p99_ms']:.2f} / {bykey['calibration',c,w]['qps']:.2f}" for c in [1,2,4,8,16,32]]
 lines.append('| '+str(w)+' | '+' | '.join(values)+' |')
lines+=['','## 运行证据与解释边界','']
for stage in ['calibration','validation','confirmation']:
 ps=[p for p in points if p['stage']==stage]
 lines.append(f"- {stage}：{sum(p['trials'] for p in ps)} 次独立运行，{sum(p['requests'] for p in ps)} 个计时请求，失败 {sum(p['failed'] for p in ps)}，节点回访 {sum(p['node_revisits'] for p in ps)}；服务停止记录全部通过：{all(p['services_stopped'] for p in ps)}。")
lines+=['',f"查询失败为零不等于通信错误为零：共 {sum(p['incomplete_metrics_requests'] for p in points)} 个成功查询出现控制通信错误并标记 metrics_complete=false，控制错误事件合计 {sum(p['control_errors'] for p in points)} 次。它们仍计入全部延迟、召回和吞吐统计，原始资源计数保留且标明不完整；平均资源指标仅使用完整样本，不能将其视为无条件总体资源均值。",'', '每个点所有数据节点的物理读取字节计数之和保存在 points_summary.csv。它是进程窗口计数，不是逐 query 物理 I/O。当前工作集可进入 RAM，本实验不能用于估计 SSD 冷态能力或多机网络性能。','',
'在同一并发度下比较线程数，才能讨论线程优化；并发1的绝对p99低于并发16不构成算法优化。若只追求绝对最低p99，减少同时在途查询通常也能减轻争用，但会改变吞吐目标。','',
'## 资源与阶段时间（较长窗口，并发16）','',
'| 每节点线程 | 平均延迟 ms | 累计分支CPU ms/query | 累计RaBitQ墙钟 ms/query | 累计精确距离墙钟 ms/query | 累计pread墙钟 ms/query | 物理读取字节总数 |','|---|---|---|---|---|---|---|']
for w in [1,4]:
 p=bykey['confirmation',16,w]
 lines.append(f"| {w} | {p['mean_ms']:.2f} | {p['mean_server_thread_cpu_ms']:.2f} | {p['mean_rabitq_ms']:.2f} | {p['mean_exact_distance_ms']:.2f} | {p['mean_pread_ms']:.2f} | {p['physical_read_bytes_total']} |")
lines+=['','上表资源均值仅使用 metrics_complete=true 的请求，样本数见 points_summary.csv；平均延迟仍包含全部成功请求。计算阶段墙钟包含线程暂停/调度等待，不能当作纯 CPU 执行时间；各分支累计耗时也不能相加为端到端占比。线程增多伴随累计 CPU 和阶段墙钟上升，与运行时竞争/调度开销增加一致，但本轮没有锁/GIL或网络栈剖析，不能把全部退化归因于单一原因。完整逐节点 CPU 核当量、RSS、OS 线程数、物理读和峰值活跃扩展见 all_nodes.csv。','',
'现有控制面、逐 query 回调服务器、短连接和同机客户端也参与端到端延迟。并发16/32的部分尾部请求接近或超过1秒，不能仅凭延迟形状认定TCP重传或SSD超时。短窗口与较长窗口的p99可能不同，因此两者原始结果均保留；后续需更大独立查询集、持续固定到达率负载和网络/调度追踪。','',
'## 可复现文件','',
'- [实验方案](PLAN.md)、[扫描计划](plan.yaml)、[较长窗口复验计划](confirmation_plan.yaml)、[冻结选择](selection_before_validation.json)。',
'- [配置点汇总](points_summary.csv)、[每次运行汇总](runs_summary.csv)、[配对比较与区间](paired_comparisons.csv)。',
'- [逐查询指标](all_queries.csv)、[逐节点指标](all_nodes.csv)；aggregated/ 含每配置总体及按 query 分组的阶段均值。',
'- calibration/、validation/、confirmation/：完整配置、report、逐 query JSONL/CSV、日志、服务停止记录。recommended/ 保存经过比较的完整实验 YAML，输出到独立目录并复用索引。',
'- [产品及主机信息](provenance.json)、harness_sha256*.json、final_artifacts_sha256.json：复现版本与校验记录。',
'- run_experiment.py 完成初筛与独立验证，confirm.py 执行冻结候选的长窗口复验，analyze.py 汇总，write_summary.py 生成本报告。',
'','首次沙箱内启动因socket权限失败，另一次实验脚本因独占创建模式不能覆盖进度JSON而中断；均在 execution_logs/ 留存。修复仅涉及实验进度记录，已完成的正常查询报告直接复用，没有修改其结果，未计入失败启动为查询失败。','']
(root/'SUMMARY.md').write_text('\n'.join(lines))
(root/'recommendations.json').write_text(json.dumps(recommended,ensure_ascii=False,indent=2))
print(json.dumps(recommended,ensure_ascii=False,indent=2))
