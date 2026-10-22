from pathlib import Path
import json,statistics,subprocess,yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
final=json.loads((R/'final_comparison.json').read_text());initial=json.loads((R/'comparison.json').read_text());screen=json.loads((R/'merge_screen_progress.json').read_text());g=final['groups'];old=g['sync'];new=g['optimized']
qgain=(new['qps_median']/old['qps_median']-1)*100;pgain=(new['p99_median']/old['p99_median']-1)*100
lines=['# 固定64搜索线程：控制注册与顶层报告优化','','本次不构成硬件吞吐上限证明。固定搜索线程，先隔离接纳路径的控制状态锁等待，再减少顶层报告深拷贝；结果按同一阶段的交替对照报告，不用不同时段的最好/最坏值拼接收益。','','## 最终三轮对照','','三台机器 intel-35、intel-32、intel-30，每节点64搜索线程；intel-35同时运行独立顶层进程。并发256，搜索预算200，top10，seq_2M全量索引，固定2000条验证向量重复到每轮20,000查询，warmup200。网络与线程布局固定：数据RX8×handler1、发送各4；顶层route16、merge4、RX4×handler4、发送各4；每逻辑发送器每目标总连接额度128。','','|实现|QPS中位数|QPS范围|每轮p99中位数 ms|Recall@10范围|','|---|---:|---:|---:|---:|']
for key,label in [('sync','原同步注册、报告复制'),('optimized','注册入队、报告所有权转交')]:
 v=g[key];lines.append(f"|{label}|{v['qps_median']:.2f}|{v['qps_range'][0]:.2f}–{v['qps_range'][1]:.2f}|{v['p99_median']:.3f}|{v['recall_range'][0]:.5f}–{v['recall_range'][1]:.5f}|")
lines += ['',f'同阶段中位数变化：QPS {qgain:+.2f}%，p99 {pgain:+.2f}%。全部测量请求成功，预算、禁止节点重访、禁止顶层图扩展以及指标完整性审计均通过；内存结果捕获未发生溢出。','','![最终对照](final_comparison.png)','','|轮次|原实现QPS|优化QPS|配对变化|原p99 ms|优化p99 ms|','|---|---:|---:|---:|---:|---:|']
for n in range(3):
 a=next(r for r in final['runs'] if r['repeat']==n and r['mode']=='sync');b=next(r for r in final['runs'] if r['repeat']==n and r['mode']=='optimized')
 lines.append(f"|{n}|{a['qps']:.2f}|{b['qps']:.2f}|{(b['qps']/a['qps']-1)*100:+.2f}%|{a['p99']:.3f}|{b['p99']:.3f}|")
lines += ['','## 等待是否转移','','以下为每轮平均值的三轮中位数。节点阶段以查询分支为单位，顶层阶段以完整查询为单位；不同主机时间不相减，重叠阶段不可累加成端到端延迟。','','|阶段|原实现 ms|优化 ms|','|---|---:|---:|']
for key,label in [('receive','完整报文收到→接纳前（包含解码/校验）'),('admission','接纳及注册提交'),('runtime_lock','其中runtime状态锁等待'),('compute_ready','计算队列及启动设置'),('search','搜索循环'),('completion_queue','完成命令排队'),('report_send_queue','报告发送队列'),('report_ack','报告响应/ACK')]:
 lines.append(f"|{label}|{old['branch_phase_mean_medians'][key]:.3f}|{new['branch_phase_mean_medians'][key]:.3f}|")
for key,label in [('route_queue','顶层路由队列'),('merge_queue','顶层合并队列'),('merge','顶层合并执行及生命周期')]:
 lines.append(f"|{label}|{old['head_phase_mean_medians'][key]:.3f}|{new['head_phase_mean_medians'][key]:.3f}|")
lines += ['',f"优化后的控制注册命令平均排队（三轮中位数）为 {new['branch_phase_mean_medians']['register_command_queue']:.3f}ms；该等待与搜索可以重叠，不阻止接纳线程继续处理下一请求。完成命令的等待另行统计。",'','|节点|原实现平均占用搜索线程|优化平均占用搜索线程|优化占64线程比例|','|---|---:|---:|---:|']
for sid,host in [('0','intel-35'),('1','intel-32'),('2','intel-30')]:
 a=statistics.median(r['timings']['occupancy'][sid]['mean_busy_search_workers'] for r in final['runs'] if r['mode']=='sync');b=statistics.median(r['timings']['occupancy'][sid]['mean_busy_search_workers'] for r in final['runs'] if r['mode']=='optimized')
 lines.append(f'|{host}|{a:.2f}|{b:.2f}|{b/64*100:.1f}%|')
lines += ['','占用量由所有测量分支的worker_enter→compute_released区间总和除以本机测量窗口得到，含线程被抢占和同步pread等待，**不是CPU利用率**。平均未占满64线程不能独立证明搜索还有多少可用算力，但排队位于接纳/控制/合并路径时，也不能断言搜索计算已达峰值。','','## 分阶段证据与未采用的配置','','第一阶段只切换queued_control_registration，三轮QPS中位数由 %.2f 到 %.2f，约提高 %.2f%%；接纳前等待从 %.3f 到 %.3f ms，但顶层合并队列从 %.3f 到 %.3f ms。故单独解除入口锁等待不足以显著改善整体吞吐。'% (initial['groups']['sync']['qps_median'],initial['groups']['queued']['qps_median'],(initial['groups']['queued']['qps_median']/initial['groups']['sync']['qps_median']-1)*100,initial['groups']['sync']['branch_phase_mean_medians']['receive'],initial['groups']['queued']['branch_phase_mean_medians']['receive'],initial['groups']['sync']['head_phase_mean_medians']['merge_queue'],initial['groups']['queued']['head_phase_mean_medians']['merge_queue']),'','只增加顶层合并线程的筛选：','','|合并线程|QPS|p99 ms|','|---|---:|---:|']
for r in screen:lines.append(f"|{r['mode']}|{r['qps']:.2f}|{r['p99']:.3f}|")
lines += ['','这两组各只有一轮，且共享机器存在其他负载，不能把退化幅度全部归因于线程数；但结果不足以支持增加到8/16线程，因此最终验证保留4个合并线程。','','## 实现与安全边界','','- `native_runtime.queued_control_registration`：短身份预留锁同步去重；注册入控制FIFO后返回Ticket，start/child/complete保持先后顺序。接纳路径不再嵌套等待controller状态锁。','- `native_runtime.move_head_reports`：顶层接收和最终合并时转移Value所有权，减少两次完整报告深拷贝。保留消息摘要、报告内容、时间戳、预算/路径校验和重试规则。','- 不改变搜索图、RaBitQ、搜索预算与远端扩展决策，也未通过减少指标记录提速。','- 57项控制、原生端到端与网络测试通过；包含并发重复身份、到期后重新注册、关闭、取消、晚到子分支失败及父报告提前返回。','','## p99时序','','代表轮次选择该实现QPS最接近三轮中位数的轮次；在该轮跨节点查询中选服务延迟最接近全体查询p99的真实记录。每个主机独立时间原点，计算线程释放后的控制等待不占用该计算线程。','','![优化p99](p99/optimized/timeline.png)','','原实现时序见[p99/sync/REPORT.md](p99/sync/REPORT.md)，优化时序见[p99/optimized/REPORT.md](p99/optimized/REPORT.md)。','','## 限制、后续方向与复现','','这些是并发256、固定查询集重复的短时热缓存测试。运行期间共享主机存在其他进程负载，原实现跨时段QPS也有明显变化；仅三轮不能宣称统计显著或已找到全局最优，更不能外推至冷SSD或百亿向量。后续优先针对剩余控制命令积压进行有时间预算的批处理/按查询分片，再测连接分配与接收通道负载；不应绕过远端扩展决定函数预发查询。','','14轮有效测试合计280,000查询。三个启动兼容性失败发生在测量前，保留在startup_failed_*中，不计入性能结果。第一阶段同机同二进制切换注册开关；最终阶段同机同二进制切换两个开关。数据二进制保持第一阶段版本，顶层第二阶段版本的源码/二进制哈希分别见deployed_provenance.json、head_v2_hashes.json及各case/head_source.json。','','完整配置见各case/plan.yaml、head_effective.yaml和layout.yaml；所有原指标汇总保存在case/result.json，逐查询与节点时间戳见case/runs。共享NVMe剩余容量较小，后续cases/runs通过符号链接归档在本机 `/var/tmp/dhdgs_control_registration_20261022` 系统盘，**移动实验时需要一并复制链接目标**。未压缩的冗余捕获分片仍在 `/tmp/dhdgs_control_registration_20261022/cases`，分析所需记录已有压缩副本与SHA256清单。','','复现程序：run_comparison.py（注册隔离）、run_merge_screen.py（线程筛选）、run_final.py（最终对照）；既有case不会被覆盖。离线分析：`.venv/bin/python test/control_registration_20261022/analyze.py --progress final_progress.json --output final_comparison`。','']
(R/'REPORT.md').write_text('\n'.join(lines))
for mode in ['sync','optimized']:
 row=min([r for r in final['runs'] if r['mode']==mode],key=lambda r:abs(r['qps']-g[mode]['qps_median']))
 subprocess.run([str(P/'.venv/bin/python'),str(R/'plot_p99.py'),'--case',str(R/'cases'/row['name']),'--output',str(R/'p99'/mode),'--cross-node'],stdout=(R/(mode+'_p99.log')).open('w'),check=True)
 if mode=='optimized':
  import shutil
  for filename in ['plan.yaml','head_effective.yaml','layout.yaml']:shutil.copy2(R/'cases'/row['name']/filename,R/('recommended_'+filename))
print(json.dumps(dict(qps_change_percent=qgain,p99_change_percent=pgain,groups=g),indent=2))
