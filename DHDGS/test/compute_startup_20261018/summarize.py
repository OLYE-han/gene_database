"""Evidence-based conclusions, separate from mechanically generated audit tables."""
from pathlib import Path
import json,statistics as st
R=Path(__file__).resolve().parent
rows=json.loads((R/'comparison.json').read_text())
def group(name):return [r for r in rows if r['group']==name]
def avg(name,key):return st.mean(r[key] for r in group(name))
before='pool_locked_w32';after='pool_reserved_w32'
lines=['# DHDGS 等待延迟优化结论','',
'连接池锁外预约与探测是本轮有重复证据的有效优化。计算预启动解决了局部启动排队，但单独不能改善端到端 p99；增加结果发送线程或远程派发线程也没有带来收益。','',
'## 成对对照：只改变连接池探测位置','',
'两个方向相反的顺序：locked→reserved，再 reserved→locked。完整 2M 索引，本机三 TCP 数据进程；每组 2000 个固定查询、预算 200、32 个固定计算线程、16 个派发线程、16 个控制线程、每目标连接上限 8，详细 trace 开启，热页缓存。表中是两次运行的算术均值，不是合并采样后的 p99。','',
'| 指标 | locked | reserved |','|---|---:|---:|']
for key,label in [('p99_ms','p99 / ms'),('mean_ms','平均延迟 / ms'),('qps','QPS'),('recall','Recall@10'),('all_report_ready_to_last_sender_ms','报告齐备至最后发送 / ms 每query'),('result_pool_wait_ms','结果 RPC 借连接 / ms 每次'),('accepted_to_worker_ms','接纳至计算线程 / ms 每分支'),('mean_cpu_cores','三个数据进程总平均 CPU 核当量')]:
    lines.append(f'| {label} | {avg(before,key):.5f} | {avg(after,key):.5f} |')
lines+=['',f"p99 降低 {(1-avg(after,'p99_ms')/avg(before,'p99_ms')):.2%}，平均延迟降低 {(1-avg(after,'mean_ms')/avg(before,'mean_ms')):.2%}，QPS 提高 {(avg(after,'qps')/avg(before,'qps')-1):.2%}。这四次运行共 8000 查询、零失败，召回约 97%。",'',
'## 为什么改善','',
'原连接池持有全池锁执行 select 和 close。系统调用释放 GIL 后，线程重新获得 GIL/CPU 的延迟会延长持锁时间，阻挡其他目标的借还。新实现只在锁内预约名额和修改容器，socket 探测及关闭在锁外；预约保持连接容量与归属约束。故障注入验证其他 endpoint 不再被阻塞，逆序负载对照验证端到端收益。未采集解释器逐线程 GIL 调度轨迹，不能把每毫秒 pool_wait 都称为 GIL 等待。','',
'计算 fixed/prestarted 模式在服务 readiness 前启动工作线程，提交只负责排队；fixed 保持一个原生分支固定在一个计算线程。它消除了大部分按需创建线程导致的接纳堵塞，但初期 8 组扫描 p99 仍约 4.25–4.61 秒，因此没有把局部排队下降误报为整体优化。','',
'## 其余消融与配置选择','',
'本轮推荐配置见 [control_delay/experiment.yaml](control_delay/experiment.yaml)：fixed32 + reserved + transfer16 + control16，初次心跳延迟为 1000 ms。这是本机突发负载的复现实验配置，不是三台物理机器的最优参数结论。','',
'| 配置 | 次数 | p99 均值 ms | QPS 均值 | 心跳 / 2000查询均值 |','|---|---:|---:|---:|---:|']
for name in ['control_base','control_delay','control_workers32','control_both','transfer32','transfer64']:
    lines.append(f"| {name} | {len(group(name))} | {avg(name,'p99_ms'):.2f} | {avg(name,'qps'):.2f} | {avg(name,'heartbeat_count'):.1f} |")
lines+=['',
'control_delay：仅将无状态变化的首次心跳延后 1000 ms；新子分支、结果和错误仍立即调度。control_workers32：仅将控制发送线程从 16 改为 32。control_both：两者同时修改。transfer32/64：只增加远程派发线程。未证明更多线程更好；初始心跳延后两轮的p99分别3669.82/3673.39 ms，比对应base的3684.29/3888.46 ms低，但收益幅度波动；按p99优先将其纳入本轮推荐实验配置，通用默认仍为0。','',
'通用 configs/dhdgs.yaml 将连接池默认改为 reserved；compute 默认仍为 executor（通用默认8线程），fixed32及首心跳延后只在这份已测高并发配置中推荐。旧 YAML 缺少 compute/pool/control 组时显式迁移到旧模式，旧实验不会悄悄改变语义。','',
'## 同一个查询的完整时间线','',
'沿用先前固定的查询 ID 734744，不按本次改善幅度挑选。分支、派发轨道可能重叠；上图轨道不能相加，下图互斥分组之和等于 API 延迟。橙色搜索后收尾表示分支等待异步派发结算，不表示计算线程仍被占用；派发线程已提前独立工作。发送端收到 ACK 可以晚于客户端返回。所有查询原始指标均保留，单个示例不代表总体。','']
for name in [before,after,'control_delay']:
    run=group(name)[0]['run'];d=R/'analysis'/name/run
    q=json.loads((d/'selected_query.json').read_text())
    lines +=[f"### {name}，延迟 {q['client_wall_ms']:.2f} ms，Recall@10 {q['recall']:.0%}",'',f'![查询时间分解](analysis/{name}/{run}/query_timeline.png)','']
lines+=['## 完整性与局限','',
f"本轮共 {len(rows)} 次全量索引运行、{sum(r['success'] for r in rows)} 个计时查询，失败 {sum(r['failed'] for r in rows)}；每组查询集合及真值匹配，预算守恒、扩展函数批准、节点不重复进入、固定计算线程、队列无拒绝、所有实验进程已结束均由 analyze.py 审计。回归结果见 regression_final.log。",'',
'这些是短突发及排空实验，不是稳态饱和测试；客户端20进程×100线程同时发起查询也会贡献调度开销。本机回环网络、buffered pread、热OS缓存使进程物理读取增量为0，不能作为冷SSD或物理三机性能结果。详细trace开销未独立消融。剩余入口接纳、远程派发排队和结果发送仍存在；本轮没有实现跨查询原生continuation或线程任务槽，也未声称消除全部等待。','',
'## 复现与文件组织','',
'- [REPORT.md](REPORT.md)：全部运行指标、关键路径、启动等待及结果RPC分解。',
'- comparison.json / comparison.csv：机器可读汇总；各配置 runs/ 内保存逐query CSV、各阶段均值、进程负载、解析后 YAML、真值及原始事件。',
'- analysis/：逐分支启动CSV、逐query关键路径CSV、查询734744原始事件、时间线CSV与PNG/SVG。',
'- sources.json / pool_sources.json / control_sources.json / transfer_sources.json：各阶段冻结哈希；相应 *_source_snapshot 保存当时源码。run_suite.py 在每组实验前校验对应阶段清单，最终版本与旧清单不同是预期，不应覆盖旧清单。',
'- final_sources.json：最终生产源码/默认YAML/测试及实验脚本哈希；provenance_verification.json 核验冻结快照与各轮 resolved 参数。',
'- 无重建索引、无删除用户数据；本轮仅本机测试，未部署到 intel-30/32。','',
'复现当前推荐组（同一索引与数据路径可用时）：','',
'```sh','gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/compute_startup_20261018/run_suite.py --manifest transfer_sources.json --groups control_delay','```','',
'若生产源码后续改变，清单校验会拒绝复用旧实验；应建立新配置目录和新的源码快照，不要修改旧实验记录。']
(R/'SUMMARY.md').write_text('\n'.join(lines)+'\n')
print(R/'SUMMARY.md')
