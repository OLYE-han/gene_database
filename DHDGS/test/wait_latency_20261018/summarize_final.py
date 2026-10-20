from pathlib import Path
import argparse,csv,json,yaml
import numpy as np
R=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('group');p.add_argument('--run');a=p.parse_args()
runs=sorted((R/a.group/'runs').glob('*/summary.json'));run=a.run or runs[-1].parent.name
out=R/'analysis'/a.group/run
summary=json.loads((R/a.group/'runs'/run/'summary.json').read_text())
w=json.loads((out/'wait_summary.json').read_text());q=json.loads((out/'selected_query.json').read_text());events=json.loads((out/'selected_events.json').read_text())
allrows=[]
for f in sorted(R.glob('*/runs/*/summary.json')):
 s=json.loads(f.read_text());allrows.append({'group':f.parents[2].name,'run':f.parent.name,**s})
def avg(group,key):
 rows=[s for s in allrows if s['group']==group]
 return np.mean([s['latency_ms'][key] if key in ('mean','p99') else s[key] for s in rows])
def table(headers,rows):
 return ['| '+' | '.join(headers)+' |','|'+'|'.join(['---']*len(headers))+'|']+['| '+' | '.join(map(str,r))+' |' for r in rows]
rel=str(out.relative_to(R));base=next(s for s in allrows if s['group']=='baseline')
lines=['# 等待时延优化：结果与未达成目标','',
'**同一 2000 查询突发负载下，整体等待仍未低于 100 ms。** 计算线程不等待远端分支结束；接纳、完成事件消费及结果发送仍有显著排队。以下保留负结果，不用降低并发、降低搜索预算或丢弃请求来换取达标。','',
'## 固定实验条件','',
'Intel-35 本机三个真实 TCP 数据进程，完整 seq_2M 不可变索引；20 个客户端进程，每进程 100 个工作线程同时释放，共 2000 条不同验证查询。各组真实峰值在途见 all_run_summaries.json，未把并发上限等同于每一时刻都恰有2000在途。全局预算200，top-k=10，召回门槛为平均Recall@10≥0.95，40次预热不计入。每原生分支固定一个计算线程，父分支继续本地搜索，子分支不回访已有节点。','',
'buffered sync_pread、应用缓存0、热OS页缓存；本轮没有清空页缓存、重建索引或部署物理三机。磁盘/网络墙钟不是纯SSD/链路服务时间。所有可调参数均来自各组experiment.yaml，源码和native模块按阶段冻结并核对哈希。','',
'## 性能结果','']
groups=['baseline','final_fulltrace','dispatch1','fastoff','faston']
if a.group not in groups:groups.append(a.group)
lines+=table(['组','轮数','平均延迟ms（各轮均值）','p99 ms（各轮均值）','QPS（各轮均值）','Recall@10范围'],[
 [g,len([s for s in allrows if s['group']==g]),f'{avg(g,"mean"):.2f}',f'{avg(g,"p99"):.2f}',f'{avg(g,"qps"):.2f}',
  f'{min(s["recall_at_k_all_requests"] for s in allrows if s["group"]==g):.3%}–{max(s["recall_at_k_all_requests"] for s in allrows if s["group"]==g):.3%}'] for g in groups])
lines+=['',
'baseline是本轮开始时实现；final_fulltrace是后期同代码的scan/GIL5ms/全详细追踪配置。跨阶段总体改善是组合效果，不归因于某一个补丁。相同代码的单变量与逆序对照见下方全表及各组冻结记录；短突发窗口、两次复测不等于稳态容量或全局最优。','',
f'最终展示配置：[experiment.yaml]({a.group}/experiment.yaml)，选用该组最后一轮 `{run}`，没有挑选该组中最快查询。推荐 fixed/32 计算线程、buffered/shared/1 接纳消费者、event/cap0 控制调度、2连接/目标、GIL切换0.1ms、全体生命周期与单query详细追踪；这些是本负载推荐值，通用默认仍保留GIL5ms。','',
'## 等待是否达标','',
f'本轮全体生命周期跟踪覆盖 {w["traced_queries"]} 条查询。下表为本次展示轮次，除入口池借用按query统计外，其余按分支/远程动作统计；**不同项不能直接相加**。','']
labels=[('entry_pool_ms','入口连接池借用'),('admission_queue_ms','接纳队列'),('compute_queue_ms','固定计算队列'),('gil_reacquire_ms','循环结束后重新取得GIL'),('native_loop_to_worker_free_ms','循环结束到计算线程可复用'),('local_done_dequeue_ms','完成事件等待消费'),('consumed_to_finalization_ms','完成消费到最终结算尝试'),('report_ready_to_sender_ms','报告就绪到发送开始'),('transfer_queue_ms','远端派发队列')]
lines+=table(['阶段','样本数','平均ms','p99 ms','最大ms'],[
 [label,v['count'],f'{v["mean"]:.3f}',f'{v["p99"]:.3f}',f'{v["max"]:.3f}'] for key,label in labels if (v:=w['branch_stage_ms'].get(key))])
e=w['query_exposed_queue_union_ms']
lines+=['',f'把已观测队列区间取并集、再排除与同查询原生搜索/入口路由重叠的部分，exposed_queue_union_ms 平均 **{e["mean"]:.2f} ms**、p99 **{e["p99"]:.2f} ms**、最大 **{e["max"]:.2f} ms**；{w["exposed_queue_over_100ms"]}/{w["traced_queries"]} 条达到或超过100ms。该指标避免并行分支重复相加，但不是完整off-CPU profile，仍可能包含少量操作及未细分的调度；不能用它宣称精确纯等待占比。接纳等单项已经超过100ms，足以确认当前目标未达到。',
'', '## 同一个查询的完整时间线','',
f'沿用先前按固定随机种子抽取的查询 **734744**，本轮API延迟 **{q["client_wall_ms"]:.2f} ms**，Recall@10 **{q["recall"]:.0%}**，展开 {q["stats"]["expanded"]} 个节点。没有根据优化后快慢重新挑选查询。',
'',f'![查询完整时间线]({rel}/query_timeline.png)','',
'黑色菱形表示计算线程已可接下一任务；报告控制和远程派发是独立轨道。上图轨道可能重叠，不能相加；下图只是互斥端到端边界分组，包含计算和等待，不是纯等待分解。','']
t0=q['client_start_ns']
lines+=table(['节点','原生循环结束/ms','线程可复用/ms','报告就绪/ms'],[[t['node_id'],f'{(t["native_loop_done_ns"]-t0)/1e6:.3f}',f'{(t["worker_available_ns"]-t0)/1e6:.3f}',f'{(t["report_ready_ns"]-t0)/1e6:.3f}'] for t in events['startup']])
lines+=['',f'[全部事件CSV]({rel}/query_timeline.csv) · [逐query等待并集CSV]({rel}/query_wait_union.csv) · [原生搜索放大图]({rel}/query_search_detail.png) · [原始选定查询]({rel}/selected_query.json)','',
'## 已实现且保留的机制','',
'- 事件驱动控制调度、到期堆及结果优先队列，避免反复扫描全部保留记录。调度器不批准远端搜索。',
'- 本地完成报告异步交接，计算线程不等待派发Future或其它分支；计时从C++循环终止开始，补上Python返回前的GIL等待。',
'- 接纳容量使用O(1)计数，缓存执行配置签名；native有membership sidecar时省去未使用的Python owner字典。',
'- 全体低体积生命周期记录与指定查询逐批次详细记录分离，仍保留每query召回、延迟、网络、读取与计算指标。',
'- 保留控制消费批量、接纳队列锁、接纳消费者数量、GIL切换间隔等YAML消融参数。配置生命周期内不可热改执行签名，同一Python进程必须使用一致的进程级设置。',
'- 补充GC诊断但未关闭GC；修复独立队列接纳/关闭竞态以及legacy过期记录重复释放容量。','',
'可选无派发完成发布快路径已实现且默认关闭。同代码 off/on/on/off 对照：off 的 p99 为1689.73/1673.59ms，on 为1808.32/1706.37ms；未见收益。on 两轮成功 try-lock 分别382/2698和58/2699分支，有Future回退698/699，锁忙回退1618/1942。第一轮on的线程释放p99也升至43.27ms；非阻塞获取锁不代表后续Python处理没有调度代价。保留此负结果，推荐关闭。','',
'## 否定结果与剩余瓶颈','',
'单纯增加计算线程32→128、连接数2→32、接纳消费者1→4/16均未改善本轮p99；shared→independent队列锁也没有消除等待。cap64/8减少了部分报告ready后的排队，却增加了其它队列等待，未作为默认最优值。direct接纳搭配fixed池也未优于buffered。不能把一个队列变短当作系统整体提速。','',
'剩余问题集中在接纳、完成事件消费及控制发送的串行Python处理和调度竞争。状态锁持有/获取已细分，不能将几百毫秒队列等待全部说成持锁时间；GIL切换和全详细追踪消融证明解释器/对象处理开销重要，但没有测出每个剩余毫秒唯一对应哪个内核调度原因。当前线程多在Condition/Event/socket等待，不能将这些时间统称为CPU忙轮询。','',
'要进一步冲击2000突发下的100ms等待，需要继续验证原生接纳/完成事件与控制发送路径，把逐query状态转换及消息编码从Python串行通道移出，同时保持一次访问、全局预算和单分支固定计算线程；还需测量有效服务容量及排队曲线。该方向尚未在本轮实现或证明达标，不应把它列入当前性能收益。','',
'特别是本轮最终实现中，接纳临界区持锁平均0.063ms、p99 0.121ms，接纳队列却平均447.14ms；最终报告发布平均0.022ms、p99 0.087ms，完成消费/发送仍排队百毫秒。不能把整段排队都解释为“某个锁被占用”，也不能仅靠删除状态保护或增加线程保证改善。剩余串行通道需要提升服务速率，队列只缓存积压，不能消除2000请求突发造成的排队。','',
'## 资源与GC诊断','',
'本机允许256个逻辑CPU（128物理核），本次读取的各级cgroup cpu.max无限额，未观察到配额节流，记录见 hardware_quota.txt；这不表示机器没有其他用户负载。最终轮节点0/1/2按各自before/after采样窗口，CPU消耗约5.43/4.03/2.65核当量；不是256核满载。三个节点物理storage_read_bytes增量均为0，逻辑pread仍执行，数据由OS页缓存满足。','',
'相同节点GC增量墙钟总计133.32/161.53/93.51ms，记录中最大单次15.38/19.12/19.83ms。GC存在开销，但不能独自解释几百毫秒排队；未采集去除冗余owner目录前的同口径GC对照，故不宣称这项修改单独消除了某一次旧暂停。','',
'## 全部实验','']
lines+=table(['组','轮次','成功/失败','真实峰值在途','平均ms','p99 ms','QPS','Recall@10'],[
 [s['group'],s['run'],f'{s["successful_queries"]}/{s["failed_queries"]}',s['actual_peak_inflight'],f'{s["latency_ms"]["mean"]:.2f}',f'{s["latency_ms"]["p99"]:.2f}',f'{s["qps"]:.2f}',f'{s["recall_at_k_all_requests"]:.3%}'] for s in allrows])
lines+=['','## 复现与审计','',
'最终完整回归 **199项通过**，见 [regression_final.log](regression_final.log)；**34轮、68000个计时请求全部成功**，全量正确性审计见 [all_runs_verification.json](all_runs_verification.json)。逐查询核验真值交集、预算守恒、每节点一次访问、扩展函数批准、固定计算线程、队列无拒绝/无遗留，以及测试服务和客户端均退出。',
'每组 runs/ 保存 resolved.yaml、queries.csv/JSONL、阶段耗时表、nodes_before/after、资源采样和服务日志；阶段 *_sources.json 与 *_snapshot/ 记录实际源码。报告选定配置的最后一轮展示详细图，其余轮次仍完整保留。',
'测试不重新分发向量或建图，不覆盖既有实验；所有新增记录位于本目录。通用YAML默认与针对2000突发的实验推荐配置分别记录，不能把单机的线程/GIL配置无验证地推广至物理三机。','']
(R/'SUMMARY.md').write_text('\n'.join(lines))
(R/'all_run_summaries.json').write_text(json.dumps(allrows,indent=2))
print(R/'SUMMARY.md')
