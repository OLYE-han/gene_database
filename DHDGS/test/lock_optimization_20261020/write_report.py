"""Render the report from completed measurements, never from planned runs."""
import json
from pathlib import Path
import statistics

ROOT=Path(__file__).resolve().parent
rows=json.loads((ROOT/'full_metrics.json').read_text())
locks=json.loads((ROOT/'lock_metrics.json').read_text())
ipc=json.loads((ROOT/'ipc_gc.json').read_text())
assert len(rows)==8 and all(r['failed']==0 and r['audit'] for r in rows)
old=next(r for r in rows if r['case']=='sustained/00_baseline')
new=next(r for r in rows if r['case']=='sustained/01_split')
selected=max(ipc,key=lambda r:sum(e['overlap_ms'] for e in r['slowest_query']['ipc_gc_overlaps']))
sample=selected['slowest_query']
q=json.loads((ROOT/selected['case']/'slowest_query.json').read_text())
timeline=json.loads((ROOT/selected['case']/'slowest_head_timeline.json').read_text())
out=[]
def add(s=''):out.append(s)

add('''# 锁拆分实验与剩余等待定位

本轮按 LOCKS.md 审计实现了接纳锁拆分、原生注册可选释放 GIL，并细分顶层结果 IPC。临界区确实缩短，但短测没有证明稳定的端到端收益，因此两个优化开关仍默认关闭。不能宣称等待低于100ms已经达成。

## 条件和完成情况

- 完整seq_2M索引，Intel-35顶层及数据0、Intel-32数据1、Intel-30数据2。
- 每节点32个搜索线程，预算200，top-k=10，在途并发256，4个顶层lane进程，预热32次。
- 6轮各10000请求的交错短测，随后基线/完整拆分各100000请求，共260000正式请求。固定2000条验证查询循环，不是260000个独立查询向量。
- 所有正式请求成功、Recall@10约97%，预算、节点不重访、无路由器参与中间图扩展及指标完整性审计均通过。
- 同一冻结源码、每机器相同的编译产物，仅改变两个locking开关；没有增搜索线程、改搜索预算或关闭GC、测试指标。
- 实验进程独立于220xx常驻集群；计时、回归及大规模分析不重叠。原始查询、节点快照、配置及源码清单均保留。
- 三机热缓存。物理读取和逻辑文件读取分开统计；本结果不代表冷SSD性能。机器未独占，仅两轮短测/方案和一对长测，不能将差异当作稳定容量保证。

首次00_baseline只有启动过程，Intel-30模块因GLIBCXX运行库版本不匹配未能加载，未计入性能结果。改用系统/usr/bin/g++ 12.3重新构建并验证import后，从10号重启同源实验。保留startup_failed.json及编译修正记录。

## 实际代码改动

`peer.locking.split_native_registration`将旧D1大临界区拆为：短锁预留查询身份和active/pending额度；锁外注册C++控制分支；短锁检查取消/关闭并提交接纳队列。清理过期记录时将最后引用的析构移到D1外。C1/C2仍保护C++共享状态。

相同重试在D1外等待同一注册Future，成功注册前不会提前ACK；冲突重试拒绝。注册失败、已有原生身份的失败、取消、过期与关闭均有明确回退/报告路径。完整406项回归通过，包括7项新增并发和故障注入测试。

`peer.locking.native_register_release_gil`在Python参数转换完毕后，允许C++注册等待mutex/DNS时释放GIL，返回和异常传播前恢复。它不代表所有Ticket调用均释放GIL。

H8 Pipe写锁继续保证消息帧完整性；新增打包、写锁等待、父进程接收及解码时间戳。benchmark前后另存父进程GC快照。没有通过删除同步保护来制造无锁路径。

## 同源短测

| 方案 | 轮次QPS | QPS中位数 | p99中位数 ms |
|---|---|---:|---:|''')
for name,label in [('baseline','基线'),('gil','仅原生注册释放GIL'),('split','完整拆分+释放GIL')]:
    group=[r for r in rows if r['case'].startswith('short/') and r['variant']==name]
    values=' / '.join(format(r['qps'],'.2f') for r in group)
    add(f"| {label} | {values} | {statistics.median(r['qps'] for r in group):.2f} | {statistics.median(r['p99_ms'] for r in group):.2f} |")
add('''
![短测对照](short_comparison.png)

仅释放GIL的收益约1%，低于本轮能据以默认采用的可信幅度。完整拆分的两轮波动较大，没有形成整体优势。不能以持锁时间下降代替QPS/p99改善。

## 持续负载对照

| 指标 | 基线 | 完整拆分 |
|---|---:|---:|''')
for label,key,fmt in [('QPS','qps','.2f'),('平均端到端 ms','mean_ms','.2f'),('p99 ms','p99_ms','.2f'),('Recall@10','recall','.5%'),('失败数','failed','.0f'),('累计就绪等待 ms/query','query_ready_queue_ms','.2f'),('累计搜索线程CPU ms/query','server_thread_cpu_ms','.2f'),('累计pread ms/query','pread_ms','.3f'),('累计精确距离 ms/query','exact_distance_ms','.3f'),('累计RaBitQ ms/query','rabitq_ms','.3f'),('扩展顶点数/query','expanded','.2f'),('最大扩展深度均值','max_expanded_depth','.3f'),('最大跨节点深度均值','max_cross_shard_depth','.3f'),('跨数据节点分支RPC/query','peer_branch_rpc_count','.4f'),('全部应用协议RPC/query','all_protocol_rpc_count','.4f'),('应用协议字节/query','all_protocol_bytes','.2f'),('逻辑文件读取bytes/query','file_read_bytes','.0f'),('三节点物理读取bytes','physical_read_bytes','.0f'),('顶层所有业务进程平均CPU核数','head_cpu_cores','.2f'),('Intel-35数据平均CPU核数','node0_cpu_cores','.2f'),('Intel-32数据平均CPU核数','node1_cpu_cores','.2f'),('Intel-30数据平均CPU核数','node2_cpu_cores','.2f')]:
    add(f'| {label} | {old[key]:{fmt}} | {new[key]:{fmt}} |')
add(f"\n本次一对长测QPS变化{(new['qps']/old['qps']-1)*100:+.2f}%，p99变化{(new['p99_ms']/old['p99_ms']-1)*100:+.2f}%。须与短测一起解释，不能据单次长测宣称稳定改善。CPU核数由进程计数窗口计算，窗口包含快照开销；它不是物理CPU占用百分比。")
add('''
## 等锁缩短后，为什么仍会等

短测基线的接纳D1平均持锁区间约0.2–0.3ms；拆分后预留和提交两段各约0.02ms。注册C++ state mutex平均等待仅约0.03–0.06ms，无法单独解释数百毫秒排队。完整拆分增加了再次取得D1的步骤及GIL切换机会，局部解耦并不保证消费者更快获得调度。

以Intel-32为例，10号基线接纳队列均值94.07ms，12号拆分247.64ms，13号拆分67.15ms；对应原生搜索约9.52/10.59/9.45ms。executor排队通常不足1ms。队列等待包含消费者唤醒、重新取得锁、GIL和OS调度，当前时间戳不能给出它们各自的独占比例。不能把这个区间全部叫作搜索计算时间，也不能全部叫作mutex争用。

顶层H8写锁等待的短测p99约0.002ms，输出IPC总体p99约140–162ms。已测证据不支持删除这把写锁或继续增加写者。`send_receive`包含父进程读取/调度及背压，不是纯管道传输耗时。

持续负载的保留trace中，Intel-32接纳队列均值从257.01ms降至134.96ms，但Intel-35从36.58ms升至161.12ms；等待发生转移，不能用单节点改善代表整体收益。两组trace保留样本不同，完整所有请求的累计就绪等待见前表。

**长等锁区间中的GC已有节点侧直接证据。** Intel-32基线一次申请D1至取得D1共827.80ms，其中752.59ms与同进程GC重叠；拆分版本相应最长区间578.56ms，GC重叠561.62ms。这不代表mutex本身独占了全部时间，也不能用同进程增加线程来绕过GC暂停。

![数据节点状态锁区间与GC重叠](lock_gc_timeline.png)

### 一个能直接验证GC影响的查询
''')
add(f"选择 **{selected['case']} / query {q['query_id']}**，完整时延{sample['client_ms']:.3f}ms，输出IPC {sample['output_ipc_ms']:.3f}ms。它是针对GC重叠选择的诊断样本，不是随机或典型查询。")
add('\n| 同机时钟下的互斥阶段 | ms |\n|---|---:|')
for r in timeline:add(f"| {r['stage']} | {r['duration_ms']:.3f} |")
for e in sample['ipc_gc_overlaps']:
    if e['overlap_ms']>1:add(f"\n父进程generation-{e['generation']} GC：墙钟{e['wall_ms']:.3f}ms、执行线程CPU {e['cpu_ms']:.3f}ms，与输出IPC重叠{e['overlap_ms']:.3f}ms。")
add('''
![诊断查询时序与输出IPC细分](query_timeline.png)

红色阴影为实际记录的父进程GC暂停。CPython GC会阻止同进程其他Python线程推进；已经释放GIL的C++搜索可以继续。因此结果已准备好，也可能迟迟不能由父进程接收、解码和发布。这不是搜索工作线程一直占着锁等结果。

### 长测GC与观测限制
''')
for r in ipc:
    if r['case'].startswith('sustained/'):
        add(f"- {r['case']}：父进程GC累计墙钟{r['parent_gc_wall_ms_delta']:.2f}ms，GC执行线程CPU {r['parent_gc_cpu_ms_delta']:.2f}ms。")
add('''
压测父进程在内存中保留全部100000条完整结果，属于本次压测路径的对象生命周期成本；不能把这部分开销直接外推为生产服务每次查询的固有代价。数据节点另有终态记录保留和GC，详见lock_metrics.json。

GC环只保留最近1024个事件，长测早期暂停会被覆盖；累计计数仍有效。未匹配到某条慢查询的GC事件不表示它没有受到GC影响。本轮不将每条秒级慢查询都断言为GC造成，也未精确分离所有GIL/OS/锁等待。

节点详细trace同样受终态保留窗口影响，长测节点阶段值只代表仍保留的分支，样本数在lock_metrics.csv。所有查询的顶层时间线和聚合指标均完整。不同主机monotonic时间戳不相减；多个分支累计CPU/I/O/RPC时间可能重叠，不能除以端到端时延直接当作互斥占比。

## 采用决定与后续设计

两个locking开关继续默认false；保留经过验证的可选实现及观测。当前未达到系统等待低于100ms的目标。下一步应优先减少长期保留的Python对象图、将完整实验结果流式归档，并保留紧凑的防重放/重试身份；不得缩短防重放期限或丢弃测试指标来换取速度。这项生命周期改造尚未实现，本轮没有禁用GC。

GC日志应增加独立的长暂停事件保留，避免大量短GC挤掉关键证据；队列还需要单独记录通知、消费者唤醒及重新取锁边界。动态扩容控制必须排除进程暂停窗口，不能在暂停期间不断加线程。H8当前不是主要等待点；C++注册锁也没有秒级平均等待，暂不做更复杂的无锁替代。

## 文件与验证

- [实验参数](experiment.yaml)、[计划](PLAN.md)、各轮plan.yaml/spec.json/result.json。
- [完整指标CSV](full_metrics.csv)、[顶层各阶段均值](head_stage_means.csv)、[锁/队列指标](lock_metrics.csv)、[IPC/GC诊断](ipc_gc.json)、[10秒吞吐窗口](throughput_windows.csv)。
- [406项回归日志](regression_final.log)、[新增并发和故障注入测试](../../tests/test_split_registration.py)。
- source_hashes.json、hardware_and_sources.json记录源码及三机编译模块；source/为本轮冻结源码。
- 查询原始记录在各轮runs/*/queries.jsonl.gz；节点快照在nodes_after.json.gz；重复worker日志压缩校验后保存在worker_results/。
- [最终集群/源码检查](final_cluster_check.json)已通过：实验监听端口29751–29753已关闭，常驻集群保持ready且零活动查询。工作区新模块通过原子替换安装并验证import，见[native_install.json](native_install.json)；旧常驻进程没有重新加载。
''')
(ROOT/'REPORT.md').write_text('\n'.join(out))
print(ROOT/'REPORT.md')
