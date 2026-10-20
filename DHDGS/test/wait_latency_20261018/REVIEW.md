# 等待路径改动审查与 DESIGN 11.13 草稿

日期：2026-10-18。范围：本轮等待时延相关的接纳、计算完成交接、事件控制调度、连接池预约、进程诊断及 native owner 目录优化。本文不填入尚待最终汇总的性能数字，也不把功能正确性测试视作性能证据。

## 审查结论

本轮发现两处必须修正的边界问题，均已在负载五组结束后修复；最终代码再次冻结。相关 78 项定向回归通过。搜索预算分配、扩展决策和访问路径算法未因上述修复改变。性能实验由主任务统一运行并核验；审查过程没有启动额外全量负载。

| 项目 | 审查结果与证据 |
| --- | --- |
| independent admission 关闭与接纳发布 | 已修复：关闭必须在 runtime 状态锁内发布 stop，与 accept 的检查及入队发布串行化。候选用例在修复前稳定复现关闭后 pending=1；正式用例修复后通过。 |
| legacy 过期记录重复扣减容量 | 已修复：清理后重建发送快照，不再对已移除记录发送及 ACK。用例验证过期记录不发送、active_count 保持 0。 |
| 独立接纳队列死锁 | 锁顺序统一为 state→queue；消费端先释放 queue，再进入 _submit_branch 的 state 锁。关闭、metrics 与 accept 遵守同一顺序。故障注入证明持有无关 state 锁时可以出队，但后续提交仍会等待 state 锁。 |
| 多接纳消费者 | dispatch_threads 默认为 1，多个消费者从同一 cv 保护的队列唯一 popleft，仍只提交一个完整 branch 给原计算池；close join 全部消费者。4 个消费者同时阻塞于 submit、随后 20 个分支各执行一次的 shared/independent 两模式用例通过；关闭取消全部未启动工作并 join 的用例通过。 |
| 计算完成交接 | event 模式 local_done 仅向 SimpleQueue 交付报告和 Future 列表并唤醒，不获取 runtime 锁、不注册 Future 回调、不等待网络确认。持有 runtime 锁时，唯一计算 worker 仍能开始下一任务的测试通过。 |
| 晚到完成及退出 | 先停止接纳、排空计算与派发，再 join 控制调度，最后 finish_stopped 消费晚到 LocalDone；该排空忽略每轮 cap。停止后的待发布报告转为明确失败，不继续发送。 |
| Future 已完成/回调竞态 | 注册时立即调用的回调只通知 dirty；检查 done 后才调用 result。新 LocalDone 在同一 turn 进入 finalize，不排到普通 dirty 大队列尾部。失败、取消、重复通知、立即完成均有回归。 |
| 结果/心跳发送 | 一条记录同一时刻最多一个 control_inflight；选择后的发送器会重新获取消息快照，可将排队心跳升级为结果。inflight 记录不被 retention 清理。发送完成后释放容量并通知 dirty。 |
| 连接池 | reserved 只在锁内预约及更新所有权，select/close 在锁外；预约期间 endpoint 不能逐出。关闭、探测异常和归还不重复释放预约。对应 12 项故障注入/配置用例通过。 |
| native directory=None | 仅 native 且 code_membership.npy 存在时省去 Python 目录；prepare_shard 直接读取 sidecar 构造 C++ SearchShard。blocking、resumable 和无 sidecar 的旧索引仍保留目录。新 shard 无 Python 目录的结果和扩展序列一致性用例通过。 |
| GC 与 GIL 配置 | GC 诊断使用有界进程级 callback，不关闭 GC、不调阈值。switch interval 同样是进程级；同进程全部 Engine/服务必须使用相同配置。callback CPU 时钟属于触发收集的线程。 |
| 预算和节点访问 | 本轮调度器只发送已存在分支状态，未调用图扩展或构造新分支。native approved_spawn 仍仅响应 C++ 批准的 payload；work_grant、branch_slots、visited_nodes 和 query_visits 原检查保留。仍需负载报告逐 query 核验，不能用“未改该函数”代替行为验证。 |

### 修复的两个交错次序

第一处问题：accept 已持有状态锁且通过 stop 检查，但尚未入队；旧 close 在锁外设置 stop，独立队列的消费线程看到 stop 且队列为空便退出；随后 accept 入队，任务无人消费。修复后 close 先持有状态锁再设置 stop，accept 要么完整发布在先，要么在后被明确拒绝；消费线程不会越过未完成的发布。

第二处问题：legacy 控制循环先复制 records，再清理过期记录，最后遍历旧快照。过期移除已经扣减一次 active_count，如果旧快照上的最后一次发送成功，又会在 ACK 时扣减。修复后发送只使用清理后的当前记录快照。optimized scan/event 不存在该旧快照发送次序。

### 本轮保留的边界

- max_events_per_turn 是每种事件来源的数量上限，不是墙钟调度期限；一次拿锁、Future 状态检查、GIL 等待或堆压缩仍可能超过期望时间。
- 已 ready 的普通心跳可能在新 LocalDone 被消费前占到空闲发送槽。结果优先表示“当前已知 ready 集合内优先”，不意味着预知尚未消费的结果。
- event 的 local_done_process_ms 包括完成消费、dirty 排空及对应 finalization，不是纯队列驻留或纯锁等待。event.cpu_ms 与旧 scan.cpu_ms 覆盖区间不同，不应直接比较为同一操作的 CPU 降幅。
- SimpleQueue 本身无独立容量参数，未处理完成事件受已接纳分支数量及 active_count 上限间接约束；不能描述成额外的无界接纳通道。
- 事件调度及 native sidecar 优化都不提供零等待、硬实时或 Recall 的数学保证。保留原取消、租约、异常和容量拒绝语义。
- GC snapshot 是诊断快照，不是跨线程事务快照；同进程不能混用不同开关/容量配置。进程中重复 configure(enabled=True) 不重建已有 profiler。
- QueuedSender 对任意外部 Future 回调抛出的 BaseException 不具备 FixedComputeExecutor 相同的防护；当前内部回调不故意抛出该类异常。此为既有组件的健壮性后续事项，不是本轮已证实的等待来源。
- Python 解释器非自由线程构建是当前实现和验证前提；独立队列取出后直接读取已发布、尚不能过期回收的 record，不能把本轮测试推广为 free-threaded Python 的正确性证明。
- 多接纳消费者保证唯一领取，不能保证计算开始或结束仍按全局 FIFO 顺序。不同查询并发本来就不保证全局结果顺序；同一 query 的身份、预算及访问检查仍保留。更多接纳线程也可能增加 GIL/锁竞争，不能未经实测作为推荐值。

### 验证范围与默认值

本轮定向运行：control scheduler/runtime/delay、admission 独立锁与关闭竞态、O(1) admission counter、GC/runtime settings、reserved pool、fixed executor、native sidecar 一致性，共 78 项通过。最终全套回归由主任务运行。

随后只增加 dispatch_threads 消融选项，新增加 3 项多消费者/关闭/迁移测试，并复跑相关 admission 测试共 17 项通过。性能实验另外验证 1/4/16 个接纳消费者；本文不预填结果。

最终另加 completion_fast_path 可选消融，默认/旧迁移 false：只有无派发 Future 的分支非阻塞尝试一次状态锁，成功则用共享 helper 立即发布，否则原完成队列。成功通知使用独立 SimpleQueue，turn 首段优先刷新后选择发送，不取得 dirty 锁。任何非空 Future 列表仍保留异步路径。52 项相关回归通过，包含 runtime/dirty 锁持有期间唯一计算 worker 仍可执行下一任务、取消和过期只发送 error、在途心跳、退出排空。该选项开启时，以下设计草稿中“计算线程不获取 runtime 锁”应准确限定为“不等待 runtime 锁；只尝试 acquire(False)”。更完整的测量要求见 [FASTPATH_REVIEW.md](FASTPATH_REVIEW.md)。

通用 YAML 当前为 scheduler:event、max_events_per_turn:0、queue_lock:shared、python_switch_interval_ms:5.0。旧配置缺失 scheduler 迁移 scan，缺失 cap 迁移 0，缺失 queue_lock 迁移 shared。事件调度候选 cap=64/8 与独立接纳锁保留为可消融选项，不仅凭设计直觉设为最优默认值。历史参考 fixture 显式使用 scan；专用事件测试覆盖新完成交接路径。

## 可合入 DESIGN.md 的 11.13 草稿

### 11.13 事件控制调度、计算完成交接与等待诊断

本轮优化围绕“已具备继续执行条件的工作为什么仍等待”展开。将接纳确认、计算执行、远端派发和最终结果回传分别记录，不把父子分支累计等待直接相加到查询总延迟上。目标仍以 Recall@10≥0.95、无失败为门槛，优先 p99，再比较 QPS。短突发、热页缓存实验的结论不直接推广到冷 SSD 或物理三机稳态流量。

**事件驱动控制路径。** 新增 peer.control.scheduler: scan | event。event 将状态变化以 key 写入去重 dirty 队列，只检查发生变化的分支；心跳、记录保留和 tombstone 到期使用带 generation 的定时堆。过期堆条目按需清理并定期压缩，避免每次唤醒遍历全部保留记录。结果与普通心跳分别排入 ready 队列，已知结果优先；发送并发仍受 max_pending_control 约束。该调度只发送已存在分支的状态，不选择远端搜索目标，不生成网络搜索分支。

本地搜索完成后，计算线程调用 local_done，把报告、已批准派发对应的 Future 列表及完成时刻交给控制路径，随后返回执行器。计算线程不获取 runtime 状态锁来完成报告、不注册派发完成回调，也不等待 TCP ACK。控制路径取得报告所有权，登记 waiting_transfer；Future 回调只发送 dirty 通知。全部 Future 已完成后才读取结果并发布最终报告，出错则发布失败。新消费的 LocalDone 在本 turn 直接检查，不先排到普通 dirty 队列尾部。

保留 peer.control.max_events_per_turn：0 表示不限制每来源的本轮数量，正整数分别限制 LocalDone、dirty、到期 timer 的消费量。已有 ready 工作先尝试发送，然后处理新事件，再选择发送；有剩余工作立即进入下一 turn。这是公平性和批量开销的消融参数，不能称为每 turn 硬时延上限。本轮通用默认保留 0；是否采用有限值由相同负载下的 p99 和完整等待拆分决定。由于 0 也具有 ready 预发和 LocalDone 同 turn 处理，它是同代码无界对照，不等于改动前逐字节实现。

peer.control.completion_fast_path 进一步提供可选的无派发完成快速路径，默认及旧迁移 false。无 Future 的分支用 acquire(False) 尝试状态锁一次，成功则调用不拿锁、不读 Future、不发消息的共享发布 helper；锁忙立即交给原 LocalDone 队列。完成 key 进入独立 SimpleQueue，由控制 turn 在首轮 ready 选择前优先刷新；计算 worker 不进入 dirty 锁或发送队列。非空 Future 列表全部保持原异步交接，取消/租约/期限在真正发布时照常校验。该路径不是硬实时“零等待”：Python/GIL/GC 及既有唤醒操作仍有开销，同步发布也增加 worker 收尾工作。必须同时比较 worker 可用时刻、报告发布后发送等待、API p99 和 QPS，避免仅把完成队列时间挪成 ready→sender 时间。

**接纳路径。** active_count 缓存尚未终态 ACK 的记录数，accept 不再为容量检查遍历全部 records；ACK 和过期移除只释放一次额度。执行配置签名在 runtime 创建时缓存，查询仍校验签名；配置在该 runtime 生命周期中不可原地更改。peer.admission.queue_lock: shared | independent 提供队列锁消融：independent 仅用独立短锁保护 queue/notify，容量、身份去重及状态仍受原状态锁保护。accept 在状态锁内完成记录发布和容量预约，再按 state→queue 入队；消费端释放 queue 后才进入 _submit_branch，避免锁顺序反转。

close 同样在状态锁内发布 stop，并按相同锁顺序通知队列，保证已通过接纳检查的任务先完成发布。已接纳任务在关闭时得到明确失败或正常完成，不可在空队列退出与迟到入队之间丢失。关闭依次排空计算和派发、join 控制线程，再强制消费晚到 LocalDone；最终排空不受 cap 限制。原 scan、legacy 路径保留，legacy 清理后重新获取发送快照，避免已移除记录再次 ACK 导致容量重复释放。

peer.admission.dispatch_threads 控制接纳队列消费者数量，默认及旧配置迁移均为 1。多个消费者在同一队列锁内唯一领取任务，释放队列锁后独立进行状态校验及计算池提交，避免单个消费者的提交/调度延迟阻挡全部后续任务。容量仍计算全部已接纳但未开始计算的分支，出队不释放额度；一个分支只提交一次，native 搜索仍由一个固定计算线程完整执行。关闭 join 全部接纳线程，历史 admission_thread 属性仅作为首线程诊断别名。此参数不提高每个 query 的内部计算并行度、不批准远端扩展；应独立对照 1/4/16，并报告接纳等待及端到端 p99 的变化，不预设线程越多越好。

**减少无用 Python 对象。** native 数据节点在 code_membership.npy 存在时直接由紧凑 membership 构造 C++ SearchShard，不再创建相同 owner 信息的 Python 大字典及逐向量 list。该目录在此路径未被搜索读取，因此不改变拓扑、量化、候选顺序或数据布局。Python/resumable 和无 sidecar 的旧索引继续保留兼容目录。减少对象及 GC 扫描对象数量是合理机制解释；实际查询尾延迟收益必须由相同负载对照和 GC 时间线验证。

**观测开关。** metrics.trace_query_ids 只为指定查询保留逐批次/逐读明细，metrics.lifecycle_trace 为全部查询保留低体积生命周期记录；二者均不改变搜索算法。metrics.gc_trace 开启有界进程级 GC 回调，输出 generation、开始/结束时间、墙钟及线程 CPU、回收数量。GC 继续按原策略运行。runtime.python_switch_interval_ms 控制 Python 进程级解释器切换间隔；不是给某个 query 分配 CPU 配额，也不能保证线程即时唤醒。同进程所有服务必须一致，实验扫描应放到独立进程；通用默认保持 5.0 ms。

**时间拆分。** 查询阶段必须使用同一时钟域内的单调时间戳，逐分支区分：

| 区间 | 定义与限制 |
| --- | --- |
| accepted→dequeued | 接纳队列驻留，包含消费者调度和可能的锁竞争。 |
| dequeued→submit_enter | 出队后状态检查及提交前处理；submit_state_lock_enter/acquired 可进一步定位。 |
| executor_enqueue→dequeue | fixed 计算队列内的真实驻留；不同于 submit 调用总墙钟。 |
| worker_enter→native_search_start | 开始工作到进入 C++ 搜索，包括状态检查、适配及 GIL。 |
| native_loop_done→native_gil_reacquired | 原生循环结束后重新获取 GIL 的区间，不等于 SSD 时间。 |
| local_done_enqueued→dequeued | 本地报告交给控制路径后等待被消费。 |
| local_done_dequeued→consumed | 消费者获取状态锁和登记报告的区间。 |
| local_done_consumed→finalize_enter | 等待派发就绪及控制路径再次处理的合并区间，应结合 Future/transfer 时间戳解释。 |
| finalize_enter→acquired | 真正消费最终报告的那次状态锁等待；未完成 Future 的早期探测不覆盖该记录。 |
| finalize_acquired→report_ready | 最终统计、状态校验及报告发布。 |
| report_ready→结果发送→API返回 | 控制容量/发送队列、RPC、入口接收合并和 API 返回；多分支按关键路径分析。 |

阶段 wall 与 thread CPU 配合解释；wall−CPU 不能直接命名为某一个锁或 GIL 等待。不同服务器的单调时钟不可直接相减。全过程继续核验预算守恒、节点不重复访问、每个远端分支对应批准决策、原生分支固定计算线程、队列拒绝、失败率及所有进程正常退出。最终默认值和推荐实验 YAML 根据完整实验报告确定；未达到目标的等待保留在报告中，不将等待转移称为消除。
