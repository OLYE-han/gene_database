# 无远端派发的完成发布 try-lock 候选

此文起初为只读评审。随后经主任务确认，已实现可消融的 `peer.control.completion_fast_path`（默认及旧迁移 false）；`local_done_fastpath_candidate.py.txt` 仍保留为历史草稿，不是可覆盖模块的实现。最终 fastpath 相关及控制/接纳回归共 52 项通过，性能结论等待同代码 off/on 逆序复测。

## 可行性

可以减少无 Future 分支的“等待控制消费者才能发布报告”阶段。前提是发布只取得一次非阻塞 runtime 状态锁，成功时完成短统计及终态发布，失败立即走原完成队列。任何带 Future 的分支，即使 Future 看起来已经完成，也保持原异步路径，以免把 done/result/callback 的锁及异常处理移回计算线程。

现有 `_finalize_ready` 不能直接用于该路径：它内部阻塞取状态锁。成功后也不能调用现有 `dirty` 来声称全路径无锁等待，因为 dirty 自身还取另一把阻塞锁。候选将完成发布抽成锁内纯辅助函数，成功 key 放到单独 SimpleQueue，由控制消费者优先接收并刷新 ready 队列。所有 socket、发送队列和控制容量仍由控制路径管理；计算线程不直接发结果。

语义安全不等于性能一定改善。若只是把 report_ready 提前，而同一个控制消费者仍在长 turn 里忙碌，原 local_done 排队可能变成 ready→sender 等待。即使增加优先通知，正在运行的 turn 也不会被打断。必须同时测 native_loop_done→worker_available、native_loop_done→首个/最终结果发送、入口返回，以及计算就绪队列是否增加。

“无等待”只能准确描述为“不等待 runtime/dirty 状态锁，不等待 Future/TCP”。Python 统计构造、GIL、GC、操作系统调度及原有 Event.set 内部锁不提供硬实时保证。同步发布也会延长计算 worker 的调用区间；若实际时间超过现有轻量交接，就可能损害后续查询。

## 兼容与异常要求

1. stop、cancel、lease、deadline 必须在取得状态锁后检查。停机前已经成功发布的报告保留终态；停机后才尝试发布则显式失败，不能绕过租约检查。
2. 一个分支只能把报告交给一个所有者：try-lock 成功与普通队列二选一。Future 列表必须由该分支计算线程在搜索结束后固定，不允许另一个线程继续追加。
3. 对不存在记录及重复终态通知采用明确策略；对 pending_report 已存在的双重所有权必须诊断，不可覆盖报告或重扣容量。正常生产只有唯一计算所有者发送一次 LocalDone。
4. 本地发布不等于 ACK，不释放 active_count。原发送 ACK 或保留期清理仍各按原规则只释放一次容量。
5. 控制 inflight 状态不能由快速发布路径擅自清除。排队/在途心跳完成后 dirty 通知会重新处理结果；发送器在实际执行时仍可把心跳升级为结果。
6. 快速通知队列只保存 key，完成消息不复制报告。close 排空该队列，不漏引用；不要在结束时重发或重写已发布报告。
7. 普通队列和快速路径共用发布 helper，保持统计公式、错误文本、lease 校验、peer_transfer 快照口径一致。不要维护两份随时间分叉的发布逻辑。
8. 采用可消融 YAML `peer.control.completion_fast_path: false`，旧配置迁移 false。仅 event 生效；待测量收益后再决定推荐值。

## 必需测试

| 用例 | 期望 |
| --- | --- |
| runtime 锁由另一线程长期持有，无 Future | local_done 及时返回、进入旧队列；唯一 compute worker 可启动下一任务。 |
| dirty 锁被持有，runtime 锁空闲 | 快速发布返回，不尝试 dirty 锁；报告立即完整终态，published 队列有一个 key。 |
| 任意非空 Future 列表 | 原路径不变。注入禁止访问的 Future.done/result/add_done_callback，确认 compute 线程都不访问。 |
| 无 Future 且 state 锁可获取 | stats 与旧异步 finalizer 等价，报告发布仅一次，active_count 未减少。 |
| 本地搜索结束时同时 cancel/stop/租约过期 | 取得锁后的真实状态决定 success/failure，不允许过期成功。 |
| 已在途心跳/发送容量已满 | 无重复 inflight、不丢结果；完成心跳后能够发送结果并 ACK。 |
| 大量普通 dirty/LocalDone 排队 | 快速通知在下一 turn 的 ready 选择之前处理；不受普通队列尾部阻挡。 |
| 消费者已清 wakeup、生产者随后发布 | 不丢唤醒，快速队列非空时不睡眠。 |
| 一方正在 close，另一方完成搜索 | 全部计算线程退出后快速队列与普通队列都清空，无重复发布/负容量/未结束分支。 |
| 重复完成通知与 pending 双重所有权故障注入 | 不覆盖已 ACK 报告，不二次释放容量；异常所有权有明确诊断。 |
| 查询算法不变量 | 预算、IDs/精确距离、访问路径、批准的扩展及 fixed compute thread 全部保持。 |

## 建议测量

固定当前最佳配置，fastpath false/true 正序和反序各一次，查询集合、2000 并发槽、搜索预算及 trace 选择一致。报告无 Future 占比、try-lock 尝试与成功率、进入快速路径的 worker 收尾 p50/p95/p99、普通 fallback 的完成队列驻留、report_ready→sender、native_loop_done→API 返回、全体 QPS/p99/Recall/失败数。

新 trace 使用 completion_publish_path、fastpath 尝试/取得锁、finalize/report_ready 时间，快速路径不伪造 local_done_enqueued/dequeued 为零耗时队列记录。GC 诊断保持一致。若只有 ready 时间提前而 API p99 不变，结论应是“发布等待前移，尚未降低整体等待”，不能采纳为已经解决瓶颈。

实现时间戳为 completion_fast_path_attempt_ns、completion_fast_path_acquired_ns、completion_fast_path_return_ns、completion_ready_enqueued_ns、completion_ready_consumed_ns，并复用 finalize_enter/acquired_ns 与 report_ready_ns。回退记录 completion_fast_path_fallback=disabled/futures/lock_busy。统计为 completion_fast_path_hits、completion_fast_path_lock_busy、completion_fast_path_future_fallback，由单控制消费者更新，运行中的快照可能暂时落后，完成队列最终排空后才完整。运行中命中占比不能忽略尚未消费的通知。
