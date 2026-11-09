# DHDGS 当前实现的锁与同步机制清单

2026-10-27补充：本轮共享资源与并行等待复测见[test/resource_parallel_20261027](test/resource_parallel_20261027/)。新增顶层计时将合并线程取任务后的查询锁等待与队列/调度等待拆开；这属于观测改动，不代表已经消除锁竞争。`head_stage_timings_ms.merge_query_lock_wait_ms` 使用取锁前后时间，`head_finalize_registry_wait_ms`覆盖完成阶段注册表锁等待；`merge_worker_enter_ns`至`merge_started_ns`是查询锁等待，`merge_enqueued_ns`至`merge_worker_enter_ns`是提交/线程池排队及调度。路由在开启`metrics.lifecycle_trace`时另外记录HNSW与种子准备的CPU/墙钟时间，内部临时字段在派发到数据节点前删除。CPU时间不能与墙钟等待相加当作互斥分解。

首次审计日期：2026-10-20。对象是工作区中的 `python/dhdgs/`、`cpp/`、`tools/`，以及实际引用的 hnswlib 头文件。包含可选实现、建图和管理路径；不把历史实验源码副本和测试用例里的同步器重复计入。首次审计只修改文档；随后按用户要求进行了第10节的锁优化，旧审计JSON的SHA-256及行号仍表示首次快照。

第1–9节是**源码审计**，不是远端常驻进程的锁实例转储；后续锁竞争实验另见第10节。不同 YAML 会启用不同路径，不能把表中的全部锁叠加到一次查询上。构造位置、源码 SHA-256 见 [审计记录](test/lock_audit_20261020/inventory.json)。行号对应首次审计快照，后续修改可能改变行号。

## 1. 最重要的结论与计数口径

1. 当前 native 数据节点搜索没有持有覆盖完整图搜索、RaBitQ 估距、精确距离和 `pread` 的分片级互斥锁。不同查询的 native 循环可以并行。
2. 查询接纳、去重、任务队列、连接池、分支完成、租约与取消仍有共享可变状态，因此还有锁。只读向量并不意味着查询运行时只读。
3. `PeerRuntime.lock` 的范围较大。接纳、启动、租约、清理、统计等会使用它；`admission.queue_lock: shared` 时，接纳队列也与它共用一把锁。这是值得优先测量的竞争点。
4. C++ 控制器有独立的状态锁，当前在锁内处理控制命令、定时器和派发准备；Python 接纳路径可能持有 runtime 锁再等待它。
5. HNSW 只读查询也使用 visited 数组池的借还锁，不能称整个系统无锁。

下面编号表示**锁的定义或角色**，不表示运行时实例数。例如，每个查询有自己的 registry 锁、每个进程有自己的 GIL、每个建图顶点有自己的邻接表锁。多个 `Condition(existing_lock)` 可能共用一个互斥量；本清单不把这些别名当成独立锁。

Python `Condition()` 未提供锁时内部默认创建 `RLock`；`wait()` / `wait_for()` 等待时释放底层锁，醒来后重新取得它。因此，线程在条件变量上休眠不等于一直持锁，也不等于 CPU 忙等。

## 2. 数据节点、发送与本地 I/O

| 编号、锁与位置 | 保护什么、谁使用 | 持锁范围、适用条件与影响 |
|---|---|---|
| D1 `PeerRuntime.lock`，`Lock`；[peer.py](python/dhdgs/peer.py) | 每个数据 runtime 的 `records`、去重墓碑、`query_visits`、活动计数、任务状态和接纳额度；接收、接纳、计算生命周期、控制和管理统计路径共用 | 不覆盖整个搜索或实际 `pread`。旧接纳路径在锁内应用native更新、清理记录及注册native分支，统计仍会复制大量信息；lease已改为O(1)索引查询。新增可选拆分注册路径见第10节。 |
| D2 `PeerRuntime.admission_cv` 底层锁；[peer.py](python/dhdgs/peer.py):330 | 接纳队列入队、取出、唤醒与停止 | `queue_lock: shared` 时就是 D1，**不是另一把锁**；`independent` 才创建独立 `Lock`。生产者按状态锁→队列锁顺序；消费者先释放队列锁，再提交任务。自适应接纳启用时，实际队列由 D8 管理。 |
| D3 `FixedComputeExecutor._cv`；[compute_executor.py](python/dhdgs/compute_executor.py):23 | 计算池队列、关闭标志、就绪/活动计数、容量与统计；提交者和所有计算线程共用 | 出入队及计数时持锁，执行查询函数和 Future 回调时不持有。每个计算池一把，不是每个计算线程一把。仅 fixed 计算模式。 |
| D4 `QueuedSender.cv`；[sender.py](python/dhdgs/sender.py):16 | 发送队列、容量、活动数量和停止状态；派发者和发送线程共用 | 网络任务和完成回调在锁外执行。不同 sender 实例各有自己的锁，例如跨节点派发、顶层入口派发；native 控制模式不会再创建 Python 控制 sender。 |
| D5 `ConnectionPool.cv` / `FairConnectionPool` 继承的同一条件锁；[peer_rpc.py](python/dhdgs/peer_rpc.py):58 | endpoint 表、空闲连接、借用额度、淘汰与关闭；FIFO 版本还保护等待者队列和授予关系 | **不会在整段 RPC 的发送/接收期间持有**。reserved/FIFO 路径把 socket 探测和关闭移到锁外；旧 locked 路径仍会在锁内做零超时 `select` 和 socket 关闭。借用额度不足的等待也不是持续持锁。每个 pool 实例各一把。 |
| D6 `LocalIO.cv`；[async_io.py](python/dhdgs/async_io.py):85 | 跨线程提交/释放命令、batch 活动额度、关闭/失败状态与统计读取；计算线程和本地 I/O reactor 使用 | reactor 取出命令后在锁外调度和处理实际 I/O。`ReadBatch.release()` 复用它。拒绝提交路径会在锁内设置 Future 异常，唤醒通知也在部分临界区内。仅异步本地 I/O 后端；native + sync_pread 不走该调度器。reactor 自有计数不全靠此锁写入，不能据此宣称统计快照全局原子。 |
| D7 `Shard.cache_lock`，`Lock`；[index.py](python/dhdgs/index.py):344 | 可变 LRU 的查找、移到队尾、插入、淘汰和大小更新 | `cache_bytes_per_shard: 0` 时读取路径不获取此锁。实际 `pread` 在锁外；native SearchShard 不通过这个 Python LRU。锁对象存在不等于查询会使用它。 |
| D8 `AdaptiveServicePool.cv`；[adaptive_service.py](python/dhdgs/adaptive_service.py):20 | 自适应服务队列、线程集合、目标数量、活动计数、EWMA 和历史；服务提交者、服务线程及伸缩逻辑共用 | 任务、完成回调、下游判断在锁外；扩容创建线程时有锁内 `thread.start()`。仅启用相应 adaptive_services 时用于 admission、transfer 或 head_dispatch；它不决定是否扩展远端图节点。 |
| D9 `ControlEventScheduler._dirty_lock`，`Lock`；[control_scheduler.py](python/dhdgs/control_scheduler.py):25 | 状态变更通知的 deque/set，合并重复 dirty 通知；事件生产者与单个控制消费者共用 | 只保护通知入队/取出。消费者取出后释放它，再取 D1，避免与生产者形成反向锁序。仅 Python event 控制调度器，native 控制器不用它。 |

## 3. 顶层每查询的公共锁

| 编号、锁与位置 | 作用 | 范围与影响 |
|---|---|---|
| Q1 `BranchRegistry.lock`，`Lock`；[peer.py](python/dhdgs/peer.py):53 | 保护单查询的涉及节点、心跳、分支报告、错误和完成判定；校验父子关系、访问路径、全局预算守恒 | 每个查询一把，各查询不直接争用这一把锁。但完成判定会在锁内遍历分支树。防止收到部分报告时过早结束。 |
| Q2 `HeadSession.delivery_lock`，`RLock`；[peer.py](python/dhdgs/peer.py):130 | 串行化同一查询的报告接收、重复报告摘要、入口派发完成、取消/关闭与终态发布 | 会嵌套取得 Q1。采用可重入锁支持同线程嵌套完成路径。基础 HeadHub 模式还会在持锁时发送 ACK；PipelineHub 的 reactor 在回调之后实际发送 ACK，没有这一相同的阻塞发送范围。 |
| Q3 `dispatch_guard`，`Lock`；[search.py](python/dhdgs/search.py):82 | 协调延迟的入口派发与生成器 finally/取消清理 | 防止查询已经取消，派发任务却又创建 session。session 注册可能嵌套取得 hub 锁；实际入口 RPC 在该锁外。每查询一个。 |

## 4. Native C++ 的互斥锁

| 编号、锁与位置 | 作用 | 范围与影响 |
|---|---|---|
| C1 `Engine::queue_mu`；[native_control.cpp](cpp/native_control.cpp):57 | 控制命令队列和停止状态，多生产者、单控制消费者 | 入队、出队和等待设置时持有；`wake` 条件变量使用此锁，等待时释放。注册分支中按 queue_mu→state_mu 嵌套获取。 |
| C2 `Engine::state_mu`；同上 | native 控制分支表、endpoint、timer、ready 队列、结果和控制统计 | 控制线程一轮 `command()`、`timers_due()`、`dispatch()` 在此锁内；snapshot 也在锁内遍历复制分支。派发组包和 Client.submit 的编码可能在其内执行，实际 socket 等待由独立 reactor 执行。是主要竞争候选。 |
| C3 `Engine::updates_mu`；同上 | native→Python 的 ACK/过期通知数组 | native 追加与 Python `take_updates()` 交换数组时持有；将取出的 C++ 值转成 Python 对象在此 mutex 外。但调用者可能仍持有 Python D1。 |
| C4 `Engine::close_mu`；同上 | 避免并发重复关闭控制器 | 覆盖停止、join 和清理，可能长时间占用；属于关闭路径，不是正常查询的距离计算路径。 |
| C5 `Client::Impl::mutex`；[native_wire.cpp](cpp/native_wire.cpp):110 | 网络客户端生产者队列、endpoint 待处理额度、停止状态 | 提交/取出任务、额度归还时持有。msgpack 编码在此 mutex 外，但调用者可能持有 C2。实际连接、收发由 reactor 执行，不持此生产者锁等待响应。 |
| C6 `Client::Impl::close_mutex`；同上 | 串行化网络 reactor 的关闭/join | 关闭路径。与 C5 是两把不同的锁。 |
| C7 `TestClient::mutex`；[native_wire.cpp](cpp/native_wire.cpp):224 | `NativeWireClient` Python 测试适配器的 completed 数组，回调追加、poll 取出 | 工具/测试适配路径；生产 NativeController 直接使用 Client，不使用这层完成数组锁。 |
| C8 `graph_locks[node]`；[native.cpp](cpp/native.cpp):292 | 并行 Vamana 建图时每个顶点邻接表的读取副本、替换与反向边裁剪 | 每顶点一把，只在建图期间使用。当前实现不同时持两把 graph_locks。运行时只读图搜索不获取这些建图锁。 |
| C9 `failure_lock`；[native.cpp](cpp/native.cpp):298 | 并行建图工作线程安全保存首个异常 | 只用于构建失败处理，不参与在线查询。 |

## 5. 顶层各运行模式的锁

这些模式有替代关系；例如 head_lanes 子进程内部使用 PipelineFlow/PipelineHub，但不再同时走独立 receiver 进程模式。

| 编号、锁与位置 | 作用 | 范围与条件 |
|---|---|---|
| H1 模块 `_lock`；[head_hub.py](python/dhdgs/head_hub.py):13 | 进程内 `_hubs` 注册表、实例共享与引用管理 | 主要用于 engine/hub 获取与释放；创建 hub 可能在锁内。不是每次邻居扩展的锁。 |
| H2 `HeadHub.lock`；[head_hub.py](python/dhdgs/head_hub.py):46 | session 表、终态去重记录、连接集合 | 基础 hub / receiver 子进程路径。终态重试的身份和摘要校验可在锁内；普通报告另用每查询 Q2/Q1。不要将它与每查询锁混淆。 |
| H3 `PipelineHub.lock` + `cv`；[pipeline_hub.py](python/dhdgs/pipeline_hub.py):106 | session、pending、终态记录、停止、心跳唤醒/工作快照 | `cv=Condition(self.lock)`，是一把底层锁。心跳会在锁内获取活动集合快照；网络 RPC 在锁外。pipeline 及 lane 子进程使用。 |
| H4 `AdaptiveIngress.cv`；[head_pipeline.py](python/dhdgs/head_pipeline.py):12 | 接收工作队列、工作线程数量、伸缩和关闭 | handler 在锁外运行；扩容时可能在锁内启动线程。外层 LaneIngress 与子进程 PipelineFlow 可各自拥有实例。 |
| H5 `PipelineFlow.cv`；[head_pipeline.py](python/dhdgs/head_pipeline.py):71 | 查询记录、容量、waiting/cancel、路由排队计数、drain | 正常路由、生成器续执行、merge 在锁外；接纳失败等特殊路径存在锁内 generator.close。pipeline 模式。 |
| H6 `LaneEngine.lock`；[head_lanes.py](python/dhdgs/head_lanes.py):142 | 父进程记录、worker 可用性/负载/归属、deadline、配置/关闭状态 | 选择 worker、登记完成等使用；非阻塞队列提交可在锁内，实际 IPC 写由 writer 在锁外执行。head_lanes 模式。 |
| H7 `lane_main.active_lock`；[head_lanes.py](python/dhdgs/head_lanes.py):48 | 单个 lane 子进程的在途 Future 表 | 查询登记、取消查找、完成删除和配置前检查；Future 操作不需要一直占用此表锁。 |
| H8 `lane_main.write_lock`；[head_lanes.py](python/dhdgs/head_lanes.py):47 | 防止多个完成/控制线程同时写同一 IPC Pipe，破坏消息帧 | **持锁执行 `conn.send_bytes()`**，父进程读取慢、管道有背压时可能延长占用。打包在锁外。 |
| H9 `QueryFlow.cv`；[head_flow.py](python/dhdgs/head_flow.py):9 | 旧续执行流程的查询记录、容量、等待/取消/排空 | head_query_workers 等旧流程使用；不与 PipelineFlow 当成同一条查询路径重复计算。 |
| H10 receiver `output_lock`；[head_receivers.py](python/dhdgs/head_receivers.py):39 | receiver 子进程发往父进程的控制消息 Pipe | 持锁写 IPC；适用于独立 receiver 模式。 |
| H11 receiver `terminal_lock`；[head_receivers.py](python/dhdgs/head_receivers.py):40 | 独立终态 Pipe 的多线程写入 | split IPC 使用，持锁写 IPC；没有独立终态 Pipe 时终态走 H10，此锁虽构造但不走该写路径。 |
| H12 receiver `registration_gate`；[head_receivers.py](python/dhdgs/head_receivers.py):41 | 协调共享身份的延迟 session 创建与注销，防止重复登记 | 共享注册模式；锁内可获取 H2、读取 H15 并创建 session。 |
| H13 `ProcessHeadHub.lock` + `deadline_cv`；[head_receivers.py](python/dhdgs/head_receivers.py):179 | 父进程 session 归属、worker 分配、deadline 堆、关闭 | `deadline_cv` 共用此锁，非另一个 mutex。独立 receiver 模式。 |
| H14 每个 receiver 的 `w['lock']`；[head_receivers.py](python/dhdgs/head_receivers.py):204 | 某个 receiver 的 pending 注册请求、session 和错误状态 | 粒度为每个 worker，减少所有 receiver 争用一个全局锁。 |
| H15 `ReportRegistry.lock`，`multiprocessing` 进程共享 Lock；[report_registry.py](python/dhdgs/report_registry.py):18 | 共享内存身份表的写入、查找、删除，避免子进程读取半条记录 | 父进程写、接收子进程读。锁内含哈希探测，`get()` 还在锁内 msgpack 解码；不是只有一次原始内存复制。网络收发在锁外。主要是 direct 的共享身份报告路径。 |
| H16 `DirectHub.lock` + `cv`；[head_direct.py](python/dhdgs/head_direct.py):34 | direct 模式的 session、pending、确认/关闭状态 | `cv` 共用 lock；仅 head_direct 模式。 |
| H17 `DirectFlow.cv`；[head_direct.py](python/dhdgs/head_direct.py):402 | direct 查询流程记录、容量、取消与排空 | 与 DirectHub 状态锁不同。另有两个信号量，见第 7 节。 |

## 6. 管理、基准与旧 RPC 路径

| 编号、锁与位置 | 作用 | 范围与影响 |
|---|---|---|
| M1 `NodeControl.lock`，`RLock`；[managed_node.py](python/dhdgs/managed_node.py):97 | 节点 accepting、读者计数、runtime 引用、配置事务和状态一致性 | 普通请求只在取 runtime/增减 readers 时短持有，`yield peer` 后已释放。status 调用 PeerRuntime.metrics 时仍持有它；配置切换在排空后进行，可在锁内创建/关闭 runtime。 |
| M2 `NodeControl.admin_lock`，`Lock`；同上 | 防止管理命令并发改变事务或快照分块状态 | 可以覆盖快照构造/编码和管理事务；普通数据请求不取它。 |
| M3 `ManagedCluster.op_lock`；[managed_cluster.py](python/dhdgs/managed_cluster.py):48 | 阻止配置、恢复、benchmark 等管理操作互相交叉 | 非阻塞获取排他管理权限，失败则拒绝；可跨整个管理事务和网络调用，不是普通每查询互斥锁。 |
| M4 `ManagedCluster.cv`；同上 | active 查询数、busy/ready 与排空等待 | 普通查询开始/结束更新计数时使用，不持有它执行整个搜索。 |
| M5 benchmark 局部 `result_lock`；[managed_cluster.py](python/dhdgs/managed_cluster.py):210 | 旧 benchmark 客户端线程收集结果 | 只保护结果容器，lane 异步 benchmark 用完成队列。 |
| M6 benchmark 局部 `lock`；[benchmark.py](python/dhdgs/benchmark.py):103 | 多工作线程领取下一个 job、追加结果 rows | 引擎搜索在锁外。不只是结果锁，也保护共享 job 迭代器。 |
| M7 `prestart_executor()` 局部 `cv`；[compute_executor.py](python/dhdgs/compute_executor.py):144 | 预启动 ThreadPoolExecutor 时确认所有线程已经启动 | 启动屏障；正常查询不重复使用它。 |
| M8 `TCPTransport.locks[sid]`；[transport.py](python/dhdgs/transport.py):79 | 旧/参考 RPC 传输共享 socket 的请求与响应配对 | **持锁覆盖该 sid 的 connect/send/receive/retry**，同目的节点串行；自主 peer 图搜索使用 peer_rpc 等路径，不能把此旧路径锁当作当前 peer 发送池的行为。 |

## 7. 库内锁、GIL、信号量、事件与原子变量

### 7.1 实际使用的 hnswlib

代码位于 `../Reference_Code/RaBitQ-Library/include/rabitqlib/third/hnswlib/`。

| 锁 | 作用 | 只读路由搜索是否涉及 |
|---|---|---|
| `VisitedListPool::poolguard`；[visited_list_pool.h](../Reference_Code/RaBitQ-Library/include/rabitqlib/third/hnswlib/visited_list_pool.h):40 | 借还独立的 visited 标记数组；池空时在锁内分配新数组 | **是**，进入/结束底层搜索时借还。取得数组后图遍历不一直持锁；数组 reset 在锁外。 |
| `HierarchicalNSW::label_op_locks_`；[hnswalg.h](../Reference_Code/RaBitQ-Library/include/rabitqlib/third/hnswlib/hnswalg.h):41 | 同标签操作的条带锁；插入、更新、删除和按标签读取 API | 普通 `searchKnn` 不逐候选获取它。 |
| `global`；同上:43 | 更新全局入口/最高层等建图共享状态 | 建图/修改使用，普通只读 `searchKnn` 不取。 |
| `link_list_locks_`；同上:44 | 每顶点邻接表建图、更新与相关读取 | 构建/更新路径使用；只读 `searchKnn` 不逐邻接表取锁。 |
| `label_lookup_lock`；同上:60 | 标签→内部 ID 映射的操作 | 标签 API 和修改使用；普通 `searchKnn` 不取。 |
| `deleted_elements_lock`；同上:71 | 删除元素集合、删除槽位复用 | 删除/复用路径，不是只读搜索路径。 |

本项目链接 `rabitqlib::core`，CMake 关闭其独立索引与 OpenMP 构建。RaBitQ 仓库其他 `src/index/` 实现中的锁，不应当算到本项目当前运行路径上。

### 7.2 GIL

当前常规 CPython 中，每个 Python 进程有自己的 GIL；同一进程内 Python 对象操作受其约束。它不是我们手写的一把 Lock，却会影响接纳、调度、组包、回调和统计。

[search_loop.hpp](cpp/search_loop.hpp):202 在 native 主循环释放 GIL，`pread`、RaBitQ 和精确距离主路径可在不同计算线程并行；远端分支 Python 回调（:279）、可选异步 I/O 回调（:310、:361、:381）等仍要重新获取。返回 Python 结果也需要 GIL。不能把“主循环释放 GIL”解释成“整个查询生命周期没有 GIL”。

`NativeController.register_branch` 旧路径和 Ticket 方法的绑定没有普遍释放 GIL；第10节新增注册阶段的可选释放开关。`snapshot`、`take_updates` 在 C++ 工作期间显式释放，再获取 GIL 转换结果。Python runtime 锁、GIL 与 native mutex 的组合可能造成间接排队，是否构成实测瓶颈要测量。

### 7.3 另外两处显式信号量

| 位置 | 作用 |
|---|---|
| `DirectFlow.http_slots`，`BoundedSemaphore`；[head_direct.py](python/dhdgs/head_direct.py):404 | 限制同时接纳的 HTTP 工作数量；非阻塞申请，满则拒绝。 |
| `DirectFlow.result_slots`，`BoundedSemaphore`；同上:405 | 限制待处理结果工作，防止结果处理/IPC 堆积失控。 |

信号量保护的是额度，允许多个持有者，不等同于一把互斥锁；其标准库实现仍有内部条件锁。

### 7.4 Event 与标准库隐式同步

源码 Event 构造位置也列入审计 JSON，职责包括：

- 查询完成/取消等待：`HeadSession.done`、resumable task.done；连接池 FIFO waiter、receiver 注册应答等就绪通知。
- 生命周期停止/唤醒：PeerRuntime.stop/control_wakeup、LaneEngine.wake、AdaptiveServicePool.halt、ResourceSampler.stop_event、部署探测 done。
- 启动/基准屏障：compute 预启动 release、head worker ready/HTTP initialized、benchmark start_event/gate。
- DirectHub 的 `asyncio.Event` 用于事件循环停止；它是协程通知器，不是跨线程 mutex。

`threading.Event` 内部有 Condition/Lock；等待通常休眠并释放内部锁，不是忙等。`TerminalSignal` 等继承 Event 的对象也继承相应内部同步，不能只搜索 `Lock()` 就忽略它们。

另外还存在标准库内部的同步：`queue.Queue` 的一个 mutex 和 not_empty/not_full/all_tasks_done 条件；`SimpleQueue` 的内部同步；Future 的条件锁；ThreadPoolExecutor 的提交/关闭锁、队列与空闲线程信号量；ProcessPoolExecutor/multiprocessing 队列与进程通信内部同步；socketserver 的停止通知等。它们保护队列、Future 状态、线程/进程生命周期，不是向量图邻接表锁。具体内部实现取决于 Python 版本，不能把每次 queue/Future 操作都简单换算成一把额外应用锁。

### 7.5 原子变量与单所有者设计

- [search_control.hpp](cpp/search_control.hpp)：lease、deadline、stopped 使用原子变量；搜索线程检查取消/超时不用每次取得 D1。
- [read_metrics.hpp](cpp/read_metrics.hpp)：累计耗时和请求/完成/失败/活动/峰值计数使用原子变量，已没有旧的分片统计 mutex。
- [native_control.cpp](cpp/native_control.cpp)：命令队列深度/峰值原子计数。
- [native_wire.cpp](cpp/native_wire.cpp)：pending 计数、测试请求编号原子计数。
- [native.cpp](cpp/native.cpp)：并行建图 next/completed/failed 原子状态；hnswlib 另有元素数、删除数、距离计算/跳数原子计数。
- [async_io.cpp](cpp/async_io.cpp)：提交计数原子变量；ring 主要由单 reactor 持有，没有另加一把完整 I/O 执行 mutex。

原子变量仍可能造成缓存行竞争；C++ `std::atomic` 也不能不检查目标平台就宣称全部硬件 lock-free。单 reactor/事件循环拥有的容器可不用额外应用 mutex，但单线程本身仍有有限处理能力。

本清单不宣称穷举 Python、libc 内存分配器、BLAS、文件缓冲器、内核 TCP、页缓存和 SSD 驱动内部所有锁。那些需要结合实际二进制和运行时分析；不能从应用源码给出精确实例数。

## 8. 当前最值得关注的持锁范围

### 8.1 数据节点状态锁

接纳路径的主要嵌套关系是：

```text
NodeControl.data(): 取节点状态锁 → 读引用/计数 → 释放
PeerRuntime.accept(): 取 runtime 状态锁
  ├─ 应用 native ACK/过期更新，清理保留记录
  ├─ 去重、容量、状态发布
  ├─ NativeController.register_branch()
  │    ├─ 短暂取得 native state_mu 查询 endpoint
  │    └─ queue_mu → state_mu 登记分支
  └─ 将任务放进接纳队列（shared 复用状态锁；independent 另取队列锁）
释放 runtime 状态锁

接纳线程：取队列 → 释放队列锁 → 提交检查 → 释放状态锁 → 计算队列
计算线程：取计算队列 → 释放队列锁 → 更新启动状态 → 释放状态锁
         → native 搜索（主循环释放 GIL）
```

具体值得测量的点：

1. **已修复：**旧`lease()`在D1内扫描全部保留记录；现在通过`query_visits`直接查找qid，保留token、renew和cancel检查。此项不再应列为当前的全表扫描。
2. `accept()` 持 D1 注册 native 分支，可能等待 C1/C2；新 endpoint 的 DNS 虽在 **native state_mu 外**，调用者仍可能持有 **Python D1 和 GIL**。因此不能简单说“DNS 完全在锁外”。
3. `_native_updates_locked()` 消费更新、删记录和清理墓碑。批量释放 Python 对象也发生在调用者持 D1 时。
4. `metrics()` 持 D1 获取 native snapshot，并遍历保留记录构建时间戳/控制事件列表及计数，还会取得其他池的统计锁。snapshot 本身又占 C2。监控频率和 trace/retention 规模会影响开销。
5. shared 接纳把这些工作与队列 dequeue 绑定到同一互斥量；independent 能分离队列锁，但消费者若仍需要 D1，就不能保证全部等待消失。
6. 新的 `mirror_outside_lock` 可将 take_updates 的获取/转换提前到 D1 外，应用更新仍需 D1；`submit_without_state_lock` 只跳过特定提交检查段，不代表系统无锁。两项均须以实际 YAML 为准。

### 8.2 控制器、IPC 与备用路径

- C2 覆盖每轮命令、到期定时器与派发准备，批量工作过多会延长注册或 snapshot 的等待。网络响应本身不在 C2 内等待，但组包/编码仍可能在它内执行。
- H8/H10/H11 为消息帧完整性持锁写 Pipe，读端消费慢时会出现背压。去掉锁而继续多写者共用同一 Pipe，可能破坏消息；改为单写者队列才是可讨论的替代结构。
- 基础 HeadHub 的 Q2 覆盖 ACK 发送，但该锁只属于一个 query；其风险不能直接套到 PipelineHub reactor 的 ACK 路径。
- M8 覆盖整次旧 RPC，但不是自主 peer 搜索的连接池。必须先核对执行模式，才能判断它是否相关。
- HNSW poolguard 的临界区通常是数组借还；池空时分配较大数组可能增加持锁时间，不能未经测量就断言它总是可忽略。

## 9. 如何理解配置与后续测量

仓库 `configs/dhdgs.yaml` 是配置模板，当前含 `control.scheduler: event`、`compute.mode: executor`、adaptive_services 默认关闭。它与最近 QPS 实验的 [candidate.yaml](test/control_capacity_20261020/candidate.yaml) 不相同。

该候选配置使用 head_lanes、native control、fixed compute、queued sender、shared admission、sync_pread、零应用 LRU。其关键锁来自 D1/D2/D3/D4/D5、Q1/Q2/Q3、C1–C6、H3–H8 和管理层；native 控制不使用 D9/Python 控制 sender，sync_pread 不使用 D6，零 LRU 不获取 D7。后续 adaptive 实验路径存在于当前源码中，但不能假定每个现存服务都已启用。

若继续做性能审计，应分别记录每把关键锁的申请、取得、释放时间，以及临界区线程 CPU 时间，并按 accept/start/lease/metrics/cleanup 分类。需要特别区分：

```text
任务排队 ≠ 等锁 ≠ 等 GIL ≠ CPU 未被调度 ≠ 网络/IPC 背压 ≠ SSD 等待
```

已有部分 admission/control 时间戳只能覆盖部分临界区，不能据此算出每把锁的完整 p99。首次审计没有运行锁竞争实验，因此第1–9节列出的是可验证的源码作用与风险，不把历史的全部等待归因于某一把锁；后续实测见第10节。

## 10. 后续锁优化与验证（2026-10-20）

新增`peer.locking.split_native_registration`与`native_register_release_gil`，默认均关闭，用于同源同二进制消融。它们不增加搜索线程、不改变图扩展决策。独立验证目录为[test/lock_optimization_20261020](test/lock_optimization_20261020/PLAN.md)，实际效果以完成的实验报告为准。

拆分注册路径的D1只覆盖两段：第一段检查重复/容量，发布`registering`记录并预留active/pending额度；第二段在原生注册完成后检查关闭/取消并将任务提交到接纳队列。中间的Python→C++数据准备、原生mutex/DNS等待不持有D1。C1/C2仍保护原生共享状态，不能因为移出D1就宣称原生控制器无锁。

完全相同的重试在D1外等待同一个注册Future，注册未成功时不会提前ACK。冲突重试直接拒绝；尚未创建原生身份的失败回退预留额度；原生身份已存在的失败保留身份并发布错误。关闭先在D1内设置stop，锁外等未完成的注册退出，再关闭计算池及原生控制器，避免关闭后晚到的任务入队。取消和过期也检查提交边界。

拆分路径在D1外取native更新；锁内仍只更新共享表，但把移除记录的最后引用保留到锁外释放，避免批量析构Python对象或C++ Ticket时一直占用D1。正在注册的过期记录由其唯一注册者回退，不能由清理线程提前删掉导致额度重复释放。这个改动不能消除CPython GC本身的全进程暂停。

`native_register_release_gil`只在原生注册完成Python对象转换之后释放GIL，C++ mutex等待与DNS阶段不再占着GIL；返回/异常传播前恢复GIL。Ticket其他Python绑定保持原语义，没有宣称所有C++调用均已释放GIL。新增原生注册的queue/state等待及持锁时间戳，并保留预留、注册、提交三段Python时间戳。

H8仍保留单Pipe消息帧的写锁，本轮先补观测：IPC打包开始/结束、写锁请求/取得、父进程接收前后及解码结束。小型固定前缀携带发送侧时间戳，仍遵守原消息大小上限；不改变外部TCP协议。测试父进程GC事件另存`head_gc.json`，以判断宽泛的输出IPC时段是否主要由写锁或Python暂停造成。不得将`send/receive`合并区间直接解释成纯管道传输。

### 10.1 实验后的判断

三机每节点32搜索线程、并发256、预算200，6轮10k短测加2轮100k长测共260000正式请求均成功，Recall@10约97%、协议审计通过；406项回归通过。[报告、原始指标和时序图](test/lock_optimization_20261020/REPORT.md)记录了完整条件和限制。

- **D1临界区缩短成立，整体提速未成立。** 短测旧接纳平均持锁约0.2–0.3ms；新预留/提交各约0.02ms，但需再次取锁，接纳排队没有稳定下降。基线/仅释放GIL/完整拆分的短测QPS中位数为1275.91/1290.18/1178.85。
- **C1/C2注册等待不是已测秒级长尾的充分解释。** C++状态mutex注册等待均值约0.03–0.06ms。此观测只覆盖注册路径，不代表每个native控制操作都没有竞争。
- **H8不是本轮结果长等待的主要位置。** 写锁等待p99约0.002ms，整体输出IPC p99约140–162ms。一条诊断查询的208.33ms输出IPC中，202.61ms与父进程GC暂停重叠。保留消息帧写锁；不把接收/调度区间冒充纯管道或网络耗时。
- **长测没有支持默认开启。** 100k基线/拆分QPS为955.12/961.27，p99为1523.36/1599.76ms。两个优化开关保持false。既没有证明稳定吞吐收益，也没有达到等待低于100ms。
- **后续重点是保留对象和调度观测。** 100k基线父进程GC累计约19.06秒，但环仅保留最近1024事件，不能据缺失事件排除GC。压测保留所有完整结果的成本须与生产查询路径分开；优先设计紧凑终态、完整指标流式归档和长暂停保留，不禁用GC，不减少防重放期限。

数据节点的D1长尾也有实际GC重叠：Intel-32基线827.80ms申请至取得区间中，752.59ms与GC重叠；拆分版本578.56ms区间中重叠561.62ms。时间线见[节点锁/GC图](test/lock_optimization_20261020/lock_gc_timeline.png)。这解释了为什么缩短临界区后仍可能出现很长的“等锁”记录。

Python锁申请至取得的墙钟区间也可能包含GIL、OS调度或GC暂停。释放时间戳在退出with后记录，存在小量调度观测误差。Condition空队列等待后的重新取锁没有全部包含在首次取锁时间戳内。上述数据不应当被解释为每把锁、每种等待的完整独占归因。

## 11. 可选 C++ 查询运行时（2026-10-20）

`native_runtime.data_enabled/head_enabled` 启用后，在线查询不再走 Python 的 D1–D5、Q1–Q3 和 lane IPC 交接；旧实现保留用于对照。原生路径的新增同步器如下，不能把迁移解释为无锁：

| 位置 | 保护的状态与边界 |
|---|---|
| `DataRuntime::Impl::mu` | 查询/分支身份、保留与墓碑、容量及任务发布。接纳在此锁内调用原生控制注册；没有 Python API/GIL，但仍可能等待原生控制锁。 |
| `Pool::mu_` + condition_variable | 有界队列及计数。取出任务即释放锁；空闲线程休眠，搜索、路由和合并在锁外执行。 |
| `Record::trace_mu` | 单分支的接纳/计算/派发时间戳。不会用一把全局 trace 锁覆盖所有查询。 |
| `HeadRuntime::Impl::mu` | 活动/保留查询的索引、完成队列与容量。报告核验和合并不占用此锁；维护线程只取活动引用与到期项。 |
| `Query::mu` | 单 query 的报告、预算清单、心跳与完成状态。合并仍占用该 query 锁，其他查询可以并行。 |
| `Server::Impl::mu` | handler 到 reactor 的响应完成队列。socket 由 reactor 独占；实际 send/recv 不持有此队列锁。 |
| `ManagedTransport::gate_` | 接纳期间共享持有；暂停/替换运行时独占持有，保护 revision 和索引所有权。管理代码独立线程可获取 GIL，数据查询 handler 不获取。 |
| 原生控制器和 RPC 客户端旧锁 | 继续使用，职责见 C1–C6；没有因为新运行时启用而删除。 |

Python 只在 API 提交、最终结果转换、管理/监控与离线分析边界运行。头部/数据工作线程可在 Python 持有 GIL 时完成路由、搜索、派发和终态处理；最终 Future 交付仍要获取 GIL，单独记为 `api_result_delivery_ms`。`SearchShard::run` 的旧适配器仍有 Python callback，只有新 `run_value` 配合原生 runtime 的路径才满足无 Python 回调约束。

完整实现、配置与测试口径见 [DESIGN.md 第25节](DESIGN.md) 和 [本轮实验报告](test/cpp_runtime_20261020/REPORT.md)。

三机每节点32计算线程、并发256、预算200的两轮原生实验中，每查询累计分支就绪等待 p99 为4.64/5.80ms，对照为200.96/435.06ms。第二轮原生节点接纳锁申请至取得 p99 分别为2.75/3.60/2.16ms；这些墙钟区间仍可能包含线程调度，不代表纯 mutex 自旋耗时。搜索循环结束至释放计算线程平均约0.21/0.26/0.21ms，计算线程不等待分支树全部结束。

原生提交至合并完成 p99 为37–53ms，客户端 p99 仍为233–279ms，已完成结果在压测消费队列平均等待149–177ms。迁移使服务热路径摆脱 Python/GIL 和 lane IPC 交接，不能据此把剩余等待直接归因于某把锁；最终 API 转换、压测消费和归档仍需分别测量。完整回归418项（416通过、2跳过），正式四轮40000请求均成功；短测和热缓存结论不能外推到长期或冷SSD运行。

## 12. 内存结果暂存与独立分析

原生捕获模式不构造完整Python结果树：Future只携带完整C++结果的所有权句柄及时间戳；计费在C++完成批次取出时计算，MessagePack编码推迟至暂存区排空时进行并释放GIL。普通API仍走原结果解码。完成通知队列仅传递Future，负载循环先补充查询再保留原生句柄，不在此编码结果、计算召回或生成JSON。该模式没有新增数据节点搜索锁或跨节点扩展路径。

memfd暂存采用单写者；兼容同步查询线程时，用独立的`result_lock`保护整帧写入，不与数据节点锁共享。内存溢出时需要等待独立进程完成批量落盘才能复用缓冲区，这是有界容量背压，不是计算线程忙等。该背压可能降低压测补发速率，记录独立的`overflow_wall_ms`及批次时间戳。配置上限约束暂存原生对象的保守计费与编码帧，不代表整个服务RSS上限。完整设计见DESIGN.md第26节。

报告阶段不持有`op_lock`/查询接纳门；`benchmark_lock`只阻止多个benchmark暂存任务叠加，普通查询与配置事务不取该锁。关闭等待当前benchmark结束，避免在归档中途丢掉原生对象。原生结果对象不再依赖路由器/数据索引所有权，配置切换后仍可完成旧结果归档；相关真实TCP测试通过。实测与容量背压见[报告](test/memory_capture_20261020/REPORT.md)。

## 13. 可选动态控制单元（2026-10-21）

`native_runtime.adaptive_control`启用后，每个控制单元拥有独立Engine的queue_mu/state_mu/updates_mu与独立结果网络客户端。Ticket固定指向注册单元，单个分支的控制状态仍由同一线程按命令顺序更新；不同单元之间可并行，不共享原全局控制状态锁。

新增Fleet::mu保护单元集合、注册归属目录、扩缩容计数及生命周期。注册选择直到原生登记结束持有它，避免缩容/关闭与登记竞态；搜索循环和正常Ticket命令提交不取它。监测每20ms读取原子队列深度和网络pending，缩容只回收保留分支已空的单元。快照聚合与take_updates也需取得管理锁，监控不应高频复制所有保留报告。Fleet::close_mu保证并发关闭不会同时join监测线程。

扩容仍可能受接纳锁、单元管理锁、同机CPU资源或远端结果接收能力限制。缩容不删除未过期状态，按保留期延迟回收；不宣称无锁，也不保证线程增加必然提高QPS。测试及设计见DESIGN.md第27节。

## 14. 统一网络模块的同步（2026-10-21）

`dhnet::Client::Core::mu`保护循环集合、停止状态、选择与预留、扩缩容记录。提交时在锁内选择最少pending的循环并预留额度，然后在锁外编码和提交底层RPC。每循环pending和逻辑客户端total为原子计数，失败/完成都会回退。缩容只能移除pending为零的循环；预留使正在编码、尚未提交的请求也不会被缩容遗漏。完成回调不持有池管理锁，也不持有底层客户端提交锁。

`dhnet::Server::Core::mu`保护监听循环集合、排空标记及累计统计；socket和协议缓冲仍由各自reactor独占。不同循环通过共享原子连接计数执行总容量接纳，不用一把全局锁包住send/recv。既有handler队列锁、完成队列锁和应用层查询状态锁仍存在。SO_REUSEPORT负责新连接分配，不用于多个线程同时读同一个socket。

监测线程在管理锁下读取轻量池统计及创建新循环，因此线程创建期间可能短暂阻塞发送池选择或metrics；它不是完全无锁或实时调度器。缩容的close/join在管理锁外进行，避免等待回调完成时阻塞其提交。接收循环必须确认已停止监听，且连接数和handler活动/队列均为零后才能回收，避免异步停止监听与accept之间的竞态。

`close_mu`串行化外部关闭，停止标记和condition_variable唤醒监测线程。客户端回调中请求close只发布停止信号，避免网络线程join自身；对象销毁仍必须由外部所有者执行，不能在它自己的回调中销毁。正常关闭完成后拒绝新提交，已接受请求通过原有成功或失败回调结束。该策略没有删除消息帧锁、查询状态一致性锁或管理配置门，也不把条件变量休眠称作CPU忙等。

新模块与第13节动态控制单元当前互斥启用。测试及容量、线程总数的计算见DESIGN.md第28节。

## 15. 父报告与子派发ACK解耦（2026-10-21）

`native_runtime.early_parent_report`开启后，完成控制器不再用未结束派发数阻止本地报告发布。本轮消除的是协议状态依赖，没有删除共享状态锁，也没有添加等待子任务的计算线程。计算线程在本地循环及报告提交结束后按原有机制释放。

子分支登记、本地完成、派发回调、结果ACK均通过既有命令队列进入Engine，由单控制线程在`state_mu`保护下更新。队列锁只用于发布命令；实际网络收发异步执行。报告发布后保持不可变，迟到派发计数更新单独的保留记录；迟到失败使用独立通知状态，ACK不能重复释放原任务额度。未结束派发记录延后回收，回调刷新保留时间。

顶层新增已声明子分支目录受单query的`Query::mu`保护，与报告、租约节点和完整性判断保持一致，避免父报告与子声明并发到达时提前合并。不同query仍分别加锁。离线指标补齐在查询测量区间之后从节点快照读取，不在搜索热路径中等待计数完整。

保留同一分支一次一条在途控制消息的限制，因此一个已经在途的心跳仍可能推迟结果发送；本轮没有将这种等待误称为已消除。控制命令排队、网络发送容量、OS调度及顶层状态锁也仍需观测。三机实测父分支完成处理至报告就绪p99从83–102ms降至约0.024–0.027ms，但整体p99没有改善，详见DESIGN.md第29节及[test/early_parent_report_20261021](test/early_parent_report_20261021/REPORT.md)。

## 16. 原生控制注册的短临界区与队列提交（2026-10-22）

`native_runtime.queued_control_registration` 启用时，C++接纳不再持有runtime状态锁同步等待controller状态锁。新增每个controller lane的 `registration_mu` 只保护身份预留与endpoint弱引用缓存，不在该锁内等待队列/状态锁或DNS。注册命令先入FIFO，Ticket的后续命令才允许发布，避免分支先完成后注册。到期时控制线程删除身份预留；快照可能暂时不包含尚未消费的注册命令，需要结合 `native_register_enqueued_ns/consumed_ns` 判断控制排队。

原同步路径保留作A/B对照；搜索循环没有新增串行锁。协议、重试、取消、关闭和重复身份并发测试覆盖新路径。实验与结果见 `test/control_registration_20261022/REPORT.md`。

## 17. 连接预热与线程放置补充（2026-10-23）

`native_wire.cpp`的有界建连池复用`native_pool.hpp`任务队列：提交/取出任务和状态计数时持锁，socket创建、connect及可选CM事件推进在锁外运行。未发布的连接由建连任务独占，使用release/acquire完成标志交给I/O reactor；不会让搜索工作线程持锁等待握手。收发仍归唯一reactor，连接迁移只发生在完整帧边界。

`native_network.cpp`中的连接均衡与缩容状态受模块mutex保护；迁移请求交由原reactor执行。draining连接优先迁出，避免连接数接近时缩容永远停留在标记状态。不同网络handler在配置的CPU集合内调度，不把多个单handler池强制放到同一CPU。

独立压测模块新增最终文件同步屏障，只让消费线程在全部网络生产者完成后执行`fdatasync`。条件变量等待释放其mutex；服务端查询、路由、网络收发不使用这把锁。每lane结果队列仍有独立短锁和硬内存/条数上限，超过容量明确使测试失败。

这些变更没有删除协议去重、查询终止、预算和任务队列所需的同步，也没有给RaBitQ距离计算或只读图遍历新增全局锁。源码、负结果及回归见[本轮报告](test/network_parallel_20261023/REPORT.md)。

## 18. 无上限扩容与搜索线程扩展对照（2026-10-23）

顶层C++注册表mutex只用于复制自身计数；读取网络模块指标移到该锁外。以前监测线程可能持有顶层锁等待网络模块的扩容临界区，使查询submit、报告登记和merge完成同时受影响。新快照不承诺跨模块原子一致性，不能拿它直接代替协议终止条件。

接收端新lane的监听器/工作线程准备移到模块mutex外，发布lane时再短暂加锁。原有接纳与连接所有权规则保留；关闭先join监测线程。持续空闲确认后逐采样周期启动回收，不再为每个lane重复等待完整idle_ms；实际回收仍受完整帧、无handler和连接迁移条件约束。

搜索批次统计原来通过共享原子double执行CAS。新增可选 `metrics.expansion_counter_stripes`，1保持原密集布局，多个stripe按线程分散写入并按缓存行分隔，读取时相加。它仍使用原子操作处理线程哈希碰撞；请求active/peak计数仍为全局原子量。不能把“无互斥锁”与“无缓存一致性争用”混为一谈，也不能在实测前断言统计是主要瓶颈。`counter_update_ms`记录finish_native区间，真实收益见[本轮实验](test/search_scaling_unbounded_20261023/REPORT.md)。

本轮消融没有证明128组分散计数优于默认1组，故未将它设为默认优化。跳过已见估距只操作查询私有的seen集合；有预算的ID直接寻址表在索引加载时构建，发布后不可变，搜索读取不新增锁。660万次正式请求的分支统计和预算检查通过；不能由此推导所有网络长尾已消除，尚有reactor内部等待需进一步定位。

2026-10-27实测补充：正常诊断组查询合并锁均值0.0448ms、p99 1.2737ms，完成注册表锁均值0.0304ms、p99 0.8755ms；三个数据节点各64搜索区间实际重叠。异常组尚无这些细分时间点，不将正常组证据作为完全排除异常锁竞争的依据。详见[本轮报告](test/resource_parallel_20261027/REPORT.md)。

## 19. 查询级逻辑区域任务（2026-10-27，R001-A10/R010）

`peer.region_tasks`启用后，节点身份注册表mutex原子判定新区域执行器或合并授权，同时保护源节点首次启动权与创建的竞态。它仍包含候选接纳、控制注册等内存操作，没有宣称整个接纳路径无锁。普通目录消息只在该锁内查找/保留查询上下文，目录集合合并在锁外执行；源权限登记与原子创建的互斥仍保留。

每个`RegionActor::mu`保护单个查询、单个逻辑区域的授权FIFO和scheduled标志。计算线程取授权和交还执行权只获取这一把区域锁，不再获取全节点注册表锁；图搜索、SSD读取、RaBitQ和RPC等待不在区域锁内。接纳时锁顺序为身份注册表→区域授权队列→候选收件队列；执行端不反向获取身份注册表。线程池提交仍有自己的短队列锁。

`RegionInbox::mu`按查询/区域独立，保护候选去重、容量裁剪和批量取走。`RegionVisits::mu_`按查询独立，保护目录的写时复制；搜索读取原子发布的不可变快照，不持目录写锁。共享指针的原子发布不等于其底层实现一定lock-free。实际展开领取仍使用预算有界CAS表，未给向量读取增加全图互斥。

`region_release_lock_enter_ns/acquired_ns`测量授权收尾时的区域锁等待；`region_worker_return_ns`标记区域工作函数返回前。此前`compute_released_ns`位于内部搜索调用返回处，不能覆盖旧区域包装函数之后等待全节点锁的时间。完整线程池归还还包括包装函数返回和池计数操作，图表明确使用已记录的返回标记。

缩小锁范围的独立对照未提高QPS，不能将这些结构变化写成已验证吞吐收益。另有主动广播开关与去除重复广播的对照；根查询尚无其他分支时无需通知空闲节点，向已声明/已打开区域补充候选也不重复广播同一目录。广播使用独立有界客户端，满时计数丢弃，不挤占搜索派发。完整正负结果见[本轮报告](test/region_tasks_20261027/REPORT.md)和[DESIGN §47](DESIGN.md#query-region-tasks)。

## 20. 查询级并行接纳（2026-10-27，R002-A05）

§19描述初版保留路径。`peer.concurrent_region_admission=true`与`region_tasks=true`同时启用时，重型接纳操作改由`QueryContext::admission_mu`保护，节点级mutex仅保护索引查找、短容量预留和记录提交。不同查询不再争用同一接纳临界区。源保留权限的读改写与执行器首次创建必须使用同一个查询锁；已经启动的源任务继续运行和合并候选。

禁止持节点mutex等待查询mutex。顺序是节点查找后释放，取得查询mutex，再短暂进入节点提交或区域授权/收件箱锁。计算线程只用独立区域锁处理FIFO，不进入查询接纳mutex。图、向量、SSD读取没有新增全局读锁。

预留额度由RAII管理；幂等重试先后两次检查，重试不能重复消耗全节点容量。`admitting`用于关闭/排空观察，`reserved_admissions`用于精确接纳限额，两者不可混用。关闭设置stopping并以条件变量等待正在接纳的操作完成；不是忙等。目录生命周期保留共享引用，防止过期回收与接纳竞态产生两个查询上下文。

本轮同配置对照的接纳锁累计等待10.82→0.0153ms/query，接收handler累计排队166.07→3.13ms/query，QPS 6646→16705；这与§19此前“仅缩小区域收尾锁”没有提速是不同改动。锁等待时间点已移到真正取得mutex处，不能与旧版错误包含其他处理的时间点直接混比。首组对照、重复测试及副本审计见[报告](test/region_admission_20261027/REPORT.md)和[DESIGN §48](DESIGN.md#query-scoped-admission)。

## 21. 副本流水线和原生加载（2026-10-28，R001-A13/A14）

流水线每版本状态锁保护块区间的WRITING/DONE登记、清单封口、取消和发布资格；hash及pwrite移到锁外。目标维护写入槽使用有界信号量，最多`replica_write_workers`个任务写入；等待不占查询计算线程。取消仅标记，清理等待在写计数、维护future和原生查询引用全部释放，禁止关闭仍在使用的fd。

原生准备释放GIL，在目录锁外完成数组读取、ID映射和记录/依赖验证；最终仅在overlay目录锁内重检取消/版本并发布不可变快照。Python版本状态锁覆盖发布与READY登记，避免退役在其中插入后出现晚到安装。搜索读取仍使用不可变版本共享引用，不新增全图读取锁。

控制器快照在短锁内抓取有界历史引用，后台C++选图不持控制器锁；Python输入/输出转换仍需要GIL。完成时短锁重检负载、epoch、容量状态和入口重叠，创建/接收端另检实际预留。一个在途计划避免队列无限积压。后台日志落盘仍有维护服务锁，未声称整个维护控制面无锁或无停顿。实现与验证口径见[DESIGN §50](DESIGN.md#replica-pipeline-native)。

## 完整区域后台规划的状态锁（2026-10-28，R001-A15）

`community.PlanWorker.lock`只保护源端单个规划任务的ID、参数、future和closed状态；同ID同参数重试幂等，忙时拒绝另一个任务，保留最近一份结果供重复poll。C++闭包/PPR、pread、future等待和线程join均在锁外；搜索不访问该锁，也不在向量读取路径新增锁。源端规划使用独立单线程，维护RPC只提交或查询已完成future，不在handler里等待规划。控制器仍在发布前复核计划新鲜度、负载和epoch。48项回归及在线证据见[本轮报告](test/hot_entry_community_20261028/REPORT.md)。

## R001-A29：异步尾部执行新增同步器

`DataRuntime::Impl::pull_mu`保护尾部上下文准入计数、待发送队列、每源在途批数及关闭标志。保存/恢复搜索前沿、距离计算、网络submit、源pread和结果解析均在锁外；该锁不跨越网络等待。`pull_idle`只用于运行时关闭时等已接纳上下文释放，正常搜索线程不在它上面等待。每个已发送批次的`atomic<bool> once`防止同步提交失败与网络回调重复完成。

尾部计算池、可选源读服务池分别复用独立`Pool::mu_`及条件变量，只保护各自有界任务队列；任务取出后释放锁。副本区域执行权在暂停期间保留，但不保留物理工作线程，也不持有这些队列锁。网络回复通过尾部队列移交同一上下文的唯一执行权。源读deferred响应只持有查询目录引用，不创建源搜索actor或消费搜索预算。详细实现/测试见[异步拉取研究](research/R001_ASYNC_RDMA_TAIL_PLAN.md)。

2026-11-02新增可选主池帮助：`help_active`以atomic CAS限制借用额度，`Pool::submit_if_idle()`复用主池已有`mu_`，在同一个短临界区内完成压力判断与入队，避免先读metrics再入队造成超额接纳。未增加mutex，不持池锁执行搜索或等待网络；RAII释放额度。不支持抢占或已入尾队列的任务窃取，普通请求仍可能在新到达时排队。候选策略年龄调整由唯一续跑线程操作，不新增候选级锁。证据见[报告](test/pull_policy_help_20261102/REPORT.md)与[DESIGN §62.5](DESIGN.md#async-rdma-tail-runtime)。

2026-11-02补充：可选跨query拉取合批复用原`pull_mu`保护pending队列、逻辑inflight、共享响应字节预留和关闭标记；`pull_wake`是同一mutex上的新条件变量，不是新增互斥锁。等待时释放锁，网络提交/源pread/行收集均在锁外。共享响应引用计数保护只读缓冲寿命，最后释放时短暂获取pull_mu归还额度；release_tail在获取pull_mu之前释放响应，避免重入。源仍逐query使用既有身份/区域接纳锁，不能把多个query的授权合成一个。新增统计用atomic，未新增覆盖完整检索的锁。见[本轮报告](test/pull_coalesce_20261102/REPORT.md)。

2026-11-02单边READ新增同步器：`dhonesided::Client::Impl::mu`与条件变量只保护有界待提交队列、连接控制回复事件、保留数和关闭标记。QP、READ操作列表、CQ推进与阶段转换由一个I/O线程独占；提交READ、解析记录/码、调用续跑回调都不持该队列锁。source `Impl::mu`仅保护有界QP连接表的建立及其指标读取；正常远端READ由RNIC执行，不获取该锁。MR/目录在运行时固定，销毁QP后回收。每批授权仍复用原query context/admission锁，未声称控制面无锁；主/尾搜索线程不为READ完成忙等。详见[单边读取报告](test/onesided_pull_20261102/REPORT.md)。

2026-11-02缺失码过滤未新增互斥锁。精确目录回调持有固定overlay/查询码的生命周期引用，仅在该查询挂起期间由I/O线程读取；在数据交给唯一续跑执行者之前完成所有查找。runtime指标新增原子累计计数。诊断查找可独立关闭，不能在基线中无意引入额外查找成本。取消/关闭复用原CQ排空与续跑所有权规则，详见[本轮报告](test/onesided_missing_20261102/REPORT.md)。

2026-11-02四项尾部优化：共享计算模式新增一个有界尾部队列，但复用`Pool::mu_`，临界区只涉及领取/入队、活动计数及CPU信用更新。线程CPU计时、搜索、READ等待和回调均不在池锁内；不会同时领取同一上下文。延迟精排复用已有挂起所有权与单边I/O队列，不增加每向量锁；离线入口分组规划也不接触在线查询锁。共享模式的信用在任务完成时记账，可能短时超配，无抢占保证。真实RNIC关闭/取消、动态网络数值指标和三机实测见[本轮报告](test/tail_four_20261102/REPORT.md)，机制见[DESIGN §63](DESIGN.md#tail-four-20261102)。


## 完整子图规划流水（2026-11-03）

- `PartitionPlanWorker.lock`：保护维护job、sequence及最近交付结果；后台图枚举、I/O和复制不在锁内。一个规划线程和有界Queue实现交付背压，读线程不领取查询任务。
- `ReplicaPlanningGraph.mu_`：保护邻接缓存目录、预取队列和关闭状态；pread在锁外，等待future在锁外。缓存不淘汰活动引用；构建所有调用结束后join读取线程，再释放整个缓存。
- 本地编排的计划Queue和发布线程只用于维护路径；在线路径在源规划线程与顶层现有构建线程之间交付。失败撤路由/排空复用既有副本生命周期锁。
- 原子取消标记与一次性promise交付避免读取完成/取消导致重复结果。新增同步均不包围距离计算、前台SSD搜索或查询网络等待。详细合同见[DESIGN §64](DESIGN.md#subgraph-pipeline-20261103)。

### 2026-11-03：增长复制维护通道

`ReplicaService._growth_pace_lock` 仅串行预约本源下一次复制 chunk 的发送时刻，使多个增长复制槽共享 `copy_mib_per_s` 总额；锁内只有时间和字节算术，睡眠、网络与 SSD I/O 全在锁外。已有 receiver 每版本锁保护增长文件高水位、写入所有权、封口和回收；原生记录生产者独占一个追加文件，无新增查询锁。ID 批次队列和复制线程池使用标准库维护同步，满队列只背压规划线程。顶层已有控制锁保护整组预约及其向已发布/排空占用的转换。


### 2026-11-03：持续负载的QueryContext回收阻塞（R002-A08）

已用栈确认：Intel-31的16个接收handler等待同一DataRuntime身份mutex（9个admit_concurrent、7个handle），维护线程在QueryContext→RegionActor→RegionInbox析构中进入jemalloc/madvise。该锁保护身份与租约相关状态，不是SSD读锁或距离计算锁。原来仅Record缓冲移到锁外，QueryContext仍在锁内析构，是遗漏的重型释放路径。

现用extract将context的map节点及对象移到锁外销毁，每轮至多检查`native_runtime.lifecycle_cleanup_max_contexts`个到期context（默认256）。唯一性、引用计数和到期校验保留；不删除该锁。新增三个轻量回收计数见[设计](DESIGN.md#bounded-query-context-reclamation)。600秒对照成功QPS+103%、持续断供显著减少，但锁内其它批量更新仍有约197ms峰值，需继续分解；不能把R002全部关闭。[报告及限制](test/admission_stall_20261103/REPORT.md)。

### 2026-11-03：顶层锁计时与独立析构池（R002/R004/R006）

本轮[报告](test/no_lb_qps_recovery_20261103/REPORT.md)给出顶层注册表、报告查表、逐query和完成登记的实测等待；短测均值约0.0016–0.0413ms，不支持将主要吞吐损失归因于一把顶层“大锁”。无副本版本的完成路径跳过`observation_mu`，仍保留实际副本报告的观测同步以及身份/终态/预算必需的锁。

开启`lifecycle_async_reclaim`时，每数据runtime新增独立`Pool`实例的队列mutex和条件变量；只保护有界退休批次入队、领取及关闭。重型Record/Context析构、allocator释放不持该池锁，不占用接纳维护线程。节点身份mutex仍负责移出记录、墓碑、活动计数与引用条件；每轮Record/context/timer有界。队列满时ACK继续记账、到期身份暂留，不能将该队列锁解释为搜索串行锁。关闭顺序为停止维护生产者→join维护线程→排空析构池。峰值持锁/析构时间仍保留观测，异步析构峰值不再等同于维护线程卡顿。

连接空闲探测修复没有新增锁；连接状态仍由原reactor独占。`MSG_PEEK`成功但暂不可读不刷新真实活动时刻，避免服务端已到期的未使用连接在下一次分配时被误判为刚使用过。定向回归与长期验证边界见报告。


## 2026-11-04：入口回源预算统计（R001）

新增`cpp/entry_return_counters.hpp`使用每入口16个缓存行分片的无锁64位原子量，将回源次数和剩余预算和打包为同一原子快照。完成线程只做原子更新，不新增mutex、文件I/O或Python回调；冲突通过CAS解决，不能据此声称没有缓存一致性开销。后台每5秒导出快照、计算均值，沿用既有副本生命周期RLock更新关闭入口和发布不可变目录。查询统计写入不获取该维护锁；系统原有query锁与observation_mu仍存在。验证见[报告](test/covered_entry_admission_20261104/REPORT.md)。


## 11. 普通远端拉取的锁、信用与完成进度（2026-11-04）

[报告](test/remote_pull_first_20261104/REPORT.md)、[设计](DESIGN.md#remote-pull-first-20261104)。`native_runtime.cpp` 的 `pull_mu` 保护按源队列、在途额度和挂起上下文计数；搜索计算不持该锁。新派发线程按源 FIFO 取最多64项，锁外组包，完成线程只归还额度/提交续跑/通知。保留必要共享状态锁，而不是把队列驻留称为持锁。

短诊断旧路径：拉取信用队列47.947ms/query，已计量锁等待合计47.993µs/query；按源FIFO+8引擎后分别0.455ms与4.355µs/query。计量仅覆盖所标记的入队、选择、完成路径；dispatcher条件变量的休眠不计入锁持有。CQ分片每引擎有独立生产者队列锁和唯一QP所有者；选择引擎时只读原子pending提示，真正接纳仍在队列锁内。全局预算、去重和源授权同步未删除。新增并发排空/关闭回归检验取消时不会遗留挂起执行器；尚不能据本轮排除其他路径或长期负载下的锁问题。

<a id="candidate-registry-reserve-20261105"></a>
## 查询目录锁内扩容与非阻塞候选交付（2026-11-05）

本轮保留共享目录、查询局部首次接纳、候选收件箱和任务队列的互斥保护。独立线程修改自身状态不能替代查询身份、唯一授权和终止计数的原子性；本轮没有给向量读取增加锁，也没有持这些锁等待网络回执。

`nonblocking_start`合并预约和首次任务提交，源按原预算冻结授权后继续搜索；目标在查询局部锁内保证唯一分支，竞争失败合并候选并退款。源在安全点结算，确需等授权时挂起上下文、释放计算工作线程。`early_result`将本地预算零报告与最后候选交付解耦，顶层仍验证交付闭环；每目标单在途候选包复用局部状态锁，有界合并期间不持锁执行网络等待。额外完成池和接纳池的实验无整体收益，推荐关闭。

定位到的数据目录插入临界区最长257.5ms、顶层约97ms来自受监测的哈希扩容路径。新增`native_runtime.registry_reserve_entries`在启动接纳前预留桶容量，0保持原行为；各表按原活跃/保留上限裁剪。它将扩容工作移到服务初始化，并没有删除身份锁、增加准入容量或改变预算。计数`registry_rehashes/ns/max_ns`是发生桶数变化的插入墙钟区间，包含插入和扩容，不是纯CPU时间或所有线程累计锁等待。

ABBA同负载对照0/2M预留，p99=214.84→156.50ms，QPS=19937.52→19703.50；预留组受监测扩容为0，不能称所有锁等待已清零或吞吐已提高。完整容量补测仍见负载开始时两次约100ms入口派发，同期网络扩容，目录维护锁峰值仅0.907ms；该残留不能继续归因于已消除的扩容。当前p99主要是计算就绪队列，实际CPU搜索并未成为E2E多数。

图表、配置、失败与回归记录见[完整报告](test/candidate_stalls_20261104/REPORT.md)，容量边界见[DESIGN](DESIGN.md#candidate-registry-reserve-20261105)。长时间过期回收、未测容器的增长与网络启动仍需独立检查。

### 同轮p99复核与计量修复补充（2026-11-05）

首跑队列≥100的最新秒级快照中，拥塞节点的64个计算工作线程均活动；续跑CPU实际约20–28%，不能把tail_fraction=0.8解读为80%线程被尾任务常驻占用。工作池队列mutex只保护取任务和计数，任务在锁外执行；执行片段CPU/墙钟约99%，当前证据优先指向首跑服务供需失衡，不是普遍的ACK持线程阻塞。该比值不能排除CPU自旋、短时线程调度或其他锁尾部。

极慢样本的pull派发66.551ms、授权53.580ms是明确的等待区间，现有证据不足以归因某个具体mutex。pull队列锁平均等待0.299/0.421/0.216微秒/次，不将其均值当作无长尾证明。下一步需要请求级信用/提交/handler/回复/回调时刻，保持各机独立时钟口径。

`native_head.cpp::apply_delivery_metrics()`补齐移动报告漏掉的最终候选通信统计，使用原查询锁，无新锁或网络等待；本机5项回归通过，未四机测性能。历史回调滞后统计不完整，不再据其宣称等待减少95.12%。详见[p99复核及计量报告](test/candidate_stalls_20261104/p99_diagnosis/REPORT.md)和[DESIGN](DESIGN.md#candidate-delivery-metrics-20261105)。

## 2026-11-09：查询驱动增量目录与持久化流水（R001/F30）

原实现的 `pwrite/fdatasync` 已在 `Cache::Impl::mu` 之外；不能把刷盘时长直接解释成搜索持锁等待。原目录的共享锁仍覆盖增量查找，后台每次唤醒又取得独占锁扫描100槽，且最后一个记录/量化码引用可能在锁内析构。

本轮保留串行/锁式路径作为开关对照，新增：

- `indexed_directory`：65536个原子桶指向只追加的不可变提交项。文件提交成功后release发布，读取acquire遍历，Cache存活期间不回收桶节点；查询持有Entry/File共享引用。无命中时先检查8KiB只增不减的缓存布隆位，位不存在即可避开目录锁，误判仅增加一次有锁查找，不允许假阴性。临时缓存的正命中仍在共享锁内获取pin，防止与淘汰竞态。
- `maintenance_interval_ms`：合并扫描唤醒；非零时临时记录只有首次达到两次展开才通知维护者，统计仍逐次原子更新。排序和淘汰均使用不可变的计数/时间快照，不能在比较器中重复读取正在变化的原子值。移除记录保留在局部retired列表，离开目录锁后析构。
- `persistence_pipeline`：一个准备线程和一个顺序写入线程。有界FIFO只保护取放及批次/字节账目；网络、路径校验、打包、文件写入、两次fdatasync、等待与批次析构均不在队列锁/目录锁中。准备中的批次及已排队批次按实际日志帧预占beta；只有提交后才对搜索发布。
- 连接闭包可以依赖前一待提交批次，维护私有pending目录用于去重，搜索不能从此目录读取未提交的增量；FIFO提交确保依赖先持久化。失败后封口且后继帧不再写入。关闭顺序为停止准备→完成准备退出→排空写者→回收目录，join期间不持目录或队列锁。
- `lock_sample_every` 控制共享查找锁的采样；输出读锁采样数/等待/持有、写锁次数/等待/持有、扫描次数，以及准备、队列、data flush、commit flush和发布的累计时间。累计跨线程时间不能直接作为互斥的端到端占比。

默认功能仍关闭，原路径可选。本机暂停写者时读请求仍能完成、第二批可完成准备；连接点只记一次、待提交连接点再次接纳和恢复测试通过。最终35项本机回归及3项RNIC回归通过，四机20组3600万请求账目通过，见[完整报告](test/demand_pipeline_20261109/REPORT.md)。

同二进制原增长/目录优化的S30正式均值：QPS17043→22460，读锁采样866803→155010/600K查询，采样平均等待1.688→0.313µs；被计量的所有独占目录操作累计等待2466→26.83ms。封口后零READ/写盘，原路径QPS17530、目录优化22753，后者有锁acquire采样为零。证明重复目录锁路径是可消除成本，不意味着每次损失都是互斥睡眠、所有查找都无锁，或整个QPS差异仅由锁解释。完整流水未获得额外稳定吞吐收益，仍可配；临时记录pin锁、身份锁及有界队列锁继续保留以保障竞态安全。S30优化后尾样本主要仍为目标计算就绪130.964ms，质量与整体负载均衡不因此验收。
