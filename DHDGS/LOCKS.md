# DHDGS 当前实现的锁与同步机制清单

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

## 11. 原生控制注册的短临界区与队列提交（2026-10-22）

`native_runtime.queued_control_registration` 启用时，C++接纳不再持有runtime状态锁同步等待controller状态锁。新增每个controller lane的 `registration_mu` 只保护身份预留与endpoint弱引用缓存，不在该锁内等待队列/状态锁或DNS。注册命令先入FIFO，Ticket的后续命令才允许发布，避免分支先完成后注册。到期时控制线程删除身份预留；快照可能暂时不包含尚未消费的注册命令，需要结合 `native_register_enqueued_ns/consumed_ns` 判断控制排队。

原同步路径保留作A/B对照；搜索循环没有新增串行锁。协议、重试、取消、关闭和重复身份并发测试覆盖新路径。实验与结果见 `test/control_registration_20261022/REPORT.md`。
