# 指标记录与测量口径

本版本在构建器、图搜索、数据服务、TCP 客户端和 benchmark 中记录指标。默认开启逐批 trace，控制项统一在 `configs/dhdgs.yaml`；单条查询输出不含在线不可知的 recall，benchmark 使用精确真值补充。

## peer 异步分支口径

默认 `search.execution: peer` 的逐查询 `rpc_count` 为入口提交和数据节点分支 RPC，`router_expand_rpc_count` 应为 0。`control_rpc_count` 包括生命周期通信，`control_by_kind` 进一步区分 heartbeat、result（含最终诊断数据）、lease、cancel；不能把整个控制通信量当作轻量心跳。`wire_bytes` 为数据请求/ACK 字节，`control_wire_bytes` 为控制及结果字节，`all_protocol_bytes` 为两者之和，均不含 TCP/IP 头和重传分组。仅成功完整往返可完整计入字节，发生错误/重试需查看 `metrics_complete`。

`visited_nodes` 为查询有序访问路径；`node_revisit_count` 应为 0；`visited_node_candidates_dropped` 记录因仅属于先前访问节点而被过滤的候选；`remote_candidates_dropped_after_forward` 记录父节点派发后不再处理的远端候选（其中可能已转交后继，不等于全局丢失）。

`branch_count` 记录分支数；`unique_expanded`、`duplicate_expansions` 区分唯一及重复扩展；`branch_unused_credit` 为未使用预算。`branch_reports` 保存分支树、决策原因/概率/效用及各分支最终结果。分支都完成不代表所有候选都展开，需结合预算与 `pending_at_stop`。

peer 的延迟分解为 `routing_wall_ms`、`peer_execution_and_control_wait_ms`、`head_merge_and_lifecycle_ms`；最后一项包括清理保活状态。各服务器 pread/计算累计时间可并行重叠，不能除以 query 延迟得到可相加占比。纯网络与物理 SSD 占比仍为 null。下面按逐批 RPC 解释的阻塞时间分解适用于 `search.execution: reference` 历史基线。

## 输出与运行

```sh
.venv/bin/dhdgs benchmark --config configs/dhdgs.yaml
```

每次测试创建独立目录，保存：

| 文件 | 内容 |
|---|---|
| `resolved.yaml` / `config_provenance.json` | 实际配置、来源及哈希 |
| `report.json` | 整体吞吐、延迟分布、召回、失败、节点负载、构建阶段、逐查询及 trace |
| `queries.jsonl` | 一行一条查询，含结果、真值、完整指标和可选批次/发现树 |
| `queries.csv` | 查询延迟、recall、通信/读取/计算计数、跳数及延迟分解和占比 |
| `query_stage_means.csv` | 整组测试中各阶段的平均每请求耗时、样本数与占比 |
| `per_query_stage_means.csv` | 同一 query_row 在多次重复测量中的阶段均值 |
| `nodes.csv` | 按数据节点累计的查询资源消耗 |
| `process_load.csv` | 进程 CPU、RSS、存储 I/O 增量及测量窗口 |
| `build_stages.csv` | 索引构建各阶段 wall/CPU 秒数；旧索引没有这些数据时不生成 |

`metrics.detailed_trace` 控制是否导出批次 trace 和发现树；基础计数和时间归因始终采集，内部服务 trace 仍随 RPC 传送。关闭详细导出不能视为关闭所有插桩。`resource_sample_interval_ms` 控制进程采样间隔，`resource_max_samples` 限制时间序列缓冲区长度，长测试只保留最近这些样本。比较性能时保持插桩设置一致。

`experiment.benchmark_concurrency` 是并发查询数，每个并发工作线程拥有一个 Engine；当前会复制路由/元数据，local 模式还会创建独立分片实例及缓存。TCP 模式这些 Engine 访问同一组数据服务。`benchmark_repetitions` 控制查询集重复次数，`warmup_queries` 为每个工作线程的预热请求数。预热使用所选查询集循环取样，不用于调参。

本机多进程测试将 `network.transport` 设为 `tcp`、节点 host 设为 `127.0.0.1`，然后执行：

```sh
.venv/bin/dhdgs local-cluster-test --config /path/to/experiment.yaml
```

该命令按 YAML 启动独立的数据服务，等待就绪后执行 benchmark，最后停止本次启动的进程。不存在索引时先按配置构建；已有索引必须匹配。输出还包含各服务日志和 `services.json`。现有端口被占用时报告失败，不终止其他服务。它测试回环 TCP，不代表真实多机性能。

## 阶段平均耗时

`report.json` 中的 `query_timing_summary` 汇总成功测量请求；`per_query_timing_means` 按 query_row 汇总重复请求。CSV 同步导出为 `query_stage_means.csv`、`per_query_stage_means.csv`。聚合在 benchmark 查询计时窗口之外进行，预热不纳入平均值。

`wall_stages` 是端到端互斥阶段；完整分解且阶段集合一致时，`wall_means_additive=true`，各 `mean_ms` 相加等于 `mean_latency_ms_for_wall_samples`。缺失、不守恒的分解不参与该汇总，并记录 `wall_unavailable_success_count`。混合不同阶段集合时，各字段按自己的 `sample_count` 求均值，不可直接相加。

`mean_query_fraction` 是每次请求阶段占比的平均值；`fraction_of_total_latency` 是累计阶段时间除以这些样本的累计查询时间，二者不能混用。`cumulative_work` 单独汇总 pread、RaBitQ、精确距离、服务 wall 时间及线程 CPU 时间，均值按实际有记录的成功且资源指标未标记不完整的请求计算。这些工作项可能并行或互相包含，不提供端到端延迟占比。未知值保留 null，失败和缺失不补零。

peer 的 `head_merge_and_lifecycle_ms` 包含保活取消及回调服务关闭，不能全部解释为 top-k 合并计算；当前入口 HNSW 与入口估距合并在 routing 阶段中。

## 逐查询指标

每条记录有唯一 `request_id`、`query_row`、`query_id`、重复轮次、状态、结果 ID、精确真值及 recall。TCP batch 另有 `rpc_request_id`，导出 trace 中关联 query ID。

| 字段 | 含义 |
|---|---|
| `latency_ms` | Engine 调用内从预处理/路由到搜索及合并结束的 wall 时间；当前没有客户端到独立路由服务这一跳 |
| `coordinator_thread_cpu_ms` | 协调线程实际 CPU 时间；不包含同进程 RPC 工作线程的 CPU |
| `server_thread_cpu_ms` | 各成功扩展响应报告的服务线程 CPU 时间累计 |
| `rpc_count` / `rpc_retries` | 数据扩展 RPC 尝试数 / 已知重试数；含首片请求，不含测试用 metrics 管理请求 |
| `request_bytes` / `response_bytes` / `wire_bytes` | 已完整发送/接收的 MessagePack 帧字节，含 4 字节长度头与 trace；不含 TCP/IP、重传分组和失败的部分帧 |
| `file_read_bytes` / `pread_calls` / `pread_ms` | 应用发起的文件读取量、次数和调用 wall 时间；OS 页缓存命中仍计入 |
| `cache_hits` | 应用层节点记录缓存命中次数；不同于 OS 缓存 |
| `rabitq_evaluations` | 入口和数据服务候选估距次数；重复估距计入 |
| `rabitq_ms` / `route_rabitq_ms` | 数据服务/路由入口 RaBitQ 估距 wall 时间累计 |
| `exact_distance_evaluations` / `exact_distance_ms` | 扩展时原向量距离计算次数/累计 wall 时间；排除 self 结果不撤销其计算成本 |
| `expanded` / `scanned_edges` | 全局派发的扩展顶点数 / 成功响应扫描的邻接边数 |
| `frontier_dropped` | 有界候选队列淘汰数量 |
| `executor_queue_ms` | RPC 任务从提交到工作线程开始执行的累计等待 |
| `candidate_queue_ms_sum` | 已派发顶点在候选队列中的累计停留时间，不是 query 总排队延迟 |
| `fallback_events` | 备用入口、触发时刻、触发前扩展量 |
| `fallback_descendant_expansions` | 首次发现路径归属于备用入口树的已完成扩展量；不是反事实意义的额外成本 |
| `improved_topk_batches` | 使当前第 k 名距离阈值改善的响应批次数；初始不足 k 阶段也会计入 |
| `per_shard` | 相同计数按执行分片归集 |
| `metrics_complete` | 失败或重试导致无法完整归集资源时为 false |

查询出现异常时保留错误类型、错误信息和可获得的部分指标。吞吐分母保留其消耗的测试时间，召回按零计入；失败指标不能当作完整资源总量。未知指标用 null：目前包括整体 admission/server 排队、HNSW 内部距离计算次数、纯网络延迟占比和物理 SSD 延迟占比。

## 延迟占比：避免并行重复计时

`latency_breakdown_ms` 将协调器的 query wall 时间划分为互不重叠的部分；`latency_breakdown_fraction` 为各部分除以 `latency_ms`，合计为 1。它是**协调器观察到的阻塞时间归因**，不是全部服务器累计资源消耗，也不是对整个分布式执行 DAG 做出的全局关键路径证明。

| 分项 | 测量口径 |
|---|---|
| `routing_wall_ms` | 预处理、HNSW、路由入口估距；含发生在该段中的运行时等待 |
| `storage_call_wait_ms` | 协调器等待期间，与使本次等待结束的批次 pread 区间重叠的时间 |
| `data_compute_wait_ms` | 同上，与明确插桩的原向量距离/RaBitQ 计算区间重叠的时间 |
| `data_service_other_wait_ms` | 对应服务区间的剩余部分，包括缓存/邻接处理、内存分配及服务线程被抢占时间 |
| `transport_and_runtime_wait_ms` | 对应 RPC 中不在已观测服务区间内的等待，包含传输、序列化、线程/协议开销等，不能全称为网络传输时间 |
| `rpc_wait_unattributed_ms` | 无法对齐服务器时钟或 trace 校验失败的 RPC 等待 |
| `coordinator_wakeup_wait_ms` | 请求已完成后，协调线程恢复执行前的等待及 wait 调用开销 |
| `coordinator_other_wall_ms` | 其余协调器执行区间，含调度、合并、队列处理、运行时及线程池收尾 |

服务器和客户端记录单调时钟纳秒区间。只有 Linux boot ID 与 time namespace 一致且时间区间满足因果包含关系，才进行跨进程区间相交；物理多机的域不同则保留 `rpc_wait_unattributed_ms`，不相减不同主机时间戳。

一次 `FIRST_COMPLETED` 等待可能有多个批次在返回时已完成，选择最早完成的那个批次解释该段阻塞，其他并发批次的服务时间只进入累计资源量。pread 的时间也含内核执行/缓存处理，距离调用 wall 时间也可能含线程抢占。因此纯网络、物理 SSD、实际 CPU 三者的精确延迟百分比仍不能由当前插桩推出。

## 跳数与通信

- `max_discovery_depth`：所有已发现顶点在本次发现树上的最大深度，包括之后被裁剪的顶点。
- `max_expanded_depth`：已完成扩展顶点的最大发现深度。
- `max_cross_shard_depth`：发现路径上执行分片变化的最大次数；不是 RPC 次数。
- `result_paths`：最终 top-k 每个结果的 parent、root、depth、cross_shard_depth 和执行分片。
- `graph_rounds`：协调器等待一批响应并合并的轮数；不是 BFS 层数。
- `server_to_server_migrations`：当前为 0，架构尚未实现服务器间直接迁移查询状态。

入口深度为 0，每次沿图发现一个新顶点增加 1；备用入口另起一棵树。采用首次发现的有效边作为 parent，不声称是最短路径。完整发现树在 detailed trace 开启时导出。

## 整体 QPS、召回和负载

精确真值准备、Engine 加载、预热和管理指标请求位于计时窗口之外。工作线程持续完成一个查询后领取下一个，形成 closed-loop 负载；QPS = 成功查询数 / 整个测量窗口 wall 秒数，completed_qps 还计入失败完成。短测试不自动成为稳态或饱和吞吐测量。记录并发数、重复次数、成功/失败数量、错误类型和失败延迟分布。

召回相对实际入索引数据计算。报告平均值、分布、单条达到目标的比例，以及达到单条召回目标的完成速率 `recall_qualified_qps`；后者不包含未配置的 p99 SLA 条件。失败 recall 为零；结果不足 k 仍以 k 为分母。mean/p50/p90/p95/p99/max 为样本统计，16 条查询的 p99 不稳定。

服务 `metrics` 管理 RPC 提供累计请求/完成/失败/活动数、峰值活动数、扩展/计算/I/O 计数、缓存量、向量数和文件大小。benchmark 在负载前后取差值，并保存进程 CPU、RSS、存储 I/O 和资源时间序列。

`mean_cpu_cores` = 进程 CPU 秒增量 / 该进程观测窗口秒数，1.0 表示平均使用一个核。CPU 计数受 `/proc` tick 精度限制，极短测试可能显示 0。`peak_rss_bytes` 是进程生命周期高水位，不是单独测量窗口峰值。`/proc/PID/io` 的 read_bytes/write_bytes 是内核记账的存储 I/O，不能精确归属到一个 query，也不提供设备 IOPS/SSD 服务时间。多个 local 分片共处同一 PID 时，进程数据重复出现，不能求和。

扩展负载还报告 max/mean 和变异系数。存储、请求和 CPU 倾斜应分别看，不能从 RF 推断查询负载均衡。

## 构建指标与当前缺口

新索引 manifest 的 `build_profile` 保存采样/读取、k-means、归属校准、每片构图与边合并、桥接连通性、RaBitQ 编码、分片写盘/fsync、HNSW 与发布准备的 wall/CPU 秒数，以及进程资源前后快照。总时间按 wall 计，截止最终 manifest 写入前；当前构建为本机串行分片构建，不能作为真实分布式构建时间。

网络 shuffle、物理 SSD IOPS/服务时间、网卡流量与重传包、独立 admission/server 队列、纯网络延迟和跨机精确关键路径尚无可靠测量，相关未实现项不以估算值冒充实测。后续若引入 eBPF、独立请求队列或校准过的跨机时钟，应增加新的明确口径字段，保留现有字段语义。

## 可恢复查询与异步 I/O 指标

`stats.peer_execution_model` 与 `stats.io_backends` 表示实际执行方式；每个 branch 的 `execution` 包含实际后端及回退原因。`blocking` 的异步计数为零，其专用阶段耗时不参与阶段均值，不能把未实现的阶段当作零开销优势。

| 字段 | 口径 |
| --- | --- |
| `async_read_requests` | io_uring 独立记录请求数，非 submit syscall 数或物理 SSD 命令数 |
| `pread_calls / pread_ms` | 同步/线程池实际 pread 数与调用 wall 时间；原生 io_uring 保持为 0 |
| `file_read_bytes` | 成功分支返回的实际文件字节，缓存命中不累计；失败/取消后台读取另见节点计数 |
| `io_batch_wall_ms` | 从本轮交给 I/O 层到完整可消费的 wall 时间，包含容量等待、队列与完成事件处理 |
| `io_capacity_wait_ms` | 本轮等待完整缓冲预约的时间，包含 reactor 接收计划的延迟 |
| `io_queue_wait_ms` | 各记录从本轮取得容量到安排提交的等待时间之和，可互相重叠 |
| `io_read_residence_ms` | 各记录安排提交到 reactor 处理其完成的时间之和；含后端排队和通知处理，非纯 SSD 时间 |
| `query_ready_queue_ms` | 分支计算执行片从入队到工作线程执行的累计等待 |
| `query_compute_slice_wall_ms` | 实际执行片 wall 时间，包括 frontier、估距、精排、提交等，可能含线程抢占 |
| `server_thread_cpu_ms` | 每个计算执行片的 thread_time 差值之和；不跨线程相减；不含独立 I/O/reactor/传输线程 CPU，后者由进程采样核对 |
| `query_io_wait_ms` | 计算片结束后到本轮完成/错误通知进入恢复队列的累计 wall 时间 |
| `query_transfer_finalize_wait_ms` | 本地搜索结束到已批准子分支传输结算完成的等待 |
| `query_state_other_ms` | 执行片与状态转换边界未归入上述四类的小量时间 |
| `query_branch_wall_ms` | 该分支任务创建到最终报告准备的 wall 时间，等于上述四类 wall 加 other |
| `query_resume_count` | 初次执行及后续恢复的执行片数量 |

每个分支的状态 wall 可以相加；多个数据节点分支并行，所以 query 级累计值不能相加后当作端到端延迟，也不能用累计 I/O 时间除以端到端时间宣称磁盘占比。原有 routing、peer execution/control wait、merge/lifecycle 的端到端互斥分解仍保留。`io_batch_wall_ms` 与状态时间有包含关系，分别报告，不重复加总。CSV 的阶段均值新增这些字段，资源均值仍排除 `metrics_complete=false` 样本。

详细 branch trace 的 `io_trace` 包含 batch ID、query/branch key、epoch、候选 ID，以及记录 request token、偏移、长度、submit/completion 时间。这里 submit 是应用把请求交给后端的时刻，CQ 的时间是应用观察到完成的时刻；没有声称测得内核内部接受/硬件完成时刻。跨节点的单调时钟不可直接相减。

`node_process_load[].peer_lifecycle_after.io` 保存后端、回退原因、提交批大小/涉及 query 数直方图、backend submit 次数、在途/预约/完成未消费峰值、取消后迟到字节和有界近期记录。这些是服务启动以来累计量（可能含预热），不是仅 benchmark 计时窗口增量。`query_io_wait_ms` 等为该查询自身测量。`local_io_quota_allocation` 记录同机进程按文件系统设备号的配额合计，需要结合 mount/lsblk 判断底层物理设备。

当前节点生命周期的 `waiting_io` 表示本轮尚未重新执行，包含完成后在 CPU 队列中等待恢复的短暂阶段；该区间在逐查询字段中由 `query_io_wait_ms` 与 `query_ready_queue_ms` 分开计时。`inflight_requests` 是后端已安排但尚未处理完成的请求数，保守包含原生 SQ 中尚未被内核接受的请求，不等于硬件队列深度。

## 通信优化指标（2026-10-18）

`stats.communication_mode` 为 `legacy` 或 `optimized`。优化模式下本地搜索结束即可释放计算工作线程，`query_transfer_finalize_wait_ms` 仍记录等待已批准子分支派发确认的生命周期时间，但**不再表示计算工作线程被占用**。native 搜索内部的 I/O 等待仍由固定计算线程承担；不要把通信收尾优化理解为已实现原生 I/O continuation。

| 字段 | 口径 |
| --- | --- |
| `peer_transfer_queue_ms` | 已批准的跨节点派发从入传输线程池队列到开始执行的累计等待 |
| `peer_pool_wait_ms` | 数据节点间派发借用连接的累计时间，包含连接池状态操作，不含新建连接 |
| `peer_connect_ms` | 创建新连接的累计 wall 时间 |
| `peer_send_ms` | 序列化、帧编码及 socket 发送的累计 wall 时间 |
| `peer_response_wait_ms` | 接收并解码响应的累计 wall 时间；包含目标端处理及调度，非纯网络 RTT |
| `peer_transfer_wall_ms` | 派发工作线程开始执行至完成/失败的累计 wall 时间，含重试，不含派发线程池排队 |
| `peer_connections_created/reused` | 数据节点之间派发使用新连接/复用连接的次数，仅在逐查询 JSON 中记录 |
| `control_by_kind` | heartbeat、result、lease、cancel 的 RPC 和应用协议字节计数；正常 optimized 查询 lease/cancel 均为0 |
| `peer_lifecycle.control_totals` | optimized 节点控制发送累计 sent/errors、queue_ms、wall_ms、connections_created/reused；legacy 未插桩这组计数，0不代表无控制开销 |
| `peer_lifecycle.control_active` | 已提交但未完成的控制任务数，包含排队和发送中任务 |

新增时间字段进入 `query_stage_means.csv`、`per_query_stage_means.csv` 的累计工作组。连接字段只含数据到数据派发，不是入口提交或控制回传的总连接数。控制总计从节点启动累计，分析测量窗口时须减去 `nodes_before`，排除预热。

派发阶段与 `query_transfer_finalize_wait_ms` 重叠，且父子分支可并行；不能把这些累计时间直接相加形成 query 延迟占比。socket wall 时间包含线程调度、协议编解码、对端接纳和确认，不能据此声称全部耗时发生在网络设备。

失败派发的部分阶段/字节可能无法收集，此时 `metrics_complete=false`，不纳入完整资源均值。结果已返回后，墓碑确认的迟到终态重试不会写回已冻结的逐查询统计；`control_totals` 提供节点侧补充记录。控制发送失败但尚未到达入口时，逐查询快照可能无法看到该次尝试，因此 `metrics_complete=true` 也不保证包含所有迟到/未送达控制消息；网络总量研究必须结合节点差值。本轮对照的控制发送错误均为0，见 [报告](test/control_path_20261018/REPORT.md)。

## 接纳队列与确认解耦指标

`peer.admission.mode=buffered` 时接收线程在原子完成去重、容量预约及入队后返回接纳确认；独立线程将任务提交给计算线程池。`direct` 保留接收线程直接 submit 的对照路径。

- `query_admission_queue_ms`：接纳记录创建至开始提交计算线程池的墙钟时间，包含接纳队列与调度等待；direct模式也会有少量状态处理时间。
- `query_executor_start_wait_ms`：开始提交至初次工作线程进入的墙钟时间，包含 submit内部锁、建线程、工作队列及调度等待，不是纯队列驻留。
- native 的 `query_ready_queue_ms` 等于上述两项之和。resumable 的ready还包含后续I/O完成后重新入队的等待，两个新增字段仅解释初次执行。
- 节点 `peer_lifecycle` 新增 `admission_mode`、`admission_queue_depth`、`admission_pending_start`、`admission_peak_pending_start`、`admission_capacity_rejections`。pending包括已经交给计算线程池但尚未开始的任务，因此容量不会因前端队列被搬空而失效；direct不维护此容量计数。

两个新增时长进入JSON与阶段CSV，是ready的细分项，不能再与ready相加。接纳确认不等于查询完成；完整延迟仍等待所有分支结果。优化是否有效须同时比较源端派发收尾、目标端ready及端到端p99，不能仅以ACK更早判定整体加速。

## 任务启动绝对时间戳

`metrics.detailed_trace: true` 时，节点 `peer_lifecycle.execution_timestamps` 按 `(query_request_id, branch_id, node_id)` 保存诊断事件。Python 使用 `time.monotonic_ns()`，C++ 使用 `CLOCK_MONOTONIC`，原生事件附带 `clock_domain`。只能在已验证相同的时钟域中对齐；不同物理主机不能直接相减。

| 字段 | 边界 |
|---|---|
| accepted_ns | 发布接纳身份后的诊断起点 |
| enqueued_ns | 加入接纳deque后、仍持有队列状态锁 |
| dequeued_ns | 接纳线程从deque取出后、仍持有队列状态锁 |
| submit_state_lock_enter_ns / acquired_ns | 提交前查询状态检查的锁获取边界 |
| submit_enter_ns / submit_return_ns | 调用计算线程池submit的进入与返回/异常退出 |
| submit_cpu_enter_ns / return_ns | 同一提交线程的CPU时间；不是单调墙钟，不与其他线程CPU时钟相减 |
| worker_enter_ns | `_run` 函数入口，在获取查询状态锁之前 |
| worker_state_lock_acquired_ns | 工作线程获得查询状态锁并更新待启动计数之后 |
| native_call_ns / native_return_ns | Python调用C++搜索函数前/返回后 |
| native_entry_ns | C++函数入口，尚持有GIL并准备输入 |
| native_search_start_ns | C++释放GIL后、量化查询预处理及图搜索之前 |

附加 `queue_depth_at_enqueue`（包含自身）、`queue_depth_after_dequeue`、`submit_thread_id`、`worker_thread_id`。`native_timestamps` 也随原生分支报告保存原生边界。submit返回可能晚于工作线程开始甚至报告发布，因此提交侧事件只写节点诊断记录，不修改已经发送的报告；需要采集节点metrics并关联到查询。记录受现有分支retention和容量限制，不是永久审计日志，长运行应在过期前导出。

完整六事件覆盖buffered接纳的native路径；blocking路径没有C++事件；direct没有deque入/出队事件；resumable路径暂不声明具有相同的完整提交/启动事件。缺失字段不能补零伪装成没有等待。

队列驻留=`dequeued-enqueued`；调用时长=`submit_return-submit_enter`；开始执行等待=`worker_enter-submit_enter`。后两项可能重叠，不能相加。提交CPU计时边界略包围墙钟边界，微小调用可能出现CPU略大于wall，分析应使用总量或分布，不能据此伪造负等待。

接纳队列等待可与同节点前序任务的submit区间求交。单个接纳线程的提交区间不重叠，但一次提交可能阻挡多个后续任务；累计交集描述任务排队来源，不是CPU利用率。详细实验及CSV导出脚本见 [启动时间戳实验](test/execution_timestamps_20261018/REPORT.md)。

补充启动诊断字段：`executor_adjust_enter_ns/return_ns`标记计算线程池调整工作线程数的区间；`executor_workers_before/after`区分新增线程和复用。submit到adjust的差值包括提交锁、Future及工作项创建/入队；没有单独测到纯提交锁等待。`receiver_thread_id`、`transfer_thread_id`和control事件中的`sender_thread_id`用于与`worker_thread_id`对照；仅限同进程/同时存活线程的身份分析。

`transfer_enqueue_enter_ns/return_ns`为计算线程提交远程任务的墙钟边界，`transfer_worker_enter_ns/return_ns`为独立发送线程执行边界；两者可重叠。`local_search_done_ns`与`report_ready_ns`定位本地报告构造完成及分支最终报告发布。节点`peer_lifecycle.control_events`按分支保存控制发送的queued/sender_enter/message_ready/sender_rpc_return时间、消息类型及RPC计时；尚不包含入口接收和唤醒的完整绝对时间链。`send_queues`报告transfer/control各自等待深度、活动任务、等待峰值、容量拒绝和工作线程数量。

### 计算提交与连接池等待消融（2026-10-18）

`peer.compute.mode=fixed` 在既有启动时间戳中增加 `executor_enqueue_ns` 与 `executor_dequeue_ns`，差值是固定计算队列的驻留时间；`native_search_start_ns - worker_enter_ns` 仍包括 Python 准备与调度，不能称为纯计算。节点 `compute_queue` 给出 queued、active、peak_queued、capacity_rejections、ready_workers 和 callback_errors；prestarted 模式另报告启动屏障耗时。预启动模式的计算线程在 readiness 后才纳入性能测量。

连接池 `pool_wait_ms` 的含义不变：它包含借用期间的锁、容量、socket 健康探测及调度时间。`reserved` 将 socket 探测与关闭移到池锁外，但该指标仍不是纯锁等待。不能用 `response_wait_ms` 直接推算物理网络 RTT。

本机多进程实验新增离线关键路径 CSV：`entry_accept_ms`、`accept_to_all_report_ready_ms`、`all_report_ready_to_last_sender_ms`、`last_sender_to_api_return_ms`。由同一查询的绝对事件边界构造，四项互斥且总和等于客户端墙钟延迟。该划分描述边界间的总时间，不将并行工作错误归因到某一分支。分支报告就绪前已排队的心跳可能在发送时升级为结果，因此单分支 `queued_ns` 可能早于 `report_ready_ns`；不能未经裁剪就相减作为结果排队时间。结果 RPC 的 ACK 处理也可能晚于客户端 API 返回，时间线单独显示。

原始记录、审计及 CSV 位于 [计算启动与连接池实验](test/compute_startup_20261018/REPORT.md)。物理跨主机时必须遵守时钟域约束，不能直接复用本机绝对时间相减方法。

### 计算线程释放、GIL 与选择性跟踪（2026-10-18）

`metrics.lifecycle_trace` 开启所有查询的头节点、分支启动、原生返回、工作线程释放及控制消息关键时间戳；不生成逐批次/逐读轨迹。`metrics.detailed_trace` 开启全体详细轨迹，`metrics.trace_query_ids` 则只为指定 uint64 查询 ID 开启详细轨迹，子分支沿用同一个 query_id。三者都关闭时，逐查询延迟、召回、网络/磁盘/计算计数及预算审计仍保留。旧配置缺少新增字段时迁移为 false/空列表。异步 LocalIO 的底层详细采集开关是实例级：指定任意详细查询仍会开启后端采集，但只有被选择的查询附带记录；本轮 sync_pread 对照不涉及该限制。

原生线程生命周期新增以下边界：

| 时间戳 | 含义 |
|---|---|
| native_loop_done_ns | 候选耗尽或预算耗尽后，离开原生搜索循环；尚未重新获得 GIL |
| native_gil_reacquired_ns | 原生调用重新获得 GIL，可以构造 Python 报告 |
| native_report_ready_ns | Python 报告物化接近完成 |
| native_return_ns | 调用返回 Python 适配器 |
| worker_call_return_ns | 固定工作线程执行及 Future 完成路径结束 |
| worker_available_ns | active 计数释放，可领取下一任务 |

`native_gil_reacquired_ns - native_loop_done_ns` 包含 GIL 获取及 OS 调度延迟，不是纯互斥锁时间。`worker_available_ns - native_return_ns` 只反映 Python 返回后的释放，不能替代从原生循环终止开始的完整释放延迟。`report_ready_ns` 是异步控制路径完成派发结算后的报告就绪，与工作线程释放独立；两者顺序不能预设。

接收侧新增 `receiver_message_ready_ns`、`receiver_accept_enter_ns/return_ns`、`receiver_ack_send_enter_ns/return_ns`。accept 内另外记录状态锁请求/获得/释放和线程 CPU 边界，CPU 时间用来区别真正执行与墙钟挂起，不能把墙钟减 CPU 全部解释成某一种锁等待。根 RPC 的 `stats.entry_rpc` 保留池借用、连接、发送、响应及绝对时间边界。ACK send 返回时对方可能已收到数据，跨端分析使用 ACK send 进入→客户端 response_received，并仅在相同 clock domain 下相减。

本轮 [等待分析脚本](test/wait_latency_20261018/wait_breakdown.py) 报告各队列 mean/p99/max，并把同一查询被观测到的队列区间取并集，避免并行分支重复相加；另减去与同查询原生搜索/入口路由重叠的区间，得到 exposed_queue_union_ms。它是已测队列在没有这些搜索区间覆盖时的观测时长，不是精确的 CPU off-CPU profile，也不覆盖所有协议及线程恢复等待。不能把端到端四段划分直接称为纯等待。只有指定查询开启详细跟踪的组，其详细逐批次分解不代表总体分布；全体生命周期统计须检查 traced_queries=2000。

控制事件进一步记录 `local_done_enqueued_ns/dequeued_ns/consumed_ns`，区分完成事件等待被领取和领取后的状态锁处理；`finalize_enter_ns/acquired_ns` 只记录实际消费最终报告的那次调用，早期发现派发 Future 未完成的探测不覆盖它们。`control_selected_ns` 与 `control_result_selected_ns` 标记选择发送时刻；已排队心跳仍可能在 sender 内升级为结果，缺少 result_selected 不表示结果没有发送。`max_turn_wall_ms` 及每来源最大消费数量用于诊断批次公平性，不能当作硬调度时限。`admission_dispatch_threads` 输出实际接纳消费者数量。

`metrics.gc_trace` 开启进程级有界 GC 事件，节点 metrics 中 `gc_profile` 保留总次数、wall/CPU 累计及最近 resource_max_samples 条事件。实验用 nodes_before/after 的累计差值排除初始化，事件用测量开始/结束裁剪；明细缓冲有上限，不应把保留事件的和当成无条件完整累计。GC 诊断不关闭 GC、不调整回收阈值；它和 GIL 参数都要求同进程配置一致。

可选完成发布快路径另外记录 `completion_publish_path`（`try_lock` 或 `queued`）及锁尝试、发布通知消费边界。节点 `control_scheduler_totals` 中 `completion_fast_path_hits`、`completion_fast_path_lock_busy`、`completion_fast_path_future_fallback` 分别表示成功发布、状态锁忙回退、存在派发Future而回退；控制消费者更新这些累计值，应在排空后取差值。此路径默认关闭，实验未见稳定p99收益。

快路径成功分支没有 `local_done_enqueued_ns/dequeued_ns`，缺少这些时间戳不是数据丢失。`local_done_dequeue_ms` 仅统计走队列的子集；离线 `completion_queue_all_branches_ms` 则为成功快路径记零，其余用队列驻留，使分母覆盖全部分支。零仅表示未经过该队列，不能表示整个完成发布不耗时。报告就绪到发送、原生循环结束到线程可复用仍需独立衡量。

### 原生完成与发送路径（2026-10-18）

`peer.control.scheduler: native`时，完整报告在释放GIL的C++区间内构造。`native_report_ready_ns`改指原生报告构造完成；`native_completion_enqueued_ns`为Ticket.complete调用返回，`native_gil_wait_start_ns`为随后开始重新获取GIL，`native_gil_reacquired_ns`为已获取。真正的GIL恢复区间使用后两者之差；不能再把`native_loop_done_ns→native_gil_reacquired_ns`整体解释为GIL等待，因为其中包含原生报告构造和入队。event/scan路径沿用旧定义。

`local_done_enqueued_ns/dequeued_ns`衡量原生完成事件消费等待，`report_ready_ns`仍指派发结算完成后的最终报告发布。两者都可能早于Python适配器返回和计算线程可复用，不得假定“Python返回→报告就绪”一定为正时长。`completion_publish_path: native`和分支`execution.completion_backend: native`标识实际后端。

节点`native_control`输出`command_queue_depth/peak`、`client_pending`、`control_active`及`retained_branches`。保留已ACK报告用于去重与诊断，因此retained不等于正在运行。`control_scheduler_totals`中的`cpu_ms`、`lock_wait_ms/lock_held_ms`为原生事件线程的真实累计测量，`event_errors`若出现应单独审计；测量期间差值不应把可能的字符串`last_event_error`作数值相减。Python终态镜像通过take_updates更新，网络发送不依赖Pythonmetrics轮询。

原生`control_events.control_thread_id`是控制线程ID，不是socket reactor ID；不生成假的sender_thread_id。`sender_enter_ns`为原生连接槽就绪、即将connect/send的时刻，`sender_rpc_return_ns`为reactor完成时刻。`message_ready_ns`在提交后记录，可能晚于reactor开始发送，不可用两者差值断言负排队错误。RPC计数`send_ms`当前包含新连接的connect区间，`connect_ms`是其子区间，不能再次相加。ACK间隔仍包含入口处理和OS调度，不代表纯网络RTT。

分析与图表见 [原生控制实验](test/native_control_20261018/REPORT.md)，所有本机时间线使用同一单调时钟；物理多机不能直接按此方式相减。

### 入口连接归还与FIFO调度（2026-10-19）

Python RPC新增以下字段，入口请求保存在每query的`stats.entry_rpc`中：

- `rpc_finally_enter_ns`：开始归还或关闭连接前的本机时间戳。
- `rpc_release_done_ns`：归还/关闭结束；成功和异常路径均记录。
- `pool_release_ms`：上述两点之间的完整墙钟区间，包括池锁获取、队列状态转移和必要关闭。
- `pool_release_lock_wait_ms`：reserved旧池/FIFO池归还时等待注册表状态锁的区间；包含被唤醒后重新调度，不能断言锁一直被单线程持有。
- `wall_including_release_ms`：本次RPC包含归还收尾的总时长；旧`wall_ms`仍保留收到响应后、进入finally之前的口径，避免改变历史统计含义。

`response_received_ns→rpc_finally_enter_ns`、`rpc_finally_enter_ns→rpc_release_done_ns`、`rpc_release_done_ns→entry_submit_return_ns`可进一步分解收到ACK后到提交函数返回的时间。只有入口请求的这些时间戳与head时间戳处于同一时钟域，可直接相减。所有新增字段保存在原始JSONL；新实验另导出入口阶段CSV。

`peer.connection_pool.head_scheduling: fifo`只改变head发起的入口/控制RPC借用连接顺序，数据节点之间的派发和C++完成回传池保持原实现。每目标容量及查询并发没有因此变大。FIFO指同一目标在池中排队者的预约顺序，不保证查询完成顺序，也不保证操作系统立即调度被唤醒的线程。

## 顶层独立报告接收进程

配置 `peer.head_receiver_processes`，0表示原主进程接收，正整数表示额外接收进程数。每条查询的 `stats.head_receiver` 在独立进程模式下记录：

|字段|含义|
|---|---|
|registration_ms|主进程向指定接收进程注册并等确认的墙钟时间，含IPC排队和双方调度|
|registered_ns|接收进程建立该查询状态的时刻|
|first_callback_dispatch_ns / last_callback_dispatch_ns|完整网络消息解码后进入回调处理；不包含此前的内核队列或消息解码等待|
|callback_count|该查询转入终态前处理的回调数量|
|terminal_publish_ns|终态报告包准备发送到主进程的时刻，数据回调ACK已处理|
|terminal_parent_received_ns|主进程收到并解码终态报告包的时刻|
|terminal_parent_applied_ns|主进程应用终态状态并即将唤醒查询的时刻|
|receiver_pid / receiver_index / clock_domain|接收进程身份与时钟域|

`terminal_parent_received_ns - terminal_publish_ns` 包括序列化、IPC等待、父进程调度和解码，不是纯通信延迟。`all_reports_received_ns` 仍表示查询调用线程wait返回；与applied之差可能还包含初始submit未返回，单独估计唤醒残余时使用 `all_reports_received_ns - max(terminal_parent_applied_ns, entry_submit_return_ns)`。注册区间与终态交付区间不能在没有检查时间轴时机械相加。所有以上跨进程差值都要求同一有效clock_domain；远端数据节点时间戳继续按各主机独立原点展示。

管理status增加head_receivers，列出PID、端点、存活/错误、命令队列深度及主进程仍持有的查询数。benchmark增加head_process_load，分别记录HNSW主进程与每个接收进程的CPU秒差、观测窗口、平均占用核数及RSS/线程数快照。CPU观测从放行客户端前开始、所有客户端结束后采样，窗口与QPS末条完成时间略有差异；平均核数不是相对机器总核数的百分比。数据节点原有指标保持原定义。

## 顶层注册细分与事件续执行

`head_receiver`新增以下同机时间点：register_enter_ns、register_parent_enqueued_ns、register_parent_dequeued_ns、register_receiver_received_ns、registered_ns、register_ack_publish_ns、register_ack_parent_received_ns、register_ack_parent_applied_ns、register_return_ns。最后四个ACK相关时间点仅在同步注册模式存在。相邻阶段分别包括父进程排队、命令传输/子进程调度、子进程注册、确认返回及恢复；不得把其中任一传输区间直接解释成纯内核通信。

续执行模式用 `registration_mode=receiver_continuation` 区分；`registration_ms=0`表示没有阻塞等待注册ACK，不代表注册瞬间完成。必须同时报告 `registration_ready_ms`（主进程分配查询到接收进程注册完成）、初始派发队列及端到端延迟。`entry_enqueued_ns`到head_timestamps.entry_submit_enter_ns为接收进程初始派发队列等待。该模式的entry_submit_enter/return_ns由实际派发工作的接收进程记录，已核对与主进程同一clock_domain，不能与远端节点直接相减。

`head_stage_queue_ms`记录查询进入API到首次就绪阶段实际执行；`head_stage_resume_queue_ms`记录入口ACK与终态报告均齐备到合并阶段实际执行。它们均包含在client_wall和stats.latency_ms中，等待状态仍占用客户端并发名额。query_ready_ns表示ACK和报告全部齐备，而非仅收到最后一个报告。主进程CPU计时按路由/合并所在的各线程分别取CPU差值再累加，避免迁移续执行线程后出现错误的thread_time差值。
# 单进程顶层流水线的追加计时

启用 `peer.head_pipeline.enabled` 时，每查询 `stats.head_pipeline` 保存同一顶层进程的monotonic_ns时间点：`receive_enqueued_ns`、`receive_started_ns`、`route_enqueued_ns`、`route_started_ns`、`dispatch_enqueued_ns`、`dispatch_started_ns`、`merge_enqueued_ns`、`merge_started_ns`、`merge_finished_ns`。最后一个时间点在指标汇总后、完整生命周期清理前，不等于API返回；完整返回仍使用 `head_timestamps.search_return_ns` 和外部 `client_end_ns`。

新增 `head_receive_queue_ms`、`head_route_queue_ms`、`head_dispatch_queue_ms`、`head_merge_queue_ms`，分别为对应started减enqueued。旧 `head_stage_queue_ms` 在此模式表示进入首路由阶段之前的总排队，与接收/路由队列存在包含关系，不能重复相加。`head_stage_resume_queue_ms` 从完整结果ready到恢复生成器计时，与新的merge_queue存在近似包含关系，也不能重复累加。

单进程模式的 `head_receiver.registration_mode` 为 `single_process_pipeline`；`registration_ms=0` 表示无同步IPC注册往返，真实本机状态表注册开销用registration_ready_ms及registered_ns说明。本模式没有terminal_publish/parent_received这类IPC时间点，分析脚本不得填造零值冒充实际测量。`query_ready_ns` 只有在分支树完整且入口派发成功时设置。

单线程回调反应器为每条消息生成ACK，计数在ACK交给非阻塞发送路径时记录。发送缓冲尚未排空、连接断开后的重试可能让逻辑协议字节与物理链路字节不同；应用层计数不等同抓包。`ack_inline` 为true时先尝试立即非阻塞发送，余量等待可写事件；false作为排队ACK对照。新增主动peer_lease计入control_by_kind.lease、control_rpc_count及all_protocol_bytes。

流水线内部压测使用固定数量的在途Future，不创建每query阻塞调用线程；从提交到结果消费结束完整计时，包含消费队列调度，不在merge完成时提前停止。端到端通过真实HTTP调用还包含socket接纳、HTTP解析/响应及客户端网络时间，内部benchmark不含这些阶段，报告必须区分。HTTP接收线程上限与流水线的receive_max相同，等待查询结果时保留socket而释放接收线程。
# 直接分发与结果进程池补充（2026-10-20）

`head_direct`模式的逐查询纳秒时刻保存在`stats.head_direct`，全部来自顶层同一主机；结果进程采用同一主机单调时钟，可以与顶层比较，不能与数据主机时刻相减。

|字段|定义|
|---|---|
|head_distribution_queue_ms|查询接纳至分发线程开始；线程池入口的排队仍计入|
|head_result_queue_ms|结果消息提交至结果工作进程开始，包含输入IPC及队列驻留|
|result_return_ipc_ms|结果进程完成至主入口完成回调获得结果，包含输出序列化/IPC/调度|
|result_write_ms|结果工作进程编码并追加JSONL的墙钟时间；包含在结果处理内，不重复相加|
|result_process_pid|处理该查询结果的工作进程PID|
|entry_submit_retries|初始peer_submit实际发送次数减一；重传沿用原query/branch身份|
|distribution_released_ns|分发线程注册异步完成回调前的释放边界标记；不是操作系统线程切出时刻|
|entry_send_done_ns|分发线程完整发送初始报文后的时刻，保存在head_receiver|
|entry_ack_received_ns|确认线程读到并接受入口ACK的时刻，保存在head_receiver|

`latency_breakdown_ms`将新模式划为分发排队、路由与发送、确认/报告等待、结果队列、结果处理和返回IPC。ACK等待与数据搜索/报告回传可能重叠，不能额外相加。HTTP外部传输仍应使用客户端墙钟衡量。顶层进程负载统计包含全部结果进程，RSS按进程相加可能重复计入共享页。结果工作进程日志为返回前快照，最终写盘/返回IPC及完整API指标使用主入口收集的查询JSONL/CSV。
# 多通道报告接收的补充测量

`peer.head_direct.queued_reports` 下，每查询 `stats.head_receiver.report_receipts` 记录报告/心跳的接收后处理时间：

- `received_ns`：完整帧已解码，进入报告处理；不包含网络首字节到完整帧/解码之前的时间。
- `validation_start_ns`、`validated_ns`：校验开始/结束。
- `ack_write_finished_ns`：本地 ACK 写入操作返回；不代表数据节点收到 ACK。
- 线程后端的 `enqueued_ns` 是有界队列 put 返回时刻。若触发背压，消费者可能先于 put 的调用者恢复，因此不要求它严格早于 validation_start_ns；处理等待用 validation_start_ns - received_ns。

进程后端的 `terminal_publish_ns → terminal_parent_received_ns` 包含终态编码、管道背压、传输、父侧解码和调度，不能称为纯传输耗时。`terminal_parent_applied_ns` 表示主进程应用状态的时间；共享注册的 `shared_identity_published_ns` 表示小身份记录的发布时间。`completion_queue_ms` 是多次通知排队的累计值，不可直接与互斥关键路径相加；inline通道消费通常为0，若入口 ACK 最后到达仍使用完成线程。

`receiver_unregister_lock_wait_ms` 与 `receiver_unregister_lock_held_ms` 分别记录注销全局成员表时等待锁、持锁更新的墙钟时间，位于 `head_receiver` 中。它们不包含随后按通道清理或共享身份移除；不能把第一项解释为临界区计算时间。实验目录的 `receiver_timings.csv` 同时导出这些字段、注册、终态IPC和完成通知累计等待。

数据侧原有 `control_events.rpc_counts.pool_wait_ms` 与 sender_enter_ns → sender_rpc_return_ns 分别测量回传连接等待、发送至ACK；必须按正式查询 request_id 筛选，不能混入预热和其他入口。只在同一主机时钟域做时间差。新增报告接收进程全部计入 `head_process_load`，角色为 `report_receiver`，与 `result_process` 分开；RSS合计可能重复计入共享页。

# 按查询划分顶层执行进程的测量（2026-10-20）

`peer.head_lanes.enabled` 模式将一次查询的路由、派发、租约、报告验证及合并放在同一个顶层执行进程内。父进程只传入原始向量、收取最终结果。`stats.head_lane` 包含 `index`、`submitted_ns`、`worker_received_ns`、`worker_finished_ns`、`parent_received_ns`。这四个时间戳均在顶层同机时钟域，可形成三个互斥区间：

| `latency_breakdown_ms` 字段 | 定义 |
|---|---|
| `input_queue_ipc_ms` | 父侧接纳至所属进程开始处理，包含排队、编码、IPC、解码与调度 |
| `worker_pipeline_ms` | 所属进程处理至最终结果就绪，包含路由、数据搜索和完整报告等待、合并及记录 |
| `output_ipc_ms` | 最终结果就绪至父侧收到，包含编码、传输、解码与调度 |

原内部阶段放在 `worker_latency_breakdown_ms`；`head_pipeline` 保留路由/派发/合并纳秒边界。它们是 `worker_pipeline_ms` 的细分，不能再与父级相加。`head_lane_write_ms` 是进程内 JSONL 编码和追加耗时，也是内部区间的一部分。所属进程的 JSONL 是返回前快照；完整返回时间以压测生成的 `queries.jsonl.gz` 为准。

`head_process_load` 中角色 `head_lane` 必须与主进程共同计入 CPU/RSS。状态提供进程 PID、健康状态、在途数、回调端点与 `source_file`，便于核验实际载入的冻结源码。CPU 核数按各进程自己的采样窗口计算，数据节点窗口还包含前后指标采集，不能据此给出严格的 CPU 饱和率归因。

当前选型以 **QPS 优先**，约束平均 Recall@10≥0.95、无失败、全局预算/不重复访问/指标完整性审计通过；p99、平均时延、接纳前积压和 offered-to-complete 时间都必须报告。内部 benchmark 包含提交至消费返回，实际 HTTP 还增加连接及请求/响应开销。短时闭环负载的 QPS 不是开放到达过程下的稳定容量。
