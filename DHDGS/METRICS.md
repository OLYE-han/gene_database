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
