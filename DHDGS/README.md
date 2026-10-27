# DHDGS

后续优化入口：[Research.md](Research.md)。按问题记录证据、候选方案及每轮尝试；验证可行后更新 [DESIGN.md](DESIGN.md) 并标记解决，实验轮次仍统一记录在 [TestLog.md](TestLog.md)。当前首个主攻方向是 [R001：负载不均与热点子图复制](Research.md#r001)。

**当前默认（2026-10-23）：四机 RDMA 已验收方案。** Intel-35 顶层、Intel-31/32/30 数据节点，每节点64搜索线程、预算200，推荐并发1024。配置在 [configs/dhdgs.yaml](configs/dhdgs.yaml) 和 [configs/rdma_deployment.yaml](configs/rdma_deployment.yaml)，使用 `.venv/bin/python tools/rdma_default.py check` 核验、`benchmark` 启动。CPU/NUMA、jemalloc 和固定 RDMA 运行库由启动器加载，操作说明见 [RDMA_DEFAULT.md](docs/RDMA_DEFAULT.md)。本机小规模验证显式使用 `configs/dhdgs_pilot.yaml`。以下早期默认值与部署描述为历史记录，以本段和当前操作说明为准。


原常驻三机部署为 **Intel-35：head + data-0；Intel-32：data-1；Intel-30：data-2**。每节点32个查询工作线程，见 [原入口操作命令](test/managed_continuation_three_20261019/README.md)。

2026-10-20：选型改为 **QPS优先**，约束平均Recall@10≥0.95且无失败。新增按查询划分顶层执行进程的 `peer.head_lanes` 模式，减少中间IPC交接；完整2M三机固定并发128短测，QPS中位数604.73→1168.58，Recall约97%。扩大去重容量后的双向扫描选中并发256、约1103QPS；但100000次连续请求仅527QPS、p99约6.24秒，持续性能问题尚未解决，未替换常驻入口。最新候选、并发扫描及长测边界见 [控制链容量优化报告](test/control_capacity_20261020/REPORT.md) 和 [运行说明](test/control_capacity_20261020/README.md)。

持久三机部署、head 下发查询配置与集中收集结果：见 [操作手册](test/managed_cluster_20261019/README.md) 和 [验证状态](test/managed_cluster_20261019/REPORT.md)。

Distributed Hierarchical Disk Graph Search：面向高维向量的分布式图检索研究系统，采用样本 HNSW 路由、可调重叠分片、RaBitQ 估距、磁盘节点记录和跨分片批量扩展。

当前版本提供可运行的端到端原型，包含 C++ 算法内核和 Python 构建、调度、TCP 服务及实验工具。已经完成本地磁盘检索与三个独立进程的 TCP 正确性测试。**当前建图是内存中的 Vamana 实现，尚未集成上游 DiskANN 的外存构建器，物理三机部署及实测状态见 [DEPLOYMENT.md](DEPLOYMENT.md)。** 规模与性能限制见下文。

所有已开放的运行及实验参数集中在 **[configs/dhdgs.yaml](configs/dhdgs.yaml)**。命令行只选择操作、配置文件和服务节点角色，不提供另一套调参覆盖项。YAML 默认值用于探索性测试，不代表已通过实验选定最终方案。

2026-10-18：默认执行模式改为 `native`，完整本地搜索循环在 C++ 中释放 GIL 执行，每个查询分支固定由一个计算线程负责；不同查询并行，I/O reactor 聚合已选定的读取。Python `blocking/resumable` 模式保留作对照。默认关闭应用 LRU，扩展统计使用原子计数，HNSW 固定每实例 `ef_search` 后支持并发查询，必要的生命周期同步保留。需重新编译原生模块。质量目标改为平均 Recall@10≥0.95，本轮完整 2M 校准后默认预算192/frontier64、每节点8个计算线程。实现和验收边界见 [DESIGN.md 第 6.14 节](DESIGN.md#614-原生完整搜索循环与固定查询线程)。

现有读取后端均为 buffered I/O；应用缓存为零、使用 C++ 或 io_uring 都不代表绕过 OS 页缓存。物理 SSD 验证计划见 DESIGN.md 第 6.13 节。默认 `local + sync_pread` 用于功能验证；启用跨查询聚合时，将 YAML 设为 `network.transport: tcp`，`peer.execution_model: native`，`storage.io.backend: io_uring`（或 `thread_pool`），启动数据服务后压测。

通信优化已实现，见 [DESIGN 第 11 节](DESIGN.md) 和 [2000 查询对照报告](test/control_path_20261018/REPORT.md)。`optimized` 模式让计算线程在本地搜索完成后立即处理其他查询，派发确认由完成回调结算；控制消息并发回传、响应携带续租，正常完成无需取消 RPC。同一进程共享入口回调服务，RPC 使用有界连接池。远端分支仍只由原扩展函数决定。这不等于原生搜索已经能在 SSD 等待中交出计算线程：native 在一次本地搜索内部仍固定线程等待 I/O。

接收端新增 `peer.admission.mode: buffered` 有界接纳队列。成功校验、去重并入队后即可返回接纳确认，计算线程池提交由独立接纳线程完成。`queue_capacity` 限制已接纳但尚未开始执行的总任务数，包括计算线程池中的等待任务；队列满明确拒绝。`direct` 用于旧路径对照，旧YAML缺少整个admission组时显式迁移为direct。接纳成功只表示进程内接纳，不是搜索完成或持久化确认。设计及指标见 DESIGN 第11.10节与 METRICS.md。

## 安装与运行

在 `gene_database/DHDGS` 目录运行。已有 `.venv` 可直接使用；原生模块需要在每台机器对应的 Python 环境中编译。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[build]'
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DPython_EXECUTABLE="$PWD/.venv/bin/python"
cmake --build build --parallel 4

.venv/bin/dhdgs validate-config --config configs/dhdgs_pilot.yaml
.venv/bin/dhdgs inspect --config configs/dhdgs_pilot.yaml
.venv/bin/dhdgs build --config configs/dhdgs_pilot.yaml
.venv/bin/dhdgs query --config configs/dhdgs_pilot.yaml
.venv/bin/dhdgs benchmark --config configs/dhdgs_pilot.yaml
.venv/bin/python -m unittest discover -s tests -v
```

上述pilot命令从 `seq_2M` 均匀抽取 4096 条 2560 维向量，构建三个分片。`build` 拒绝覆盖已有 `paths.output/index`；已有测试索引只有在构建参数与 YAML 一致时才能直接执行 `query` 和 `benchmark`；例如将 RaBitQ 从 4 bit 改为 1 bit 后，需要新建输出目录并重建。新建实验索引前修改 YAML 的 `paths.output`。每次查询、检查或 benchmark 自动生成独立的 `runs/<操作>_<时间>_<随机标识>/`，保存可重新加载的 `resolved.yaml`、配置来源哈希和 `report.json`。

所有相对路径均相对于 YAML 文件位置。未知、重复、缺失字段和非法参数组合均报错，已支持的旧配置按显式迁移规则补全并记录。`runtime.blas_threads` 控制 Python 数值库线程数。`search.max_inflight` 在 legacy peer 模式控制传输工作线程数，在 reference 模式控制单查询并行分片请求数；optimized peer 使用 `peer.communication.transfer_workers`。CMake 构建类型和编译进程数属于开发工具设置，不是检索实验参数。

## 配置与实验

| YAML 分组 | 主要控制内容 | 修改后是否重建索引 |
|---|---|---|
| `paths`、`data` | 输入、输出、距离、维度、索引样本量、处理块大小 | 距离/维度/样本量需要；更换数据需建立新索引 |
| `seed`、`partition` | 随机种子、归属函数、ρ、最大副本数、训练和校准样本、k-means | 需要 |
| `build` | Vamana 度数、候选集、剪枝 alpha、内存预估上限 | 图参数需要；内存上限不需要 |
| `quantization` | RaBitQ 总位数，含符号位，支持 1–9 位 | 需要 |
| `storage` | 节点页大小、应用缓存、异步 I/O 后端/队列/字节预算 | 页大小需要；缓存和 I/O 参数不需要 |
| `routing` | 样本比例与数量、HNSW M/efConstruction/efSearch、备用入口候选数 | 采样和建图参数需要；查询参数不需要 |
| `search` | top-k、frontier、全局扩展预算、本地批量、并发、self 排除、备用入口 | 不需要 |
| `peer`、`peer.communication` | 计算线程、租约/接纳、通信模式、派发与控制线程、待发送上限、连接复用与容量 | 不需要；数据节点和入口配置必须一致 |
| `scheduler` | adaptive/reference/local_first、收益和不确定性权重、等待阈值、冷启动成本、EWMA | 不需要 |
| `network`、`cluster` | local/tcp、批大小、超时、帧大小、重试、节点地址、分片目录 | 改分片数需要；部署和通信参数不需要 |
| `query` | 查询文件、可选全局 ID 文件、查询起始行 | 不需要 |
| `metrics` | 详细 trace 导出、进程资源采样间隔与缓冲上限 | 不需要 |
| `assignment`、`experiment` | 归属计划输入模型、几何实验参数列表、基准查询数、精确计算块大小、目标召回 | 不需要；作用于相应实验命令 |

查询会核对构建参数与索引元数据；修改位数、分片函数等后直接使用旧索引会报错，避免“配置变了但实际实验没有变”。`row_limit: 0` 表示全库，但当前内存建图器会先检查预估开销；不要据此认为已经具备全库或百亿规模构建能力。

`partition.overlap_fraction` 即 ρ，控制校准样本中多归属点的比例；应用到其他数据时应报告实际比例。**RF 是各分片向量总份数 / 唯一向量数**，不是服务器数量。例如 10% 的点有两个副本、其余只有一个副本，则 RF=1.1；若其中部分点有三个副本，RF 更高。两者分别记录在索引 manifest 中。

```sh
.venv/bin/dhdgs partition-study --config configs/dhdgs_pilot.yaml
.venv/bin/dhdgs assign --config configs/dhdgs_pilot.yaml
```

几何实验使用 `experiment.metrics/overlap_fractions/seeds`，保存到 `partition/seed_<seed>/`；训练、校准和评估 ID 互不重叠并排除官方查询 ID。`assign` 读取 `assignment.model/threshold`，流式生成全库归属计划，不执行网络 shuffle。其 metric、方法和 ρ 以所选模型与阈值文件为依据，配置必须匹配。几何实验目录和归属计划目录同样拒绝覆盖。

`benchmark` 从 `query.row` 开始取 `experiment.benchmark_queries` 条查询。它对**实际入索引的数据**分块重算精确 top-k，不能用全库 top-100 标签过滤来冒充子集真值。返回 Recall@k、逐查询结果、延迟分位数、扩展数、RPC 数、读取量与备用入口触发率。benchmark 先准备精确真值，再在独立计时窗口按配置并发执行查询。输出实测窗口 QPS；少量查询的 p99 仅是样本描述，不能据此声称达到稳态饱和吞吐。默认距离是探索性的原始平方 L2；`cosine` 使用归一化后 float32 向量的平方 L2，返回的 `squared_distances` 仍是该值，约等于两倍 cosine distance。

## 三个独立数据服务

将 YAML 的 `network.transport` 改为 `tcp`，分别在三个终端运行：

```sh
.venv/bin/dhdgs serve --config configs/dhdgs_pilot.yaml --node-id 0
.venv/bin/dhdgs serve --config configs/dhdgs_pilot.yaml --node-id 1
.venv/bin/dhdgs serve --config configs/dhdgs_pilot.yaml --node-id 2
```

再执行 `query` 或 `benchmark`。默认地址是本机回环地址，测试不依赖另外两台服务器。服务启动时输出监听端点；Ctrl-C 停止。peer 模式只在入口提交和跨节点分支时传输查询与种子，节点内部直接调用本地扩展；各分支主动回传最终结果，顶层负责保活及合并。协议校验索引版本、配置、预算、目的节点和消息长度；失败显式上报。

真实多机部署时，设置各节点可达的 `host/port/index_directory`，将构建完成的 `index/shard_<id>` 完整分发至相应节点，路由端保留根目录文件。构建命令始终在本地输出分片，不自动复制到远端自定义目录。各机器需要对应环境及原生模块；RPC 当前用于可信私有网络，没有 TLS/身份认证。物理三机部署、分片传输、网络检查及测试可使用 [部署流程](DEPLOYMENT.md)，执行结果以对应测试报告为准。

## 当前实现边界

- HNSW 使用参考库所带 hnswlib；RaBitQ 使用其真实 core API、FHT/Kac 旋转、残差中心和校正因子，并持久化实际旋转状态。估距误差尺度只参与调度，**不是确定性距离下界或召回保证**。见 [THIRD_PARTY.md](THIRD_PARTY.md)。
- C++ Vamana 实现随机初始化、贪心候选搜索、RobustPrune 和反向边更新。Python 构建器合并重复顶点邻接表，额外保留分片内双向环和中心 MST 桥接，并检查全局强连通。最终度数可能超过 `build.degree`，以各片 manifest 的 `max_degree` 为准。保护边属于当前原型的固定结构规则，其效能需单独评估。
- 节点文件包含 float32 原向量及 uint64 全局邻居 ID，按页对齐，以 `pread` 读取并精排；量化码和位置映射在内存中。计数是文件读取调用/字节，不能当作物理 SSD I/O，应用缓存关闭也不等于 OS 缓存关闭。
- 搜索从一个 HNSW 样本入口开始，支持自适应批调度、去重、在途请求、备用入口及全局预算。有限 frontier 会淘汰候选，并记录数量；连通性不意味着有限预算下一定高召回或几跳收敛。
- 默认 `search.execution: peer` 由数据节点自主搜索，每节点最多向一个未访问节点派发。派发后原节点继续本地搜索，向新节点派发的权限交给后继；查询携带 `visited_nodes`，每台数据服务器至多启动一次。顶层只负责入口、保活及最终合并。`reference` 保留旧协调器作为实验对照。peer 使用顶点快照去重，仍可能在不同节点上重复扩展副本；重复工作计入预算。节点回访计数应始终为 0。`network.transport: local` 是顺序执行的进程内模拟，真实异步测试使用 `tcp`。
- DESIGN 第 7 节的效用通过 `scheduler.remote_weight` 与 `probability_temperature` 转为远端触发概率；超时和本地耗尽可强制发送。`peer.max_active_queries` 当前实际限制未确认完成的分支数，`handoff_timeout_seconds` 为分支传输 RPC 超时。尚未实现查询上下文缓存、预算回收及故障恢复。
- 调度成本用实测服务时间按节点数归一化更新，并估计 TCP 额外开销；带宽仍由 YAML 给定。等待阈值是有可用发送槽时的优先级规则，无法保证在途请求或过载期间的硬延迟上限。备用入口的净额外成本须用开/关对照实验衡量。
- 构建保留选中数据及图在 RAM 中，查询位置目录及分片远端邻居码也未做大规模有界化；当前实现用于算法正确性与小规模实验，不是百亿数据部署方案。

完整研究设计和待验证方法分别见 [DESIGN.md](DESIGN.md)、[EXPERIMENTS.md](EXPERIMENTS.md)。本阶段测试和真实数据结果见 [PHASE2_RESULTS.md](PHASE2_RESULTS.md)，历史几何实验见 [PHASE1_RESULTS.md](PHASE1_RESULTS.md)，硬件调查见 [HARDWARE.md](HARDWARE.md)。

## 指标记录与本机多节点测试

已实现逐查询延迟分解、召回、协议字节/RPC、文件读取、距离计算、CPU、发现路径与跳数，以及节点资源采样、并发 QPS 和构建阶段记录。输出 JSON、JSONL 与 CSV；字段含义、并行归因及未测量项见 [METRICS.md](METRICS.md)。

`network.transport: tcp` 且节点均为回环地址时，可以使用 `.venv/bin/dhdgs local-cluster-test --config <实验 YAML>` 自动启动数据服务、运行测试并清理进程。真实数据单机三进程测试结果见 [METRICS_TEST_RESULTS.md](METRICS_TEST_RESULTS.md)。

新架构的单机多节点测试记录统一放在 [test/](test/README.md)，每次测试独立归档配置、日志、逐查询指标和汇总；旧结果文档描述当时的协调器架构。

测试同时导出 `query_stage_means.csv`（整组测试的各阶段平均耗时）和 `per_query_stage_means.csv`（每条查询重复运行的阶段均值），端到端阶段与累计节点工作分别记录。

## 节点内异步读取

在完整 YAML 中设置 `peer.execution_model: resumable`、`search.execution: peer` 和 `network.transport: tcp`，使用 `storage.io.backend: io_uring` 开启原生异步读取；`thread_pool` 提供有界 I/O 线程池对照。默认仍为 `blocking` 同步基线。CMake 检测 liburing；未编译支持或启动探测失败时，按 `storage.io.fallback_backend` 明确报错或回退，并记录实际后端。所有新调节项均在 [configs/dhdgs.yaml](configs/dhdgs.yaml)，切换无需重建索引。

查询等待读盘时释放计算线程；一个本地 beam 完整消费后才进入下一次搜索决定。I/O 完成、容量释放和等待计时不会直接派发远端查询。新增 decision ID 可审计每个远端分支的搜索批准来源。当前跨 query 聚合的是独立读请求提交，尚不减少记录读取量，也未合并连续区间或复用在途读取。

`*_per_device` 当前表示该数据进程分得的设备额度；同机多进程共用 SSD 时需在 YAML 中分配保守配额，启动器报告合计，不会自动创建主机级限流器。实现细节见 [DESIGN.md 第 6.12.12 节](DESIGN.md#61212-首阶段实现范围与使用方式2026-10-17)，测试见 [异步 SSD 测试记录](test/async_ssd_20261017/SUMMARY.md)。

旧 v1 配置若同时缺少 `peer.execution_model` 与 `storage.io`，会按 blocking 兼容加载并在快照记录补齐过程；其他字段继续严格校验。

物理三机自动部署入口：`.venv/bin/python tools/deploy_cluster.py --plan <deployment.yaml> --phase all`。全部部署与运行参数从一个 YAML 生成，远端离线安装、独立编译、测试后自动清理本次服务；详见 [DEPLOYMENT.md](DEPLOYMENT.md)。

完整 seq_2M 的集中构建与测试使用 [FULL_DATASET.md](FULL_DATASET.md) 中的流程：支持分片内并行 Vamana、分片检查点、完整索引容量预检、流式部署打包、随机且互不重叠的校准/验证查询，以及已有全库真值的精确抽查。主配置与记录位于 `test/seq2m_full_20261017/`，不会覆盖历史 pilot 实验。

数据节点TCP收发与计算职责隔离：新配置默认 `peer.sender.mode: queued`，预启动独立的远程派发和控制发送线程。计算线程仅提交消息，通信线程负责连接、编码、发送及等待ACK；连接处理线程负责接收和协议回复。`peer.sender.transfer_queue_capacity`限制等待派发的任务数，队列满明确失败；`peer.communication.transfer_workers/control_workers`控制发送并发。旧YAML缺少sender组时迁移为executor对照模式。此设置在optimized通信模式生效，不改变远端扩展算法，也不把一个查询分支拆给多个计算线程。详细设计见DESIGN第11.11节。

C++完成报告和控制发送路径已实现。在完整实验YAML中设置`peer.control.scheduler: native`，同时使用`peer.execution_model: native`、`network.transport: tcp`、`storage.io.backend: sync_pread`、`peer.communication.mode: optimized`、`reuse_connections: true`和`peer.control.completion_fast_path: false`。重新编译原生模块、重启服务即可使用，不需要重建索引。该路径用一个原生事件线程和一个TCP reactor替代Python完成/控制发送队列；接纳、远程分支派发和入口merge仍由Python执行。通用默认保持event以兼容其他后端。

可复用的完整配置、222项回归和2000查询ABBA对照见 [C++控制路径测试报告](test/native_control_20261018/REPORT.md)。同负载p99约降低25.9%，报告控制等待大幅缩短，但接纳队列仍未满足100ms系统等待目标。

负载均衡研究原型：见 [R001实现与配置](research/R001_IMPLEMENTATION.md)、[质量/性能实验](test/load_balance_20261026/REPORT.md)。`load_balance.enabled` 默认关闭；只有独立质量及来源净卸载校准通过的副本才能分流。
