# 完整 seq_2M 的集中构建与三机测试

本流程在 Intel-35 上读取全部 2,000,000 条、2,560 维 float32 向量，集中进行聚类、归属计算、建图、重合顶点邻接表合并及 HNSW 构建，随后将各数据分片部署到 Intel-35、Intel-32、Intel-30。没有原始数据预分发或网络重分片步骤。构建仍属于内存构建，不是百亿规模的外存构建器。

初始构建主配置为 [test/seq2m_full_20261017/deployment.yaml](test/seq2m_full_20261017/deployment.yaml)，修复 TCP 监听队列后的三机复测主配置为 [backlog128/deployment.selected.yaml](test/seq2m_full_20261017/backlog128/deployment.selected.yaml)。每次实验由一个主 YAML 驱动：`dhdgs` 包含索引、搜索和节点参数，`deployment` 包含部署与压测参数，`full_run` 包含并行构建、磁盘预留、检查点、真值验证与预算校准参数。派生的构建配置及选中配置自动保存，不需手工维护多套参数。

## 构建

在 DHDGS 目录执行：

```sh
.venv/bin/python tools/build_full.py \
  --plan test/seq2m_full_20261017/deployment.yaml --phase preflight
.venv/bin/python tools/build_full.py \
  --plan test/seq2m_full_20261017/deployment.yaml --phase full
```

`data.row_limit=0` 表示完整语料；预检规模由 `full_run.preflight_rows` 决定。构建先校验内存预算，完成归属后根据实际副本数、记录对齐、图度数上界、量化编码和路由规模检查完整索引磁盘预算，并额外保留 `reserve_disk_bytes`。磁盘检查不是空间预分配，运行期间仍需避免其他任务大量占用同一文件系统。

`shard_workers` 控制同时构建的分片数，`graph_threads` 控制每个分片内部的线程数。每个顶点的邻接表使用独立互斥锁；读取候选邻接表时复制快照，替换出边和反向边剪枝时持有对应顶点的锁，不同时持有两把图锁。线程完成一个剪枝 alpha 阶段后汇合，再开始下一阶段。单线程仍按固定种子顺序执行；多线程的更新交错使图结构不保证逐字节可复现，实验必须保留实际索引哈希并验证召回率。

原生构建支持每阶段进度回调，`build/progress.jsonl` 保存时间戳、分片、alpha 和已完成顶点数。每个分片建图完成后将邻接表以紧凑数组原子写入检查点。检查点键包含数据身份、ID、归属、构建参数、原生二进制哈希和建图线程数，输入不一致不会复用。检查点保存已完成的分片，不保存正在构建的中间图。现有输出目录不覆盖；失败后应使用新的构建输出目录并指向原检查点目录。

并行图任务重叠，`build_profile.phases` 的 `shard_*_graph_and_union` 表示主构建线程的等待/合并阶段，不是每个建图线程的独占耗时。各分片实际建图墙钟时间保存在 `graph_complete` 事件和检查点中，整体构建时间以最终 manifest 为准。

`DHDGS_NATIVE_OPTIMIZATION=ON` 为构建主机启用本机指令优化和双精度距离归约 SIMD。每台远端独立编译自身原生模块，不复制 Intel-35 的机器码；RaBitQ 存储格式保持不变。

## 真值、校准与独立验证

```sh
.venv/bin/python tools/full_truth.py \
  --plan test/seq2m_full_20261017/deployment.yaml
.venv/bin/python tools/calibrate_full.py \
  --plan test/seq2m_full_20261017/deployment.yaml
```

本轮采用未归一化原始 L2、排除查询自身，复用数据集提供的 Euclidean top-100 真值。验证脚本检查查询 ID、文件范围、距离定义和文件身份，抽取校准与验证区间的查询，对全部 200 万条向量进行直接差分、float64 精确扫描，核对 top-k。其他提供的真值行没有全部重新计算，报告须保留这一范围限制。供应真值只能用于完整 ID 集合，不能用于 pilot 子集索引。

校准使用独立的 128 条查询，在 Intel-35 上启动三个真实 TCP 数据进程，按 YAML 中递增的预算/候选队列组合测试。查询通过 `full_run.query_selection_seed` 从完整 20 万查询中无放回随机抽样，校准与验证集合互不重叠，实际行号保存于 `query_selection.json`；这些显式行号覆盖连续查询的 `query.row` 设置。首次达到校准目标且无失败、指标完整的配置生成 `deployment.selected.yaml`。该选择只是预算校准，不是穷尽参数组合后的最优 p99 证明，也不能代替物理三机独立验证。

随后用不同的 1,024 条查询进行三机测试，报告平均 Recall@10、逐查询召回和延迟、p99、QPS、阶段均值、节点资源与通信记录。若独立验证未达到目标，必须如实记录，不能将校准结果当作通过；进一步调参应另外保留未用于调参的验证查询。

`calibration_target_recall` 是停止增加预算的扫描阈值，可以略高于最终要求，用于保留更高召回的对照。物理部署前，按照用户要求的 `experiment.target_recall`，从所有成功且指标完整的校准点中选择最低 p99，再比较 QPS。选择记录保存为 `calibration/deployment_selection.json`，扫描停止时的配置另存为 `deployment.calibration_stop.yaml`，实际部署使用重新生成的 `deployment.selected.yaml`。最终召回是否达标仍以独立查询为准。

## 三机部署

```sh
.venv/bin/python tools/run_full_deployment.py \
  --plan test/seq2m_full_20261017/deployment.selected.yaml --phase all
```

分片校验和压缩使用流式读取，不将整个分片装入打包缓冲区。`deployment.payload_directory` 指向 `/tmp` 所在另一块 SSD，避免完整索引和两份远端传输包同时挤占数据 NVMe。压缩等级也由 YAML 指定。主测试目录保存配置、清单、日志和结果；另一块 SSD 只用于临时传输包。

`deployment.reuse_remote_index=true` 可在复测时跳过分片上传，但远端仍逐文件校验本地索引哈希；缺失或不匹配会失败。它只适用于已经部署了相同索引的机器，全新部署须设为 false。每次复测使用新的模式名和结果目录，保留旧服务日志。

`network.listen_backlog` 控制数据服务与入口回调的 TCP 待接收队列，当前标准配置为 128。旧 YAML 缺失该字段时保留 Python 原默认值 5，并在配置快照记录兼容迁移。控制 RPC 出错时，逐查询 JSON 额外记录 `control_error_details` 中的操作、节点及异常类型；有错误的请求仍标记为指标不完整，不通过修改计数掩盖错误。

数据节点代码、venv 和分片安装到各机独立的 `seq2m_full_20261017` 目录，旧 pilot 记录与索引不覆盖。服务由 SSH stdin 绑定的 supervisor 管理，结束时只清理本次启动的服务。物理三机流程及手动服务运行方式亦见 [DEPLOYMENT.md](DEPLOYMENT.md)。

本流程不修改远端扩展选择规则。I/O 调度仍只作用于本节点，远端分支须由扩展决定函数批准；父节点继续本地搜索，已经访问的数据节点不再启动。

应用缓存关闭不等于操作系统页缓存关闭。刚写入或传输的索引可能命中页缓存；文件请求字节、进程物理存储读量和实际设备等待必须分别解释，不能把 pread 时间直接称为纯 SSD 延迟。

## 测试结束后启动已安装服务

自动实验结束会停止服务，安装环境和索引保留。在三个终端分别运行：

Intel-35，进入本项目 DHDGS 目录：

```sh
.venv/bin/dhdgs serve \
  --config test/seq2m_full_20261017/backlog128/configs/blocking_backlog128_node0.yaml --node-id 0
```

Intel-32：

```sh
cd /sharenvme/userhome/xulihan/dhdgs_deploy/seq2m_full_20261017/gene_database/DHDGS
.venv/bin/dhdgs serve \
  --config /sharenvme/userhome/xulihan/dhdgs_deploy/seq2m_full_20261017/configs/blocking_backlog128.yaml --node-id 1
```

Intel-30：

```sh
cd /sharenvme/xulihan_dhdgs/seq2m_full_20261017/gene_database/DHDGS
.venv/bin/dhdgs serve \
  --config /sharenvme/xulihan_dhdgs/seq2m_full_20261017/configs/blocking_backlog128.yaml --node-id 2
```

随后在 Intel-35 的 DHDGS 目录执行一次查询：

```sh
.venv/bin/dhdgs query --config test/seq2m_full_20261017/backlog128/configs/blocking_backlog128_node0.yaml
```

Ctrl-C 可正常停止各前台服务。单次 query 使用 YAML 的 `query.row`；随机验证集的行号以 `query_selection.json` 为准。普通 `dhdgs benchmark` 会逐查询重新执行全库精确扫描；本轮大规模测试使用 `run_full_deployment.py` 接入已验证的供应真值，以避免重复扫描。自动测试目录不覆盖，新的自动实验应使用新的目录和配置，保留既有结果。
