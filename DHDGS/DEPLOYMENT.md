# 三台物理机器的部署与测试

当前部署已调整为 **Intel-35：head + 数据节点1；Intel-30：数据节点2、3**。每节点32个查询工作线程、客户端并发128，见 [最新部署命令](test/managed_35_30_20261019/README.md)。

2026-10-19：intel-30 当前持久服务版本的代码已迁移至 `/sharenvme/xulihan/gene_database/DHDGS`。旧 release 的代码路径保留兼容软链接；索引、共享 Python 环境和历史实验仍在原位置。详见 [迁移记录](test/relocation_intel30_20261019/README.md)。下文旧批次部署路径属于历史版本。

**持久服务入口（2026-10-19）**：需要独立 head/data 进程、通过 head 热改查询参数并拉取节点结果时，使用 [新部署操作手册](test/managed_cluster_20261019/README.md) 和 [本轮验证报告](test/managed_cluster_20261019/REPORT.md)。下面保留的是旧版每轮启动/停止的批次部署流程。

部署入口是 `tools/deploy_cluster.py`，节点辅助程序是 `tools/deploy_node.py`。完整输入采用一个 YAML：`deployment` 管理 SSH、目录、依赖版本、构建与测试组合，`dhdgs` 包含全部现有运行参数。脚本从这个文件生成各机完整配置和每组 benchmark 的配置快照，避免手工修改三份搜索参数造成配置签名不一致。

当前实验输入见 [test/three_hosts_20261017/deployment.yaml](test/three_hosts_20261017/deployment.yaml)。本机同时承担入口、压测客户端和数据节点 0；intel-32/30 分别承担数据节点 1/2。第四台入口机尚未参与本次部署。

完整 200 万条 seq_2M 的集中构建、检查点、独立查询校准与三机测试入口见 [FULL_DATASET.md](FULL_DATASET.md)。旧三机实验目录保留为 4096 条索引的历史记录，不用于覆盖生成全量实验。

## 执行

在 DHDGS 目录中：

```sh
.venv/bin/python tools/deploy_cluster.py \
  --plan test/three_hosts_20261017/deployment.yaml --phase all
```

也可以先 `--phase prepare`，确认准备成功后再 `--phase run`。prepare 成功时生成 `prepared.json`；再次 prepare 会核对 YAML、源码与索引哈希，匹配则复用，不重复安装。run 阶段再次核对源码与 YAML，防止混合配置。已有运行记录不覆盖；新实验应建立新的测试目录、复制并修改主 YAML，为远端也指定新的实验根目录。

## 自动完成的步骤

1. 打包 DHDGS 源码及未改动的 RaBitQ 参考库，排除旧虚拟环境、构建产物和原生二进制。
2. 向各远端仅上传自己的完整分片，以及旧索引需要的全局 `ids.npy`/`membership.npy`。不上传整个原始数据集。上传文件逐项 SHA-256 核验。
3. 在各远端独立目录创建 venv，本机下载 YAML 指定版本和目标 Python ABI 的 wheel 并随包上传，远端使用 --no-index 离线安装，再 Release 编译原生模块。无系统软件安装；要求 Python 开发头文件、编译器、CMake、liburing 开发库已经可用。
4. 真实读取一小段索引文件，验证 io_uring 可用且返回字节与 pread 一致。失败会显式停止准备，不静默回退。
5. 每种后端启动三机服务，确认真实主机名、PID、监听地址。通过三机分别连接所有数据端口和入口临时回调端口，检验完整数据通路和回传通路。
6. 每个并发设置先运行单查询冒烟测试，再运行 benchmark；导出逐查询结果、阶段均值、节点资源、网络与预算审计。
7. 每个后端测试后停止本次服务，再进入下一种后端；最终保存完成状态。

每个服务有一个跟随 SSH stdin 生命周期的 supervisor。正常结束发送 stop；SSH EOF 同样清理它启动的子进程。只使用该子进程的独立进程组，不按进程名或端口批量杀进程。清理使用 SIGINT 等待退出，必要时才针对该进程组升级终止。数据服务 stdout/stderr 和退出状态回传本机，远端也保留一份。如果远端退出确认丢失，脚本报告清理未确认，不能把它当作已停止。

## 配置注意事项

- `cluster.nodes[].host` 是数据机实际绑定并供其他机器连接的地址。本次使用 192.168.33.x IPoIB；TCP over IPoIB 不等于 RDMA。
- `peer.head_advertise_host` 必须是各数据机可达的入口地址；本次为 192.168.33.35。`head_bind_host=0.0.0.0`、`head_port=0` 支持每查询动态回调。仅放通数据服务端口不足以支持结果回传。
- 节点目录属于各机本地文件系统；即使挂载路径名字相同也不会自动共享索引。
- 全局节点表以及 search、scheduler、quantization、data 和大多数 peer 字段必须一致。当前 `peer.worker_threads` 也属于执行签名，不能单独给某一台机器改值。各机路径字段允许不同。
- 本机 benchmark 需要访问原始数据和查询文件，用实际索引 ID 计算精确真值；远端 serve 不需要原始数据。更改部署或 I/O 参数不重建索引；更改距离、分片数、图或量化构建参数需要新索引。
- 主 YAML 的 `deployment.client_concurrencies` 只改变入口发压；每种后端的服务不因客户端并发变化重启。
- 每种后端在相同缓存/搜索配置下运行。`storage.io.*_per_device` 是该数据进程的额度，不是跨机器共享限流。三个物理数据机各自访问本机 SSD。
- Python/依赖版本、机器型号和缓存状态须随结果保留。远端 Python 3.10 与入口 Python 3.12 的环境不同，不能把本轮测试解释为严格隔离硬件因素的实验。

## 文件与解释

每个后端目录内有 `services/`、各节点 `network_node_*.json`、`c1/c4/c8/`。每个并发点保存 `experiment.yaml`、`smoke.json`、`audit.json` 和 `run/` 中的完整报告、JSONL、CSV、配置来源。主目录保存准备日志、payload 清单、prepared.json、comparison.json 和 completed.json。

network 记录的 `tcp_connect_ms` 是应用层建立 TCP 连接的耗时，不是独立测得的链路 RTT。节点进程的存储读量和文件请求数分开报告；小索引/刚传输的索引可能全部由页缓存满足。第一次部署优先检验无失败、预算守恒、无回访、每个远端分支都有决定函数批准；满足召回后再按 p99 优先、QPS 次之评价性能。

本轮结果见 [三机测试报告](test/three_hosts_20261017/SUMMARY.md)。

构建会将 CMake 选定的 liburing 实体库放入原生模块旁的 `.native_libs/`，并设置模块运行时查找路径。此举用于避免某些主机开发链接指向新版、动态加载器却根据相同 SONAME 加载旧版库的 ABI 混用；不修改系统库。原生模块和私有运行库都在目标机器构建/选取，部署源码包不复制本机二进制。

远端 `deployment.hosts[].cxx` 显式选择与系统 C++ 运行库匹配的编译器，`build_directory` 指定独立构建目录。当前两台远端使用 `/usr/bin/g++`，避免默认 `/usr/local/bin/c++` 的 GCC 14 产物要求系统尚未提供的 GLIBCXX 版本。改变编译器应使用新构建目录，不复用不兼容的旧 CMake 缓存。

## 使用已经安装好的三机环境手动运行

自动实验结束后服务会停止，代码、venv、索引和配置保留。以下命令可在各机对应终端前台启动同步服务；如需原生异步，将配置名 `blocking` 改成 `io_uring`，三台同时切换。新服务启动前确认上一次测试已结束。

本机，在原 DHDGS 目录：

```sh
.venv/bin/dhdgs serve \
  --config test/three_hosts_20261017/configs/blocking_node0.yaml --node-id 0
```

intel-32：

```sh
cd /sharenvme/userhome/xulihan/dhdgs_deploy/three_hosts_20261017/gene_database/DHDGS
.venv/bin/dhdgs serve \
  --config /sharenvme/userhome/xulihan/dhdgs_deploy/three_hosts_20261017/configs/blocking.yaml --node-id 1
```

intel-30：

```sh
cd /sharenvme/xulihan_dhdgs/three_hosts_20261017/gene_database/DHDGS
.venv/bin/dhdgs serve \
  --config /sharenvme/xulihan_dhdgs/three_hosts_20261017/configs/blocking.yaml --node-id 2
```

三个服务启动后，本机运行：

```sh
.venv/bin/dhdgs query \
  --config test/three_hosts_20261017/configs/blocking_node0.yaml
.venv/bin/dhdgs benchmark \
  --config test/three_hosts_20261017/configs/blocking_node0.yaml
```

query/benchmark 会生成新的 `runs/<操作>_<时间>_<标识>/`，可以重复运行，不覆盖已有报告。手动前台服务通过 Ctrl-C 正常停止。自动部署脚本用于新建整套独立实验；已完成的自动实验目录不重复覆盖。
