# 三数据节点硬件调查

日期：2026-10-16（Asia/Shanghai）。通过本地命令与用户授权的 SSH 只读检查得到，未运行 fio/iperf、未安装远端软件、未部署服务。CPU 和 RAM 为系统可见值，不等于已经预留的进程资源；空闲容量会变化。

拓扑按最新说明采用当前机器、intel-32、intel-30 三个数据节点，第四台尚未就绪的机器作为独立 HNSW 路由节点。见 [硬件发现记录](reports/hardware_discovery_20261016.json)；当前唯一运行配置见 [dhdgs.yaml](configs/dhdgs.yaml)。

| 项目 | 当前机器 | intel-32 | intel-30 |
| --- | --- | --- | --- |
| hostname | sugon-intel-four-numa | cxl-four-numa | intel-bigmem-nvm-stratix |
| CPU | Xeon Gold 6448H | Xeon Gold 6448H | Xeon Platinum 8358P |
| 插槽 / 物理核 / 逻辑线程 | 4 / 128 / 256 | 4 / 128 / 256 | 2 / 64 / 128 |
| NUMA 节点 | 5，其中一个无 CPU | 4 | 4 |
| 系统可见 RAM（十进制） | 2.302 TB | 1.622 TB | 2.164 TB |
| SIMD | AVX2、AVX-512 | AVX2、AVX-512 | AVX2、AVX-512 |
| /sharenvme 设备 | nvme2n1，P5910CH0384M00 | nvme1n1，SAMSUNG MZQL23T8HCLS-00B7C | nvme1n1，INTEL SSDPF2KX038TZ |
| 设备标称容量 | 3.841 TB | 3.841 TB | 3.841 TB |
| /sharenvme 可用空间快照 | 约 64.2 GB | 约 50.7 GB | 约 252.1 GB |
| 管理地址 | 172.16.33.35 | 172.16.33.32 | 172.16.33.30 |
| 候选数据地址 | 192.168.33.35 | 192.168.33.32 | 192.168.33.30 |

## 网络

- 当前 SSH 地址经 172.16.33.0/24 以太网路径：当前机器 bond0、intel-32 enP1s11f0、intel-30 eno1，sysfs 显示 1000 Mb/s。
- 三台均发现 UP 的 IPoIB 接口和 192.168.33.x 地址，相应接口/端口报告 200 Gb/s。intel-30/32 到 192.168.33.35 的路由选择相应 IB 接口。
- 这些是接口配置与标称速率，没有验证全互通、应用可用端口、RTT 或吞吐。TCP over IPoIB 不等于 RDMA，也不能直接以 200 Gb/s 填入性能结论。
- 后续在正式通信实验中测量三台之间的实际小消息 RTT、大块传输、并发吞吐及排队，分别记录管理网络/数据网络。调度成本模型使用实测值。

## 存储与部署约束

用户指出 2M 数据三分片的空间足够，这与容量计算一致：原向量每台均值约 6.83 GB，RF=1.2 时约 8.19 GB。按每个节点记录约 3 个 4 KiB 页估算，RF=1.2 时每台约 9.83 GB，边界 sidecar、编码、模型、临时构建文件另计。当前余量支持这一轮小规模实验，不因长期容量问题阻塞 2M 实验。

避免在每台复制完整 20.48 GB 输入再另外生成多个全量索引；使用分片流式传输、独立实验目录与显式空间预算。初始阶段只生成小型模型、ID 和分片计划。

intel-30 的登录目录为 /home/xulihan，其根文件系统只剩约 7.45 GB，不能把数据索引默认写到该目录。intel-32 的登录目录为 `/sharenvme/userhome/xulihan`（userhome 为单数），位于本地 NVMe。两台远端都没有本机的 `/sharenvme/usershome/xulihan` 路径；最终实验输出目录仍待确定。不能把相同挂载点名称误认为同一共享文件系统。

三机 CPU/NUMA/内存不一致。构建和并发实验需记录每机线程配额、NUMA 绑定与缓存预算，不能使用总核心数作为相同服务能力的假设。

本地已创建 DHDGS 项目内 `.venv` 并安装 NumPy/打包工具；未修改系统 Python。intel-30 已确认 Python 3.10.12、GCC 12.3.0、CMake 3.22.1；intel-32 已确认 Python 3.10.12、GCC 14.2.0、CMake 3.22.1。第四台路由节点的地址和资源尚未知。
