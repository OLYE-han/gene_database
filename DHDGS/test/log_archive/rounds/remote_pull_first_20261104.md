<!-- experiment:remote_pull_first_20261104:start -->
## 普通搜索优先RDMA拉取与连续访问派发（remote_pull_first_20261104）

**状态**：已完成诊断、修复与复验；新策略整体收益未证实

**开始时间**：2026-11-04T14:36:48+08:00

**结束时间**：2026-11-04T15:49:42+08:00

**目的 / 要解决的问题**：检验不以网络成本推迟更好远端候选是否减少搜索工作；排查异步拉取与续跑开销

**假设**：短暂远端需求拉取、持续需求派发，在固定线程下可流水并保持预算

**期望效果**：保留质量、减少无效计算或查询延迟，吞吐收益待实测

**环境与配置 / 对照**：无LB；Intel35入口/客户端，31/32/30数据；D200、beam11、64线程、200K自然查询

**本轮改变的变量**：原策略/距离优先直接push/pull2；短诊断C512按源FIFO与1/8 CQ；原负载C1536最终复验及续跑CPU份额0.5/0.8、0.8重复；总QP/搜索预算/搜索线程固定

**代码改动**：普通base距离优先RDMA拉取、连续同源切换、异步固定池续跑；按源FIFO派发与CQ分片；按源及pending分配引擎；锁/进度观测、取消回归、schema/YAML。生产模块本机已编译，三机冻结测试部署完成。DESIGN/LOCKS/Research更新。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：10组正式测量共4,320,000请求零失败/超预算；完整配置C1536/D200/64线程，最终Recall约97.23%；锁等待不是主要驻留，持续带宽未饱和，已解除派发与完成进度耦合。原矩阵暂停后未继续pull4/8。最终生产54项回归，三机各47项及各1项取消回归通过。其他实验占用期间误启服务在发查询前已停止，有效查询流无重叠；首次保存的configure阶段为日志起点，源码编辑更早未单独记录。 同轮离线补图：p50附近79.889ms、p99附近541.739ms，新增同机端到端、各分支拆分与根分支拉取时序；未新增压测。 离线语义审查：唯一区域执行器并非唯一branch_id；p99八份预算授权合计展开198、重建未用2，守恒但重复授权排队；严格每节点唯一分支语义尚待修复，未新增实验。

**报告、图表与证据**

- test/remote_pull_first_20261104/REPORT.md
- test/remote_pull_first_20261104/original_p99_timeline.png
- test/remote_pull_first_20261104/repaired_p99_breakdown.png
- test/remote_pull_first_20261104/diagnosis_comparison.png
- test/remote_pull_first_20261104/final_source_identity.json
- test/remote_pull_first_20261104/query_load_overlap_audit.json
- DESIGN.md#remote-pull-first-20261104
- LOCKS.md
- Research.md
- test/remote_pull_first_20261104/latency_samples/REPORT.md
- test/remote_pull_first_20261104/latency_samples/p50_breakdown.png
- test/remote_pull_first_20261104/latency_samples/p99_breakdown.png
- test/remote_pull_first_20261104/latency_samples/p50_breakdown.svg
- test/remote_pull_first_20261104/latency_samples/p99_breakdown.svg
- test/remote_pull_first_20261104/latency_samples/breakdown.json
- test/remote_pull_first_20261104/latency_samples/outer_stages.csv
- test/remote_pull_first_20261104/latency_samples/branch_stages.csv
- test/remote_pull_first_20261104/latency_samples/plot_samples.py
- research/R005_BRANCH_BUDGET_AUDIT_20261104.md

**下一步建议**：默认保留原策略；分解细碎批次维护与重复区域授权CPU，设计查询年龄/续跑公平，再讨论累计远端需求语义；等Recall、长时及冷SSD尚待验证

**日志更新时间**：2026-11-04T16:27:59+08:00
<!-- experiment:remote_pull_first_20261104:end -->
