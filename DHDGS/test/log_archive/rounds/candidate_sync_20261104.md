<!-- experiment:candidate_sync_20261104:start -->
## 唯一分支与异步候选同步（candidate_sync_20261104）

**状态**：已完成

**开始时间**：2026-11-04T19:43:39+08:00

**结束时间**：2026-11-04T21:29:27+08:00

**目的 / 要解决的问题**：唯一分支与异步候选收件箱，消除重复授权排队并验证固定预算下的质量与性能

**假设**：复用搜索上下文及候选增量交付减少重复排队；独立接收避免计算饱和阻塞同步

**期望效果**：同配置QPS提升、p99下降；零失败、预算守恒，召回影响如实报告

**环境与配置 / 对照**：无LB，原200K自然查询，64搜索线程/节点，D200/beam11，Intel35入口及客户端，31/32/30数据节点

**本轮改变的变量**：sync关闭/10；拉取总上限64/4/1及直接推送/旧成本策略；全根结束屏障→单根快速结束→预算零快速报告；网络idle1500/30000ms、预热观察0/2000ms；预算零目标消息省略false/true；64线程/D200/C1536固定，20K诊断后200K自然查询正式十组

**代码改动**：C++规范分支预约、异步有界Inbox/幂等交付/原预算恢复；单根及预算零快速报告；预算零目标缓存；候选ACK/消费/预算统计与指标白名单；网络预热扩容观察期和YAML校验；30项回归及紧凑实验客户端、离线统计/时序图

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：正式十组6M查询零失败/超预算/预算账目错误，新模式重复分支0；三项关键配置各两次600K合并，基线17414.52QPS/523.74ms，p99优先18866.86/237.20，吞吐优先19250.60/283.47；Recall97.2312%→97.0547%/97.0569%，p50上升；原成本参考22046.34/172.78。早期无收益/343次失败、失效增量构建均保留。暖页缓存、无LB；未验证所有等待<100ms、冷SSD或长时稳定性。

**报告、图表与证据**

- test/candidate_sync_20261104/REPORT.md
- test/candidate_sync_20261104/comparison.png
- test/candidate_sync_20261104/node_load.png
- test/candidate_sync_20261104/quiet_pooled_p50.png
- test/candidate_sync_20261104/quiet_pooled_p99.png
- test/candidate_sync_20261104/best_latency.yaml
- test/candidate_sync_20261104/best_throughput.yaml
- test/candidate_sync_20261104/source_hashes_quiet.json
- test/candidate_sync_20261104/incremental_build_audit.json
- DESIGN.md#candidate-sync-20261104
- Research.md

**下一步建议**：保留p99/吞吐两套参数；比较固定预算下首次派发的队列和小剩余工作成本，补充CPU热点剖析、待发背压及冷SSD/长期稳定性；已存在分支不重新拉取或追加预算

**日志更新时间**：2026-11-04T21:29:27+08:00
<!-- experiment:candidate_sync_20261104:end -->
