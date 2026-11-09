<!-- experiment:subgraph_methods_20261029:start -->
## 在线几何切图与历史轨迹及热点h跳的等容量回源比较（subgraph_methods_20261029）

**状态**：已完成

**开始时间**：2026-10-29T19:19:00+08:00

**结束时间**：2026-10-29T22:34:13+08:00

**目的 / 要解决的问题**：实现可在线发布的完整几何局部，比较三种方法的回源阶段、top-k贡献与源净资源

**假设**：缓冲几何完整区域加有向导航可提高独立热点目标覆盖，但补桥和回源可能抵消收益

**期望效果**：不预设赢家；在近似等实际包字节和相同检索参数下比较质量、源CPU、QPS和生命周期

**环境与配置 / 对照**：全量2M/2560D/原始L2；Intel-35入口，intel-31/32/30数据；64搜索线程、16接收reactor、RDMA、预算200、并发1024、remote_weight=.25、max_wait16ms、子预算.75、根分流.8。源0受控占88.6%。6000独立历史，5932独立校验，42898独立测试ID组成200K回放，三集合无交集；热缓存和tmpfs捕获。

**本轮改变的变量**

- T历史工作集合、H完整h跳/PPR边界代理、G全入口几何归属/导航；无副本对照
- 校验768/1280MiB预算及4/8入口组、T760MiB匹配；冻结Tmatch_760/H_768/G8bounded_768，实际759.64/715.81/696.85MiB，最大差9.01%；G目录另27.83MiB
- 24组每组200K：4方法×饱和/6000QPS×3轮；另4000QPS下在线G单区规划/发布200K；所有查询采集后集中Python离线分析
- 保留初期编译/导入/计时字段错误、独立容量验证拒绝、分析调度中断、两次SIGKILL及端口冲突；仅关闭自有PID，SHA核验后将58.06GB既有捕获迁至NUMA2，冻结配置补测

**代码改动**：实现python/dhdgs/geometry.py、cpp/replica_geometry.hpp及native绑定：epoch精确归属/原图双向树目录、完整几何缓冲与导航、独立成员及当前容量复验；接入源后台PlanWorker、既有传输发布，新增build_geometry_directory CLI、YAML和回归。搜索事件增加本机派发/已用时间，源验证独立计时。仅增加实验选择器，未替换默认；G尚无PPR整单元扩充，在线API每次一块。末尾仅更正文档与YAML注释。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：500万正式完成请求零失败，预算/同区域执行/同节点去重核验通过；75个数据服务实例退出，当前三机无本轮遗留服务。三轮中位QPS无副本8366、T9110、H9401、G9200；H相对+12.38%，同6000QPS源占核44.45→40.57（−8.73%）。T/H/G接流8.68/27.73/8.52%，实际回源90.19/约100/100%，首次回源62.87/35.81/32.92展开点。源新增最终top10中包外占24.55/53.67/61.29%，包内未找到占2.31/1.37/5.84%；每命中配对源CPU节省3.74/2.46/2.20ms。总体Recall均>95%，自身命中93.075/96.028/91.575%；同请求损失.705/.435/.261pp，均未证明命中集0.2pp非劣。共同133ID上G略更准，不能混淆不同命中群。G导航补点66.92%、零缓冲、无PPR，未证实图质量提升。目录332.14s/27.83MiB；在线33.57s接流2837次，但命中Recall86.10%。24组通信补齐；在线74条缺顶层在途心跳事后字节，保留不完整标志。13项最终正确性/集成通过，早期回归含导入重复执行。短测、共享机器、受控重复流、暖缓存/tmpfs及NUMA压力限制明确；SIGKILL根因未获内核证据。

**报告、图表与证据**

- [完整报告](../../subgraph_methods_20261029/REPORT.md)
- [回源与触发阶段](../../subgraph_methods_20261029/figures/return_rate_stage.png)
- [top-k贡献](../../subgraph_methods_20261029/figures/source_topk_contribution.png)
- [源净卸载](../../subgraph_methods_20261029/figures/source_resource_relief.png)
- [吞吐与p99](../../subgraph_methods_20261029/figures/throughput_latency.png)
- [召回](../../subgraph_methods_20261029/figures/recall.png)
- [容量](../../subgraph_methods_20261029/figures/storage_budget.png)
- [审计](../../subgraph_methods_20261029/audit.json)
- [统一参数](../../subgraph_methods_20261029/experiment.yaml)
- [实际设置](../../subgraph_methods_20261029/settings.yaml)
- [冻结成员](../../subgraph_methods_20261029/frozen_finalists.json)
- [被测源码哈希](../../subgraph_methods_20261029/source_hashes_bounded.json)
- [源码快照](../../subgraph_methods_20261029/source_bounded)
- [通信缺口](../../subgraph_methods_20261029/metric_gaps.json)
- [研究闭环](../../../Research.md#r001-a23)
- [实现边界](../../../DESIGN.md#entry-geometry-directory)

**下一步建议**：R001保持开放。冻结H成员做预算/派发独立消融及同质量吞吐；G比较局部导航根、非零缓冲与完整单元扩充；0.75/1.5/2.5GiB等实际容量曲线受alpha=.25守卫。先证明覆盖/净卸载再优化在线速度；新校验与测试重新冻结，补真正SSD/长时稳定与顶层控制尾部观测。

**日志更新时间**：2026-10-29T22:34:13+08:00
<!-- experiment:subgraph_methods_20261029:end -->
