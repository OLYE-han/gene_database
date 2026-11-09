# DHDGS 实验日志

<!-- testlog:compact-v1 -->

用户手写待办独立保存在 [todo.md](todo.md)，不由实验日志生成器更新。

按问题归组，组内按开始时间从早到晚排列；时间为 Asia/Shanghai（UTC+08:00），每轮一行，子测试见报告，详细配置、改动及失败记录见对应详细日志。

[整理前完整日志](test/log_archive/TestLog_20261103.md)；详细记录保留起止时间、变量、代码改动、失败与限制，本次仅整理文档，未运行性能实验。

数据保留说明：[10-23](test/storage_cleanup_before_20261022_20261023/REPORT.md)、[10-27](test/storage_cleanup_20261027/README.md)、[10-29](test/storage_cleanup_before_20261028_20261029/README.md)、[11-03](test/storage_cleanup_before_20261102_20261103/README.md)；历史原始数据可能已清理，报告与图表保留。

<a id="2026-10-23-历史部署数据清理说明非性能实验"></a>

2026-11-05 数据清理（非性能实验）：仅最近两轮完整保留，旧实验的原始明细与编译缓存已按清单清理，长期回放输入及共用客户端依赖保留；[清理记录](test/storage_cleanup_keep_latest2_20261105/README.md)。

## 评测接口与实验流程

<!-- experiment:network_api_load_20261022:start -->
<!-- testlog-entry:{"id":"network_api_load_20261022","group":"评测接口与实验流程","started_at":"2026-10-22；历史实验未采集墙钟起止时间，无法可靠恢复时分秒","status":"已完成（补记）","summary":"独立网络API与持续结果消费完成30万请求零失败，五档并发最高约8,939 QPS，32 lane未显示明显收益，旧接口计时口径不同","report":"test/network_api_load_20261022/REPORT.md","report_label":"报告","aliases":["2026-10-22-独立网络-api-与持续结果消费历史补记network_api_load_20261022"]} -->
- <a id="network_api_load_20261022"></a><a id="2026-10-22-独立网络-api-与持续结果消费历史补记network_api_load_20261022"></a>**2026-10-22**（已完成（补记））：独立网络API与持续结果消费完成30万请求零失败，五档并发最高约8,939 QPS，32 lane未显示明显收益，旧接口计时口径不同（[报告](test/network_api_load_20261022/REPORT.md)；[详细记录](test/log_archive/rounds/network_api_load_20261022.md)）。
<!-- experiment:network_api_load_20261022:end -->

<!-- experiment:experiment_logging_20261022:start -->
<!-- testlog-entry:{"id":"experiment_logging_20261022","group":"评测接口与实验流程","started_at":"2026-10-22T23:15:41+08:00","status":"已完成","summary":"实验日志工具通过幂等、异常生命周期及24个并发记录者校验，建立自动记录流程，未进行性能测量","report":"test/log_archive/rounds/experiment_logging_20261022.md","report_label":"验证记录","aliases":["2026-10-22-实验日志流程接入与校验experiment_logging_20261022"]} -->
- <a id="experiment_logging_20261022"></a><a id="2026-10-22-实验日志流程接入与校验experiment_logging_20261022"></a>**2026-10-22 23:15:41**（已完成）：实验日志工具通过幂等、异常生命周期及24个并发记录者校验，建立自动记录流程，未进行性能测量（[验证记录](test/log_archive/rounds/experiment_logging_20261022.md)；[详细记录](test/log_archive/rounds/experiment_logging_20261022.md)）。
<!-- experiment:experiment_logging_20261022:end -->

## 网络通信与动态扩缩容

<!-- experiment:rdma_transport_20261023:start -->
<!-- testlog-entry:{"id":"rdma_transport_20261023","group":"网络通信与动态扩缩容","started_at":"2026-10-23T00:32:00+08:00","status":"已完成","summary":"TCP/RDMA及32–128搜索线程对照中，64线程RDMA中位QPS 6,036低于TCP 7,137，建连长尾明显，100万正式请求零失败","report":"test/rdma_transport_20261023/REPORT.md","report_label":"报告","aliases":["2026-10-23-独立顶层三数据节点-tcprdma-对照rdma_transport_20261023"]} -->
- <a id="rdma_transport_20261023"></a><a id="2026-10-23-独立顶层三数据节点-tcprdma-对照rdma_transport_20261023"></a>**2026-10-23 00:32:00**（已完成）：TCP/RDMA及32–128搜索线程对照中，64线程RDMA中位QPS 6,036低于TCP 7,137，建连长尾明显，100万正式请求零失败（[报告](test/rdma_transport_20261023/REPORT.md)；[详细记录](test/log_archive/rounds/rdma_transport_20261023.md)）。
<!-- experiment:rdma_transport_20261023:end -->

<!-- experiment:network_parallel_20261023:start -->
<!-- testlog-entry:{"id":"network_parallel_20261023","group":"网络通信与动态扩缩容","started_at":"2026-10-23T02:44:21+08:00","status":"已完成","summary":"网络并行与动态扩容使主复测达到16,208 QPS，C1024/2048通过吞吐及网络占比门槛，C512未通过","report":"test/network_parallel_20261023/REPORT.md","report_label":"报告","aliases":["四机64搜索线程网络并行与动态扩容优化network_parallel_20261023"]} -->
- <a id="network_parallel_20261023"></a><a id="四机64搜索线程网络并行与动态扩容优化network_parallel_20261023"></a>**2026-10-23 02:44:21**（已完成）：网络并行与动态扩容使主复测达到16,208 QPS，C1024/2048通过吞吐及网络占比门槛，C512未通过（[报告](test/network_parallel_20261023/REPORT.md)；[详细记录](test/log_archive/rounds/network_parallel_20261023.md)）。
<!-- experiment:network_parallel_20261023:end -->

<!-- experiment:rdma_default_persistence_20261023:start -->
<!-- testlog-entry:{"id":"rdma_default_persistence_20261023","group":"网络通信与动态扩缩容","started_at":"2026-10-23T10:26:52+08:00","status":"已完成","summary":"固化RDMA默认配置、依赖与部署快照，47项回归及2,128条正式端到端请求通过，仍有少量协议统计未闭合","report":"test/rdma_default_persistence_20261023/REPORT.md","report_label":"报告","aliases":["默认rdma方案持久化验证rdma_default_persistence_20261023"]} -->
- <a id="rdma_default_persistence_20261023"></a><a id="默认rdma方案持久化验证rdma_default_persistence_20261023"></a>**2026-10-23 10:26:52**（已完成）：固化RDMA默认配置、依赖与部署快照，47项回归及2,128条正式端到端请求通过，仍有少量协议统计未闭合（[报告](test/rdma_default_persistence_20261023/REPORT.md)；[详细记录](test/log_archive/rounds/rdma_default_persistence_20261023.md)）。
<!-- experiment:rdma_default_persistence_20261023:end -->

## 搜索线程、并发与CPU资源

<!-- experiment:search_scaling_unbounded_20261023:start -->
<!-- testlog-entry:{"id":"search_scaling_unbounded_20261023","group":"搜索线程、并发与CPU资源","started_at":"2026-10-23T10:38:10+08:00","status":"已完成","summary":"16/32/48/64/96线程扫描中64线程均值21,315 QPS最佳，96线程未线性扩展，660万请求零失败，结论限于2K查询循环与暖缓存","report":"test/search_scaling_unbounded_20261023/REPORT.md","report_label":"报告","aliases":["搜索线程1632486496与无应用层上限动态网络扩缩容search_scaling_unbounded_20261023"]} -->
- <a id="search_scaling_unbounded_20261023"></a><a id="搜索线程1632486496与无应用层上限动态网络扩缩容search_scaling_unbounded_20261023"></a>**2026-10-23 10:38:10**（已完成）：16/32/48/64/96线程扫描中64线程均值21,315 QPS最佳，96线程未线性扩展，660万请求零失败，结论限于2K查询循环与暖缓存（[报告](test/search_scaling_unbounded_20261023/REPORT.md)；[详细记录](test/log_archive/rounds/search_scaling_unbounded_20261023.md)）。
<!-- experiment:search_scaling_unbounded_20261023:end -->

<!-- experiment:concurrency_best_20261023:start -->
<!-- testlog-entry:{"id":"concurrency_best_20261023","group":"搜索线程、并发与CPU资源","started_at":"2026-10-23T14:36:55+08:00","status":"已完成","summary":"64线程下扫描C128–2048，C1024均值22,346 QPS最高而C2048增大排队，200万请求零失败，属2K查询循环短测","report":"test/concurrency_best_20261023/REPORT.md","report_label":"报告","aliases":["最优64搜索线程配置的四机并发扫描concurrency_best_20261023"]} -->
- <a id="concurrency_best_20261023"></a><a id="最优64搜索线程配置的四机并发扫描concurrency_best_20261023"></a>**2026-10-23 14:36:55**（已完成）：64线程下扫描C128–2048，C1024均值22,346 QPS最高而C2048增大排队，200万请求零失败，属2K查询循环短测（[报告](test/concurrency_best_20261023/REPORT.md)；[详细记录](test/log_archive/rounds/concurrency_best_20261023.md)）。
<!-- experiment:concurrency_best_20261023:end -->

<!-- experiment:resource_parallel_20261027:start -->
<!-- testlog-entry:{"id":"resource_parallel_20261027","group":"搜索线程、并发与CPU资源","started_at":"2026-10-27T02:14:10+08:00","status":"已完成","summary":"八组CPU竞争复测确认搜索实际并行、正常锁等待较低，但保留两组严重低QPS异常，副本发布后源节点仍满载；原始明细已清理","report":"test/resource_parallel_20261027/REPORT.md","report_label":"报告","aliases":["r002共享cpu竞争复测与并行等待诊断resource_parallel_20261027"]} -->
- <a id="resource_parallel_20261027"></a><a id="r002共享cpu竞争复测与并行等待诊断resource_parallel_20261027"></a>**2026-10-27 02:14:10**（已完成）：八组CPU竞争复测确认搜索实际并行、正常锁等待较低，但保留两组严重低QPS异常，副本发布后源节点仍满载；原始明细已清理（[报告](test/resource_parallel_20261027/REPORT.md)；[详细记录](test/log_archive/rounds/resource_parallel_20261027.md)）。
<!-- experiment:resource_parallel_20261027:end -->

## 负载均衡：选图、入口覆盖与源节点卸载

<!-- experiment:load_balance_20261026:start -->
<!-- testlog-entry:{"id":"load_balance_20261026","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-26T13:30:20+08:00","status":"已完成","summary":"初版部分副本质量不达标，完整副本在偏斜流单次QPS提升29.9%并减少源CPU，但复制10.767 GiB、资格建立222秒，紧凑副本收益未证明","report":"test/load_balance_20261026/REPORT.md","report_label":"报告","aliases":["r001部分子图副本与入口负载均衡load_balance_20261026"]} -->
- <a id="load_balance_20261026"></a><a id="r001部分子图副本与入口负载均衡load_balance_20261026"></a>**2026-10-26 13:30:20**（已完成）：初版部分副本质量不达标，完整副本在偏斜流单次QPS提升29.9%并减少源CPU，但复制10.767 GiB、资格建立222秒，紧凑副本收益未证明（[报告](test/load_balance_20261026/REPORT.md)；[详细记录](test/log_archive/rounds/load_balance_20261026.md)）。
<!-- experiment:load_balance_20261026:end -->

<!-- experiment:replica_relief_20261027:start -->
<!-- testlog-entry:{"id":"replica_relief_20261027","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-27T10:35:18+08:00（本轮最早保留的子测试起始时间）","status":"已完成（负载均衡效果未达标）","summary":"快速生效副本降低命中查询源CPU却显著损失Recall，定位整副本排除和单次派发导致漏点，未达质量约束下卸载目标；原始明细已清理","report":"test/replica_relief_20261027/REPORT.md","report_label":"报告","aliases":["r001副本快速生效与源节点实际卸载replica_relief_20261027"]} -->
- <a id="replica_relief_20261027"></a><a id="r001副本快速生效与源节点实际卸载replica_relief_20261027"></a>**2026-10-27 10:35:18**（已完成（负载均衡效果未达标））：快速生效副本降低命中查询源CPU却显著损失Recall，定位整副本排除和单次派发导致漏点，未达质量约束下卸载目标；原始明细已清理（[报告](test/replica_relief_20261027/REPORT.md)；[详细记录](test/log_archive/rounds/replica_relief_20261027.md)）。
<!-- experiment:replica_relief_20261027:end -->

<!-- experiment:hot_entry_community_20261028:start -->
<!-- testlog-entry:{"id":"hot_entry_community_20261028","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-28T17:27:26+08:00","status":"已完成","summary":"完整热点闭包与PPR提高命中率并减少源工作，但总CPU增加、命中群体召回退化，在线QPS收益有限；原始明细已清理","report":"test/hot_entry_community_20261028/REPORT.md","report_label":"报告","aliases":["完整热点闭包与ppr副本选图hot_entry_community_20261028"]} -->
- <a id="hot_entry_community_20261028"></a><a id="完整热点闭包与ppr副本选图hot_entry_community_20261028"></a>**2026-10-28 17:27:26**（已完成）：完整热点闭包与PPR提高命中率并减少源工作，但总CPU增加、命中群体召回退化，在线QPS收益有限；原始明细已清理（[报告](test/hot_entry_community_20261028/REPORT.md)；[详细记录](test/log_archive/rounds/hot_entry_community_20261028.md)）。
<!-- experiment:hot_entry_community_20261028:end -->

<!-- experiment:replica_residence_20261028:start -->
<!-- testlog-entry:{"id":"replica_residence_20261028","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-28T17:29:25+08:00","status":"已完成","summary":"32K/128K子图几乎全部回源且近邻贡献低，完整源片诊断回源为零，证实副本范围与入口不匹配及新增工作抵消卸载；原始明细已清理","report":"test/replica_residence_20261028/REPORT.md","report_label":"报告","aliases":["r001副本驻留真实近邻覆盖与回源因果诊断replica_residence_20261028"]} -->
- <a id="replica_residence_20261028"></a><a id="r001副本驻留真实近邻覆盖与回源因果诊断replica_residence_20261028"></a>**2026-10-28 17:29:25**（已完成）：32K/128K子图几乎全部回源且近邻贡献低，完整源片诊断回源为零，证实副本范围与入口不匹配及新增工作抵消卸载；原始明细已清理（[报告](test/replica_residence_20261028/REPORT.md)；[详细记录](test/log_archive/rounds/replica_residence_20261028.md)）。
<!-- experiment:replica_residence_20261028:end -->

<!-- experiment:replica_region_20261029:start -->
<!-- testlog-entry:{"id":"replica_region_20261029","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-29T01:09:00+08:00","status":"已完成","summary":"完整区域副本在热点流QPS提高13.59%，自然200K无稳定收益，源CPU下降但总CPU增加且命中质量未过0.2个百分点门槛；原始明细已清理","report":"test/replica_region_20261029/REPORT.md","report_label":"报告","aliases":["完整区域覆盖与200k在线卸载验证replica_region_20261029"]} -->
- <a id="replica_region_20261029"></a><a id="完整区域覆盖与200k在线卸载验证replica_region_20261029"></a>**2026-10-29 01:09:00**（已完成）：完整区域副本在热点流QPS提高13.59%，自然200K无稳定收益，源CPU下降但总CPU增加且命中质量未过0.2个百分点门槛；原始明细已清理（[报告](test/replica_region_20261029/REPORT.md)；[详细记录](test/log_archive/rounds/replica_region_20261029.md)）。
<!-- experiment:replica_region_20261029:end -->

<!-- experiment:replica_locality_20261029:start -->
<!-- testlog-entry:{"id":"replica_locality_20261029","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-29T09:50:28+08:00","status":"已完成","summary":"完整局域多副本使受控热点QPS配对中位提高21.05%，自然留出仅约1.98%，部分区域质量退化，在线自动均衡未验收；原始明细已清理","report":"test/replica_locality_20261029/REPORT.md","report_label":"报告","aliases":["完整局域多副本容量曲线与独立请求泛化replica_locality_20261029"]} -->
- <a id="replica_locality_20261029"></a><a id="完整局域多副本容量曲线与独立请求泛化replica_locality_20261029"></a>**2026-10-29 09:50:28**（已完成）：完整局域多副本使受控热点QPS配对中位提高21.05%，自然留出仅约1.98%，部分区域质量退化，在线自动均衡未验收；原始明细已清理（[报告](test/replica_locality_20261029/REPORT.md)；[详细记录](test/log_archive/rounds/replica_locality_20261029.md)）。
<!-- experiment:replica_locality_20261029:end -->

<!-- experiment:entry_voronoi_pilot_20261029:start -->
<!-- testlog-entry:{"id":"entry_voronoi_pilot_20261029","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-29T16:09:56+08:00","status":"已完成","summary":"几何入口核心与缓冲区完成覆盖诊断，但枚举约536秒且部分拓扑未完成，未部署或测量在线QPS与Recall；原始明细已清理","report":"test/entry_voronoi_pilot_20261029/REPORT.md","report_label":"报告","aliases":["入口几何归属完整导航与卸载收益快速验证entry_voronoi_pilot_20261029"]} -->
- <a id="entry_voronoi_pilot_20261029"></a><a id="入口几何归属完整导航与卸载收益快速验证entry_voronoi_pilot_20261029"></a>**2026-10-29 16:09:56**（已完成）：几何入口核心与缓冲区完成覆盖诊断，但枚举约536秒且部分拓扑未完成，未部署或测量在线QPS与Recall；原始明细已清理（[报告](test/entry_voronoi_pilot_20261029/REPORT.md)；[详细记录](test/log_archive/rounds/entry_voronoi_pilot_20261029.md)）。
<!-- experiment:entry_voronoi_pilot_20261029:end -->

<!-- experiment:subgraph_methods_20261029:start -->
<!-- testlog-entry:{"id":"subgraph_methods_20261029","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-29T19:19:00+08:00","status":"已完成","summary":"等容量比较轨迹、h跳与几何切图，h跳热点QPS提升12.38%但仍几乎全部回源，三方法均未证明命中质量非劣；原始明细已清理","report":"test/subgraph_methods_20261029/REPORT.md","report_label":"报告","aliases":["在线几何切图与历史轨迹及热点h跳的等容量回源比较subgraph_methods_20261029"]} -->
- <a id="subgraph_methods_20261029"></a><a id="在线几何切图与历史轨迹及热点h跳的等容量回源比较subgraph_methods_20261029"></a>**2026-10-29 19:19:00**（已完成）：等容量比较轨迹、h跳与几何切图，h跳热点QPS提升12.38%但仍几乎全部回源，三方法均未证明命中质量非劣；原始明细已清理（[报告](test/subgraph_methods_20261029/REPORT.md)；[详细记录](test/log_archive/rounds/subgraph_methods_20261029.md)）。
<!-- experiment:subgraph_methods_20261029:end -->

<!-- experiment:hop_capacity_20261030:start -->
<!-- testlog-entry:{"id":"hop_capacity_20261030","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-30T10:03:09+08:00","status":"已完成","summary":"H/HP/F/FP容量10%–90%的144组正式扫描零失败，质量筛选后H α=0.8最高约15,510 QPS，部分小区域Recall仍不足95%；原始明细已清理","report":"test/hop_capacity_20261030/REPORT.md","report_label":"报告","aliases":["hhpffp源容量10至90的回源曲线hop_capacity_20261030"]} -->
- <a id="hop_capacity_20261030"></a><a id="hhpffp源容量10至90的回源曲线hop_capacity_20261030"></a>**2026-10-30 10:03:09**（已完成）：H/HP/F/FP容量10%–90%的144组正式扫描零失败，质量筛选后H α=0.8最高约15,510 QPS，部分小区域Recall仍不足95%；原始明细已清理（[报告](test/hop_capacity_20261030/REPORT.md)；[详细记录](test/log_archive/rounds/hop_capacity_20261030.md)）。
<!-- experiment:hop_capacity_20261030:end -->

<!-- experiment:replica_targets_20261031:start -->
<!-- testlog-entry:{"id":"replica_targets_20261031","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-31T13:07:47+08:00","status":"已完成","summary":"同总容量双目标副本使确认QPS提高16.21%、固定流量源CPU下降19.61%，但p99、Recall和全系统CPU成本恶化；原始明细已清理","report":"test/replica_targets_20261031/REPORT.md","report_label":"报告","aliases":["同总容量双目标副本提高qpsreplica_targets_20261031"]} -->
- <a id="replica_targets_20261031"></a><a id="同总容量双目标副本提高qpsreplica_targets_20261031"></a>**2026-10-31 13:07:47**（已完成）：同总容量双目标副本使确认QPS提高16.21%、固定流量源CPU下降19.61%，但p99、Recall和全系统CPU成本恶化；原始明细已清理（[报告](test/replica_targets_20261031/REPORT.md)；[详细记录](test/log_archive/rounds/replica_targets_20261031.md)）。
<!-- experiment:replica_targets_20261031:end -->

<!-- experiment:replica_tiers_20261031:start -->
<!-- testlog-entry:{"id":"replica_tiers_20261031","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-10-31T20:05:45+08:00","status":"已完成","summary":"小/中/大副本分级对照最高约18,206 QPS，能减少源排队但总CPU增加，满份额可造成目标拥堵，自动生命周期尚未验收；原始明细已清理","report":"test/replica_tiers_20261031/REPORT.md","report_label":"报告","aliases":["分级副本小容量尾部卸载与持久热点容量策略replica_tiers_20261031"]} -->
- <a id="replica_tiers_20261031"></a><a id="分级副本小容量尾部卸载与持久热点容量策略replica_tiers_20261031"></a>**2026-10-31 20:05:45**（已完成）：小/中/大副本分级对照最高约18,206 QPS，能减少源排队但总CPU增加，满份额可造成目标拥堵，自动生命周期尚未验收；原始明细已清理（[报告](test/replica_tiers_20261031/REPORT.md)；[详细记录](test/log_archive/rounds/replica_tiers_20261031.md)）。
<!-- experiment:replica_tiers_20261031:end -->

<!-- experiment:small_replica_peak_20261102:start -->
<!-- testlog-entry:{"id":"small_replica_peak_20261102","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-02T10:12:09+08:00","status":"已完成","summary":"小副本S30满份额单批200K达到13,430.50 QPS、零失败和命中Recall 95.397%，未刷新历史峰值","report":"test/small_replica_peak_20261102/REPORT.md","report_label":"报告","aliases":["当前最佳小副本s30满份额峰值批次确认small_replica_peak_20261102"]} -->
- <a id="small_replica_peak_20261102"></a><a id="当前最佳小副本s30满份额峰值批次确认small_replica_peak_20261102"></a>**2026-11-02 10:12:09**（已完成）：小副本S30满份额单批200K达到13,430.50 QPS、零失败和命中Recall 95.397%，未刷新历史峰值（[报告](test/small_replica_peak_20261102/REPORT.md)；[详细记录](test/log_archive/rounds/small_replica_peak_20261102.md)）。
<!-- experiment:small_replica_peak_20261102:end -->

<!-- experiment:tail_four_20261102:start -->
<!-- testlog-entry:{"id":"tail_four_20261102","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-02T20:29:27+08:00","status":"已完成","summary":"同容量分放与尾部优化使受控热点QPS提高26.7%、源CPU减少，命中召回损失区间仍越过0.5个百分点，未形成通用质量保证；原始明细已清理","report":"test/tail_four_20261102/REPORT.md","report_label":"报告","aliases":["有限召回损失下的四项尾部优化tail_four_20261102"]} -->
- <a id="tail_four_20261102"></a><a id="有限召回损失下的四项尾部优化tail_four_20261102"></a>**2026-11-02 20:29:27**（已完成）：同容量分放与尾部优化使受控热点QPS提高26.7%、源CPU减少，命中召回损失区间仍越过0.5个百分点，未形成通用质量保证；原始明细已清理（[报告](test/tail_four_20261102/REPORT.md)；[详细记录](test/log_archive/rounds/tail_four_20261102.md)）。
<!-- experiment:tail_four_20261102:end -->

<!-- experiment:lb_combined_coverage_20261103:start -->
<!-- testlog-entry:{"id":"lb_combined_coverage_20261103","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-03T18:31:00+08:00","status":"已完成","summary":"组合修复兼容且成功发布副本，同容量入口64→90增加命中，但源CPU节省被总CPU增加和命中Recall下降2.634个百分点抵消，新增入口默认关闭；原始明细已清理","report":"test/lb_combined_coverage_20261103/REPORT.md","report_label":"报告","aliases":["组合修复与同容量入口覆盖lb_combined_coverage_20261103"]} -->
- <a id="lb_combined_coverage_20261103"></a><a id="组合修复与同容量入口覆盖lb_combined_coverage_20261103"></a>**2026-11-03 18:31:00**（已完成）：组合修复兼容且成功发布副本，同容量入口64→90增加命中，但源CPU节省被总CPU增加和命中Recall下降2.634个百分点抵消，新增入口默认关闭；原始明细已清理（[报告](test/lb_combined_coverage_20261103/REPORT.md)；[详细记录](test/log_archive/rounds/lb_combined_coverage_20261103.md)）。
<!-- experiment:lb_combined_coverage_20261103:end -->

<!-- experiment:entry_threshold_sweep_20261104:start -->
<!-- testlog-entry:{"id":"entry_threshold_sweep_20261104","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-04T14:06:23+08:00","status":"已撤销并删除","summary":"入口未收敛，按用户要求删除测试数据及图表，撤回性能排序；由收敛实验替代","report":"test/entry_threshold_converged_20261104/PLAN.md","report_label":"替代实验计划","aliases":["r001-固定副本入口回源预算阈值20至100短测entry_threshold_sweep_20261104"]} -->
- <a id="entry_threshold_sweep_20261104"></a><a id="r001-固定副本入口回源预算阈值20至100短测entry_threshold_sweep_20261104"></a>**2026-11-04 14:06:23**（已撤销并删除）：入口未收敛，按用户要求删除测试数据及图表，撤回性能排序；由收敛实验替代（[替代实验计划](test/entry_threshold_converged_20261104/PLAN.md)；[详细记录](test/log_archive/rounds/entry_threshold_sweep_20261104.md)）。
<!-- experiment:entry_threshold_sweep_20261104:end -->

<!-- experiment:entry_threshold_converged_20261104:start -->
<!-- testlog-entry:{"id":"entry_threshold_converged_20261104","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-04T16:03:13+08:00","status":"中断（用户更改指标）","summary":"用户改为每次检索期望回源预算；旧条件均值轮次排空中断，保留T100与T20前缀证据；原始明细已清理","report":"test/entry_threshold_converged_20261104/INTERRUPTED.md","report_label":"中断记录","aliases":["r001-固定查询与副本的入口阈值收敛实验entry_threshold_converged_20261104"]} -->
- <a id="entry_threshold_converged_20261104"></a><a id="r001-固定查询与副本的入口阈值收敛实验entry_threshold_converged_20261104"></a>**2026-11-04 16:03:13**（中断（用户更改指标））：用户改为每次检索期望回源预算；旧条件均值轮次排空中断，保留T100与T20前缀证据；原始明细已清理（[中断记录](test/entry_threshold_converged_20261104/INTERRUPTED.md)；[详细记录](test/log_archive/rounds/entry_threshold_converged_20261104.md)）。
<!-- experiment:entry_threshold_converged_20261104:end -->

<!-- experiment:entry_expected_work_sweep_20261104:start -->
<!-- testlog-entry:{"id":"entry_expected_work_sweep_20261104","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-04T16:39:17+08:00","status":"已完成","summary":"新期望预算语义八档均收敛，100.4M零失败且计数一致；低阈值压低接流，基线漂移5.05%使QPS选优仍待交错复验；原始明细已清理","report":"test/entry_expected_work_sweep_20261104/REPORT.md","report_label":"报告与复核结论","aliases":["r001-每次入口检索期望回源预算阈值八档收敛实验entry_expected_work_sweep_20261104"]} -->
- <a id="entry_expected_work_sweep_20261104"></a><a id="r001-每次入口检索期望回源预算阈值八档收敛实验entry_expected_work_sweep_20261104"></a>**2026-11-04 16:39:17**（已完成）：新期望预算语义八档均收敛，100.4M零失败且计数一致；低阈值压低接流，基线漂移5.05%使QPS选优仍待交错复验；原始明细已清理（[报告与复核结论](test/entry_expected_work_sweep_20261104/REPORT.md)；[详细记录](test/log_archive/rounds/entry_expected_work_sweep_20261104.md)）。
<!-- experiment:entry_expected_work_sweep_20261104:end -->

<!-- experiment:lb_pull_sync_20261105:start -->
<!-- testlog-entry:{"id":"lb_pull_sync_20261105","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-05T11:08:50+08:00","status":"停止，需检查首批队列/测量证据","summary":"功能验证通过；首批自动流程停止：TimeoutError('等待窗口超出YAML上限；未抢占其他测试')；未宣称性能收益","report":"test/lb_pull_sync_20261105/REPORT.md","report_label":"实现与验证记录","aliases":["常驻单边read与唯一分支同步的副本负载均衡lb_pull_sync_20261105"]} -->
- <a id="lb_pull_sync_20261105"></a><a id="常驻单边read与唯一分支同步的副本负载均衡lb_pull_sync_20261105"></a>**2026-11-05 11:08:50**（停止，需检查首批队列/测量证据）：功能验证通过；首批自动流程停止：TimeoutError('等待窗口超出YAML上限；未抢占其他测试')；未宣称性能收益（[实现与验证记录](test/lb_pull_sync_20261105/REPORT.md)；[详细记录](test/log_archive/rounds/lb_pull_sync_20261105.md)）。
<!-- experiment:lb_pull_sync_20261105:end -->

<!-- experiment:replica_pair_20261105:start -->
<!-- testlog-entry:{"id":"replica_pair_20261105","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-05T20:36:51+08:00","status":"已完成","summary":"副本源限定协议720万正式请求零失败，命中召回改善约2.3pp，但S30/D80未获稳定QPS与p99双赢","report":"test/replica_pair_20261105/REPORT.md","report_label":"报告","aliases":["副本与源节点限定rdma拉取及同步replica_pair_20261105"]} -->
- <a id="replica_pair_20261105"></a><a id="副本与源节点限定rdma拉取及同步replica_pair_20261105"></a>**2026-11-05 20:36:51**（已完成）：副本源限定协议720万正式请求零失败，命中召回改善约2.3pp，但S30/D80未获稳定QPS与p99双赢（[报告](test/replica_pair_20261105/REPORT.md)；[详细记录](test/log_archive/rounds/replica_pair_20261105.md)）。
<!-- experiment:replica_pair_20261105:end -->

<!-- experiment:replica_branch_metrics_20261105:start -->
<!-- testlog-entry:{"id":"replica_branch_metrics_20261105","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-05T20:37:53+08:00","status":"已完成：离线计量与历史样本复核（非性能压测）","summary":"允许第三节点分支；旧D200全开放217个命中样本中直接0、后继1（0.46%，余量4/授权2）；9项计量回归通过","report":"test/replica_branch_metrics_20261105/REPORT.md","report_label":"报告","aliases":["副本向第三节点分支的计量与历史样本复核replica_branch_metrics_20261105"]} -->
- <a id="replica_branch_metrics_20261105"></a><a id="副本向第三节点分支的计量与历史样本复核replica_branch_metrics_20261105"></a>**2026-11-05 20:37:53**（已完成：离线计量与历史样本复核（非性能压测））：允许第三节点分支；旧D200全开放217个命中样本中直接0、后继1（0.46%，余量4/授权2）；9项计量回归通过（[报告](test/replica_branch_metrics_20261105/REPORT.md)；[详细记录](test/log_archive/rounds/replica_branch_metrics_20261105.md)）。
<!-- experiment:replica_branch_metrics_20261105:end -->

<!-- experiment:replica_pair_skew_20261105:start -->
<!-- testlog-entry:{"id":"replica_pair_skew_20261105","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-05T22:22:29+08:00","status":"已完成","summary":"热点协议QPS下降约24%/14%；同查询离线审计发现第三节点漏达，但主要召回差额来自基线无分支查询，组合暂不采用","report":"test/replica_pair_skew_20261105/REPORT.md","report_label":"报告","aliases":["历史热点负载下副本源限定read与同步对照replica_pair_skew_20261105"]} -->
- <a id="replica_pair_skew_20261105"></a><a id="历史热点负载下副本源限定read与同步对照replica_pair_skew_20261105"></a>**2026-11-05 22:22:29**（已完成）：热点协议QPS下降约24%/14%；同查询离线审计发现第三节点漏达，但主要召回差额来自基线无分支查询，组合暂不采用（[报告](test/replica_pair_skew_20261105/REPORT.md)；[详细记录](test/log_archive/rounds/replica_pair_skew_20261105.md)）。
<!-- experiment:replica_pair_skew_20261105:end -->

<!-- experiment:boundary_portals_20261106:start -->
<!-- testlog-entry:{"id":"boundary_portals_20261106","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-06T01:37:27+08:00","status":"已完成","summary":"全边界使S30同组第三节点漏达15→4，但整体Recall仅+0.0839pp、D80+0.1295pp，均未达95%；代表性无质量收益，默认关闭","report":"test/boundary_portals_20261106/REPORT.md","report_label":"报告","aliases":["副本有向边界点及连接路径增强boundary_portals_20261106"]} -->
- <a id="boundary_portals_20261106"></a><a id="副本有向边界点及连接路径增强boundary_portals_20261106"></a>**2026-11-06 01:37:27**（已完成）：全边界使S30同组第三节点漏达15→4，但整体Recall仅+0.0839pp、D80+0.1295pp，均未达95%；代表性无质量收益，默认关闭（[报告](test/boundary_portals_20261106/REPORT.md)；[详细记录](test/log_archive/rounds/boundary_portals_20261106.md)）。
<!-- experiment:boundary_portals_20261106:end -->

<!-- experiment:demand_growth_20261106:start -->
<!-- testlog-entry:{"id":"demand_growth_20261106","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-06T15:05:50+08:00","status":"已完成","summary":"完成100槽缓存及增量学习20组对照，hot虽降低源CPU但S30/D80 QPS下降24.73%/9.50%且召回不足95%，保留实现默认关闭","report":"test/demand_growth_20261106/REPORT.md","report_label":"报告","aliases":["查询驱动共享缓存与副本增量学习demand_growth_20261106"]} -->
- <a id="demand_growth_20261106"></a><a id="查询驱动共享缓存与副本增量学习demand_growth_20261106"></a>**2026-11-06 15:05:50**（已完成）：完成100槽缓存及增量学习20组对照，hot虽降低源CPU但S30/D80 QPS下降24.73%/9.50%且召回不足95%，保留实现默认关闭（[报告](test/demand_growth_20261106/REPORT.md)；[详细记录](test/log_archive/rounds/demand_growth_20261106.md)）。
<!-- experiment:demand_growth_20261106:end -->

<!-- experiment:demand_pipeline_20261109:start -->
<!-- testlog-entry:{"id":"demand_pipeline_20261109","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-09T10:02:42+08:00","status":"已完成","summary":"自然/热点无副本为2.85万/1.73万QPS，目录优化使S30/D80吞吐提高31.8%/9.6%并被选为indexed基线，流水无稳定额外收益且副本Recall不足95%","report":"test/demand_pipeline_20261109/REPORT.md","report_label":"报告","aliases":["无副本基线归因与增量缓存持久化流水线demand_pipeline_20261109"]} -->
- <a id="demand_pipeline_20261109"></a><a id="无副本基线归因与增量缓存持久化流水线demand_pipeline_20261109"></a>**2026-11-09 10:02:42**（已完成）：自然/热点无副本为2.85万/1.73万QPS，目录优化使S30/D80吞吐提高31.8%/9.6%并被选为indexed基线，流水无稳定额外收益且副本Recall不足95%（[报告](test/demand_pipeline_20261109/REPORT.md)；[详细记录](test/log_archive/rounds/demand_pipeline_20261109.md)）。
<!-- experiment:demand_pipeline_20261109:end -->

<!-- experiment:entry_routing_20261109:start -->
<!-- testlog-entry:{"id":"entry_routing_20261109","group":"负载均衡：选图、入口覆盖与源节点卸载","started_at":"2026-11-09T14:05:39+08:00","status":"已完成","summary":"同图最短队列使S30/D80吞吐+21.1%/+4.0%、p99−32.0%/−44.9%，成本模式仅S30尾部更优；干扰组排除，严格95%召回仍未通过","report":"test/entry_routing_20261109/REPORT.md","report_label":"报告","aliases":["固定图下最短队列与全路径成本入口调度对照entry_routing_20261109"]} -->
- <a id="entry_routing_20261109"></a><a id="固定图下最短队列与全路径成本入口调度对照entry_routing_20261109"></a>**2026-11-09 14:05:39**（已完成）：同图最短队列使S30/D80吞吐+21.1%/+4.0%、p99−32.0%/−44.9%，成本模式仅S30尾部更优；干扰组排除，严格95%召回仍未通过（[报告](test/entry_routing_20261109/REPORT.md)；[详细记录](test/log_archive/rounds/entry_routing_20261109.md)）。
<!-- experiment:entry_routing_20261109:end -->

## 负载均衡：访问语义与正确性

<!-- experiment:replica_revisit_20261026:start -->
<!-- testlog-entry:{"id":"replica_revisit_20261026","group":"负载均衡：访问语义与正确性","started_at":"2026-10-26T16:35:59+08:00","status":"已完成","summary":"实现跨机重访和本机展开去重，140万请求零失败，最终副本仅命中0.239%且配对召回下降，未证明整体均衡收益","report":"test/replica_revisit_20261026/REPORT.md","report_label":"报告","aliases":["r001可重访搜索与全量在线子图复制replica_revisit_20261026"]} -->
- <a id="replica_revisit_20261026"></a><a id="r001可重访搜索与全量在线子图复制replica_revisit_20261026"></a>**2026-10-26 16:35:59**（已完成）：实现跨机重访和本机展开去重，140万请求零失败，最终副本仅命中0.239%且配对召回下降，未证明整体均衡收益（[报告](test/replica_revisit_20261026/REPORT.md)；[详细记录](test/log_archive/rounds/replica_revisit_20261026.md)）。
<!-- experiment:replica_revisit_20261026:end -->

<!-- experiment:hotness_logical_20261026:start -->
<!-- testlog-entry:{"id":"hotness_logical_20261026","group":"负载均衡：访问语义与正确性","started_at":"2026-10-26T22:04:50+08:00","status":"已完成","summary":"指数衰减热度与逻辑区域实现通过140万请求正确性核验，但命中群体Recall下降1.7734个百分点，共享环境吞吐异常未归因；原始明细已清理","report":"test/hotness_logical_20261026/REPORT.md","report_label":"报告","aliases":["r001指数衰减热度与逻辑区域访问hotness_logical_20261026"]} -->
- <a id="hotness_logical_20261026"></a><a id="r001指数衰减热度与逻辑区域访问hotness_logical_20261026"></a>**2026-10-26 22:04:50**（已完成）：指数衰减热度与逻辑区域实现通过140万请求正确性核验，但命中群体Recall下降1.7734个百分点，共享环境吞吐异常未归因；原始明细已清理（[报告](test/hotness_logical_20261026/REPORT.md)；[详细记录](test/log_archive/rounds/hotness_logical_20261026.md)）。
<!-- experiment:hotness_logical_20261026:end -->

<!-- experiment:region_tasks_20261027:start -->
<!-- testlog-entry:{"id":"region_tasks_20261027","group":"负载均衡：访问语义与正确性","started_at":"2026-10-27T12:52:05+08:00（四机部分最早保留的启动记录；此前协议回归未保存完整墙钟起止）","status":"已完成（协议子项通过；性能与负载均衡质量未达标）","summary":"查询级区域任务与多目标分支修复访问语义，但新协议QPS由17,300降至7,199，接收校验等待显著增加，默认关闭；原始明细已清理","report":"test/region_tasks_20261027/REPORT.md","report_label":"报告","aliases":["r001r010查询级逻辑区域任务多目标分支和副本源节点保留权region_tasks_20261027"]} -->
- <a id="region_tasks_20261027"></a><a id="r001r010查询级逻辑区域任务多目标分支和副本源节点保留权region_tasks_20261027"></a>**2026-10-27 12:52:05**（已完成（协议子项通过；性能与负载均衡质量未达标））：查询级区域任务与多目标分支修复访问语义，但新协议QPS由17,300降至7,199，接收校验等待显著增加，默认关闭；原始明细已清理（[报告](test/region_tasks_20261027/REPORT.md)；[详细记录](test/log_archive/rounds/region_tasks_20261027.md)）。
<!-- experiment:region_tasks_20261027:end -->

## 查询接纳、锁等待与长期稳定性

<!-- experiment:region_admission_20261027:start -->
<!-- testlog-entry:{"id":"region_admission_20261027","group":"查询接纳、锁等待与长期稳定性","started_at":"2026-10-27T14:56:48+08:00","status":"已完成","summary":"缩短区域接纳临界区使QPS由6,646恢复至约16,500–16,700，140万请求零失败，但副本命中质量仍下降；原始明细已清理","report":"test/region_admission_20261027/REPORT.md","report_label":"报告","aliases":["r001r002区域协议接收积压锁等待与负载均衡修复region_admission_20261027"]} -->
- <a id="region_admission_20261027"></a><a id="r001r002区域协议接收积压锁等待与负载均衡修复region_admission_20261027"></a>**2026-10-27 14:56:48**（已完成）：缩短区域接纳临界区使QPS由6,646恢复至约16,500–16,700，140万请求零失败，但副本命中质量仍下降；原始明细已清理（[报告](test/region_admission_20261027/REPORT.md)；[详细记录](test/log_archive/rounds/region_admission_20261027.md)）。
<!-- experiment:region_admission_20261027:end -->

<!-- experiment:admission_stall_20261103:start -->
<!-- testlog-entry:{"id":"admission_stall_20261103","group":"查询接纳、锁等待与长期稳定性","started_at":"2026-11-03T16:01:42+08:00","status":"已完成（根因定位与局部修复验证；系统级目标未全部解决）","summary":"查询上下文锁外分批回收使600秒QPS约翻倍至13,588、搜索断供266.08→2.13秒，但仍有89条失败和RSS增长；原始明细已清理","report":"test/admission_stall_20261103/REPORT.md","report_label":"报告","aliases":["r002持续负载接纳停顿定位admission_stall_20261103"]} -->
- <a id="admission_stall_20261103"></a><a id="r002持续负载接纳停顿定位admission_stall_20261103"></a>**2026-11-03 16:01:42**（已完成（根因定位与局部修复验证；系统级目标未全部解决））：查询上下文锁外分批回收使600秒QPS约翻倍至13,588、搜索断供266.08→2.13秒，但仍有89条失败和RSS增长；原始明细已清理（[报告](test/admission_stall_20261103/REPORT.md)；[详细记录](test/log_archive/rounds/admission_stall_20261103.md)）。
<!-- experiment:admission_stall_20261103:end -->

<!-- experiment:no_lb_baseline_20m_20261103:start -->
<!-- testlog-entry:{"id":"no_lb_baseline_20m_20261103","group":"查询接纳、锁等待与长期稳定性","started_at":"2026-11-03T18:17:43+08:00","status":"已完成","summary":"20分钟无LB自然回放成功QPS 11,323、失败106,335，第10–15分钟维护积压明显，离线分析另发现API外围等待与前端CPU配额变化；原始明细已清理","report":"test/no_lb_baseline_20m_20261103/REPORT.md","report_label":"报告","aliases":["r001-无负载均衡20分钟可回放基准no_lb_baseline_20m_20261103"]} -->
- <a id="no_lb_baseline_20m_20261103"></a><a id="r001-无负载均衡20分钟可回放基准no_lb_baseline_20m_20261103"></a>**2026-11-03 18:17:43**（已完成）：20分钟无LB自然回放成功QPS 11,323、失败106,335，第10–15分钟维护积压明显，离线分析另发现API外围等待与前端CPU配额变化；原始明细已清理（[报告](test/no_lb_baseline_20m_20261103/REPORT.md)；[详细记录](test/log_archive/rounds/no_lb_baseline_20m_20261103.md)）。
<!-- experiment:no_lb_baseline_20m_20261103:end -->

<!-- experiment:no_lb_qps_recovery_20261103:start -->
<!-- testlog-entry:{"id":"no_lb_qps_recovery_20261103","group":"查询接纳、锁等待与长期稳定性","started_at":"2026-11-03T20:30:17+08:00","status":"已完成","summary":"无LB最终600秒达21396成功QPS、0失败、Recall@10=96.884%，定位搜索成本与维护回收而非顶层大锁，并保留两轮20分钟失败复验；原始明细已清理","report":"test/no_lb_qps_recovery_20261103/REPORT.md","report_label":"报告","aliases":["r002-无lb吞吐恢复与顶层锁定位no_lb_qps_recovery_20261103"]} -->
- <a id="no_lb_qps_recovery_20261103"></a><a id="r002-无lb吞吐恢复与顶层锁定位no_lb_qps_recovery_20261103"></a>**2026-11-03 20:30:17**（已完成）：无LB最终600秒达21396成功QPS、0失败、Recall@10=96.884%，定位搜索成本与维护回收而非顶层大锁，并保留两轮20分钟失败复验；原始明细已清理（[报告](test/no_lb_qps_recovery_20261103/REPORT.md)；[详细记录](test/log_archive/rounds/no_lb_qps_recovery_20261103.md)）。
<!-- experiment:no_lb_qps_recovery_20261103:end -->

## 负载均衡：复制生效与生命周期

<!-- experiment:replica_ready_20261027:start -->
<!-- testlog-entry:{"id":"replica_ready_20261027","group":"负载均衡：复制生效与生命周期","started_at":"2026-10-27T16:22:15+08:00","status":"已完成","summary":"对照shadow校验、ready发布与精简维护，副本入口命中增加且100万请求零失败，净吞吐收益未证实；原始明细已清理","report":"test/replica_ready_20261027/REPORT.md","report_label":"报告","aliases":["r001副本就绪即发布与维护流程精简replica_ready_20261027"]} -->
- <a id="replica_ready_20261027"></a><a id="r001副本就绪即发布与维护流程精简replica_ready_20261027"></a>**2026-10-27 16:22:15**（已完成）：对照shadow校验、ready发布与精简维护，副本入口命中增加且100万请求零失败，净吞吐收益未证实；原始明细已清理（[报告](test/replica_ready_20261027/REPORT.md)；[详细记录](test/log_archive/rounds/replica_ready_20261027.md)）。
<!-- experiment:replica_ready_20261027:end -->

<!-- experiment:replica_pipeline_20261028:start -->
<!-- testlog-entry:{"id":"replica_pipeline_20261028","group":"负载均衡：复制生效与生命周期","started_at":"2026-10-28T14:22:25+08:00","status":"已完成","summary":"流水复制、原生加载和后台选图使固定计划READY中位由4.584降至2.966秒，160万请求零失败，但查询QPS未稳定提高；原始明细已清理","report":"test/replica_pipeline_20261028/REPORT.md","report_label":"报告","aliases":["流水复制原生加载与后台选图实现和验证replica_pipeline_20261028"]} -->
- <a id="replica_pipeline_20261028"></a><a id="流水复制原生加载与后台选图实现和验证replica_pipeline_20261028"></a>**2026-10-28 14:22:25**（已完成）：流水复制、原生加载和后台选图使固定计划READY中位由4.584降至2.966秒，160万请求零失败，但查询QPS未稳定提高；原始明细已清理（[报告](test/replica_pipeline_20261028/REPORT.md)；[详细记录](test/log_archive/rounds/replica_pipeline_20261028.md)）。
<!-- experiment:replica_pipeline_20261028:end -->

<!-- experiment:replica_lifecycle_20261029:start -->
<!-- testlog-entry:{"id":"replica_lifecycle_20261029","group":"负载均衡：复制生效与生命周期","started_at":"2026-10-29T02:34:28+08:00","status":"已完成","summary":"自适应衰退与多目标扩退容通过58项检查及本地三节点1→2→1→0验证，负载为注入模型，未证明实机QPS收益；原始明细已清理","report":"test/replica_lifecycle_20261029/REPORT.md","report_label":"报告","aliases":["热点副本自适应衰退与多目标扩展正确性replica_lifecycle_20261029"]} -->
- <a id="replica_lifecycle_20261029"></a><a id="热点副本自适应衰退与多目标扩展正确性replica_lifecycle_20261029"></a>**2026-10-29 02:34:28**（已完成）：自适应衰退与多目标扩退容通过58项检查及本地三节点1→2→1→0验证，负载为注入模型，未证明实机QPS收益；原始明细已清理（[报告](test/replica_lifecycle_20261029/REPORT.md)；[详细记录](test/log_archive/rounds/replica_lifecycle_20261029.md)）。
<!-- experiment:replica_lifecycle_20261029:end -->

<!-- experiment:subgraph_pipeline_20261103:start -->
<!-- testlog-entry:{"id":"subgraph_pipeline_20261103","group":"负载均衡：复制生效与生命周期","started_at":"2026-11-03T00:30:00+08:00","status":"已完成","summary":"子图规划复制流水化将首次发布约57秒降至29–30秒，440万请求零失败且早期完成量增加，但稳态QPS未提高","report":"test/subgraph_pipeline_20261103/REPORT.md","report_label":"报告","aliases":["r001-子图规划与复制流水化subgraph_pipeline_20261103"]} -->
- <a id="subgraph_pipeline_20261103"></a><a id="r001-子图规划与复制流水化subgraph_pipeline_20261103"></a>**2026-11-03 00:30:00**（已完成）：子图规划复制流水化将首次发布约57秒降至29–30秒，440万请求零失败且早期完成量增加，但稳态QPS未提高（[报告](test/subgraph_pipeline_20261103/REPORT.md)；[详细记录](test/log_archive/rounds/subgraph_pipeline_20261103.md)）。
<!-- experiment:subgraph_pipeline_20261103:end -->

<!-- experiment:subgraph_growth_stream_20261103:start -->
<!-- testlog-entry:{"id":"subgraph_growth_stream_20261103","group":"负载均衡：复制生效与生命周期","started_at":"2026-11-03T11:53:45+08:00","status":"已完成","summary":"边扩张边复制与双槽使全部发布55.26→47.23秒，186万请求零失败，短稳态QPS近似不变","report":"test/subgraph_growth_stream_20261103/REPORT.md","report_label":"报告","aliases":["r001-子图边扩张边复制subgraph_growth_stream_20261103"]} -->
- <a id="subgraph_growth_stream_20261103"></a><a id="r001-子图边扩张边复制subgraph_growth_stream_20261103"></a>**2026-11-03 11:53:45**（已完成）：边扩张边复制与双槽使全部发布55.26→47.23秒，186万请求零失败，短稳态QPS近似不变（[报告](test/subgraph_growth_stream_20261103/REPORT.md)；[详细记录](test/log_archive/rounds/subgraph_growth_stream_20261103.md)）。
<!-- experiment:subgraph_growth_stream_20261103:end -->

<!-- experiment:lb_sustained_20261103:start -->
<!-- testlog-entry:{"id":"lb_sustained_20261103","group":"负载均衡：复制生效与生命周期","started_at":"2026-11-03T13:37:30+08:00","status":"已完成（性能验收未通过）","summary":"30分钟自然200K回放出现5次计划零发布、8.62%失败和约843秒接纳拥堵伴搜索空闲，长期性能验收未通过；原始明细已清理","report":"test/lb_sustained_20261103/REPORT.md","report_label":"报告","aliases":["r00130分钟自然200k持续查询观察lb_sustained_20261103"]} -->
- <a id="lb_sustained_20261103"></a><a id="r00130分钟自然200k持续查询观察lb_sustained_20261103"></a>**2026-11-03 13:37:30**（已完成（性能验收未通过））：30分钟自然200K回放出现5次计划零发布、8.62%失败和约843秒接纳拥堵伴搜索空闲，长期性能验收未通过；原始明细已清理（[报告](test/lb_sustained_20261103/REPORT.md)；[详细记录](test/log_archive/rounds/lb_sustained_20261103.md)）。
<!-- experiment:lb_sustained_20261103:end -->

<!-- experiment:auto_replica_pipeline_20261103:start -->
<!-- testlog-entry:{"id":"auto_replica_pipeline_20261103","group":"负载均衡：复制生效与生命周期","started_at":"2026-11-03T16:12:03+08:00","status":"已完成：流程验证通过，性能验收未通过","summary":"实际源容量预约与统一期限接入完整流水后5次实际规划全部发布，但旧搜索版本仍有约8.96%失败和低命中，性能未验收；原始明细已清理","report":"test/auto_replica_pipeline_20261103/REPORT.md","report_label":"报告","aliases":["r001自动副本预算完整流水和规划截止时间auto_replica_pipeline_20261103"]} -->
- <a id="auto_replica_pipeline_20261103"></a><a id="r001自动副本预算完整流水和规划截止时间auto_replica_pipeline_20261103"></a>**2026-11-03 16:12:03**（已完成：流程验证通过，性能验收未通过）：实际源容量预约与统一期限接入完整流水后5次实际规划全部发布，但旧搜索版本仍有约8.96%失败和低命中，性能未验收；原始明细已清理（[报告](test/auto_replica_pipeline_20261103/REPORT.md)；[详细记录](test/log_archive/rounds/auto_replica_pipeline_20261103.md)）。
<!-- experiment:auto_replica_pipeline_20261103:end -->

## 副本回源与RDMA尾部执行

<!-- experiment:replica_wait_20261028:start -->
<!-- testlog-entry:{"id":"replica_wait_20261028","group":"副本回源与RDMA尾部执行","started_at":"2026-10-28T20:53:13+08:00","status":"已完成","summary":"远端等待阈值调至16 ms使热点/原始流QPS中位提升12.97%/15.33%，质量通过当轮门槛，但仍100%回源；原始明细已清理","report":"test/replica_wait_20261028/REPORT.md","report_label":"报告","aliases":["副本远端等待阈值优化replica_wait_20261028"]} -->
- <a id="replica_wait_20261028"></a><a id="副本远端等待阈值优化replica_wait_20261028"></a>**2026-10-28 20:53:13**（已完成）：远端等待阈值调至16 ms使热点/原始流QPS中位提升12.97%/15.33%，质量通过当轮门槛，但仍100%回源；原始明细已清理（[报告](test/replica_wait_20261028/REPORT.md)；[详细记录](test/log_archive/rounds/replica_wait_20261028.md)）。
<!-- experiment:replica_wait_20261028:end -->

<!-- experiment:hop_return_20261030:start -->
<!-- testlog-entry:{"id":"hop_return_20261030","group":"副本回源与RDMA尾部执行","started_at":"2026-10-30T01:42:25+08:00","status":"已完成","summary":"同容量完整h跳预算扩展提升QPS约6.74%，有界拉取降低回源却使QPS下降6.56%，600万请求零失败，拉取不默认开启；原始明细已清理","report":"test/hop_return_20261030/REPORT.md","report_label":"报告","aliases":["完整h跳副本的预算扩展与有界rdma拉取hop_return_20261030"]} -->
- <a id="hop_return_20261030"></a><a id="完整h跳副本的预算扩展与有界rdma拉取hop_return_20261030"></a>**2026-10-30 01:42:25**（已完成）：同容量完整h跳预算扩展提升QPS约6.74%，有界拉取降低回源却使QPS下降6.56%，600万请求零失败，拉取不默认开启；原始明细已清理（[报告](test/hop_return_20261030/REPORT.md)；[详细记录](test/log_archive/rounds/hop_return_20261030.md)）。
<!-- experiment:hop_return_20261030:end -->

<!-- experiment:hop_tuning_20261031:start -->
<!-- testlog-entry:{"id":"hop_tuning_20261031","group":"副本回源与RDMA尾部执行","started_at":"2026-10-31T06:21:50+08:00","status":"已完成","summary":"联合调整容量、接流份额和拉取阈值可降低回源，但份额微调QPS差异不足3%，原确认按用户要求部分停止，保留候选而非全局最优结论；原始明细已清理","report":"test/hop_tuning_20261031/CONCLUSION.md","report_label":"报告","aliases":["容量接流比例与尾部rdma拉取阈值联合优化hop_tuning_20261031"]} -->
- <a id="hop_tuning_20261031"></a><a id="容量接流比例与尾部rdma拉取阈值联合优化hop_tuning_20261031"></a>**2026-10-31 06:21:50**（已完成）：联合调整容量、接流份额和拉取阈值可降低回源，但份额微调QPS差异不足3%，原确认按用户要求部分停止，保留候选而非全局最优结论；原始明细已清理（[报告](test/hop_tuning_20261031/CONCLUSION.md)；[详细记录](test/log_archive/rounds/hop_tuning_20261031.md)）。
<!-- experiment:hop_tuning_20261031:end -->

<!-- experiment:async_pull_20261031:start -->
<!-- testlog-entry:{"id":"async_pull_20261031","group":"副本回源与RDMA尾部执行","started_at":"2026-10-31T22:10:25+08:00","status":"已完成","summary":"异步尾部拉取降低目标可执行排队，但相对同步QPS下降0.45%，命中Recall约94.66%未达95%，默认关闭；原始明细已清理","report":"test/async_pull_20261031/REPORT.md","report_label":"报告","aliases":["有界异步rdma尾部执行与源端读服务解耦async_pull_20261031"]} -->
- <a id="async_pull_20261031"></a><a id="有界异步rdma尾部执行与源端读服务解耦async_pull_20261031"></a>**2026-10-31 22:10:25**（已完成）：异步尾部拉取降低目标可执行排队，但相对同步QPS下降0.45%，命中Recall约94.66%未达95%，默认关闭；原始明细已清理（[报告](test/async_pull_20261031/REPORT.md)；[详细记录](test/log_archive/rounds/async_pull_20261031.md)）。
<!-- experiment:async_pull_20261031:end -->

<!-- experiment:pull_workers_threshold_20261031:start -->
<!-- testlog-entry:{"id":"pull_workers_threshold_20261031","group":"副本回源与RDMA尾部执行","started_at":"2026-10-31T23:54:49+08:00","status":"已完成","summary":"副本专用4/8线程与阈值筛选未显示稳定QPS收益，额度64→16减少尾队列与带宽但源排队恶化，命中质量未达标；原始明细已清理","report":"test/pull_workers_threshold_20261031/REPORT.md","report_label":"报告","aliases":["主64副本专用48线程及剩余预算阈值快速筛选pull_workers_threshold_20261031"]} -->
- <a id="pull_workers_threshold_20261031"></a><a id="主64副本专用48线程及剩余预算阈值快速筛选pull_workers_threshold_20261031"></a>**2026-10-31 23:54:49**（已完成）：副本专用4/8线程与阈值筛选未显示稳定QPS收益，额度64→16减少尾队列与带宽但源排队恶化，命中质量未达标；原始明细已清理（[报告](test/pull_workers_threshold_20261031/REPORT.md)；[详细记录](test/log_archive/rounds/pull_workers_threshold_20261031.md)）。
<!-- experiment:pull_workers_threshold_20261031:end -->

<!-- experiment:pull_policy_help_20261102:start -->
<!-- testlog-entry:{"id":"pull_policy_help_20261102","group":"副本回源与RDMA尾部执行","started_at":"2026-11-02T09:35:09+08:00","status":"已完成","summary":"修正暂停策略年龄并由空闲主池帮助续跑，续跑排队4.108→0.670 ms，但QPS仅增1.02%且命中质量未达标","report":"test/pull_policy_help_20261102/REPORT.md","report_label":"报告","aliases":["暂停策略年龄与空闲主池帮助rdma尾部续跑pull_policy_help_20261102"]} -->
- <a id="pull_policy_help_20261102"></a><a id="暂停策略年龄与空闲主池帮助rdma尾部续跑pull_policy_help_20261102"></a>**2026-11-02 09:35:09**（已完成）：修正暂停策略年龄并由空闲主池帮助续跑，续跑排队4.108→0.670 ms，但QPS仅增1.02%且命中质量未达标（[报告](test/pull_policy_help_20261102/REPORT.md)；[详细记录](test/log_archive/rounds/pull_policy_help_20261102.md)）。
<!-- experiment:pull_policy_help_20261102:end -->

<!-- experiment:pull_coalesce_20261102:start -->
<!-- testlog-entry:{"id":"pull_coalesce_20261102","group":"副本回源与RDMA尾部执行","started_at":"2026-11-02T11:24:07+08:00","status":"已完成","summary":"跨查询拉取合批使物理RPC减少23.13%，但QPS仅增0.29%、源净卸载和命中质量未达标，默认关闭","report":"test/pull_coalesce_20261102/REPORT.md","report_label":"报告","aliases":["跨查询rdma拉取聚合与副本布局决策pull_coalesce_20261102"]} -->
- <a id="pull_coalesce_20261102"></a><a id="跨查询rdma拉取聚合与副本布局决策pull_coalesce_20261102"></a>**2026-11-02 11:24:07**（已完成）：跨查询拉取合批使物理RPC减少23.13%，但QPS仅增0.29%、源净卸载和命中质量未达标，默认关闭（[报告](test/pull_coalesce_20261102/REPORT.md)；[详细记录](test/log_archive/rounds/pull_coalesce_20261102.md)）。
<!-- experiment:pull_coalesce_20261102:end -->

<!-- experiment:onesided_pull_20261102:start -->
<!-- testlog-entry:{"id":"onesided_pull_20261102","group":"副本回源与RDMA尾部执行","started_at":"2026-11-02T13:12:05+08:00","status":"已完成","summary":"真实单边RDMA与链式提交减少post/CQ及I/O CPU开销，但无稳定QPS收益且命中质量不足，读取对象为注册内存","report":"test/onesided_pull_20261102/REPORT.md","report_label":"报告","aliases":["单边rdma读取与cotra通信机制评估onesided_pull_20261102"]} -->
- <a id="onesided_pull_20261102"></a><a id="单边rdma读取与cotra通信机制评估onesided_pull_20261102"></a>**2026-11-02 13:12:05**（已完成）：真实单边RDMA与链式提交减少post/CQ及I/O CPU开销，但无稳定QPS收益且命中质量不足，读取对象为注册内存（[报告](test/onesided_pull_20261102/REPORT.md)；[详细记录](test/log_archive/rounds/onesided_pull_20261102.md)）。
<!-- experiment:onesided_pull_20261102:end -->

<!-- experiment:onesided_missing_20261102:start -->
<!-- testlog-entry:{"id":"onesided_missing_20261102","group":"副本回源与RDMA尾部执行","started_at":"2026-11-02T14:25:30+08:00","status":"已完成","summary":"单边缺失码过滤在干净对照中减少95.53% READ、确认QPS提高7.40%，但命中Recall约94.71%，仅局部机制通过","report":"test/onesided_missing_20261102/REPORT.md","report_label":"报告","aliases":["单边缺失码过滤与续跑优化onesided_missing_20261102"]} -->
- <a id="onesided_missing_20261102"></a><a id="单边缺失码过滤与续跑优化onesided_missing_20261102"></a>**2026-11-02 14:25:30**（已完成）：单边缺失码过滤在干净对照中减少95.53% READ、确认QPS提高7.40%，但命中Recall约94.71%，仅局部机制通过（[报告](test/onesided_missing_20261102/REPORT.md)；[详细记录](test/log_archive/rounds/onesided_missing_20261102.md)）。
<!-- experiment:onesided_missing_20261102:end -->

<!-- experiment:replica_no_dwell_20261104:start -->
<!-- testlog-entry:{"id":"replica_no_dwell_20261104","group":"副本回源与RDMA尾部执行","started_at":"2026-11-04T11:30:01+08:00","status":"已完成","summary":"移除源驻留后回源83.26%、剩余98/200，QPS21,469；接流降至6.83%，历史对照非同图配对，卸载仍不足；原始明细已清理","report":"test/replica_no_dwell_20261104/REPORT.md","report_label":"报告","aliases":["r001-取消副本源驻留门槛与回源预算观测replica_no_dwell_20261104"]} -->
- <a id="replica_no_dwell_20261104"></a><a id="r001-取消副本源驻留门槛与回源预算观测replica_no_dwell_20261104"></a>**2026-11-04 11:30:01**（已完成）：移除源驻留后回源83.26%、剩余98/200，QPS21,469；接流降至6.83%，历史对照非同图配对，卸载仍不足；原始明细已清理（[报告](test/replica_no_dwell_20261104/REPORT.md)；[详细记录](test/log_archive/rounds/replica_no_dwell_20261104.md)）。
<!-- experiment:replica_no_dwell_20261104:end -->

## 负载均衡：自动复制、接流与真实卸载

<!-- experiment:lb_recovered_long_20261103:start -->
<!-- testlog-entry:{"id":"lb_recovered_long_20261103","group":"负载均衡：自动复制、接流与真实卸载","started_at":"2026-11-03T23:26:23+08:00","status":"已完成","summary":"新配置LB20分钟2307万请求零失败、19226QPS；副本接流1.04%且回源83.66%，尚无净收益，修复容量重复规划与0值校验；原始明细已清理","report":"test/lb_recovered_long_20261103/REPORT.md","report_label":"报告","aliases":["r001-新吞吐配置自动负载均衡长测lb_recovered_long_20261103"]} -->
- <a id="lb_recovered_long_20261103"></a><a id="r001-新吞吐配置自动负载均衡长测lb_recovered_long_20261103"></a>**2026-11-03 23:26:23**（已完成）：新配置LB20分钟2307万请求零失败、19226QPS；副本接流1.04%且回源83.66%，尚无净收益，修复容量重复规划与0值校验；原始明细已清理（[报告](test/lb_recovered_long_20261103/REPORT.md)；[详细记录](test/log_archive/rounds/lb_recovered_long_20261103.md)）。
<!-- experiment:lb_recovered_long_20261103:end -->

## 负载均衡：自动构建、复制与发布

<!-- experiment:dynamic_replica_policy_20261104:start -->
<!-- testlog-entry:{"id":"dynamic_replica_policy_20261104","group":"负载均衡：自动构建、复制与发布","started_at":"2026-11-04T01:23:09+08:00","status":"已完成","summary":"α改为单小图上限，三轮连续发布2/4/2份、峰值总α≈.596；最终20,091QPS零失败但未改善基线与源排队，149项回归通过；原始明细已清理","report":"test/dynamic_replica_policy_20261104/REPORT.md","report_label":"报告","aliases":["单区域α与在线副本布局闭环dynamic_replica_policy_20261104"]} -->
- <a id="dynamic_replica_policy_20261104"></a><a id="单区域α与在线副本布局闭环dynamic_replica_policy_20261104"></a>**2026-11-04 01:23:09**（已完成）：α改为单小图上限，三轮连续发布2/4/2份、峰值总α≈.596；最终20,091QPS零失败但未改善基线与源排队，149项回归通过；原始明细已清理（[报告](test/dynamic_replica_policy_20261104/REPORT.md)；[详细记录](test/log_archive/rounds/dynamic_replica_policy_20261104.md)）。
<!-- experiment:dynamic_replica_policy_20261104:end -->

## 负载均衡与热点副本

<!-- experiment:covered_entry_admission_20261104:start -->
<!-- testlog-entry:{"id":"covered_entry_admission_20261104","group":"负载均衡与热点副本","started_at":"2026-11-04T10:23:11+08:00","status":"已完成","summary":"全部覆盖入口开放与无新增统计锁的回源预算关闭已实现，单机三节点计数/关闭及23+119项检查通过，真实吞吐与质量待验证","report":"test/covered_entry_admission_20261104/REPORT.md","report_label":"报告","aliases":["r001-全覆盖入口与回源预算准入covered_entry_admission_20261104"]} -->
- <a id="covered_entry_admission_20261104"></a><a id="r001-全覆盖入口与回源预算准入covered_entry_admission_20261104"></a>**2026-11-04 10:23:11**（已完成）：全部覆盖入口开放与无新增统计锁的回源预算关闭已实现，单机三节点计数/关闭及23+119项检查通过，真实吞吐与质量待验证（[报告](test/covered_entry_admission_20261104/REPORT.md)；[详细记录](test/log_archive/rounds/covered_entry_admission_20261104.md)）。
<!-- experiment:covered_entry_admission_20261104:end -->

## R001 负载均衡

<!-- experiment:covered_entry_long_20261104:start -->
<!-- testlog-entry:{"id":"covered_entry_long_20261104","group":"R001 负载均衡","started_at":"2026-11-04T10:39:05+08:00","status":"已完成","summary":"600秒发布6份副本、开放入口反馈生效，接流21.90%；QPS20,655但Recall下降1.18pp，联合目标未通过；原始明细已清理","report":"test/covered_entry_long_20261104/REPORT.md","report_label":"报告","aliases":["r001-全覆盖入口准入10分钟三机接流测试covered_entry_long_20261104"]} -->
- <a id="covered_entry_long_20261104"></a><a id="r001-全覆盖入口准入10分钟三机接流测试covered_entry_long_20261104"></a>**2026-11-04 10:39:05**（已完成）：600秒发布6份副本、开放入口反馈生效，接流21.90%；QPS20,655但Recall下降1.18pp，联合目标未通过；原始明细已清理（[报告](test/covered_entry_long_20261104/REPORT.md)；[详细记录](test/log_archive/rounds/covered_entry_long_20261104.md)）。
<!-- experiment:covered_entry_long_20261104:end -->

## 搜索扩展、RDMA与负载均衡

<!-- experiment:remote_pull_first_20261104:start -->
<!-- testlog-entry:{"id":"remote_pull_first_20261104","group":"搜索扩展、RDMA与负载均衡","started_at":"2026-11-04T14:36:48+08:00","status":"已完成诊断、修复与复验；新策略整体收益未证实","summary":"按源FIFO与CQ分片将优先RDMA拉取从2482恢复至16593–17518QPS、p99降至524–545ms，但仍低于原策略20688QPS；原始明细已清理","report":"test/remote_pull_first_20261104/REPORT.md","report_label":"报告","aliases":["普通搜索优先rdma拉取与连续访问派发remote_pull_first_20261104"]} -->
- <a id="remote_pull_first_20261104"></a><a id="普通搜索优先rdma拉取与连续访问派发remote_pull_first_20261104"></a>**2026-11-04 14:36:48**（已完成诊断、修复与复验；新策略整体收益未证实）：按源FIFO与CQ分片将优先RDMA拉取从2482恢复至16593–17518QPS、p99降至524–545ms，但仍低于原策略20688QPS；原始明细已清理（[报告](test/remote_pull_first_20261104/REPORT.md)；[详细记录](test/log_archive/rounds/remote_pull_first_20261104.md)）。
<!-- experiment:remote_pull_first_20261104:end -->

## 远端拉取、分支与搜索预算

<!-- experiment:candidate_sync_20261104:start -->
<!-- testlog-entry:{"id":"candidate_sync_20261104","group":"远端拉取、分支与搜索预算","started_at":"2026-11-04T19:43:39+08:00","status":"已完成","summary":"完成唯一分支与异步同步，复测QPS提升8.34%–10.55%、p99降低45.87%–54.71%，召回约97.05%，p50变差且仍慢于原成本策略；原始明细已清理","report":"test/candidate_sync_20261104/REPORT.md","report_label":"报告","aliases":["唯一分支与异步候选同步candidate_sync_20261104"]} -->
- <a id="candidate_sync_20261104"></a><a id="唯一分支与异步候选同步candidate_sync_20261104"></a>**2026-11-04 19:43:39**（已完成）：完成唯一分支与异步同步，复测QPS提升8.34%–10.55%、p99降低45.87%–54.71%，召回约97.05%，p50变差且仍慢于原成本策略；原始明细已清理（[报告](test/candidate_sync_20261104/REPORT.md)；[详细记录](test/log_archive/rounds/candidate_sync_20261104.md)）。
<!-- experiment:candidate_sync_20261104:end -->

<!-- experiment:candidate_stalls_20261104:start -->
<!-- testlog-entry:{"id":"candidate_stalls_20261104","group":"远端拉取、分支与搜索预算","started_at":"2026-11-04T22:23:06+08:00","status":"已完成；局部优化通过，实际搜索CPU主导目标未达成","summary":"协议异步化提高QPS6.09%、容量预留降低p99但未提升吞吐，离线复核定位首次计算排队及拉取控制长尾并修复候选报告漏计","report":"test/candidate_stalls_20261104/REPORT.md","report_label":"完整报告","aliases":["分支与候选交付的非阻塞收尾candidate_stalls_20261104","分支交付异步化与共享目录扩容停顿修复candidate_stalls_20261104"]} -->
- <a id="candidate_stalls_20261104"></a><a id="分支与候选交付的非阻塞收尾candidate_stalls_20261104"></a><a id="分支交付异步化与共享目录扩容停顿修复candidate_stalls_20261104"></a>**2026-11-04 22:23:06**（已完成；局部优化通过，实际搜索CPU主导目标未达成）：协议异步化提高QPS6.09%、容量预留降低p99但未提升吞吐，离线复核定位首次计算排队及拉取控制长尾并修复候选报告漏计（[完整报告](test/candidate_stalls_20261104/REPORT.md)；[详细记录](test/log_archive/rounds/candidate_stalls_20261104.md)）。
<!-- experiment:candidate_stalls_20261104:end -->

<!-- experiment:pull_sync_compare_20261105:start -->
<!-- testlog-entry:{"id":"pull_sync_compare_20261105","group":"远端拉取、分支与搜索预算","started_at":"2026-11-05T09:01:29+08:00","status":"已完成；当前方案未获得吞吐/延迟收益","summary":"同C1536/D200/64线程，拉取同步方案较原成本策略QPS变化-8.90%、p99变化+24.34%，Recall增加0.1889pp，剔除两组外部干扰","report":"test/pull_sync_compare_20261105/REPORT.md","report_label":"完整报告","aliases":["原成本分支与当前rdma拉取加候选同步的同期对照pull_sync_compare_20261105"]} -->
- <a id="pull_sync_compare_20261105"></a><a id="原成本分支与当前rdma拉取加候选同步的同期对照pull_sync_compare_20261105"></a>**2026-11-05 09:01:29**（已完成；当前方案未获得吞吐/延迟收益）：同C1536/D200/64线程，拉取同步方案较原成本策略QPS变化-8.90%、p99变化+24.34%，Recall增加0.1889pp，剔除两组外部干扰（[完整报告](test/pull_sync_compare_20261105/REPORT.md)；[详细记录](test/log_archive/rounds/pull_sync_compare_20261105.md)）。
<!-- experiment:pull_sync_compare_20261105:end -->

<!-- experiment:budget_sweep_20261105:start -->
<!-- testlog-entry:{"id":"budget_sweep_20261105","group":"远端拉取、分支与搜索预算","started_at":"2026-11-05T11:10:38+08:00","status":"已完成","summary":"两策略五档预算对照完成：95%门槛下当前QPS约2.60万，比原方案低12.55%，新增CPU工作和Intel31计算排队仍需优化","report":"test/budget_sweep_after_return_20261105/REPORT.md","report_label":"完整报告","aliases":["原成本分支与拉取同步策略的搜索预算扫描budget_sweep_20261105"]} -->
- <a id="budget_sweep_20261105"></a><a id="原成本分支与拉取同步策略的搜索预算扫描budget_sweep_20261105"></a>**2026-11-05 11:10:38**（已完成）：两策略五档预算对照完成：95%门槛下当前QPS约2.60万，比原方案低12.55%，新增CPU工作和Intel31计算排队仍需优化（[完整报告](test/budget_sweep_after_return_20261105/REPORT.md)；[详细记录](test/log_archive/rounds/budget_sweep_20261105.md)）。
<!-- experiment:budget_sweep_20261105:end -->

## 入口、控制与结果回传等待

<!-- experiment:head_return_20261105:start -->
<!-- testlog-entry:{"id":"head_return_20261105","group":"入口、控制与结果回传等待","started_at":"2026-11-05T14:24:22+08:00","status":"已完成","summary":"有界响应批次与头部资源分配使返回路径样本均值33.21→1.74ms，QPS29069→35549，端到端p9988.12→93.52ms","report":"test/head_return_20261105/REPORT.md","report_label":"完整报告","aliases":["顶层合并与api回传长等待定位修复head_return_20261105"]} -->
- <a id="head_return_20261105"></a><a id="顶层合并与api回传长等待定位修复head_return_20261105"></a>**2026-11-05 14:24:22**（已完成）：有界响应批次与头部资源分配使返回路径样本均值33.21→1.74ms，QPS29069→35549，端到端p9988.12→93.52ms（[完整报告](test/head_return_20261105/REPORT.md)；[详细记录](test/log_archive/rounds/head_return_20261105.md)）。
<!-- experiment:head_return_20261105:end -->
