<!-- experiment:boundary_portals_20261106:start -->
## 副本有向边界点及连接路径增强（boundary_portals_20261106）

**状态**：已完成

**开始时间**：2026-11-06T01:37:27+08:00

**结束时间**：2026-11-06T02:38:27+08:00

**目的 / 要解决的问题**：检验边界增强是否恢复第三节点可达与副本Recall，同时评估额外空间和性能

**假设**：原源图的方向性边界及有向连接路径可改善副本路径，全部边界为上界对照；单纯边界数量可能不是主要瓶颈

**期望效果**：正确保留原图边与入口；记录空间/构建/复制成本，量化召回/第三节点/源卸载/QPS/p99；不预设成功

**环境与配置 / 对照**：原成本分支D128/beam11/C1536/每节点64线程；此前热点200K occurrence（42898不同query）原样重放；冻结上一轮搜索二进制/CPU/NUMA

**本轮改变的变量**：N、S30/D80原核心、1024/方向代表性、三跳内全部可达边界，core不变，独立额外3GiB硬限仍计实际容量；200K热点序列，每正式组600K预热+600K正式、两重复正反序；S30份额1，D80目标份额0.85/0.60

**代码改动**：新增boundary_portals构建/原有向父链证书/独立容量及YAML/测试；config/community/locality/replica_budget/load_balance/replicas接入构建与容量；收尾绑定tier及活动/认证入口、重算实际core、外层期限、空proof兼容。性能冻结原搜索二进制，收尾兼容代码和验证单列，不改变搜索/派发/预算或维护暂停的READ/sync。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：14组8.4M正式查询零失败及预算/作用域错误；S30全边界QPS+3.08%、p99−7.80%、Recall94.0286→94.1125%，D80 QPS−0.72%、Recall93.8548→93.9843%；边界证书/容量局部通过，主要质量/净收益未解决。32项工作区回归及真实大计划验证通过。D80 UUID分流群体变化取共同命中交集；N_B预热干扰及原策略重复物理报告保留，正式窗口通过。预建对照未验证在线自动增长追加边界。

**报告、图表与证据**

- test/boundary_portals_20261106/REPORT.md
- test/boundary_portals_20261106/comparison.png
- test/boundary_portals_20261106/branch_destinations.png
- test/boundary_portals_20261106/example_flow.png
- test/boundary_portals_20261106/comparison.json
- test/boundary_portals_20261106/runs.csv
- test/boundary_portals_20261106/planning.json
- test/boundary_portals_20261106/final_audit.json
- test/boundary_portals_20261106/compatibility_unit.log
- test/boundary_portals_20261106/compatibility_real_source.json
- test/boundary_portals_20261106/source_identity.json
- test/boundary_portals_20261106/compatibility_source_identity.json

**下一步建议**：保持默认关闭；仅建议研究入口—核心—门户联合覆盖和预算交接，先补门户实际展开/源外候选拒绝原因；自动增长额外预约及在线成本另验，本轮未执行进一步优化

**日志更新时间**：2026-11-06T02:38:27+08:00
<!-- experiment:boundary_portals_20261106:end -->
