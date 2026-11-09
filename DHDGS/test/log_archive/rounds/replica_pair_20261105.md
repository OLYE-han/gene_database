<!-- experiment:replica_pair_20261105:start -->
## 副本与源节点限定RDMA拉取及同步（replica_pair_20261105）

**状态**：已完成

**开始时间**：2026-11-05T20:36:51+08:00

**结束时间**：2026-11-05T21:32:00+08:00

**目的 / 要解决的问题**：验证限定机制能否改善S30/D80，且不影响无副本普通分支

**假设**：将READ与同步限于副本源关系可减少回源等待且避免全局协议成本

**期望效果**：零失败及预算错误；QPS改善、p99下降、源实际工作减少；实际召回约95%

**环境与配置 / 对照**：D128/beam11/C1536/3数据节点64搜索线程；同200K自然查询，统一预热和CPU布局；head interleave=2,3，Intel30为56物理核上的64工作线程；tmpfs副本/暖缓存；12次×600K正式与同量预热

**本轮改变的变量**：N/S30/D80，pair协议开关，正反序两重复；容量、入口、条件份额固定；首批localalloc内存位置出现NUMA2/3差异，正式c/d统一head interleave=2,3，首批a仅诊断保留

**代码改动**：新增replica_source作用域、配对actor目录、逐分支门控和作用域回归；紧凑客户端核对配对协议范围

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：正式7.2M零失败及预算/范围错误；S30 QPS+2.67%而p99+2.00%，D80 QPS−0.83%且p99变化约一个分桶；命中召回均提升约2.3pp，但源进程CPU未降、自然副本命中仅2.60%/3.37%，无稳定吞吐/长尾双赢；无副本on零READ/sync但仍预注册26.27GiB。首批NUMA、配置/脚本失败均保留，默认原策略不变。

**报告、图表与证据**

- test/replica_pair_20261105/REPORT.md
- test/replica_pair_20261105/comparison.png
- test/replica_pair_20261105/node_comparison.png
- test/replica_pair_20261105/d80_on_c_p99.png
- test/replica_pair_20261105/comparison.json
- test/replica_pair_20261105/resources.json

**下一步建议**：保持默认off；后续按Research F02/F13/F26评估入口覆盖、源队列感知交接及源按需注册，本轮未执行这些新优化

**日志更新时间**：2026-11-05T21:32:00+08:00
<!-- experiment:replica_pair_20261105:end -->
