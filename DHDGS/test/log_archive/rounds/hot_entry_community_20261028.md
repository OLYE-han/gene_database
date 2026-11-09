<!-- experiment:hot_entry_community_20261028:start -->
## 完整热点闭包与PPR副本选图（hot_entry_community_20261028）

**状态**：已完成

**开始时间**：2026-10-28T17:27:26+08:00

**结束时间**：2026-10-28T20:09:32+08:00

**目的 / 要解决的问题**：验证完整区域质量、热点承接及真实源CPU卸载

**假设**：完整热点邻域与PPR边界扩张可减少内部缺口和回源

**期望效果**：质量不退化且提高有效吞吐/源卸载；失败亦记录

**环境与配置 / 对照**：完整seq_2M不可变L2图、全语料self-excluded Recall@10；Intel-35头/原生负载端、Intel-31/32/30数据节点，各64搜索线程、预算200、并发1024、RDMA、固定NUMA。热页缓存；静态预建与在线维护分开。

**本轮改变的变量**

- 旧工作集合+halo1、完整热点闭包、基础/宽PPR；h=1/2/3，K=16/64/256/1024，完整记录上限65K/131K/262K，PPR轮数0/1/4及时间/扫描上限探索。
- 静态部署同一1GiB/65,536条上限，根分流0.8，副本子分支预算比例0.5/0.75；自然20K、早期实际39.615%热点60K、更正后80%主动热点分量60K；基线和wide75各3次。
- 在线200K包含规划/复制/安装：初始同步源端RPC及最终有界start/poll；用户确认另一轮DHDGS结束后，基线1→在线1→在线2→基线2，固定同序occurrence。

**代码改动**：新增cpp/replica_community.hpp及绑定、community.py；完整有向h跳热点闭包/PPR完整球扩张、依赖字节估算、源图独立完整性/可达性复验；load_balance/replicas/replica_pipeline接入证明与固定成员；单任务start/poll后台源规划；config/YAML参数；48项回归。修正rdma_benchmark的query ID去重以保留发生次数。保存source/source_final/source_verified/source_async及哈希，未切换默认策略。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：48项回归、全图复验和最终80万查询预算/执行器/去重/排空/对账通过。29份查询摘要共238万次，0查询失败，包含中断/干扰/通信异常记录，非独立向量数。静态热点3次：命中32.56%（旧参照4.69%），源搜索CPU中位−8.3%、展开−10.9%；QPS仅+1.1%、总CPU+6.7%、查询协议字节+20.2%。整体Recall约96.65%，命中配对损失0.40～0.44pp，未通过0.2pp条件；新出口率仍高于旧工作集合。最终在线两组均值QPS+2.32%、源搜索CPU−5.22%、总搜索CPU+3.97%，源进程CPU占用没有下降；条件Recall95.343%/95.047%，配对损失约0.9pp。采样最大间隔241/240ms、发布8.44/7.72s，无>5s请求；不宣布初次17s通信异常根因解决。保留早期元数据/测试夹具失败、负载去重修正、主动中断none、受干扰worksets_hot_r2、初始在线回传长等待及30s退出检查超时、离线汇总缺少空事件文件的失败和重跑。最终三机12个补测PID退出，源码一致。热缓存/共享机/短测/重复身份限制；本轮完成不等于R001目标已解决。 补充离线审计：原1.6万是原始查询流，新9千是源0热点流；源每query搜索CPU3.634→6.642ms，总CPU近似，固定源计算供给估算8843QPS。未进行新增性能测试，未完全排除版本/配置与外部负载影响。

**报告、图表与证据**

- [完整报告](../../hot_entry_community_20261028/REPORT.md)
- [静态指标与范围图](../../hot_entry_community_20261028/hot80_comparison.png)
- [结构与规划代价图](../../hot_entry_community_20261028/region_quality.png)
- [在线发布图](../../hot_entry_community_20261028/runs/dynamic_async200k_r1/cases/dynamic_async200k_r1/online_timeline.png)
- [在线指标与CPU采样](../../hot_entry_community_20261028/online_summary.json)
- [逐组索引](../../hot_entry_community_20261028/all_case_summary.json)
- [源码哈希](../../hot_entry_community_20261028/source_async_hashes.json)
- [三机源码/退出审计](../../hot_entry_community_20261028/final_deployment_shutdown_audit.json)
- [R001-A15](../../../Research.md#r001-a15)、[已验证机制](../../../DESIGN.md#complete-hot-entry-region)
- [QPS口径与源瓶颈分析](../../hot_entry_community_20261028/QPS_COMPARISON.md)

**下一步建议**：R001保持开放。先固定区域分离执行器状态复用/小额授权/源副本重复工作的贡献，再校准净CPU收益和热点寿命；独立条件召回、整进程CPU与QPS联合验收。补长测、传输错误观测、冷SSD及更大规模；不能靠删内部点、提前跨机或整区排除制造收益。

**日志更新时间**：2026-10-28T20:23:16+08:00
<!-- experiment:hot_entry_community_20261028:end -->
