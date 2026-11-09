<!-- experiment:search_scaling_unbounded_20261023:start -->
## 搜索线程16/32/48/64/96与无应用层上限动态网络扩缩容（search_scaling_unbounded_20261023）

**状态**：已完成

**开始时间**：2026-10-23T10:38:10+08:00

**结束时间**：2026-10-23T14:11:40+08:00

**目的 / 要解决的问题**：检验搜索线程增加能否提高吞吐、降低计算排队；定位非线性扩展原因并按证据优化

**假设**：解除网络线程/连接数量上限后，搜索工作量与可用物理核决定扩展曲线；NUMA、SMT、负载不均或控制开销可能限制加速

**期望效果**：固定预算及查询质量，获得可解释的线程扩展曲线和网络资源时间序列，不预设线性加速成立

**环境与配置 / 对照**：Intel35 head+独立网络客户端；Intel31/32/30数据；seq_2M完整索引，预算200，top10，初始并发1024，每正式测试20万请求、预热2000；热缓存

**本轮改变的变量**

- 搜索线程16/32/48/64/96，固定并发1024、预算200；原版每档2次，最终版16/32/48各1次、64/96各2次，每次20万请求、预热2000。
- 所有服务端收发reactor、连接及公共API数量上限为0；busy-lane比例0.5、预热连接8、并行建连2、压力4样本、采样10ms、冷却100ms、空闲1500ms。网络资源动态增长/回收；CPU亲和范围固定，客户端容量4096。
- 跳过已见RaBitQ估距关闭/开启；缩短顶层指标与接收扩容锁范围；计数器stripe=1/128；有预算ID直接寻址关闭/64MiB。
- 96线程跨插槽物理核与原插槽物理核+SMT布局诊断；source/source_opt/source_head/source_lookup四个冻结版本、同二进制开关对照；全部负结果与重试保留。

**代码改动**：实现网络数量上限0、繁忙lane比例扩容与空闲回收、轻量网络资源采样、压缩帧读取；增加skip_seen_estimates、有容量预算的native_id_lookup_bytes、可选expansion_counter_stripes及counter_update_ms；缩短顶层监测/接收扩容锁范围。当前工作区重新编译；没有替换既有默认发布快照。DESIGN §43、LOCKS §18及实验复现脚本同步更新。

**登记时 Git HEAD**：6bfdad4cccbfbebeadab3a27ca5f3d93ae97e1c1

**结果与限制**：33次正式测量660万请求全部成功，Recall@10 97.017%–97.060%，8892908个分支完整时序，预算/重访违规0。最终16/32/48/64/96线程QPS均值为6903/12962/19132/21315/19629；64线程比原版均值提升16.1%，两次p99为107.48/127.92ms；平均网络/控制残余16.94%/19.48%，固定近p99样本2.26%/1.33%，均通过既定吞吐与双占比条件。96线程理想线性扩展未达成：最忙节点每分支CPU成本4.961→8.661ms，另有NUMA/SMT及负载不均因素；同插槽SMT诊断22815 QPS但p99 129.60ms。分散计数器未稳定提速，网络仍存在未完全定位的长尾。1条在途心跳导致统计完整性标记未完成，结果与搜索时序完整。最新回归60通过/1项io_uring按配置跳过，四机源码和实际加载二进制核对通过。热缓存、2000条验证向量循环，不能外推冷SSD/百亿数据；64线程完整输出吞吐含最后同步落盘为10629–10820。lookup_s96在预热中因控制会话中断，保留证据后以retry重测；初次配置回归、构建辅助脚本及离线idle字段审计的错误已修正，均未混入成功性能结论。

**报告、图表与证据**

- [完整报告](../../search_scaling_unbounded_20261023/REPORT.md)
- [完整测量/验收/资源表](../../search_scaling_unbounded_20261023/RESULTS.md)
- [吞吐、p99及排队曲线](../../search_scaling_unbounded_20261023/scaling_curves.png)
- [同版本扩展效率](../../search_scaling_unbounded_20261023/scaling_efficiency.png)
- [逐节点计算成本](../../search_scaling_unbounded_20261023/per_node_scaling.png)
- [64线程跨节点p99时序](../../search_scaling_unbounded_20261023/cases/lookup_s64/p99_timeline.png)
- [64线程复测p99时序](../../search_scaling_unbounded_20261023/cases/lookup_s64_repeat/p99_timeline.png)
- [网络长尾负例](../../search_scaling_unbounded_20261023/network_tail_diagnosis.json)
- [推荐配置摘要](../../search_scaling_unbounded_20261023/recommended.yaml)
- [源码哈希核对](../../search_scaling_unbounded_20261023/final_source_audit.json)
- [实际加载二进制/归档核对](../../search_scaling_unbounded_20261023/capture_audit.json)
- [空闲资源回收](../../search_scaling_unbounded_20261023/idle_summary.json)
- [最终进程核验](../../search_scaling_unbounded_20261023/final_process_audit.json)
- [复现步骤](../../search_scaling_unbounded_20261023/USAGE.md)

**下一步建议**：当前吞吐候选为64线程，48线程单次p99更低；96线性目标尚未完成。增加reactor内部时间点，固定节点负载做NUMA局部/远端与量化码加载/算术剖析，再评估紧凑ID、邻居批量处理、NUMA局部缓存及热点分片负载均衡。进一步用更多独立查询、长期流量与冷SSD验证。

**日志更新时间**：2026-10-23T14:11:40+08:00
**数据保留状态（2026-10-27）**：本轮大体积原始数据已按用户要求清理，报告/图表/汇总和选定样本保留；具体文件见[清理清单](../../storage_cleanup_20261027/README.md)。部分离线重分析需重新采集数据。

<!-- experiment:search_scaling_unbounded_20261023:end -->
