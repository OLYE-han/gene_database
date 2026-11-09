<!-- experiment:demand_pipeline_20261109:start -->
## 无副本基线归因与增量缓存持久化流水线（demand_pipeline_20261109）

**状态**：已完成

**开始时间**：2026-11-09T10:02:42+08:00

**结束时间**：2026-11-09T12:01:28+08:00

**目的 / 要解决的问题**：解释自然/热点无副本QPS差异，降低增量提供器和持久化对读取的干扰

**假设**：工作负载与物理核数解释部分基线变化；共享目录和维护唤醒可能增加检索成本；独立有界写入流水可重叠准备与刷盘

**期望效果**：同预算正确性不变，量化锁/准备/刷盘/发布开销，并用同条件对照验证收益

**环境与配置 / 对照**：Intel35路由与客户端，31/32/30数据；预算128、64搜索线程、并发1536；普通pull/sync关闭

**本轮改变的变量**：自然/热点查询；S30/D80增长关闭、串行、目录优化、目录+流水；共享负载新臂head32核/data4核门槛，前期严格门槛被干扰组另列；核心/入口/预算/64线程固定

**代码改动**：cpp/demand_growth.cpp/.hpp：只追加原子目录、负缓存过滤、维护合并、不可变排名/锁外回收、锁与持久化计量、有界准备/顺序写者流水、beta排队预约与连接去重；config.py及YAML开关、35项回归和独立冻结四机脚本；DESIGN/LOCKS/Research文档更新，用户todo不改。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：20组正式测试共36M查询（预热/在线/冻结各12M）零失败及预算/容量/候选对账错误，本机35+3回归、三机冻结版本各34通过。自然N28524/热点N17275，旧热点14908低谷部分恢复但单位搜索CPU波动的NUMA/缓存/频率因果尚未隔离。S30 serial→indexed QPS17043→22460（+31.79%）、p99 194.74→144.58ms；D80 26805→29372（+9.57%）、147.59→137.60ms。封口零READ/写盘仍有目录收益。流水相对indexed吞吐−1.60%/−2.07%，无稳定额外收益。Recall94.4393%/94.5256%低于95%；off重复波动、共享环境、独立最终增长图、暖缓存及未冷SSD/百亿验证限制保留。目录成本子项局部通过，增长总体默认关闭。

**报告、图表与证据**

- test/demand_pipeline_20261109/REPORT.md
- test/demand_pipeline_20261109/metrics.csv
- test/demand_pipeline_20261109/paired_quality.json
- test/demand_pipeline_20261109/baseline_comparison.png
- test/demand_pipeline_20261109/warmup_comparison.png
- test/demand_pipeline_20261109/measure_comparison.png
- test/demand_pipeline_20261109/frozen_comparison.png
- test/demand_pipeline_20261109/mechanism_comparison.png
- test/demand_pipeline_20261109/shared_s30_indexed_a_measure_p99.png
- test/demand_pipeline_20261109/timeline_samples.json
- test/demand_pipeline_20261109/final_audit.json
- test/demand_pipeline_20261109/sampled_evidence_manifest.json
- test/demand_pipeline_20261109/implementation.patch
- test/demand_pipeline_20261109/final_validation_test.patch
- test/demand_pipeline_20261109/final_validation_identity.json
- test/demand_pipeline_20261109/final_unit.log
- test/demand_pipeline_20261109/final_rdma.log
- DESIGN.md#demand-persistence-pipeline-20261109
- LOCKS.md
- Research.md

**下一步建议**：建议固定同一增量图微剖析cycles/LLC/NUMA；按目标真实服务成本限制引流；在D128不变的前提定位副本命中质量与控制尾部。均为后续建议，本轮未执行。

**日志更新时间**：2026-11-09T12:01:28+08:00
<!-- experiment:demand_pipeline_20261109:end -->
