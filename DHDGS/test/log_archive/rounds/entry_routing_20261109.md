<!-- experiment:entry_routing_20261109:start -->
## 固定图下最短队列与全路径成本入口调度对照（entry_routing_20261109）

**状态**：已完成

**开始时间**：2026-11-09T14:05:39+08:00

**结束时间**：2026-11-09T14:49:41+08:00

**目的 / 要解决的问题**：R001/F10验证朴素最短计算队列是否优于固定份额与较复杂成本策略

**假设**：已登记入口选择较短队列可提高有效吞吐；复杂成本/在途代理未必带来额外收益

**期望效果**：同图/D128/64线程，改善QPS/p99并测量召回损失及源卸载

**环境与配置 / 对照**：Intel35路由+独立客户端，31/32/30数据；S30/D80、indexed学习后封口同图、C1536、200K原热点流循环

**本轮改变的变量**：fixed、least_queue、cost；同布局固定封口图，D128/beam11/C1536/64线程；20ms统一原生负载采样/100msTTL；三轮交错，S30取有效b/c，首D80干扰排除，D80补测组间资源门禁后取有效a/c；参数/核心/代码不因策略改变

**代码改动**：entry_routing.hpp：最短队列/softmax成本与物理在途代理；native_head.cpp/.hpp：原生选择、生命周期结算、版本级成本反馈、异步采样；native_runtime.cpp：常数大小负载RPC；runtime_bindings.hpp/config.py/YAML：参数/运行时模式切换；load_balance.py：防旧adjust双写；原生选择器及实际三节点回归、冻结部署/资源审计/交错实验/报告脚本；DESIGN/LOCKS/Research/README同步

**登记时 Git HEAD**：20930700b3ea566b7fb850c4d3b4ccecba0eb87f

**结果与限制**：共3个运行、23.4M查询（正式16.2M）零失败、预算/分支账目错误和普通pull/sync越界；封口后零READ/增长，在途根归零、原图身份不变、测试进程退出。每布局两次有效匹配（每策略1.2M），S30固定/朴素/成本QPS22373/27083/26824，p99 143.16/97.37/79.03ms；D80 29926/31114/30401，132.41/73.02/74.77ms。朴素Recall94.6410%/94.5371%，严格95%未达。成本没有额外吞吐优势、S30尾部更低；D80小差异不当显著胜负。S30固定a、首D80不完整及补测fixed_b等受干扰均保留排除。82项本机回归及三机各35通过，独立原生选择/并发计数测试通过。共享主机、暖缓存、循环同200K、两有效重复及成本模型简化限制保留。初始端口和CMake缓存预检失败未产生正式查询；默认入口fixed/遥测关闭与indexed基线不变。

**报告、图表与证据**

- test/entry_routing_20261109/REPORT.md
- test/entry_routing_20261109/PLAN.md
- test/entry_routing_20261109/metrics.csv
- test/entry_routing_20261109/metrics.json
- test/entry_routing_20261109/paired_quality.json
- test/entry_routing_20261109/comparison.png
- test/entry_routing_20261109/load_comparison.png
- test/entry_routing_20261109/s30_load_timeline.png
- test/entry_routing_20261109/d80_load_timeline.png
- test/entry_routing_20261109/timeline_samples.json
- test/entry_routing_20261109/final_audit.json
- test/entry_routing_20261109/source_identity.json
- test/entry_routing_20261109/selected_cases.json
- test/entry_routing_20261109/sampled_evidence_manifest.json
- test/entry_routing_20261109/unit.log
- test/entry_routing_20261109/validation.json
- test/entry_routing_20261109/deploy_v2_build.log
- test/entry_routing_20261109/retry_experiment.json
- test/entry_routing_20261109/d80_cost_p99.png
- test/entry_routing_20261109/d80_fixed_p99.png
- test/entry_routing_20261109/d80_least_queue_p99.png
- test/entry_routing_20261109/s30_cost_p99.png
- test/entry_routing_20261109/s30_fixed_p99.png
- test/entry_routing_20261109/s30_least_queue_p99.png
- DESIGN.md#dynamic-entry-routing-20261109
- LOCKS.md
- Research.md

**下一步建议**：建议优先最短队列，S30严格p99场景保留成本选项；仅建议下一轮消融最短队列＋在途计数、自然流/固定到达率/持续增长/质量准入。未执行额外优化，不关闭R001/R004。

**日志更新时间**：2026-11-09T14:49:41+08:00
<!-- experiment:entry_routing_20261109:end -->
