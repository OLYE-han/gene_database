# R001：热点副本的自适应衰退与多目标扩展

2026-10-29。研究实现已完成并通过58项定向正确性检查；长期真实多机变化热点的性能尚未验收。按用户要求先做完整区域/200K对照，再验证生命周期与多副本策略。[R001-A19最终报告](../test/replica_region_20261029/REPORT.md)已完成：原始200K流当前副本命中约1.7%，未见稳定QPS收益；固定到达率热点流源搜索CPU下降10.97%，但总搜索CPU增加8.11%。因此本方案坚持独立校验正源卸载与目标容量，不能用增加副本数代替有效区域选择。本文件不等于生产能力声明。所有公式使用纯文本。

## 1. 实验给出的约束

热点60K×3：当前区域中位9522 QPS，无副本8383；较窄3跳区域只承接约800/60K，尽管命中群体本地真近邻包含改善。不能用区域深度、条数或目标收到请求数代替净卸载。当前副本仍大量回源，命中群体质量高于用户95%目标，但相对无副本存在约0.5pp损失。原始200K主测、同到达率补测和资源异常复验均已结束；当前预建副本仅约1.7%根请求命中，在线规划也出现晚发布/过期，不能外推热点收益。

因此生命周期策略必须分清：逻辑热点的需求、某个物理副本的实际流量、源节点真正节省的计算、其他节点增加的代价。减少某个副本的路由份额不能被当作热点自然消失；加副本也不能把同一query主动广播到多个副本。

## 2. 衰退信号和状态

使用完成请求携带的原始source/entry/route-score，统计属于该区域入口条件的全部请求；不以实际replica命中数作需求。它是完成端需求估计，存在查询时延的滞后，不能称为瞬时入口到达率。内部校验请求不计。观察队列丢弃、采样间隙过大、源或目标负载过期时标unknown，暂停衰退计时；unknown不等于0。

对健康观察窗口dt、样本计数n与采样间隔s：

```text
lambda = n * s / dt
fast = exp(-ln(2)*dt/Hfast)*fast + (1-exp(-ln(2)*dt/Hfast))*lambda
slow = exp(-ln(2)*dt/Hslow)*slow + (1-exp(-ln(2)*dt/Hslow))*lambda
peak = max(fast, peak * exp(-ln(2)*dt/Hpeak))
cold_threshold = max(min_keep_qps, cold_ratio * peak)
hot_threshold  = max(reheat_qps, reheat_ratio * peak)
```

Hfast < Hslow < Hpeak，reheat_ratio > cold_ratio，reheat_qps > min_keep_qps。max(fast,slow)低于cold_threshold才进入COOLING；fast高于hot_threshold取消冷却。介于两阈值的窗口不能无限累计“确定冷却”时间，本轮实现重置连续冷却证据。使用实际dt，不把每次轮询当成相等时间。

HOT → COOLING → WITHDRAWN → DRAINING → RECLAIMED。短暂下降可被slow缓冲，反弹取消COOLING；最短驻留时间必须满足。不能把CPU高低直接当作该热点的需求。

## 3. 自适应保留期限

每个热点族只保留有界的小型历史：入口签名、上一次冷却时长、平滑反弹间隔/次数、最近实际构建就绪耗时；删除大图后不保留SSD文件、向量/量化数组或图成员列表。相同source、epoch与入口签名用于识别回暖；入口变化时需记录无法准确匹配的限制，不能伪称识别所有语义相似热点。

```text
base_hold = base_ms + build_cost_weight * ready_ms
                     + rebound_weight * gap_ewma_ms * rebound_confidence
hold = clip(base_hold * (1 - pressure_discount * pressure), hold_min_ms, hold_max_ms)
```

ready_ms是实际选图到就绪的关键路径成本，不能相加重复计算流水并发阶段。pressure来自可用副本容量与目标磁盘余量，映射0..1；资源紧张缩短保留，但不跳过最短驻留和在途保护。反弹置信随时间下降，不让一次反弹永久锁住长保留；多次短期回暖增加保留，长期稳定冷却降低保留。所有阈值/半衰期/历史上限写入统一YAML。

## 4. 安全释放与控制面解耦

旧retire同步阻塞观察器，会使其他目录TTL失效；本轮已改为有界后台回收。首先撤销新路由，再对目标调用retire，已有pin继续执行，reap确认所有引用释放后才删文件/释放容量。迟到入口请求使用现有明确non-admission回退，丢失ACK不能被当作未接纳。

DRAINING仍计入物理副本数和字节预算。超时/网络失败保留待回收记录并重试，不能记作已释放。清理source spool、target原生数组/FD/文件以及大体积控制状态；允许保留有界标量墓碑和指标。控制器关闭必须等待或明确报告未回收项。

## 5. 多目标扩展

区分内容身份（source、epoch、完整成员摘要）、热点族、每个物理副本版本。相同入口可映射一个副本集合；同一请求只抽取一个READY目标，剩余概率仍回原源。所有物理副本份额之和<=统一max_fraction，不能让两个0.8叠加为1.6。过期目标的份额返回源，不能偷偷扩大其他目标的流量。第一版仅根入口，多副本peer重定向独立禁止，避免破坏已访问区域与预算协议。

优先在现有副本间调整份额。需要新增目标时必须同时满足：源持续高压；现有副本的可用服务能力不足；需求和负载新鲜；有容量；无同族在建副本；有冷却时间；有正净源卸载证据。单纯“源CPU高”不足以证明复制有效。

设lambda为区域需求，g为每个分流请求节省的源CPU秒，c_j为目标j新增CPU秒/请求，W为搜索线程数，u为观察CPU占用/线程数：

```text
required_extra_fraction = max(0, (u_source - source_goal) * W_source) / (lambda * g)
capacity_j_requests = max(0, (target_goal - u_j) * W_j + lambda*f_j*c_j) / c_j
```

g<=0、成本不足样本或过期时不自动扩容；ready发布没有shadow成本，不能把None当0，也不能用预期值冒充实测。成本需按同入口条件的无副本/副本观察匹配，或沿用明确的校验统计并限制有效期。规划新目标前扣除在建/在排空资源及已承诺的路由增量。目的节点不等于源、已有目标或同族在建目标。

新增副本从小份额开始，按可用容量分配，逐步爬坡；目标排队/利用率越界立即回退份额。持续低负载时先撤销冗余副本份额，确认剩余副本有余量后退休最不需要的一份；热点整体消退由第2–4节释放全部副本。创建与删除使用不同持续窗口和冷却时间，避免相互触发振荡。具体实现应优先保持一条有界构建流水线，而非为多目标同时无限复制。

## 6. 验证清单

1. 健康恒热、短暂降温后反弹、长冷却、周期性热点、一次性热点；静态idle超时作对照，统计错误删除、重复制字节/CPU、冷驻留GiB·s、回暖恢复时间。
2. 丢样、负载RPC失败、长停顿：不把缺失观察当冷却；恢复后重新累积证据。
3. 退休时仍有查询持有pin：新流量撤销、旧查询完成、文件保留到最后引用释放；回收失败仍占预算。
4. 多副本路由分布、份额守恒、目标过期、查询预算/单执行器、源合法回退；不能复制查询扇出。
5. 一副本不足的热点阶跃：1→2份、冷却2→1→0、不同目标速率与失败；无正卸载证据时拒绝扩容。
6. 最后再在真实三节点与长时变化流上比较QPS/源CPU/维护代价。合成时间加速的正确性试验不能替代生产时间尺度与吞吐验收。


## 7. 已实现版本、默认参数和操作边界

实现位置：[策略](../python/dhdgs/replica_policy.py)、[控制器](../python/dhdgs/load_balance.py)、[根路由](../cpp/native_head.cpp)、[物理生命周期](../python/dhdgs/replicas.py)、[流水回收](../python/dhdgs/replica_pipeline.py)。新增独立回收池，默认1线程；查询搜索线程不执行退休RPC。原生路由读取不可变目录，不在查询热路径运行本节EWMA和容量计算。

统一参数位于[主YAML](../configs/dhdgs.yaml)的`load_balance.adaptive_decay`与`adaptive_expansion`，旧配置可继续加载。新开关均为false；启用前需要先开启原有load_balance、使用dynamic模式并设置实际副本数/字节预算。三台数据服务器，一个源最多两个其他物理目标，`max_replicas: 2`仅适合测试单热点族双份；多个热点族要另计总容量。以下为参数含义，不是宣称实测最优值：

|模块/参数|默认|作用|
|---|---:|---|
|decay fast/slow/peak_half_life_ms|2000/10000/300000|短变、平滑需求、历史峰值|
|max_sample_gap_ms|2000|长观察空洞冻结健康时钟|
|min_keep_qps/reheat_qps|20/40|低热和回暖的绝对门槛|
|cold_ratio/reheat_ratio|0.15/0.35|相对历史峰值的冷热滞回|
|base_hold_ms、hold_min/max_ms|30000、10000/300000|基础保留与范围|
|decay.min_residency_ms|60000|整体热点衰退的最短健康驻留|
|build_cost_weight/rebound_weight|4/2|昂贵复制或频繁回暖延长保留|
|rebound_window_ms|60000|仅较短缺失记作误删风险；很久以后再热不惩罚之前删除|
|pressure_discount|0.5|资源压力最多将原始保留缩短一半，仍守上下限|
|history_limit/history_ttl_ms|128/3600000|已退热点有界入口历史；不含成员/向量数组|
|reclaim_workers/reclaim_retry_ms|1/1000|后台安全排空及失败重试|
|expansion.max_copies_per_family|3|同族物理份数上限；还受实际节点、全局数量/字节预算约束|
|source_goal/target_goal|0.75/0.70|按搜索CPU/线程归一化的压力目标|
|target_queue_ms|2|目标排队余量门禁|
|sustained_windows/scale_down_windows|5/10|扩/缩持续证据次数|
|cooldown_ms|30000|同族扩缩之间的冷却时间|
|fraction_step/min_fraction|0.05/0.02|单次增量上限和有效最小增量|
|min_saved_s/min_validation_queries|0.0001/32|最小正源CPU收益与成本样本量|
|require_qualified/cost_max_age_ms|true/60000|质量校验及成本有效期|
|new_target_cost_margin|2|新机器尚未校验前，按旧成本保守加价|
|decision_horizon_ms/amortization_factor|60000/2|限制就绪时长占决策时域的比例；不是净CPU回本证明|
|scale_down_margin|0.25|剩余副本吸收被删份额要保留25%额外余量|

原有`load_balance.min_residency_ms`用于额外副本缩容，decay子节的同名项用于全族退出，两者分别控制，默认同为60s。`max_fraction`控制整个热点族份额，不是每份各占这么多。`fraction_step`原控制器与子节也分别保留，开启扩展后由子节负责同族增长，原控制器继续负责目标过载时回退。

实际实现按“新增份额”计算余量，等价于第5节总容量公式扣除已有份额：

```text
need = max(0, (u_source - source_goal)*W_source) / (lambda*g)
room_existing = max(0, (target_goal-u_target)*W_target) / (lambda*c)
room_new = max(0, (target_goal-u_new)*W_new) / (lambda*c*new_target_cost_margin)
delta = min(need, room, max_fraction-sum(fractions), fraction_step)
```

使用当前族的保守最小g与最大c。任何现有目标有足够room就先增加其份额，不复制。新增目标必须连续选中同一目的节点；构建串行，排空中的物理目标和容量仍被占用。新副本需要自己的校验，不能把兄弟副本的估计直接写成验证结果。缩容检查存活目标新增room是否至少覆盖待删份额乘(1+scale_down_margin)，满足持续窗口与最短驻留后才转移份额并退休。

`ready_seconds`包含选图端到端等待和构建/校验到发布的关键路径；多份克隆复用已知成员，不再次计入首次选图成本。各流水阶段相互重叠，不作机械相加。历史图的选图端到端值不可用时仅使用已有planner记录，明确为近似。

## 8. 验证结果与未关闭问题

[完整报告与状态图](../test/replica_lifecycle_20261029/REPORT.md)、[58项测试日志](../test/replica_lifecycle_20261029/focused_attempt8.log)、[模拟输入](../test/replica_lifecycle_20261029/simulation.yaml)、[TestLog](../TestLog.md)。本地三节点实测多目标路由、过期区间回源、实际1→2→1→0复制/排空、搜索在途pin阻止提前删文件。压力与成本由测试注入，不能冒充源负载实测收益。

明确的5秒复制成本模型下，自适应重建1次、固定idle重建2次；冷容量积分146.79 vs158.38 GiB·s。资源压力下自适应109.50；60s遥测缺失则增至207.19。未知观测时更保守必然可能增加冷占用，不能宣称所有情形两者同时优化。

下一步首先提供独立校验流并开展真实变化热点长测。当前最佳快速发布路径`ready`没有校验成本，因此会被自动扩容门禁阻止；本轮不捏造该成本。已有成本60s后过期会停止自动增长，周期重校验仍待实现。多副本仅根入口，要求`peer.region_tasks: true`且`load_balance.peer_routes_enabled: false`。剩余热点寿命目前是配置时域，语义热点漂移、异构目标成本和维护流量开销需要实机校准。R001主问题继续开放。


### 8.1 扩容经济性的下一步补充（设计，未实现门禁）

本轮自动扩容已有正源卸载、容量和时域门禁，但`copy_ready_seconds × amortization_factor ≤ decision_horizon`只是保守时长限制，尚未完整计入复制维护CPU/网络的回本。A19显示全查询检索可能比选图更短，因此下一轮应连续采集规划开始至退休完成的维护成本，再加入：

```text
predicted_source_cpu_saved = lambda * delta_fraction * g * max(0, horizon - ready_seconds)
require predicted_source_cpu_saved > measured_source_build_cpu_seconds * safety_margin
```

这是源CPU维度的门禁；目标新增CPU、复制网络字节、SSD写与冷占用应分别列账，再按部署资源价格加权，不能把单位不同的量直接相减。当前尚无完整尾部维护CPU证据，不给它填写虚构的实测值，也不声称已实现该比较。源整体高压不代表每个很小的热点都值得复制；这项门禁用于排除覆盖很少、收益为正但无法抵消维护代价的副本。
