# p99真实查询时序分解

来源：final_r1_optimized，20000条查询。在跨节点查询中选择最接近本轮全体查询p99的记录；这不是跨节点子集的p99。

本轮p99为136.396150890ms；query_id=1334922、job=17315，延迟136.393297000ms，相差0.002853890ms，Recall@10=1.00。

![时序图](timeline.png)

顶层面板使用同一主机时钟，阶段互斥且总和等于端到端服务延迟。每个数据节点单独以收到完整报文为零点，不能跨主机直接对齐时间轴或相加。节点内计算、控制、派发存在重叠；虚线表示计算线程释放时刻，之后的控制/网络等待不占用该计算线程。结果ACK时刻与用户Future完成时刻也不同。

|顶层阶段|耗时ms|占服务延迟|
|---|---:|---:|
|API submission|0.034303|0.03%|
|Route queue|0.010507|0.01%|
|HNSW routing|1.038794|0.76%|
|Entry dispatch incl. queue|0.074699|0.05%|
|Data / RPC / control|134.407445|98.54%|
|Report validation / completion gating|0.549515|0.40%|
|Merge queue|0.010304|0.01%|
|Merge|0.172875|0.13%|
|API result handoff|0.094855|0.07%|

## Intel-32 / PARENT

|阶段|本节点相对起点ms|耗时ms|
|---|---:|---:|
|Receive queue / validation|0.000000|0.163185|
|Admission + registration|0.163185|0.060428|
|Compute queue + setup|0.223613|0.119093|
|Search loop|0.342706|15.278085|
|Report build / worker release|15.620791|0.192223|
|Registration command queue (overlaps search)|0.222265|44.333077|
|Completion command wait|15.739992|65.570072|
|Completion to report publication|81.310064|0.763331|
|Schedule + encode / submit|82.073395|52.221115|
|Result outbound queue / event loop|134.294510|0.023521|
|Result send incl. connect|134.318031|0.013995|
|Result response / ACK wait|134.332026|0.606027|
|Cross-node submit to ACK callback|3.602420|0.654220|

## Intel-35 / CHILD

|阶段|本节点相对起点ms|耗时ms|
|---|---:|---:|
|Receive queue / validation|0.000000|0.354065|
|Admission + registration|0.354065|0.031128|
|Compute queue + setup|0.385193|0.093552|
|Search loop|0.478745|5.172339|
|Report build / worker release|5.651084|0.232097|
|Registration command queue (overlaps search)|0.381293|0.007710|
|Completion command wait|5.732995|0.014523|
|Completion to report publication|5.747518|0.007145|
|Schedule + encode / submit|5.754663|0.134465|
|Result outbound queue / event loop|5.889128|0.007642|
|Result send incl. connect|5.896770|0.013332|
|Result response / ACK wait|5.910102|0.317098|

结果RPC排队起点由 sender_enter_ns - round(pool_wait_ms * 1e6) 还原；pool_wait_ms来自底层同一主机时钟的queued_ns与send_start_ns之差。旧message_ready_ns是在异步提交成功后记录，网络线程可能已开始发送，不能用它直接计算发送前队列。还原数值保存在stages.json/CSV，未改写原始时间戳。

接收队列/验证、完成控制和发送前等待均为观测区间，可能包含OS调度及状态锁等待，不能据图将其全部认定为锁、CPU计算或纯链路传输。单条p99附近查询用于定位现象，不代表所有尾延迟样本。selected.json保留完整源记录，stages.json/CSV保存精确阶段数值。
