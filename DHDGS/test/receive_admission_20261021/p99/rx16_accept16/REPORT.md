# p99真实查询时序分解

来源：confirm_r0_rx16_accept16，20000条查询。在跨节点查询中选择最接近本轮全体查询p99的记录；这不是跨节点子集的p99。

本轮p99为137.348159390ms；query_id=1612113、job=15591，延迟137.347368000ms，相差0.000791390ms，Recall@10=1.00。

![时序图](timeline.png)

顶层面板使用同一主机时钟，阶段互斥且总和等于端到端服务延迟。每个数据节点单独以收到完整报文为零点，不能跨主机直接对齐时间轴或相加。节点内计算、控制、派发存在重叠；虚线表示计算线程释放时刻，之后的控制/网络等待不占用该计算线程。结果ACK时刻与用户Future完成时刻也不同。

|顶层阶段|耗时ms|占服务延迟|
|---|---:|---:|
|API submission|0.027703|0.02%|
|Route queue|0.017852|0.01%|
|HNSW routing|0.484243|0.35%|
|Entry dispatch incl. queue|0.110482|0.08%|
|Data / RPC / control|133.840350|97.45%|
|Report validation / completion gating|0.617467|0.45%|
|Merge queue|0.759399|0.55%|
|Merge|0.651356|0.47%|
|API result handoff|0.838516|0.61%|

## Intel-32 / PARENT

|阶段|本节点相对起点ms|耗时ms|
|---|---:|---:|
|Receive queue / validation|0.000000|68.213603|
|Admission + registration|68.213603|1.560993|
|Compute queue + setup|69.774596|5.463581|
|Search loop|75.238177|20.086827|
|Report build / worker release|95.325004|0.205466|
|Completion command wait|95.432603|30.526215|
|Completion to report publication|125.958818|0.010825|
|Schedule + encode / submit|125.969643|7.642637|
|Result outbound queue / event loop|133.612280|0.017680|
|Result send incl. connect|133.629960|0.013252|
|Result response / ACK wait|133.643212|0.683685|
|Cross-node submit to ACK callback|92.098577|1.440584|

## Intel-30 / CHILD

|阶段|本节点相对起点ms|耗时ms|
|---|---:|---:|
|Receive queue / validation|0.000000|0.485470|
|Admission + registration|0.485470|0.041817|
|Compute queue + setup|0.527287|0.670183|
|Search loop|1.197470|4.808361|
|Report build / worker release|6.005831|0.313231|
|Completion command wait|6.124245|0.014469|
|Completion to report publication|6.138714|0.008228|
|Schedule + encode / submit|6.146942|0.141644|
|Result outbound queue / event loop|6.288586|0.160760|
|Result send incl. connect|6.449346|0.040523|
|Result response / ACK wait|6.489869|0.626554|

结果RPC排队起点由 sender_enter_ns - round(pool_wait_ms * 1e6) 还原；pool_wait_ms来自底层同一主机时钟的queued_ns与send_start_ns之差。旧message_ready_ns是在异步提交成功后记录，网络线程可能已开始发送，不能用它直接计算发送前队列。还原数值保存在stages.json/CSV，未改写原始时间戳。

接收队列/验证、完成控制和发送前等待均为观测区间，可能包含OS调度及状态锁等待，不能据图将其全部认定为锁、CPU计算或纯链路传输。单条p99附近查询用于定位现象，不代表所有尾延迟样本。selected.json保留完整源记录，stages.json/CSV保存精确阶段数值。
