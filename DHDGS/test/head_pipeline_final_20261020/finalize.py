"""Publish reviewable candidate config and qualify the controlled findings."""
from pathlib import Path
import csv,hashlib,json,shutil
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
selection=json.loads((R/'selection.json').read_text());mode=selection['mode']
plan=yaml.safe_load((R/'cases'/selection['case']/'plan.yaml').read_text())
plan={k:plan[k] for k in ('dhdgs','control')}
plan['control'].update(port=22491,results_directory=str(R/'service'))
plan['dhdgs']['peer']['head_port']=22480
(R/'candidate.yaml').write_text(yaml.safe_dump(plan,sort_keys=False))
controls=list(csv.DictReader((R/'control_transport.csv').open()))
assert all(float(r['control_error_delta'])==0 and int(r['failed_query_control_attempts'])==0 for r in controls)
metrics=json.loads((R/'metrics.json').read_text());summary=json.loads((R/'summary.json').read_text())
http=json.loads((R/'http/verification.json').read_text());assert http['receiver_subprocesses']==0
source=json.loads((R/'source_hashes.json').read_text());changed=[];current={}
for name,digest in source.items():
    path=P/name;actual=hashlib.sha256(path.read_bytes()).hexdigest();current[name]=actual
    if actual!=digest:changed.append(name)
    out=R/'validated_sources'/name;out.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,out)
(R/'validated_source_hashes.json').write_text(json.dumps(dict(files=current,changed_after_matrix=changed),indent=2))
assert set(changed)<= {'python/dhdgs/head_pipeline.py','python/dhdgs/metrics.py'},changed
report=(R/'REPORT.md').read_text()
intro='''
## 结论与保留的限制

**单进程方案已按要求实现，但本轮没有降低完整顶层延迟。** 按p99优先选出的单进程q1配置为1个路由线程、1个合并线程、4个派发线程、1至4个动态接收线程、1个结果接收线程、1个主动心跳线程。最佳单进程组的p99中位数368.89ms，旧双进程组286.14ms，增加28.9%；QPS为523.95与606.48，下降13.6%。i1立即非阻塞ACK的p99中位数415.67ms，没有证明优于q1。

保持已部署的旧双进程常驻服务不变；新方案保存在candidate.yaml，可按README独立启动。测试中的临时顶层均已关闭，数据节点持续运行。不能把局部合并队列改善称作整体提速，也不能宣称达到等待低于100ms。

同一最终矩阵中，旧版首次/合并就绪队列均值83.923/83.716ms；q1的接收、路由、派发、合并队列均值分别0.967/193.083/3.380/0.270ms。独立合并线程解决了合并与路由共用队列的问题，但更多等待集中到单个路由线程。增加到2/4路由线程的预备扫描没有更好p99。数据节点计算就绪队列均值由1.030变为0.645ms，计算线程CPU累计仍约11.4–11.6ms，并非数据线程等待其他分支未释放。

数据节点报告发送到ACK的均值范围由旧版约1.34–1.97ms变为q1约3.24–4.74ms；报告就绪到发送也增加（详见control_transport.csv）。它包含链路、接收解码/状态处理、ACK及两端调度，不能全部归因于网络或GIL。单进程把路由、合并、编解码和状态管理放到同一个Python运行时，删除IPC也失去了独立接收进程的运行时隔离；现有证据与调度/串行处理竞争一致，但尚未分离GIL和内核调度的各自贡献。

预备12组24000查询和ACK复测9组18000查询全部成功，但重复使用回调端口使数据节点复用了已关闭的旧连接，每轮各节点出现少量控制传输重试；保留在前两个目录。最终本页9组18000查询使用独立回调端口，27个节点窗口控制传输错误增量均为0，正式查询对应的控制事件失败次数均为0，排除了该干扰。原有metrics_complete不能单独证明原生发送从未重试，因此另检查原始控制事件；前两轮应用层协议字节不能当作完整物理网络流量。

最终298项回归全部通过。真实HTTP验证32查询、16客户端并发成功，另经HTTP管理接口启动32查询benchmark成功；实测接收线程峰值2、没有接收子进程。短HTTP测试仅验证功能，不用于宣称高负载性能；正式矩阵是同负载内部API对照。

性能矩阵冻结源码见snapshot/。矩阵之后仅调整max_active_queries=1时完成回调立即提交下一查询的容量释放顺序，以及增加CSV顶层排队列；这两项经过最终回归与真实HTTP验证，没有为其另行宣称性能增益。最终可运行源码快照及与矩阵的差异在validated_sources/、validated_source_hashes.json。

'''
heading,body=report.split('\n',1)
(R/'REPORT.md').write_text(heading+'\n'+intro+body)
for dirname in ('head_pipeline_20261019','head_pipeline_ack_20261020'):
    p=R.parent/dirname/'README.md'
    p.write_text('''# 预备单进程实验记录

本目录保留对应源码快照、原始查询及测试记录。重复使用相同回调端口时观察到少量原生控制旧连接重试，因此不以本目录单独判定架构性能。最终采用独立回调端口的交错对照和全部结论见 [最终报告](../head_pipeline_final_20261020/REPORT.md)。
''')
print('candidate',mode,'post-matrix changes',changed,'HTTP ingress',http['status']['head_pipeline'])
