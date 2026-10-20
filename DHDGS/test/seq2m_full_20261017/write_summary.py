"""Summarize completed runs; never replace raw results or their provenance."""
from pathlib import Path
import csv,hashlib,json,statistics
import numpy as np
root=Path(__file__).resolve().parent
project=root.parent.parent
final=root/'backlog128'
def read(p):return json.loads(p.read_text())
def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for block in iter(lambda:f.read(8*1024*1024),b''):h.update(block)
    return h.hexdigest()
verified=read(final/'verification.json');assert verified['passed'] and verified['all_recall_targets_met']
manifest=read(root/'build/index/manifest.json');calibration=read(root/'calibration/comparison.json')
rows=[]
for r in verified['results']:
    c=r['concurrency'];folder=final/r['mode']/f'c{c}'/'run';report=read(folder/'report.json');qs=report['queries']
    metrics={key:statistics.mean(q['stats'][key] for q in qs) for key in ['server_thread_cpu_ms','pread_ms','rabitq_ms','exact_distance_ms','file_read_bytes','all_protocol_bytes','all_protocol_rpc_count','server_to_server_branches','max_expanded_depth','fallback_entries']}
    depths=[min(p['depth'] for p in q['stats']['result_paths'] if p['id']==q['ids'][0]) for q in qs]
    metrics['mean_top1_min_discovery_depth']=statistics.mean(depths)
    metrics['remote_query_fraction']=statistics.mean(q['stats']['branch_count']>1 for q in qs)
    metrics['fallback_query_fraction']=report['fallback_query_fraction']
    metrics['perfect_recall_fraction']=statistics.mean(q['recall']==1 for q in qs)
    metrics['zero_recall_queries']=sum(q['recall']==0 for q in qs)
    metrics['stage_means']=list(csv.DictReader((folder/'query_stage_means.csv').open()))
    rows.append({**r,'query_metrics':metrics,'report':str(folder/'report.json')})
# Verify the original experiment's source hashes against its saved revision.
# Files changed for the backlog fix were copied before editing; all other
# original entries are checked against the unchanged live source.
original=read(root/'prepared.json');checked=0
for name,digest in original['source_sha256'].items():
    rel=Path(name).relative_to('DHDGS') if name.startswith('DHDGS/') else None
    archived=root/'baseline_sources'/rel if rel is not None else None
    p=archived if archived is not None and archived.exists() else project.parent/name
    assert sha(p)==digest,name
    checked+=1
assert original['index_sha256']==read(final/'prepared.json')['index_sha256']
baseline=read(root/'comparison.json')
(root/'baseline_verification.json').write_text(json.dumps({'source_files_verified':checked,'same_immutable_index_as_final':True,
    'tests':len(baseline),'requests':sum(r['queries'] for r in baseline),'incomplete_metrics':sum(r['audit']['incomplete_metrics'] for r in baseline),
    'scope':'Original source revision and index provenance verified; baseline concurrency 8 has incomplete control metrics and did not pass strict audit'},indent=2))
index_bytes=sum(p.stat().st_size for p in (root/'build/index').rglob('*') if p.is_file())
aggregate={'index_rows':manifest['rows'],'index_bytes':index_bytes,'build_seconds':manifest['build_seconds'],
    'build_peak_rss_bytes':manifest['build_profile']['process_after']['peak_rss_bytes'],'partition':manifest['partition'],
    'final_results':rows,'baseline':baseline,'calibration':calibration}
(root/'aggregate.json').write_text(json.dumps(aggregate,indent=2))
lines=['# seq_2M 全量构建与三台物理机器测试（2026-10-17）','',
'已对完整 **2,000,000 × 2,560** 的 float32 数据集中构建索引，并部署到 Intel-35、Intel-32、Intel-30。最终版本通过三档并发、共 **3,072 个计时请求** 的验证：所有查询成功、指标完整，均达到本轮平均 Recall@10≥0.99 的目标。另有六档预算校准的 768 个计时请求，以及修复前保留的 3,072 个计时请求。独立质量样本为 1,024 条，跨并发重复使用不增加独立样本数。','',
'顶层 HNSW 和客户端仍与 Intel-35 数据节点共机。构建在 Intel-35 集中执行，远端接收已建好的分片；没有原始数据预分发或网络重分片。','',
'## 构建与容量','',
f'- 总构建时间：**{manifest["build_seconds"]:.2f} 秒（{manifest["build_seconds"]/60:.2f} 分钟）**。',
f'- 三个分片加路由及元数据的本机完整构建产物：**{index_bytes/2**30:.2f} GiB**。本机保留完整构建产物；远端另有自己的活动分片，不能把这两类存储重复计入逻辑 RF。',
f'- 构建进程观测峰值 RSS：**{manifest["build_profile"]["process_after"]["peak_rss_bytes"]/2**30:.2f} GiB**；512 GiB 配置是构建内存预检上限，不是操作系统硬限额。',
f'- 实际 RF：**{manifest["partition"]["replication_factor"]:.7f}**，重合向量比例：{manifest["partition"]["overlap_fraction_observed"]:.7f}。',
'- 3 个分片同时构建，每个分片 16 个原生建图线程；原始 L2、1-bit RaBitQ、Vamana degree=32、candidate_list_size=96、两轮剪枝。并发建图顺序不保证逐字节确定性，实际索引哈希已保存。','',
'| 节点 | 主归属数 | 含副本记录数 | 内存编码条数（含远端邻居） | 最大合并度数 |','|---|---:|---:|---:|---:|']
for i,s in enumerate(manifest['shards']):lines.append(f'| Intel-{[35,32,30][i]} | {manifest["partition"]["primary_counts"][i]} | {s["rows"]} | {s["code_rows"]} | {s["max_degree"]} |')
lines+=['','每个磁盘节点记录为 12 KiB，包含原始向量与邻接边。全量 ID/归属、全局强连通性验证通过；三个节点对之间的全部重合顶点副本均核验向量与边集一致，每个分片另抽查 1,024 条原始向量与 1,024 条 RaBitQ 重编码。见 [index_validation.json](index_validation.json)。','',
'## 搜索预算校准','',
'从 200,000 条查询中用固定种子 20261017 无放回抽取 128 条校准查询和另外 1,024 条验证查询，保存于 [query_selection.json](query_selection.json)。校准使用完整索引、Intel-35 上三个 TCP 数据进程；以下性能不是物理三机测试结果。','',
'| 扩展预算 | frontier | 平均 Recall@10 | p99 ms | QPS |','|---:|---:|---:|---:|---:|']
for r in calibration:lines.append(f'| {r["candidate"]["max_expansions"]} | {r["candidate"]["frontier_size"]} | {r["recall"]:.6f} | {r["p99_ms"]:.2f} | {r["qps"]:.2f} |')
lines+=['','扫描到 0.995 停止，保留高召回对照；实际部署按用户目标 0.99 筛选成功且指标完整的点，再优先最低 p99，选中 **预算 1,024 / frontier 256**。这不是穷尽所有参数的最优性证明。','',
'## 最终三机结果','',
'每个数据节点 1 个计算工作线程，BLAS 线程数 1；同步 pread，应用缓存关闭，详细指标开启。三档并发使用相同独立验证集，每组 1,024 请求，预热、真值准备、服务/Engine 加载不计入测量窗口。','',
'| 客户端并发 | 平均延迟 ms | p99 ms | QPS | 平均 Recall@10 | 召回 bootstrap 95% 区间 |','|---:|---:|---:|---:|---:|---|']
for r in rows:
    lo,hi=r['recall_bootstrap_95_percent_interval'];lines.append(f'| {r["concurrency"]} | {r["latency_ms"]["mean"]:.2f} | {r["latency_ms"]["p99"]:.2f} | {r["qps"]:.2f} | {r["recall"]:.6f} | [{lo:.6f}, {hi:.6f}] |')
lines+=['','按本轮 p99 优先的要求，优先选择客户端并发 1。更高并发提高吞吐并增加排队及尾延迟。表中是一次有限长度、闭环窗口的实测；区间按独立查询身份重采样，均值达到目标不构成对未来全部查询的召回保证。',
f'并发 1 中，完整召回 top-10 的查询比例为 {rows[0]["query_metrics"]["perfect_recall_fraction"]:.4%}，Recall@10=0 的查询数为 {rows[0]["query_metrics"]["zero_recall_queries"]}；各条查询的具体召回见 JSONL/CSV。','',
'## 逐查询开销与节点负载','',
'以下为各组逐查询均值；协议字节是应用协议统计，不包含全部 TCP/IP 链路开销。所有协议 RPC 包含入口提交、控制与结果回传，不等于跨数据节点的搜索分支次数。','',
'| 并发 | 协议 KiB/query | 协议 RPC/query | 跨数据节点分支/query | 文件 MiB/query | 最大已扩展路径深度均值 | 返回 top-1 最短记录路径深度均值 |','|---:|---:|---:|---:|---:|---:|---:|']
for r in rows:
    q=r['query_metrics'];lines.append(f'| {r["concurrency"]} | {q["all_protocol_bytes"]/1024:.2f} | {q["all_protocol_rpc_count"]:.2f} | {q["server_to_server_branches"]:.4f} | {q["file_read_bytes"]/2**20:.2f} | {q["max_expanded_depth"]:.2f} | {q["mean_top1_min_discovery_depth"]:.2f} |')
lines+=['','路径深度是记录的发现路径边数，扩展预算是工作量；不能把 1,024 次扩展解释为 1,024 跳。返回 top-1 的路径深度也不代表查询可以在该时刻安全终止并保持相同召回。','',
'| 并发 | 累计数据线程 CPU ms/query | RaBitQ ms/query | 精确距离 ms/query | 文件 pread ms/query |','|---:|---:|---:|---:|---:|']
for r in rows:
    q=r['query_metrics'];lines.append(f'| {r["concurrency"]} | {q["server_thread_cpu_ms"]:.2f} | {q["rabitq_ms"]:.2f} | {q["exact_distance_ms"]:.2f} | {q["pread_ms"]:.2f} |')
lines+=['','上述为各节点累计工作时间，分支可以并行，子阶段还可能包含于更大的阶段，不能相加当作端到端占比。端到端路由、数据执行与控制等待、合并及清理的可加和分解见各组 `query_stage_means.csv`；纯网络等待及纯物理 SSD 等待占比尚无法由这些指标精确分离。','',
'| 并发 | 节点 | 平均使用 CPU 核数 | 测量结束 RSS GiB | 精确距离计算次数 |','|---:|---|---:|---:|---:|']
for r in rows:
    for n in r['nodes']:lines.append(f'| {r["concurrency"]} | Intel-{[35,32,30][n["shard_id"]]} | {n["mean_cpu_cores"]:.3f} | {n["process"]["rss_bytes"]/2**30:.3f} | {n["counter_deltas"]["exact_distance_evaluations"]} |')
lines+=['','各组测量窗口的进程物理存储读取量均为 **0**。本轮是热页缓存结果，不能证明 SSD 冷读性能或异步 I/O 收益。RSS 不包含所有操作系统文件页缓存。','',
'## 并发 8 暴露的问题与复测','',
'首轮三档并发的 3,072 个查询都返回成功，召回也达到目标，但并发 8 有 **14 条控制通信指标不完整**，严格审计因此失败。原记录、失败审计和源码校验值均保留，未把这 14 条记录改成通过。','',
'错误发生在租约续期控制通道；旧记录只有错误计数，无法事后恢复每次异常的具体类型。检查发现 Python TCP 监听队列默认仅为 5，低于并发连接突发需求。新增 YAML 参数 `network.listen_backlog`，最终设为 128，应用于数据服务和入口回调；增加控制错误的节点、操作和异常类型记录。未放宽控制超时，未修改图、量化或搜索预算。突发连接回归及完整回归共 **66 项通过**。','',
'复测目录为 [backlog128/](backlog128/)，复用同一个不可变索引，远端重新核验所有分片文件哈希后运行。最终三组均无查询失败、无已记录的控制通信错误、指标完整；预算守恒、无节点回访、每次远端派发均有扩展决定函数批准。修复前后的性能属于单轮对照，不能当作排除所有环境波动的因果量化。','',
'## 文件、复现与范围','',
'- [最终统一 YAML](backlog128/deployment.selected.yaml)：当前代码、部署与测试参数；`reuse_remote_index=true` 仅适用于已安装且哈希一致的分片，新机器首次部署应设为 false。',
'- [运行与重启指南](../../FULL_DATASET.md)。',
'- [最终独立核验](backlog128/verification.json)、[最终完成状态](backlog128/completed.json)、[汇总数据](aggregate.json)。',
'- [初始主 YAML](deployment.yaml)、[构建配置](build/build.yaml)、[构建 manifest](build/index/manifest.json)、[构建进度](build/progress.jsonl)。',
'- [随机查询真值核验](truth_validation.json)：8 条随机集合内查询对完整 200 万条数据做 float64 直接差分精确扫描，另保留前期 8 条连续区间查询的核验；其余供应真值行未全部重新扫描。',
'- [原始版本核验](baseline_verification.json)、[首轮审计失败记录](pipeline_failed.json)、[最新回归日志](regression_backlog.log)。','',
'最终每个 `backlog128/blocking_backlog128/c1|c4|c8/run/` 包含 `report.json`、`queries.jsonl`、`queries.csv`、`query_stage_means.csv`、`per_query_stage_means.csv`、`nodes.csv`、`process_load.csv`、`build_stages.csv` 及完整配置来源。JSONL 含逐查询决定、分支、路径和控制错误详情，CSV 便于筛选与统计。','',
'本轮不是对全部 20 万条查询的遍历压测，也不是完整 200M/百亿数据验证。Intel-35 同时承担入口与发压；本机 Python 3.12、远端 Python 3.10，硬件与依赖版本均记录。测试服务已经正常退出，三机代码、环境与分片保留，可按指南重新启动。']
(root/'SUMMARY.md').write_text('\n'.join(lines)+'\n')
(root/'completion.json').write_text(json.dumps({'complete':True,'build_rows':2000000,'final_requests':verified['requests'],
    'final_verification':str(final/'verification.json'),'initial_control_metric_issue_preserved':True,
    'all_final_recall_targets_met':True,'all_final_services_stopped':True},indent=2))
print('Wrote SUMMARY.md, aggregate.json, baseline_verification.json and completion.json')
