from pathlib import Path
import collections,csv,hashlib,json,statistics
import numpy as np
import yaml

root=Path(__file__).resolve().parent
plan=yaml.safe_load((root/'plan.yaml').read_text())
calibration=json.loads((root/'calibration_results.json').read_text())
selection=json.loads((root/'final_selection.json').read_text())['selection']
base=yaml.safe_load((root/'base.yaml').read_text())
provenance=json.loads((root/'provenance.json').read_text())
assert all(hashlib.sha256((root.parent.parent/p).read_bytes()).hexdigest()==h for p,h in provenance['source_sha256'].items())
keys=('expanded','unique_expanded','duplicate_expansions','branch_unused_credit','branch_count',
      'peer_branch_rpc_count','control_rpc_count','all_protocol_bytes','pread_calls','file_read_bytes',
      'cache_hits','pread_ms','rabitq_ms','exact_distance_ms','rabitq_evaluations','exact_distance_evaluations',
      'server_thread_cpu_ms','coordinator_thread_cpu_ms','max_expanded_depth','max_cross_shard_depth',
      'fallback_entries','frontier_dropped','visited_node_candidates_dropped','node_revisit_count')
validation_reports={};metrics=[];node_rows=[];per_query=[];cal_rows=[]

def check_report(r):
    assert r['failed_queries']==0
    assert all(s['stopped'] for s in r['local_cluster']['services'])
    for q in r['queries']:
        s=q['stats']
        assert s['node_revisit_count']==0 and s['router_expand_rpc_count']==0
        assert s['peer_branch_rpc_count']==s['branch_count']-1<=2
        assert len(s['visited_nodes'])==len(set(s['visited_nodes']))
        assert s['expanded']==s['unique_expanded']+s['duplicate_expansions']
        assert s['all_protocol_bytes']==s['wire_bytes']+s['control_wire_bytes']
    for node in r['node_process_load']:
        life=node['peer_lifecycle_after'];assert life['running']==life['queued']==life['unacknowledged']==0

for row in calibration:
    report=json.loads(Path(row['report']).read_text());check_report(report)
    cal_rows.append({**row,**{'mean_'+k:statistics.mean(q['stats'][k] for q in report['queries']) for k in keys}})

for selected in selection:
    if selected['status']!='selected':metrics.append(selected);continue
    name=selected['name']
    if name not in validation_reports:
        report=json.loads(Path(selected['validation_report']).read_text());check_report(report)
        assert report['unique_query_rows']==256 and report['query_count']==512
        validation_reports[name]=report
        for node in report['node_process_load']:
            node_rows.append({'configuration':name,'node_id':node['shard_id'],'mean_cpu_cores':node['mean_cpu_cores'],
                             'rss_mib':node['process']['rss_bytes']/2**20,'expanded':node['counter_deltas']['exact_distance_evaluations'],
                             'storage_read_bytes':node['process_deltas']['storage_read_bytes']})
        for q in report['queries']:
            s=q['stats'];per_query.append({'configuration':name,'query_row':q['query_row'],'repetition':q['repetition'],
                  'recall':q['recall'],'latency_ms':s['latency_ms'],**{k:s[k] for k in keys},
                  **s['latency_breakdown_ms'],
                  **{k.replace('_ms','_fraction'):v for k,v in s['latency_breakdown_fraction'].items()}})
    r=validation_reports[name];rows=r['queries']
    by_query=collections.defaultdict(list)
    for q in rows:by_query[q['query_row']].append(q['recall'])
    # Resample distinct query IDs; repeated requests are not independent query samples.
    recalls=np.array([statistics.mean(v) for v in by_query.values()])
    rng=np.random.default_rng(42)
    boot=recalls[rng.integers(0,len(recalls),size=(plan['bootstrap_draws'],len(recalls)))].mean(axis=1)
    low,high=map(float,np.quantile(boot,[.025,.975]))
    target=selected['target']
    item={'target_recall':target,'configuration':name,'max_expansions':selected['max_expansions'],
          'frontier_size':selected['frontier_size'],'remote_weight':selected['remote_weight'],
          'calibration_recall':selected['recall'],'validation_recall':r['recall_at_k'],
          'validation_recall_ci95_low':low,'validation_recall_ci95_high':high,
          'target_met_on_validation_mean':r['recall_at_k']>=target,
          'ci95_low_meets_target':low>=target,'query_fraction_reaching_target':float(np.mean(recalls>=target)),
          'mean_ms':r['latency_ms']['mean'],'p50_ms':r['latency_ms']['p50'],
          'p95_ms':r['latency_ms']['p95'],'p99_ms':r['latency_ms']['p99'],'qps':r['qps'],
          'failed_queries':r['failed_queries'],'incomplete_result_queries':r['incomplete_result_queries'],
          'metrics_complete_queries':sum(q['stats']['metrics_complete'] for q in rows),
          **{'mean_'+k:statistics.mean(q['stats'][k] for q in rows) for k in keys},
          'calibration_report':str(Path(selected['report']).relative_to(root)),
          'validation_report':str(Path(selected['validation_report']).relative_to(root))}
    metrics.append(item)
    out=root/'recommended';out.mkdir(exist_ok=True)
    c=yaml.safe_load((Path(selected['validation_report']).parent/'resolved.yaml').read_text())
    c['experiment']['target_recall']=target
    (out/f'recall_{target:.2f}.yaml').write_text(yaml.safe_dump(c,sort_keys=False))

def csv_file(name,rows):
    fields=list(dict.fromkeys(k for r in rows for k in r))
    with (root/name).open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)

csv_file('calibration_grid.csv',cal_rows);csv_file('target_summary.csv',metrics)
csv_file('validation_queries.csv',per_query);csv_file('validation_nodes.csv',node_rows)
(root/'target_summary.json').write_text(json.dumps(metrics,indent=2))
lines=['# 目标 Recall@10 0.70–0.99 的参数实验','',
'2026-10-16，Asia/Shanghai。默认 peer 搜索采用每节点最多启动一次、原节点派发后继续本地执行的访问链。','',
'## 实验定义','',
'- 目标是跨查询的平均 Recall@10，不是每条查询都达标。Top-k=10 时，单条查询 recall 以 0.1 为步长，0.99 的逐查询要求实际上意味着 1.0。',
'- 固定既有 4096 × 2560 索引、原始平方 L2、RaBitQ 1 bit、三个回环 TCP 数据进程、查询并发度 1。索引重复因子与建图参数保持不变。',
'- 扫描 11 个扩展预算 × 5 个候选队列/权重组合，共 55 组。预算为 10/12/16/24/32/48/64/96/128/192/256；组合为 (32,1)、(64,0.05)、(64,1)、(64,2)、(256,1)。并非全部超参数的全组合优化。',
'- 校准查询行为 [512,640)，128 条，各执行一次；独立验证查询行为 [1024,1280)，256 条，各执行两次。两组互不重叠，也不使用前几轮实验中的前 128 条查询。',
'- 校准执行顺序固定种子打乱；仅在校准召回达到目标+0.005 的组合中，选实测平均延迟最低者。在查看验证结果前冻结选择；不根据验证结果重新调参。',
'- 每个配置重启三个数据服务，预热 4 条查询；应用缓存 16 MiB/节点，OS 缓存未控制。详细批次 trace 关闭，基础指标和调度决策仍采集。',
'- 精确 top-k 针对该 4096 条索引计算一次并缓存，使用数据集身份、索引 ID 哈希、距离定义、k、query 字节哈希和排除 ID 校验复用。真值准备/加载不计入查询延迟，未改变检索算法源码。','',
'## 各目标选出的参数与独立验证','',
'| 目标 | max_expansions | frontier_size | remote_weight | 校准召回 | 验证召回 | 验证均值达标 | 平均 ms | p99 ms | QPS |',
'|---|---|---|---|---|---|---|---|---|---|']
for m in metrics:
    if 'validation_recall' not in m:
        lines.append(f"| {m['target']:.2f} | 无校准达标组合 | | | | | 否 | | | |");continue
    lines.append(f"| {m['target_recall']:.2f} | {m['max_expansions']} | {m['frontier_size']} | {m['remote_weight']:g} | {m['calibration_recall']:.5f} | {m['validation_recall']:.5f} | {'是' if m['target_met_on_validation_mean'] else '否'} | {m['mean_ms']:.2f} | {m['p99_ms']:.2f} | {m['qps']:.2f} |")
lines+=['',
'同一配置可能满足多个目标；它只做一次独立验证，表中共享该结果。离散参数使实际召回可能高于目标，不能用插值把它伪装成恰好达到目标。所选配置是此次有限网格中的校准结果，不是全局最优解。','',
'## 验证召回的不确定性','',
'| 目标 | 验证均值 | 按不同查询重采样的 95% 区间 | 区间下限达标 | 查询均值达到目标的比例 |',
'|---|---|---|---|---|']
for m in metrics:
    if 'validation_recall' not in m:continue
    lines.append(f"| {m['target_recall']:.2f} | {m['validation_recall']:.5f} | [{m['validation_recall_ci95_low']:.5f}, {m['validation_recall_ci95_high']:.5f}] | {'是' if m['ci95_low_meets_target'] else '否'} | {100*m['query_fraction_reaching_target']:.2f}% |")
lines+=['','区间使用 2000 次 query-level bootstrap；两次重复请求先在同一 query 内取均值，不能当成 512 条独立查询。它只描述这批查询的不确定性，不提供新数据分布、全量数据或每条查询的召回保证。','',
'## 平均每查询资源指标','',
'| 目标 | 实际扩展 | 重复扩展 | 跨节点 RPC | 全部协议 KiB | pread 次数 | 文件读取 KiB | 分支 CPU ms | 最大图深度均值 | 最大跨节点深度均值 |',
'|---|---|---|---|---|---|---|---|---|---|']
for m in metrics:
    if 'validation_recall' not in m:continue
    lines.append(f"| {m['target_recall']:.2f} | {m['mean_expanded']:.2f} | {m['mean_duplicate_expansions']:.2f} | {m['mean_peer_branch_rpc_count']:.3f} | {m['mean_all_protocol_bytes']/1024:.2f} | {m['mean_pread_calls']:.2f} | {m['mean_file_read_bytes']/1024:.2f} | {m['mean_server_thread_cpu_ms']:.2f} | {m['mean_max_expanded_depth']:.2f} | {m['mean_max_cross_shard_depth']:.2f} |")
lines+=['',
'协议字节包含最终结果与诊断数据，不含 TCP/IP 头；pread 包含 OS 页缓存命中，不等于物理 SSD 读取。各分支累计计算/pread 时间可能重叠，不能直接换算成可相加的端到端延迟占比。validation_queries.csv 保存互斥的顶层路由、搜索/控制等待、合并/清理分解及比例，也保存计算和读取累计时间。','',
'## 保持固定的参数','',
'| 参数 | 值 |','|---|---|',
'| data.metric / quantization.bits | l2 / 1 |',
'| search.top_k / local_beam | 10 / 4 |',
'| scheduler.max_wait_ms / probability_temperature | 1.0 / 1.0 |',
'| peer.child_budget_fraction / max_branches | 0.5 / 16（实际最多 3 个节点） |',
'| routing.ef_search / entry_candidates | 64 / 16 |',
'| search.fallback_enabled / max_fallback_entries | true / 2 |',
'| build.degree / candidate_list_size | 24 / 64 |',
'| partition.overlap_fraction / max_copies | 0.1 / 3 |',
'| experiment.benchmark_concurrency | 1 |',
'| metrics.detailed_trace | false |','',
'建图时间约 38.067 s 来自复用索引的历史记录，本轮没有重建索引。索引仅通过符号链接复用。与先前开启详细 trace 的性能数字不能作严格配对比较。','',
'## 文件与复现','',
'- [55 组校准数据](calibration_grid.csv)、[各目标汇总 CSV](target_summary.csv)、[汇总 JSON](target_summary.json)、[全部验证逐查询指标](validation_queries.csv)、[验证节点负载](validation_nodes.csv)。',
'- [扫描计划](plan.yaml)、[查看验证结果前冻结的选择](selection_before_validation.json)、[源码和索引哈希](provenance.json)。',
'- run_experiment.py 执行校准/选择/验证，worker.py 运行现有三节点 benchmark 并复用精确真值，analyze.py 汇总。原始报告、服务日志、逐查询 JSONL/CSV 与服务清理状态分别归档在 calibration/、validation/。',
'- recommended/ 中为每个目标导出的完整 YAML；experiment.target_recall 仅改变评价门槛，实际检索行为由搜索与调度参数决定。','']
for m in metrics:
    if 'validation_recall' in m:
        lines.append(f"- 目标 {m['target_recall']:.2f}：[配置](recommended/recall_{m['target_recall']:.2f}.yaml)、[独立验证报告]({m['validation_report']})。")
notes=['## 结果解读','',
'- 55 组校准共 7040 次请求，6 个不同配置的独立验证共 3072 次请求，总计 10112 次查询成功，未发现节点回访；测试服务已停止。首次沙箱因 socket 权限失败的服务启动记录单独保留，不计入查询指标。',
'- 本轮最主要的变化是扩展预算从 10 增加到 64；候选队列 32 或 64 已能支持所选目标。remote_weight 在有限网格中选到 1 或 2，不能把这个选择解读成普遍最优值或单调规律。',
'- 八个目标的独立验证平均值全部达标，但目标 0.70、0.80、0.99 的 bootstrap 95% 区间下限低于目标。对于 0.99，观测均值约 0.99238，区间约 [0.98633,0.99727]，尚不能据此宣称稳定达到 0.99。',
'- 当业务需要更高置信度时，应在新的校准/验证划分上扩大查询样本、增加独立重复，并评估更保守预算；不应反复查看同一验证集后调参。',
'- 当前索引只有 4096 条，数据常驻缓存，部分低预算查询只搜索入口节点。全量 2M / 200M 的参数阈值、SSD 和网络成本都需要重新实验。','']
position=lines.index('## 各目标选出的参数与独立验证')
lines[position:position]=notes
(root/'SUMMARY.md').write_text('\n'.join(lines)+'\n')
print(json.dumps([{k:m.get(k) for k in ('target_recall','max_expansions','frontier_size','remote_weight','validation_recall','validation_recall_ci95_low','target_met_on_validation_mean','mean_ms','p99_ms','qps','mean_peer_branch_rpc_count')} for m in metrics],indent=2))
