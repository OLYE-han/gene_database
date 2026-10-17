"""Summarize the archived sweep without changing its raw reports."""
from pathlib import Path
from collections import Counter
import csv
import json
import statistics
import hashlib
import numpy as np
import yaml

root=Path(__file__).resolve().parent
plan=yaml.safe_load((root/'sweep.yaml').read_text())
summary=[];nodes=[];repetitions=[];queries=[];links=[]
metric_keys=('router_submit_rpc_count','router_expand_rpc_count','peer_branch_rpc_count','control_rpc_count',
             'all_protocol_rpc_count','wire_bytes','control_wire_bytes','all_protocol_bytes','expanded',
             'unique_expanded','duplicate_expansions','branch_count','branch_unused_credit','pread_calls',
             'file_read_bytes','cache_hits','scanned_edges','rabitq_evaluations','exact_distance_evaluations',
             'pread_ms','rabitq_ms','exact_distance_ms','server_thread_cpu_ms','coordinator_thread_cpu_ms',
             'max_expanded_depth','max_cross_shard_depth','fallback_entries','frontier_dropped')
for w in plan['weights_descending']:
    paths=sorted((root/f'weight_{w:g}'/'runs').glob('*/report.json'))
    if len(paths)!=1:raise RuntimeError(f'Expected one completed report for {w}, found {len(paths)}')
    path=paths[0];r=json.loads(path.read_text());rows=r['queries']
    assert len(rows)==384 and r['successful_queries']==384 and r['failed_queries']==0
    assert all(s['stopped'] for s in r['local_cluster']['services'])
    reasons=Counter();remote_reasons=Counter();probabilities=[]
    for q in rows:
        s=q['stats'];assert s['metrics_complete']
        assert s['router_submit_rpc_count']==1 and s['router_expand_rpc_count']==0
        assert s['peer_branch_rpc_count']==s['branch_count']-1 and s['expanded']<=256
        assert s['all_protocol_bytes']==s['wire_bytes']+s['control_wire_bytes']
        assert s['control_rpc_count']==sum(v['rpc_count'] for v in s['control_by_kind'].values())
        assert s['expanded']+s['branch_unused_credit']==256
        assert s['unique_expanded']+s['duplicate_expansions']==s['expanded']
        assert abs(sum(s['latency_breakdown_ms'].values())-s['latency_ms'])<1e-6
        qr={'weight':w,'query_row':q['query_row'],'repetition':q['repetition'],
            'request_id':q['request_id'],'recall':q['recall'],'latency_ms':s['latency_ms'],
            **{key:s[key] for key in metric_keys}}
        for kind,v in s['control_by_kind'].items():
            qr[kind+'_rpc_count']=v['rpc_count'];qr[kind+'_bytes']=v['request_bytes']+v['response_bytes']
        qr.update(s['latency_breakdown_ms'])
        for key,val in s['latency_breakdown_fraction'].items():qr[key.replace('_ms','_fraction')]=val
        qreasons=Counter()
        for branch in q['branch_reports']:
            for d in branch['decisions']:
                reasons[d['reason']]+=1
                if d['reason']=='probability_draw':probabilities.append(d['probability'])
                if d['remote']:
                    remote_reasons[d['reason']]+=1;qreasons[d['reason']]+=1
        qr.update(probability_remote_count=qreasons['probability_draw'],max_wait_remote_count=qreasons['max_wait'],
                  local_exhausted_remote_count=qreasons['local_exhausted'])
        queries.append(qr)
    item={'weight':w,'queries':len(rows),'recall':r['recall_at_k'],'qps':r['qps'],
          'latency_mean_ms':r['latency_ms']['mean'],'latency_p50_ms':r['latency_ms']['p50'],
          'latency_p95_ms':r['latency_ms']['p95'],'latency_p99_ms':r['latency_ms']['p99'],
          'failed_queries':r['failed_queries'],'query_fraction_recall_ge_099':r['queries_meeting_recall_fraction'],
          'fallback_query_fraction':r['fallback_query_fraction'],
          **{'mean_'+key:statistics.mean(q['stats'][key] for q in rows) for key in metric_keys},
          'probability_decisions':reasons['probability_draw'],
          'probability_remote_branches':remote_reasons['probability_draw'],
          'mean_probability_at_eligible_decisions':statistics.mean(probabilities) if probabilities else None,
          'probability_realized_fraction':remote_reasons['probability_draw']/max(1,reasons['probability_draw']),
          'max_wait_remote_branches':remote_reasons['max_wait'],
          'forced_remote_fraction':sum(v for k,v in remote_reasons.items() if k in ('max_wait','local_exhausted'))/max(1,sum(remote_reasons.values())),
          'report_path':str(path.relative_to(root))}
    item['duplicate_expansion_fraction']=item['mean_duplicate_expansions']/item['mean_expanded']
    for rep in range(3):
        subset=[q for q in rows if q['repetition']==rep]
        repetitions.append({'weight':w,'repetition':rep,'queries':len(subset),
            'mean_latency_ms':statistics.mean(q['stats']['latency_ms'] for q in subset),
            'p99_latency_ms':float(np.percentile([q['stats']['latency_ms'] for q in subset],99)),
            'recall':statistics.mean(q['recall'] for q in subset),
            'mean_peer_rpc_count':statistics.mean(q['stats']['peer_branch_rpc_count'] for q in subset)})
    for key in rows[0]['stats']['latency_breakdown_ms']:
        item['mean_'+key]=statistics.mean(q['stats']['latency_breakdown_ms'][key] for q in rows)
    for kind in rows[0]['stats']['control_by_kind']:
        item['mean_'+kind+'_rpc_count']=statistics.mean(q['stats']['control_by_kind'][kind]['rpc_count'] for q in rows)
    for p in r['node_process_load']:
        lifecycle=p['peer_lifecycle_after']
        assert lifecycle['running']==lifecycle['queued']==lifecycle['unacknowledged']==lifecycle['failed_retained']==0
        nodes.append({'weight':w,'node_id':p['shard_id'],'mean_cpu_cores':p['mean_cpu_cores'],
                      'rss_mib':p['process']['rss_bytes']/2**20,
                      'expanded':p['counter_deltas']['exact_distance_evaluations'],
                      'storage_read_bytes':p['process_deltas']['storage_read_bytes'],
                      'retained_branches':lifecycle['retained_branches']})
    summary.append(item)
    links.append(f"- w={w:g}：[配置](weight_{w:g}/config.yaml)、[原始报告]({item['report_path']})、[逐查询 CSV]({path.parent.relative_to(root)}/queries.csv)。同目录包含 JSONL、服务日志、配置快照及节点资源数据。")

def save_csv(name,rows):
    keys=list(dict.fromkeys(k for row in rows for k in row))
    with (root/name).open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=keys);writer.writeheader();writer.writerows(rows)

save_csv('summary.csv',summary);save_csv('nodes.csv',nodes)
save_csv('repetitions.csv',repetitions);save_csv('all_queries.csv',queries)
(root/'summary.json').write_text(json.dumps(summary,indent=2))
lines=['# remote_weight 从 2 降至 0.02 的单机三节点实验','',
'2026-10-16，Asia/Shanghai。三个独立数据服务通过回环 TCP 执行异步分支。固定 4096 × 2560 索引、平方 L2、RaBitQ 1 bit、top-k=10、扩展预算 256、最多 16 分支、temperature=1、max_wait_ms=1.0、查询并发度 1。只改变 remote_weight；每组同样 128 条查询重复 3 轮，预热 4 条，详细 trace 开启。共 2688 次测量查询。','',
'## 召回、延迟与吞吐','',
'| weight | Recall@10 | 平均 ms | p50 ms | p95 ms | p99 ms | QPS |',
'|---|---|---|---|---|---|---|']
for s in summary:
    lines.append(f"| {s['weight']:g} | {s['recall']:.6f} | {s['latency_mean_ms']:.2f} | {s['latency_p50_ms']:.2f} | {s['latency_p95_ms']:.2f} | {s['latency_p99_ms']:.2f} | {s['qps']:.2f} |")
lines+=['','## 平均每查询通信与扩展','',
'| weight | 数据节点间 RPC | 控制/结果 RPC | 总协议 KiB | 重复扩展/256 | 最深路径均值 | 最深跨节点路径均值 |',
'|---|---|---|---|---|---|---|']
for s in summary:
    lines.append(f"| {s['weight']:g} | {s['mean_peer_branch_rpc_count']:.2f} | {s['mean_control_rpc_count']:.2f} | {s['mean_all_protocol_bytes']/1024:.2f} | {s['mean_duplicate_expansions']:.2f} | {s['mean_max_expanded_depth']:.2f} | {s['mean_max_cross_shard_depth']:.2f} |")
lines+=['','所有查询顶层入口提交为 1，顶层逐批扩展 RPC 为 0。控制/结果包括 heartbeat、lease、result、cancel，result 包含最终诊断数据；all_queries.csv 中已分别列出。总协议字节不含 TCP/IP 头和重传分组。','',
'## 概率机制是否实际生效','',
'| weight | 可抽样决策数 | 平均计算概率 | 抽样实际选远端比例 | 抽样触发远端数 | 超时触发远端数 | 强制触发占远端比例 |',
'|---|---|---|---|---|---|---|']
for s in summary:
    lines.append(f"| {s['weight']:g} | {s['probability_decisions']} | {s['mean_probability_at_eligible_decisions']:.4f} | {s['probability_realized_fraction']:.4f} | {s['probability_remote_branches']} | {s['max_wait_remote_branches']} | {100*s['forced_remote_fraction']:.2f}% |")
lines+=['','概率只在同时有本地和远端可执行工作且尚未超时的决策上作用。权重改变后搜索轨迹、效用及等待时间也改变，因此不是对同一批动作的固定概率重复抽样。超时或本地耗尽仍可强制发送，w 接近 0 不代表跨机通信接近 0。','',
'## 平均每查询存储与计算','',
'| weight | pread 次数 | 文件读取 KiB | 累计 pread ms | 累计精确距离 wall ms | 累计 RaBitQ wall ms | 分支计算线程 CPU ms |',
'|---|---|---|---|---|---|---|']
for s in summary:
    lines.append(f"| {s['weight']:g} | {s['mean_pread_calls']:.2f} | {s['mean_file_read_bytes']/1024:.2f} | {s['mean_pread_ms']:.2f} | {s['mean_exact_distance_ms']:.2f} | {s['mean_rabitq_ms']:.2f} | {s['mean_server_thread_cpu_ms']:.2f} |")
lines+=['','多分支累计 wall 时间存在重叠，不能当作端到端延迟占比。互斥的 routing / peer execution and control wait / head merge and lifecycle 时间及比例在 all_queries.csv；纯网络和物理 SSD 占比没有可靠测量。','',
'## 节点负载','', '| weight | 节点 | 平均 CPU 核数 | 结束 RSS MiB | 扩展数 | 物理读取计数字节 |', '|---|---|---|---|---|---|']
for p in nodes:lines.append(f"| {p['weight']:g} | {p['node_id']} | {p['mean_cpu_cores']:.3f} | {p['rss_mib']:.1f} | {p['expanded']} | {p['storage_read_bytes']} |")
lines+=['','## 测量边界与复现','',
'- 全部 2688 条查询成功，预算、通信分类求和与延迟分解均已自动校验；所有测试服务停止，结束时无运行/排队/未确认分支。',
'- 这是一组固定顺序、小规模、缓存环境的参数扫描，不是完整 2M 数据集或真实跨机 SSD 性能。真值只针对入索引的 4096 条数据。',
'- 三轮在同一组服务中顺序运行并共享缓存，不是三次独立重启实验；384 次请求仅来自 128 条不同查询。每轮结果见 repetitions.csv。',
'- 权重按从大到小执行，时间漂移与系统噪声未随机化消除。少量延迟差异不足以确立最优权重；本实验没有更改默认参数。',
'- 复用既有索引，不重复复制或重建；历史构建耗时约 38.067 s，不随此次搜索权重变化。',
'- 原始报告含 source_sha256；provenance.json 保存实验开始时源码与索引哈希。sweep.yaml 保存扫描计划。run_sweep.py 负责执行，analyze.py 负责汇总。',
'- 首次沙箱因禁用 socket 启动失败的日志单独保留，获准运行后完成七组实验；环境失败不混入查询指标。','',
'## 文件导航','',
'- [总体 CSV](summary.csv)、[总体 JSON](summary.json)、[全部逐查询指标](all_queries.csv)、[各重复轮次](repetitions.csv)、[各节点负载](nodes.csv)。',*links]
first,last=summary[0],summary[-1]
observations=['## 观察结论','',
    f"- 从 w=2 降至 0.02，平均跨节点分支 RPC 从 {first['mean_peer_branch_rpc_count']:.3f} 降到 {last['mean_peer_branch_rpc_count']:.3f}，仅减少 {100*(1-last['mean_peer_branch_rpc_count']/first['mean_peer_branch_rpc_count']):.2f}%；总协议字节仅减少 {100*(1-last['mean_all_protocol_bytes']/first['mean_all_protocol_bytes']):.2f}%。",
    f"- 可抽样决策的平均概率从 {100*first['mean_probability_at_eligible_decisions']:.2f}% 降到 {100*last['mean_probability_at_eligible_decisions']:.2f}%，表明概率参数在发挥作用。但强制发送占远端分支的比例从 {100*first['forced_remote_fraction']:.2f}% 升至 {100*last['forced_remote_fraction']:.2f}%，max_wait_ms=1.0 限制了它对总通信量的影响。",
    '- 召回保持在 99.92%–99.97%，平均延迟约 55 ms、QPS 约 18，没有随权重递减而单调改善。不能据此认定某个权重最优。',
    '- 各组每查询仍展开 256 次，其中约 47–48 次是跨分支重复扩展；降低权重没有显著解决重复工作。',
    '- 各组重复轮次延迟逐轮上升，与权重变化分开存在。以 w=2 为例，三轮约 50.92、54.43、59.73 ms。运行时状态积累、资源竞争与时间漂移需要单独 profiling，当前没有定位其根因。',
    '- 下一轮建议固定这一查询集和预算，联合扫描 max_wait_ms 与 remote_weight，并随机化/交错参数顺序；另设独立运行分析分支记录保留等状态对延迟的影响。本轮未修改默认设置。','',
    '## 各轮平均延迟（ms）','',
    '| weight | 第 1 轮 | 第 2 轮 | 第 3 轮 |', '|---|---|---|---|']
for w in plan['weights_descending']:
    values=[r['mean_latency_ms'] for r in repetitions if r['weight']==w]
    observations.append(f"| {w:g} | {values[0]:.2f} | {values[1]:.2f} | {values[2]:.2f} |")
position=lines.index('## 召回、延迟与吞吐')
lines[position:position]=observations+['']
(root/'SUMMARY.md').write_text('\n'.join(lines)+'\n')
print(json.dumps([{k:s[k] for k in ('weight','recall','latency_mean_ms','latency_p99_ms','qps','mean_peer_branch_rpc_count','mean_all_protocol_bytes','mean_duplicate_expansions','forced_remote_fraction','mean_probability_at_eligible_decisions')} for s in summary],indent=2))
