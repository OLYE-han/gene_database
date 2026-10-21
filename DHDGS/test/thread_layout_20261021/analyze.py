"""Summarize the measured thread search, then draw final p99 samples."""
import collections
import gzip
import json
import statistics
import subprocess
from pathlib import Path
import yaml
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
rows = json.loads((ROOT/'progress.json').read_text())
settings = yaml.safe_load((ROOT/'sweep.yaml').read_text())
assert (ROOT/'completed.json').exists()
groups = collections.defaultdict(list)
for row in rows:
    if row['stage'] == 'confirm':
        groups[json.dumps(row['layout'], sort_keys=True)].append(row)
confirmed = []
for key, values in groups.items():
    assert len(values) == 3
    confirmed.append(dict(layout=json.loads(key), cases=[v['name'] for v in values],
                          median_qps=statistics.median(v['qps'] for v in values),
                          min_qps=min(v['qps'] for v in values), max_qps=max(v['qps'] for v in values),
                          median_per_run_p99=statistics.median(v['p99'] for v in values),
                          aggregate_qps=sum(v['queries'] for v in values)/sum(v['wall_seconds'] for v in values)))
confirmed.sort(key=lambda value:value['median_qps'], reverse=True)
winner = confirmed[0]
baseline = next(g for g in confirmed if g['layout'] == settings['baseline'])
gain = winner['median_qps']/baseline['median_qps']-1
layout = winner['layout']
ratios = dict(
    data_search_request_handlers_rx_transfer_tx_report_tx_controller=[
        layout['search'], layout['data_rx']*layout['data_handlers_per_rx'],
        layout['data_rx'], layout['data_tx'], layout['data_tx'], 1],
    head_route_merge_report_handlers_rx_entry_tx_lease_tx=[
        layout['route'], layout['merge'], layout['head_rx']*layout['head_handlers_per_rx'],
        layout['head_rx'], layout['head_tx'], layout['head_tx']])
out = dict(winner=winner, confirmed=confirmed, baseline=baseline,
           qps_gain=gain, ratios=ratios, cases=len(rows), queries=sum(r['queries'] for r in rows))
(ROOT/'comparison.json').write_text(json.dumps(out, indent=2))
(ROOT/'best_layout.yaml').write_text(yaml.safe_dump(dict(
    scope='Measured best median QPS at concurrency256 / budget200 / full seq2M / warm cache',
    layout=layout, ratios=ratios, median_qps=winner['median_qps'],
    median_per_run_p99_ms=winner['median_per_run_p99'],
    baseline_median_qps=baseline['median_qps'], qps_gain=gain), sort_keys=False))
representative = min((r for r in rows if r['name'] in winner['cases']),
                     key=lambda r:abs(r['qps']-winner['median_qps']))
winner_case = ROOT/'cases'/representative['name']
data_plan = yaml.safe_load((winner_case/'plan.yaml').read_text())
head_plan = dict(data_plan)
head_plan['dhdgs'] = yaml.safe_load((winner_case/'head_effective.yaml').read_text())
for filename, plan in [('best_data_plan.yaml', data_plan), ('best_head_plan.yaml', head_plan),
                       ('best_layout_flat.yaml', layout)]:
    (ROOT/filename).write_text(yaml.safe_dump(plan, sort_keys=False))
for suffix, extra in [('overall', []), ('cross_node', ['--cross-node'])]:
    subprocess.run([str(PROJECT/'.venv/bin/python'), str(ROOT/'plot_p99.py'),
                    '--case', str(ROOT/'cases'/representative['name']),
                    '--output', str(ROOT/'p99'/suffix), *extra],
                   stdout=subprocess.DEVNULL, check=True)

fig, axes = plt.subplots(2, 1, figsize=(16, 8), layout='constrained')
for ax, metric, ylabel in zip(axes, ['qps', 'p99'], ['QPS', 'Per-run p99 / ms']):
    for stage, color in [('screen','#298ca4'), ('baseline','#8c9ca8'),
                         ('refine','#6c9d45'), ('confirm','#b76734')]:
        selected = [(i,r) for i,r in enumerate(rows) if r['stage']==stage]
        ax.scatter([i for i,_ in selected], [r[metric] for _,r in selected],
                   c=color, s=30, label=stage)
    ax.set_xticks(range(len(rows)), [r['name'] for r in rows], rotation=85, fontsize=6)
    ax.set_ylabel(ylabel); ax.grid(alpha=.2); ax.legend(ncol=4, fontsize=8)
fig.suptitle('Native thread layout search | concurrency256 | budget200 | fixed total connection capacity')
fig.savefig(ROOT/'thread_sweep.png', dpi=160); plt.close(fig)

fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout='constrained')
for ax, metric, title in zip(axes, ['qps','p99'], ['QPS','Per-run p99 / ms']):
    for i, group in enumerate(confirmed):
        vals = [r[metric] for r in rows if r['name'] in group['cases']]
        ax.scatter([i]*len(vals), vals, s=40)
        ax.plot([i-.2,i+.2], [statistics.median(vals)]*2, color='black')
    ax.set_xticks(range(len(confirmed)), ['winner' if i==0 else 'baseline' if g['layout']==settings['baseline'] else 'candidate' for i,g in enumerate(confirmed)])
    ax.set_ylabel(title); ax.grid(alpha=.2)
fig.savefig(ROOT/'confirmation.png', dpi=180); plt.close(fig)

lines = ['# 三机线程布局与QPS实验', '',
         f"共{len(rows)}轮、{sum(r['queries'] for r in rows)}次正式查询。GitHub源码提交4093380；所有轮次应用源码与二进制不变，仅调整配置。",
         '', '## 清理及实验条件', '',
         '40个早于2026-10-20的本地旧测试目录已清理，释放约12.84GiB；全量索引与在用配置迁至artifacts。三个旧路径仅保留运行兼容链接。原始删除清单、保留文件及空间变化见cleanup_completed.json。', '',
         'Intel-35同时运行顶层与数据节点0，Intel-32为数据节点1，Intel-30为数据节点2。全量seq_2M索引、2560维、top10、预算200、并发256，200次预热；相同2000条验证查询重复。服务延迟从实际进入API至返回计时，不包含客户端等待并发额度的backlog；offered_to_complete_ms另存完整批次排队时间。筛选每轮10000请求，最终每布局3轮50000请求并交替顺序。', '',
         '搜索为C++原生固定线程，sync_pread；父报告独立提前发布。每个逻辑发送客户端到每目标的总连接上限128固定，随TX线程数分摊。接收handler排队总容量4096固定；所有reactor固定min=max。网络模块与动态控制单元互斥，控制单元本轮保持1；旧Python线程参数和sync_pread下不工作的IO池参数不作为有效旋钮。', '',
         '结果先暂存在内存，测量结束后独立离线处理；初期短测暂存上限4GiB，后续及全部确认轮次32GiB。所有纳入轮次均无暂存溢出。', '', '## 三轮确认结果', '',
         '|布局|QPS中位数|QPS范围|各轮p99中位数 ms|合并请求/合并运行时长 QPS|',
         '|---|---:|---:|---:|---:|']
for i,g in enumerate(confirmed):
    label = '最高吞吐' if i==0 else '基线' if g['layout']==settings['baseline'] else '候选'
    lines.append(f"|{label}|{g['median_qps']:.2f}|{g['min_qps']:.2f}–{g['max_qps']:.2f}|{g['median_per_run_p99']:.2f}|{g['aggregate_qps']:.2f}|")
lines += ['', f"实测最高吞吐组合相对基线中位QPS变化{gain*100:+.2f}%。各轮p99的中位数不是汇总所有请求后的p99。", '',
          '![确认复测](confirmation.png)', '', '## 实测最高吞吐线程布局', '',
          '|角色|配置|数量|', '|---|---|---:|']
for title,key in [('每数据节点搜索线程','search'),('数据节点发送reactor/每逻辑客户端','data_tx'),
                  ('数据节点接收reactor','data_rx'),('每接收reactor数据handler','data_handlers_per_rx'),
                  ('顶层路由线程','route'),('顶层合并线程','merge'),
                  ('顶层发送reactor/每逻辑客户端','head_tx'),('顶层接收reactor','head_rx'),
                  ('顶层每接收reactor报告handler','head_handlers_per_rx')]:
    lines.append(f'|{title}|{key}|{layout[key]}|')
lines += ['', '线程比例按明确角色记录在best_layout.yaml。数据节点“搜索:请求handler:接收reactor:派发TX:报告TX:完成控制”为`'+':'.join(map(str,ratios['data_search_request_handlers_rx_transfer_tx_report_tx_controller']))+'`。顶层“路由:合并:报告handler:接收reactor:入口TX:租约TX”为`'+':'.join(map(str,ratios['head_route_merge_report_handlers_rx_entry_tx_lease_tx']))+'`。', '',
          '每个逻辑客户端独立创建TX线程，因此data_tx/head_tx不是该进程全部发送线程数。数据接收reactor还各有一个独立管理handler；网络监测、状态维护、Python API/采样等线程另计，下面实际进程线程数用于核对。', '',
          '## 所有轮次', '',
          '|轮次|搜索|数据TX/RX/handler每RX|顶层路由/合并|顶层TX/RX/handler每RX|QPS|p99 ms|Recall@10|网络指标完整|',
          '|---|---:|---|---|---|---:|---:|---:|---|']
for row in rows:
    l=row['layout']
    lines.append(f"|{row['name']}|{l['search']}|{l['data_tx']}/{l['data_rx']}/{l['data_handlers_per_rx']}|{l['route']}/{l['merge']}|{l['head_tx']}/{l['head_rx']}/{l['head_handlers_per_rx']}|{row['qps']:.2f}|{row['p99']:.2f}|{row['recall']:.5f}|{row['metrics_complete']}|")
lines += ['', '![全部线程扫描](thread_sweep.png)', '', '## 最终布局负载及协议指标', '',
          '|轮次|数据进程线程数0/1/2|顶层进程线程数|数据平均CPU核0/1/2|顶层平均CPU核|每query协议字节|RPC次数|pread ms|计算就绪累计 ms|',
          '|---|---|---:|---|---:|---:|---:|---:|---:|']
for row in rows:
    if row['stage']!='confirm':continue
    d=json.loads((ROOT/'cases'/row['name']/'result.json').read_text())
    m=d['query_metric_distributions'];nodes=d['node_process_load'];head=d['head_process_load'][0]
    lines.append(f"|{row['name']}|"+'/'.join(str(n['process']['threads']) for n in nodes)+f"|{head['process']['threads']}|"+'/'.join(f"{n['mean_cpu_cores']:.2f}" for n in nodes)+f"|{head['mean_cpu_cores']:.2f}|{m['all_protocol_bytes']['mean']:.0f}|{m['all_protocol_rpc_count']['mean']:.2f}|{m['pread_ms']['mean']:.3f}|{m['query_ready_queue_ms']['mean']:.3f}|")
lines += ['', '|轮次|精确距离 ms/query|RaBitQ ms/query|搜索线程CPU ms/query|展开顶点数|最大图深度|跨分片深度|分支RPC/query|文件请求读取字节/query|三节点物理读取字节/快照窗口|',
          '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
for row in rows:
    if row['stage'] != 'confirm': continue
    d=json.loads((ROOT/'cases'/row['name']/'result.json').read_text())
    m=d['query_metric_distributions']
    values=[m[k]['mean'] for k in ['exact_distance_ms','rabitq_ms','server_thread_cpu_ms',
        'expanded','max_expanded_depth','max_cross_shard_depth','peer_branch_rpc_count','file_read_bytes']]
    physical=sum(n['process_deltas']['storage_read_bytes'] for n in d['node_process_load'])
    lines.append('|'+row['name']+'|'+ '|'.join(f'{v:.3f}' for v in values)+f'|{physical}|')
lines += ['', 'CPU平均核数采用进程快照窗口，包含快照开销，不能作为查询测量窗口的精确CPU利用率。pread为系统调用墙钟，包含页缓存命中，不等于物理SSD延迟。文件读取字节是逻辑读取量，不等于物理块设备读量；最大图深度也不等于展开顶点数。逐查询延迟、召回、跳数、网络、磁盘与CPU统计、节点trace和阶段均值均保留在每轮runs目录。', '',
          '## p99时序图', '',
          f"选择最高吞吐布局三轮中QPS最接近其中位数的一轮：{representative['name']}。首先从全体请求选最接近本轮全体p99的记录；另提供跨节点查询中最接近同一全体p99的样本，不将其称为跨节点子集p99。", '',
          '![全体p99附近查询](p99/overall/timeline.png)', '',
          '![跨节点p99附近查询](p99/cross_node/timeline.png)', '',
          '各主机使用本地单调时钟，分支面板不能直接对齐或相加。搜索、派发、报告可能重叠，计算线程释放后控制等待不占用该搜索线程。', '',
          '## 结论边界与复现', '',
          '这是所测线程空间、当前硬件、并发256和预算200下的最高吞吐布局；坐标搜索没有穷举所有交互，也不证明全局最优。主要命中热缓存；每轮仍只持续数秒，三轮重复不能替代长时间稳态实验。提高并发、改变预算、SSD冷热状态或查询分布后需重新校准。', '',
          '每轮要求零失败、Recall≥95%、不重复访问/预算/无顶层扩展审计通过及内存暂存无溢出。网络指标完整性单独保留，不按该标记删除慢轮次或慢请求。部分延迟超过心跳周期的查询在交付时仍有租约RPC在途，网络字节可能尚未齐，不能将这些计数伪装为完整。原始交付记录按SHA256无损压缩，最终queries.jsonl.gz含独立离线补齐的网络计数。应用源码哈希、各机扩展哈希、硬件和配置见source_hashes.json/deployed_provenance.json/hardware.json及每轮plan/head_effective/spec。', '',
          '执行入口为run_sweep.py，参数集中在sweep.yaml；筛选/细调和最终确认布局由progress.json完整保存。最终配置另存best_layout.yaml，未自动改变生产模板或重启220xx服务。', '']
lines.append('\n## 结果解释与剩余等待\n\n最高吞吐布局的三轮中位QPS比基线高6.80%，但各轮p99中位数增加16.60%（99.51→116.03ms）；两组QPS范围重叠，仅3轮不足以证明稳定或统计显著的提升。按最高测得中位QPS记录候选；若继续优先最低p99，当前基线更合适。没有自动更改生产默认值。\n\n代表查询为confirm_r1_c2的query_id=1024132、job=20992，服务延迟116.026244ms，Recall@10=1.00，本轮p99=116.026256ms。全体p99附近样本本身已有父子跨节点分支，因此两张选样图选择同一查询。\n\n|父分支（Intel-32）阶段|耗时ms|\n|---|---:|\n|收到完整请求后至接纳开始：接收排队/校验|83.944|\n|接纳与注册|12.965|\n|其中接纳状态锁等待（上一行的子区间）|12.866|\n|计算队列及设置|0.192|\n|搜索循环|15.925|\n|报告构造/搜索线程释放|0.525|\n|完成命令等待|0.028|\n|完成命令至报告发布|0.016|\n|报告调度与编码提交|0.495|\n|报告发送队列|0.021|\n\n接纳状态锁等待已有独立时间戳直接支持；83.944ms尚包含handler排队、回调调度及校验，当前时间点不能把它全部归为某一种锁或网络传输。父计算线程在本节点收到报文后的113.551ms释放；子分支在Intel-35上的搜索仅0.312ms。跨机面板有独立起点，不直接相加。顶层路由队列0.013ms、HNSW计算0.653ms、入口派发0.070ms、合并队列0.012ms；此样本主要剩余等待发生在数据接纳路径，不能断言搜索计算是唯一瓶颈。\n\n有一轮短测的3条请求交付时仍有心跳控制RPC在途，网络计数不完整；该轮及慢请求均保留。9轮最终确认的网络指标完整性标记全部为true。全部45轮查询失败为0、Recall约97.03%，没有内存暂存溢出。\n\n可复用配置保存为best_layout_flat.yaml、best_data_plan.yaml与best_head_plan.yaml；best_layout.yaml另含测量结果和按角色定义的线程比例。三台机器的本轮37751–37753/37810实验监听端口均已关闭，未重启原220xx服务。\n')
(ROOT/'REPORT.md').write_text('\n'.join(lines))
print(json.dumps(out,indent=2))
