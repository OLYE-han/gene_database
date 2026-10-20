"""Readable metric tables derived only from completed experiment records."""
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def fmt(value, digits=2):
    return '未测得' if value is None else f'{value:.{digits}f}'


def main():
    rows = sorted(json.loads((ROOT / 'comparison.json').read_text()), key=lambda p: (p['concurrency'], p['worker_threads']))
    # The runner exports every latency distribution key with an _ms suffix;
    # count is a sample count, so correct that label in the derived CSV.
    comparison_path=ROOT/'comparison.csv'
    with comparison_path.open() as f:
        reader=csv.DictReader(f);fields=reader.fieldnames;table=list(reader)
    if 'latency_count_ms' in fields:
        fields=['latency_sample_count' if k=='latency_count_ms' else k for k in fields]
        for row in table:row['latency_sample_count']=row.pop('latency_count_ms')
        with comparison_path.open('w') as f:
            writer=csv.DictWriter(f,fields);writer.writeheader();writer.writerows(table)
    done = (ROOT / 'completed.json').exists()
    lines = ['# 全量三机线程扫描：指标对照', '',
             f'状态：{"扫描完成" if done else "部分结果"}，{len(rows)}/12 组。完整参数见 [experiment.yaml](experiment.yaml)，方法见 [PLAN.md](PLAN.md)。', '',
             '每组使用相同 1,024 条查询，所有数据节点同时设置相同计算线程数。各配置一次测量，累计指标均值使用指标完整的成功查询。',
             'QPS 为成功请求数除以计时窗口；延迟分位数为成功请求延迟，召回分母包含失败查询。', '',
             '| 线程/节点 | 客户端并发 | QPS | 平均 ms | p50 ms | p95 ms | p99 ms | Recall@10 | 失败 | 指标不完整 |',
             '|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for p in rows:
        l = p['latency_ms']; a = p['audit']
        lines.append(f'| {p["worker_threads"]} | {p["concurrency"]} | {fmt(p["qps"])} | {fmt(l["mean"])} | {fmt(l["p50"])} | {fmt(l["p95"])} | {fmt(l["p99"])} | {fmt(p["recall"],6)} | {a["failures"]} | {a["incomplete_metrics"]} |')
    lines += ['', '## 每查询计算与文件访问', '',
              'CPU 是跨数据节点累计线程 CPU 时间；估距、精排和 pread 是累计墙钟时间，可能重叠，并受线程调度影响。不能把下面几列相加作为端到端延迟，也不能把 pread 时间当作纯 SSD 等待。', '',
              '| 线程/节点 | 并发 | CPU ms | RaBitQ ms | 精排 ms | pread ms | 逻辑 MiB | pread 次数 | 物理读取 MiB/整组 |',
              '|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for p in rows:
        m = p['mean_metrics_complete_queries']
        values = [m.get(k) for k in ('server_thread_cpu_ms','rabitq_ms','exact_distance_ms','pread_ms')]
        filebytes = m.get('file_read_bytes'); reads = p['physical_read_bytes']
        lines.append('| ' + ' | '.join([str(p['worker_threads']), str(p['concurrency']), *map(fmt,values),
                     fmt(filebytes/2**20 if filebytes is not None else None), fmt(m.get('pread_calls')),
                     fmt(reads/2**20 if reads is not None else None)]) + ' |')
    lines += ['', '## 每查询通信与图搜索', '',
              '协议字节为应用层统计，不含全部 TCP/IP 开销。总 RPC 包含入口提交、控制、keep-alive 和结果回传；跨数据节点分支才是跨机搜索扩展次数。',
              '扩展顶点数是工作量，路径深度是图的边数；返回 top-1 的最短记录路径不代表能够在那一刻安全停止。', '',
              '| 线程/节点 | 并发 | 协议 KiB | 总 RPC | 跨机分支 | 扩展顶点 | 最大扩展深度 | top1 最短记录深度 | 备用入口数 | 重复扩展数 |',
              '|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for p in rows:
        m = p['mean_metrics_complete_queries']; size = m.get('all_protocol_bytes')
        values = [size/1024 if size is not None else None, *[m.get(k) for k in
                  ('all_protocol_rpc_count','server_to_server_branches','expanded','max_expanded_depth',
                   'top1_min_discovery_depth','fallback_entries','duplicate_expansions')]]
        lines.append('| ' + ' | '.join([str(p['worker_threads']),str(p['concurrency']),*map(fmt,values)]) + ' |')
    lines += ['', '## 数据节点资源', '',
              '平均用核数等于进程 CPU 秒除以测量窗口秒。RSS 不包含全部 OS 文件页缓存。Intel-35 上的入口和压测进程不计入此表。', '',
              '| 线程/节点 | 并发 | Intel-35 核数 | Intel-32 核数 | Intel-30 核数 | 总核数 | Intel-35 RSS GiB | Intel-32 RSS GiB | Intel-30 RSS GiB |',
              '|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for p in rows:
        nodes = sorted(p['nodes'],key=lambda n:n['shard_id'])
        values = [*[n['mean_cpu_cores'] for n in nodes], p['cpu_cores_sum'], *[n['process']['rss_bytes']/2**30 for n in nodes]]
        lines.append('| ' + ' | '.join([str(p['worker_threads']),str(p['concurrency']),*[fmt(v,3) for v in values]]) + ' |')
    lines += ['', '## 端到端阶段均值', '',
              '下表三段来自同一端到端墙钟分解，可以相加。数据执行与控制等待包含队列、计算、I/O 和通信等待，不等于纯网络延迟。', '',
              '| 线程/节点 | 并发 | 路由 ms | 数据执行及控制等待 ms | 合并及清理 ms |',
              '|---:|---:|---:|---:|---:|']
    stage_rows = []
    for p in rows:
        stages = {s['stage']:float(s['mean_ms']) for s in p['stage_means'] if s['category']=='wall_stages' and s['mean_ms']}
        lines.append('| ' + ' | '.join([str(p['worker_threads']), str(p['concurrency']), *[fmt(stages.get(k)) for k in
                     ('routing_wall_ms','peer_execution_and_control_wait_ms','head_merge_and_lifecycle_ms')]]) + ' |')
        stage_rows += [{'worker_threads':p['worker_threads'],'client_concurrency':p['concurrency'],**s} for s in p['stage_means']]
    with (ROOT/'stage_comparison.csv').open('w') as f:
        writer=csv.DictWriter(f,list(stage_rows[0]) if stage_rows else []);writer.writeheader();writer.writerows(stage_rows)
    lines += ['', '## 选择与局限', '']
    for concurrency in sorted({p['concurrency'] for p in rows}):
        group=[p for p in rows if p['concurrency']==concurrency]
        eligible=[p for p in group if p['valid_measurement'] and p['target_recall_met']]
        if not eligible:
            lines.append(f'- 并发 {concurrency}：没有同时满足召回与完整性条件的配置。');continue
        best=min(eligible,key=lambda p:(p['latency_ms']['p99'],-p['qps']))
        baseline=next((p for p in group if p['worker_threads']==1),None)
        message=f'- 并发 {concurrency}：已测合格配置中最低 p99 为每节点 {best["worker_threads"]} 线程，p99={best["latency_ms"]["p99"]:.2f} ms，QPS={best["qps"]:.2f}。'
        if baseline:
            message+=f' 相比 1 线程，p99 变化 {(best["latency_ms"]["p99"]/baseline["latency_ms"]["p99"]-1)*100:+.1f}%，QPS 变化 {(best["qps"]/baseline["qps"]-1)*100:+.1f}%。'
        lines.append(message)
    lines += ['', '本轮没有修改默认配置。单次窗口只能说明已测负载下的变化，不能证明统计显著性或全局最优线程数。',
              '并发 32 是整个客户端的并发数，不保证每台节点都能同时占满 32 个工作线程。',
              '沿用已验证完整索引，构建时间为上一轮的约 2511 秒；本次未重复构建，不能评价计算工作线程数对建图的影响。',
              '纯网络与物理 SSD 延迟占比尚无法精确识别；本轮保留相同详细追踪，因此结论包含该观测开销。', '']
    (ROOT/'ANALYSIS.md').write_text('\n'.join(lines))
    print('Wrote ANALYSIS.md and stage_comparison.csv')


if __name__=='__main__': main()
