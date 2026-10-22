"""Offline validation and Chinese report for the fixed-load worker-count experiment."""
from pathlib import Path
import copy,hashlib,json,statistics,subprocess
import yaml

ROOT=Path(__file__).resolve().parent;PROJECT=ROOT.parents[1]
options=yaml.safe_load((ROOT/'experiment.yaml').read_text())
data=json.loads((ROOT/'comparison.json').read_text());groups=data['groups'];runs=data['runs']
results={r['name']:json.loads((ROOT/'cases'/r['name']/'result.json').read_text()) for r in runs}

def median(mode,fn):return statistics.median(fn(r,results[r['name']]) for r in runs if r['mode']==mode)
def change(a,b):return (a/b-1)*100

baseline=None;head_baseline=None;selection_hash=None;head_identity=None;audits=[]
for r in runs:
    case=ROOT/'cases'/r['name'];plan=yaml.safe_load((case/'plan.yaml').read_text());c=copy.deepcopy(plan['dhdgs']);s=results[r['name']]
    assert c['peer']['worker_threads']==r['threads']
    assert c['search']['max_expansions']==options['search_budget'] and c['search']['top_k']==options['top_k']
    assert c['native_runtime']['queued_control_registration'] is True
    assert plan['control']['benchmark']==dict(queries=options['count'],concurrency=options['concurrency'],warmup=options['warmup'])
    c['peer'].pop('worker_threads');c['peer']['head_lanes'].pop('results_directory')
    if baseline is None:baseline=c
    assert c==baseline,(r['name'],'data configuration mismatch beyond workers and artifact path')
    h=yaml.safe_load((case/'head_effective.yaml').read_text())
    assert h['native_runtime']['move_head_reports'] is True
    h['peer'].pop('worker_threads');h['peer']['head_lanes'].pop('results_directory')
    if head_baseline is None:head_baseline=h
    assert h==head_baseline,(r['name'],'head configuration mismatch')
    selection=hashlib.sha256((case/'selection.json').read_bytes()).hexdigest()
    if selection_hash is None:selection_hash=selection
    assert selection==selection_hash
    identity=json.loads((case/'head_source.json').read_text())
    if head_identity is None:head_identity=identity
    assert identity==head_identity,(r['name'],'head source/binary changed during comparison')
    assert s['query_count']==options['count'] and s['failed_queries']==0 and all(s['audit'].values())
    assert s['query_metric_distributions']['expanded']['mean']==options['search_budget']
    assert s['query_metric_distributions']['expanded']['p50']==options['search_budget']
    assert s['query_metric_distributions']['file_read_bytes']['mean']==2457600
    assert s['actual_peak_inflight']==options['concurrency'] and s['result_capture']['overflow_batches']==0
    assert all(not e['during_queries'] for e in s['result_capture']['native_drain_events'])
    expected_branches=round(s['query_count']*(1+s['query_metric_distributions']['peer_branch_rpc_count']['mean']))
    assert r['timings']['branches']['search']['count']==expected_branches
    audits.append(dict(case=r['name'],workers=r['threads'],trace_count=expected_branches,passed=True))

g32=groups['threads32'];g64=groups['threads64']
qdelta=change(g32['qps_median'],g64['qps_median']);pdelta=change(g32['p99_median'],g64['p99_median'])
pairs=[]
for repeat in range(len(options['orders'])):
    a=next(r for r in runs if r['repeat']==repeat and r['threads']==64)
    b=next(r for r in runs if r['repeat']==repeat and r['threads']==32)
    pairs.append(dict(repeat=repeat,qps_change_percent=change(b['qps'],a['qps']),p99_change_percent=change(b['p99'],a['p99'])))
evidence=dict(qps_change_percent=qdelta,p99_change_percent=pdelta,pairs=pairs,selection_sha256=selection_hash,configuration_equivalence=True,audits=audits)
(ROOT/'validation.json').write_text(json.dumps(evidence,indent=2))

lines=['# 三台机器：搜索线程从64降至32','','本轮只改变每个数据节点的搜索工作线程数；比较同一最新实现、同一时段的交替对照，不使用历史最佳QPS充当64线程基线。', '',f'QPS四轮中位数变化 **{qdelta:+.2f}%**，每轮p99的四轮中位数变化 **{pdelta:+.2f}%**。这里只报告实测差异；四个短时配对不能证明统计显著性或硬件性能上限。','','## 配置与方法','','- intel-35同时运行独立顶层进程和数据节点0，intel-32为节点1，intel-30为节点2。','- seq_2M全量索引、2560维；同一2000条验证向量重复至每轮20,000个请求，warmup200；并发256、搜索预算200、top10。','- 4轮配对，顺序为64→32、32→64、64→32、32→64；共160,000个计时查询。','- 数据网络接收8个reactor，每个1个handler；两类逻辑发送器各4个reactor。顶层route16、merge4、接收4×handler4，两类发送器各4个reactor；每逻辑客户端每目标总连接额度128。固定网络配置，关闭自适应扩缩容。','- queued_control_registration、move_head_reports和early_parent_report均启用；搜索算法、图、RaBitQ、跨节点扩展决策及指标记录不变。','- 预先采用5%作为实用吞吐下降参考线；它不是统计显著性检验。各轮独立启动实验服务，原常驻服务保留。','','## 吞吐、延迟与召回','','以下是每轮指标的中位数，不是合并80,000条查询重新计算的分位数。','','|指标|64线程/节点|32线程/节点|32相对64|','|---|---:|---:|---:|']
for label,fn,fmt in [
    ('QPS',lambda r,s:s['qps'],'.2f'),
    ('平均服务延迟 ms',lambda r,s:s['latency_ms']['mean'],'.3f'),
    ('p50 ms',lambda r,s:s['latency_ms']['p50'],'.3f'),
    ('p95 ms',lambda r,s:s['latency_ms']['p95'],'.3f'),
    ('p99 ms',lambda r,s:s['latency_ms']['p99'],'.3f'),
    ('最大服务延迟 ms',lambda r,s:s['latency_ms']['max'],'.3f'),
    ('Recall@10',lambda r,s:s['recall_at_k_all_requests'],'.5f'),
    ('搜索线程CPU时间/查询 ms',lambda r,s:s['query_metric_distributions']['server_thread_cpu_ms']['mean'],'.3f'),
]:
    a=median('threads64',fn);b=median('threads32',fn)
    lines.append(f'|{label}|{a:{fmt}}|{b:{fmt}}|{change(b,a):+.2f}%|')
lines += ['',f"64线程QPS范围：{g64['qps_range'][0]:.2f}–{g64['qps_range'][1]:.2f}；32线程：{g32['qps_range'][0]:.2f}–{g32['qps_range'][1]:.2f}。全部160,000条查询成功、审计通过、无结果捕获溢出。",'','![QPS、p99和阶段对比](comparison.png)','','|配对轮次 / 顺序|64 QPS|32 QPS|QPS变化|64 p99 ms|32 p99 ms|','|---|---:|---:|---:|---:|---:|']
for p in pairs:
    a=next(r for r in runs if r['repeat']==p['repeat'] and r['threads']==64);b=next(r for r in runs if r['repeat']==p['repeat'] and r['threads']==32)
    order='→'.join(map(str,options['orders'][p['repeat']]))
    lines.append(f"|{p['repeat']+1} / {order}|{a['qps']:.2f}|{b['qps']:.2f}|{p['qps_change_percent']:+.2f}%|{a['p99']:.3f}|{b['p99']:.3f}|")
lines += ['','## 等待和计算阶段','','节点数据以分支为单位；下表为各轮分支平均值的中位数。不同线程、不同主机以及控制/计算区间可能重叠，不能把这些数相加当作端到端时延。','','|节点阶段|64线程 ms|32线程 ms|','|---|---:|---:|']
for key,label in [('receive','接收队列、解码与校验'),('admission','接纳及注册提交'),('runtime_lock','其中runtime状态锁等待'),('compute_ready','计算队列及启动设置'),('search','搜索循环'),('report_worker','报告构造至计算线程释放'),('register_command_queue','注册控制命令队列（可与搜索重叠）'),('completion_queue','完成控制命令队列'),('publication','完成命令处理至报告发布'),('report_send_queue','报告发送队列'),('report_ack','报告响应/ACK')]:
    lines.append(f"|{label}|{g64['branch_phase_mean_medians'][key]:.3f}|{g32['branch_phase_mean_medians'][key]:.3f}|")
lines += ['','|顶层阶段|64线程 ms|32线程 ms|','|---|---:|---:|']
for key,label in [('route_queue','路由队列'),('routing','HNSW路由'),('entry_send','入口派发含排队'),('data_and_control','数据节点检索及控制等待'),('merge_queue','合并队列'),('merge','合并执行及生命周期')]:
    lines.append(f"|{label}|{g64['head_phase_mean_medians'][key]:.3f}|{g32['head_phase_mean_medians'][key]:.3f}|")
lines += ['','## 各节点负载','','平均占用线程数 = 测量分支worker_enter到compute_released的时长总和 / 本节点计算窗口；占用比例再除以实际配置的32或64。该值包含线程被抢占和pread等待，**不等于CPU利用率**。','','|节点|64平均占用|占64比例|32平均占用|占32比例|64计算队列均值ms|32计算队列均值ms|','|---|---:|---:|---:|---:|---:|---:|']
for sid,host in [('0','intel-35'),('1','intel-32'),('2','intel-30')]:
    a=median('threads64',lambda r,s:r['timings']['occupancy'][sid]['mean_busy_search_workers']);b=median('threads32',lambda r,s:r['timings']['occupancy'][sid]['mean_busy_search_workers'])
    aq=median('threads64',lambda r,s:r['timings']['nodes'][sid]['compute_ready']['mean']);bq=median('threads32',lambda r,s:r['timings']['nodes'][sid]['compute_ready']['mean'])
    lines.append(f'|{host}|{a:.2f}|{a/64*100:.1f}%|{b:.2f}|{b/32*100:.1f}%|{aq:.3f}|{bq:.3f}|')
lines += ['','节点进程CPU、RSS、物理读写增量及采样窗口均保存在各case/result.json的node_process_load中。它们的采样窗口包含查询结束后的状态收集，不能直接当作仅计时查询区间的CPU利用率。','','## 搜索量、网络和I/O一致性','','|指标（逐查询均值的四轮中位数）|64线程|32线程|','|---|---:|---:|']
for key,label in [('expanded','扩展向量数'),('max_expanded_depth','最大图深度'),('peer_branch_rpc_count','数据节点间派发RPC'),('all_protocol_rpc_count','全部协议RPC'),('all_protocol_bytes','应用协议字节'),('file_read_bytes','逻辑文件读取字节'),('pread_ms','pread ms'),('exact_distance_ms','精确距离 ms'),('rabitq_ms','RaBitQ ms')]:
    a=median('threads64',lambda r,s:s['query_metric_distributions'][key]['mean']);b=median('threads32',lambda r,s:s['query_metric_distributions'][key]['mean'])
    lines.append(f'|{label}|{a:.3f}|{b:.3f}|')
lines += ['','每轮实际扩展200个向量/查询，逻辑文件读取2,457,600字节/查询。网络字节为应用协议统计，未加TCP/IP头或链路重传。测试使用同步pread和OS页缓存；这不是冷SSD测试，逻辑读取不能等同于物理SSD读取。计时服务延迟从API提交到Future完成，不含客户端为维持并发256而产生的提交前积压，也不含结束后独立进程的JSON处理。','','## p99样例','','每组选择QPS最接近该组中位数的一轮，再从跨节点查询中选择最接近该轮整体p99的一条；不是跨节点子集p99。主机时钟各自为零点，避免跨机时钟直接相减。计算线程释放后的控制/网络等待不占用原计算线程。','','![32线程p99](p99/threads32/timeline.png)','','[32线程详细时序](p99/threads32/REPORT.md)；[64线程详细时序](p99/threads64/REPORT.md)。','','## 文件、复现和适用范围','','所有调节项见experiment.yaml，运行`.venv/bin/python test/search_threads_32_64_20261022/run.py`。已有完成案例会复用；新测量需使用新实验目录及scratch/archive路径。离线分析运行analyze.py，再运行finalize.py。','','代码与二进制版本证据见preflight.json、deployed_provenance.json、head_v2_hashes.json；validation.json逐轮核验配置仅搜索线程数和结果路径不同、相同查询顺序、完整分支时间戳、预算和结果审计。各case保留plan.yaml、head_effective.yaml、result.json和SHA256归档清单。','','共享NVMe空间紧张，压缩逐查询和节点记录实际归档在`/var/tmp/dhdgs_search_threads_32_64_20261022`，test目录通过cases/*/runs链接引用；搬迁实验时需一起复制链接目标。临时原始捕获位于`/tmp/dhdgs_search_threads_32_64_20261022`。','','本实验观察的是固定并发256、固定查询集重复、热缓存、短时负载下的32/64线程取舍。共享机器的外部负载、OS调度和网络时序会造成波动，不能外推到冷SSD、更高并发或百亿向量。未更改应用默认线程数、索引或常驻服务。','']
(ROOT/'REPORT.md').write_text('\n'.join(lines))
for mode in ['threads32','threads64']:
    row=min([r for r in runs if r['mode']==mode],key=lambda r:abs(r['qps']-groups[mode]['qps_median']))
    with (ROOT/(mode+'_p99.log')).open('w') as log:
        subprocess.run([str(PROJECT/'.venv/bin/python'),str(ROOT/'plot_p99.py'),'--case',str(ROOT/'cases'/row['name']),'--output',str(ROOT/'p99'/mode),'--cross-node'],stdout=log,check=True)
print(json.dumps(evidence,indent=2))
