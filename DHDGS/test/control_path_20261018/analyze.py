"""Audit all paired runs and regenerate the communication optimization report."""
from pathlib import Path
import csv,gzip,hashlib,json,math,os,sys
import numpy as np
import yaml

R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path.insert(0,str(P/'python'))
os.environ.setdefault('MPLCONFIGDIR',str(R/'.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'font.weight':500,'axes.titleweight':500,'axes.labelweight':500,'axes.unicode_minus':False,'svg.fonttype':'path'})

def save(path,data):path.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False))
def table(headers,rows):
    return ['| '+' | '.join(headers)+' |','|'+'|'.join(['---']*len(headers))+'|']+['| '+' | '.join(map(str,row))+' |' for row in rows]

def audit(mode,run,round_no):
    with gzip.open(run/'report.json.gz','rt') as f:r=json.load(f)
    qs=r['queries'];selection=json.loads((R/mode/'query_selection.json').read_text())
    assert len(qs)==2000 and len({q['request_id'] for q in qs})==2000
    assert sorted(q['query_row'] for q in qs)==selection['validation']
    assert len(set(selection['validation']))==2000
    assert not set(selection['validation'])&set(selection['calibration_excluded'])
    with gzip.open(run/'queries.jsonl.gz','rt') as f:assert [json.loads(x) for x in f]==qs
    for q in qs:
        assert q['status']=='ok' and q['stats']['metrics_complete']
        assert q['recall']==len(set(q['ids'])&set(q['truth_ids']))/10
        assert math.isclose(q['client_wall_ms'],(q['client_end_ns']-q['client_start_ns'])/1e6,abs_tol=1e-6)
        s=q['stats'];assert s['expanded']<=200 and s['node_revisit_count']==s['router_expand_rpc_count']==0
        assert math.isclose(sum(s['latency_breakdown_ms'].values()),s['latency_ms'],abs_tol=1e-6)
        assert s['communication_mode']==mode
        if mode=='optimized':assert s['control_by_kind']['lease']['rpc_count']==s['control_by_kind']['cancel']['rpc_count']==0
        for b in q['branch_reports']:
            assert b['native_search_loop'] and b['compute_thread_migrations']==0
            assert b['work_grant']==b['work_spent']+b['work_unused']+sum(ch['work_grant'] for ch in b['children'])
            for ch in b['children']:
                assert sum(d['remote'] and d['decision_id']==ch['decision_id'] and d['selected_node']==ch['node_id'] for d in b['decisions'])==1
    from dhdgs.data import Dataset,load_complete_npy
    c=yaml.safe_load((R/mode/'experiment.yaml').read_text());dataset=Dataset(c['paths']['dataset']);qv=load_complete_npy(Path(c['query']['vectors']))
    for q in qs[:32]:
        diff=dataset.take(np.array(q['ids'],dtype='u8')).astype('f8')-qv[q['query_row']].astype('f8')
        np.testing.assert_allclose(q['squared_distances'],np.einsum('ij,ij->i',diff,diff),rtol=1e-12,atol=1e-10)
    services=json.loads((run/'services.json').read_text());clients=json.loads((run/'clients_lifecycle.json').read_text())
    assert services['test_completed'] and all(x['stopped'] and x['exit_code'] in (0,-15) for x in services['services'])
    assert len(clients)==20 and all(x['stopped'] and x['exit_code']==0 for x in clients)
    cur=peak=0
    for _,delta in sorted([(q['client_start_ns'],1) for q in qs]+[(q['client_end_ns'],-1) for q in qs]):
        cur+=delta;peak=max(peak,cur)
    assert cur==0 and peak==r['actual_peak_inflight']
    before=json.loads((run/'nodes_before.json').read_text());after=json.loads((run/'nodes_after.json').read_text())
    control={}
    for sid in after:
        a=after[sid]['peer_lifecycle']['control_totals'];b=before[sid]['peer_lifecycle']['control_totals']
        for k in a:control[k]=control.get(k,0)+a[k]-b[k]
        assert after[sid]['peer_lifecycle']['unacknowledged']==0
    assert control['errors']==0
    row={'mode':mode,'round':round_no,'run':str(run.relative_to(R)),
         'success':r['successful_queries'],'failures':r['failed_queries'],'peak_inflight':peak,
         'qps':r['qps'],'mean_ms':r['latency_ms']['mean'],'p50_ms':r['latency_ms']['p50'],
         'p95_ms':r['latency_ms']['p95'],'p99_ms':r['latency_ms']['p99'],
         'recall':r['recall_at_k_all_requests'],'mean_cpu_cores':sum(n['mean_cpu_cores'] for n in r['node_process_load']),
         'physical_read_bytes':sum(n['process_deltas']['storage_read_bytes'] for n in r['node_process_load']),
         'measured_wall_seconds':r['measured_wall_seconds']}
    for k in ('all_protocol_rpc_count','all_protocol_bytes','file_read_bytes','expanded','server_to_server_branches','max_expanded_depth','peer_connections_created','peer_connections_reused'):
        row[k]=float(np.mean([q['stats'][k] for q in qs]))
    for kind in ('heartbeat','result','lease','cancel'):
        row[kind+'_rpc_total']=sum(q['stats']['control_by_kind'][kind]['rpc_count'] for q in qs)
    for k,v in r['query_timing_summary']['cumulative_work'].items():row[k]=v['mean_ms']
    row['control_window_deltas']=control
    return row,{k:v for k,v in r.items() if k!='queries'}

def main():
    for manifest in ('sources.json','sources_final.json'):
        for name,sha in json.loads((R/manifest).read_text()).items():
            path=P/name
            if manifest=='sources.json' and name=='python/dhdgs/peer_rpc.py':path=R/'peer_rpc_first_round.py.txt'
            assert hashlib.sha256(path.read_bytes()).hexdigest()==sha,(manifest,name)
    rows=[];reports={}
    for mode in ('legacy','optimized'):
        runs=sorted((R/mode/'runs').glob('*/report.json.gz'));assert len(runs)>=2
        for i,path in enumerate(runs):
            row,r=audit(mode,path.parent,i+1);rows.append(row);reports[(mode,i+1)]=r
    save(R/'comparison.json',rows)
    fields=[k for k in rows[0] if k!='control_window_deltas']
    with (R/'comparison.csv').open('w') as f:
        w=csv.DictWriter(f,fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
    verification={'runs':len(rows),'requests_verified':len(rows)*2000,'source_manifests_verified':True,
                  'query_identity_recall_and_raw_records_verified':True,'budget_dispatch_no_revisit_and_native_thread_invariants_verified':True,
                  'independent_exact_distance_checks':len(rows)*320,'all_test_processes_stopped':True,
                  'runtime_control_errors':0,'optimized_normal_lease_and_cancel_rpc':0}
    save(R/'verification.json',verification)
    last=max(x['round'] for x in rows);a=next(x for x in rows if x['mode']=='legacy' and x['round']==last);b=next(x for x in rows if x['mode']=='optimized' and x['round']==last)
    fig,axes=plt.subplots(2,3,figsize=(13,8),layout='constrained')
    for ax,(key,title,unit) in zip(axes.flat,[('qps','吞吐量','QPS'),('p99_ms','尾延迟','ms'),('all_protocol_rpc_count','每查询协议请求数','RPC / query'),('mean_cpu_cores','三个数据进程 CPU 合计','核当量'),('peer_transfer_queue_ms','派发排队累计均值','ms / query'),('query_ready_queue_ms','计算就绪排队累计均值','ms / query')]):
        rounds=sorted({x['round'] for x in rows});x=np.arange(len(rounds))
        for mode,offset,color in [('legacy',-.18,'#7c8b9b'),('optimized',.18,'#238f82')]:
            vals=[next(v[key] for v in rows if v['round']==i and v['mode']==mode) for i in rounds]
            bars=ax.bar(x+offset,vals,.34,label=mode,color=color);ax.bar_label(bars,fmt='%.2f',padding=3,fontsize=9)
        ax.set_xticks(x,[f'第 {i} 轮' for i in rounds]);ax.set_title(title);ax.set_ylabel(unit);ax.margins(y=.18);ax.grid(axis='y',alpha=.15)
    axes[0,0].legend(loc='upper left',fontsize=9);fig.suptitle('通信路径优化：完整 2M 索引，本机三进程，每轮 2000 查询\n热页缓存、预算 200、每节点计算线程上限 256；累计阶段不可相加为端到端延迟',fontsize=13)
    for ext in ('png','svg'):fig.savefig(R/f'comparison.{ext}',dpi=160)
    plt.close(fig)
    lines=['# 通信路径优化：设计、实现与对照实验','',
      f"设计已写入 [DESIGN 第 11 节](../../DESIGN.md)，实现通过 **87 项回归测试**。两轮新旧模式对照共 {len(rows)*2000} 次请求，全部成功。最终代码第 {last} 轮：QPS {a['qps']:.2f} → {b['qps']:.2f}（{(b['qps']/a['qps']-1)*100:+.2f}%），p99 {a['p99_ms']:.2f} → {b['p99_ms']:.2f} ms（{(b['p99_ms']/a['p99_ms']-1)*100:+.2f}%）。CPU 尚未满载，通信排队也未消失。",'',
      '![两轮对照](comparison.png)','',
      '## 实现及保证','',
      '- 本地搜索结束后释放计算工作线程，已批准的子分支派发完成后由回调发布最终报告。派发失败、取消或过期不能发布成功。',
      '- 控制回传使用有界并发线程池，每分支最多一个在途控制消息；心跳按到期/节点变化发送，终态优先。',
      '- 心跳响应携带续租，优化模式不再周期主动发送 lease；完整终态正常结束不再发送 cancel，异常结束仍取消。',
      '- 同进程 Engine 共享回调端点；已完成查询保留有界身份和报告摘要墓碑，安全确认重复终态。RPC 使用有界独占借用的持久连接，空闲连接可读/已关闭时丢弃。',
      '- 保留扩展函数、全局预算、单次节点访问、父分支继续/子分支丢弃已访问节点候选及固定原生计算线程。通信线程不能自行创建远端搜索。','',
      '新增实现位于 peer.py、peer_rpc.py、head_hub.py；入口接入在 search.py，配置校验/迁移在 config.py，记录字段在 metrics.py。生命周期和连接池仍使用必要的短时状态锁，不持锁等待子分支网络完成；没有取消正确性同步。','',
      '## 对照条件与配置','',
      'Intel-35 单机三个 TCP 数据进程，共享 128 物理核 / 256 硬件线程；不是三台物理机。完整 2M×2560 索引复用，原始平方 L2、1-bit RaBitQ、排除 self、top-k=10、全局预算 200、frontier=64、local_beam=4。同步 buffered pread，应用缓存=0，详细 trace 开启。测试前已有热页缓存。','',
      '每组 20 客户端进程×100查询线程、2000 条固定且不同的验证查询，每进程另预热2条；排除原128条校准查询。数据节点计算线程池上限均为256、接纳上限2048、listen backlog4096、心跳1000ms、租约30s、控制RPC超时5s、派发超时30s。每条只运行一次，负载开始后逐步排空，不是持续稳态压测。','',
      '两模式使用相同当前实现，通过 mode 切换旧/新通信行为。legacy 保留每查询回调服务、串行控制发送、3个派发线程和阻塞收尾。optimized 使用以下配置。模式之间除通信组与输出目录外配置一致，未改搜索算法、预算及trace。','',
      '```yaml','peer:','  communication:','    mode: optimized','    transfer_workers: 16','    control_workers: 16','    max_pending_control: 64','    reuse_connections: true','    connections_per_endpoint: 8','    max_connection_endpoints: 64','```','',
      '新默认 YAML 使用 optimized；旧 YAML 完全缺少该组时显式迁移为 legacy，迁移记录可见。mode=legacy 可作回退对照。上述通信参数是本轮候选工作点，未做逐参数消融，不能宣称16线程或8连接最优。计算线程默认仍为8、默认预算192；本压测的256/200只在实验 YAML 中，不替换默认值。','',
      '## 两轮结果','']
    lines+=table(['轮次','模式','成功/失败','峰值在途','QPS','平均ms','p99 ms','Recall@10','平均CPU核当量'],[[x['round'],x['mode'],f"{x['success']}/{x['failures']}",x['peak_inflight'],f"{x['qps']:.2f}",f"{x['mean_ms']:.2f}",f"{x['p99_ms']:.2f}",f"{x['recall']:.3%}",f"{x['mean_cpu_cores']:.3f}"] for x in sorted(rows,key=lambda x:(x['round'],x['mode']))])
    lines+=['','每轮先 legacy 后 optimized，只有两轮短窗口，不能据此给出稳态容量或统计显著性。第一轮后增加空闲断开连接检测，第二轮采用最终代码；旧源文件、两份源代码哈希清单均保留。两轮均为完整相同查询集合，时间相关启发式可改变路径，因此召回与分支数略有不同，不要求逐查询结果相同。','',
      f'## 最终代码第 {last} 轮详细指标','']
    metric_names=[('qps','QPS'),('mean_ms','平均延迟 ms'),('p50_ms','p50 ms'),('p95_ms','p95 ms'),('p99_ms','p99 ms'),('all_protocol_rpc_count','协议 RPC / query'),('all_protocol_bytes','应用协议 B / query'),('server_to_server_branches','跨节点分支 / query'),('expanded','扩展顶点 / query'),('max_expanded_depth','最大扩展深度均值'),('file_read_bytes','逻辑读取 B / query'),('mean_cpu_cores','数据进程总平均CPU核当量'),('physical_read_bytes','物理读取增量 B')]
    lines+=table(['指标','legacy','optimized'],[[label,f'{a[k]:.3f}',f'{b[k]:.3f}'] for k,label in metric_names])
    lines+=['','应用协议字节包括消息体和帧，不是网卡/TCP实际流量。物理读取为进程 /proc I/O 计数增量；本轮为0不表示未调用 pread，而是没有观测到缓存未命中的设备读取。','',
      '### 端到端互斥阶段','']
    wa=reports[('legacy',last)]['query_timing_summary']['wall_stages'];wb=reports[('optimized',last)]['query_timing_summary']['wall_stages']
    lines+=table(['阶段','legacy平均ms','optimized平均ms','optimized占内部wall'],[[k,f"{wa[k]['mean_ms']:.3f}",f"{wb[k]['mean_ms']:.3f}",f"{wb[k]['fraction_of_total_latency']:.2%}"] for k in wa])
    lines+=['','### 分支累计工作时间','',
      '以下按每个查询跨分支累计后取均值，不是端到端互斥阶段，不能相加或画成延迟占比。派发收尾与派发队列/传输时间重叠；优化后收尾仍有等待，但已经不占用计算工作线程。','']
    timings=[('query_ready_queue_ms','计算就绪排队'),('query_compute_slice_wall_ms','计算执行区间wall'),('server_thread_cpu_ms','搜索线程CPU'),('query_transfer_finalize_wait_ms','派发收尾等待'),('peer_transfer_queue_ms','派发线程池排队'),('peer_pool_wait_ms','连接池借用等待'),('peer_connect_ms','新建连接'),('peer_send_ms','序列化与发送'),('peer_response_wait_ms','等待响应与解码'),('peer_transfer_wall_ms','派发执行wall，不含派发队列'),('pread_ms','pread调用'),('exact_distance_ms','精确距离wall'),('rabitq_ms','RaBitQ wall')]
    lines+=table(['字段/含义','legacy平均ms','optimized平均ms'],[[f'{k}：{label}',f'{a[k]:.3f}',f'{b[k]:.3f}'] for k,label in timings])
    lines+=['','线程与队列的等待没有全部消失。更快地接纳请求、释放父分支工作线程，会改变计算/派发队列的压力分布；第一轮计算就绪等待明显上升。这是实测现象，不能把所有差异都归因于GIL或网络。socket响应阶段含目标节点处理、Python调度和消息编解码，不是纯网络RTT。新建连接减少也不保证每个连接的等待更小。','',
      '### 控制消息与复用','']
    lines+=table(['类型','legacy总次数','optimized总次数'],[[k,a[k+'_rpc_total'],b[k+'_rpc_total']] for k in ('heartbeat','result','lease','cancel')])
    ctrl=b['control_window_deltas'];created=ctrl['connections_created'];reused=ctrl['connections_reused']
    lines+=['',f"优化模式节点控制发送的前后快照差值：发送 {ctrl['sent']}，错误 {ctrl['errors']}，新建连接 {created}，复用 {reused}（复用占比 {reused/max(1,created+reused):.2%}）。发送线程池累计排队 {ctrl['queue_ms']:.3f} ms，发送执行累计 {ctrl['wall_ms']:.3f} ms；这些是多任务累计量。逐查询 peer_connections_* 仅统计数据节点之间的派发连接，不能代替全部控制连接。",'',
      '心跳计数有明显轮间波动：第一轮 legacy→optimized 为2392→4239，第二轮为2639→1348。并发发送、终态时机及涉及节点变更都会影响心跳次数，不能把心跳减少单独视作稳定收益。两轮均消除了主动lease和成功cancel，并未实现跨查询心跳打包。返回给用户后的迟到终态重试由墓碑确认，不追加到已冻结的逐查询统计；节点累计发送计数是检查这类重试的补充。本轮窗口内控制发送错误为0。','',
      '### 每节点负载（最终代码）','']
    node_rows=[]
    for mode in ('legacy','optimized'):
        for n in reports[(mode,last)]['node_process_load']:
            node_rows.append([mode,n['shard_id'],f"{n['mean_cpu_cores']:.3f}",f"{n['process_window_seconds']:.3f}",n['process']['threads'],f"{n['process']['rss_bytes']/2**30:.3f}",n['process_deltas']['storage_read_bytes']])
    lines+=table(['模式','节点','平均CPU核当量','快照窗口s','结束线程数','RSS GiB','物理读取B'],node_rows)
    lines+=['','CPU核当量=user+system秒增量/快照窗口秒数；窗口略宽于查询计时。线程数包含计算、控制、派发、接收等线程，不代表同时执行数量。整机利用率还包含客户端及其他任务，不能全部归因于数据服务。','',
      '## 尚存瓶颈与下一步实验','',
      '本轮证据支持“解除部分通信等待、减少重复控制请求可降低p99并提高吞吐”，不支持“CPU已跑满”或“SSD瓶颈已解决”。优先进行独立消融：固定计算线程与负载，扫描派发线程/连接上限；单独关闭详细trace，测量大报告的序列化与回传代价；对控制发送加入细分CPU/排队测量，再考虑跨查询心跳合包。每组以平均Recall@10≥.95、无失败为门槛，优先p99，再比较QPS。','',
      '原生查询在SSD等待时暂停/恢复、每计算线程轮转多个query仍需独立C++状态机实现。本轮未执行该改造，未在通信调度层增加远端扩展。物理三机网络、冷缓存/工作集超过RAM、长时间稳态和故障注入也尚未由本实验验证。','',
      '## 核验、原始记录和复现','',
      '87项回归涵盖只有一个计算worker时派发等待不阻塞下一query、立即完成/失败/取消的终态处理、慢控制端点、多查询隔离、错误token、重复/冲突终态、连接容量及断开检测、旧YAML迁移、原生搜索及io_uring。未声称穷尽所有网络故障时序。','',
      f"审计全部{len(rows)*2000}条记录：唯一请求ID、冻结查询集合、排除校准集、召回重算、JSONL一致性、预算≤200、父子预算守恒、每个子分支来自批准决策、无节点回访、无顶层扩展RPC、固定原生计算线程。另独立复算每组32条查询的top10距离，共{len(rows)*320}个距离；这不是重算全库ground truth。所有测试进程均退出，所有分支终态获得确认。",'',
      '- [参数与结果对照 CSV](comparison.csv)、[JSON](comparison.json)、[审计](verification.json)、[回归日志](regression_final.log)。',
      '- [新模式 YAML](optimized/experiment.yaml)、[旧模式 YAML](legacy/experiment.yaml)、[负载参数](optimized/load_plan.yaml)。',
      '- [第一轮源码哈希](sources.json)、[最终源码哈希](sources_final.json)、[图表 SVG](comparison.svg)。']
    for x in sorted(rows,key=lambda x:(x['round'],x['mode'])):
        rel=x['run'];lines.append(f"- 第{x['round']}轮{x['mode']}：[汇总]({rel}/summary.json)、[逐查询]({rel}/queries.csv)、[原始JSONL]({rel}/queries.jsonl.gz)、[完整报告]({rel}/report.json.gz)、[资源采样]({rel}/resource_samples.json.gz)、[阶段均值]({rel}/query_stage_means.csv)。")
    lines+=['','在仓库父目录运行（需要本机TCP权限，两个模式共享端口，必须依次执行）：','',
      '```sh','gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/control_path_20261018/legacy/run_load.py',
      'gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/control_path_20261018/optimized/run_load.py',
      'gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/control_path_20261018/analyze.py','```','',
      'run_load.py保存新时间戳目录；分析脚本审核所有已存配对轮次。prepare.py仅供首次生成，重复运行会改写配置与来源清单，历史实验不应覆盖。大体积原始数据保留在本机test目录，不纳入Git。','']
    (R/'REPORT.md').write_text('\n'.join(lines))
    save(R/'artifact_manifest.json',{str(p.relative_to(R)):hashlib.sha256(p.read_bytes()).hexdigest() for p in R.rglob('*') if p.is_file() and not p.is_symlink() and not {'before','.mplconfig','__pycache__'}.intersection(p.parts) and p.name not in ('artifact_manifest.json','analysis.log')})
    print(json.dumps({'verification':verification,'final_pair':[{k:x[k] for k in ('mode','round','qps','p99_ms','recall','mean_cpu_cores','all_protocol_rpc_count')} for x in (a,b)]},ensure_ascii=False))

if __name__=='__main__':main()
