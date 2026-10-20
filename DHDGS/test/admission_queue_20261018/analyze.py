"""Audit frozen admission experiments, including waiting transferred to targets."""
from pathlib import Path
import csv,gzip,hashlib,json,math,os
import numpy as np
R=Path(__file__).resolve().parent;P=R.parents[1]
os.environ.setdefault('MPLCONFIGDIR',str(R/'.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'font.weight':500,'axes.titleweight':500,'axes.labelweight':500,'axes.unicode_minus':False,'svg.fonttype':'path'})
def save(path,data):path.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False))
def table(h,rows):return ['| '+' | '.join(h)+' |','|'+'|'.join(['---']*len(h))+'|']+['| '+' | '.join(map(str,r))+' |' for r in rows]
def audit(mode,run,round_no):
    with gzip.open(run/'report.json.gz','rt') as f:r=json.load(f)
    qs=r['queries'];selection=json.loads((R/mode/'query_selection.json').read_text())
    assert len(qs)==2000 and len({q['request_id'] for q in qs})==2000
    assert sorted(q['query_row'] for q in qs)==selection['validation']
    assert not set(selection['validation'])&set(selection['calibration_excluded'])
    with gzip.open(run/'queries.jsonl.gz','rt') as f:assert [json.loads(x) for x in f]==qs
    for q in qs:
        assert q['status']=='ok' and q['stats']['metrics_complete']
        assert q['recall']==len(set(q['ids'])&set(q['truth_ids']))/10
        s=q['stats'];assert s['expanded']<=200 and s['node_revisit_count']==s['router_expand_rpc_count']==0
        assert math.isclose(s['query_ready_queue_ms'],s['query_admission_queue_ms']+s['query_executor_start_wait_ms'],abs_tol=1e-6)
        for b in q['branch_reports']:
            assert b['native_search_loop'] and b['compute_thread_migrations']==0
            assert b['work_grant']==b['work_spent']+b['work_unused']+sum(ch['work_grant'] for ch in b['children'])
            for ch in b['children']:
                assert sum(d['remote'] and d['decision_id']==ch['decision_id'] and d['selected_node']==ch['node_id'] for d in b['decisions'])==1
    services=json.loads((run/'services.json').read_text());clients=json.loads((run/'clients_lifecycle.json').read_text())
    assert services['test_completed'] and all(x['stopped'] and x['exit_code'] in (0,-15) for x in services['services'])
    assert len(clients)==20 and all(x['stopped'] and x['exit_code']==0 for x in clients)
    row={'mode':mode,'round':round_no,'run':str(run.relative_to(R)),'success':r['successful_queries'],'failures':r['failed_queries'],
         'qps':r['qps'],'mean_ms':r['latency_ms']['mean'],'p50_ms':r['latency_ms']['p50'],'p99_ms':r['latency_ms']['p99'],
         'recall':r['recall_at_k_all_requests'],'peak_inflight':r['actual_peak_inflight'],
         'mean_cpu_cores':sum(n['mean_cpu_cores'] for n in r['node_process_load']),
         'physical_read_bytes':sum(n['process_deltas']['storage_read_bytes'] for n in r['node_process_load'])}
    for k in ('all_protocol_rpc_count','all_protocol_bytes','server_to_server_branches'):
        row[k]=float(np.mean([q['stats'][k] for q in qs]))
    for k,v in r['query_timing_summary']['cumulative_work'].items():row[k]=v['mean_ms']
    node_rows=[]
    for n in r['node_process_load']:
        a=n['peer_lifecycle_after']
        assert a['admission_mode']==mode and a['admission_pending_start']==a['admission_queue_depth']==a['unacknowledged']==0
        assert a['admission_capacity_rejections']==a['control_totals']['errors']==0
        assert a['admission_peak_pending_start']<=2048
        node_rows.append({'node':n['shard_id'],'cpu_cores':n['mean_cpu_cores'],'threads_after':n['process']['threads'],
            'rss_gib':n['process']['rss_bytes']/2**30,'pending_peak_since_start':a['admission_peak_pending_start']})
    row['nodes']=node_rows
    return row
def main():
    for manifest in ('sources.json','sources_final.json'):
        for name,sha in json.loads((R/manifest).read_text()).items():
            path=P/name
            if manifest=='sources.json' and name=='python/dhdgs/peer.py':path=R/'peer_first_round.py.txt'
            assert hashlib.sha256(path.read_bytes()).hexdigest()==sha,(manifest,name)
    rows=[]
    for mode in ('direct','buffered'):
        runs=sorted((R/mode/'runs').glob('*/report.json.gz'));assert len(runs)==2
        rows.extend(audit(mode,p.parent,i+1) for i,p in enumerate(runs))
    save(R/'comparison.json',rows)
    with (R/'comparison.csv').open('w') as f:
        fields=[k for k in rows[0] if k!='nodes'];w=csv.DictWriter(f,fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
    save(R/'verification.json',{'runs':4,'requests':8000,'all_successful':True,'source_hashes_verified':True,
        'query_identity_and_recall_verified':True,'budget_dispatch_single_visit_and_fixed_thread_verified':True,
        'ready_wait_split_verified':True,'all_test_processes_stopped':True,'queue_capacity_rejections':0,'pending_at_end':0})
    fig,axes=plt.subplots(2,3,figsize=(13,8),layout='constrained')
    for ax,(k,title,unit) in zip(axes.flat,[('qps','吞吐量','QPS'),('p99_ms','尾延迟','ms'),('mean_ms','平均查询延迟','ms'),
        ('query_transfer_finalize_wait_ms','源节点派发收尾等待','累计ms / query'),('peer_transfer_queue_ms','源节点派发队列等待','累计ms / query'),('query_ready_queue_ms','目标节点就绪等待','累计ms / query')]):
        for mode,off,color in [('direct',-.18,'#70879a'),('buffered',.18,'#248b7c')]:
            vals=[next(x[k] for x in rows if x['mode']==mode and x['round']==i) for i in (1,2)]
            bars=ax.bar(np.arange(2)+off,vals,.34,label=mode,color=color);ax.bar_label(bars,fmt='%.1f',padding=3,fontsize=9)
        ax.set_xticks([0,1],['第1轮','第2轮']);ax.set_title(title);ax.set_ylabel(unit);ax.margins(y=.25);ax.grid(axis='y',alpha=.15)
    axes[0,0].legend(loc='upper left',fontsize=8)
    fig.suptitle('接纳队列：发送端等待与目标端排队必须同时观察\n完整2M索引 · 本机三进程 · 每组2000查询 · 预算200 · 热页缓存',fontsize=14)
    for ext in ('png','svg'):fig.savefig(R/f'comparison.{ext}',dpi=160)
    plt.close(fig)
    a=next(x for x in rows if x['round']==2 and x['mode']=='direct');b=next(x for x in rows if x['round']==2 and x['mode']=='buffered')
    lines=['# 有界接纳队列：快速确认与等待转移对照','',
      '实现接收线程完成校验、去重、容量预约和入队后立即返回accepted；独立接纳线程负责计算线程池提交。95项回归通过，含真正TCP连接在后台submit阻塞时收到确认、队列容量、重复/冲突、取消/过期、后台提交失败和关闭。设计见 [DESIGN 11.10](../../DESIGN.md)。','',
      '![对照图](comparison.png)','',
      '## 配置与范围','',
      '两种模式都使用optimized通信、native计算循环和同步buffered pread。完整2M×2560索引复用；预算200/frontier64、每节点计算线程池256、派发16、控制16、连接上限8/端点、接纳上限2048。新增queue_capacity=2048，按尚未开始初次计算的总任务数计算，包含已交给计算线程池的任务；不是仅前端deque长度。默认YAML的接纳容量为128，与默认max_active_queries相配，本实验值没有替换默认容量。','',
      '同一2000条不同验证查询，排除既有128条校准查询；20客户端进程×100查询线程，每组40次预热不计入查询统计。两轮共8000请求，第一轮direct→buffered，第二轮buffered→direct，全部成功且指标完整。不是持续稳态测试，不是物理三机或冷SSD结果。','',
      'direct仍由接收线程直接调用计算线程池submit；buffered由一个独立接纳线程调用submit。计算、派发、控制线程池及扩展函数未重构，未预启动全部工作线程；本轮只隔离接纳队列的效果。第一轮后补齐了resumable初次提交失败的退出通知，该修复不经过本实验native正常执行路径；两份源代码清单和首轮peer.py均保存。','',
      '```yaml','peer:','  admission:','    mode: buffered   # direct为同步提交对照','    queue_capacity: 128','```','',
      '旧YAML缺少整个admission组时显式迁移为direct，避免历史配置悄然改变行为。接纳确认表示进程内任务已被接纳，不代表计算完成或持久化；后台启动失败仍通过原控制通道回报错误，入口必须等全部分支结果。','',
      '## 两轮端到端结果','']
    ordered=sorted(rows,key=lambda x:(x['round'],x['mode']))
    lines+=table(['轮次','模式','成功/失败','实际并发峰值','QPS','平均ms','p99 ms','Recall@10'],[[x['round'],x['mode'],f"{x['success']}/{x['failures']}",x['peak_inflight'],f"{x['qps']:.2f}",f"{x['mean_ms']:.2f}",f"{x['p99_ms']:.2f}",f"{x['recall']:.3%}"] for x in ordered])
    lines+=['','## 等待发生在哪里','',
      '下表是每查询跨分支累计均值。admission与executor-start是ready的细分，finalize与transfer队列/执行重叠，所有列不可直接相加为端到端延迟。','']
    lines+=table(['轮次','模式','派发队列ms','派发响应阶段ms','派发收尾ms','接纳至提交ms','提交至初次执行ms','ready总ms'],[[x['round'],x['mode'],*[f'{x[k]:.3f}' for k in ('peer_transfer_queue_ms','peer_response_wait_ms','query_transfer_finalize_wait_ms','query_admission_queue_ms','query_executor_start_wait_ms','query_ready_queue_ms')]] for x in ordered])
    lines+=['',f"最终第二轮：派发收尾 {a['query_transfer_finalize_wait_ms']:.3f}→{b['query_transfer_finalize_wait_ms']:.3f}ms，目标端ready {a['query_ready_queue_ms']:.3f}→{b['query_ready_queue_ms']:.3f}ms，平均延迟 {a['mean_ms']:.3f}→{b['mean_ms']:.3f}ms，p99 {a['p99_ms']:.3f}→{b['p99_ms']:.3f}ms。",'',
      '接纳确认与计算启动解耦后，源端可以更早归还连接和释放派发线程，从而降低后续派发任务排队。但计算任务没有消失：独立接纳线程仍调用现有ThreadPoolExecutor.submit，仍受提交锁、按需建线程及Python调度约束，等待可以转移到目标端。第一轮平均延迟上升，尽管p99和QPS小幅改善；不能仅以源端等待下降宣称全部查询更快。两轮短窗口不提供统计显著性或最优配置结论。','',
      'peer_response_wait_ms是源端发送完成到接收/解码响应的累计墙钟时间，包含目标处理及双方调度，不是纯网络ACK延迟。新接纳队列的目的已由阻塞注入测试确认，但实际吞吐仍取决于计算启动和控制回传路径。','',
      '## 节点负载、容量与正确性','']
    node_rows=[]
    for x in ordered:
        for n in x['nodes']:node_rows.append([x['round'],x['mode'],n['node'],f"{n['cpu_cores']:.3f}",n['threads_after'],f"{n['rss_gib']:.3f}",n['pending_peak_since_start']])
    lines+=table(['轮次','模式','节点','平均CPU核当量','结束线程数','RSS GiB','待初次执行峰值*'],node_rows)
    lines+=['','*峰值包含服务预热，direct不维护该容量计数；direct为0不表示没有排队。测量结束时所有模式pending、接纳deque及未确认分支均为0；没有容量拒绝或控制发送错误。物理读取增量均为0，符合热页缓存范围。','',
      '审计8000条记录的唯一身份、冻结查询集合、原始JSONL一致性、召回重算、预算≤200、父子预算守恒、每个远端分支有批准决策、无节点回访、无顶层扩展RPC、固定原生线程，以及ready等于两个新增细分项之和。未在此轮重新计算全库ground truth。所有客户端和数据服务均已停止。','',
      '## 后续选择','',
      '保留direct/buffered切换。队列容量应随接纳上限和延迟目标校准，不能无限增大；下一步优先测计算任务提交的锁/建线程等待，并验证固定工作线程提前就绪或专用计算任务队列，避免单纯把等待转移到后台。按用户要求继续以平均Recall@10≥.95、无失败为门槛，优先p99、再比较QPS，同时公开平均延迟退化。','',
      '- [结果JSON](comparison.json)、[CSV](comparison.csv)、[审计](verification.json)、[回归日志](regression_final.log)。',
      '- [buffered配置](buffered/experiment.yaml)、[direct配置](direct/experiment.yaml)、[负载配置](buffered/load_plan.yaml)。',
      '- [首轮代码清单](sources.json)、[最终代码清单](sources_final.json)、[图表SVG](comparison.svg)。']
    for x in ordered:
        rel=x['run'];lines.append(f"- 第{x['round']}轮{x['mode']}：[汇总]({rel}/summary.json)、[原始查询]({rel}/queries.jsonl.gz)、[CSV]({rel}/queries.csv)、[阶段均值]({rel}/query_stage_means.csv)、[资源采样]({rel}/resource_samples.json.gz)。")
    lines+=['','两个模式共享端口，必须顺序运行各自run_load.py。prepare.py拒绝覆盖已执行目录；analyze.py读取已冻结的两轮记录重新生成报告。大体积原始记录保留本机，Git保留代码、配置及紧凑结果。','']
    (R/'REPORT.md').write_text('\n'.join(lines))
    save(R/'artifact_manifest.json',{str(p.relative_to(R)):hashlib.sha256(p.read_bytes()).hexdigest() for p in R.rglob('*') if p.is_file() and not p.is_symlink() and not {'before','.mplconfig','__pycache__'}.intersection(p.parts) and p.name not in ('artifact_manifest.json','analysis.log')})
    print(json.dumps([{k:x[k] for k in ('round','mode','qps','mean_ms','p99_ms','recall','query_transfer_finalize_wait_ms','query_ready_queue_ms')} for x in ordered],ensure_ascii=False))
if __name__=='__main__':main()
