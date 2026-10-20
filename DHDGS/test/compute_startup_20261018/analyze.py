"""Audit all completed startup ablations and keep critical-path wait distinct."""
from pathlib import Path
import csv,gzip,json,os
import numpy as np
import yaml
R=Path(__file__).resolve().parent
os.environ.setdefault('MPLCONFIGDIR',str(R/'.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'font.weight':500,'axes.titleweight':500,
                    'axes.labelweight':500,'axes.unicode_minus':False,'svg.fonttype':'path'})
def save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False))
def table(headers,rows):return ['| '+' | '.join(headers)+' |','|'+'|'.join(['---']*len(headers))+'|']+['| '+' | '.join(map(str,row))+' |' for row in rows]
def csvfile(path,rows):
    with path.open('w') as f:
        w=csv.DictWriter(f,list(rows[0]));w.writeheader();w.writerows(rows)
def audit(source):
    run=source.parent;group=run.parent.parent;cfg=yaml.safe_load((run/'resolved.yaml').read_text())
    with gzip.open(source,'rt') as f:r=json.load(f)
    qs=r['queries'];selection=json.loads((group/'query_selection.json').read_text())
    assert len(qs)==len({q['request_id'] for q in qs})==2000
    assert sorted(q['query_row'] for q in qs)==selection['validation']
    assert not set(selection['validation'])&set(selection['calibration_excluded'])
    ts={};events=[]
    for n in r['node_process_load']:
        life=n['peer_lifecycle_after'];assert life['admission_pending_start']==life['admission_queue_depth']==life['unacknowledged']==0
        assert life['admission_capacity_rejections']==life['control_totals']['errors']==0
        for m in (life.get('send_queues') or {}).values():assert m['queued']==m['active']==m['capacity_rejections']==0
        if life.get('compute_queue'):
            m=life['compute_queue'];assert m['queued']==m['active']==m['capacity_rejections']==m['callback_errors']==0
        for t in life['execution_timestamps']:ts[t['branch_id']]=t
        events.extend(life.get('control_events',[]))
    ev={}
    for e in events:ev.setdefault(e['query_request_id'],[]).append(e)
    branches=[];paths=[]
    for q in qs:
        assert q['status']=='ok' and q['stats']['metrics_complete']
        assert q['recall']==len(set(q['ids'])&set(q['truth_ids']))/10
        s=q['stats'];assert s['expanded']<=200 and s['node_revisit_count']==s['router_expand_rpc_count']==0
        for b in q['branch_reports']:
            t=ts[b['branch_id']]
            assert t['query_request_id']==q['request_id'] and t['worker_thread_id']==b['compute_thread_id'] and b['compute_thread_migrations']==0
            assert b['work_grant']==b['work_spent']+b['work_unused']+sum(c['work_grant'] for c in b['children'])
            for c in b['children']:assert sum(d['remote'] and d['decision_id']==c['decision_id'] and d['selected_node']==c['node_id'] for d in b['decisions'])==1
            chain=[t[k] for k in ('enqueued_ns','dequeued_ns','submit_enter_ns','worker_enter_ns','native_search_start_ns','native_return_ns')]
            assert chain==sorted(chain)
            if 'executor_enqueue_ns' in t:assert t['submit_enter_ns']<=t['executor_enqueue_ns']<=t['executor_dequeue_ns']<=t['worker_enter_ns']
            branches.append({'branch_id':b['branch_id'],'request_id':q['request_id'],'node':b['node_id'],
                'admission_queue_ms':(t['dequeued_ns']-t['enqueued_ns'])/1e6,
                'submit_ms':(t['submit_return_ns']-t['submit_enter_ns'])/1e6,
                'submit_cpu_ms':(t['submit_cpu_return_ns']-t['submit_cpu_enter_ns'])/1e6,
                'accepted_to_worker_ms':(t['worker_enter_ns']-t['accepted_ns'])/1e6,
                'submit_to_worker_ms':(t['worker_enter_ns']-t['submit_enter_ns'])/1e6,
                'worker_to_cpp_ms':(t['native_search_start_ns']-t['worker_enter_ns'])/1e6,
                'created_thread':t.get('executor_workers_after',0)>t.get('executor_workers_before',0),
                'compute_queue_ms':(t['executor_dequeue_ns']-t['executor_enqueue_ns'])/1e6 if 'executor_enqueue_ns' in t else None})
        bt=[ts[b['branch_id']] for b in q['branch_reports']]
        root=next(t for t in bt if t['node_id']==s['visited_nodes'][0])
        ready=max(t['report_ready_ns'] for t in bt)
        result_events=[e for e in ev[q['request_id']] if e['kind']=='result']
        sent_start=max(e['sender_enter_ns'] for e in result_events)
        assert q['client_start_ns']<=root['accepted_ns']<=ready<=sent_start<=q['client_end_ns']
        paths.append({'request_id':q['request_id'],'query_id':q['query_id'],
            'entry_accept_ms':(root['accepted_ns']-q['client_start_ns'])/1e6,
            'accept_to_all_report_ready_ms':(ready-root['accepted_ns'])/1e6,
            'all_report_ready_to_last_sender_ms':(sent_start-ready)/1e6,
            'last_sender_to_api_return_ms':(q['client_end_ns']-sent_start)/1e6})
    services=json.loads((run/'services.json').read_text());clients=json.loads((run/'clients_lifecycle.json').read_text())
    assert services['test_completed'] and all(s['stopped'] for s in services['services'])
    assert len(clients)==20 and all(c['stopped'] and c['exit_code']==0 for c in clients)
    out=R/'analysis'/group.name/run.name;out.mkdir(parents=True,exist_ok=True)
    csvfile(out/'branches.csv',branches);csvfile(out/'critical_path.csv',paths)
    row={'group':group.name,'run':run.name,'compute_mode':cfg['peer']['compute']['mode'],
        'workers':cfg['peer']['worker_threads'],'pool_mode':cfg['peer'].get('connection_pool',{}).get('probe_mode','locked'),
        'control_workers':cfg['peer']['communication']['control_workers'],
        'initial_heartbeat_delay_ms':cfg['peer'].get('control',{}).get('initial_heartbeat_delay_ms',0),
        'success':r['successful_queries'],'failed':r['failed_queries'],'recall':r['recall_at_k_all_requests'],
        'qps':r['qps'],'mean_ms':r['latency_ms']['mean'],'p99_ms':r['latency_ms']['p99'],
        'startup_seconds':r['local_cluster']['startup_seconds'],'branches':len(branches),
        'mean_cpu_cores':sum(n['mean_cpu_cores'] for n in r['node_process_load']),
        'physical_read_bytes':sum(n['process_deltas']['storage_read_bytes'] for n in r['node_process_load']),
        'created_threads_during_queries':sum(b['created_thread'] for b in branches)}
    for k in ('admission_queue_ms','submit_ms','submit_cpu_ms','accepted_to_worker_ms','submit_to_worker_ms','worker_to_cpp_ms'):
        row[k]=float(np.mean([b[k] for b in branches]))
    for k in ('entry_accept_ms','accept_to_all_report_ready_ms','all_report_ready_to_last_sender_ms','last_sender_to_api_return_ms'):
        row[k]=float(np.mean([p[k] for p in paths]))
    for k in ('query_transfer_finalize_wait_ms','query_compute_slice_wall_ms','query_ready_queue_ms','all_protocol_bytes','all_protocol_rpc_count'):
        row[k]=float(np.mean([q['stats'][k] for q in qs]))
    query_ids={q['request_id'] for q in qs}
    measured_events=[e for e in events if e['query_request_id'] in query_ids]
    result_events=[e for e in measured_events if e['kind']=='result']
    row['heartbeat_count']=sum(e['kind']=='heartbeat' for e in measured_events)
    row['result_count']=len(result_events)
    row['result_sender_wall_ms']=float(np.mean([(e['sender_rpc_return_ns']-e['sender_enter_ns'])/1e6 for e in result_events]))
    for k in ('pool_wait_ms','connect_ms','send_ms','response_wait_ms'):
        row['result_'+k]=float(np.mean([e['rpc_counts'].get(k,0) for e in result_events]))
    chosen=next(q for q in qs if q['query_id']==734744)
    save(out/'selected_query.json',chosen)
    save(out/'selected_events.json',{'startup':[ts[b['branch_id']] for b in chosen['branch_reports']],
        'control':ev[chosen['request_id']]})
    save(out/'verification.json',{'queries':2000,'branches':len(branches),'all_successful':True,
        'timestamps_budget_fixed_thread_approved_dispatch_verified':True,'all_test_processes_stopped':True})
    return row
def main():
    sources=sorted(R.glob('*/runs/*/report.json.gz'))
    rows=[audit(p) for p in sources]
    save(R/'comparison.json',rows);csvfile(R/'comparison.csv',rows)
    save(R/'verification.json',{'runs':len(rows),'queries':sum(r['success'] for r in rows),
        'failures':sum(r['failed'] for r in rows),'all_run_audits_passed':True})
    fig,axes=plt.subplots(2,2,figsize=(15,10),layout='constrained')
    labels=[r['group']+'\n'+r['run'][-6:] for r in rows]
    for ax,(key,label) in zip(axes.flat,[('p99_ms','p99 / ms'),('qps','QPS'),
            ('accepted_to_worker_ms','接纳至worker / ms 每分支'),('all_report_ready_to_last_sender_ms','全部报告就绪至最后发送开始 / ms 每query')]):
        ax.bar(range(len(rows)),[r[key] for r in rows],color=['#238f82' if r['pool_mode']=='reserved' else '#7794ac' for r in rows])
        ax.set_xticks(range(len(rows)),labels,rotation=70,ha='right',fontsize=8);ax.set_title(label);ax.grid(axis='y',alpha=.2)
    fig.suptitle('计算启动与连接池消融：必须同时看局部等待和端到端延迟\n完整2M · 本机三节点进程 · 每组2000查询 · 预算200 · 热缓存')
    for ext in ('png','svg'):fig.savefig(R/f'comparison.{ext}',dpi=150)
    plt.close(fig)
    lines=['# 等待瓶颈与计算启动优化实验','',
        '全部组使用同一2000查询集合、完整seq_2M索引、native搜索预算200、queued独立发送队列；本机三TCP数据进程、客户端并发上限2000、40次预热排除。搜索扩展决策、预算、每节点单次访问和固定分支线程语义保持一致。所有实验在原索引上复测，不重建索引。','',
        '![对照结果](comparison.png)','',
        '## 所有运行','']
    lines+=table(['配置','轮次','成功','Recall@10','QPS','平均ms','p99 ms'],[
        [r['group'],r['run'][-6:],r['success'],f"{r['recall']:.3%}",f"{r['qps']:.2f}",f"{r['mean_ms']:.2f}",f"{r['p99_ms']:.2f}"] for r in rows])
    lines+=['','## 启动等待（分支均值）','']
    lines+=table(['配置','轮次','建线程次数','submit ms','接纳至worker ms','worker至C++ ms','服务启动s'],[
        [r['group'],r['run'][-6:],r['created_threads_during_queries'],f"{r['submit_ms']:.4f}",f"{r['accepted_to_worker_ms']:.2f}",f"{r['worker_to_cpp_ms']:.2f}",f"{r['startup_seconds']:.2f}"] for r in rows])
    lines+=['','## 端到端时间的互斥分组（query均值）','',
        '用同一单调时钟域的绝对事件切分：客户端开始→根分支接纳→全部分支报告就绪→最后一个结果发送函数开始→API返回。四项之和等于平均查询延迟，不能将其他累计分支耗时再加进来。','']
    lines+=table(['配置','轮次','入口到接纳ms','接纳至报告齐备ms','报告齐备至发送ms','发送开始至返回ms'],[
        [r['group'],r['run'][-6:],*[f"{r[k]:.2f}" for k in ('entry_accept_ms','accept_to_all_report_ready_ms','all_report_ready_to_last_sender_ms','last_sender_to_api_return_ms')]] for r in rows])
    lines+=['','## 结果RPC阶段（每次结果发送均值）','',
        'pool_wait包含借用、容量等待与空闲连接健康探测及调度，不是纯容量等待；send/response也不是纯网络时间。','']
    lines+=table(['配置','轮次','pool ms','connect ms','send ms','response ms'],[
        [r['group'],r['run'][-6:],*[f"{r['result_'+k]:.3f}" for k in ('pool_wait_ms','connect_ms','send_ms','response_wait_ms')]] for r in rows])
    lines+=['','所有运行的逐分支、关键路径CSV、固定查询734744原始事件及审计保存在analysis/配置/运行目录。',
        '资源使用、协议字节、服务启动耗时与物理读取增量见comparison.json；完整queries.csv、阶段均值、进程负载见各组runs目录。',
        '本轮是热页缓存突发负载，不代表物理三机、冷SSD或稳态容量；详细trace开销未单独消融。预启动把线程创建成本移至服务启动，不是将其消除。','']
    (R/'REPORT.md').write_text('\n'.join(lines))
    print(json.dumps([{k:r[k] for k in ('group','run','qps','p99_ms','accepted_to_worker_ms','all_report_ready_to_last_sender_ms')} for r in rows]))
if __name__=='__main__':main()
