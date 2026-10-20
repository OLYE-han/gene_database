"""Join node diagnostics to measured queries and audit submission bottlenecks."""
from pathlib import Path
import csv
import gzip
import hashlib
import json
import math
import os
import random
import numpy as np
import yaml

R = Path(__file__).resolve().parent
P = R.parents[1]
os.environ.setdefault('MPLCONFIGDIR', str(R / '.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
font = FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(), 'font.weight':500,
                    'axes.titleweight':500, 'axes.labelweight':500,
                    'axes.unicode_minus':False, 'svg.fonttype':'path'})


def save(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False))


def write_csv(path, rows):
    with path.open('w') as f:
        w = csv.DictWriter(f, list(rows[0]))
        w.writeheader();w.writerows(rows)


def table(headers, rows):
    return ['| '+' | '.join(headers)+' |', '|'+'|'.join(['---']*len(headers))+'|'] + [
        '| '+' | '.join(map(str,r))+' |' for r in rows]


def ms(t, a, z):
    return (t[z]-t[a])/1e6


def audit(mode, run_name='*', label=None):
    config_group=mode
    files = list((R/mode/'runs').glob(run_name+'/report.json.gz'))
    assert len(files)==1
    mode=label or mode
    run = files[0].parent
    with gzip.open(files[0], 'rt') as f:r=json.load(f)
    qs=r['queries'];selection=json.loads((R/config_group/'query_selection.json').read_text())
    assert len(qs)==len({q['request_id'] for q in qs})==2000
    assert sorted(q['query_row'] for q in qs)==selection['validation']
    assert not set(selection['validation'])&set(selection['calibration_excluded'])
    qids={q['request_id'] for q in qs}
    nodes=json.loads((run/'nodes_after.json').read_text())
    all_traces={};control=[]
    for node in nodes.values():
        diag=node['peer_lifecycle']
        assert diag['admission_pending_start']==diag['admission_queue_depth']==diag['unacknowledged']==0
        assert diag['admission_capacity_rejections']==diag['control_totals']['errors']==0
        for kind,m in (diag.get('send_queues') or {}).items():
            assert m['capacity_rejections']==m['queued']==m['active']==0,(kind,m)
        for t in diag['execution_timestamps']:all_traces[t['branch_id']]=t
        control.extend(diag.get('control_events',[]))
    submissions={sid:[] for sid in nodes}
    for t in all_traces.values():
        if 'submit_return_ns' in t:
            submissions[str(t['node_id'])].append((t['submit_enter_ns'],t['submit_return_ns']))
    for intervals in submissions.values():
        intervals.sort()
        assert all(a[1]<=b[0] for a,b in zip(intervals,intervals[1:]))
    rows=[]
    for q in qs:
        s=q['stats']
        assert q['status']=='ok' and s['metrics_complete']
        assert q['recall']==len(set(q['ids'])&set(q['truth_ids']))/10
        assert s['expanded']<=200 and s['node_revisit_count']==s['router_expand_rpc_count']==0
        for b in q['branch_reports']:
            t=all_traces[b['branch_id']]
            assert t['query_request_id']==q['request_id'] and b['node_id']==t['node_id']
            chain=['accepted_ns','enqueued_ns','dequeued_ns','submit_state_lock_enter_ns',
                   'submit_state_lock_acquired_ns','submit_enter_ns','worker_enter_ns',
                   'worker_state_lock_acquired_ns','native_call_ns','native_entry_ns',
                   'native_search_start_ns','native_return_ns']
            assert [t[k] for k in chain]==sorted(t[k] for k in chain)
            assert t['submit_return_ns']>=t['submit_enter_ns']
            assert b['compute_thread_id']==t['worker_thread_id'] and b['compute_thread_migrations']==0
            assert b['work_grant']==b['work_spent']+b['work_unused']+sum(c['work_grant'] for c in b['children'])
            for child in b['children']:
                assert sum(d['remote'] and d['decision_id']==child['decision_id'] and d['selected_node']==child['node_id']
                           for d in b['decisions'])==1
            if 'transfer_thread_id' in t:assert t['transfer_thread_id']!=t['worker_thread_id']
            assert t.get('receiver_thread_id')!=t['worker_thread_id']
            wait_start,wait_end=t['enqueued_ns'],t['dequeued_ns']
            covered=sum(max(0,min(wait_end,z)-max(wait_start,a)) for a,z in submissions[str(t['node_id'])])/1e6
            row={'mode':mode,'query_request_id':q['request_id'],'branch_id':b['branch_id'],'query_id':q['query_id'],
                 'node':t['node_id'],'queue_ms':ms(t,'enqueued_ns','dequeued_ns'),
                 'queue_covered_by_prior_submit_ms':covered,
                 'dequeue_to_submit_ms':ms(t,'dequeued_ns','submit_enter_ns'),
                 'state_lock_ms':ms(t,'submit_state_lock_enter_ns','submit_state_lock_acquired_ns'),
                 'submit_wall_ms':ms(t,'submit_enter_ns','submit_return_ns'),
                 'submit_cpu_ms':ms(t,'submit_cpu_enter_ns','submit_cpu_return_ns'),
                 'submit_to_worker_ms':ms(t,'submit_enter_ns','worker_enter_ns'),
                 'worker_to_cpp_ms':ms(t,'worker_enter_ns','native_search_start_ns'),
                 'worker_state_entry_ms':ms(t,'worker_enter_ns','worker_state_lock_acquired_ns'),
                 'worker_before_submit_return':t['worker_enter_ns']<t['submit_return_ns']}
            if 'executor_adjust_enter_ns' in t:
                assert t['submit_enter_ns']<=t['executor_adjust_enter_ns']<=t['executor_adjust_return_ns']<=t['submit_return_ns']
                row.update(before_adjust_ms=ms(t,'submit_enter_ns','executor_adjust_enter_ns'),
                           adjust_ms=ms(t,'executor_adjust_enter_ns','executor_adjust_return_ns'),
                           created_worker=t['executor_workers_after']>t['executor_workers_before'],
                           transfer_enqueue_ms=(ms(t,'transfer_enqueue_enter_ns','transfer_enqueue_return_ns')
                                                if 'transfer_enqueue_enter_ns' in t else None))
            assert covered<=row['queue_ms']+1e-6
            rows.append(row)
    for event in control:
        if event['query_request_id'] in qids:
            assert event['sender_thread_id']!=all_traces[event['branch_id']]['worker_thread_id']
    clients=json.loads((run/'clients_lifecycle.json').read_text())
    services=json.loads((run/'services.json').read_text())
    assert len(clients)==20 and all(c['stopped'] and c['exit_code']==0 for c in clients)
    assert services['test_completed'] and len(services['services'])==3 and all(c['stopped'] for c in services['services'])
    write_csv(R/(mode+'_branches.csv'),rows)
    node_stats=[]
    for sid in nodes:
        group=[x for x in rows if x['node']==int(sid)]
        summary={'node':int(sid),'branches':len(group)}
        for key in ('queue_ms','dequeue_to_submit_ms','state_lock_ms','submit_wall_ms','submit_cpu_ms',
                    'submit_to_worker_ms','worker_to_cpp_ms','worker_state_entry_ms','before_adjust_ms','adjust_ms'):
            if key not in group[0]:continue
            values=[x[key] for x in group]
            summary[key]={'mean':float(np.mean(values)),'p99':float(np.percentile(values,99)),'max':max(values)}
        summary['queue_covered_by_prior_submit_fraction']=sum(x['queue_covered_by_prior_submit_ms'] for x in group)/sum(x['queue_ms'] for x in group)
        summary['worker_before_submit_return_count']=sum(x['worker_before_submit_return'] for x in group)
        if 'created_worker' in group[0]:
            summary['adjust_by_creation']={str(created):{
                'count':sum(x['created_worker']==created for x in group),
                **{key:float(np.mean([x[key] for x in group if x['created_worker']==created]))
                   for key in ('submit_wall_ms','before_adjust_ms','adjust_ms')}} for created in (True,False)}
        node_stats.append(summary)
    summary={'mode':mode,'config_group':config_group,'run':str(run.relative_to(R)),'qps':r['qps'],'mean_ms':r['latency_ms']['mean'],
             'p99_ms':r['latency_ms']['p99'],'recall':r['recall_at_k_all_requests'],
             'success':r['successful_queries'],'failed':r['failed_queries'],'nodes':node_stats,
             'mean_cpu_cores':sum(n['mean_cpu_cores'] for n in r['node_process_load']),
             'physical_read_bytes':sum(n['process_deltas']['storage_read_bytes'] for n in r['node_process_load'])}
    if 'transfer_enqueue_ms' in rows[0]:
        values=[x['transfer_enqueue_ms'] for x in rows if x['transfer_enqueue_ms'] is not None]
        summary['transfer_enqueue_ms']={'count':len(values),'mean':float(np.mean(values)),
                                       'p99':float(np.percentile(values,99)),'max':max(values)}
    return summary,r,all_traces,control


def plot_query(mode,q,traces,control):
    origin=q['client_start_ns'];end=q['client_wall_ms']
    branches=sorted(q['branch_reports'],key=lambda b:len(b['visited_nodes']))
    fig,axes=plt.subplots(len(branches),1,figsize=(14,3*len(branches)+1),squeeze=False,layout='constrained')
    intervals=[('enqueued_ns','dequeued_ns','接纳队列','#bf863e'),
               ('dequeued_ns','submit_enter_ns','出队至提交','#a9acb1'),
               ('submit_enter_ns','worker_enter_ns','提交至worker入口','#6997bd'),
               ('worker_enter_ns','native_search_start_ns','worker至C++开始','#966d9f'),
               ('native_search_start_ns','native_return_ns','原生调用搜索及返回','#218e7e')]
    for index,b in enumerate(branches):
        ax=axes[index,0];t=traces[b['branch_id']]
        for a,z,label,color in intervals:
            ax.broken_barh([((t[a]-origin)/1e6,ms(t,a,z))],(.1,.6),facecolors=color,label=label)
        ax.broken_barh([((t['submit_enter_ns']-origin)/1e6,ms(t,'submit_enter_ns','submit_return_ns'))],(1.1,.6),facecolors='#c86b64',label='submit调用（可与worker重叠）')
        for e in control:
            if e['branch_id']==b['branch_id'] and e['kind']=='result' and 'sender_rpc_return_ns' in e:
                ax.broken_barh([((e['sender_enter_ns']-origin)/1e6,ms(e,'sender_enter_ns','sender_rpc_return_ns'))],(2.1,.6),facecolors='#605c8a',label='结果发送函数（包含ACK）')
        ax.axvline(end,color='black',linestyle='--',linewidth=1)
        ax.set_yticks([.4,1.4,2.4],['启动及计算','接纳线程submit','结果发送线程'])
        ax.set_xlim(0,max(end,max((e.get('sender_rpc_return_ns',origin)-origin)/1e6 for e in control if e['query_request_id']==q['request_id']))*1.03)
        ax.set_ylim(-.1,3)
        ax.set_title(f"节点{b['node_id']}：入队后等待 {ms(t,'enqueued_ns','dequeued_ns'):.2f} ms；submit {ms(t,'submit_enter_ns','submit_return_ns'):.2f} ms",loc='left')
        ax.set_xlabel('相对客户端提交的实测毫秒；虚线=API返回；不同泳道可重叠')
    handles,labels=axes[-1,0].get_legend_handles_labels()
    fig.legend(handles,labels,loc='outside lower center',ncol=3,fontsize=9)
    fig.suptitle(f"{mode}：query {q['query_id']}，总延迟 {end:.2f} ms\n空白为未绘制区间，不能解释为CPU空闲；结果ACK可能晚于API返回",fontsize=13)
    for ext in ('png','svg'):fig.savefig(R/f'{mode}_query.{ext}',dpi=150)
    plt.close(fig)


def main():
    for manifest,base in [('sources.json',R/'source_snapshot'),('sources_senders.json',R/'sender_source_snapshot')]:
        for name,digest in json.loads((R/manifest).read_text()).items():
            assert hashlib.sha256((base/name).read_bytes()).hexdigest()==digest,name
    data={'baseline_1':audit('buffered','20261018T134609','baseline_1'),
          'baseline_2':audit('buffered','20261018T134716','baseline_2'),
          **{mode:audit(mode) for mode in ('sender_executor','sender_queued')}}
    rows=[v[0] for v in data.values()];save(R/'comparison.json',rows)
    save(R/'verification.json',{'requests':8000,'all_successful':True,'hashes_verified':True,
        'timestamps_and_budgets_verified':True,'tcp_sender_thread_separation_verified':True,
        'all_test_processes_stopped':True,'pending_at_end':0,'queue_rejections':0})
    params=yaml.safe_load((R/'analysis.yaml').read_text())
    candidates=sorted([q for q in data['baseline_2'][1]['queries'] if len(q['branch_reports'])>1],key=lambda q:q['query_row'])
    selected=random.Random(params['selection_seed']).choice(candidates)
    save(R/'selection.json',{'seed':params['selection_seed'],'population':'initial instrumented baseline multi-node queries, sorted by query_row',
        'eligible':len(candidates),'query_id':selected['query_id'],'query_row':selected['query_row']})
    for mode in ('sender_executor','sender_queued'):
        _,r,ts,events=data[mode]
        q=next(q for q in r['queries'] if q['query_row']==selected['query_row'])
        save(R/(mode+'_selected_query.json'),q)
        ids={b['branch_id'] for b in q['branch_reports']}
        save(R/(mode+'_selected_events.json'),{'startup':[t for key,t in ts.items() if key in ids],
                                              'control':[e for e in events if e['branch_id'] in ids]})
        plot_query(mode,q,ts,events)
    fig,axes=plt.subplots(1,3,figsize=(13,4.5),layout='constrained')
    for ax,key,label in zip(axes,('queue_ms','submit_wall_ms','worker_to_cpp_ms'),('接纳队列驻留','submit调用墙钟','worker入口到C++开始')):
        for mode,offset,color in [('sender_executor',-.18,'#7890a5'),('sender_queued',.18,'#218e7e')]:
            vals=[n[key]['mean'] for n in data[mode][0]['nodes']]
            bars=ax.bar(np.arange(3)+offset,vals,.34,label=mode,color=color);ax.bar_label(bars,fmt='%.2f',padding=3,fontsize=9)
        ax.set_xticks([0,1,2],['节点0','节点1','节点2']);ax.set_title(label);ax.set_ylabel('平均ms/分支');ax.margins(y=.25)
    axes[0].legend(fontsize=8)
    fig.suptitle('同代码发送路径对照：启动等待分解（完整2M、本机三进程、每组2000查询）')
    for ext in ('png','svg'):fig.savefig(R/f'comparison.{ext}',dpi=150)
    plt.close(fig)
    lines=['# 启动时间戳诊断与通信发送解耦实验','',
        '先运行仅增加六阶段时间戳的现有实现，再用同一最终代码对比executor发送线程池与queued预启动发送队列。生产计算线程池未替换；没有改变扩展函数、搜索预算或每分支固定计算线程语义。', '',
        '## 实验条件与总体结果','',
        '完整seq_2M（2M×2560），raw L2、1-bit RaBitQ、top10排除自身，预算200/frontier64/localbeam4；Intel-35本机三个TCP数据进程共享CPU/SSD。每节点计算线程上限256，派发16、控制16、接纳上限2048；queued远端等待队列2048，control排队+在途上限64。20客户端进程×100线程，每组2000条相同冻结验证查询，40次预热不计入结果；排除128条历史校准查询。每组为一次突发并排空，不代表稳态容量；只有一个顺序对照，不能声明统计显著性或最优参数。','',
        '同步buffered pread、应用缓存0、详细trace开启；物理读取增量0，属热页缓存。复用既有全库真值缓存，没有重复扫描全库。','']
    lines+=table(['组','成功/失败','Recall@10','QPS','平均ms','p99 ms'],[[r['mode'],f"{r['success']}/{r['failed']}",f"{r['recall']:.3%}",f"{r['qps']:.2f}",f"{r['mean_ms']:.2f}",f"{r['p99_ms']:.2f}"] for r in rows])
    lines+=['','baseline_1是上一条被中断工作已启动并完成的测试，baseline_2为本轮诊断复测；两次均保留并重新审计。sender_executor与sender_queued才是同代码、同插桩的发送模式对照。','',
            '## 启动等待实际发生在哪里','', '![启动等待对照](comparison.png)','',
            '以下均为测量查询的分支平均值；不含预热。队列驻留使用真实enqueue/dequeue，submit墙钟与submit到worker可重叠，不能相加。','']
    lines+=table(['组','节点','分支数','队列ms','出队至submit ms','submit墙钟ms','submit CPU ms','submit→worker ms','worker→C++ ms','队列等待被前序submit覆盖'],[
        [r['mode'],n['node'],n['branches'],*[f"{n[k]['mean']:.3f}" for k in ('queue_ms','dequeue_to_submit_ms','submit_wall_ms','submit_cpu_ms','submit_to_worker_ms','worker_to_cpp_ms')],f"{n['queue_covered_by_prior_submit_fraction']:.2%}"] for r in rows for n in r['nodes']])
    lines+=['','每个接纳线程逐个submit，一次submit迟迟未返回会阻止后续出队。将每个任务的enqueue→dequeue区间与同节点前序submit区间求交，可量化这种阻挡。交集是多个查询的等待累计，不是CPU利用率；未被覆盖的部分包括接纳线程调度、队列状态锁及其他处理。','',
            '## submit内部与按需建线程','',
            '通过覆盖_adjust_thread_count并调用标准库原实现记录边界；未重写submit。进入adjust前已完成线程池提交锁获取、Future/工作项创建及入队，所以这一差值不能全部命名为“锁等待”。adjust中可能检查空闲线程或新建线程，按工作线程数是否增加分组。','']
    lines+=table(['组','节点','创建线程?','次数','submit均值ms','进入adjust前ms','adjust区间ms'],[
        [r['mode'],n['node'],flag,g['count'],f"{g['submit_wall_ms']:.3f}",f"{g['before_adjust_ms']:.3f}",f"{g['adjust_ms']:.3f}"]
        for r in rows[2:] for n in r['nodes'] for flag,g in n['adjust_by_creation'].items()])
    lines+=['','submit线程CPU明显小于墙钟只能说明存在非CPU执行时间，尚不能把差额严格分配给GIL、标准库锁、线程启动握手或OS调度。最终因果判断需要这些机制的独立消融；当前证据已足以把主要启动瓶颈定位到串行提交路径。','',
            '## 计算线程与TCP收发隔离','',
            '接收线程处理来自入口/数据节点的请求和ACK；transfer发送线程处理扩展函数批准的搜索请求；control发送线程处理心跳、完成结果及错误回报。queued模式启动服务前创建发送线程，计算线程只进行短临界区入队，不调用发送线程池submit、不执行TCP、不等待ACK。结果与远端派发分别排队，不用一个阻塞发送线程串行处理所有连接。','']
    lines+=['新模式仍有阶段退化：节点0的worker入口到C++开始从约0.35ms增加到24.83ms。下面将进入查询状态临界区前的等待单独列出，包含获取运行机会和获得状态锁，不是已经测得的纯锁等待。','']
    lines+=table(['模式','节点','worker进入状态临界区平均ms','p99 ms'],[
        [r['mode'],n['node'],f"{n['worker_state_entry_ms']['mean']:.3f}",f"{n['worker_state_entry_ms']['p99']:.3f}"]
        for r in rows[2:] for n in r['nodes']])
    lines+=['','因此不能宣称所有等待都消失；后续计算工作线程预启动及队列/状态锁隔离需要独立实验。','']
    lines+=table(['模式','远端任务数','入队调用平均ms','p99 ms','最大ms'],[[r['mode'],r['transfer_enqueue_ms']['count'],*[f"{r['transfer_enqueue_ms'][k]:.3f}" for k in ('mean','p99','max')]] for r in rows[2:]])
    lines+=['','入队仍需要Python执行和短队列锁，并不承诺零等待。网络回复仍由接收连接线程发送，属于通信线程而非计算线程；入口首次派发在路由调用者执行，也不属于数据节点计算线程。legacy模式保留历史对照语义。','',
            '## 固定查询的绝对时间线','',
            f"初始基线从{len(candidates)}条多节点查询按固定种子{params['selection_seed']}随机抽取query {selected['query_id']}（row={selected['query_row']}），两种发送模式展示同一数据query的新执行。",'',
            '![旧发送提交](sender_executor_query.png)','', '![预启动发送队列](sender_queued_query.png)','',
            '所有绝对时间只在本机共同单调时钟域对齐；submit返回可能晚于worker开始。结果发送泳道包含等待入口ACK，API返回可早于发送方收到ACK。没有将不同泳道相加为总延迟。入口接收与调用线程唤醒的逐事件链仍未全部插桩，空白不等于CPU空闲。','',
            '## 验证与复现','',
            '103项回归通过，覆盖真实TCP接纳确认、提交返回晚于worker执行、C++时间戳包围批次、预启动发送不再建线程、队列满明确失败、取消/关闭、错误后继续服务、resumable发送失败撤销pending计数，以及禁止ThreadPoolExecutor.submit时计算线程仍可提交远端请求和回报结果。8000查询全部成功，预算与唯一批准派发、无节点回访、固定计算线程和源代码哈希审计通过；最终两组另外核验TCP发送线程身份。所有服务与客户端均已停止；结束队列与待确认任务为0，无容量拒绝或控制发送错误。','',
            '- [汇总JSON](comparison.json)、[审计](verification.json)、[回归](regression_final.log)、[固定查询选择](selection.json)。',
            '- `*_branches.csv`保存每分支启动拆分；每个运行目录的nodes_after.json保存完整事件，可按query_request_id和branch_id关联。',
            '- [初始基线源代码哈希](sources.json)对应source_snapshot；[发送对照哈希](sources_senders.json)对应sender_source_snapshot中的冻结版本。当前工作区另有后续计算执行器改动，本报告不将其算入发送对照。冻结文件逐字节匹配实验前哈希，详见[snapshot来源](snapshot_provenance.json)。',
            '- 各组run_load.py执行新实验；analyze.py验证已有记录并重画图。所有实验参数在各组experiment.yaml/load_plan.yaml，抽样参数在analysis.yaml。']
    for row in rows:
        rel=row['run'];lines.append(f"- {row['mode']}：[配置]({row['config_group']}/experiment.yaml)、[汇总]({rel}/summary.json)、[节点事件]({rel}/nodes_after.json)、[逐查询阶段均值]({rel}/query_stage_means.csv)。")
    (R/'REPORT.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps([{k:r[k] for k in ('mode','qps','mean_ms','p99_ms','recall')} for r in rows],ensure_ascii=False))


if __name__=='__main__':main()
