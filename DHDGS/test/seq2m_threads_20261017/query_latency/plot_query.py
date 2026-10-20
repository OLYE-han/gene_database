"""Measured single-branch latency attribution; never sum overlapping branches."""
from pathlib import Path
import csv
import hashlib
import json
import math
import os
import sys

ROOT = Path(__file__).resolve().parent
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / '.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import numpy as np

FONT = '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc'
font = FontProperties(fname=FONT)
plt.rcParams.update({'font.family':font.get_name(), 'axes.unicode_minus':False,
                     'font.size':12, 'font.weight':500, 'axes.titleweight':500,
                     'axes.labelweight':500, 'svg.fonttype':'path', 'figure.facecolor':'white'})


def decompose(q):
    assert q['status']=='ok' and q['stats']['metrics_complete']
    assert len(q['branch_reports'])==1
    s=q['stats']; b=q['branch_reports'][0]; p=s['latency_breakdown_ms']
    traces=[batch['service_trace'] for batch in b['local_batch_trace']]
    assert len({t['clock_domain'] for t in traces})==1
    previous=None
    totals={'pread':0,'rabitq':0,'exact_distance':0}
    service_ns=0
    intervals=[]
    first=traces[0]['start_ns']
    for t in traces:
        a,z=t['start_ns'],t['end_ns']
        assert a<=z and (previous is None or previous<=a)
        if previous is not None and previous<a: intervals.append(('branch_other',previous,a))
        cursor=a
        for name,start,end in sorted(t['spans'],key=lambda s:s[1]):
            assert name in totals and cursor<=start<=end<=z
            if cursor<start: intervals.append(('service_other',cursor,start))
            intervals.append((name,start,end));totals[name]+=end-start;cursor=end
        if cursor<z: intervals.append(('service_other',cursor,z))
        service_ns+=z-a;previous=z
    service=service_ns/1e6
    for name,v in totals.items(): assert math.isclose(v/1e6,s[name+'_ms'],abs_tol=1e-7)
    assert math.isclose(service,s['service_ms'],abs_tol=1e-7)
    wall=b['stats']['branch_wall_ms']; latency=s['latency_ms']
    parts=[('routing','HNSW 路由与入口处理',p['routing_wall_ms']),
           ('outside_branch','提交 / 排队 / 回传 / 控制等待（合并项）',p['peer_execution_and_control_wait_ms']-wall),
           ('pread','pread 文件读取调用（含调用内等待）',totals['pread']/1e6),
           ('exact_distance','精确距离代码区间（含等待）',totals['exact_distance']/1e6),
           ('rabitq','RaBitQ 估距代码区间（含等待）',totals['rabitq']/1e6),
           ('service_other','节点处理其余区间（解码 / 校验 / 邻居收集等，含等待）',service-sum(totals.values())/1e6),
           ('branch_other','搜索控制 / 候选维护 / 批次衔接（含等待）',wall-service),
           ('merge','结果合并与清理',p['head_merge_and_lifecycle_ms'])]
    assert all(v>=0 for _,_,v in parts)
    assert math.isclose(sum(v for _,_,v in parts),latency,abs_tol=1e-6)
    data=[{'key':key,'label':label,'ms':v,'percent':v/latency*100} for key,label,v in parts]
    return data,intervals,{'branch_wall_ms':wall,'search_thread_cpu_ms':s['server_thread_cpu_ms'],
                          'branch_wall_minus_thread_cpu_ms':wall-s['server_thread_cpu_ms'],
                          'thread_cpu_over_branch_wall_percent':s['server_thread_cpu_ms']/wall*100,
                          'timeline_origin_ns':first,'timeline_end_ns':previous,
                          'service_trace_thread_cpu_ms':sum(t['thread_cpu_ms'] for t in traces)}


def main():
    q=json.loads((ROOT/'selected_query.json').read_text());s=q['stats']
    rows,intervals,extra=decompose(q)
    colors={'routing':'#64748b','outside_branch':'#b09a70','pread':'#9362b7',
            'exact_distance':'#1d9c9b','rabitq':'#4f9c44','service_other':'#4079b4',
            'branch_other':'#de8f39','merge':'#64748b'}
    with (ROOT/'breakdown.csv').open('w') as f:
        writer=csv.DictWriter(f,['key','label','ms','percent']);writer.writeheader();writer.writerows(rows)
    payload={'query_id':q['query_id'],'query_row':q['query_row'],'request_id':q['request_id'],
             'latency_ms':s['latency_ms'],'parts':rows,**extra,
             'method':'Non-overlapping duration decomposition for one synchronous branch. Service intervals verified disjoint; nested spans verified against counters. Residual durations retain scheduling/lock/GIL/I/O waits, not pure CPU categories.',
             'pure_network_ms':None,'pure_physical_ssd_ms':None,'pure_gil_wait_ms':None,
             'plotting':{'python':sys.version,'matplotlib':matplotlib.__version__,'numpy':np.__version__},
             'input_sha256':hashlib.sha256((ROOT/'selected_query.json').read_bytes()).hexdigest()}
    (ROOT/'breakdown.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2))

    fig=plt.figure(figsize=(15,11),layout='constrained')
    gs=fig.add_gridspec(3,1,height_ratios=[1.4,5.6,1.5])
    flow=fig.add_subplot(gs[0]);flow.set_axis_off()
    flow.set_title(f'实测查询 ID {q["query_id"]}：总延迟 {s["latency_ms"]/1000:.3f} 秒',fontsize=20,loc='left',pad=25)
    stages=[('HNSW 入口',s['latency_breakdown_ms']['routing_wall_ms']),
            ('数据执行与控制等待',s['latency_breakdown_ms']['peer_execution_and_control_wait_ms']),
            ('合并与清理',s['latency_breakdown_ms']['head_merge_and_lifecycle_ms'])]
    for i,(name,ms) in enumerate(stages):
        x=.16+i*.34
        flow.text(x,.42,f'{name}\n{ms:,.2f} ms  /  {ms/s["latency_ms"]*100:.3f}%',ha='center',va='center',
                  bbox=dict(boxstyle='round,pad=.7',fc='#edf3f8',ec='#aec1d1'),fontsize=13)
        if i<2:flow.annotate('',xy=(x+.22,.42),xytext=(x+.12,.42),arrowprops=dict(arrowstyle='->',lw=2,color='#657987'))
    flow.text(0,-.15,'每节点 32 计算线程 · 客户端并发 32 · Recall@10 = 1.0 · 仅 Intel-35 · 1,024 顶点 / 257 批次',fontsize=12)
    ax=fig.add_subplot(gs[1]);ys=np.arange(len(rows))
    ax.barh(ys,[r['percent'] for r in rows],color=[colors[r['key']] for r in rows],height=.62)
    ax.set_yticks(ys,[r['label'] for r in rows]);ax.invert_yaxis();ax.set_xlim(0,84)
    for y,r in zip(ys,rows):ax.text(r['percent']+.8,y,f'{r["ms"]:,.2f} ms  ({r["percent"]:.3f}%)',va='center',fontsize=11)
    ax.set_xlabel('占端到端延迟的百分比（各项互斥，相加为 100%）')
    ax.set_title('按代码区间细分：节点内各项交错发生，并非八个连续阶段',loc='left',pad=15)
    ax.grid(axis='x',alpha=.2);ax.set_axisbelow(True)
    for spine in ('top','right','left'):ax.spines[spine].set_visible(False)
    cpu=fig.add_subplot(gs[2]);percent=extra['thread_cpu_over_branch_wall_percent']
    cpu.barh([0],[percent],color='#254c6b',height=.45)
    cpu.barh([0],[100-percent],left=[percent],color='#dce4ec',height=.45)
    cpu.set_xlim(0,100);cpu.set_ylim(-.6,.6);cpu.set_yticks([])
    cpu.set_title(f'另一种观察口径：分支墙钟 {extra["branch_wall_ms"]:,.2f} ms 中，搜索线程 CPU 仅 {extra["search_thread_cpu_ms"]:,.2f} ms',loc='left',fontsize=13)
    cpu.text(percent/2,0,f'{percent:.2f}%',ha='center',va='center',color='white',fontsize=11)
    cpu.text(percent+(100-percent)/2,0,f'其余 {100-percent:.2f}% 未在该搜索线程上执行 CPU\n含 GIL / 调度 / 锁 / I/O 等等待；尚不能逐项区分',ha='center',va='center',fontsize=11)
    cpu.set_xlabel('仅用于解释等待，不与上图各项相加；pread 时间也不等于纯 SSD 延迟。')
    for spine in cpu.spines.values():spine.set_visible(False)
    for suffix in ('png','svg'):fig.savefig(ROOT/f'latency_breakdown.{suffix}',dpi=160,bbox_inches='tight')
    plt.close(fig)

    # A genuine local monotonic timeline, without aligning clocks across hosts.
    fig,axes=plt.subplots(2,1,figsize=(15,7),layout='constrained')
    order=['branch_other','service_other','pread','exact_distance','rabitq']
    labels=['搜索控制 / 批次间区间','节点处理其余区间','pread 调用','精确距离','RaBitQ 估距']
    origin=extra['timeline_origin_ns'];end=(extra['timeline_end_ns']-origin)/1e6
    for ax,limit,title in zip(axes,[end,500],['节点内实际时间轴（首个处理批次开始为零点）','前 500 ms 放大；每种颜色是相应代码区间的墙钟跨度']):
        for i,key in enumerate(order):
            spans=[((a-origin)/1e6,(z-a)/1e6) for k,a,z in intervals if k==key and (a-origin)/1e6<limit]
            ax.broken_barh(spans,(i-.32,.64),facecolors=colors[key])
        ax.set_yticks(range(len(order)),labels);ax.invert_yaxis();ax.set_xlim(0,limit);ax.set_xlabel('本机单调时钟，相对时间 ms');ax.set_title(title,loc='left');ax.grid(axis='x',alpha=.2)
    fig.suptitle('查询 264815：同一数据节点上的 257 个顺序扩展批次',fontsize=17)
    fig.supxlabel('不包含首批次前 / 末批次后的分支代码和入口阶段；彩色区间内也可能等待，不能视为纯 CPU 运行。',fontsize=11)
    for suffix in ('png','svg'):fig.savefig(ROOT/f'local_timeline.{suffix}',dpi=160,bbox_inches='tight')
    plt.close(fig)

    comparisons=[]
    for w in (1,2,4,8,16,32):
        path=ROOT/('selected_query.json' if w==32 else f'same_query_w{w}.json')
        if not path.exists():continue
        other=json.loads(path.read_text());parts,_,info=decompose(other)
        comparisons.append({'worker_threads':w,'latency_ms':other['stats']['latency_ms'],
            'branch_wall_ms':info['branch_wall_ms'],'search_thread_cpu_ms':info['search_thread_cpu_ms'],
            'outside_branch_execution_ms':next(r['ms'] for r in parts if r['key']=='outside_branch'),
            'recall':other['recall'],'expanded':other['stats']['expanded']})
    with (ROOT/'same_query_threads.csv').open('w') as f:
        writer=csv.DictWriter(f,list(comparisons[0]));writer.writeheader();writer.writerows(comparisons)
    lines=['# 查询 264815 的时延拆解','',
           '选择规则：在每节点 32 线程 / 客户端并发 32 组中，从成功、单分支、Recall@10=1 的查询中选择最接近整组成功查询延迟中位数的一条。',
           '该查询 10,921.51 ms，整组中位数 10,928.71 ms。只访问 Intel-35，无跨数据节点搜索。单分支选择用于避免并行时间重复累计，不代表所有查询的组成。','',
           '![时延占比](latency_breakdown.png)','',
           '| 代码区间 | 时间 ms | 端到端占比 |','|---|---:|---:|']
    for r in rows:lines.append(f'| {r["label"]} | {r["ms"]:.3f} | {r["percent"]:.4f}% |')
    lines+=['','所有区间是墙钟跨度，包含该区间内发生的等待。细分源于原始 trace，检查了批次顺序、跨度互斥和累计值与统计字段一致。',
            '节点处理其余区间 = service_ms − pread − 精确距离 − RaBitQ；搜索控制与批次衔接 = branch_wall_ms − service_ms。',
            '分支外提交/排队/返回/控制是合并残差，不能当作纯网络耗时；路由与合并来自入口端计时。',
            f'分支墙钟 {extra["branch_wall_ms"]:.3f} ms，搜索线程 CPU {extra["search_thread_cpu_ms"]:.3f} ms，相当于 {extra["thread_cpu_over_branch_wall_percent"]:.2f}%。',
            '差值包含 GIL、操作系统调度、锁、I/O 等等待和其他线程执行的工作；没有独立的 GIL/锁/纯网络/纯 SSD 计时，不能伪造这些成分的占比。',
            '本查询逻辑读取 12 MiB / 1024 次 pread；所在整组进程物理读取统计为零，属于热页缓存测量。','',
            '## 同一查询在不同线程数下（客户端并发均为 32）','',
            '| 线程/节点 | 端到端 ms | 分支执行墙钟 ms | 搜索线程 CPU ms | 分支外提交/排队/返回/控制 ms |',
            '|---:|---:|---:|---:|---:|']
    for r in comparisons:lines.append('| '+ ' | '.join([str(r['worker_threads']),*[f'{r[k]:.3f}' for k in ('latency_ms','branch_wall_ms','search_thread_cpu_ms','outside_branch_execution_ms')]])+' |')
    lines+=['','以上都扩展 1024 顶点、Recall@10=1；单条查询的调度位置不同，不能代替整组统计。',
            '![节点时间轴](local_timeline.png)','',
            '原始查询见 selected_query.json；选择与来源见 selection.json；数值见 breakdown.json / breakdown.csv / same_query_threads.csv。',
            '图表用 Matplotlib 生成，PNG 和 SVG 均保留，可运行 plot_query.py 复现。','']
    (ROOT/'README.md').write_text('\n'.join(lines))
    print(json.dumps({'query_id':q['query_id'],'latency_ms':s['latency_ms'],'parts_sum_ms':sum(r['ms'] for r in rows),**extra}))


if __name__=='__main__':main()
