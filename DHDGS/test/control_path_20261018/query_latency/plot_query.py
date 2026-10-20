"""Reproduce one randomly selected multi-node request's measured time breakdown."""
from pathlib import Path
import csv,gzip,hashlib,json,math,os,random
import numpy as np
R=Path(__file__).resolve().parent;E=R.parent
os.environ.setdefault('MPLCONFIGDIR',str(R/'.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'font.weight':500,'axes.titleweight':500,'axes.labelweight':500,'axes.unicode_minus':False,'svg.fonttype':'path'})

def save(p,data):p.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False))
def table(h,rows):return ['| '+' | '.join(h)+' |','|'+'|'.join(['---']*len(h))+'|']+['| '+' | '.join(map(str,r))+' |' for r in rows]
def main():
    source=E/'optimized/runs/20261018T120948/report.json.gz'
    with gzip.open(source,'rt') as f:report=json.load(f)
    eligible=sorted([q for q in report['queries'] if len(q['branch_reports'])>1],key=lambda q:q['query_row'])
    q=random.Random(20261018).choice(eligible);save(R/'selected_query.json',q)
    save(R/'selection.json',{'seed':20261018,'method':'random.Random.choice over query_row-sorted multi-node requests',
        'eligible':len(eligible),'query_row':q['query_row'],'query_id':q['query_id'],'request_id':q['request_id'],
        'source':str(source.relative_to(E)),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest()})
    s=q['stats'];total=q['client_wall_ms'];origin=q['client_start_ns']
    assert math.isclose(total,(q['client_end_ns']-origin)/1e6,abs_tol=1e-6)
    branches=sorted(q['branch_reports'],key=lambda b:len(b['visited_nodes']))
    parts=s['latency_breakdown_ms'];outer=total-s['latency_ms'];assert outer>=0
    top=[('路由与入口处理',parts['routing_wall_ms']),('数据执行与控制等待',parts['peer_execution_and_control_wait_ms']),
         ('入口准备、合并与生命周期剩余',parts['head_merge_and_lifecycle_ms']),('API计时边界外侧差额',outer)]
    assert math.isclose(sum(x[1] for x in top),total,abs_tol=1e-6)
    detailed=[];windows=[];raw=[];domains=set()
    life_keys=['query_ready_queue_ms','query_compute_slice_wall_ms','query_transfer_finalize_wait_ms','query_state_other_ms']
    life_labels=['接纳后至工作线程开始','搜索执行区间','本地结束后等派发确认','其他状态衔接']
    for b in branches:
        st=b['stats'];sid=b['node_id'];trace=b['local_batch_trace']
        start=min(x['service_trace']['start_ns'] for x in trace);end=max(x['service_trace']['end_ns'] for x in trace)
        windows.append({'node':sid,'start_ms':(start-origin)/1e6,'end_ms':(end-origin)/1e6})
        vals={'pread':0.,'exact_distance':0.,'rabitq':0.};service=0.
        for batch_idx,batch in enumerate(trace):
            t=batch['service_trace'];domains.add(t['clock_domain']);service+=(t['end_ns']-t['start_ns'])/1e6
            for name,a,z in t['spans']:
                assert origin<=a<=z<=q['client_end_ns']
                vals[name]+=(z-a)/1e6
                raw.append({'node':sid,'batch':batch_idx,'kind':name,'start_ms':(a-origin)/1e6,'end_ms':(z-origin)/1e6,'duration_ms':(z-a)/1e6})
        for k,v in vals.items():assert math.isclose(v,st[k+'_ms'],abs_tol=1e-6)
        assert math.isclose(service,st['service_ms'],abs_tol=1e-6)
        assert math.isclose(sum(st[k] for k in life_keys),st['query_branch_wall_ms'],abs_tol=1e-6)
        core=st['branch_wall_ms'];wrapper=st['query_compute_slice_wall_ms']
        items=[('pread调用',vals['pread']),('精确距离',vals['exact_distance']),('RaBitQ估距',vals['rabitq']),
               ('批次内其他',service-sum(vals.values())),('C++循环其余区间',core-service),('适配、报告转换与其他外围等待',wrapper-core)]
        assert min(v for _,v in items)>=0
        detailed.append({'node':sid,'lifecycle_ms':{k:st[k] for k in life_keys},'branch_total_ms':st['query_branch_wall_ms'],
            'compute_parts_ms':dict(items),'native_core_wall_ms':core,'thread_cpu_ms':st['server_thread_cpu_ms'],
            'transfer':b['peer_transfer'],'fork_events':b['fork_events']})
    assert len(domains)==1
    # This selected query has disjoint measured search windows. Never add
    # concurrent branch windows to manufacture end-to-end percentages.
    ordered=sorted(windows,key=lambda w:w['start_ms']);cursor=0.;timeline=[]
    for i,w in enumerate(ordered):
        assert cursor<=w['start_ms']<=w['end_ms']<=total
        label='提交至首个本地批次开始' if i==0 else '两个节点批次窗口之间（未细分）'
        if w['start_ms']>cursor:timeline.append({'label':label,'start_ms':cursor,'end_ms':w['start_ms']})
        timeline.append({'label':f"节点{w['node']}首批开始至末批结束",'start_ms':w['start_ms'],'end_ms':w['end_ms']})
        cursor=w['end_ms']
    timeline.append({'label':'最后一个本地批次结束至API返回','start_ms':cursor,'end_ms':total})
    for t in timeline:t['duration_ms']=t['end_ms']-t['start_ms'];t['percent']=100*t['duration_ms']/total
    assert math.isclose(sum(t['duration_ms'] for t in timeline),total,abs_tol=1e-6)
    data={'query_id':q['query_id'],'query_row':q['query_row'],'total_ms':total,'top_parts_ms':dict(top),
          'clock_domain':next(iter(domains)),'timeline':timeline,'branches':detailed,
          'unavailable':['true executor enqueue/dequeue times','GIL/lock wait split','initial submit RPC stages','per-query result-control queue/send/ACK stages','physical SSD service time']}
    save(R/'breakdown.json',data)
    for name,rows in [('timeline.csv',timeline),('record_spans.csv',raw)]:
        with (R/name).open('w') as f:
            w=csv.DictWriter(f,list(rows[0]));w.writeheader();w.writerows(rows)
    colors=['#527b9c','#bd873c','#3b9c89','#8f96a0']
    fig,axes=plt.subplots(3,1,figsize=(13,10),layout='constrained')
    ax=axes[0]
    for i,(label,v) in enumerate(top):
        ax.barh(i,v,color=colors[i]);ax.text(v+25,i,f'{v:.3f} ms  ({100*v/total:.3f}%)',va='center')
    ax.set_yticks(range(len(top)),[p[0] for p in top]);ax.invert_yaxis();ax.set_xlim(0,total*1.32)
    ax.set_title('端到端耗时：互斥分组，合计覆盖100%（不是绝对时间轴）',loc='left');ax.set_xlabel('毫秒')
    ax=axes[1]
    ax.broken_barh([(0,total)],(1.65,.7),facecolors='#e8edf2')
    ax.text(total*.5,2,f'提交 0 → API返回 {total:.3f} ms',ha='center',va='center')
    for i,w in enumerate(windows):
        ax.broken_barh([(w['start_ms'],w['end_ms']-w['start_ms'])],(i-.3,.6),facecolors='#238f82')
        ax.annotate(f"{w['start_ms']:.3f}–{w['end_ms']:.3f} ms",xy=(w['start_ms'],i),xytext=(30,15),textcoords='offset points',fontsize=10,arrowprops={'arrowstyle':'-'})
    ax.set_yticks([0,1,2],[f"节点{w['node']} 批次窗口" for w in windows]+['客户端'])
    ax.set_xlim(-20,total+60);ax.set_ylim(-.7,2.7);ax.set_xlabel('相对客户端提交的实测时间 / ms')
    ax.set_title('真实时间轴：仅画有绝对时间戳的首批至末批区间；空白不可当作空闲CPU',loc='left')
    ax=axes[2]
    for i,d in enumerate(detailed):
        left=0
        for j,(key,label) in enumerate(zip(life_keys,life_labels)):
            v=d['lifecycle_ms'][key]
            ax.barh(i,v,left=left,color=colors[j],label=label if i==0 else None)
            if v>60:ax.text(left+v/2,i,f'{v:.2f}',ha='center',va='center',color='white')
            left+=v
        ax.text(left+15,i,f'{left:.2f} ms',va='center')
    ax.set_yticks(range(len(detailed)),[f"节点{d['node']}" for d in detailed]);ax.invert_yaxis()
    ax.set_xlim(0,max(d['branch_total_ms'] for d in detailed)*1.2);ax.set_xlabel('分支自身累计毫秒；不同分支不可相加为query延迟')
    ax.set_title('分支生命周期耗时组成：各分支从零起画，不表示共同起始时刻',loc='left')
    ax.legend(loc='upper left',bbox_to_anchor=(0,-.28),ncol=2,fontsize=10)
    fig.suptitle(f"查询ID {q['query_id']}：总延迟 {total:.2f} ms，节点2→1，Recall@10 {q['recall']:.0%}\n完整2M索引 · 本机三进程 · 预算200 · 同步读取/热页缓存",fontsize=15)
    for ext in ('png','svg'):fig.savefig(R/f'timeline.{ext}',dpi=160)
    plt.close(fig)
    lines=['# 单查询从提交到完成的时间拆分','',f"查询ID **{q['query_id']}**，query_row={q['query_row']}，request_id=`{q['request_id']}`。从最终优化实验中{len(eligible)}条多节点查询按固定种子20261018随机抽取，未按延迟筛选；不能用一条查询代表总体。总延迟 **{total:.6f} ms**，Recall@10={q['recall']:.0%}，路径节点2→1，全局200次扩展（153+47）。",'',
      '![耗时拆分和真实时间轴](timeline.png)','',
      '## 覆盖全部端到端时间的分组','',
      '从客户端调用search前的monotonic_ns，到search返回后的monotonic_ns；不含调用前预处理/排队及返回后真值比较、文件写出。以下字段互斥且合计等于总延迟。','']
    lines+=table(['分组','耗时ms','占比'],[[label,f'{v:.6f}',f'{100*v/total:.4f}%'] for label,v in top])
    lines+=['','“入口准备、合并与生命周期剩余”由总API时间减去路由和执行区间得到，含派发前的状态准备，不能把它全部画在执行结束后。API外侧差额也未记录为单个连续区间。','',
      '## 用绝对时间戳定位的完整顺序','',
      '本机进程共享单调时钟域，批次时间戳落在客户端调用边界内。此表按已记录的首/末批次切分全部时间；未细分区间保留原状，没有将网络、GIL或线程池等待虚构成独立实测值。','']
    lines+=table(['起点ms','终点ms','区间','耗时ms'],[[f"{t['start_ms']:.6f}",f"{t['end_ms']:.6f}",t['label'],f"{t['duration_ms']:.6f}"] for t in timeline])
    lines+=['','这两个批次窗口互不重叠，合计约14.677ms。窗口包含批次间代码及等待，不等于纯CPU时间，也不覆盖首批之前和末批之后的搜索适配/报告处理。','',
      '## 每个分支的生命周期','']
    lines+=table(['阶段','节点2 ms','节点1 ms'],[[label,*[f"{d['lifecycle_ms'][key]:.6f}" for d in detailed]] for key,label in zip(life_keys,life_labels)]+[['分支生命周期合计',*[f"{d['branch_total_ms']:.6f}" for d in detailed]]])
    lines+=['','就绪计时从创建接纳记录到_run开始，包含提交线程池前等待，不能等同于纯工作队列等待。计算执行区间含Python适配/C++报告转换和相关等待。节点2派发后继续本地搜索；其本地结束后的派发确认等待已释放计算线程。分支生命周期结束在最终报告准备好时，不包含之后全部结果回传等待。','',
      '## 搜索执行区间进一步分解','']
    labels=list(detailed[0]['compute_parts_ms'])
    lines+=table(['区间','节点2 ms','节点1 ms'],[[label,*[f"{d['compute_parts_ms'][label]:.6f}" for d in detailed]] for label in labels])
    lines+=['','上述六项分别相加为各节点搜索执行区间13.923143ms与352.610012ms。批次内其他包括邻接解析、候选数据收集等；C++其余区间包含查询预处理、候选维护、扩展选择、派发回调及调度等待。最后一项是外围执行区间与C++内部计时之差，含进入原生循环之前的准备、返回时的GIL等待和Python报告构造等，无法再精确拆分。','']
    lines+=table(['辅助指标（不再相加）','节点2 ms','节点1 ms'],[['C++内部搜索计时',*[f"{d['native_core_wall_ms']:.6f}" for d in detailed]],['搜索线程实际CPU时间',*[f"{d['thread_cpu_ms']:.6f}" for d in detailed]]])
    lines+=['','节点1外围差额约350.30ms，但搜索线程CPU只有3.77ms。这说明352.61ms不能称为纯计算耗时。现有插桩不能断定这350.30ms全部发生在返回阶段，也不能全部归为GIL。','',
      '## 节点2→1派发耗时','']
    tr=detailed[0]['transfer']
    transfer_labels=[('peer_transfer_queue_ms','派发线程池等待'),('peer_pool_wait_ms','借用连接'),('peer_connect_ms','新建连接（本次复用）'),('peer_send_ms','编码/发送'),('peer_response_wait_ms','等待接纳响应/解码'),('peer_transfer_wall_ms','派发执行总时长，不含线程池排队')]
    lines+=table(['阶段','ms'],[[label,f"{tr[key]:.6f}"] for key,label in transfer_labels])
    residual=tr['peer_transfer_wall_ms']-sum(tr[k] for k in ('peer_pool_wait_ms','peer_connect_ms','peer_send_ms','peer_response_wait_ms'))
    lines+=['',f'派发执行的其余状态/函数边界时间为{residual:.6f}ms。线程池等待+派发执行为{tr["peer_transfer_queue_ms"]+tr["peer_transfer_wall_ms"]:.6f}ms，与父分支搜索及派发收尾存在重叠，不可重复加进总延迟。等待响应包括目标端处理，不等于纯网络时间。','',
      '节点2在已扩展105个顶点后批准派发，给节点1预算47，自己最终扩展153。没有绕过扩展函数。','',
      '## 尚不能拆开的时间','',
      '- 初始入口提交的借连接/发送/接纳时长未保留到该查询输出；只能放在端到端执行区间中。',
      '- 真正任务入队、取出、GIL等待、状态锁等待没有逐查询时间戳。',
      '- 结果/心跳发送队列、编码、接收、ACK仅有节点累计计数，没有本查询的完整事件链。',
      '- pread是文件调用wall，含页缓存和调度；本轮物理读取增量0，不能给出本查询纯SSD耗时。',
      '- 不能把最后831.538ms全归因于结果网络传输，它还可包括报告构造、控制排队、调度和入口唤醒。','',
      '“所有时间”在端到端总量和上述时间轴中完整覆盖；每个内部子阶段还没有完整可观测性，需要额外插桩才能继续拆分。','',
      '- [原始查询](selected_query.json)、[抽样依据](selection.json)、[结构化拆分](breakdown.json)。',
      '- [完整时间轴CSV](timeline.csv)、[每次pread/精排/RaBitQ区间CSV](record_spans.csv)、[SVG图](timeline.svg)。',
      '- 本目录plot_query.py从冻结原始实验重新选取、校验并生成全部图表；不执行新查询或修改产品代码。','']
    (R/'REPORT.md').write_text('\n'.join(lines))
    print(json.dumps({'query':q['query_id'],'total_ms':total,'timeline':timeline,'top':top},ensure_ascii=False))

if __name__=='__main__':main()
