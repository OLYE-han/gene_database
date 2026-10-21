"""Compact 16:9 native-runtime topology; counts follow 06_observe, not live ps."""
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parent
font_manager.fontManager.addfont('/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':'WenQuanYi Zen Hei', 'svg.fonttype':'none'})
fig = plt.figure(figsize=(20, 11.25), facecolor='#f7f9fc')
ax = fig.add_axes([0, 0, 1, 1])
ax.set(xlim=(0,160), ylim=(0,90)); ax.axis('off')
C = dict(blue='#2474c5', purple='#9150b8', teal='#008b85', orange='#d58020',
         gray='#65758a', dark='#1d3048', green='#278065')

def text(x,y,s,size=12,color=None,ha='center',va='center',weight='normal'):
    ax.text(x,y,s,fontsize=size,color=color or C['dark'],ha=ha,va=va,
            fontweight=weight,linespacing=1.45,zorder=5)

def box(x,y,w,h,title,body='',color='gray',fill='white',ts=13,bs=11):
    col=C.get(color,color)
    ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle='round,pad=0.25,rounding_size=0.7',
                             facecolor=fill,edgecolor=col,linewidth=1.3,zorder=2))
    if body:
        text(x+w/2,y+h-1.8,title,ts,col,weight='bold')
        text(x+w/2,y+(h-3)/2,body,bs)
    else: text(x+w/2,y+h/2,title,ts,col)

def line(points,color='gray',dash=False,lw=2,arrow=True):
    col=C.get(color,color)
    xs,ys=zip(*points)
    ax.plot(xs,ys,color=col,lw=lw,linestyle=(0,(4,3)) if dash else '-',zorder=3)
    if arrow:
        a,b=points[-2:]
        ax.add_patch(FancyArrowPatch(a,b,arrowstyle='-|>',mutation_scale=14,
                                    color=col,linewidth=lw,zorder=4))

def label(x,y,s,color='gray',size=10):
    ax.text(x,y,s,fontsize=size,color=C[color],ha='center',va='center',zorder=6,
            bbox=dict(facecolor='#f7f9fc',edgecolor='none',pad=1.7))

text(4,87.7,'DHDGS 线程与通信架构',23,ha='left',weight='bold')
text(156,87.5,'06_observe 配置 · C++ 原生路径 · 动态网络模块开启',12,ha='right')

# One expanded head process.
box(4,51,118,32,'',color='#a5b6ce',fill='#edf3fb')
text(7,81.2,'顶层路由节点  ·  1 个业务进程',15,ha='left',weight='bold')
text(119,81.2,'只路由、保活与合并；不参与逐跳搜索',10,ha='right')
box(8,68.8,32,9.2,'路由池 ×16','HNSW / 样本 RaBitQ\n选择入口，生成初始分支',color='blue',bs=11)
box(46,68.8,32,9.2,'查询状态 + 合并池 ×4','报告处理后更新分支完整性\n合并 top-k → 最终 API 消费线程 ×1',color='purple',bs=10.5)
box(84,68.8,32,9.2,'租约 / deadline 线程 ×1','维护 involved_nodes\n向已涉及节点续租 / 取消',color='teal',bs=11)
box(8,54,32,10.3,'入口 Client · 网络模块','发送 reactor：1–4 线程\n独立伸缩 monitor：1 线程\n请求队列 + 每目标连接池',color='blue',bs=10.5)
box(46,54,32,10.3,'报告 Server · 网络模块','reactor：1–4；handler：4–16\n每 reactor 配 4 个 handler\n独立伸缩 monitor：1 线程',color='purple',bs=10.5)
box(84,54,32,10.3,'租约 Client · 网络模块','发送 reactor：1–4 线程\n独立伸缩 monitor：1 线程\nlease / cancel；接收应答',color='teal',bs=10.5)
line([(24,68.8),(24,64.3)],'blue'); label(24,66.5,'初始任务入发送队列','blue')
line([(62,64.3),(62,68.8)],'purple'); label(62,66.5,'解码 / 校验 / 状态更新','purple')
line([(100,68.8),(100,64.3)],'teal',True); label(100,66.5,'定时控制命令','teal')
text(119,52.2,'另有主 / API 调用线程 ×1；共享查询状态不是线程',9,ha='right')

# Expanded data process with network modules kept inside the process boundary.
box(4,8.5,118,36,'',color='#a3bdb2',fill='#eef6f2')
text(7,43,'数据节点 A  ·  1 个业务进程',15,ha='left',weight='bold')
text(119,43,'同一 query 可在不同节点各有一个并行分支',10,ha='right')
box(8,30,32,10.3,'入站 Server · 网络模块','reactor：1–4；handler：4–16\nadmin：1–4；每 reactor 配 4+1\n独立伸缩 monitor：1 线程',color='blue',bs=10.3)
box(46,30,32,10.3,'报告 Client · 网络模块','发送 reactor：1–4 线程\n独立伸缩 monitor：1 线程\n心跳 / 本分支 top-k；接收 ACK',color='purple',bs=10.3)
box(84,30,32,10.3,'子分支 Client · 网络模块','发送 reactor：1–4 线程\n独立伸缩 monitor：1 线程\n接纳 ACK 回调 → 控制事件队列',color='orange',bs=10.3)
box(8,15.2,32,10.2,'有界计算队列 → 搜索池 ×64','一条本地分支由一个线程执行\n图遍历 / RaBitQ / 精排 / sync_pread',color='green',bs=10.5)
box(46,15.2,32,10.2,'控制调度线程 ×1','处理 start / child / local-done\n完成条件、心跳、重试与回收',color='purple',bs=10.5)
box(84,15.2,32,10.2,'边界扩展决定函数','在搜索线程内执行，不是额外线程\n启发式批准后，才提交远端任务',color='orange',bs=10.5)
line([(24,30),(24,25.4)],'blue'); label(24,27.8,'校验 / 去重 / 接纳入队','blue')
line([(62,25.4),(62,30)],'purple'); label(62,26.8,'完成报告 / 心跳入队','purple')
line([(100,25.4),(100,30)],'orange'); label(100,27.8,'获准子分支入队','orange')
# Separate low and high horizontal corridors avoid a tangle around the control box.
line([(40,22.3),(43,22.3),(43,13.2),(62,13.2),(62,15.2)],'purple')
label(62,12.9,'本地完成事件','purple')
line([(40,24.2),(42,24.2),(42,29),(81,29),(81,22.4),(84,22.4)],'orange')
label(65,29,'遇边界 → 评估远端收益','orange',9.5)
text(7,10.1,'其他线程：主线程 ×1  ·  记录维护 ×1  ·  资源采样 ×1',9.5,ha='left')
text(119,10.1,'本地存储：图记录 + 原始向量；pread 可能命中页缓存',9.5,ha='right')

# Three inter-process channels; one small crossing in the gap, with a bridge.
line([(18,54),(18,40.3)],'blue',lw=2.5)
label(13.8,47.2,'①','blue',13)
line([(62,40.3),(62,54)],'purple',lw=2.5)
label(67,47.2,'②','purple',13)
# Leave a gap at the purple vertical trunk: crossing, not a junction.
line([(100,54),(100,48.2),(64,48.2)],'teal',True,arrow=False)
line([(60,48.2),(35,48.2),(35,40.3)],'teal',True)
label(104,48.2,'③','teal',13)

# Two opaque peers. Only one fan-out trunk is drawn.
box(130,30,26,14.5,'数据节点 B','内部线程同 A（省略）\n接纳 → 搜索 → 直接报告顶层\n接收顶层 lease / cancel',color='orange',fill='#fff9ef',bs=10.5)
box(130,10,26,14.5,'数据节点 C','内部线程同 A（省略）\n接纳 → 搜索 → 直接报告顶层\n接收顶层 lease / cancel',color='orange',fill='#fff9ef',bs=10.5)
line([(116,35.2),(126,35.2),(126,17.2),(130,17.2)],'orange',lw=2.4)
line([(126,35.2),(130,35.2)],'orange',lw=2.4)
label(125.8,27,'④','orange',13)
text(143,6.8,'B、C 的②③通道与 A 相同，省略重复连线',9)

# Right-hand compact flow key doubles as the external/API description.
box(130,51,26,32,'',color='#bdc7d5',fill='white')
text(143,80.7,'通信主干',14,weight='bold')
text(132,76.6,'① 初始搜索请求',11,C['blue'],ha='left',weight='bold')
text(132,72.9,'顶层 → 入口数据节点',10,ha='left')
text(132,68.8,'② 心跳 / 各分支 top-k',11,C['purple'],ha='left',weight='bold')
text(132,65.1,'每个数据节点直接 → 顶层',10,ha='left')
text(132,61,'③ lease / cancel',11,C['teal'],ha='left',weight='bold')
text(132,57.3,'顶层 → 所有已涉及的数据节点',10,ha='left')
text(132,53.3,'④ A → B / C：获准子分支',10.5,C['orange'],ha='left',weight='bold')

text(4,5.2,'客户端 / API：提交 → 路由 → 数据节点搜索 → 顶层合并 → 返回',11,ha='left',weight='bold')
text(4,2.5,'所有网络箭头均含原连接应答（ACK 省略反向线）；网络 reactor 非阻塞收发，不等待搜索完成。',10,ha='left')
text(156,5.2,'动态范围按每个逻辑模块独立计算；并非整个进程合计 1–4 线程',10,ha='right')
text(156,2.5,'扩展后 A 继续；子分支仅保留目标候选；已访问节点禁止重访。',10,ha='right')

fig.savefig(ROOT/'architecture_16x9.png', dpi=160)
fig.savefig(ROOT/'architecture_16x9.svg')
plt.close(fig)
print(ROOT/'architecture_16x9.png')
