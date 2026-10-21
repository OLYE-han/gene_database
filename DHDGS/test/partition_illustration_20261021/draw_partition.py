"""16:9 illustration, using actual partition.py for every membership and cutoff."""
from pathlib import Path
import sys, json
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.colors import ListedColormap
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parents[1]/'python'))
from dhdgs.partition import Model, calibrate, memberships, summary
font_manager.fontManager.addfont('/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':'WenQuanYi Zen Hei','svg.fonttype':'none','mathtext.fontset':'dejavusans'})
rng=np.random.default_rng(42)
centers=np.array([[-2.,-1.],[2.,-1.],[0.,2.]])
cal=np.vstack([rng.normal(c,.95,(500,2)) for c in centers])
ev=np.vstack([rng.normal(c,.95,(180,2)) for c in centers])
labels=((cal[:,None,:]-centers[None,:,:])**2).sum(2).argmin(1)
radii=np.array([np.sqrt(np.mean(np.sum((cal[labels==j]-centers[j])**2,axis=1))) for j in range(3)])
model=Model(centers,radii,'l2')
cp,cs=model.scores(cal); ids=np.arange(len(cal),dtype=np.uint64)
ep,es=model.scores(ev); eids=np.arange(10000,10000+len(ev),dtype=np.uint64)
threshold=calibrate(cs,ids,.1)
xx,yy=np.meshgrid(np.linspace(-4.5,4.5,420),np.linspace(-3.5,4.2,360))
grid=np.c_[xx.ravel(),yy.ravel()]
gp,gs=model.scores(grid)
gm=memberships(gp,gs,np.arange(100000,100000+len(grid),dtype=np.uint64),threshold,3)
examples=np.array([[-2.,-1.],[0.,-1.],[0.,-1/6]])
xp,xs=model.scores(examples)
xm=memberships(xp,xs,np.array([8000,8001,8002],dtype=np.uint64),threshold,3)
assert xm.sum(1).tolist()==[1,2,3]

fig=plt.figure(figsize=(20,11.25),facecolor='#f8fafc')
canvas=fig.add_axes([0,0,1,1]);canvas.set(xlim=(0,160),ylim=(0,90));canvas.axis('off')
dark='#203249'; muted='#66758b'; cols=['#397dc6','#26a18b','#9262b8'];gold='#d89a25'
def txt(x,y,s,size=12,color=dark,ha='left',weight='normal'):
    canvas.text(x,y,s,fontsize=size,color=color,ha=ha,va='center',fontweight=weight,linespacing=1.5,zorder=8)
def box(x,y,w,h,edge='#c9d3e0',fill='white'):
    canvas.add_patch(FancyBboxPatch((x,y),w,h,boxstyle='round,pad=.2,rounding_size=.8',facecolor=fill,edgecolor=edge,lw=1.2,zorder=1))
def arrow(a,b,col=muted):
    canvas.add_patch(FancyArrowPatch(a,b,arrowstyle='-|>',color=col,mutation_scale=15,lw=1.8,zorder=5))

txt(4,86.8,'DHDGS：按边界裕量进行重叠图划分',23,weight='bold')
txt(156,86.8,'当前实现：partition.method = margin',12,ha='right',color=muted)
box(4,24,69,58)
txt(6,79.8,'① 最近中心确定主分片；边界附近增加副归属',14,weight='bold')
ax=fig.add_axes([.046,.325,.396,.51])
ax.contourf(xx,yy,gp.reshape(xx.shape),levels=[-.5,.5,1.5,2.5],cmap=ListedColormap(['#e6effa','#e5f4ef','#efe7f5']))
ax.contourf(xx,yy,(gm.sum(1)>1).reshape(xx.shape).astype(float),levels=[.5,1.5],colors=['#f5c657'],alpha=.45)
ax.contour(xx,yy,gp.reshape(xx.shape),levels=[.5,1.5],colors='#a2acba',linewidths=.8,alpha=.7)
for j in range(3):
    ax.scatter(ev[ep==j,0],ev[ep==j,1],s=6,c=cols[j],alpha=.28,edgecolors='none')
    ax.scatter(*centers[j],s=175,c=cols[j],marker='*',edgecolors='white',linewidths=1.1,zorder=6)
    ax.annotate(f'中心 {chr(65+j)}',centers[j],xytext=(9,-17),textcoords='offset points',fontsize=11,color=cols[j])
for i,p in enumerate(examples):
    ax.scatter(*p,s=90,facecolors='white',edgecolors=dark,lw=1.6,zorder=7)
    offsets=[(-29,17),(15,-16),(16,17)]
    ax.annotate(f'x{i+1}',p,xytext=offsets[i],textcoords='offset points',fontsize=13,color=dark,zorder=8,
                arrowprops=dict(arrowstyle='-',color=muted,lw=.8))
# Show h_B(x2) using a separate off-boundary explanatory point.
p=np.array([-.5,-1.9]);ax.plot([p[0],0],[p[1],p[1]],color=gold,lw=2.4)
ax.scatter(*p,s=18,c=gold);ax.text(-.25,-2.17,r'$h_B(x)$',fontsize=12,color='#9e6e13',ha='center')
ax.text(-3.95,3.65,'浅色：最近中心区域\n金色：满足副归属阈值的重叠带',fontsize=10.5,color=dark,
        bbox=dict(facecolor='white',alpha=.9,edgecolor='none',pad=5))
ax.set(xlim=(-4.4,4.4),ylim=(-3.3,4.1),aspect='equal',xticks=[],yticks=[])
for spine in ax.spines.values():spine.set_visible(False)
txt(6,27.7,'二维合成示意，归属由真实代码计算；不是 2560 维数据投影。',10,color=muted)

box(77,57,79,25)
txt(79,79.5,'② 用归一化边界裕量决定副本',14,weight='bold')
txt(80,75,r'$d_j(x)=\|x-c_j\|_2^2,\quad a=\arg\min_j\,(d_j(x),j)$',16)
txt(80,68.9,r'$\gamma_j(x)=\frac{d_j(x)-d_a(x)}{2\|c_j-c_a\|_2\,s_{aj}},\quad s_{aj}=\max\!\left(\frac{r_a+r_j}{2},\varepsilon_s\right)$',18)
txt(80,62.8,'γ 越小，越靠近边界；主归属 a 始终保留。',12)
txt(80,59.3,'副中心按 γ 从小到大选取，要求 γ ≤ τ；总副本数不超过 Cmax。',11.5)

box(77,40,79,15)
txt(79,52.6,'③ 同一阈值下的三个向量',13,weight='bold')
for i,(cx,title,desc) in enumerate([(90,'x1：簇内部','仅 A'),(116,'x2：两簇边界','A + B'),(143,'x3：三簇交界','A + B + C')]):
    txt(cx,48.6,title,12,ha='center')
    txt(cx,44.6,desc,16,color=cols[0] if i==0 else gold,ha='center',weight='bold')
txt(79,41.9,'示例 Cmax = 3；副本共享 global_id，分别存入归属分片。',10,color=muted)

box(77,24,79,14)
txt(79,35.6,'④ 一个主旋钮：ρ = partition.overlap_fraction',13,weight='bold')
txt(79,31.4,r'$b(x)=\min_{j\ne a}\gamma_j(x),\qquad \tau=Q_{\rho}\{b(x)\}$',16)
txt(79,26.7,'ρ 增大 → 校准阈值 τ 增大 → 重叠带变宽 → 更多向量多归属',11.5,color='#ad7816')

txt(4,20.6,'构建路径',13,weight='bold')
steps=[('样本聚类','训练 N 个中心\n计算 RMS 半径 r'),('校准阈值','独立样本的 b 分数\n按目标 ρ 选择阈值'),('全量归属','每点：主归属 + 副归属\n按 Cmax 限制副本数'),('重分发','向所有归属节点传向量\n更新路由样本位置'),('本地建图','各分片构建 Vamana\n保存 RaBitQ 编码'),('副本边同步','同 ID 出边取并集\n同步给副本持有者')]
for i,(title,body) in enumerate(steps):
    x=4+i*25.7
    box(x,7.5,23.3,10.6,edge=cols[i%3])
    txt(x+11.65,15.8,title,13,color=cols[i%3],ha='center',weight='bold')
    txt(x+11.65,11.3,body,10.2,ha='center')
    if i<5:arrow((x+23.7,12.6),(x+25.1,12.6))
txt(4,3.8,'ρ 控制目标多归属比例，非随机复制概率；RF 为平均副本数。边界重叠不自动保证全图连通，建图后仍须检查并修复。',11)
txt(156,20.6,'默认 ρ = 0.1，Cmax = 3；同分值用 hash(seed, global_id) 与 ID 稳定裁决',10,color=muted,ha='right')

report={'kind':'synthetic illustration, not benchmark','metric':'l2','seed':42,'centers':centers.tolist(),'radii':radii.tolist(),'rho':.1,'threshold':threshold.to_dict(),'max_copies':3,'examples':[],'rho_sweep':[]}
for i in range(3):
    report['examples'].append({'name':f'x{i+1}','coordinates':examples[i].tolist(),'primary':int(xp[i]),'secondary_scores':{chr(65+j):float(xs[i,j]) for j in range(3) if j!=xp[i]},'memberships':np.flatnonzero(xm[i]).tolist()})
for rho in [0,.1,.3]:
    th=calibrate(cs,ids,rho)
    mm=memberships(ep,es,eids,th,3)
    report['rho_sweep'].append({'rho':rho,'threshold':th.to_dict(),**summary(mm,ep)})
(ROOT/'illustration_data.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
fig.savefig(ROOT/'partition_16x9.png',dpi=160)
fig.savefig(ROOT/'partition_16x9.svg')
plt.close(fig)
print(ROOT/'partition_16x9.png')
