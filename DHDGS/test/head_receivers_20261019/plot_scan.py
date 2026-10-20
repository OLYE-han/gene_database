from pathlib import Path
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
R=Path(__file__).resolve().parent;a=json.loads((R/'analysis.json').read_text())
f=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':f.get_name(),'axes.unicode_minus':False})
fig,axes=plt.subplots(1,3,figsize=(15,4),layout='constrained')
for ax,key,title in zip(axes,['p99','qps','registration_mean'],['端到端p99 / ms（越低越好）','QPS（越高越好）','本机注册往返均值 / ms']):
 for i,r in enumerate(a['medians']):
  if key not in r:continue
  vals=[row[key] for row in a['rows'] if row['receivers']==r['receivers']]
  ax.scatter([i]*len(vals),vals,s=28,color='gray',alpha=.6)
  ax.scatter(i,r[key],s=70,color='#1676a0',marker='_')
 ax.set_xticks(range(5),['0','1','2','4','8']);ax.set_xlabel('额外报告接收进程数');ax.set_title(title);ax.grid(axis='y',alpha=.2)
fig.suptitle('三物理机 · 32线程/节点 · 并发128 · 预算200\n每组3轮×2000查询；灰点为逐轮值，蓝线为中位数')
for ext in ('png','svg'):fig.savefig(R/f'receiver_scan.{ext}',dpi=160,bbox_inches='tight')
