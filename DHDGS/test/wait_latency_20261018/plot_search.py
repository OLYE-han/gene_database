"""Zoom into selected native loops; pread wall time is not physical SSD time."""
from pathlib import Path
import argparse,json,csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
R=Path(__file__).resolve().parent
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'font.weight':500,'axes.titleweight':500,
                    'axes.labelweight':500,'axes.unicode_minus':False,'svg.fonttype':'path'})
p=argparse.ArgumentParser();p.add_argument('group');p.add_argument('--run');a=p.parse_args()
d=R/'analysis'/a.group/(a.run or sorted((R/'analysis'/a.group).iterdir())[-1].name)
q=json.loads((d/'selected_query.json').read_text());e=json.loads((d/'selected_events.json').read_text())
times={t['branch_id']:t for t in e['startup']};rows=[]
for b in q['branch_reports']:
    t=times[b['branch_id']];wall=(t['native_loop_done_ns']-t['native_search_start_ns'])/1e6
    spans={k:0. for k in ['pread','exact_distance','rabitq']}
    for batch in b['local_batch_trace']:
        for kind,start,end in batch['service_trace']['spans']:spans[kind]+=(end-start)/1e6
    rows.append({'node':b['node_id'],'wall_ms':wall,**spans,
                 'other':max(0.,wall-sum(spans.values()))})
fig,ax=plt.subplots(figsize=(11,4),layout='constrained')
names=['pread','exact_distance','rabitq','other'];labels=['buffered pread','精确距离','RaBitQ估距','其他原生逻辑/回调与调度'];colors=['#688eb1','#71b89b','#e3b46f','#c1b5cd']
for i,r in enumerate(rows):
    left=0.
    for key,label,color in zip(names,labels,colors):
        width=r[key];ax.barh(i,width,left=left,color=color,label=label if i==0 else None)
        if width>.2:ax.text(left+width/2,i,f'{width:.3f}',ha='center',va='center',fontsize=9)
        left+=width
    ax.text(left+.03,i,f"合计 {r['wall_ms']:.3f} ms",va='center')
ax.set_yticks(range(len(rows)),[f"节点{r['node']}分支" for r in rows]);ax.invert_yaxis()
ax.set_xlabel('原生循环内墙钟时长 / ms；每行独立，不能相加成查询总延迟')
ax.set_title(f"查询 {q['query_id']} 的原生搜索放大图\n热OS页缓存，pread不代表物理SSD耗时；每项均可能包含OS调度")
ax.legend(loc='upper left',bbox_to_anchor=(0,-.22),ncols=2);ax.grid(axis='x',alpha=.2);ax.margins(x=.25)
for ext in ['png','svg']:fig.savefig(d/f'query_search_detail.{ext}',dpi=150)
with (d/'query_search_detail.csv').open('w') as f:
    w=csv.DictWriter(f,list(rows[0]));w.writeheader();w.writerows(rows)
print(d/'query_search_detail.png')
