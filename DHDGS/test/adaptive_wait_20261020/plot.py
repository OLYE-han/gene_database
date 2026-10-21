"""Same-host head phases and data-node phases; no cross-machine clock subtraction."""
import importlib.util,json,gzip,csv,os
from pathlib import Path
from collections import defaultdict
import numpy as np
ROOT=Path(__file__).resolve().parent
os.environ['MPLCONFIGDIR']=str(ROOT/'.mplconfig')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
spec=importlib.util.spec_from_file_location('previous_analysis',ROOT.parent/'control_capacity_20261020/analyze.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
rows=[];selected={}
paths=sorted(ROOT.glob('short/*/result.json'))
for path in paths:
 r=json.loads(path.read_text());run=Path(r['directory']);sums=defaultdict(float);n=0
 after=json.load(gzip.open(run/'nodes_after.json.gz','rt'))
 traces={t['query_request_id']:t for node in after.values() for t in node['peer_lifecycle']['execution_timestamps']}
 with (run/'queries.csv').open() as f:
  eligible=[q for q in csv.DictReader(f) if q['status']=='ok' and q['request_id'] in traces and int(q['branch_count'])==1]
 chosen=min(eligible,key=lambda q:abs(float(q['latency_ms'])-r['latency_ms']['p99']))
 with gzip.open(run/'queries.jsonl.gz','rt') as f:
  for line in f:
   q=json.loads(line);parts=m.timeline(q);n+=1
   for v in parts:sums[v['stage']]+=v['duration_ms']
   if q['request_id']==chosen['request_id']:selected[path.parent.name]=(q,parts,traces[q['request_id']])
 for stage,total in sums.items():rows.append(dict(case=path.parent.name,stage=stage,mean_ms=total/n))
m.write_csv(ROOT/'head_stage_means.csv',rows)
best=max(paths,key=lambda p:json.loads(p.read_text())['qps'])
cases=['00_baseline',best.parent.name]
if cases[0]==cases[1]:cases[1]=paths[-1].parent.name
fig,axes=plt.subplots(2,2,figsize=(16,11))
for col,case in enumerate(cases):
 q,head,t=selected[case]
 (ROOT/(case+'_selected.json')).write_text(json.dumps(dict(query=q,node_trace=t,selection='Nearest p99 among successful retained single-branch requests'),indent=2))
 stages=[('Receiver / admission',t['receiver_message_ready_ns']),('Accept processing',t['enqueued_ns']),
 ('Admission queue',t['dequeued_ns']),('Submit / executor',t['worker_enter_ns']),
 ('Worker state lock interval',t['worker_state_lock_acquired_ns']),('Native preparation',t['native_search_start_ns']),
 ('Native search',t['native_loop_done_ns']),('Native report selection',t['control_result_selected_ns'])]
 node=[dict(stage=name,start_ms=(stages[i-1][1]-stages[0][1])/1e6,duration_ms=(stamp-stages[i-1][1])/1e6) for i,(name,stamp) in enumerate(stages) if i]
 for row,(parts,title) in enumerate([(head,'Head: own clock'),(node,'Data node: own clock')]):
  ax=axes[row,col];ax.barh([v['stage'] for v in parts],[v['duration_ms'] for v in parts],left=[v['start_ms'] for v in parts],color=plt.cm.tab20(np.linspace(0,1,len(parts))))
  for i,v in enumerate(parts):ax.text(v['start_ms']+v['duration_ms'],i,f" {v['duration_ms']:.2f}",va='center',fontsize=8)
  ax.invert_yaxis();ax.set_xlabel('ms since first event on this host');ax.set_title(case+' / '+title);ax.margins(x=.2)
fig.suptitle('Tail query examples (single branch, nearest p99); independent clocks, never add rows')
fig.tight_layout();fig.savefig(ROOT/'waiting_timeline.png',dpi=150);fig.savefig(ROOT/'waiting_timeline.svg');plt.close(fig)
data=json.loads((ROOT/'rounds.json').read_text());fig,axes=plt.subplots(1,3,figsize=(16,5))
for ax,k,label in zip(axes,['qps','p99_ms','ready_ms'],['QPS','End-to-end p99 (ms)','Cumulative ready wait / query (ms)']):
 bars=ax.bar([r['case'] for r in data],[r[k] for r in data]);ax.bar_label(bars,fmt='%.1f',fontsize=8);ax.set_title(label);ax.tick_params(axis='x',rotation=40)
fig.tight_layout();fig.savefig(ROOT/'comparison.png',dpi=150)
