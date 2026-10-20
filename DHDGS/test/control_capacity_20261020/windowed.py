"""Completion-rate windows for the sustained run; retain failures and edge windows."""
from pathlib import Path
import collections,csv,gzip,json
import numpy as np
import os
import yaml
R=Path(__file__).resolve().parent
os.environ.setdefault('MPLCONFIGDIR',str(R/'.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
results=list((R/'sustained').glob('*/result.json'))
assert len(results)==1
s=json.loads(results[0].read_text());groups=collections.defaultdict(list)
width=float(yaml.safe_load((R/'selection_policy.yaml').read_text())['window_seconds'])
assert width>0
with gzip.open(Path(s['directory'])/'queries.jsonl.gz','rt') as f:
    for line in f:
        q=json.loads(line);t=(q['client_end_ns']-q['offered_ns'])/1e9
        groups[int(t//width)].append(q)
rows=[]
for i,items in sorted(groups.items()):
    start=i*width;end=min((i+1)*width,s['measured_wall_seconds'])
    ok=[q for q in items if q['status']=='ok'];lat=[q['client_wall_ms'] for q in items]
    rows.append(dict(start_s=start,end_s=end,partial_window=(end-start)<width,
        completed=len(items),failed=len(items)-len(ok),successful_qps=len(ok)/(end-start),
        mean_ms=float(np.mean(lat)),p99_ms=float(np.percentile(lat,99)),
        mean_recall=float(np.mean([q['recall'] for q in items]))))
with (R/'sustained/windows.csv').open('w') as f:
    w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
fig,axes=plt.subplots(2,1,figsize=(10,7),sharex=True)
x=[(v['start_s']+v['end_s'])/2 for v in rows]
axes[0].plot(x,[v['successful_qps'] for v in rows],marker='o');axes[0].set_ylabel('Successful completions / s')
axes[1].plot(x,[v['p99_ms'] for v in rows],marker='o',color='tab:orange');axes[1].set_ylabel('Completion-window p99 (ms)')
axes[1].set_xlabel('Seconds since workload offered')
for ax in axes:ax.axvline(60,color='gray',linestyle='--');ax.grid(alpha=.25)
fig.suptitle(f"{s['query_count']:,} requests: {width:g}-second completion windows (last window may be partial)")
fig.tight_layout();fig.savefig(R/'sustained/windowed.png',dpi=160)
print(json.dumps(rows,indent=2))
