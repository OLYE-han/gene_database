import csv
import json
import os
from pathlib import Path
import statistics

ROOT=Path(__file__).resolve().parent
os.environ['MPLCONFIGDIR']=str(ROOT/'.mplconfig')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

rows=json.loads((ROOT/'full_metrics.json').read_text())
groups={name:[r for r in rows if r['case'].startswith('short/') and r['variant']==name]
        for name in ['baseline','gil','split']}
fig,axes=plt.subplots(1,2,figsize=(11,4))
for ax,key,title in zip(axes,['qps','p99_ms'],['Complete-query QPS','Complete-query p99 (ms)']):
    names=[n for n,v in groups.items() if v]
    med=[statistics.median(float(v[key]) for v in groups[n]) for n in names]
    bars=ax.bar(names,med,color=['#777777','#4c78a8','#54a24b'][:len(names)])
    ax.bar_label(bars,fmt='%.1f')
    for i,n in enumerate(names):ax.scatter([i]*len(groups[n]),[float(v[key]) for v in groups[n]],color='black',s=20,zorder=3)
    ax.set_title(title);ax.set_ylim(0,max(med)*1.25);ax.grid(axis='y',alpha=.2)
fig.suptitle('Three hosts / 32 search workers per node / concurrency 256 / budget 200\nMedians with individual short-run observations')
fig.tight_layout();fig.savefig(ROOT/'short_comparison.png',dpi=160);plt.close(fig)

ipc=json.loads((ROOT/'ipc_gc.json').read_text())
# Deliberately select a diagnostic example with a retained GC event. The
# bounded GC ring can lose old events in sustained runs; absence is not proof
# of no pause. This example is not a random or representative latency sample.
chosen=max(ipc,key=lambda r:sum(e['overlap_ms'] for e in r['slowest_query']['ipc_gc_overlaps']))
case=ROOT/chosen['case'];q=json.loads((case/'slowest_query.json').read_text());t=q['stats']['head_lane']
head=json.loads((case/'slowest_head_timeline.json').read_text())
spans=[('Before encoding',t['worker_finished_ns'],t['ipc_pack_enter_ns']),
       ('Encode',t['ipc_pack_enter_ns'],t['ipc_pack_done_ns']),
       ('Before lock',t['ipc_pack_done_ns'],t['ipc_write_lock_enter_ns']),
       ('Write lock wait',t['ipc_write_lock_enter_ns'],t['ipc_write_lock_acquired_ns']),
       ('Send + parent receive/scheduling',t['ipc_write_lock_acquired_ns'],t['ipc_receive_done_ns']),
       ('Decode',t['ipc_receive_done_ns'],t['ipc_decode_done_ns'])]
fig,axes=plt.subplots(2,1,figsize=(14,10),gridspec_kw={'height_ratios':[2,1]})
axes[0].barh([r['stage'] for r in head],[r['duration_ms'] for r in head],left=[r['start_ms'] for r in head],color=plt.cm.tab20(np.linspace(0,1,len(head))))
for i,r in enumerate(head):axes[0].text(r['end_ms'],i,f" {r['duration_ms']:.2f}",va='center',fontsize=8)
axes[0].invert_yaxis();axes[0].margins(x=.12);axes[0].set_xlabel('ms since client submit, Intel-35 clock')
axes[0].set_title(f"Diagnostic example: {chosen['case']}, query {q['query_id']}, {q['client_wall_ms']:.2f} ms\nSelected for a retained parent-GC overlap; not a typical-query estimate")
origin=t['worker_finished_ns']
for i,(name,a,b) in enumerate(spans):
    axes[1].barh([name],[(b-a)/1e6],left=[(a-origin)/1e6],color=plt.cm.Set2(i/len(spans)))
    axes[1].text((b-origin)/1e6,i,f' {(b-a)/1e6:.3f}',va='center',fontsize=8)
gc=json.loads((Path(json.loads((case/'result.json').read_text())['directory'])/'head_gc.json').read_text())
for event in gc['after']['events']:
    a=max(event['start_ns'],origin);b=min(event['end_ns'],t['parent_received_ns'])
    if a<b and event['wall_ms']>1:axes[1].axvspan((a-origin)/1e6,(b-origin)/1e6,color='red',alpha=.12)
axes[1].invert_yaxis();axes[1].margins(x=.15);axes[1].set_xlabel('ms since result ready; red shading = retained parent GC event >1 ms')
axes[1].set_title('Output IPC breakdown: write-lock waiting is measured separately')
for ax in axes:ax.grid(axis='x',alpha=.2)
fig.tight_layout();fig.savefig(ROOT/'query_timeline.png',dpi=160);fig.savefig(ROOT/'query_timeline.svg');plt.close(fig)
print(chosen['case'],q['query_id'])

locks=json.loads((ROOT/'lock_metrics.json').read_text())
fig,axes=plt.subplots(2,1,figsize=(12,5.5),sharex=True)
for ax,name in zip(axes,['sustained/00_baseline','sustained/01_split']):
    node=next(r for r in locks if r['case']==name and r['node']=='1')
    event=next(e for e in node['longest_lock_waits'] if e['stage']=='accept_state_lock_enter_ns')
    origin=event['start_ns'];duration=event['wall_ms']
    ax.barh(['Acquire D1 interval'],[duration],color='#4c78a8')
    for g in event['gc_overlaps']:
        a=max(g['start_ns'],origin);b=min(g['end_ns'],event['end_ns'])
        ax.barh(['GC overlap'],[(b-a)/1e6],left=[(a-origin)/1e6],color='#e45756')
    overlap=sum(e['overlap_ms'] for e in event['gc_overlaps'])
    ax.set_title(f'Intel-32 {name}: acquire interval {duration:.2f} ms, GC overlap {overlap:.2f} ms')
    ax.invert_yaxis();ax.grid(axis='x',alpha=.2)
axes[-1].set_xlabel('ms since lock acquisition attempt, Intel-32 local clock for each run')
fig.suptitle('Long acquire intervals include process pauses; this is not pure mutex blocking time')
fig.tight_layout();fig.savefig(ROOT/'lock_gc_timeline.png',dpi=160);fig.savefig(ROOT/'lock_gc_timeline.svg');plt.close(fig)
