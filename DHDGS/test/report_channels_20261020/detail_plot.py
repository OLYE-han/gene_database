"""One query: mutually exclusive report-wait subintervals, head clock only."""
from pathlib import Path
import csv,json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
R=Path(__file__).resolve().parent/'striped'
q=json.loads((R/'selected_query.json').read_text());r=q['stats']['head_receiver'];t=q['stats']['head_direct']
stamps=[t['distribution_send_done_ns'],r['terminal_publish_ns'],r['terminal_parent_received_ns'],
        r['terminal_parent_applied_ns'],r['completion_started_ns'],t['result_enqueued_ns']]
labels=['Search + report arrival + receiver validation','Terminal encode / IPC / parent scheduling',
        'Apply terminal state in parent','Wait for completion consumer (ACK gate included)',
        'Completion validation + cleanup + result submission']
rows=[dict(stage=k,start_ms=(a-q['client_start_ns'])/1e6,end_ms=(b-q['client_start_ns'])/1e6,
           duration_ms=(b-a)/1e6) for k,a,b in zip(labels,stamps,stamps[1:])]
assert all(x['duration_ms']>=0 for x in rows)
assert abs(sum(x['duration_ms'] for x in rows)-(stamps[-1]-stamps[0])/1e6)<1e-6
with (R/'selected_wait_detail.csv').open('w') as f:
    w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
fig,ax=plt.subplots(figsize=(12,5))
for i,x in enumerate(rows):
    ax.barh(i,x['duration_ms'],left=x['start_ms'],color=plt.cm.tab10(i))
    ax.text(x['end_ms']+2,i,f"{x['duration_ms']:.3f} ms",va='center',fontsize=9)
ack=(r['entry_ack_received_ns']-q['client_start_ns'])/1e6
ax.axvline(ack,color='black',linestyle='--',label=f'Entry ACK observed: {ack:.3f} ms')
ax.set_yticks(range(len(rows)),labels);ax.invert_yaxis();ax.set_xlabel('Time since API submission (ms), Intel-35 clock domain')
ax.set_xlim(0,q['client_wall_ms']*1.2);ax.legend(loc='lower right',fontsize=8)
ax.set_title(f"Query {q['query_id']}: report-wait interval {(stamps[-1]-stamps[0])/1e6:.3f} ms")
fig.tight_layout();fig.savefig(R/'query_wait_detail.png',dpi=170);plt.close(fig)
print(rows)
