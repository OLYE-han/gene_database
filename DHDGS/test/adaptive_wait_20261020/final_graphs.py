"""Plot finalized aggregates; no performance workload should run concurrently."""
import csv
import json
import importlib.util
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.environ['MPLCONFIGDIR'] = str(ROOT / '.mplconfig')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


def read(name):
    with (ROOT / name).open() as f:
        return list(csv.DictReader(f))


metrics = {r['case']: r for r in read('full_metrics.csv')}
selected = ['00_baseline', '01_shared4', '05_elastic', '12_owned_fifo',
            '13_owned_connections', '14_owned_connections', '15_owned_fifo',
            '24_baseline_network', '25_nodelay', '26_network_auto',
            '27_network_auto', '28_nodelay', '29_head8_network_auto']
fig, axes = plt.subplots(1, 2, figsize=(14, 7), sharey=True)
for ax, key, title in zip(axes, ['qps', 'p99_ms'], ['Complete-query QPS', 'Complete-query p99 (ms)']):
    values = [float(metrics[c][key]) for c in selected]
    bars = ax.barh(np.arange(len(selected)), values, color='#4c78a8')
    ax.bar_label(bars, fmt='%.1f', padding=3, fontsize=8)
    ax.set_yticks(np.arange(len(selected)), selected)
    ax.set_xlim(0, max(values) * 1.2)
    ax.set_title(title)
    ax.grid(axis='x', alpha=.2)
axes[0].invert_yaxis()
fig.suptitle('3 hosts / 32 search workers per node / concurrency 256 / budget 200\n'
             'Selected short ablations, 10,000 requests each; see report for source/config differences')
fig.tight_layout()
fig.savefig(ROOT / 'short_comparison.png', dpi=150)
plt.close(fig)

long_cases = ['30_old_long', '31_new_long']
windows = read('throughput_windows.csv')
fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
for case, color in zip(long_cases, ['#e45756', '#4c78a8']):
    rows = [r for r in windows if r['case'] == case]
    full = [r for r in rows if float(r['duration_s']) >= 9.99]
    tail = [r for r in rows if float(r['duration_s']) < 9.99]
    for ax, key in zip(axes, ['qps', 'p99_ms']):
        ax.plot([float(r['start_s']) + 5 for r in full], [float(r[key]) for r in full],
                '.-', color=color, label=case)
        if tail:
            ax.scatter([float(r['start_s']) + float(r['duration_s'])/2 for r in tail],
                       [float(r[key]) for r in tail], marker='x', color=color,
                       label=case + ' partial final window')
        ax.grid(alpha=.2)
        ax.legend(fontsize=8)
axes[0].set_ylabel('Successful completions / second')
axes[1].set_ylabel('p99 of completed queries (ms)')
axes[1].set_xlabel('Seconds since benchmark offered the request set (head host clock)')
axes[0].set_title('100,000 requests per version: 10-second completion windows\n'
                  'Default implementation comparison, not an isolated lease-only ablation')
fig.tight_layout()
fig.savefig(ROOT / 'sustained_windows.png', dpi=160)
fig.savefig(ROOT / 'sustained_windows.svg')
plt.close(fig)

stages = read('head_stage_means.csv')
cases = ['00_baseline', '07_owned', '18_branch_owner_auto', *long_cases]
names = ['Route queue', 'Dispatch queue', 'Entry RPC incl. ACK',
         'Wait for complete reports', 'Merge queue', 'Client consumer']
fig, ax = plt.subplots(figsize=(13, 5))
left = np.zeros(len(cases))
for name, color in zip(names + ['Other measured stages'], plt.cm.tab10.colors):
    values = []
    for case in cases:
        values.append(sum(float(r['mean_ms']) for r in stages if r['case'] == case and
                          (r['stage'] == name if name != 'Other measured stages' else r['stage'] not in names)))
    ax.barh(cases, values, left=left, label=name, color=color)
    left += np.array(values)
for y, value in enumerate(left):
    ax.text(value + 2, y, f'{value:.1f} ms', va='center', fontsize=9)
ax.set_xlim(0, max(left) * 1.17)
ax.invert_yaxis()
ax.set_xlabel('Average client-submit-to-return latency (ms); mutually exclusive head intervals')
ax.set_title('Waiting shifts between stages\n"Wait for complete reports" also includes data search')
ax.legend(loc='upper center', bbox_to_anchor=(.5, -.17), ncol=3, fontsize=8)
fig.tight_layout()
fig.savefig(ROOT / 'stage_comparison.png', dpi=160, bbox_inches='tight')
plt.close(fig)

spec = importlib.util.spec_from_file_location('head_phases', ROOT.parent / 'control_capacity_20261020/analyze.py')
phases = importlib.util.module_from_spec(spec)
spec.loader.exec_module(phases)
q = json.loads((ROOT / '31_new_long_slowest_query.json').read_text())
diagnosis = json.loads((ROOT / 'long_wait_diagnosis.json').read_text())
panels = [dict(title=f"Head: query {q['query_id']} / {q['client_wall_ms']:.2f} ms / longest in new long run",
               xlabel='ms since client submit on Intel-35', rows=phases.timeline(q))]
for node in diagnosis:
    if node['case'] != '31_new_long':
        continue
    for t in node['slowest_query_traces']:
        stamps = [('Receive + accept', t['receiver_message_ready_ns']),
                  ('Admission queue', t['enqueued_ns']), ('Submit + executor', t['dequeued_ns']),
                  ('Worker prepare', t['worker_enter_ns']), ('Native loop', t['native_search_start_ns']),
                  ('Worker return', t['native_loop_done_ns']), ('Available', t['worker_available_ns'])]
        rows = [dict(stage=stamps[i-1][0], start_ms=(stamps[i-1][1]-stamps[0][1])/1e6,
                     end_ms=(v-stamps[0][1])/1e6, duration_ms=(v-stamps[i-1][1])/1e6)
                for i, (_, v) in enumerate(stamps) if i]
        panels.append(dict(title=f"Data node {node['node']}: its own receive-to-worker-release clock",
                           xlabel='ms since message ready on this data host; separate origin', rows=rows))
fig, axes = plt.subplots(len(panels), 1, figsize=(14, 13), gridspec_kw={'height_ratios': [2.4, 1, 1]})
for ax, panel in zip(axes, panels):
    rows = panel['rows']
    ax.barh([r['stage'] for r in rows], [r['duration_ms'] for r in rows],
            left=[r['start_ms'] for r in rows], color=plt.cm.tab20(np.linspace(0, 1, len(rows))))
    for i, r in enumerate(rows):
        ax.text(r['end_ms'], i, f"  {r['duration_ms']:.3f}", va='center', fontsize=8)
    ax.invert_yaxis()
    ax.set_xlim(0, max(r['end_ms'] for r in rows)*1.15)
    ax.set_title(panel['title'], fontsize=11)
    ax.set_xlabel(panel['xlabel'], fontsize=9)
    ax.grid(axis='x', alpha=.2)
fig.tight_layout()
fig.savefig(ROOT / 'current_long_query_timeline.png', dpi=160)
fig.savefig(ROOT / 'current_long_query_timeline.svg')
(ROOT / 'current_long_query_timeline.json').write_text(json.dumps(panels, indent=2))
plt.close(fig)
print(json.dumps({'plots': ['short_comparison.png', 'sustained_windows.png', 'stage_comparison.png',
                           'current_long_query_timeline.png']}))
