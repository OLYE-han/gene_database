"""Finalize waiting-resource diagnostics only after timing workloads stop."""
import csv,gzip,importlib.util,json,os
from pathlib import Path
from collections import defaultdict
import numpy as np
ROOT=Path(__file__).resolve().parent
os.environ['MPLCONFIGDIR']=str(ROOT/'.mplconfig')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
spec=importlib.util.spec_from_file_location('phases',ROOT.parent/'control_capacity_20261020/analyze.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
rows=[];stages=[];conn=[];all_windows=[]
for path in sorted(ROOT.glob('*/*/result.json')):
 r=json.loads(path.read_text());run=Path(r['directory']);sums=defaultdict(float);vals=defaultdict(list);n=0;windows=defaultdict(list)
 worst=None;long_worst=None
 with gzip.open(run/'queries.jsonl.gz','rt') as f:
  for line in f:
   q=json.loads(line);n+=1
   for part in m.timeline(q):sums[part['stage']]+=part['duration_ms']
   for k in ['peer_transfer_queue_ms','peer_pool_wait_ms','peer_response_wait_ms','peer_send_ms','query_transfer_finalize_wait_ms']:
    vals[k].append(q['stats'].get(k,0))
   windows[int((q['client_end_ns']-q['offered_ns'])/1e9//10)].append(q['client_wall_ms'])
   if q['stats']['peer_branch_rpc_count']==1 and (worst is None or q['client_wall_ms']>worst['client_wall_ms']):worst=q
   if str(path.parent.relative_to(ROOT)) in ('30_old_long','31_new_long') and (long_worst is None or q['client_wall_ms']>long_worst['client_wall_ms']):long_worst=q
 if str(path.parent.relative_to(ROOT))=='07_owned':(ROOT/'connection_wait_query.json').write_text(json.dumps(worst,indent=2))
 if long_worst is not None:(ROOT/(str(path.parent.relative_to(ROOT))+'_slowest_query.json')).write_text(json.dumps(long_worst,indent=2))
 row=dict(case=str(path.parent.relative_to(ROOT)),variant=r['variant'],queries=n,qps=r['qps'],mean_ms=r['latency_ms']['mean'],p99_ms=r['latency_ms']['p99'],recall=r['recall_at_k_all_requests'],failed=r['failed_queries'],audit=all(r['audit'].values()))
 for k,values in vals.items():row[k]=float(np.mean(values));row[k+'_p99']=float(np.percentile(values,99))
 for k,v in r['query_metric_distributions'].items():row[k]=v['mean']
 row['physical_read_bytes']=sum(x['process_deltas']['storage_read_bytes'] or 0 for x in r['node_process_load'])
 row['head_cpu_cores']=sum(x['mean_cpu_cores'] for x in r['head_process_load'])
 row['head_rss_gib']=sum(x['process']['rss_bytes'] for x in r['head_process_load'])/2**30
 for x in r['node_process_load']:row[f"node{x['shard_id']}_cpu_cores"]=x['mean_cpu_cores']
 rows.append(row)
 for k,total in sums.items():stages.append(dict(case=str(path.parent.relative_to(ROOT)),stage=k,mean_ms=total/n))
 for b,lat in sorted(windows.items()):
  duration=min(10,r['measured_wall_seconds']-b*10)
  all_windows.append(dict(case=str(path.parent.relative_to(ROOT)),start_s=b*10,duration_s=duration,qps=len(lat)/duration,p99_ms=float(np.percentile(lat,99))))
 after=json.load(gzip.open(run/'nodes_after.json.gz','rt'))
 for sid,node in after.items():
  assert node['peer_lifecycle']['compute_queue']['workers']==32
  pool=node['peer_lifecycle'].get('adaptive_connections')
  if pool:conn.append(dict(case=str(path.parent.relative_to(ROOT)),node=sid,**pool))
for name,rs in [('full_metrics',rows),('head_stage_means',stages),('throughput_windows',all_windows)]:
 keys=list(dict.fromkeys(k for row in rs for k in row))
 with (ROOT/(name+'.csv')).open('w') as f:
  w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows(rs)
 (ROOT/(name+'.json')).write_text(json.dumps(rs,indent=2))
(ROOT/'connection_events.json').write_text(json.dumps(conn,indent=2))
print(json.dumps(rows,indent=2))
