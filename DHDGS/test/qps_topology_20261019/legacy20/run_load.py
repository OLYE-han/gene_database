"""Experiment-only multi-process closed-loop driver; production search is unmodified."""
import os
for name in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[name]='1'
from pathlib import Path
import argparse,collections,gzip,json,queue,selectors,subprocess,sys,threading,time,traceback,uuid
import multiprocessing as mp
import numpy as np
import yaml
R=Path(__file__).resolve().parent;P=R.parents[2]
sys.path[:0]=[str(P/'python'),str(P/'tools')]
from dhdgs.config import load_config,snapshot
from dhdgs.managed_node import revision
from dhdgs.search import Engine
from dhdgs.transport import TCPTransport
from dhdgs.metrics import process_snapshot,distribution,export_query_csv,summarize_query_timings,summarize_per_query_timings,export_timing_csvs

def save(path,data):
 tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_text(json.dumps(data,allow_nan=False));tmp.replace(path)
def host_cpu():
 rows={}
 for line in Path('/proc/stat').read_text().splitlines():
  parts=line.split()
  if parts and parts[0].startswith('cpu'):
   vals=list(map(int,parts[1:]));rows[parts[0]]={'total':sum(vals[:8]),'busy':sum(vals[:3])+sum(vals[5:8])}
 return {'monotonic_ns':time.monotonic_ns(),'cpu':rows}
def client(index,run):
 c=load_config(R/'experiment.yaml');c['_cluster_revision']=revision(c);plan=yaml.safe_load((R/'load_plan.yaml').read_text());n=plan['threads_by_process'][index];total=c['experiment']['benchmark_queries']*c['experiment']['benchmark_repetitions'];np_=plan['processes']
 threading.stack_size(plan['native_thread_stack_bytes'])
 rows=json.loads((R/'query_selection.json').read_text())['validation'];qvec=np.load(c['query']['vectors'],mmap_mode='r');qids=np.load(c['query']['ids'],mmap_mode='r')
 t=time.monotonic();engine=Engine(c);load=time.monotonic()-t
 for i in range(plan['warmup_by_process'][index]):engine.search(qvec[rows[(index*2+i)%len(rows)]],int(qids[rows[(index*2+i)%len(rows)]]))
 # Shared Engine has read-only router/quantizer/metadata; all peer search state is per call.
 gate=threading.Event();ready=threading.Condition();ready_count=0
 output=queue.Queue();errors=[]
 def write():
  with gzip.open(run/f'client_{index}.jsonl.gz','wt',compresslevel=1) as f:
   while True:
    item=output.get()
    if item is None:break
    f.write(json.dumps(item,allow_nan=False)+'\n')
 writer=threading.Thread(target=write);writer.start()
 def worker(w):
  nonlocal ready_count
  with ready:ready_count+=1;ready.notify_all()
  gate.wait()
  first=sum(plan['threads_by_process'][:index])+w
  for job in range(first,total,sum(plan['threads_by_process'])):
   row=rows[job%len(rows)];qid=int(qids[row]);start=time.monotonic_ns()
   try:found=engine.search(qvec[row],qid)
   except Exception as exc:
    context=getattr(exc,'dhdgs_metrics',{})
    found={'status':'error','request_id':context.get('request_id',str(uuid.uuid4())),'ids':[],'squared_distances':[], 'error_type':type(exc).__name__,'error':str(exc),'stats':context.get('stats',{'metrics_complete':False,'latency_ms':(time.monotonic_ns()-start)/1e6})}
   end=time.monotonic_ns()
   found.update(job=job,worker_process=index,worker_thread=w,repetition=job//len(rows),query_row=row,query_id=qid,client_start_ns=start,client_end_ns=end,client_wall_ms=(end-start)/1e6,offered_ns=offered_ns,client_backlog_ms=(start-offered_ns)/1e6,offered_to_complete_ms=(end-offered_ns)/1e6)
   output.put(found)
 threads=[threading.Thread(target=worker,args=(i,)) for i in range(n)]
 for thread in threads:thread.start()
 with ready:ready.wait_for(lambda:ready_count==n)
 save(run/f'client_{index}.ready.json',{'pid':os.getpid(),'threads_ready':ready_count,'engine_load_seconds':load,'process':process_snapshot()})
 while not (run/'START.json').exists():time.sleep(.02)
 offered_ns=json.loads((run/'START.json').read_text())['monotonic_ns']
 gate.set()
 for thread in threads:thread.join()
 output.put(None);writer.join();engine.close()
 save(run/f'client_{index}.done.json',{'pid':os.getpid(),'process':process_snapshot()})

def client_process(index,run):
 fd=os.open(run/f'client_{index}.log',os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o644)
 os.dup2(fd,1);os.dup2(fd,2);os.close(fd)
 client(index,run)

def highload(c,run):
 plan=yaml.safe_load((R/'load_plan.yaml').read_text());processes=[];handles=[];transport=TCPTransport(c)
 ctx=mp.get_context('spawn')
 try:
  for i in range(plan['processes']):
   proc=ctx.Process(target=client_process,args=(i,run));proc.start();processes.append(proc)
  deadline=time.monotonic()+plan['startup_timeout_seconds']
  while len(list(run.glob('client_*.ready.json')))<len(processes):
   if any(p.exitcode is not None for p in processes):raise RuntimeError('Client failed during readiness; inspect client logs')
   if time.monotonic()>deadline:raise TimeoutError('Client startup deadline')
   time.sleep(.5)
  before=transport.metrics();save(run/'nodes_before.json',before)
  save(run/'cpu_before.json',host_cpu())
  start=time.monotonic_ns();save(run/'START.json',{'monotonic_ns':start,'configured_concurrency':c['experiment']['benchmark_concurrency'],'ready_threads':sum(json.loads(p.read_text())['threads_ready'] for p in run.glob('client_*.ready.json'))})
  print('START:',c['experiment']['benchmark_concurrency'],'client workers ready',flush=True)
  resource=[];deadline=time.monotonic()+plan['measurement_timeout_seconds'];last=start
  while any(p.exitcode is None for p in processes):
   resource.append({'host_cpu':host_cpu(),'monotonic_ns':time.monotonic_ns(),'clients':[process_snapshot(p.pid) for p in processes if p.exitcode is None],'nodes':[]})
   if time.monotonic()>deadline:raise TimeoutError('Measurement watchdog deadline')
   now=time.monotonic_ns()
   if now-last>30e9:print('progress: client processes finished',sum(p.exitcode is not None for p in processes),'/',len(processes),flush=True);last=now
   time.sleep(plan['resource_sample_interval_seconds'])
  assert all(p.exitcode==0 for p in processes),[p.exitcode for p in processes]
  save(run/'cpu_after.json',host_cpu())
  after=transport.metrics();save(run/'nodes_after.json',after)
  with gzip.open(run/'resource_samples.json.gz','wt',compresslevel=1) as f:json.dump(resource,f)
  qs=[]
  for i in range(len(processes)):
   with gzip.open(run/f'client_{i}.jsonl.gz','rt') as f:qs.extend(json.loads(line) for line in f)
  qs.sort(key=lambda q:q['job']);assert [q['job'] for q in qs]==list(range(c['experiment']['benchmark_queries']*c['experiment']['benchmark_repetitions']))
  from full_truth import FullTruth
  from dhdgs.data import Dataset
  truth=FullTruth(plan['baseline_plan']);dataset=Dataset(c['paths']['dataset']);indexed=np.load(R/'index/ids.npy',mmap_mode='r')
  cache={}
  for q in qs:
   row=q['query_row']
   if row not in cache:cache[row]=truth(dataset,indexed,truth.queries[row],10,'l2',q['query_id'],8192)
   ids,ds=cache[row];q.update(truth_ids=ids,truth_squared_distances=ds,recall=len(set(q['ids'])&set(ids))/10,returned_count=len(q['ids']))
  end=max(q['client_end_ns'] for q in qs);seconds=(end-start)/1e9;ok=[q for q in qs if q['status']=='ok'];bad=[q for q in qs if q['status']!='ok']
  events=sorted([(q['client_start_ns'],1) for q in qs]+[(q['client_end_ns'],-1) for q in qs]);active=peak=0;integral=0;last=start
  for stamp,delta in events:integral+=active*(stamp-last);active+=delta;peak=max(peak,active);last=stamp
  assert active==0
  nodes=[]
  for sid,cur in after.items():
   old=before[sid];proc,op=cur['process'],old['process'];span=(proc['monotonic_ns']-op['monotonic_ns'])/1e9
   deltas={k:proc[k]-op[k] for k in ('cpu_seconds','storage_read_bytes','storage_write_bytes')}
   nodes.append({'shard_id':sid,'process':proc,'process_window_seconds':span,'process_deltas':deltas,'mean_cpu_cores':deltas['cpu_seconds']/span,'counter_deltas':{k:v-old['counters'].get(k,0) for k,v in cur['counters'].items() if k not in ('active','peak_active')},'peer_lifecycle_after':cur.get('peer_lifecycle'),'peak_active_since_start':cur['counters']['peak_active']})
  result={'query_count':len(qs),'successful_queries':len(ok),'failed_queries':len(bad),'qps':len(ok)/seconds,'completed_qps':len(qs)/seconds,'measured_wall_seconds':seconds,'configured_concurrency':c['experiment']['benchmark_concurrency'],'actual_peak_inflight':peak,'mean_inflight':integral/(end-start),'latency_ms':distribution([q['client_wall_ms'] for q in ok]),'failure_latency_ms':distribution([q['client_wall_ms'] for q in bad]),'all_latency_ms':distribution([q['client_wall_ms'] for q in qs]),'recall_at_k_all_requests':sum(q['recall'] for q in qs)/len(qs),'recall_at_k_successes':sum(q['recall'] for q in ok)/len(ok) if ok else None,'error_types':dict(collections.Counter(q['error_type']+': '+q['error'] for q in bad)),'incomplete_success_metrics':sum(not q['stats'].get('metrics_complete',False) for q in ok),'node_process_load':nodes,'ground_truth_source':truth.provenance,'query_timing_summary':summarize_query_timings(qs),'queries':qs}
  result['client_backlog_ms']=distribution([q['client_backlog_ms'] for q in qs])
  result['offered_to_complete_ms']=distribution([q['offered_to_complete_ms'] for q in qs])
  export_query_csv(run/'queries.csv',qs);export_timing_csvs(run,result['query_timing_summary'],summarize_per_query_timings(qs))
  with gzip.open(run/'queries.jsonl.gz','wt',compresslevel=1) as f:
   for q in qs:f.write(json.dumps(q,allow_nan=False)+'\n')
  save(run/'summary.json',{k:v for k,v in result.items() if k not in ('queries','ground_truth_source','query_timing_summary','node_process_load')})
  return result
 finally:
  transport.close()
  for p in processes:
   if p.exitcode is None:p.terminate()
  for p in processes:
   p.join(timeout=10)
   if p.exitcode is None:p.kill();p.join()
  for h in handles:h.close()
  save(run/'clients_lifecycle.json',[{'pid':p.pid,'exit_code':p.exitcode,'stopped':p.exitcode is not None} for p in processes])

def main():
 from datetime import datetime
 from zoneinfo import ZoneInfo
 c=load_config(R/'experiment.yaml');c['_cluster_revision']=revision(c)
 for repeat in range(3):
  run=R/'runs'/(datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%dT%H%M%S')+f'_r{repeat}')
  snapshot(c,run)
  report=highload(c,run)
  with gzip.open(run/'report.json.gz','wt',compresslevel=1) as f:json.dump(report,f,allow_nan=False)
  print(json.dumps({'run':str(run),**{k:report[k] for k in ('query_count','successful_queries','failed_queries','qps','actual_peak_inflight','latency_ms')}}),flush=True)
  assert report['failed_queries']==0 and report['incomplete_success_metrics']==0
if __name__=='__main__':main()
