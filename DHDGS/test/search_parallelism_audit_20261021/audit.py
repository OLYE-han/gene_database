import json,gzip,hashlib
from pathlib import Path
ROOT=Path(__file__).resolve().parent;PROJECT=ROOT.parents[1]
outputs=[]
for name in ['00_c256','03_c2048']:
 p=PROJECT/'test/concurrency_scale_20261020/three'/name;d=json.loads((p/'result.json').read_text());nodes={}
 with gzip.open(Path(d['directory'])/'queries.jsonl.gz','rt') as f:
  for line in f:
   for b in json.loads(line)['branch_reports']:
    t=b['native_timestamps'];node=b['node_id'];x=nodes.setdefault(node,dict(events=[],threads=set(),cpu_ms=0,branches=0,clock=t['clock_domain']))
    assert x['clock']==t['clock_domain'];x['events'] += [(t['native_search_start_ns'],1),(t['native_loop_done_ns'],-1)];x['threads'].add(b['compute_thread_id']);x['cpu_ms']+=b['stats']['server_thread_cpu_ms'];x['branches']+=1
 out={}
 for node,x in nodes.items():
  events=sorted(x['events']);active=peak=area=0;last=events[0][0]
  for ts,delta in events:area+=active*(ts-last);active+=delta;peak=max(peak,active);last=ts
  assert active==0;window=(events[-1][0]-events[0][0])/1e9
  out[node]=dict(distinct_compute_threads=len(x['threads']),peak_overlapping_search_intervals=peak,mean_overlapping_search_intervals=area/(window*1e9),search_interval_window_seconds=window,search_thread_cpu_seconds=x['cpu_ms']/1000,mean_search_cpu_cores=x['cpu_ms']/1000/window,branches=x['branches'])
 outputs.append(dict(case=name,nodes=out))
provenance={}
for file in ['native_pool.hpp','native_runtime.cpp','native_control.cpp','search_loop.hpp','runtime_bindings.hpp','native.cpp']:
 a=PROJECT/'cpp'/file;b=PROJECT/'test/cpp_runtime_20261020/source/cpp'/file
 provenance[file]=dict(current=hashlib.sha256(a.read_bytes()).hexdigest(),deployed_source=hashlib.sha256(b.read_bytes()).hexdigest())
(ROOT/'results.json').write_text(json.dumps(dict(cases=outputs,sources=provenance),indent=2));print(json.dumps(outputs,indent=2))
