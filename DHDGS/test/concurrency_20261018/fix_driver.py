"""Archive the shared-counter probe, then remove its artificial admission lock."""
from pathlib import Path
import hashlib,json,shutil
R=Path(__file__).resolve().parent;P=R.parents[1]
probe=R/'harness_probe';probe.mkdir()
for name in ('sources.json','driver.py.txt','comparison.json','comparison.csv','final_runs.json','analysis_initial.log'):
 if (R/name).exists():shutil.copyfile(R/name,probe/name)
(probe/'README.md').write_text('Invalid high-concurrency benchmark: cross-process job-counter semaphore dominated at c2000 (actual peak197, claim p99 1796ms). These nine runs are retained for diagnosis only, excluded from final comparisons. Production code is unchanged. Final driver preassigns disjoint worker-strided jobs without any job-claim lock.\n')
source=(R/'driver.py.txt').read_text()
source=source.replace('def client(index,run,jobs_counter):','def client(index,run):')
source=source.replace("  while True:\n   claim_start=time.monotonic_ns()\n   with jobs_counter.get_lock():\n    job=jobs_counter.value\n    if job>=total:return\n    jobs_counter.value+=1\n   claim_end=time.monotonic_ns()", "  first=sum(plan['threads_by_process'][:index])+w\n  for job in range(first,total,sum(plan['threads_by_process'])):")
source=source.replace(',job_claim_ms=(claim_end-claim_start)/1e6', '')
source=source.replace('def client_process(index,run,jobs_counter):','def client_process(index,run):').replace(' client(index,run,jobs_counter)',' client(index,run)')
source=source.replace("ctx=mp.get_context('spawn');jobs_counter=ctx.Value('i',0)","ctx=mp.get_context('spawn')").replace('args=(i,run,jobs_counter)','args=(i,run)')
source=source.replace("  result['job_claim_ms']=distribution([q['job_claim_ms'] for q in qs])\n",'')
(R/'driver.py.txt').write_text(source)
manifest=json.loads((R/'sources.json').read_text())
for d in R.glob('c[0-9]*'):
 if not d.is_dir():continue
 (d/'runs').rename(d/'harness_probe_runs')
 (d/'run_load.py').write_text(source);compile(source,str(d/'run_load.py'),'exec')
 import yaml
 plan=yaml.safe_load((d/'load_plan.yaml').read_text());plan['offered_model']='2000 jobs ready at START; bounded inflight; disjoint worker-strided assignment without claim lock'
 (d/'load_plan.yaml').write_text(yaml.safe_dump(plan,sort_keys=False))
 for f in (d/'run_load.py',d/'load_plan.yaml'):manifest[str(f.relative_to(P))]=hashlib.sha256(f.read_bytes()).hexdigest()
(R/'sources.json').write_text(json.dumps(manifest,indent=2))
p=R/'analyze.py';s=p.read_text().replace("'offered_to_complete_ms','job_claim_ms'", "'offered_to_complete_ms'");p.write_text(s)
print('Archived probe and froze corrected driver.')
