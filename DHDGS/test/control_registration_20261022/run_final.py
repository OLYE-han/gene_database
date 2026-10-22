"""Same-binary registration A/B; temporary large records on local root disk."""
from pathlib import Path
import json,subprocess,hashlib,shutil,time,os
os.environ['DHDGS_TEST_HEAD_SOURCE']=str(Path(__file__).resolve().parent/'source_head_v2')
import yaml
ROOT=Path(__file__).resolve().parent;PROJECT=ROOT.parents[1]
SCRATCH=Path('/tmp/dhdgs_control_registration_20261022/cases')
LAYOUT=dict(search=64,route=16,merge=4,data_tx=4,data_rx=8,data_handlers_per_rx=1,head_tx=4,head_rx=4,head_handlers_per_rx=4)
options=dict(count=20000,concurrency=256,search_budget=200,top_k=10,orders=[['sync','optimized'],['optimized','sync'],['sync','optimized']],layout=LAYOUT)
(ROOT/'final_experiment.yaml').write_text(yaml.safe_dump(options,sort_keys=False))
rows=[]
for repeat,order in enumerate(options['orders']):
 for mode in order:
  name=f'final_r{repeat}_{mode}';case=SCRATCH/name;persist=ROOT/'cases'/name;persist.mkdir(parents=True,exist_ok=True)
  layout={**LAYOUT,'queued_registration':mode=='optimized','move_head_reports':mode=='optimized'};lf=persist/'layout.yaml';lf.write_text(yaml.safe_dump(layout))
  if not (case/'result.json').exists():
   assert not case.exists(),f'Unfinished case: {case}'
   args=[str(PROJECT/'.venv/bin/python'),str(ROOT/'experiment.py'),str(case),'cpp','--topology','three','--count',str(options['count']),'--concurrency',str(options['concurrency']),'--head-connections','128','--network-mode','fixed','--connections','128','--control-capacity','1024','--early','--layout',str(lf)]
   with (persist/'execution.log').open('w') as log:
    result=subprocess.run(args,cwd=PROJECT,stdout=log,stderr=subprocess.STDOUT)
   if result.returncode:raise RuntimeError(f'{name} failed: {persist}/execution.log')
  s=json.loads((case/'result.json').read_text());run=Path(s['directory'])
  assert s['failed_queries']==0 and s['recall_at_k_all_requests']>=.95
  assert s['result_capture']['overflow_batches']==0
  assert all(v for k,v in s['audit'].items() if k!='metrics_complete')
  volume=Path('/var/tmp/dhdgs_control_registration_20261022')/name/'runs';volume.mkdir(parents=True,exist_ok=True)
  if not (persist/'runs').exists():(persist/'runs').symlink_to(volume,target_is_directory=True)
  archive=persist/'runs'/run.name;archive.mkdir(parents=True,exist_ok=True)
  manifest=[]
  # Enriched query records and lifecycle traces are sufficient for offline
  # timings. Redundant raw capture parts stay on scratch, not scarce NVMe.
  for source in [*case.glob('*.json'),*case.glob('*.yaml'),run/'queries.jsonl.gz',run/'nodes_before.json.gz',run/'nodes_after.json.gz']:
   dest=(archive if source.parent==run else persist)/source.name
   shutil.copy2(source,dest);manifest.append(dict(file=str(dest.relative_to(persist)),sha256=hashlib.sha256(dest.read_bytes()).hexdigest(),bytes=dest.stat().st_size))
  (persist/'archive_manifest.json').write_text(json.dumps(dict(scratch=str(case),persistent_run=str(archive),files=manifest),indent=2))
  row=dict(name=name,mode=mode,repeat=repeat,qps=s['qps'],p99=s['latency_ms']['p99'],mean=s['latency_ms']['mean'],recall=s['recall_at_k_all_requests'],queries=s['queries'] if 'queries' in s else options['count'],failed=s['failed_queries'],audit=s['audit'],directory=str(archive))
  rows.append(row);(ROOT/'final_progress.json').write_text(json.dumps(rows,indent=2));print(json.dumps(row),flush=True)
(ROOT/'final_completed.json').write_text(json.dumps(dict(time=time.time(),runs=len(rows)),indent=2))
