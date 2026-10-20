from pathlib import Path
import yaml,json,hashlib
import numpy as np
R=Path(__file__).resolve().parent;P=R.parents[1]
c=yaml.safe_load((P/'test/native_w32_b200_c3000_20261018/experiment.yaml').read_text())
c['paths']['output']=str(R)
c['peer'].update(worker_threads=256,max_active_queries=2048,control_timeout_seconds=5.0,handoff_timeout_seconds=30.0,lease_timeout_ms=30000,heartbeat_interval_ms=1000)
c['network']['listen_backlog']=4096
c['experiment'].update(benchmark_concurrency=2000,benchmark_queries=2000,benchmark_repetitions=1)
for i,n in enumerate(c['cluster']['nodes']):n['port']=19931+i
(R/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
plan=yaml.safe_load((P/'test/native_w32_b200_c3000_20261018/load_plan.yaml').read_text())
plan.update(processes=20,threads_per_process=100,resource_sample_interval_seconds=0.1)
(R/'load_plan.yaml').write_text(yaml.safe_dump(plan))
old=json.loads((P/'test/native_search_20261018/query_selection.json').read_text())
pop=np.setdiff1d(np.arange(old['population']),old['validation']+old['calibration'])
extra=np.random.default_rng(202610182000).choice(pop,2000-len(old['validation']),replace=False).tolist()
sel={'seed':202610182000,'population':old['population'],'calibration_excluded':old['calibration'],'original_validation':old['validation'],'extra':sorted(extra),'validation':sorted(old['validation']+extra)}
(R/'query_selection.json').write_text(json.dumps(sel,indent=2))
if not (R/'index').exists():(R/'index').symlink_to(P/'test/seq2m_full_20261017/build/index',target_is_directory=True)
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'cpp').glob('*.*'))+list((P/'python/dhdgs').glob('_native*.so'))+list(R.glob('*.py'))+list(R.glob('*.yaml'))+[R/'query_selection.json']
(R/'source_manifest.json').write_text(json.dumps({str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
