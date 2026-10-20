from pathlib import Path
import yaml,json,hashlib
R=Path(__file__).resolve().parent;P=R.parents[1]
c=yaml.safe_load((P/'test/native_w32_b200_20261018/experiment.yaml').read_text())
c['paths']['output']=str(R);c['experiment']['benchmark_concurrency']=3000;c['experiment']['benchmark_repetitions']=6
for i,n in enumerate(c['cluster']['nodes']):n['port']=19831+i
(R/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
(R/'load_plan.yaml').write_text(yaml.safe_dump({'processes':30,'threads_per_process':100,'startup_timeout_seconds':600,'measurement_timeout_seconds':600,'native_thread_stack_bytes':1048576,'resource_sample_interval_seconds':0.5,'baseline_plan':str(P/'test/seq2m_full_20261017/backlog128/deployment.selected.yaml')}))
if not (R/'index').exists():(R/'index').symlink_to(P/'test/seq2m_full_20261017/build/index',target_is_directory=True)
(R/'query_selection.json').write_text((P/'test/native_search_20261018/query_selection.json').read_text())
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'cpp').glob('*.*'))+list((P/'python/dhdgs').glob('_native*.so'))+list(R.glob('*.py'))+list(R.glob('*.yaml'))
(R/'source_manifest.json').write_text(json.dumps({str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
