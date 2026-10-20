"""Recreate fresh groups using the corrected lock-free job assignment template.

Existing groups are deliberately rejected; use a new experiment directory for
new source versions. Original shared-counter probe is in harness_probe/.
"""
from pathlib import Path
import hashlib,json,shutil,yaml
R=Path(__file__).resolve().parent;P=R.parents[1];B=P/'test/native_control_20261018/load_native'
source=(R/'driver.py.txt').read_text()
assert 'jobs_counter' not in source
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'python/dhdgs').glob('_native*.so'))+list((P/'cpp').glob('*.*'))+list((P/'python/dhdgs/.native_libs').glob('*'))+[P/'CMakeLists.txt',P/'configs/dhdgs.yaml']
for f in files:
 out=R/'snapshot'/f.relative_to(P);out.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(f,out)
for limit in (1,16,32,64,128,256,512,1000,2000):
 d=R/f'c{limit}';d.mkdir(exist_ok=False)
 c=yaml.safe_load((B/'experiment.yaml').read_text());c['paths']['output']=str(d);c['experiment']['benchmark_concurrency']=limit
 for i,node in enumerate(c['cluster']['nodes']):node['port']=20561+i
 (d/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
 plan=yaml.safe_load((B/'load_plan.yaml').read_text());plan.pop('threads_per_process');plan['threads_by_process']=[limit//20+int(i<limit%20) for i in range(20)]
 plan['offered_model']='2000 jobs ready at START; bounded inflight; disjoint worker-strided assignment without claim lock'
 (d/'load_plan.yaml').write_text(yaml.safe_dump(plan,sort_keys=False))
 shutil.copyfile(B/'query_selection.json',d/'query_selection.json');(d/'index').symlink_to((B/'index').resolve(),target_is_directory=True)
 (d/'run_load.py').write_text(source);compile(source,str(d/'run_load.py'),'exec')
 files+=list(d.glob('*.yaml'))+[d/'run_load.py',d/'query_selection.json']
(R/'sources.json').write_text(json.dumps({str(f.relative_to(P)):hashlib.sha256(f.read_bytes()).hexdigest() for f in files},indent=2))
print('Prepared nine groups; fixed 20 client processes and 40 warmup requests.')
