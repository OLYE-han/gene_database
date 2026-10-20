from pathlib import Path
import argparse,hashlib,json,shutil,yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
p=argparse.ArgumentParser();p.add_argument('group');p.add_argument('--phase',required=True);p.add_argument('--updates',default='{}');p.add_argument('--threads',type=int,default=100);a=p.parse_args()
base=P/'test/compute_startup_20261018/control_delay';d=R/a.group
if d.exists():raise RuntimeError('Refusing overwrite '+str(d))
d.mkdir();c=yaml.safe_load((base/'experiment.yaml').read_text());c['paths']['output']=str(d)
for key,value in json.loads(a.updates).items():
    target=c;parts=key.split('.')
    for part in parts[:-1]:target=target.setdefault(part,{})
    target[parts[-1]]=value
for i,node in enumerate(c['cluster']['nodes']):node['port']=20261+i
c['experiment']['benchmark_concurrency']=20*a.threads
(d/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
for name in ('run_load.py','query_selection.json'):shutil.copyfile(base/name,d/name)
plan=yaml.safe_load((base/'load_plan.yaml').read_text());plan['threads_per_process']=a.threads
(d/'load_plan.yaml').write_text(yaml.safe_dump(plan,sort_keys=False));(d/'index').symlink_to((base/'index').resolve(),target_is_directory=True)
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'python/dhdgs').glob('_native*.so'))+list((P/'cpp').glob('*.*'))+[P/'configs/dhdgs.yaml']
snapshot=R/(a.phase+'_snapshot')
for f in files:
    dst=snapshot/f.relative_to(P);dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(f,dst)
files+=list(d.glob('*.yaml'))+[d/'run_load.py',d/'query_selection.json']
(R/(a.phase+'_sources.json')).write_text(json.dumps({str(f.relative_to(P)):hashlib.sha256(f.read_bytes()).hexdigest() for f in files},indent=2))
print(d)
