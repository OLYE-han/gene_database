"""Create paired pool ablations without rewriting phase-one provenance."""
from pathlib import Path
import hashlib,json,shutil,yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
base=R/'fixed_w32'
groups=['pool_locked_w32','pool_reserved_w32']
for group in groups:
    d=R/group
    if d.exists():raise RuntimeError('Refusing to overwrite '+str(d))
    d.mkdir()
    c=yaml.safe_load((base/'experiment.yaml').read_text())
    c['paths']['output']=str(d)
    c['peer']['connection_pool']={'probe_mode':group.split('_')[1]}
    (d/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
    for name in ('run_load.py','load_plan.yaml','query_selection.json'):
        shutil.copyfile(base/name,d/name)
    (d/'index').symlink_to((base/'index').resolve(),target_is_directory=True)
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'python/dhdgs').glob('_native*.so'))+list((P/'cpp').glob('*.*'))
snapshot=R/'pool_source_snapshot'
for p in files:
    dst=snapshot/p.relative_to(P);dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,dst)
for group in groups:
    files+=list((R/group).glob('*.yaml'))+[R/group/'run_load.py',R/group/'query_selection.json']
(R/'pool_sources.json').write_text(json.dumps({str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
print(groups)
