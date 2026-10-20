"""Freeze first-heartbeat and result-sender capacity ablations."""
from pathlib import Path
import hashlib,json,shutil,yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
base=R/'pool_reserved_w32'
specs=[('control_base',16,0),('control_delay',16,1000),('control_workers32',32,0),('control_both',32,1000)]
for group,workers,delay in specs:
    d=R/group
    if d.exists():raise RuntimeError('Refusing to overwrite '+str(d))
    d.mkdir()
    c=yaml.safe_load((base/'experiment.yaml').read_text())
    c['paths']['output']=str(d)
    c['peer']['control']={'initial_heartbeat_delay_ms':delay}
    c['peer']['communication']['control_workers']=workers
    (d/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
    for name in ('run_load.py','load_plan.yaml','query_selection.json'):
        shutil.copyfile(base/name,d/name)
    (d/'index').symlink_to((base/'index').resolve(),target_is_directory=True)
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'python/dhdgs').glob('_native*.so'))+list((P/'cpp').glob('*.*'))
for p in files:
    dst=R/'control_source_snapshot'/p.relative_to(P);dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,dst)
for group,_,_ in specs:
    files+=list((R/group).glob('*.yaml'))+[R/group/'run_load.py',R/group/'query_selection.json']
(R/'control_sources.json').write_text(json.dumps({str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
print([x[0] for x in specs])
