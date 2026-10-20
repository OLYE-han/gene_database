"""Transfer sender sweep, keeping control and pool settings fixed."""
from pathlib import Path
import hashlib,json,shutil,yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
base=R/'control_base'
specs=[('transfer32',32),('transfer64',64)]
for group,workers in specs:
    d=R/group
    if d.exists():raise RuntimeError('Refusing to overwrite '+str(d))
    d.mkdir()
    c=yaml.safe_load((base/'experiment.yaml').read_text())
    c['paths']['output']=str(d)
    c['peer']['communication']['transfer_workers']=workers
    (d/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
    for name in ('run_load.py','load_plan.yaml','query_selection.json'):
        shutil.copyfile(base/name,d/name)
    (d/'index').symlink_to((base/'index').resolve(),target_is_directory=True)
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'python/dhdgs').glob('_native*.so'))+list((P/'cpp').glob('*.*'))
for p in files:
    dst=R/'transfer_source_snapshot'/p.relative_to(P);dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,dst)
for group in ['control_base','control_delay','transfer32','transfer64']:
    files+=list((R/group).glob('*.yaml'))+[R/group/'run_load.py',R/group/'query_selection.json']
(R/'transfer_sources.json').write_text(json.dumps({str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
print(specs)
