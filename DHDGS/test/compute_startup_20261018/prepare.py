"""Freeze compute-startup ablations on the existing full seq_2M index."""
from pathlib import Path
import hashlib,json,shutil,yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
base=P/'test/execution_timestamps_20261018/sender_queued'
groups=[('executor',256),('prestarted',256)]+[('fixed',n) for n in (8,16,32,64,128,256)]
for mode,workers in groups:
    d=R/f'{mode}_w{workers}'
    if d.exists():raise RuntimeError('Refusing to overwrite '+str(d))
    d.mkdir()
    c=yaml.safe_load((base/'experiment.yaml').read_text())
    c['paths']['output']=str(d)
    c['peer']['worker_threads']=workers
    c['peer']['compute']={'mode':mode,'queue_capacity':2048}
    c['peer']['sender']={'mode':'queued','transfer_queue_capacity':2048}
    for i,n in enumerate(c['cluster']['nodes']):n['port']=20161+i
    (d/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
    for name in ('run_load.py','load_plan.yaml','query_selection.json'):
        shutil.copyfile(base/name,d/name)
    (d/'index').symlink_to((base/'index').resolve(),target_is_directory=True)
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'python/dhdgs').glob('_native*.so'))+list((P/'cpp').glob('*.*'))
snapshot=R/'source_snapshot'
for p in files:
    dst=snapshot/p.relative_to(P);dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,dst)
files+=list(R.glob('*/*.yaml'))+list(R.glob('*/run_load.py'))+list(R.glob('*/query_selection.json'))
(R/'sources.json').write_text(json.dumps({str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
print([f'{m}_w{w}' for m,w in groups])
