"""Freeze direct/buffered admission configs; do not overwrite completed runs."""
from pathlib import Path
import hashlib,json,yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
base=P/'test/control_path_20261018/optimized'
for mode in ('direct','buffered'):
    d=R/mode;d.mkdir(exist_ok=True)
    if (d/'runs').exists():raise RuntimeError('Refusing to overwrite an executed experiment')
    c=yaml.safe_load((base/'experiment.yaml').read_text())
    c['paths']['output']=str(d)
    c['peer']['admission']={'mode':mode,'queue_capacity':2048}
    for i,n in enumerate(c['cluster']['nodes']):n['port']=20131+i
    (d/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
    for name in ('load_plan.yaml','query_selection.json','run_load.py'):(d/name).write_text((base/name).read_text())
    if not (d/'index').exists():(d/'index').symlink_to((base/'index').resolve(),target_is_directory=True)
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'python/dhdgs').glob('_native*.so'))+list((P/'cpp').glob('*.*'))+[P/'configs/dhdgs.yaml']
files+=list(R.glob('*/*.yaml'))+list(R.glob('*/run_load.py'))+list(R.glob('*/query_selection.json'))
(R/'sources.json').write_text(json.dumps({str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
