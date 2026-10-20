"""Freeze a fresh buffered-admission run without overwriting prior experiments."""
from pathlib import Path
import hashlib
import json
import shutil
import yaml

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
BASE = PROJECT / 'test/admission_queue_20261018/buffered'
DEST = ROOT / 'buffered'
if DEST.exists():
    raise RuntimeError('Refusing to overwrite the prepared experiment')
DEST.mkdir()
config = yaml.safe_load((BASE / 'experiment.yaml').read_text())
config['paths']['output'] = str(DEST)
for index, node in enumerate(config['cluster']['nodes']):
    node['port'] = 20141 + index
(DEST / 'experiment.yaml').write_text(yaml.safe_dump(config, sort_keys=False))
for name in ('load_plan.yaml', 'query_selection.json', 'run_load.py'):
    shutil.copyfile(BASE / name, DEST / name)
(DEST / 'index').symlink_to((BASE / 'index').resolve(), target_is_directory=True)
(ROOT / 'analysis.yaml').write_text(yaml.safe_dump({
    'selection_seed': 20261018,
    'selection_population': 'successful_multi_node_queries_sorted_by_query_row',
}, sort_keys=False))
files = (list((PROJECT / 'python/dhdgs').glob('*.py'))
         + list((PROJECT / 'python/dhdgs').glob('_native*.so'))
         + list((PROJECT / 'cpp').glob('*.*'))
         + list(DEST.glob('*.yaml')) + list(DEST.glob('*.py'))
         + [DEST / 'query_selection.json', ROOT / 'analysis.yaml'])
(ROOT / 'sources.json').write_text(json.dumps({
    str(p.relative_to(PROJECT)): hashlib.sha256(p.read_bytes()).hexdigest()
    for p in files
}, indent=2))
print(DEST)
