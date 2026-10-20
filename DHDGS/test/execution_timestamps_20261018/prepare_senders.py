"""Freeze same-code executor/queued sender comparisons with all timing probes."""
from pathlib import Path
import hashlib
import json
import shutil
import yaml

R = Path(__file__).resolve().parent
P = R.parents[1]
for mode in ('executor', 'queued'):
    d = R / ('sender_' + mode)
    if d.exists():
        raise RuntimeError('Refusing to overwrite an experiment')
    d.mkdir()
    c = yaml.safe_load((R / 'buffered/experiment.yaml').read_text())
    c['paths']['output'] = str(d)
    c['peer']['sender'] = {'mode': mode, 'transfer_queue_capacity': 2048}
    for i, node in enumerate(c['cluster']['nodes']):
        node['port'] = 20151 + i
    (d / 'experiment.yaml').write_text(yaml.safe_dump(c, sort_keys=False))
    for name in ('run_load.py', 'load_plan.yaml', 'query_selection.json'):
        shutil.copyfile(R / 'buffered' / name, d / name)
    (d / 'index').symlink_to((R / 'buffered/index').resolve(), target_is_directory=True)
files = (list((P / 'python/dhdgs').glob('*.py')) + list((P / 'python/dhdgs').glob('_native*.so'))
         + list((P / 'cpp').glob('*.*')) + list(R.glob('sender_*/*.yaml'))
         + list(R.glob('sender_*/*.py')) + list(R.glob('sender_*/query_selection.json')))
(R / 'sources_senders.json').write_text(json.dumps({str(p.relative_to(P)): hashlib.sha256(p.read_bytes()).hexdigest()
                                                   for p in files}, indent=2))
