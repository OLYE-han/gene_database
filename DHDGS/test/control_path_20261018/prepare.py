from pathlib import Path
import hashlib,json,yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
base=P/'test/native_cpu256_q2000_20261018'
source=(base/'run_load.py').read_text()
default=yaml.safe_load((P/'configs/dhdgs.yaml').read_text())
for mode in ('legacy','optimized'):
 d=R/mode;d.mkdir(exist_ok=True)
 c=yaml.safe_load((base/'experiment.yaml').read_text());c['paths']['output']=str(d)
 c['peer']['communication']=dict(default['peer']['communication'],mode=mode)
 for i,n in enumerate(c['cluster']['nodes']):n['port']=20031+i
 (d/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
 (d/'load_plan.yaml').write_text((base/'load_plan.yaml').read_text())
 (d/'query_selection.json').write_text((base/'query_selection.json').read_text())
 (d/'run_load.py').write_text(source.replace('P=R.parents[1]','P=R.parents[2]'))
 if not (d/'index').exists():(d/'index').symlink_to(base/'index',target_is_directory=True)
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'python/dhdgs').glob('_native*.so'))+list((P/'cpp').glob('*.*'))+[P/'configs/dhdgs.yaml']
files+=list(R.glob('*/*.yaml'))+list(R.glob('*/run_load.py'))+list(R.glob('*/query_selection.json'))
(R/'sources.json').write_text(json.dumps({str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
