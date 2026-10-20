from pathlib import Path
import hashlib,json,random,subprocess
import yaml
P=Path(__file__).resolve().parents[2];R=Path(__file__).resolve().parent
c=yaml.safe_load((P/'test/native_search_20261018/recommended_sync.yaml').read_text())
c['paths']['output']=str(R);c['peer']['worker_threads']=32;c['search']['max_expansions']=200
c['experiment']['warmup_queries']=2
for i,n in enumerate(c['cluster']['nodes']):n['port']=19731+i
(R/'experiment.yaml').write_text(yaml.safe_dump(c,sort_keys=False))
idx=P/'test/seq2m_full_20261017/build/index'
if not (R/'index').exists():(R/'index').symlink_to(idx,target_is_directory=True)
selection=json.loads((P/'test/native_search_20261018/query_selection.json').read_text())
(R/'frozen_query_selection.json').write_text(json.dumps(selection,indent=2))
(R/'random_selection_plan.json').write_text(json.dumps({'seed':2026101832200,'method':'random.Random(seed).randrange(1024), applied to measured queries sorted by query_row','population':1024,'selected_position':random.Random(2026101832200).randrange(1024)},indent=2))
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'python/dhdgs').glob('_native*.so'))+list((P/'cpp').glob('*.*'))+[R/'experiment.yaml',P/'test/native_search_20261018/run_config.py',P/'tools/full_truth.py',R/'frozen_query_selection.json']
(R/'source_manifest.json').write_text(json.dumps({str(f.relative_to(P)):hashlib.sha256(f.read_bytes()).hexdigest() for f in files},indent=2))
with (R/'environment.txt').open('w') as f:
 for cmd in [['date','-Is'],['hostname'],['lscpu'],['free','-h'],['df','-h',str(R)],['lsblk','-o','NAME,TYPE,SIZE,ROTA,MOUNTPOINTS'],['git','-C',str(P),'status','--short']]:
  f.write('\n$ '+' '.join(cmd)+'\n');f.flush();subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
print(R)
