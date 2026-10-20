from pathlib import Path
import hashlib,json,yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
phases=[('sources.json','source_snapshot'),('pool_sources.json','pool_source_snapshot'),('control_sources.json','control_source_snapshot'),('transfer_sources.json','transfer_source_snapshot')]
checks=[]
for manifest,snapshot in phases:
    entries=json.loads((R/manifest).read_text())
    for name,digest in entries.items():
        archived=R/snapshot/name
        source=archived if archived.exists() else P/name
        assert hashlib.sha256(source.read_bytes()).hexdigest()==digest,(manifest,name)
    checks.append({'manifest':manifest,'verified_files':len(entries)})
def subset(a,b,path=''):
    if isinstance(a,dict):
        for k,v in a.items():subset(v,b[k],path+'.'+k)
    else:assert a==b,(path,a,b)
runs=[]
for p in sorted(R.glob('*/runs/*/resolved.yaml')):
    cfg=yaml.safe_load(p.read_text());original=yaml.safe_load((p.parents[2]/'experiment.yaml').read_text())
    subset(original,cfg)
    runs.append(str(p.parent.relative_to(R)))
(R/'provenance_verification.json').write_text(json.dumps({'frozen_manifests':checks,'resolved_configs_match':runs},indent=2))
files=list((P/'python/dhdgs').glob('*.py'))+list((P/'python/dhdgs').glob('_native*.so'))+list((P/'cpp').glob('*.*'))+list((P/'tests').glob('test_*.py'))+[P/'configs/dhdgs.yaml']+list(R.glob('*.py'))
(R/'final_sources.json').write_text(json.dumps({str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
print('Verified',len(runs),'resolved configurations and all',len(phases),'frozen source phases')
