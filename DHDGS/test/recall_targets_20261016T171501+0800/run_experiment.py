"""Calibration grid, frozen target selection, then disjoint validation."""
from pathlib import Path
from datetime import datetime,timezone
import copy,hashlib,json,random,subprocess,sys,time
import numpy as np
import yaml
from threadpoolctl import threadpool_limits
from dhdgs.config import load_config
from dhdgs.data import Dataset,load_complete_npy,write_json
from dhdgs.benchmark import exact_topk,query_input

root=Path(__file__).resolve().parent
plan=yaml.safe_load((root/'plan.yaml').read_text());base=yaml.safe_load((root/'base.yaml').read_text())
log_dir=root/'execution_logs'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
log_dir.mkdir(parents=True)
cache_file=root/'exact_truth.json'
if not cache_file.exists():
    print('Preparing exact truth for 128 calibration and 256 validation queries...',flush=True)
    c=load_config(root/'base.yaml');dataset=Dataset(c['paths']['dataset'])
    indexed=load_complete_npy(Path(plan['index'])/'ids.npy')
    cache={'dataset':dataset.identity(),'indexed_ids_sha256':hashlib.sha256(indexed.tobytes()).hexdigest(),
           'indexed_rows':len(indexed),'metric':c['data']['metric'],'top_k':c['search']['top_k'],'entries':{}}
    begin=time.perf_counter()
    with threadpool_limits(limits=base['runtime']['blas_threads']):
        for split in ('calibration','validation'):
            c['query']['row']=plan[split]['row'];vectors,ids,_=query_input(c,plan[split]['queries'])
            for i,q in enumerate(vectors):
                qid=None if ids is None else int(ids[i]);excluded=qid if c['search']['exclude_self'] else None
                found,distances=exact_topk(dataset,indexed,q,c['search']['top_k'],c['data']['metric'],excluded,c['experiment']['exact_block_rows'])
                key=hashlib.sha256(np.asarray(q,dtype='<f4').tobytes()).hexdigest()+':'+str(excluded)
                cache['entries'][key]={'ids':found,'squared_distances':distances}
    dataset.assert_unchanged();cache['precompute_seconds']=time.perf_counter()-begin
    write_json(cache_file,cache)
    print(f"Truth ready in {cache['precompute_seconds']:.2f}s",flush=True)

def config_for(folder,budget,frontier,weight,split):
    output=root/split/folder;output.mkdir(parents=True,exist_ok=True)
    if not (output/'index').exists():(output/'index').symlink_to(plan['index'],target_is_directory=True)
    c=copy.deepcopy(base);c['paths']['output']=str(output)
    c['search'].update(max_expansions=budget,frontier_size=frontier)
    c['scheduler']['remote_weight']=weight
    spec=plan[split];c['query']['row']=spec['row']
    c['experiment'].update(benchmark_queries=spec['queries'],benchmark_repetitions=spec['repetitions'])
    path=output/'config.yaml'
    if not path.exists():path.write_text(yaml.safe_dump(c,sort_keys=False))
    return path

records=[]
def run(path,label):
    # Completed configurations are immutable and reusable when resuming a blocked run.
    reports=sorted((path.parent/'runs').glob('*/report.json'))
    if reports:
        p=reports[-1];r=json.loads(p.read_text())
        if r['failed_queries']:raise RuntimeError('Existing failed benchmark requires review')
        return p,r
    cmd=[sys.executable,str(root/'worker.py'),str(path)]
    with (log_dir/(label+'.log')).open('x') as f:
        result=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
    records.append({'label':label,'command':cmd,'returncode':result.returncode})
    (log_dir/'commands.json').write_text(json.dumps(records,indent=2))
    if result.returncode:
        print((log_dir/(label+'.log')).read_text()[-3500:],flush=True)
        raise SystemExit(result.returncode)
    message=json.loads((log_dir/(label+'.log')).read_text().strip().splitlines()[-1])
    print(label+' '+json.dumps(message),flush=True)
    p=Path(message['report']);return p,json.loads(p.read_text())

configs=[(b,p['frontier_size'],p['remote_weight']) for b in plan['budgets'] for p in plan['profiles']]
random.Random(plan['randomize_execution_seed']).shuffle(configs)
calibration=[]
for i,(budget,frontier,weight) in enumerate(configs):
    name=f'b{budget}_f{frontier}_w{weight:g}'
    print(f'Calibration {i+1}/{len(configs)}: '+name,flush=True)
    p,r=run(config_for(name,budget,frontier,weight,'calibration'),'cal_'+name)
    calibration.append({'name':name,'max_expansions':budget,'frontier_size':frontier,'remote_weight':weight,
                        'recall':r['recall_at_k'],'mean_ms':r['latency_ms']['mean'],
                        'p99_ms':r['latency_ms']['p99'],'qps':r['qps'],'report':str(p)})
    (root/'calibration_results.json').write_text(json.dumps(calibration,indent=2))
selection=[]
for target in plan['targets']:
    threshold=min(1.,target+plan['calibration_recall_margin'])
    feasible=[r for r in calibration if r['recall']>=threshold]
    if not feasible:
        selection.append({'target':target,'selection_threshold':threshold,'status':'no_calibration_configuration'})
        continue
    winner=min(feasible,key=lambda r:(r['mean_ms'],r['max_expansions'],r['frontier_size'],r['remote_weight']))
    selection.append({'target':target,'selection_threshold':threshold,'status':'selected',**winner})
# Freeze these choices before running any validation query. Never adapt to validation recall.
frozen={'selection':selection,'criterion':plan['selection']}
frozen_path=root/'selection_before_validation.json'
if frozen_path.exists():
    if json.loads(frozen_path.read_text())!=frozen:raise RuntimeError('Frozen calibration selection differs; use a new experiment directory')
else:write_json(frozen_path,frozen)
validated={}
for item in selection:
    if item['status']!='selected':continue
    name=item['name']
    if name not in validated:
        print('Validation: '+name,flush=True)
        p,r=run(config_for(name,item['max_expansions'],item['frontier_size'],item['remote_weight'],'validation'),'val_'+name)
        validated[name]=str(p)
    item['validation_report']=validated[name]
final={'selection':selection}
final_path=root/'final_selection.json'
if final_path.exists():
    if json.loads(final_path.read_text())!=final:raise RuntimeError('Existing final selection differs')
else:write_json(final_path,final)
print('Experiment complete: '+str(root),flush=True)
