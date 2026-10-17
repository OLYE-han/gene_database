"""Thread-count experiment; product code is unchanged. Resume completed runs only."""
from pathlib import Path
from datetime import datetime, timezone
import copy, hashlib, json, os, random, subprocess, sys, time
import numpy as np
import yaml
from threadpoolctl import threadpool_limits
from dhdgs.config import load_config
from dhdgs.data import Dataset, load_complete_npy, write_json
from dhdgs.benchmark import exact_topk, query_input

root=Path(__file__).resolve().parent
plan=yaml.safe_load((root/'plan.yaml').read_text())
base=yaml.safe_load((root/'base.yaml').read_text())
provenance=json.loads((root/'provenance.json').read_text())
for name,digest in provenance['source_sha256'].items():
    if hashlib.sha256(Path(name).read_bytes()).hexdigest()!=digest:
        raise RuntimeError('Product source changed: '+name)
index=Path(plan['index'])
if hashlib.sha256((index/'manifest.json').read_bytes()).hexdigest()!=provenance['index_manifest_sha256']:
    raise RuntimeError('Index changed')
log_dir=root/'execution_logs'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
log_dir.mkdir(parents=True)
cache_file=root/'exact_truth.json'
if not cache_file.exists():
    print('Preparing exact truth: new calibration rows 2048..2175, validation rows 4096..4351',flush=True)
    c=load_config(root/'base.yaml');dataset=Dataset(c['paths']['dataset'])
    indexed=load_complete_npy(index/'ids.npy')
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

commands=[]
def run(split,w,c,trial):
    name=f'w{w}_c{c}_trial{trial}'
    output=root/split/name;output.mkdir(parents=True,exist_ok=True)
    if not (output/'index').exists():(output/'index').symlink_to(index,target_is_directory=True)
    cfg=copy.deepcopy(base);cfg['paths']['output']=str(output)
    cfg['peer']['worker_threads']=w
    cfg['query']['row']=plan[split]['row']
    cfg['experiment'].update(benchmark_concurrency=c,benchmark_queries=plan[split]['queries'],
        benchmark_repetitions=plan[split]['repetitions'],warmup_queries=plan['warmup_per_worker'])
    path=output/'config.yaml'
    if path.exists():
        if yaml.safe_load(path.read_text())!=cfg:raise RuntimeError('Configuration changed: '+str(path))
    else:path.write_text(yaml.safe_dump(cfg,sort_keys=False))
    reports=sorted((output/'runs').glob('*/report.json'))
    if reports:
        p=reports[-1];r=json.loads(p.read_text())
    else:
        cmd=[sys.executable,str(root/'worker.py'),str(path)]
        started=time.time();load_before=os.getloadavg()
        log=log_dir/f'{split}_{name}.log'
        with log.open('x') as f:
            result=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
        commands.append({'label':split+'/'+name,'command':cmd,'returncode':result.returncode,
                         'start_time':started,'wall_seconds':time.time()-started,'load_before':load_before,'load_after':os.getloadavg()})
        (log_dir/'commands.json').write_text(json.dumps(commands,indent=2))
        if result.returncode:
            print(log.read_text()[-4000:],flush=True)
            raise SystemExit(result.returncode)
        message=json.loads(log.read_text().strip().splitlines()[-1]);p=Path(message['report']);r=json.loads(p.read_text())
    record={'stage':split,'threads':w,'concurrency':c,'trial':trial,'report':str(p),
            'qps':r['qps'],'recall':r['recall_at_k'],'failed':r['failed_queries'],'mean_ms':r['latency_ms']['mean'],
            'p99_ms':r['latency_ms']['p99'],'seconds':r['measured_wall_seconds'],'successful':r['successful_queries']}
    print(json.dumps(record),flush=True)
    return record

calibration=[]
for trial in range(plan['trials']):
    grid=[(w,c) for w in plan['worker_threads'] for c in plan['concurrency']]
    random.Random(plan['seed']+trial).shuffle(grid)
    for i,(w,c) in enumerate(grid):
        print(f'Calibration trial {trial+1}/{plan["trials"]}, point {i+1}/{len(grid)}',flush=True)
        calibration.append(run('calibration',w,c,trial))
        (root/'calibration_results.json').write_text(json.dumps(calibration,indent=2))

selection=[]
for c in plan['concurrency']:
    points=[]
    for w in plan['worker_threads']:
        runs=[r for r in calibration if r['threads']==w and r['concurrency']==c]
        latency=[]
        for item in runs:
            r=json.loads(Path(item['report']).read_text())
            latency.extend(q['stats']['latency_ms'] for q in r['queries'] if q['status']=='ok')
        points.append({'threads':w,'concurrency':c,'qps':sum(r['successful'] for r in runs)/sum(r['seconds'] for r in runs),
            'p99_ms':float(np.percentile(latency,99)) if latency else None,
            'recall':float(np.mean([r['recall'] for r in runs])),
            'eligible':all(r['failed']==0 and r['recall']>=plan['target_recall'] for r in runs),
            'trial_qps':[r['qps'] for r in runs]})
    eligible=[p for p in points if p['eligible']]
    if eligible:
        peak=max(eligible,key=lambda x:x['qps'])
        chosen=min(eligible,key=lambda x:(x['p99_ms'],-x['qps'],x['threads']))
        selection.append({'concurrency':c,'status':'selected','chosen':chosen,'peak':peak,'points':points})
    else:selection.append({'concurrency':c,'status':'no_feasible_configuration','points':points})
frozen={'criterion':plan['selection'],'selection':selection}
frozen_path=root/'selection_before_validation.json'
if frozen_path.exists():
    if json.loads(frozen_path.read_text())!=frozen:raise RuntimeError('Frozen selection differs')
else:write_json(frozen_path,frozen)
validation=[]
grid={(plan['baseline_threads'],c) for c in plan['concurrency']}
grid.update((s['chosen']['threads'],s['concurrency']) for s in selection if s['status']=='selected')
for trial in range(plan['trials']):
    pairs=sorted(grid);random.Random(plan['seed']+100+trial).shuffle(pairs)
    for i,(w,c) in enumerate(pairs):
        print(f'Validation trial {trial+1}/{plan["trials"]}, point {i+1}/{len(pairs)}',flush=True)
        validation.append(run('validation',w,c,trial))
        (root/'validation_results.json').write_text(json.dumps(validation,indent=2))
for name,digest in provenance['source_sha256'].items():
    if hashlib.sha256(Path(name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Source changed during run')
write_json(root/'completed.json',{'finished':datetime.now().isoformat(),'calibration_runs':len(calibration),'validation_runs':len(validation)})
print('COMPLETE '+str(root),flush=True)
