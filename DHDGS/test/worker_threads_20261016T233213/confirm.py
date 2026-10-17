"""Longer runs of preselected candidates; no retuning on validation results."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,random,subprocess,sys,time,os
import yaml
root=Path(__file__).resolve().parent
plan=yaml.safe_load((root/'confirmation_plan.yaml').read_text())
assert (root/'completed.json').exists()
provenance=json.loads((root/'provenance.json').read_text())
for name,digest in provenance['source_sha256'].items():
    assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest,name
selection=json.loads((root/'selection_before_validation.json').read_text())['selection']
grid={(4,s['concurrency']) for s in selection}
grid.update((s['chosen']['threads'],s['concurrency']) for s in selection if s['status']=='selected')
log_dir=root/'execution_logs'/('confirmation_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
log_dir.mkdir(parents=True)
records=[];commands=[]
for trial in range(plan['trials']):
    pairs=sorted(grid);random.Random(plan['seed']+trial).shuffle(pairs)
    for i,(w,c) in enumerate(pairs):
        print(f'Confirmation trial {trial+1}/{plan["trials"]}, point {i+1}/{len(pairs)}: w={w}, c={c}',flush=True)
        output=root/'confirmation'/f'w{w}_c{c}_trial{trial}';output.mkdir(parents=True,exist_ok=True)
        cfg=yaml.safe_load((root/'validation'/f'w{w}_c{c}_trial0/config.yaml').read_text())
        cfg['experiment']['benchmark_repetitions']=plan['repetitions'];cfg['paths']['output']=str(output)
        if not (output/'index').exists():(output/'index').symlink_to(yaml.safe_load((root/'plan.yaml').read_text())['index'],target_is_directory=True)
        path=output/'config.yaml'
        if path.exists():assert yaml.safe_load(path.read_text())==cfg
        else:path.write_text(yaml.safe_dump(cfg,sort_keys=False))
        reports=sorted((output/'runs').glob('*/report.json'))
        if reports:p=reports[-1]
        else:
            log=log_dir/f'w{w}_c{c}_trial{trial}.log';cmd=[sys.executable,str(root/'worker.py'),str(path)]
            begin=time.time();load=os.getloadavg()
            with log.open('x') as f:result=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
            commands.append({'command':cmd,'returncode':result.returncode,'wall_seconds':time.time()-begin,'load_before':load,'load_after':os.getloadavg()})
            (log_dir/'commands.json').write_text(json.dumps(commands,indent=2))
            if result.returncode:
                print(log.read_text()[-4000:],flush=True);raise SystemExit(result.returncode)
            p=Path(json.loads(log.read_text().strip().splitlines()[-1])['report'])
        r=json.loads(p.read_text())
        row={'stage':'confirmation','threads':w,'concurrency':c,'trial':trial,'report':str(p),
             'qps':r['qps'],'recall':r['recall_at_k'],'failed':r['failed_queries'],'mean_ms':r['latency_ms']['mean'],
             'p99_ms':r['latency_ms']['p99'],'seconds':r['measured_wall_seconds'],'successful':r['successful_queries']}
        records.append(row);(root/'confirmation_results.json').write_text(json.dumps(records,indent=2))
        print(json.dumps(row),flush=True)
for name,digest in provenance['source_sha256'].items():
    assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest,name
(root/'confirmation_completed.json').write_text(json.dumps({'runs':len(records),'finished':datetime.now().isoformat()}))
print('Confirmation complete',flush=True)
