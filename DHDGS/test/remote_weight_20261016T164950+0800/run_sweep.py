from pathlib import Path
from datetime import datetime,timezone
import subprocess,sys,json,yaml
trial=Path(__file__).resolve().parent
root=trial.parent.parent
logs=trial/'execution_logs'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
logs.mkdir(parents=True)
records=[]
for w in yaml.safe_load((trial/'sweep.yaml').read_text())['weights_descending']:
    name=f'weight_{w:g}'
    cmd=[sys.executable,'-m','dhdgs','local-cluster-test','--config',str(trial/name/'config.yaml')]
    print('Running '+name,flush=True)
    with (logs/(name+'.log')).open('x') as f:
        result=subprocess.run(cmd,cwd=root,stdout=f,stderr=subprocess.STDOUT)
    records.append({'weight':w,'command':cmd,'returncode':result.returncode,'log':str(logs/(name+'.log'))})
    (logs/'commands.json').write_text(json.dumps(records,indent=2))
    print((logs/(name+'.log')).read_text()[-2500:],flush=True)
    if result.returncode:sys.exit(result.returncode)
print('Sweep complete: '+str(trial),flush=True)
