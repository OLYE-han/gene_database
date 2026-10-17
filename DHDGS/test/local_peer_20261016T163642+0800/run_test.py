"""Reproduce this local three-process test; each invocation keeps separate logs."""
from pathlib import Path
from datetime import datetime, timezone
import json
import subprocess
import sys

trial=Path(__file__).resolve().parent
root=trial.parent.parent
stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
logs=trial/'execution_logs'/stamp
logs.mkdir(parents=True)
commands=[[sys.executable,'-m','unittest','discover','-s',str(root/'tests'),'-v']]
commands += [[sys.executable,'-m','dhdgs','local-cluster-test','--config',str(trial/f'concurrency_{n}'/'config.yaml')] for n in (1,4)]
records=[]
for name,cmd in zip(('regression','concurrency_1','concurrency_4'),commands):
    print('Running '+name,flush=True)
    with (logs/(name+'.log')).open('x') as out:
        result=subprocess.run(cmd,cwd=root,stdout=out,stderr=subprocess.STDOUT)
    records.append({'name':name,'command':cmd,'returncode':result.returncode,'log':str(logs/(name+'.log'))})
    (logs/'commands.json').write_text(json.dumps(records,indent=2))
    print((logs/(name+'.log')).read_text()[-5000:],flush=True)
    if result.returncode:sys.exit(result.returncode)
print('Logs: '+str(logs),flush=True)
