from pathlib import Path
import json,os,shlex,subprocess,sys
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from cluster_ctl import request
plan=load_plan(P/'test/managed_35_30_20261019/cluster.yaml')
code="import json,subprocess,sys,os;print(json.dumps({'python':sys.version,'affinity':sorted(os.sched_getaffinity(0)),'cpu':json.loads(subprocess.check_output(['lscpu','-J'])),'route':json.loads(subprocess.check_output(['ip','-j','route','get',sys.argv[1]]))}))"
local=json.loads(subprocess.check_output([sys.executable,'-c',code,'192.168.33.30']))
h=plan['deployment']['hosts'][1]
remote=json.loads(subprocess.check_output(['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8','intel-30',shlex.join([h['python'],'-c',code,'192.168.33.35'])],timeout=30))
(R/'hardware.json').write_text(json.dumps({'Intel-35':local,'Intel-30':remote},indent=2))
ping=subprocess.run(['ping','-n','-c','30','-i','0.02','-W','1','192.168.33.30'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=40)
(R/'ping.log').write_bytes(ping.stdout)
(R/'production_final.json').write_text(json.dumps(request(plan,'status'),indent=2))
print('Hardware, RTT, and final production status recorded')
