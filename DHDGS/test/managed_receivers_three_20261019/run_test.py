"""Three physical hosts, persistent head, unchanged query settings.

Verify Intel-32's shard, start the new cluster, retire the idle previous three-host
deployment only once the new cluster is ready, then repeat the benchmark.
"""
from pathlib import Path
import hashlib,json,shlex,subprocess,sys
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from cluster_ctl import request
from manage_cluster import Deployment
from dhdgs.managed_node import save_json

def ssh(h):
    return ['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10',
            '-o','ServerAliveInterval=60','-o','ServerAliveCountMax=3',
            '-o','ControlMaster=auto','-o','ControlPersist=300',
            '-o','ControlPath=/tmp/dhdgs-three-20261019-%C',h['ssh']]

e=yaml.safe_load((R/'experiment.yaml').read_text())
plan=load_plan(R/e['plan']);old=load_plan((R/e['previous_plan']).resolve())
d=Deployment(R/e['plan']);d.ssh=ssh
hosts=plan['deployment']['hosts']
save_json(R/'index_provenance.json',dict(reused=True,verification=str(P/'test/managed_three_hosts_20261019/shard_1_verification.json')))
hardware_code="""
import json,subprocess,sys,os,socket
print(json.dumps({'hostname':socket.gethostname(),'python':sys.version,
 'affinity':sorted(os.sched_getaffinity(0)),
 'cpu':json.loads(subprocess.check_output(['lscpu','-J'])),
 'disks':json.loads(subprocess.check_output(['lsblk','-J','-o','NAME,SIZE,TYPE,ROTA,MOUNTPOINT'])),
 'addresses':json.loads(subprocess.check_output(['ip','-j','addr']))}))
"""
hw={}
for h in hosts:
    argv=[h['python'],'-c',hardware_code]
    cmd=ssh(h)+[shlex.join(argv)] if h['ssh'] else argv
    hw[str(h['id'])]=json.loads(subprocess.check_output(cmd,timeout=180))
save_json(R/'hardware.json',hw)
old_status=request(old,'status');save_json(R/'previous_status.json',old_status)
assert not old_status['active_queries'] and not old_status['busy']
assert all(n.get('quiescent') for n in old_status['nodes'])
for h in hosts:d.start(h,'data')
d.start(hosts[0],'head');d.wait_head()
request(plan,'recover');initial=request(plan,'status');save_json(R/'initial_status.json',initial)
assert initial['ready'] and len({n['hostname'] for n in initial['nodes']})==3
assert len(initial['head_receivers'])==e['receiver_processes'] and all(w['alive'] and not w['error'] for w in initial['head_receivers'])
assert all(n['worker_threads']==e['worker_threads'] and n['settings']['search.max_expansions']==e['budget'] for n in initial['nodes'])
save_json(R/'previous_shutdown.json',dict(nodes=request(old,'stop_nodes'),head=request(old,'shutdown')))
print('Three-host cluster ready; previous idle cluster stopped',flush=True)
runs=[]
for repeat in range(e['repetitions']):
    result=request(plan,'benchmark',{k:e[k] for k in ('queries','concurrency','warmup')})
    save_json(R/f'run_{repeat}.json',result);runs.append(result)
    print(json.dumps({'repeat':repeat,**{k:result[k] for k in ('directory','qps','latency_ms','recall_at_k_all_requests','failed_queries')}}),flush=True)
    assert result['successful_queries']==e['queries'] and result['failed_queries']==0
    assert all(result['audit'].values())
save_json(R/'collection.json',request(plan,'collect'))
final=request(plan,'status');save_json(R/'final_status.json',final)
assert final['ready'] and all(n['quiescent'] for n in final['nodes'])
assert initial['head_receivers'][0]['pid']==final['head_receivers'][0]['pid']
assert all(w['alive'] and not w['active_queries'] and not w['error'] for w in final['head_receivers'])
same=all((a['pid'],a['index_fd'])==(b['pid'],b['index_fd']) for a,b in zip(initial['nodes'],final['nodes']))
assert same
save_json(R/'verification.json',dict(three_physical_hosts=True,same_pid_and_fd=same,
    all_audits_passed=True,successful_queries=sum(r['successful_queries'] for r in runs),
    worker_threads=e['worker_threads'],concurrency=e['concurrency'],budget=e['budget'],
    services_left_running=True,previous_cluster_stopped=True))
sources={str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest()
         for p in sorted(R.iterdir()) if p.suffix in ('.py','.yaml')}
save_json(R/'experiment_sources.json',sources)
