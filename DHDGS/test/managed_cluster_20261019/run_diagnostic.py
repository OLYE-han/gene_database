"""Two physical hosts ONLY; exercise persistent head + full-corpus shards.

Temporary data-1 is local because Intel-32 SSH is unavailable. Never report this
as a physical three-host measurement. All owned services are stopped in finally.
"""
from pathlib import Path
import copy,json,os,shlex,subprocess,sys,time
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path.insert(0,str(P/'tools'));sys.path.insert(0,str(P/'python'))
from cluster_ctl import request
from dhdgs.managed_cluster import admin_rpc
from dhdgs.managed_node import save_json
from dhdgs.config import validate

def main():
    root=R/'two_host_diagnostic';root.mkdir(exist_ok=True)
    plan=yaml.safe_load((R/'cluster.yaml').read_text());c=plan['dhdgs'];settings=plan['control']
    settings.update(results_directory=str(root),port=20981)
    c['peer']['head_port']=20980
    for i,n in enumerate(c['cluster']['nodes']):n['port']=20951+i
    c['cluster']['nodes'][1].update(host='192.168.33.35',index_directory=str(P/'test/seq2m_full_20261017/build/index/shard_1'))
    for h in plan['deployment']['hosts']:
        if h['id'] in (0,1):
            h.update(ssh=None,root=str(root),state_file=str(root/f"node_{h['id']}_state.json"),token_file=settings['token_file'])
        else:
            h['state_file']=h['root']+'/diagnostic/node_2_state.json'
    validate(c);path=root/'cluster.yaml';path.write_text(yaml.safe_dump(plan,sort_keys=False))
    remote=json.loads((R/'deployment_node2.json').read_text());h=plan['deployment']['hosts'][2]
    remoteplan=h['root']+'/diagnostic/cluster.yaml'
    ssh=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8','-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2','intel-30']
    script='from pathlib import Path;import sys;p=Path(sys.argv[1]);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(sys.stdin.buffer.read())'
    subprocess.run(ssh+[shlex.join([h['python'],'-c',script,remoteplan])],input=path.read_bytes(),check=True,timeout=30)
    procs=[];logs=[];token=Path(settings['token_file']).read_text().strip();ready=False
    try:
        for i in range(3):
            if i<2:args=[sys.executable,str(P/'tools/cluster_service.py'),'data','--plan',str(path),'--node',str(i)]
            else:args=ssh+[shlex.join([h['python'],remote['project']+'/tools/cluster_service.py','data','--plan',remoteplan,'--node','2'])]
            log=(root/f'data{i}.log').open('a');logs.append(log)
            procs.append(subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT))
        log=(root/'head.log').open('a');logs.append(log)
        procs.append(subprocess.Popen([sys.executable,str(P/'tools/cluster_service.py'),'head','--plan',str(path)],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT))
        deadline=time.monotonic()+240;last=None
        while time.monotonic()<deadline:
            if any(p.poll() is not None for p in procs):raise RuntimeError('A service exited; inspect logs')
            try:
                state=request(plan,'status')
                if all('error' not in n for n in state['nodes']):ready=True;break
                last=state
            except Exception as exc:last=str(exc)
            time.sleep(1)
        if not ready:raise RuntimeError('Startup timeout '+str(last))
        request(plan,'recover');initial=request(plan,'status');save_json(root/'initial_status.json',initial)
        print('ready: TWO PHYSICAL HOSTS, three logical shards',flush=True)
        baseline=request(plan,'benchmark',dict(queries=2000,concurrency=32,warmup=32));save_json(root/'baseline.json',baseline)
        print(json.dumps({'baseline':baseline}),flush=True)
        changed=request(plan,'configure',{'search.max_expansions':160,'scheduler.remote_weight':.5,'peer.worker_threads':6})
        save_json(root/'changed_status.json',changed)
        for a,b in zip(initial['nodes'],changed['nodes']):
            assert (a['pid'],a['index_fd'])==(b['pid'],b['index_fd'])
            assert b['settings']['search.max_expansions']==160 and b['settings']['scheduler.remote_weight']==.5
        second=request(plan,'benchmark',dict(queries=2000,concurrency=32,warmup=32));save_json(root/'changed_benchmark.json',second)
        print(json.dumps({'changed':second}),flush=True)
        request(plan,'configure',{'search.max_expansions':200,'scheduler.remote_weight':1.})
        final=request(plan,'status');save_json(root/'final_status.json',final)
        for a,b in zip(initial['nodes'],final['nodes']):assert (a['pid'],a['index_fd'])==(b['pid'],b['index_fd'])
        collected=request(plan,'collect');save_json(root/'collection.json',collected)
        save_json(root/'verification.json',dict(physical_hosts=2,logical_data_nodes=3,temporary_local_data1=True,
            same_pid_and_fd=True,configuration_restored=True,baseline=baseline['directory'],changed=second['directory'],collected=collected['directory']))
    finally:
        cleanup=[]
        for n in c['cluster']['nodes']:
            try:admin_rpc(n,token,'admin_shutdown',timeout=5);cleanup.append({'node':n['id'],'stop':'acknowledged'})
            except Exception as exc:cleanup.append({'node':n['id'],'stop_error':str(exc)})
        try:request(plan,'shutdown')
        except Exception as exc:cleanup.append({'head_stop_error':str(exc)})
        for p in procs:
            try:p.wait(timeout=20)
            except subprocess.TimeoutExpired:p.terminate();p.wait(timeout=10)
        for f in logs:f.close()
        save_json(root/'cleanup.json',cleanup)
if __name__=='__main__':main()
