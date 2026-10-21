"""Isolated, repeatable full-index runtime comparison. Never touches 220xx services."""
import os
for key in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[key]='1'
import argparse,copy,hashlib,json,shlex,shutil,subprocess,sys,time
from pathlib import Path
import yaml
ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parents[1]
sys.path[:0]=[str(PROJECT/'python'),str(PROJECT/'tools')]
from dhdgs.config import load_config,validate
from dhdgs.managed_cluster import ClusterHead,admin_rpc
from dhdgs.managed_node import revision
from full_truth import FullTruth


def save(path,value):path.write_text(json.dumps(value,indent=2))

def base_config():
    plan=yaml.safe_load((PROJECT/'test/control_capacity_20261020/candidate.yaml').read_text())
    # load_config explicitly migrates archived fields from canonical YAML.
    archive=ROOT/'base_input.yaml';archive.write_text(yaml.safe_dump(plan['dhdgs']))
    c=load_config(archive);plan['dhdgs']={k:v for k,v in c.items() if not k.startswith('_')}
    return plan


def run_case(name,backend,topology='local',count=2000,concurrency=128,threads=6):
    case=ROOT/name;case.mkdir(parents=True,exist_ok=False)
    if (ROOT/'source_hashes.json').exists():shutil.copy2(ROOT/'source_hashes.json',case/'source_hashes.json')
    if (ROOT/'deployed_provenance.json').exists():shutil.copy2(ROOT/'deployed_provenance.json',case/'deployed_provenance.json')
    plan=base_config();c=plan['dhdgs'];native=backend=='cpp'
    c['native_runtime'].update(data_enabled=native,head_enabled=native,route_threads=4,merge_threads=4)
    c['peer']['worker_threads']=threads
    c['peer']['head_lanes'].update(enabled=not native,save_results=False,results_directory=str(case/'worker_results'))
    c['peer']['head_direct']['save_results']=False
    defaults=yaml.safe_load((PROJECT/'configs/dhdgs.yaml').read_text())
    c['peer'].setdefault('locking',copy.deepcopy(defaults['peer']['locking'])).update(split_native_registration=False,native_register_release_gil=False)
    c['peer'].setdefault('adaptive_services',copy.deepcopy(defaults['peer']['adaptive_services']))['enabled']=False
    c['peer']['head_port']=30610
    for n in c['cluster']['nodes']:n['port']=30551+n['id']
    if topology=='local':
        for n in c['cluster']['nodes']:
            n.update(host='127.0.0.1',index_directory=str(Path(c['paths']['output'])/'index'/f"shard_{n['id']}"))
        c['peer'].update(head_bind_host='127.0.0.1',head_advertise_host='127.0.0.1')
    elif topology!='three':raise ValueError('Unknown topology')
    for h in plan['deployment']['hosts']:
        if topology=='local':
            h.update(ssh=None,python=str(PROJECT/'.venv/bin/python'),project=str(PROJECT),root=str(case/f"data{h['id']}"),
                     token_file=plan['control']['token_file'])
        else:
            deployment=json.loads((ROOT/'deployment.json').read_text())[str(h['id'])]
            h.update(deployment);h['root']=str(Path(h['root'])/name)
        Path(h['root']).mkdir(parents=True,exist_ok=True) if not h['ssh'] else None
        h['state_file']=str(Path(h['root'])/'state.json')
    selected=json.loads(Path(plan['control']['query_selection']).read_text())['validation']
    selected=(selected*((count+len(selected)-1)//len(selected)))[:count]
    save(case/'selection.json',dict(validation=selected))
    plan['control'].update(results_directory=str(case),query_selection=str(case/'selection.json'),max_queries=count,
                           max_client_threads=concurrency,benchmark=dict(queries=count,concurrency=concurrency,warmup=32))
    validate(c);(case/'plan.yaml').write_text(yaml.safe_dump(plan,sort_keys=False))
    save(case/'spec.json',dict(backend=backend,topology=topology,count=count,concurrency=concurrency,threads=threads))
    children=[];launched=[]
    try:
        for h in plan['deployment']['hosts']:
            script='''import json,os,subprocess,sys
from pathlib import Path
p=json.load(sys.stdin);root=Path(p['root']);root.mkdir(parents=True,exist_ok=True)
plan=root/'plan.yaml';plan.write_text(p['plan'])
with (root/'data.log').open('wb',buffering=0) as log:
 child=subprocess.Popen([p['python'],str(Path(p['project'])/'tools/cluster_service.py'),'data','--plan',str(plan),'--node',str(p['id'])],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
print(json.dumps({'pid':child.pid,'root':str(root)}))
'''
            argv=[h['python'],'-c',script]
            if h['ssh']:argv=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8',h['ssh'],shlex.join(argv)]
            launch=subprocess.run(argv,input=json.dumps({**h,'plan':yaml.safe_dump(plan,sort_keys=False)}),text=True,capture_output=True,timeout=180,check=True)
            info=json.loads(launch.stdout);launched.append({'host':h,**info});save(case/'launch.json',launched)
        token=Path(plan['control']['token_file']).read_text().strip()
        for n in c['cluster']['nodes']:
            deadline=time.monotonic()+180
            while True:
                try:
                    status=admin_rpc(n,token,'admin_status',timeout=2)
                    assert status['quiescent'] and status['worker_threads']==threads
                    admin_rpc(n,token,'admin_resume',revision=revision(c));break
                except (OSError,RuntimeError):
                    if time.monotonic()>deadline:raise
                    time.sleep(.25)
        truth=FullTruth(plan['control']['truth_plan'])
        head=ClusterHead(c,plan['control'],token,lambda row,k:list(map(int,truth.ids[row,:k])))
        try:
            head.ready=True;save(case/'before.json',head.status())
            result=head.benchmark({});save(case/'result.json',result)
            save(case/'after.json',head.status())
            print(json.dumps({k:result[k] for k in ('qps','failed_queries','latency_ms','recall_at_k_all_requests','audit')}),flush=True)
            assert result['failed_queries']==0 and result['recall_at_k_all_requests']>=.95 and all(result['audit'].values())
        finally:head.close()
    finally:
        token=Path(plan['control']['token_file']).read_text().strip()
        for entry in launched:
            sid=entry['host']['id'];n=c['cluster']['nodes'][sid]
            try:admin_rpc(n,token,'admin_shutdown',timeout=3)
            except Exception as e:print('Shutdown:',sid,repr(e),flush=True)
        save(case/'finished.json',dict(timestamp=time.time()))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('name');p.add_argument('backend',choices=['cpp','python']);p.add_argument('--topology',default='local');p.add_argument('--count',type=int,default=2000);p.add_argument('--concurrency',type=int,default=128);p.add_argument('--threads',type=int,default=6)
    a=p.parse_args();run_case(a.name,a.backend,a.topology,a.count,a.concurrency,a.threads)
