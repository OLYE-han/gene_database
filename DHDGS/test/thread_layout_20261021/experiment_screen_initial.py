"""Isolated, repeatable full-index runtime comparison. Never touches 220xx services."""
import os
for key in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[key]='1'
import argparse,copy,hashlib,json,shlex,shutil,subprocess,sys,time
from pathlib import Path
import yaml
ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parents[1]
BASE=PROJECT/'test/cpp_runtime_20261020'
LEGACY='--legacy-capture' in sys.argv
if LEGACY:sys.argv.remove('--legacy-capture')
HEAD_SOURCE=ROOT/'source'
sys.path[:0]=[str(HEAD_SOURCE/'python'),str(HEAD_SOURCE/'tools')]
from dhdgs.config import load_config,validate
from dhdgs.managed_cluster import ClusterHead,admin_rpc
from dhdgs.managed_node import revision
from full_truth import FullTruth


def save(path,value):path.write_text(json.dumps(value,indent=2))

def base_config():
    plan=yaml.safe_load((ROOT/'base_plan.yaml').read_text())
    # load_config explicitly migrates archived fields from canonical YAML.
    archive=ROOT/'base_input.yaml';archive.write_text(yaml.safe_dump(plan['dhdgs']))
    c=load_config(archive);plan['dhdgs']={k:v for k,v in c.items() if not k.startswith('_')}
    return plan


def run_case(name,backend,topology='local',count=2000,concurrency=128,threads=6,capture_limit=None,route_threads=4,head_connections=32,adaptive=False,max_lanes=8,network_mode="disabled",connections=32,control_capacity=64,early=False,layout_path=None):
    layout=yaml.safe_load(Path(layout_path).read_text())
    threads=layout['search'];route_threads=layout['route']
    case=ROOT/name;case.mkdir(parents=True,exist_ok=False)
    if (ROOT/'source_hashes.json').exists():shutil.copy2(ROOT/'source_hashes.json',case/'source_hashes.json')
    if (ROOT/'deployed_provenance.json').exists():shutil.copy2(ROOT/'deployed_provenance.json',case/'deployed_provenance.json')
    plan=base_config();c=plan['dhdgs'];native=backend=='cpp'
    save(case/'head_source.json',dict(legacy_capture=LEGACY,root=str(HEAD_SOURCE),files={str(p.relative_to(HEAD_SOURCE)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((HEAD_SOURCE/'python/dhdgs').glob('*.py'))},native_files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (HEAD_SOURCE/'python/dhdgs').glob('*.so')}))
    c['native_runtime'].update(data_enabled=native,head_enabled=native,route_threads=route_threads,merge_threads=layout['merge'])
    c['native_runtime']['adaptive_control']=copy.deepcopy(yaml.safe_load((PROJECT/'configs/dhdgs.yaml').read_text())['native_runtime']['adaptive_control'])
    c['native_runtime']['adaptive_control']['enabled']=False
    net=copy.deepcopy(yaml.safe_load((PROJECT/'configs/dhdgs.yaml').read_text())['native_runtime']['network_module'])
    net.update(enabled=network_mode!='disabled',send_max_reactors=4 if network_mode=='adaptive' else 1,receive_max_reactors=4 if network_mode=='adaptive' else 1,connections_per_endpoint_per_reactor=connections)
    net.update(send_min_reactors=layout['data_tx'],send_max_reactors=layout['data_tx'],receive_min_reactors=layout['data_rx'],receive_max_reactors=layout['data_rx'],handlers_per_receive_reactor=layout['data_handlers_per_rx'],connections_per_endpoint_per_reactor=128//layout['data_tx'],receive_queue_capacity_per_reactor=4096//layout['data_rx'])
    c['native_runtime']['network_module']=net
    c['native_runtime']['early_parent_report']=early
    c['peer']['communication']['max_pending_control']=control_capacity
    c['peer']['communication']['connections_per_endpoint']=connections
    c['native_runtime']['connections_per_endpoint']=connections
    c['native_runtime']['adaptive_control']['max_lanes']=max_lanes
    c['peer']['worker_threads']=threads
    if capture_limit is not None:
        if topology!='local' or LEGACY:raise ValueError('Capture-limit probe uses current local data servers')
        c['benchmark_results']=yaml.safe_load((PROJECT/'configs/dhdgs.yaml').read_text())['benchmark_results']
        c['benchmark_results']['memory_limit_bytes']=capture_limit
    c['peer']['head_lanes'].update(enabled=not native,save_results=False,results_directory=str(case/'worker_results'))
    c['peer']['head_direct']['save_results']=False
    defaults=yaml.safe_load((PROJECT/'configs/dhdgs.yaml').read_text())
    c['peer'].setdefault('locking',copy.deepcopy(defaults['peer']['locking'])).update(split_native_registration=False,native_register_release_gil=False)
    c['peer'].setdefault('adaptive_services',copy.deepcopy(defaults['peer']['adaptive_services']))['enabled']=False
    c['peer']['head_port']=37810
    for n in c['cluster']['nodes']:n['port']=37751+n['id']
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
                           max_client_threads=concurrency,benchmark=dict(queries=count,concurrency=concurrency,warmup=200))
    validate(c);(case/'plan.yaml').write_text(yaml.safe_dump(plan,sort_keys=False))
    save(case/'spec.json',dict(backend=backend,topology=topology,count=count,concurrency=concurrency,threads=threads,route_threads=route_threads,head_connections=head_connections,adaptive=False,max_lanes=max_lanes,network_mode=network_mode,connections=connections,control_capacity=control_capacity,early=early,layout=layout,source_commit='40933805ab6fbe5fc860c54bb3f9eab6877771bc'))
    children=[];launched=[]
    try:
        for h in plan['deployment']['hosts']:
            script='''import json,os,resource,subprocess,sys
from pathlib import Path
soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE,(min(65536,hard),hard))
p=json.load(sys.stdin);root=Path(p['root']);root.mkdir(parents=True,exist_ok=True)
plan=root/'plan.yaml';plan.write_text(p['plan'])
with (root/'data.log').open('wb',buffering=0) as log:
 child=subprocess.Popen([p['python'],str(Path(p['project'])/'tools/cluster_service.py'),'data','--plan',str(plan),'--node',str(p['id'])],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
print(json.dumps({'pid':child.pid,'root':str(root)}))
'''
            argv=[h['python'],'-c',script]
            if h['ssh']:argv=['ssh','-T','-o','ControlMaster=auto','-o','ControlPersist=1800','-o','ControlPath=/tmp/dhdgs-route-%r@%h:%p','-o','BatchMode=yes','-o','ConnectTimeout=8',h['ssh'],shlex.join(argv)]
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
        head_config=copy.deepcopy(c);head_config['native_runtime']['connections_per_endpoint']=head_connections
        hnet=head_config['native_runtime']['network_module']
        hnet.update(send_min_reactors=layout['head_tx'],send_max_reactors=layout['head_tx'],receive_min_reactors=layout['head_rx'],receive_max_reactors=layout['head_rx'],handlers_per_receive_reactor=layout['head_handlers_per_rx'],connections_per_endpoint_per_reactor=128//layout['head_tx'],receive_queue_capacity_per_reactor=4096//layout['head_rx'])
        validate(head_config);(case/'head_effective.yaml').write_text(yaml.safe_dump(head_config))
        assert revision(head_config)==revision(c)
        head=ClusterHead(head_config,plan['control'],token,lambda row,k:list(map(int,truth.ids[row,:k])))
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
    p.add_argument('--capture-limit',type=int)
    p.add_argument('--route-threads',type=int,default=4);p.add_argument('--head-connections',type=int,default=32)
    p.add_argument('--adaptive',action='store_true');p.add_argument('--max-lanes',type=int,default=8)
    p.add_argument('--network-mode',choices=['disabled','fixed','adaptive'],default='disabled')
    p.add_argument('--connections',type=int,default=32);p.add_argument('--control-capacity',type=int,default=64)
    p.add_argument('--early',action='store_true')
    p.add_argument('--layout',required=True)
    a=p.parse_args();run_case(a.name,a.backend,a.topology,a.count,a.concurrency,a.threads,a.capture_limit,a.route_threads,a.head_connections,a.adaptive,a.max_lanes,a.network_mode,a.connections,a.control_capacity,a.early,a.layout)
