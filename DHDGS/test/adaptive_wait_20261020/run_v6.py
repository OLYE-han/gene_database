"""One-variable compute-thread ablation; every case uses fresh isolated services."""
import os
for key in ('OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'OMP_NUM_THREADS'):
    os.environ[key] = '1'
import argparse
import concurrent.futures
import copy
import gzip
import hashlib
import io
import json
from pathlib import Path
import shlex
import shutil
import statistics
import subprocess
import sys
import tarfile
import time
import yaml

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
BASE = PROJECT / 'test/managed_continuation_three_20261019'
SNAPSHOT = ROOT / 'source_v6'


def save(path, value):
    path.write_text(json.dumps(value, indent=2))


def experiment():
    return yaml.safe_load((ROOT / 'experiment_v6.yaml').read_text())


def remote(host, script, payload=None, binary=None):
    argv = [host['python'], '-c', script]
    if host['ssh']:
        argv = ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                host['ssh'], shlex.join(argv)]
    p = subprocess.run(argv, input=binary if binary is not None else json.dumps(payload).encode(),
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
    if p.returncode:
        raise RuntimeError(f"Host {host['id']}: {p.stderr.decode()[-3000:]}")
    return json.loads(p.stdout)


def initialize():
    cfg = experiment()
    base = yaml.safe_load((ROOT / cfg['base_plan']).read_text())
    SNAPSHOT.mkdir(exist_ok=True)
    manifest = {}
    files = [*sorted((PROJECT/'python/dhdgs').glob('*.py')),
             *sorted((PROJECT/'cpp').glob('*.cpp')), *sorted((PROJECT/'cpp').glob('*.hpp')),
             *sorted((PROJECT/'tools').glob('*.py')), PROJECT/'configs/dhdgs.yaml']
    for f in files:
        rel = f.relative_to(PROJECT); dest = SNAPSHOT/rel
        dest.parent.mkdir(parents=True, exist_ok=True); shutil.copy2(f, dest)
        manifest[str(rel)] = hashlib.sha256(f.read_bytes()).hexdigest()
    for f in (PROJECT/'python/dhdgs').glob('*.so'):
        shutil.copy2(f, SNAPSHOT/'python/dhdgs'/f.name)
    libs = PROJECT/'python/dhdgs/.native_libs'
    if libs.exists() and not (SNAPSHOT/'python/dhdgs/.native_libs').exists():
        shutil.copytree(libs, SNAPSHOT/'python/dhdgs/.native_libs')
    save(ROOT/'source_hashes_v6.json', manifest)
    blob = io.BytesIO()
    with tarfile.open(fileobj=blob, mode='w:gz') as tf:
        for rel in manifest:
            tf.add(SNAPSHOT/rel, arcname=rel)
    hosts = base['deployment']['hosts']
    for h in hosts:
        old = json.loads((BASE/f"deployment_node{h['id']}.json").read_text())
        h['source_project'] = old['project']
        h['root'] = str(ROOT/'data') if not h['ssh'] else str(Path(old['host']['root']).parent/ROOT.name)
        h['project'] = str(SNAPSHOT) if not h['ssh'] else str(Path(h['root'])/'source_v6')
    save(ROOT/'hosts_v6.json', hosts)
    deploy = r'''
import hashlib,io,json,os,shutil,socket,sys,tarfile
from pathlib import Path
p=json.loads(sys.argv[1]);target=Path(p['project']);old=Path(p['source_project'])
if p['ssh']:
 target.mkdir(parents=True,exist_ok=False)
 with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read()),mode='r:gz') as tf:
  for member in tf.getmembers():
   if not member.isfile() or '..' in Path(member.name).parts or Path(member.name).is_absolute():raise ValueError('Unsafe source member')
   dest=target/member.name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(tf.extractfile(member).read())
 for f in (old/'python/dhdgs').glob('*.so'):shutil.copy2(f,target/'python/dhdgs'/f.name)
 libs=old/'python/dhdgs/.native_libs'
 if libs.exists():shutil.copytree(libs,target/'python/dhdgs/.native_libs')
cpu=Path('/proc/cpuinfo').read_text();physical=set()
for block in cpu.split('\n\n'):
 d=dict(line.split(':',1) for line in block.splitlines() if ':' in line);d={k.strip():v.strip() for k,v in d.items()}
 if 'physical id' in d:physical.add((d['physical id'],d.get('core id')))
quota={}
for f in ('/sys/fs/cgroup/cpu.max','/sys/fs/cgroup/cpu/cpu.cfs_quota_us','/sys/fs/cgroup/cpu/cpu.cfs_period_us'):
 if Path(f).exists():quota[f]=Path(f).read_text().strip()
cpp_differences=[str(f.relative_to(target)) for f in (target/'cpp').glob('*') if f.is_file() and (old/f.relative_to(target)).exists() and f.read_bytes()!=(old/f.relative_to(target)).read_bytes()]
print(json.dumps(dict(hostname=socket.gethostname(),logical_cpus=os.cpu_count(),affinity=sorted(os.sched_getaffinity(0)),physical_cores=len(physical),quota=quota,loadavg=os.getloadavg(),cpp_differences=cpp_differences,hashes={str(f.relative_to(target)):hashlib.sha256(f.read_bytes()).hexdigest() for f in (target/'python/dhdgs').glob('*') if f.is_file()})))
'''
    def provision(h):
        # Script is static; JSON host metadata is shell-quoted by remote().
        script = 'import sys;sys.argv.append('+repr(json.dumps(h))+')\n'+deploy
        return h['id'], remote(h, script, binary=blob.getvalue())
    with concurrent.futures.ThreadPoolExecutor(3) as pool:
        hardware = dict(pool.map(provision, hosts))
    save(ROOT/'hardware_and_sources_v6.json', hardware)
    for sid, data in hardware.items():
        if data['cpp_differences']:
            raise RuntimeError(f'Native source differs from existing binary on {sid}')
        for rel, digest in manifest.items():
            if rel.startswith('python/') and data['hashes'].get(rel) != digest:
                raise RuntimeError(f'Python source mismatch on {sid}: {rel}')
    print('PROVISIONED', {i:dict(cpus=d['logical_cpus'],affinity=len(d['affinity'])) for i,d in hardware.items()},flush=True)


def launch(plan_path, case):
    plan = yaml.safe_load(plan_path.read_text())
    hosts = json.loads((ROOT/'hosts_v6.json').read_text())
    def start(h):
        target = str(Path(h['root'])/case.parent.name/case.name)
        script = '''
import json,os,socket,subprocess,sys
from pathlib import Path
p=json.load(sys.stdin);root=Path(p['root']);root.mkdir(parents=True,exist_ok=False)
plan=root/'cluster.yaml';plan.write_text(p['plan'])
argv=[sys.executable,str(Path(p['project'])/'tools/cluster_service.py'),'data','--plan',str(plan),'--node',str(p['id'])]
with (root/'data.log').open('wb',buffering=0) as log:
 child=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True,close_fds=True)
(root/'data.pid').write_text(str(child.pid))
print(json.dumps(dict(pid=child.pid,root=str(root),affinity=sorted(os.sched_getaffinity(child.pid)),hostname=socket.gethostname())))
'''
        return h['id'], remote(h, script, dict(root=target,project=h['project'],id=h['id'],plan=plan_path.read_text()))
    with concurrent.futures.ThreadPoolExecutor(3) as pool:
        launched = dict(pool.map(start, hosts))
    save(case/'launch.json', launched)
    sys.path[:0] = [str(SNAPSHOT/'python'), str(SNAPSHOT/'tools')]
    from dhdgs.managed_cluster import admin_rpc
    from dhdgs.managed_node import revision
    token = Path(plan['control']['token_file']).read_text().strip()
    deadline = time.monotonic()+180
    for n in plan['dhdgs']['cluster']['nodes']:
        while True:
            try:
                s = admin_rpc(n,token,'admin_status',timeout=2)
                assert s['revision']==revision(plan['dhdgs']) and s['quiescent']
                admin_rpc(n,token,'admin_resume',revision=s['revision']);break
            except (OSError,RuntimeError):
                if time.monotonic()>deadline:raise
                time.sleep(.5)


def stop(plan_path):
    sys.path[:0] = [str(SNAPSHOT/'python')]
    from dhdgs.managed_cluster import admin_rpc
    plan=yaml.safe_load(plan_path.read_text());token=Path(plan['control']['token_file']).read_text().strip()
    for n in plan['dhdgs']['cluster']['nodes']:
        s=admin_rpc(n,token,'admin_status')
        if not s['quiescent']:raise RuntimeError('Refusing to stop active data node')
        admin_rpc(n,token,'admin_shutdown')


def child(case):
    sys.path[:0]=[str(SNAPSHOT/'python'),str(SNAPSHOT/'tools')]
    from cluster_service import load_plan
    from dhdgs.managed_cluster import ClusterHead
    from dhdgs.managed_node import revision
    from full_truth import FullTruth
    plan=load_plan(case/'plan.yaml');spec=json.loads((case/'spec.json').read_text())
    truth=FullTruth(plan['control']['truth_plan'])
    head=ClusterHead(plan['dhdgs'],plan['control'],Path(plan['control']['token_file']).read_text().strip(),
                     lambda row,k:list(map(int,truth.ids[row,:k])))
    try:
        status=head.status();save(case/'before_status.json',status)
        assert all(n['quiescent'] and n['accepting'] and n['revision']==revision(head.c)
                   and n['worker_threads']==spec['threads'] for n in status['nodes'])
        head.ready=True
        save(case/'loaded_sources.json',{k:v.__file__ for k,v in sys.modules.items()
             if k.startswith('dhdgs') and getattr(v,'__file__',None)})
        result=head.benchmark({k:spec[k] for k in ('queries','concurrency','warmup')})
        result.update(threads=spec['threads'],phase=spec['phase'],variant=spec['variant'])
        save(case/'result.json',result);save(case/'after_status.json',head.status())
        print(json.dumps({k:result[k] for k in ('qps','latency_ms','recall_at_k_all_requests','failed_queries')}),flush=True)
        assert not result['failed_queries'] and all(result['audit'].values())
    finally:head.close()


def archive_worker_logs(case, scratch):
    target=case/'worker_results';target.mkdir(exist_ok=False)
    hashes={}
    for src in sorted(scratch.glob('*.jsonl')):
        digest=hashlib.sha256();dest=target/(src.name+'.gz')
        with src.open('rb') as r,gzip.open(dest,'wb',compresslevel=1) as w:
            while block:=r.read(1024*1024):digest.update(block);w.write(block)
        check=hashlib.sha256()
        with gzip.open(dest,'rb') as r:
            while block:=r.read(1024*1024):check.update(block)
        assert check.digest()==digest.digest()
        hashes[src.name]=dict(bytes=src.stat().st_size,sha256=digest.hexdigest())
        src.unlink()  # Only this case's verified temporary duplicate.
    save(target/'checksums.json',hashes)


def one_case(phase, index, threads):
    cfg=experiment();case=ROOT/phase/f'{index:02d}_{threads}'
    case.mkdir(parents=True,exist_ok=False)
    sys.path[:0]=[str(SNAPSHOT/'python'),str(SNAPSHOT/'tools')]
    from cluster_ctl import request
    production=yaml.safe_load((BASE/'cluster.yaml').read_text())
    old=request(production,'status')
    save(case/'production_before.json',old)
    assert old['ready'] and not old['busy'] and not old['active_queries']
    if shutil.disk_usage(ROOT).free<cfg['minimum_result_free_bytes']:
        raise RuntimeError('Insufficient result disk space; no existing artifacts will be deleted')
    plan=yaml.safe_load((ROOT/cfg['base_plan']).read_text())
    plan['control']['results_directory']=str(case)
    count=cfg[phase]['queries']
    selection=json.loads(Path(plan['control']['query_selection']).read_text())
    original=selection['validation'];selection['validation']=(original*((count+len(original)-1)//len(original)))[:count]
    save(case/'query_selection.json',selection)
    plan['control'].update(query_selection=str(case/'query_selection.json'),max_queries=count)
    c=plan['dhdgs'];c['peer']['worker_threads']=32
    variant=cfg['variants'][threads]
    c['peer']['admission'].update(variant.get('admission',{}))
    c['network'].update(variant.get('network',{}))
    c['runtime'].update(variant.get('runtime',{}))
    c['peer']['head_lanes'].update(variant.get('head_lanes',{}))
    c['peer']['connection_pool'].update(variant.get('connection_pool',{}))
    if 'adaptive_services' in variant:
        defaults=yaml.safe_load((SNAPSHOT/'configs/dhdgs.yaml').read_text())
        c['peer']['adaptive_services']=defaults['peer']['adaptive_services']
        c['peer']['adaptive_services'].update(variant['adaptive_services'])
    c['peer']['head_port']=cfg['callback_port_base']+8*(index+(20 if phase=='sustained' else 0))
    scratch=Path(cfg['scratch_root'])/phase/case.name;scratch.mkdir(parents=True,exist_ok=False)
    c['peer']['head_lanes']['results_directory']=str(scratch)
    for node in c['cluster']['nodes']:node['port']=cfg['data_port_base']+node['id']
    hosts=json.loads((ROOT/'hosts_v6.json').read_text())
    for h in hosts:
        h['state_file']=str(Path(h['root'])/phase/case.name/f"node_{h['id']}_state.json")
    plan['deployment']['hosts']=hosts
    (case/'plan.yaml').write_text(yaml.safe_dump(plan,sort_keys=False))
    spec=dict(phase=phase,variant=threads,threads=32,queries=count,concurrency=cfg['concurrency'],warmup=cfg['warmup'])
    save(case/'spec.json',spec)
    print('START',phase,case.name,flush=True)
    launch(case/'plan.yaml',case)
    try:
        with (case/'execution.log').open('w') as f:
            subprocess.run([sys.executable,__file__,'--child',str(case)],stdout=f,stderr=subprocess.STDOUT,
                           check=True,timeout=cfg['case_timeout_seconds'])
    finally:
        stop(case/'plan.yaml')
    archive_worker_logs(case,scratch)
    save(case/'production_after.json',request(production,'status'))
    result=json.loads((case/'result.json').read_text())
    print('DONE',case.name,'QPS',result['qps'],'p99',result['latency_ms']['p99'],flush=True)
    return result


def suite(phase):
    cfg=experiment()
    if phase=='short':
        for i,t in enumerate(cfg['short']['order'],start=24):
            if not (ROOT/phase/f'{i:02d}_{t}'/'result.json').exists():one_case(phase,i,t)
    else:
        grouped={t:[] for t in cfg['thread_counts']}
        for path in (ROOT/'short').glob('*/result.json'):
            r=json.loads(path.read_text())
            if r['failed_queries']==0 and r['recall_at_k_all_requests']>=cfg['minimum_recall'] and all(r['audit'].values()):
                grouped[r['threads']].append(r['qps'])
        assert all(len(v)==3 for v in grouped.values())
        medians={t:statistics.median(v) for t,v in grouped.items()}
        baseline=cfg['baseline_threads'];candidate=max((t for t in medians if t>baseline),key=medians.get)
        save(ROOT/'selection.json',dict(short_qps_medians=medians,baseline=baseline,candidate=candidate,
            policy='Highest median QPS among increased thread counts; sustained ABBA validates even a negative short result'))
        for i,label in enumerate(cfg['sustained']['order']):one_case(phase,i,baseline if label=='baseline' else candidate)


def resource_probe():
    script = r'''
import json,time,os
from pathlib import Path
before=Path('/proc/stat').read_text();time.sleep(1);after=Path('/proc/stat').read_text()
print(json.dumps(dict(loadavg=Path('/proc/loadavg').read_text(),
 before=[s for s in before.splitlines() if s.startswith(('cpu ','procs_'))],
 after=[s for s in after.splitlines() if s.startswith(('cpu ','procs_'))],
 pressure={s:Path('/proc/pressure/'+s).read_text() for s in ('cpu','io','memory') if Path('/proc/pressure/'+s).exists()})))
'''
    hosts=json.loads((ROOT/'hosts_v6.json').read_text())
    with concurrent.futures.ThreadPoolExecutor(3) as pool:
        values=dict(pool.map(lambda h:(h['id'],remote(h,script)),hosts))
    save(ROOT/f'host_pressure_{time.time_ns()}.json',values)
    print(json.dumps(values),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--initialize',action='store_true');p.add_argument('--phase',choices=['short','sustained']);p.add_argument('--child',type=Path);p.add_argument('--probe',action='store_true')
    a=p.parse_args()
    if a.initialize:initialize()
    elif a.probe:resource_probe()
    elif a.child:child(a.child)
    else:suite(a.phase)
