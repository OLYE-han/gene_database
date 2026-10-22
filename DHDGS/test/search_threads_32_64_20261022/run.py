"""Balanced, same-binary comparison; all treatment settings live in experiment.yaml."""
from pathlib import Path
import concurrent.futures, hashlib, json, os, shlex, shutil, subprocess, time
import yaml

ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parents[1]
OPTIONS=yaml.safe_load((ROOT/'experiment.yaml').read_text())
os.environ['DHDGS_TEST_HEAD_SOURCE']=str(PROJECT/OPTIONS['head_source'])

def save(path,data):
    path.write_text(json.dumps(data,indent=2))

def preflight():
    deployment=json.loads((ROOT/'deployment.json').read_text())
    previous=json.loads((ROOT/'deployed_provenance.json').read_text())
    script='''import sys,json,hashlib,socket,os
from pathlib import Path
p=json.load(sys.stdin);sys.path.insert(0,p['project']+'/python')
from dhdgs import _native
f=Path(_native.__file__);control=Path(p['project'])/'cpp/native_control.cpp'
for port in [37751,37752,37753,37810]:
 s=socket.socket();s.bind(('0.0.0.0',port));s.close()
print(json.dumps(dict(native_sha256=hashlib.sha256(f.read_bytes()).hexdigest(),control_sha256=hashlib.sha256(control.read_bytes()).hexdigest(),python=sys.version,loadavg=os.getloadavg(),ports_free=True)))
'''
    def check(item):
        sid,h=item;args=[h['python'],'-c',script]
        if h['ssh']:args=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8',h['ssh'],shlex.join(args)]
        result=subprocess.run(args,input=json.dumps(h),text=True,capture_output=True,timeout=60,check=True)
        result=json.loads(result.stdout)
        for key in ['native_sha256','control_sha256']:assert result[key]==previous[sid][key],(sid,key)
        return sid,result
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:results=dict(pool.map(check,deployment.items()))
    hashes=json.loads((ROOT/'head_v2_hashes.json').read_text())
    # Current application code equals the frozen head snapshot except for the
    # intentional default enablement of the two runtime switches.
    checked={}
    for rel,expected in hashes.items():
        if not (rel.startswith('cpp/') or (rel.startswith('python/') and rel.endswith('.py'))):continue
        actual=hashlib.sha256((PROJECT/rel).read_bytes()).hexdigest()
        assert actual==expected,(rel,'workspace differs from frozen source')
        checked[rel]=actual
    results['workspace_source_hashes']=checked
    results['head_binary_hashes']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (PROJECT/OPTIONS['head_source']/'python/dhdgs').glob('*.so')}
    save(ROOT/'preflight.json',results)

def archive(case,persist,result):
    run=Path(result['directory']);volume=Path(OPTIONS['archive'])/case.name/'runs'
    volume.mkdir(parents=True,exist_ok=True)
    if not (persist/'runs').exists():(persist/'runs').symlink_to(volume,target_is_directory=True)
    dest_run=persist/'runs'/run.name;dest_run.mkdir(parents=True,exist_ok=True)
    manifest=[]
    for source in [*case.glob('*.json'),*case.glob('*.yaml'),run/'queries.jsonl.gz',run/'nodes_before.json.gz',run/'nodes_after.json.gz']:
        dest=(dest_run if source.parent==run else persist)/source.name
        shutil.copy2(source,dest)
        manifest.append(dict(file=str(dest.relative_to(persist)),sha256=hashlib.sha256(dest.read_bytes()).hexdigest(),bytes=dest.stat().st_size))
    save(persist/'archive_manifest.json',dict(scratch=str(case),persistent_run=str(dest_run.resolve()),files=manifest))
    return dest_run

def main():
    preflight();rows=[]
    for repeat,order in enumerate(OPTIONS['orders']):
        for threads in order:
            name=f'r{repeat}_threads{threads}';case=Path(OPTIONS['scratch'])/'cases'/name;persist=ROOT/'cases'/name
            persist.mkdir(parents=True,exist_ok=True)
            layout={**OPTIONS['layout'],'search':threads}
            lf=persist/'layout.yaml';lf.write_text(yaml.safe_dump(layout))
            if not (case/'result.json').exists():
                assert not case.exists(),f'Unfinished case: {case}'
                args=[str(PROJECT/'.venv/bin/python'),str(ROOT/'experiment.py'),str(case),'cpp','--topology','three','--count',str(OPTIONS['count']),'--concurrency',str(OPTIONS['concurrency']),'--head-connections',str(OPTIONS['connections']),'--network-mode','fixed','--connections',str(OPTIONS['connections']),'--control-capacity',str(OPTIONS['control_capacity']),'--early','--layout',str(lf)]
                print(f'START {name}',flush=True)
                with (persist/'execution.log').open('w') as log:
                    result=subprocess.run(args,cwd=PROJECT,stdout=log,stderr=subprocess.STDOUT)
                if result.returncode:raise RuntimeError(f'{name} failed: {persist}/execution.log')
            s=json.loads((case/'result.json').read_text())
            assert s['failed_queries']==0 and s['recall_at_k_all_requests']>=.95
            assert s['result_capture']['overflow_batches']==0
            assert all(v for k,v in s['audit'].items() if k!='metrics_complete')
            spec=json.loads((case/'spec.json').read_text());assert spec['threads']==threads
            dest_run=archive(case,persist,s)
            row=dict(name=name,mode=f'threads{threads}',threads=threads,repeat=repeat,qps=s['qps'],p99=s['latency_ms']['p99'],mean=s['latency_ms']['mean'],recall=s['recall_at_k_all_requests'],queries=OPTIONS['count'],failed=s['failed_queries'],audit=s['audit'],directory=str(dest_run))
            rows.append(row);save(ROOT/'progress.json',rows);print(json.dumps(row),flush=True)
    save(ROOT/'completed.json',dict(time=time.time(),runs=len(rows)))

if __name__=='__main__':main()
