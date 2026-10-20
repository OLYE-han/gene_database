"""Start/stop isolated admission comparison nodes, reusing indexes and releases."""
import argparse
import copy
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time
import yaml

ROOT=Path(__file__).resolve().parent;P=ROOT.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from dhdgs.managed_cluster import admin_rpc
from dhdgs.managed_node import revision

BASE=P/'test/managed_continuation_three_20261019'

def main(mode,variant):
    destination=ROOT/(variant+'_data');destination.mkdir(exist_ok=True)
    path=destination/'cluster.yaml'
    if mode=='start':
        plan=load_plan(BASE/'cluster.yaml')
        plan['control']['results_directory']=str(destination)
        plan['dhdgs']['peer']['admission']['mode']='direct' if variant=='direct' else 'buffered'
        if variant=='independent':plan['dhdgs']['peer']['admission']['queue_lock']='independent'
        port=35051 if variant=='direct' else 35151
        if variant=='capacity':
            settings=yaml.safe_load((ROOT/'capacity_settings.yaml').read_text())
            plan['dhdgs']['peer']['max_tombstones']=settings['max_tombstones']
            plan['dhdgs']['network']['max_message_bytes']=settings['max_message_bytes']
            port=settings['data_port_base']
        for n in plan['dhdgs']['cluster']['nodes']:n['port']=port+n['id']
        for h in plan['deployment']['hosts']:
            h['root']=str(Path(h['root']).parent/'control_capacity_20261020'/(variant+'_data')) if h['ssh'] else str(destination)
            h['state_file']=str(Path(h['root'])/f"node_{h['id']}_state.json")
        path.write_text(yaml.safe_dump(plan,sort_keys=False))
        for h in plan['deployment']['hosts']:
            rec=json.loads((BASE/f"deployment_node{h['id']}.json").read_text())
            # Data node source is the same existing release, not a new native build.
            script=r'''
import hashlib,json,os,subprocess,sys
from pathlib import Path
p=json.load(sys.stdin);root=Path(p['root']);root.mkdir(parents=True,exist_ok=True)
config=root/'cluster.yaml';config.write_text(p['plan']);pidfile=root/'data.pid'
project=Path(p['project']);argv=[sys.executable,str(project/'tools/cluster_service.py'),'data','--plan',str(config),'--node',str(p['id'])]
pid=None
if pidfile.exists():
    pid=int(pidfile.read_text());cmd=Path('/proc')/str(pid)/'cmdline'
    if cmd.exists():
        if not all(a.encode() in cmd.read_bytes().split(b'\0') for a in argv[1:]):raise RuntimeError('PID belongs to another process')
    else:pid=None
if pid is None:
    with (root/'data.log').open('ab',buffering=0) as log:
        child=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True,close_fds=True)
    pid=child.pid;pidfile.write_text(str(pid))
hashes={str(f.relative_to(project)):hashlib.sha256(f.read_bytes()).hexdigest() for f in list((project/'python/dhdgs').glob('*.py'))+list((project/'python/dhdgs').glob('*.so'))}
print(json.dumps({'node':p['id'],'pid':pid,'project':str(project),'source_hashes':hashes}))
'''
            argv=[h['python'],'-c',script]
            if h['ssh']:argv=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10',h['ssh'],shlex.join(argv)]
            payload=dict(root=h['root'],project=rec['project'],id=h['id'],plan=path.read_text())
            result=subprocess.run(argv,input=json.dumps(payload).encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True,timeout=120)
            (destination/f"launch_node{h['id']}.json").write_bytes(result.stdout)
        token=Path(plan['control']['token_file']).read_text().strip()
        deadline=time.monotonic()+180
        for node in plan['dhdgs']['cluster']['nodes']:
            while True:
                try:
                    status=admin_rpc(node,token,'admin_status',timeout=2)
                    assert status['revision']==revision(plan['dhdgs']) and status['quiescent']
                    admin_rpc(node,token,'admin_resume',revision=revision(plan['dhdgs']))
                    print(json.dumps(status),flush=True);break
                except (OSError,RuntimeError) as exc:
                    if time.monotonic()>deadline:raise
                    time.sleep(1)
    else:
        plan=load_plan(path);token=Path(plan['control']['token_file']).read_text().strip()
        for node in plan['dhdgs']['cluster']['nodes']:
            status=admin_rpc(node,token,'admin_status')
            assert status['quiescent'],'Refusing to stop active isolated node'
            print(admin_rpc(node,token,'admin_shutdown'),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['start','stop']);p.add_argument('--variant',choices=['direct','independent','capacity'],default='direct')
    a=p.parse_args();main(a.mode,a.variant)
