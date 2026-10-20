"""Exercise final detached start-data CLI on reachable hosts, then stop them."""
from pathlib import Path
import socket,subprocess,sys,time
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path.insert(0,str(P/'tools'));sys.path.insert(0,str(P/'python'))
from cluster_service import load_plan
from dhdgs.managed_cluster import admin_rpc
from dhdgs.managed_node import save_json
plan=load_plan(R/'cluster.yaml');token=Path(plan['control']['token_file']).read_text().strip()
nodes=[n for n in plan['dhdgs']['cluster']['nodes'] if n['id'] in (0,2)]
for n in nodes:
    try:
        with socket.create_connection((n['host'],n['port']),2):pass
    except OSError:continue
    raise RuntimeError('Refusing to test on an already occupied port')
status=[];cleanup=[]
try:
    subprocess.run([sys.executable,str(P/'tools/manage_cluster.py'),'--plan',str(R/'cluster.yaml'),'start-data','--nodes','0','2'],check=True,timeout=120)
    for n in nodes:
        deadline=time.monotonic()+90
        while True:
            try:
                value=admin_rpc(n,token,'admin_status',timeout=2)
                assert value['node_id']==n['id'] and not value['accepting'];status.append(value);break
            except OSError:
                if time.monotonic()>deadline:raise
                time.sleep(.2)
    save_json(R/'launch_verification.json',dict(nodes=status,distinct_hostnames=len({n['hostname'] for n in status})==2,paused_until_head_recover=True))
finally:
    for n in nodes:
        try:
            value=admin_rpc(n,token,'admin_shutdown',timeout=3);cleanup.append({'node':n['id'],'pid':value['pid'],'shutdown':'acknowledged'})
        except Exception as exc:cleanup.append({'node':n['id'],'error':str(exc)})
    time.sleep(.5)
    for n,item in zip(nodes,cleanup):
        try:
            with socket.create_connection((n['host'],n['port']),2):item['port_closed']=False
        except OSError:item['port_closed']=True
    save_json(R/'launch_cleanup.json',cleanup)
    assert all(n.get('port_closed') and 'error' not in n for n in cleanup),cleanup
