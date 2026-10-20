"""Prepare with a persistent SSH session for slow remote login setup."""
from pathlib import Path
import json,sys
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from manage_cluster import Deployment
from dhdgs.managed_node import save_json
d=Deployment(R/'cluster.yaml')
d.ssh=lambda h:['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ServerAliveInterval=60','-o','ServerAliveCountMax=3','-o','ControlMaster=auto','-o','ControlPersist=300','-o','ControlPath=/tmp/dhdgs-three-20261019-%C',h['ssh']]
digest,archive=d.package()
for h in d.opts['hosts']:
    if h['id']==1:d.prepare(h,digest,archive)
save_json(R/'intel32_prepared.json',dict(source_digest=digest,prepared=True))
