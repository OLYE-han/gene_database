"""Deploy immutable code releases, reuse existing shards, start persistent roles.

Run on Intel-35. Query settings/results are controlled via cluster_ctl.py.
No index transfer or rebuild is performed by this tool.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
import time
from cluster_service import load_plan
from cluster_ctl import request
from dhdgs.managed_node import save_json

PROJECT=Path(__file__).resolve().parents[1]

class Deployment:
    def __init__(self,path):
        self.path=path.resolve();self.plan=load_plan(path);self.opts=self.plan['deployment']
        self.root=Path(self.plan['control']['results_directory']);self.root.mkdir(parents=True,exist_ok=True)
        self.logs=self.root/'deployment_logs';self.logs.mkdir(exist_ok=True)
    def ssh(self,h):
        return ['ssh','-T','-o','BatchMode=yes','-o',f"ConnectTimeout={self.opts['connect_timeout_seconds']}",
                '-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2',h['ssh']]
    def run(self,h,argv,label,timeout=None):
        cmd=self.ssh(h)+[shlex.join(argv)] if h['ssh'] else argv
        log=self.logs/f"node{h['id']}_{label}.log"
        with log.open('ab') as f:
            result=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,timeout=timeout or self.opts['command_timeout_seconds'])
        if result.returncode:raise RuntimeError(f"{label} failed for node {h['id']}: {log}")
    def package(self):
        files=[]
        for rel in ('python','cpp','tools'):
            for p in sorted((PROJECT/rel).rglob('*')):
                if p.is_file() and '__pycache__' not in p.parts and '.native_libs' not in p.parts and p.suffix not in ('.so','.pyc'):
                    files.append((p,'gene_database/DHDGS/'+str(p.relative_to(PROJECT))))
        files.append((PROJECT/'CMakeLists.txt','gene_database/DHDGS/CMakeLists.txt'))
        ref=PROJECT.parent/'Reference_Code/RaBitQ-Library'
        for p in sorted(ref.rglob('*')):
            rel=p.relative_to(ref)
            if any(part.startswith('.') or part.startswith('build') or part=='__pycache__' for part in rel.parts):continue
            if p.is_file() and (rel.parts[0] in ('include','src','cmake') or len(rel.parts)==1):
                files.append((p,'gene_database/Reference_Code/RaBitQ-Library/'+str(rel)))
        manifest={name:hashlib.sha256(p.read_bytes()).hexdigest() for p,name in files}
        digest=hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest()[:20]
        dest=self.root/'payloads';dest.mkdir(exist_ok=True);archive=dest/(digest+'.tar.gz')
        if not archive.exists():
            with tarfile.open(archive,'w:gz',compresslevel=1) as tar:
                for p,name in files:tar.add(p,arcname=name,recursive=False)
        save_json(dest/(digest+'.json'),manifest)
        return digest,archive
    def prepare(self,h,digest,archive):
        root=Path(h['root']);release=root/'releases'/digest
        if h['ssh']:
            self.run(h,['mkdir','-p',str(root/'releases'),str(root/'logs')],'mkdir')
            project=release/'gene_database/DHDGS'
            probe=subprocess.run(self.ssh(h)+[shlex.join(['test','-f',str(release/'.prepared')])],
                stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=self.opts['command_timeout_seconds'])
            if probe.returncode not in (0,1):raise RuntimeError('Cannot inspect remote release')
            if probe.returncode:
                # Completed releases are immutable, including their binaries.
                scp=['scp','-q','-o','BatchMode=yes','-o',f"ConnectTimeout={self.opts['connect_timeout_seconds']}"]
                subprocess.run(scp+[str(archive),h['ssh']+':'+str(root/'source.tar.gz')],check=True,timeout=self.opts['command_timeout_seconds'])
                self.run(h,['mkdir','-p',str(release)],'release')
                self.run(h,['tar','-xzf',str(root/'source.tar.gz'),'-C',str(release)],'extract')
                self.run(h,['cmake','-S',str(project),'-B',str(release/'build'),'-DCMAKE_BUILD_TYPE=Release',
                    '-DCMAKE_CXX_COMPILER=/usr/bin/g++','-DPython_EXECUTABLE='+h['python'],'-DDHDGS_NATIVE_OPTIMIZATION=ON'],'cmake',self.opts['build_timeout_seconds'])
                self.run(h,['cmake','--build',str(release/'build'),'-j',str(self.opts['build_jobs'])],'build',self.opts['build_timeout_seconds'])
            # Transfer config/key via SSH stdin into mode-0600 files, never argv/log.
            token=Path(self.plan['control']['token_file']).read_text()
            payload=json.dumps({'plan':self.path.read_text(),'token':token}).encode()
            script="import json,os,sys;from pathlib import Path;p=json.load(sys.stdin);r=Path(sys.argv[1]);r.mkdir(parents=True,exist_ok=True);[(lambda fd,text: (os.write(fd,text.encode()),os.close(fd)))(os.open(r/name,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600),p[key]) for name,key in [('cluster.yaml','plan'),('cluster.token','token')]]"
            subprocess.run(self.ssh(h)+[shlex.join([h['python'],'-c',script,str(root)])],input=payload,check=True,timeout=self.opts['command_timeout_seconds'])
        else:
            project=PROJECT;(root/'logs').mkdir(exist_ok=True)
        config=root/'cluster.yaml'
        if not h['ssh'] and config.resolve()!=self.path:config.write_bytes(self.path.read_bytes())
        node=self.plan['dhdgs']['cluster']['nodes'][h['id']]
        check="import json,sys;from pathlib import Path;sys.path.insert(0,sys.argv[1]);from dhdgs import _native;p=Path(sys.argv[2]);m=json.loads((p/'manifest.json').read_text());assert hasattr(_native,'NativeController');print(json.dumps({'epoch':m['epoch'],'node':m['shard_id'],'index':str(p),'native':_native.__file__}))"
        self.run(h,[h['python'],'-c',check,str(project/'python'),node['index_directory']],'verify')
        if h['ssh']:self.run(h,['touch',str(release/'.prepared')],'prepared_marker')
        record=dict(project=str(project),plan=str(config),python=h['python'],source_digest=digest,host=h)
        launcher=[h['python'],str(project/'tools/cluster_service.py'),'data','--plan',str(config),'--node',str(h['id'])]
        script="from pathlib import Path;import sys;p=Path(sys.argv[1]);p.write_text(sys.argv[2]);p.chmod(0o700)"
        self.run(h,[h['python'],'-c',script,str(root/'run_data.sh'),'#!/bin/sh\nexec '+shlex.join(launcher)+'\n'],'launcher')
        save_json(self.root/f"deployment_node{h['id']}.json",record)
        print(json.dumps(dict(prepared=h['id'],source_digest=digest,project=str(project))),flush=True)
    def start(self,h,role):
        rec=json.loads((self.root/f"deployment_node{h['id']}.json").read_text())
        argv=[h['python'],str(Path(rec['project'])/'tools/cluster_service.py'),role,'--plan',rec['plan']]
        if role=='data':argv+=['--node',str(h['id'])]
        log=str(Path(h['root'])/'logs'/f'{role}.log');pidfile=str(Path(h['root'])/f'{role}.pid')
        # Refuse a duplicate live role, but do not kill unrelated/stale PID owners.
        script=r'''
import json,os,subprocess,sys
from pathlib import Path
argv=json.loads(sys.argv[1]);log,pidfile=sys.argv[2:4];p=Path(pidfile)
if p.exists():
    pid=int(p.read_text());cmd=Path('/proc')/str(pid)/'cmdline'
    if cmd.exists() and all(a.encode() in cmd.read_bytes().split(b'\0') for a in argv[1:]):
        print(json.dumps({'already_started':pid}));sys.exit(0)
with open(log,'ab',buffering=0) as out:
    child=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=out,stderr=out,start_new_session=True,close_fds=True)
p.write_text(str(child.pid));print(json.dumps({'started':child.pid,'log':log}))
'''
        self.run(h,[h['python'],'-c',script,json.dumps(argv),log,pidfile],'start_'+role)
        print(f"Started {role} on node {h['id']}; logs: {log}",flush=True)
    def wait_head(self):
        deadline=time.monotonic()+self.opts['start_timeout_seconds'];error=None
        while time.monotonic()<deadline:
            try:
                result=request(self.plan,'status')
                if all('error' not in n for n in result['nodes']):return result
                error=str(result['nodes'])
            except Exception as exc:error=str(exc)
            time.sleep(1)
        raise RuntimeError('Services not ready: '+str(error))

def main():
    p=argparse.ArgumentParser();p.add_argument('--plan',type=Path,required=True)
    p.add_argument('command',choices=['prepare','start','start-data','start-head','stop'])
    p.add_argument('--nodes',type=int,nargs='+',help='Default: all data nodes')
    args=p.parse_args();d=Deployment(args.plan);hosts=d.opts['hosts']
    selected=[h for h in hosts if args.nodes is None or h['id'] in args.nodes]
    if not selected:raise ValueError('No hosts selected')
    if args.command=='prepare':
        digest,archive=d.package();errors=[]
        for h in selected:
            try:d.prepare(h,digest,archive)
            except Exception as exc:
                errors.append(dict(node=h['id'],error=str(exc)));print(json.dumps(errors[-1]),flush=True)
        save_json(d.root/'prepare_status.json',dict(errors=errors,source_digest=digest))
        if errors:raise SystemExit(1)
    elif args.command in ('start','start-data','start-head'):
        if args.command!='start-head':
            for h in selected:d.start(h,'data')
        if args.command!='start-data':
            d.start(hosts[0],'head');d.wait_head()
            print(json.dumps(request(d.plan,'recover'),indent=2))
    else:
        print(json.dumps(request(d.plan,'stop_nodes')))
        print(json.dumps(request(d.plan,'shutdown')))

if __name__=='__main__':main()
