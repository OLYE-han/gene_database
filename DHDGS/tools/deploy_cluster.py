"""Prepare and exercise three physical hosts from one deployment YAML.

Run with DHDGS's Python environment. All settings, including SSH timeouts,
remote paths, build jobs and benchmark variants, come from that YAML.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import selectors
import shlex
import socket
import subprocess
import sys
import tarfile
import threading
import time

import yaml
from threadpoolctl import threadpool_limits

PROJECT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT/'python'))
from dhdgs.config import load_config, snapshot, validate, StrictLoader
from dhdgs.metrics import export_query_csv,export_resource_csvs


def save(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(value,indent=2,allow_nan=False));temp.replace(path)


def digest(path):
    with Path(path).open('rb') as handle:
        hasher=hashlib.sha256()
        for chunk in iter(lambda:handle.read(8*1024*1024),b''):hasher.update(chunk)
        return hasher.hexdigest()


class Deployment:
    def __init__(self,path):
        self.plan_path=Path(path).resolve();self.plan=yaml.load(self.plan_path.read_text(),Loader=StrictLoader)
        self.d=self.plan['deployment'];self.base=self.plan['dhdgs'];validate(self.base)
        self.root=self.plan_path.parent;self.index=Path(self.d['index_source'])
        self.hosts=self.d['hosts'];self.modes=self.d['modes']
        self.truth_provider=None
        self.query_rows=None
        if [h['id'] for h in self.hosts]!=[n['id'] for n in self.base['cluster']['nodes']]:
            raise ValueError('Host IDs and runtime node IDs must match')
        if len(self.hosts)!=3 or self.hosts[0]['ssh'] is not None:raise ValueError('Expected local entry/data node 0 and two SSH hosts')
        if self.base['network']['transport']!='tcp' or self.base['search']['execution']!='peer':raise ValueError('Physical tests require peer/TCP')
        if self.base['peer']['head_advertise_host'] in ('127.0.0.1','localhost','0.0.0.0'):raise ValueError('A remotely reachable callback address is required')
        if len(set(m['name'] for m in self.modes))!=len(self.modes):raise ValueError('Duplicate mode names')
        for m in self.modes:
            if not m['name'].replace('_','').isalnum():raise ValueError('Invalid mode name')
        self.ssh=['ssh','-T','-o','BatchMode=yes','-o',f"ConnectTimeout={self.d['ssh_connect_timeout_seconds']}"]

    def progress(self,message):print(message,flush=True)

    def command(self,host,args):
        return args if host['ssh'] is None else [*self.ssh,host['ssh'],shlex.join(list(map(str,args)))]

    def run(self,host,args,log,input_file=None,timeout=None):
        log=Path(log);log.parent.mkdir(parents=True,exist_ok=True)
        with log.open('a') as out:
            out.write('\nCOMMAND '+shlex.join(list(map(str,args)))+'\n');out.flush()
            result=subprocess.run(self.command(host,args),stdin=input_file or subprocess.DEVNULL,
                stdout=out,stderr=subprocess.STDOUT,timeout=timeout or self.d['command_timeout_seconds'])
        if result.returncode:raise RuntimeError(f"Host {host['id']} command failed ({result.returncode}); see {log}")

    def project(self,host):return PROJECT if host['ssh'] is None else Path(host['root'])/'gene_database/DHDGS'
    def python(self,host):return str(self.project(host)/'.venv/bin/python')
    def helper(self,host):return str(self.project(host)/'tools/deploy_node.py')

    def settings(self,mode,host=None,concurrency=None):
        c=copy.deepcopy(self.base)
        c['peer']['execution_model']=mode['execution_model'];c['storage']['io']['backend']=mode['backend']
        if host is not None and host['ssh'] is not None:c['paths']['output']=str(Path(host['root'])/'output')
        else:c['paths']['output']=str(self.root/mode['name'])
        if concurrency is not None:c['experiment']['benchmark_concurrency']=concurrency
        validate(c);return c

    def config(self,mode,host):
        return (self.root/'configs'/f"{mode['name']}_node0.yaml" if host['ssh'] is None
                else Path(host['root'])/'configs'/f"{mode['name']}.yaml")

    def source_files(self):
        paths=[PROJECT/'CMakeLists.txt',PROJECT/'pyproject.toml',PROJECT/'README.md']
        for folder in ('cpp','python/dhdgs','configs','tools'):
            paths += [p for p in (PROJECT/folder).rglob('*') if p.is_file()
                      and '__pycache__' not in p.parts and '.native_libs' not in p.parts and p.suffix not in ('.so','.pyc')]
        ref=PROJECT.parent/'Reference_Code/RaBitQ-Library'
        paths += [p for p in ref.rglob('*') if p.is_file() and not any(x in p.parts for x in ('.git','build','__pycache__'))]
        return paths

    def prepare(self):
        self.progress('Preparing immutable payloads and node configurations')
        self.root.joinpath('configs').mkdir(exist_ok=True)
        wheels=self.root/'wheels';wheels.mkdir(exist_ok=True)
        target=self.d['wheel_target']
        args=[sys.executable,'-m','pip','download','--only-binary=:all:',
              '--python-version',target['python_version'],'--implementation',target['implementation'],
              '--abi',target['abi'],'--dest',str(wheels)]
        for platform in target['platforms']:args+=['--platform',platform]
        self.run(self.hosts[0],[*args,*self.d['python_requirements']],self.root/'wheel_download.log',
                 timeout=self.d['build_timeout_seconds'])
        paths=self.source_files();source_hashes={str(p.relative_to(PROJECT.parent)):digest(p) for p in paths}
        provenance={'plan_sha256':digest(self.plan_path),'source_sha256':source_hashes,
                    'index_sha256':{str(p.relative_to(self.index)):digest(p) for p in self.index.rglob('*') if p.is_file()}}
        existing=self.root/'prepared.json'
        if existing.exists():
            if json.loads(existing.read_text())!=provenance:raise ValueError('Prepared inputs changed; use a new deployment directory')
            self.progress('Existing preparation matches source, index and plan');return
        for mode in self.modes:
            out=self.root/mode['name'];out.mkdir(exist_ok=True)
            if not (out/'index').exists():(out/'index').symlink_to(self.index,target_is_directory=True)
            self.config(mode,self.hosts[0]).write_text(yaml.safe_dump(self.settings(mode),sort_keys=False))
        payload_dir=Path(self.d.get('payload_directory',self.root/'payloads'));payload_dir.mkdir(parents=True,exist_ok=True)
        def remote_prepare(host):
            sid=host['id'];root=Path(host['root']);payload=payload_dir/f'node_{sid}.tar.gz'
            entries={f'gene_database/{p.relative_to(PROJECT.parent)}':p for p in paths}
            for p in wheels.glob('*.whl'):entries['wheels/'+p.name]=p
            index_entries={}
            for p in (self.index/f'shard_{sid}').iterdir():
                if p.is_file():index_entries[f'index/shard_{sid}/{p.name}']=p
            for name in ('ids.npy','membership.npy'):index_entries['index/'+name]=self.index/name
            if not self.d.get('reuse_remote_index',False):entries.update(index_entries)
            for mode in self.modes:
                entries[f"configs/{mode['name']}.yaml"]=yaml.safe_dump(self.settings(mode,host),sort_keys=False).encode()
            project=self.project(host);python=project/'.venv/bin/python'
            requirements=self.d['python_requirements']
            build_dir=host['build_directory'];compiler=host['cxx']
            bootstrap=['set -eu',f'cd {shlex.quote(str(project))}',
                f'python3 {shlex.quote(self.helper(host))} verify {shlex.quote(str(root))}',
                'python3 -m venv .venv',
                shlex.join([str(python),'-m','pip','install','--no-index','--find-links',str(root/'wheels'),*requirements]),
                shlex.join([str(python),'-m','pip','install','--no-build-isolation','--no-deps','-e','.']),
                shlex.join([compiler,'--version']),
                shlex.join(['cmake','-S','.','-B',build_dir,'-DCMAKE_BUILD_TYPE=Release',
                            '-DDHDGS_NATIVE_OPTIMIZATION='+('ON' if self.d.get('native_optimization',False) else 'OFF'),
                            f'-DCMAKE_CXX_COMPILER={compiler}',f'-DPython_EXECUTABLE={python}']),
                shlex.join(['cmake','--build',build_dir,'--parallel',str(self.d['build_jobs'])]),
                f'mkdir -p {shlex.quote(str(root/"output"))}',
                f'test -e {shlex.quote(str(root/"output/index"))} || ln -s {shlex.quote(str(root/"index"))} {shlex.quote(str(root/"output/index"))}',
                shlex.join([str(python),self.helper(host),'environment','--file',str(root/f'index/shard_{sid}/nodes.bin'),
                            '--timeout',str(self.d['network_probe_timeout_seconds'])])]
            entries['bootstrap.sh']=('\n'.join(bootstrap)+'\n').encode()
            manifest={name:digest(data) if isinstance(data,Path) else hashlib.sha256(data).hexdigest() for name,data in entries.items()}
            # Reused shards are still fully verified on the remote machine before
            # starting any process. Only the redundant upload is omitted.
            manifest.update({name:digest(data) for name,data in index_entries.items()})
            entries['payload_manifest.json']=json.dumps(manifest,indent=2).encode()
            with tarfile.open(payload,'w:gz',compresslevel=self.d.get('compression_level',6)) as archive:
                for name,data in entries.items():
                    if isinstance(data,Path):
                        archive.add(data,arcname=name,recursive=False)
                    else:
                        info=tarfile.TarInfo(name);info.size=len(data);info.mode=0o600
                        archive.addfile(info,io.BytesIO(data))
            save(payload_dir/f'node_{sid}_manifest.json',manifest)
            self.progress(f'Uploading node {sid} payload ({payload.stat().st_size} bytes)')
            # Only our isolated run root is touched. No deletion or system installs.
            script=f'mkdir -p {shlex.quote(str(root))} && tar -xzf - -C {shlex.quote(str(root))} && sh {shlex.quote(str(root/"bootstrap.sh"))}'
            with payload.open('rb') as incoming:
                self.run(host,['sh','-c',script],self.root/f'prepare_node_{sid}.log',incoming,timeout=self.d['build_timeout_seconds'])
            self.progress(f'Node {sid} prepared and native asynchronous read verified')
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(remote_prepare,h) for h in self.hosts if h['ssh'] is not None]
            for future in futures:future.result()
        self.run(self.hosts[0],[self.python(self.hosts[0]),self.helper(self.hosts[0]),'environment',
            '--file',str(self.index/'shard_0/nodes.bin'),'--timeout',str(self.d['network_probe_timeout_seconds'])],self.root/'prepare_node_0.log')
        save(existing,provenance)

    def start(self,mode,host):
        directory=self.root/mode['name']/'services';directory.mkdir(exist_ok=True)
        sid=host['id'];remote_log=(directory/f'node_{sid}' if host['ssh'] is None else Path(host['root'])/'logs'/mode['name'])
        args=[self.python(host),self.helper(host),'supervise','--python',self.python(host),
              '--project',str(self.project(host)),'--config',str(self.config(mode,host)),'--node',str(sid),
              '--log',str(remote_log),'--timeout',str(self.d['service_start_timeout_seconds']),
              '--stop-timeout',str(self.d['service_stop_timeout_seconds'])]
        err=(directory/f'node_{sid}_supervisor.stderr.log').open('x')
        proc=subprocess.Popen(self.command(host,args),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True)
        record={'node_id':sid,'proc':proc,'error_log':err,'host':host,'remote_log':str(remote_log)}
        try:
            with selectors.DefaultSelector() as sel:
                sel.register(proc.stdout,selectors.EVENT_READ)
                if not sel.select(self.d['command_timeout_seconds']):raise TimeoutError(f'Node {sid} SSH/start timeout')
                line=proc.stdout.readline()
            if not line:raise RuntimeError(f'Node {sid} supervisor exited; inspect {err.name}')
            ready=json.loads(line);record['initial_event']=ready
            if ready['status']!='ready':raise RuntimeError(str(ready))
            record['ready']=ready;save(directory/f'node_{sid}_ready.json',ready)
            self.progress(f"{mode['name']}: data node {sid} ready on {ready['hostname']}")
            return record
        except BaseException:
            self.stop(mode,record);raise

    def stop(self,mode,record):
        proc=record['proc'];sid=record['node_id'];directory=self.root/mode['name']/'services'
        try:
            output,_=proc.communicate(input='stop\n' if proc.poll() is None else None,
                timeout=self.d['command_timeout_seconds'])
            events=[json.loads(line) for line in output.splitlines() if line.strip()]
            stopped=next((r for r in events if r.get('status')=='stopped'),None)
            if stopped is None and record.get('initial_event',{}).get('status')=='stopped':stopped=record['initial_event']
            save(directory/f'node_{sid}_stopped.json',{'supervisor_exit_code':proc.returncode,'event':stopped,'events':events})
            if stopped is not None:
                (directory/f'node_{sid}.stdout.log').write_text(stopped.get('stdout',''))
                (directory/f'node_{sid}.stderr.log').write_text(stopped.get('stderr',''))
            if stopped is None:raise RuntimeError(f'Node {sid} cleanup not confirmed; inspect supervisor logs')
        finally:
            record['error_log'].close()

    def probes(self,mode,records):
        # Verify data-plane full mesh AND return connectivity to a temporary head port.
        listener=socket.socket();listener.bind((self.base['peer']['head_bind_host'],0));listener.listen()
        listener.settimeout(.2);done=threading.Event()
        def accept():
            while not done.is_set():
                try:client,_=listener.accept();client.close()
                except socket.timeout:pass
                except OSError:return
        thread=threading.Thread(target=accept,daemon=True);thread.start()
        endpoints=[[n['host'],n['port']] for n in self.base['cluster']['nodes']]
        endpoints.append([self.base['peer']['head_advertise_host'],listener.getsockname()[1]])
        try:
            def check(record):
                proc=record['proc'];sid=record['node_id']
                request={'op':'probe','endpoints':endpoints,'timeout':self.d['network_probe_timeout_seconds'],
                         'repetitions':self.d['network_probe_repetitions']}
                proc.stdin.write(json.dumps(request)+'\n');proc.stdin.flush()
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout,selectors.EVENT_READ)
                    if not selector.select(self.d['command_timeout_seconds']):raise TimeoutError('Network probe timeout')
                    response=json.loads(proc.stdout.readline())
                save(self.root/mode['name']/f'network_node_{sid}.json',response)
                if response.get('status')=='stopped':record['initial_event']=response
                if response.get('status')!='probe_complete':raise RuntimeError(str(response))
            with ThreadPoolExecutor(max_workers=3) as pool:
                futures=[pool.submit(check,r) for r in records]
                for f in futures:f.result()
        finally:done.set();listener.close();thread.join()

    def benchmark(self,mode,concurrency):
        import dhdgs.benchmark as benchmark
        from dhdgs.search import Engine
        root=self.root/mode['name']/f'c{concurrency}';root.mkdir()
        cfg=root/'experiment.yaml';cfg.write_text(yaml.safe_dump(self.settings(mode,concurrency=concurrency),sort_keys=False))
        c=load_config(cfg);snapshot(c,root/'run')
        with threadpool_limits(limits=c['runtime']['blas_threads']):
            queries,ids,_=benchmark.query_input(c,1,rows=self.query_rows[:1] if self.query_rows is not None else None)
            engine=Engine(c)
            try:smoke=engine.search(queries[0],None if ids is None else int(ids[0]))
            finally:engine.close()
            save(root/'smoke.json',smoke)
            report=benchmark.run_benchmark(c,truth_provider=self.truth_provider,query_rows=self.query_rows)
            if self.truth_provider is not None:
                report['truth_preparation_seconds']=report.pop('exact_scan_seconds')
                report['ground_truth_source']=self.truth_provider.provenance
        save(root/'run/report.json',report)
        export_query_csv(root/'run/queries.csv',report['queries']);export_resource_csvs(root/'run',report)
        with (root/'run/queries.jsonl').open('x') as f:
            for row in report['queries']:f.write(json.dumps(row,allow_nan=False)+'\n')
        audit={'failures':report['failed_queries'],'incomplete_metrics':0,'node_revisits':0,'unapproved_dispatches':0}
        for row in report['queries']:
            if row['status']!='ok':continue
            s=row['stats'];audit['incomplete_metrics']+=int(not s.get('metrics_complete',False))
            audit['node_revisits']+=s['node_revisit_count']
            if s['expanded']>c['search']['max_expansions'] or s['router_expand_rpc_count']!=0:
                raise ValueError('Query budget or head-role invariant failed')
            for branch in row['branch_reports']:
                assert branch['work_spent']+branch['work_unused']+sum(ch['work_grant'] for ch in branch['children'])==branch['work_grant']
                for ch in branch['children']:
                    match=[d for d in branch['decisions'] if d['decision_id']==ch['decision_id'] and d['remote'] and d['selected_node']==ch['node_id']]
                    audit['unapproved_dispatches']+=int(len(match)!=1)
        summary={'mode':mode['name'],'concurrency':concurrency,'qps':report['qps'],'recall':report['recall_at_k'],
            'latency_ms':report['latency_ms'],'queries':report['query_count'],'audit':audit,
            'physical_read_bytes':sum(n['process_deltas']['storage_read_bytes'] or 0 for n in report['node_process_load'])}
        save(root/'audit.json',summary);self.progress(json.dumps(summary));return summary

    def run_all(self):
        marker=json.loads((self.root/'prepared.json').read_text())
        if marker['plan_sha256']!=digest(self.plan_path):raise ValueError('Plan changed after preparation')
        for p in self.source_files():
            if marker['source_sha256'].get(str(p.relative_to(PROJECT.parent)))!=digest(p):raise ValueError('Source changed after preparation')
        results=[]
        for mode in self.modes:
            started=[]
            try:
                with ThreadPoolExecutor(max_workers=3) as pool:
                    futures=[pool.submit(self.start,mode,h) for h in self.hosts]
                    errors=[]
                    for f in futures:
                        try:started.append(f.result())
                        except Exception as exc:errors.append(exc)
                    if errors:raise errors[0]
                self.probes(mode,started)
                for concurrency in self.d['client_concurrencies']:
                    results.append(self.benchmark(mode,concurrency));save(self.root/'comparison.json',results)
            finally:
                with ThreadPoolExecutor(max_workers=3) as pool:
                    futures=[pool.submit(self.stop,mode,r) for r in started]
                    for future in futures:future.result()
        save(self.root/'completed.json',{'tests':len(results),'queries':sum(r['queries'] for r in results),'all_services_stopped':True})


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--plan',required=True)
    parser.add_argument('--phase',choices=('prepare','run','all'),default='all')
    args=parser.parse_args();deployment=Deployment(args.plan)
    if args.phase in ('prepare','all'):deployment.prepare()
    if args.phase in ('run','all'):deployment.run_all()


if __name__=='__main__':main()
