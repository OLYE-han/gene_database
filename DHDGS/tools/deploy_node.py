"""Deployment helper: supervision is tied to the SSH stdin lifetime.

No daemonization or broad process-name cleanup. Only the child started here is
signalled; EOF or an orchestrator stop request terminates that child.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import time
from types import SimpleNamespace


def emit(value):
    print(json.dumps(value),flush=True)


def verify(root):
    manifest=json.loads((root/'payload_manifest.json').read_text())
    for name,digest in manifest.items():
        path=root/name
        with path.open('rb') as handle:
            hasher=hashlib.sha256()
            for chunk in iter(lambda:handle.read(8*1024*1024),b''):hasher.update(chunk)
        if hasher.hexdigest()!=digest:
            raise ValueError(f'Payload hash mismatch: {name}')
    emit({'verified_files':len(manifest),'hostname':socket.gethostname()})


def supervise(args):
    log=Path(args.log);log.mkdir(parents=True,exist_ok=True)
    command=[args.python,'-m','dhdgs','serve','--config',args.config,'--node-id',str(args.node)]
    with (log/'stdout.log').open('x') as stdout,(log/'stderr.log').open('x') as stderr:
        child=subprocess.Popen(command,stdout=stdout,stderr=stderr,start_new_session=True,cwd=args.project)
        state={'pid':child.pid,'node_id':args.node,'hostname':socket.gethostname(),'command':command}
        (log/'process.json').write_text(json.dumps(state,indent=2))
        try:
            deadline=time.monotonic()+args.timeout
            while True:
                if child.poll() is not None:raise RuntimeError('Data service exited before readiness')
                line=(log/'stdout.log').read_text().splitlines()
                if line:
                    ready=json.loads(line[0]);assert ready['status']=='listening'
                    emit({'status':'ready',**state,'endpoint':ready['endpoint']});break
                if time.monotonic()>deadline:raise TimeoutError('Data service readiness timeout')
                if select.select([sys.stdin],[],[],.05)[0]:
                    raise RuntimeError('Supervisor stdin closed/stopped before readiness')
            while child.poll() is None:
                if select.select([sys.stdin],[],[],.2)[0]:
                    line=sys.stdin.readline()
                    if not line or line.strip()=='stop':break
                    try:
                        request=json.loads(line)
                        if request['op']!='probe':raise ValueError('Unsupported supervisor operation')
                        probe(SimpleNamespace(endpoints=json.dumps(request['endpoints']),
                            timeout=request['timeout'],repetitions=request['repetitions']))
                    except Exception as exc:emit({'status':'probe_error','error':str(exc)})
        finally:
            if child.poll() is None:
                os.killpg(child.pid,signal.SIGINT)
                try:child.wait(timeout=args.stop_timeout)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid,signal.SIGTERM)
                    try:child.wait(timeout=args.stop_timeout)
                    except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait()
            state.update(stopped=True,exit_code=child.returncode)
            (log/'process.json').write_text(json.dumps(state,indent=2))
            stdout.flush();stderr.flush()
            emit({'status':'stopped',**state,'stdout':(log/'stdout.log').read_text(),
                  'stderr':(log/'stderr.log').read_text()})


def probe(args):
    endpoints=json.loads(args.endpoints);rows=[]
    for endpoint in endpoints:
        times=[];local=None
        for _ in range(args.repetitions):
            start=time.perf_counter()
            with socket.create_connection(tuple(endpoint),args.timeout) as sock:local=sock.getsockname()[0]
            times.append((time.perf_counter()-start)*1000)
        rows.append({'endpoint':endpoint,'source_ip':local,'tcp_connect_ms':times})
    emit({'status':'probe_complete','hostname':socket.gethostname(),'connections':rows})


def environment(args):
    from dhdgs import _native
    import numpy,yaml,msgpack,threadpoolctl,platform
    state={'hostname':socket.gethostname(),'python':sys.version,'platform':platform.platform(),
           'numpy':numpy.__version__,'yaml':yaml.__version__,'msgpack':msgpack.__version__,
           'threadpoolctl':threadpoolctl.__version__,'io_uring_compiled':_native.io_uring_compiled,
           'native_module_sha256':hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest(),
           'native_runtime_libraries':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in
                (Path(_native.__file__).parent/'.native_libs').glob('*') if p.is_file()},
           'logical_cpus':os.cpu_count(),'index_filesystem_device':os.stat(args.file).st_dev,
           'cpu_model':next((line.split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines()
                             if line.startswith('model name')),None),
           'mount':subprocess.check_output(['findmnt','-T',args.file,'-J'],text=True,timeout=args.timeout)}
    fd=os.open(args.file,os.O_RDONLY)
    try:
        reader=_native.AsyncReader(fd,4)
        reader.submit([(1,0,16)])
        deadline=time.monotonic()+args.timeout
        while True:
            rows=reader.poll(4)
            if rows:
                token,raw,error,ms=rows[0]
                assert token==1 and not error and raw==os.pread(fd,16,0)
                state['native_read_verified']=True;break
            if time.monotonic()>deadline:raise TimeoutError('Native read probe timeout')
            time.sleep(.001)
        reader.close()
    finally:os.close(fd)
    emit(state)


def main():
    parser=argparse.ArgumentParser();subs=parser.add_subparsers(dest='action',required=True)
    p=subs.add_parser('verify');p.add_argument('root',type=Path)
    p=subs.add_parser('supervise')
    for key in ('python','config','project','log'):p.add_argument('--'+key,required=True)
    p.add_argument('--node',type=int,required=True);p.add_argument('--timeout',type=float,required=True)
    p.add_argument('--stop-timeout',type=float,required=True)
    p=subs.add_parser('probe');p.add_argument('--endpoints',required=True)
    p.add_argument('--timeout',type=float,required=True);p.add_argument('--repetitions',type=int,required=True)
    p=subs.add_parser('environment');p.add_argument('--file',required=True);p.add_argument('--timeout',type=float,required=True)
    args=parser.parse_args()
    if args.action=='verify':verify(args.root)
    else:globals()[args.action](args)


if __name__=='__main__':main()
