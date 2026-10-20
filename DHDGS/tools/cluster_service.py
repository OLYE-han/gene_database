"""Long-lived data or HNSW head process; one YAML describes all three hosts."""
import os
for key in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[key]='1'
import argparse
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
from pathlib import Path
import signal
import sys
import threading

PROJECT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT/'python'))
from dhdgs.config import StrictLoader,validate
from dhdgs.managed_node import create_managed_server,save_json
from dhdgs.managed_cluster import ClusterHead
from dhdgs.head_http import DeferredHandler,PipelineHTTPServer
import yaml


def load_plan(path):
    value=yaml.load(Path(path).read_text(),Loader=StrictLoader);validate(value['dhdgs']);return value


def serve_head(c,settings,token):
    from full_truth import FullTruth
    truth=FullTruth(settings['truth_plan'])
    # FullTruth has verified dataset/query identities. Reuse its exact row ids;
    # query k/exclude-self semantics must remain compatible with that truth.
    if c['data']['metric']!='l2':raise ValueError('Managed benchmark currently requires verified L2 truth')
    if str(Path(c['query']['vectors']).resolve())!=str(truth.qpath.resolve()) or str(Path(c['query']['ids']).resolve())!=str(truth.idpath.resolve()):
        raise ValueError('Managed query files differ from validated truth')
    def expected(row,k):
        if not head.c['search']['exclude_self']:raise ValueError('Supplied truth requires exclude_self=true')
        if not 0<k<=truth.ids.shape[1]:raise ValueError('top_k exceeds supplied truth width')
        return list(map(int,truth.ids[row,:k]))
    head=ClusterHead(c,settings,token,expected)
    if head.engine.manifest['dataset']!=truth.identity or head.engine.manifest['rows']!=truth.dataset.rows:
        head.close();raise ValueError('Head index differs from validated full-corpus truth')
    contiguous=(head.engine.corpus_ids_contiguous if hasattr(head.engine,'corpus_ids_contiguous') else
                len(head.engine.owners)==truth.dataset.rows and all(gid==i for i,gid in enumerate(head.engine.owners)))
    if not contiguous:
        head.close();raise ValueError('Full truth requires the complete contiguous corpus ID directory')
    pipeline=any(c['peer'].get(k,{}).get('enabled',False) for k in ('head_pipeline','head_direct','head_lanes'))
    class Handler(DeferredHandler if pipeline else BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):self.handle_api()
        def do_POST(self):self.handle_api()
        def handle_api(self):
            self.connection.settimeout(settings['command_timeout_seconds'])
            try:
                if not hmac.compare_digest(self.headers.get('Authorization',''),'Bearer '+token):
                    self.reply(401,{'error':'Invalid token'});return
                size=int(self.headers.get('Content-Length','0'))
                if size<0 or size>settings['max_api_bytes']:raise ValueError('Request body exceeds limit')
                body=json.loads(self.rfile.read(size)) if size else {}
                if pipeline:
                    operations={('GET','/status'):lambda:head.status(),
                        ('POST','/configure'):lambda:head.configure(body),
                        ('POST','/recover'):lambda:head.configure({},recover=True),
                        ('POST','/collect'):lambda:head.collect(),
                        ('POST','/stop_nodes'):lambda:head.stop_nodes(),
                        ('POST','/benchmark'):lambda:head.benchmark(body)}
                    action=operations.get((self.command,self.path))
                    if action is not None:
                        self.defer_query(self.server.management.submit(action));return
                if self.command=='GET' and self.path=='/status':result=head.status()
                elif self.command=='POST' and self.path=='/configure':result=head.configure(body)
                elif self.command=='POST' and self.path=='/recover':result=head.configure({},recover=True)
                elif self.command=='POST' and self.path=='/collect':result=head.collect()
                elif self.command=='POST' and self.path=='/stop_nodes':result=head.stop_nodes()
                elif self.command=='POST' and self.path=='/benchmark':result=head.benchmark(body)
                elif self.command=='POST' and self.path=='/query':
                    row=body.get('row')
                    if type(row)is not int or not 0<=row<len(truth.qids):raise ValueError('Invalid query row')
                    if pipeline:
                        self.defer_query(head.query_async(truth.queries[row],int(truth.qids[row])))
                        return
                    result=head.query(truth.queries[row],int(truth.qids[row]))
                elif self.command=='POST' and self.path=='/shutdown':
                    with head.exclusive('shutdown',require_ready=False):head.ready=False
                    result={'stopping':True};threading.Thread(target=self.server.shutdown,daemon=True).start()
                else:self.reply(404,{'error':'Unknown operation'});return
                self.reply(200,{'ok':True,'result':result})
            except (ValueError,RuntimeError,KeyError,TypeError,OSError) as exc:self.reply(409,{'ok':False,'error':str(exc)})
        def reply(self,status,value):
            raw=json.dumps(value,allow_nan=False).encode();self.send_response(status)
            self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers()
            try:self.wfile.write(raw)
            except (BrokenPipeError,ConnectionResetError):pass
    server=(PipelineHTTPServer((settings['bind_host'],settings['port']),Handler,head.engine.head_flow,c['peer']['max_active_queries'])
            if pipeline else ThreadingHTTPServer((settings['bind_host'],settings['port']),Handler))
    server.daemon_threads=True
    server.request_timeout=settings['command_timeout_seconds']
    try:
        print(json.dumps({'role':'head','pid':os.getpid(),'endpoint':server.server_address,'ready':False,'action':'run recover to synchronize nodes'}),flush=True)
        serve(server)
    finally:server.server_close();head.close()


def serve(server):
    def stop(*_):threading.Thread(target=server.shutdown,daemon=True).start()
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    server.serve_forever(poll_interval=.1)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('role',choices=['data','head']);parser.add_argument('--plan',type=Path,required=True)
    parser.add_argument('--node',type=int);args=parser.parse_args();plan=load_plan(args.plan)
    if args.role=='data':
        if args.node is None:raise ValueError('--node required')
        host=next(h for h in plan['deployment']['hosts'] if h['id']==args.node)
        token=Path(host['token_file']).read_text().strip()
        server=create_managed_server(plan['dhdgs'],args.node,token,host['state_file'])
        try:
            print(json.dumps({'role':'data','pid':os.getpid(),'node_id':args.node,'endpoint':server.server_address}),flush=True)
            serve(server)
        finally:server.server_close()
    else:
        token=Path(plan['control']['token_file']).read_text().strip()
        serve_head(plan['dhdgs'],plan['control'],token)

if __name__=='__main__':main()
