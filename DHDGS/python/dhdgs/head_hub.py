"""Process-shared callback endpoint and bounded terminal identity tombstones."""
from collections import OrderedDict
import hashlib
import json
import socket
import socketserver
import threading
import time
import msgpack
from .transport import send,receive
from .peer_rpc import ConnectionPool,FairConnectionPool,rpc_exchange

_lock=threading.Lock()
_hubs={}


def acquire_hub(c):
    key=json.dumps({'peer':c['peer'],'network':c['network'],'nodes':c['cluster']['nodes']},sort_keys=True)
    with _lock:
        if key not in _hubs:
            if c['peer'].get('head_direct',{}).get('enabled',False):
                from .head_direct import DirectHub
                _hubs[key]=DirectHub(c,key)
            elif c['peer'].get('head_pipeline',{}).get('enabled',False):
                from .pipeline_hub import PipelineHub
                _hubs[key]=PipelineHub(c,key)
            elif c['peer'].get('head_receiver_processes',0):
                from .head_receivers import ProcessHeadHub
                _hubs[key]=ProcessHeadHub(c,key)
            else:_hubs[key]=HeadHub(c,key)
        hub=_hubs[key];hub.references+=1
        return hub


def release_hub(hub):
    with _lock:
        hub.references-=1
        if hub.references:return
        _hubs.pop(hub.key,None)
    hub.close()


class HeadHub:
    def __init__(self,c,key):
        self.c,self.key=c,key;self.references=0
        self.lock=threading.Lock();self.sessions={};self.closed=OrderedDict();self.connections=set()
        comm=c['peer']['communication']
        pool_settings=c['peer']['connection_pool']
        if not comm['reuse_connections']:self.pool=None
        elif pool_settings.get('head_scheduling','broadcast')=='fifo':
            self.pool=FairConnectionPool(comm['connections_per_endpoint'],comm['max_connection_endpoints'])
        else:
            self.pool=ConnectionPool(comm['connections_per_endpoint'],comm['max_connection_endpoints'],
                                     pool_settings['probe_mode'])
        owner=self
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                with owner.lock:
                    settings=c['peer'].get('head_direct',{})
                    if settings.get('queued_reports',False) and len(owner.connections)>=settings.get('report_connections_per_channel',64):return
                    owner.connections.add(self.request)
                try:
                    self.request.settimeout(c['network']['timeout_seconds'])
                    while True:
                        try:message,rx=receive(self.request,c['network']['max_message_bytes'])
                        except (OSError,ValueError,msgpack.UnpackException):return
                        try:owner.deliver(self.request,message,rx)
                        except (ValueError,RuntimeError,KeyError,TypeError) as exc:
                            send(self.request,{'ok':False,'error':str(exc)},c['network']['max_message_bytes'])
                except OSError:pass
                finally:
                    with owner.lock:owner.connections.discard(self.request)
        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address=True;daemon_threads=True
            request_queue_size=c['network']['listen_backlog']
        self.server=Server((c['peer']['head_bind_host'],c['peer']['head_port']),Handler)
        self.endpoint=[c['peer']['head_advertise_host'],self.server.server_address[1]]
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.01},name='dhdgs-head',daemon=True)
        self.thread.start()

    def register(self,session):
        with self.lock:
            qid=session.registry.query_id
            if qid in self.sessions or qid in self.closed:raise ValueError('Duplicate head query identity')
            self.sessions[qid]=session
        return self.endpoint

    def unregister(self,session,success):
        # Caller holds session.delivery_lock, also used by every callback.
        qid=session.registry.query_id
        with self.lock:
            self.sessions.pop(qid,None)
            self.closed[qid]={'token':session.registry.token,'time':time.monotonic(),
                              'nodes':sorted(session.registry.nodes),'success':success,
                              'digests':dict(session.report_digests)}
            now=time.monotonic()
            while self.closed and (len(self.closed)>self.c['peer']['max_tombstones'] or
                  now-next(iter(self.closed.values()))['time']>self.c['peer']['retention_seconds']):
                self.closed.popitem(last=False)

    def deliver(self,sock,message,rx):
        measured=self.c['peer'].get('head_direct',{}).get('queued_reports',False)
        received=time.monotonic_ns() if measured else 0
        qid=message.get('query_request_id')
        with self.lock:session=self.sessions.get(qid)
        if session is None:
            return self.deliver_closed(sock,message)
        with session.delivery_lock:
            if session.closed:return self.deliver_closed(sock,message)
            started=time.monotonic_ns() if measured else 0
            nodes=session.registry.update(message)
            for report in message.get('reports',[]):
                session.report_digests[report['branch_id']]=hashlib.sha256(msgpack.packb(report,use_bin_type=True)).hexdigest()
            kind='result' if message.get('reports') or message.get('error') else 'heartbeat'
            reply={'ok':True,'cancelled':False,'lease_ms':self.c['peer']['lease_timeout_ms'],'involved_nodes':nodes}
            validated=time.monotonic_ns() if measured else 0
            try:
                tx=send(sock,reply,self.c['network']['max_message_bytes'])
                session.record_control(kind,{'rpc_count':1,'request_bytes':rx,'response_bytes':tx})
            except OSError as exc:
                session.record_control_error(kind,message.get('node_id'),exc)
                raise
            finally:
                if measured:
                    session.receiver_metrics.setdefault('report_receipts',[]).append(dict(
                        kind=kind,received_ns=received,validation_start_ns=started,validated_ns=validated,
                        ack_write_finished_ns=time.monotonic_ns(),bytes=rx))
                session.done.set()

    def deliver_closed(self,sock,message):
        with self.lock:
            old=self.closed.get(message.get('query_request_id'))
            if old is None or time.monotonic()-old['time']>self.c['peer']['retention_seconds']:
                raise ValueError('Unknown or expired head query')
            if message.get('token')!=old['token']:raise ValueError('Invalid query control token')
            if old['success']:
                for report in message.get('reports',[]):
                    digest=hashlib.sha256(msgpack.packb(report,use_bin_type=True)).hexdigest()
                    if old['digests'].get(report['branch_id'])!=digest:raise ValueError('Conflicting terminal report')
            reply={'ok':True,'cancelled':True,'lease_ms':self.c['peer']['lease_timeout_ms'],'involved_nodes':old['nodes']}
        send(sock,reply,self.c['network']['max_message_bytes'])

    def exchange(self,endpoint,message,limit,timeout):return rpc_exchange(endpoint,message,limit,timeout,self.pool)

    def close(self):
        self.server.shutdown()
        with self.lock:connections=list(self.connections)
        for sock in connections:
            try:sock.shutdown(socket.SHUT_RDWR)
            except OSError:pass
            sock.close()
        self.server.server_close();self.thread.join()
        if self.pool is not None:self.pool.close()
