"""In-process query state table, dispatch workers, callback reactor and leases."""
from collections import OrderedDict
from concurrent.futures import Future, InvalidStateError
import hashlib
import selectors
import socket
import struct
import threading
import time

import msgpack

from .head_hub import HeadHub
from .metrics import CLOCK_DOMAIN
from .peer_rpc import FairConnectionPool
from .sender import QueuedSender


def resolve(f,value=None,error=None):
    try:
        if error is None:f.set_result(value)
        else:f.set_exception(error)
    except InvalidStateError:
        if not f.done():raise


class CallbackReactor:
    """One nonblocking result receiver. Partial frames cannot block other sockets."""
    def __init__(self,c,handler):
        self.c=c;self.handler=handler;self.selector=selectors.DefaultSelector();self.closed=False
        self.listener=socket.socket();self.listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.listener.bind((c['peer']['head_bind_host'],c['peer']['head_port']))
        self.listener.listen(c['network']['listen_backlog']);self.listener.setblocking(False)
        self.selector.register(self.listener,selectors.EVENT_READ,None)
        self.wake,self.signal=socket.socketpair();self.wake.setblocking(False)
        self.selector.register(self.wake,selectors.EVENT_READ,'wake')
        self.endpoint=[c['peer']['head_advertise_host'],self.listener.getsockname()[1]]
        self.connections={};self.thread=threading.Thread(target=self.run,name='dhdgs-head-results',daemon=True)
        self.thread.start()

    def drop(self,sock):
        self.connections.pop(sock,None)
        try:self.selector.unregister(sock)
        except (KeyError,ValueError):pass
        sock.close()

    def run(self):
        limit=self.c['network']['max_message_bytes']
        maximum=self.c['peer']['head_pipeline']['callback_connections']
        try:
            while not self.closed:
                for key,mask in self.selector.select(.1):
                    sock,rec=key.fileobj,key.data
                    if rec=='wake':self.wake.recv(1024);continue
                    if rec is None:
                        conn,_=sock.accept();conn.setblocking(False)
                        if len(self.connections)>=maximum:conn.close();continue
                        rec=dict(input=bytearray(),output=bytearray(),touched=time.monotonic())
                        self.connections[conn]=rec;self.selector.register(conn,selectors.EVENT_READ,rec);continue
                    try:
                        if mask&selectors.EVENT_READ:
                            data=sock.recv(65536)
                            if not data:self.drop(sock);continue
                            rec['input'].extend(data);rec['touched']=time.monotonic()
                            if len(rec['input'])>=4:
                                size=struct.unpack('!I',rec['input'][:4])[0]
                                if not 0<size<=limit:raise ValueError('Invalid callback frame length')
                                if len(rec['input'])>=size+4:
                                    if len(rec['input'])!=size+4:raise ValueError('Pipelined callback not supported')
                                    message=msgpack.unpackb(rec['input'][4:],raw=False);rec['input'].clear()
                                    if not isinstance(message,dict):raise ValueError('Callback must be a mapping')
                                    reply,after=self.handler(message,size+4)
                                    body=msgpack.packb(reply,use_bin_type=True)
                                    if len(body)>limit:raise ValueError('Callback response exceeds frame limit')
                                    rec['output'].extend(struct.pack('!I',len(body))+body)
                                    if self.c['peer']['head_pipeline'].get('ack_inline',False):
                                        try:
                                            n=sock.send(rec['output']);del rec['output'][:n]
                                        except BlockingIOError:pass
                                    self.selector.modify(sock,selectors.EVENT_WRITE if rec['output'] else selectors.EVENT_READ,rec)
                                    # Count the ACK when queued for transport; never wait for socket IO
                                    # while holding a query state lock. Failed ACKs can be retried.
                                    if after is not None:after(len(body)+4)
                        if mask&selectors.EVENT_WRITE:
                            n=sock.send(rec['output']);del rec['output'][:n]
                            if not rec['output']:self.selector.modify(sock,selectors.EVENT_READ,rec)
                    except BlockingIOError:pass
                    except (OSError,ValueError,KeyError,TypeError,msgpack.UnpackException):self.drop(sock)
                now=time.monotonic()
                for sock,rec in list(self.connections.items()):
                    if now-rec['touched']>self.c['network']['timeout_seconds']:self.drop(sock)
        finally:
            for sock in list(self.connections):self.drop(sock)
            self.listener.close();self.wake.close();self.selector.close()

    def close(self):
        self.closed=True
        try:self.signal.send(b'x')
        except OSError:pass
        self.thread.join();self.signal.close()


class PipelineHub(HeadHub):
    def __init__(self,c,key):
        self.c=c;self.key=key;self.references=0
        self.lock=threading.Lock();self.cv=threading.Condition(self.lock)
        self.sessions={};self.pending={};self.closed=OrderedDict();self.stopping=False
        p=c['peer'];comm=p['communication']
        a=p.get('adaptive_services',{})
        if a.get('enabled') and a['head_connections']:
            from .adaptive_connections import AdaptiveConnectionPool
            self.pool=AdaptiveConnectionPool(a,comm['max_connection_endpoints'])
        else:self.pool=FairConnectionPool(comm['connections_per_endpoint'],comm['max_connection_endpoints'])
        self.pool.tcp_nodelay=c['network'].get('rpc_tcp_nodelay',False)
        # Lease traffic has independent connections and cannot consume entry capacity.
        self.lease_pool=FairConnectionPool(1,comm['max_connection_endpoints'])
        self.lease_pool.tcp_nodelay=c['network'].get('rpc_tcp_nodelay',False)
        a=p.get('adaptive_services',{})
        if a.get('enabled') and a['head_dispatch']:
            from .adaptive_service import AdaptiveServicePool
            self.dispatcher=AdaptiveServicePool(a,a['head_dispatch_min'],a['head_dispatch_max'],
                p['max_active_queries'],'dhdgs-head-dispatch')
        else:self.dispatcher=QueuedSender(p['head_pipeline']['dispatch_workers'],p['max_active_queries'],'dhdgs-head-dispatch')
        self.reactor=CallbackReactor(c,self.callback);self.endpoint=self.reactor.endpoint
        self.heartbeat=threading.Thread(target=self._heartbeat,name='dhdgs-head-heartbeat',daemon=True)
        self.heartbeat.start()

    def register(self,s,deferred=False):
        now=time.monotonic_ns()
        with self.cv:
            qid=s.registry.query_id
            if self.stopping or len(self.sessions)>=self.c['peer']['max_active_queries']:raise RuntimeError('Head state table capacity exceeded')
            if qid in self.sessions or qid in self.closed:raise ValueError('Duplicate head query identity')
            self.sessions[qid]=s
            self.pending[qid]=dict(future=Future(),entry=None,deadline=float('inf'),
                next_lease=time.monotonic()+self.c['peer']['heartbeat_interval_ms']/1000)
            self.cv.notify()
        s.receiver_metrics.update(registration_mode='single_process_pipeline',registration_ms=0.,
            registration_ready_ms=(time.monotonic_ns()-now)/1e6,registered_ns=now,clock_domain=CLOCK_DOMAIN)
        return self.endpoint

    def enqueue(self,fn):
        """fn creates state on the dispatch thread, then sends the entry RPC."""
        proxy=Future()
        def run():
            if proxy.cancelled():return
            try:
                operation=fn()
                def complete(f):
                    try:resolve(proxy,f.result())
                    except BaseException as exc:resolve(proxy,error=exc)
                operation.add_done_callback(complete)
                proxy.add_done_callback(lambda f:operation.cancel() if f.cancelled() else None)
            except BaseException as exc:resolve(proxy,error=exc)
        try:self.dispatcher.submit(run)
        except BaseException as exc:resolve(proxy,error=exc)
        return proxy

    def submit_query(self,s,state,endpoint,deadline):
        with self.cv:
            item=self.pending[s.registry.query_id];item['deadline']=deadline;self.cv.notify()
        enter=time.monotonic_ns();s.receiver_metrics['entry_submit_enter_ns']=enter
        try:
            with s.delivery_lock:
                if s.closed:raise RuntimeError('Query cancelled before entry dispatch')
            if time.monotonic()>=deadline:raise TimeoutError('Query deadline before entry dispatch')
            reply,counts=self.exchange(endpoint,{'version':1,'op':'peer_submit','state':state},
                self.c['network']['max_message_bytes'],self.c['peer']['handoff_timeout_seconds'])
            with s.delivery_lock:
                item['entry']=dict(reply=reply,counts=counts,enter_ns=enter,return_ns=time.monotonic_ns())
                self._finish(s,item)
                cancelled=s.closed
            if cancelled:
                self.exchange(endpoint,dict(version=1,op='peer_cancel',query_request_id=s.registry.query_id,
                    token=s.registry.token),self.c['network']['max_message_bytes'],self.c['peer']['control_timeout_seconds'])
        except (OSError,RuntimeError,ValueError) as exc:resolve(item['future'],error=exc)
        return item['future']

    def _finish(self,s,item):
        if item['future'].done():return
        try:complete=s.registry.complete()
        except (RuntimeError,ValueError,KeyError,TypeError) as exc:resolve(item['future'],error=exc);return
        if complete and item['entry'] is not None:
            s.receiver_metrics['query_ready_ns']=time.monotonic_ns()
            resolve(item['future'],item['entry'])

    def callback(self,message,rx):
        qid=message.get('query_request_id')
        with self.lock:s=self.sessions.get(qid)
        if s is None:
            with self.lock:old=self.closed.get(qid)
            if old is None or time.monotonic()-old['time']>self.c['peer']['retention_seconds']:
                return dict(ok=False,error='Unknown or expired query'),None
            if message.get('token')!=old['token']:return dict(ok=False,error='Invalid token'),None
            for r in message.get('reports',[]):
                if old['success'] and old['digests'].get(r['branch_id'])!=hashlib.sha256(msgpack.packb(r,use_bin_type=True)).hexdigest():
                    return dict(ok=False,error='Conflicting terminal report'),None
            return dict(ok=True,cancelled=True,lease_ms=self.c['peer']['lease_timeout_ms'],involved_nodes=old['nodes']),None
        with s.delivery_lock:
            if s.closed:return self.callback(message,rx)
            try:nodes=s.registry.update(message)
            except (ValueError,RuntimeError,KeyError,TypeError) as exc:return dict(ok=False,error=str(exc)),None
            for r in message.get('reports',[]):s.report_digests[r['branch_id']]=hashlib.sha256(msgpack.packb(r,use_bin_type=True)).hexdigest()
            stamp=time.monotonic_ns();s.receiver_metrics.setdefault('first_callback_dispatch_ns',stamp)
            s.receiver_metrics['last_callback_dispatch_ns']=stamp
            kind='result' if message.get('reports') or message.get('error') else 'heartbeat'
        def queued(tx):
            with s.delivery_lock:
                if s.closed:return
                s.record_control(kind,dict(rpc_count=1,request_bytes=rx,response_bytes=tx))
                s.done.set()
                with self.lock:item=self.pending.get(qid)
                if item is not None:self._finish(s,item)
        return dict(ok=True,cancelled=False,lease_ms=self.c['peer']['lease_timeout_ms'],involved_nodes=nodes),queued

    def _heartbeat(self):
        from .peer_rpc import rpc_exchange
        interval=self.c['peer']['heartbeat_interval_ms']/1000
        while True:
            with self.cv:
                if self.stopping:return
                rows=[(s,self.pending[q]) for q,s in self.sessions.items()]
                due=min((min(i['deadline'],i['next_lease']) for _,i in rows if not i['future'].done()),default=time.monotonic()+interval)
                delay=due-time.monotonic()
                if delay>0:self.cv.wait(delay);continue
            for s,item in rows:
                if item['future'].done():continue
                now=time.monotonic()
                if now>=item['deadline']:
                    resolve(item['future'],error=TimeoutError('Global query deadline exceeded'));continue
                if now<item['next_lease']:continue
                item['next_lease']=now+interval
                with s.delivery_lock:
                    if s.closed:continue
                    completed={r['node_id'] for r in s.registry.reports.values()}
                    nodes=list(s.registry.nodes-completed)
                for sid in nodes:
                    if item['future'].done():break
                    node=self.c['cluster']['nodes'][sid]
                    try:
                        _,counts=rpc_exchange([node['host'],node['port']],dict(version=1,op='peer_lease',
                            query_request_id=s.registry.query_id,token=s.registry.token),
                            self.c['network']['max_message_bytes'],self.c['peer']['control_timeout_seconds'],self.lease_pool)
                        with s.delivery_lock:
                            if not s.closed:s.record_control('lease',counts)
                    except (OSError,ValueError,RuntimeError) as exc:
                        with s.delivery_lock:
                            if not s.closed:s.record_control_error('lease',sid,exc)

    def unregister(self,s,success):
        super().unregister(s,success)
        with self.cv:self.pending.pop(s.registry.query_id,None);self.cv.notify()

    def close(self):
        with self.cv:self.stopping=True;self.cv.notify_all();items=list(self.pending.values())
        for item in items:resolve(item['future'],error=RuntimeError('Head pipeline closed'))
        self.heartbeat.join();self.dispatcher.shutdown(wait=True);self.reactor.close()
        self.pool.close();self.lease_pool.close()
