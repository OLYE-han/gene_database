"""Authenticated, quiescent configuration changes for a persistent data process.

Only query-time settings are mutable. A transaction swaps PeerRuntime, retaining
Shard (including its fd and native graph/quantizer). Startup is fail-closed until
the head verifies and resumes all nodes. Data RPC is still a trusted LAN protocol.
"""
from contextlib import contextmanager
import copy
import hashlib
import hmac
import json
import os
from pathlib import Path
import socket
import socketserver
import threading
import time
import uuid

import msgpack

from .config import validate
from .index import Shard, shard_path
from .peer import PeerRuntime
from .cpp_runtime import make_peer
from .transport import send, receive
from .runtime import apply_process_runtime
from .metrics import ResourceSampler

HOT_FIELDS = frozenset('''search.top_k search.exclude_self search.frontier_size
search.local_beam search.max_expansions search.max_inflight search.fallback_enabled
search.max_fallback_entries search.stagnation_batches
routing.ef_search routing.entry_candidates
scheduler.mode scheduler.kappa scheduler.exploration scheduler.max_wait_ms
scheduler.local_batch_ms scheduler.remote_rtt_ms scheduler.bandwidth_bytes_per_ms
scheduler.ewma_weight scheduler.remote_weight scheduler.remote_selection
scheduler.probability_temperature peer.worker_threads peer.child_budget_fraction
peer.max_branches'''.split())


def clean_config(c):
    return copy.deepcopy({k:v for k,v in c.items() if not k.startswith('_')})


def hot_values(c):
    return {key:c[key.split('.')[0]][key.split('.')[1]] for key in sorted(HOT_FIELDS)}


def revision(c):
    return hashlib.sha256(json.dumps(hot_values(c),sort_keys=True,allow_nan=False).encode()).hexdigest()


def patched(c, patch):
    if not isinstance(patch,dict) or set(patch)-HOT_FIELDS:
        raise ValueError('Unsupported live settings: '+str(sorted(set(patch or {})-HOT_FIELDS)))
    out=clean_config(c)
    for key,value in patch.items():
        group,name=key.split('.');out[group][name]=copy.deepcopy(value)
    validate(out)
    if out['search']['execution']!='peer' or out['network']['transport']!='tcp':
        raise ValueError('Managed nodes require peer/TCP')
    return out


def save_json(path, value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name(path.name+'.tmp')
    with temp.open('w') as f:
        json.dump(value,f,indent=2,allow_nan=False);f.flush();os.fsync(f.fileno())
    temp.replace(path)


def idle(metrics):
    if any(metrics.get(k,0) for k in ('unacknowledged','running','queued','waiting_io',
           'waiting_transfer','admission_queue_depth','admission_pending_start','control_active')):return False
    queues=list((metrics.get('send_queues') or {}).values())+[metrics.get('compute_queue') or {}]
    if any(q.get('queued',0) or q.get('active',0) for q in queues):return False
    n=metrics.get('native_control') or {}
    return not any(n.get(k,0) for k in ('client_pending','command_queue_depth','control_active'))


class NodeControl:
    @property
    def accepting(self):
        return self._accepting

    @accepting.setter
    def accepting(self, value):
        self._accepting = value
        transport = getattr(self, '_native_transport', None)
        if transport is not None:
            transport.configure(self.peer, value, self.rev)

    @property
    def peer(self):
        return self._peer

    @peer.setter
    def peer(self, value):
        self._peer = value
        transport = getattr(self, '_native_transport', None)
        if transport is not None:
            transport.configure(value, self.accepting, self.rev)

    def __init__(self,c,sid,token,state_file):
        if len(token)<32:raise ValueError('Management token must have at least 32 characters')
        self.token=token;self.sid=sid;self.state_file=Path(state_file)
        self.c=clean_config(c)
        if self.state_file.exists():
            saved=json.loads(self.state_file.read_text())
            if saved['node_id']!=sid:raise ValueError('Wrong persisted node identity')
            # Only restore allowed query settings; never restore paths or ports.
            self.c=patched(self.c,saved['settings'])
        apply_process_runtime(self.c)
        self.worker=Shard(shard_path(self.c,sid),self.c['storage']['cache_bytes_per_shard'])
        m=self.worker.meta
        if (m['shard_id'],m['dimension'],m['metric'],m['bits'],m['header_bytes'])!=(sid,
                self.c['data']['dimension'],self.c['data']['metric'],self.c['quantization']['bits'],self.c['storage']['page_bytes']):
            self.worker.close();raise ValueError('Node configuration differs from immutable shard')
        self.lock=threading.RLock();self.admin_lock=threading.Lock()
        self.accepting=False;self.readers=0;self.transaction=None;self.last_transaction=None
        self.snapshot_data=None;self.snapshot_id=None
        self.rev=revision(self.c)
        self.peer=make_peer(self.c,self.worker)
        self.sampler=ResourceSampler(self.c['metrics']['resource_sample_interval_ms'],self.c['metrics']['resource_max_samples']).start()

    @contextmanager
    def data(self,request):
        with self.lock:
            if request.get('op')=='peer_submit':
                if not self.accepting:raise RuntimeError('Data node is paused for configuration')
                if request.get('state',{}).get('cluster_revision')!=self.rev:
                    raise ValueError('Query configuration revision mismatch')
            self.readers+=1;peer=self.peer
        try:yield peer
        finally:
            with self.lock:self.readers-=1

    def status(self):
        with self.lock:
            metrics=self.peer.metrics()
            return dict(node_id=self.sid,pid=os.getpid(),hostname=socket.gethostname(),
                epoch=self.worker.meta['epoch'],revision=self.rev,settings=hot_values(self.c),
                accepting=self.accepting,quiescent=self.readers==0 and idle(metrics),
                transaction=None if self.transaction is None else dict(id=self.transaction['id'],phase=self.transaction['phase']),
                last_transaction=self.last_transaction,index_fd=self.worker.fd,
                index_directory=str(self.worker.directory),worker_threads=self.c['peer']['worker_threads'])

    def metrics(self):
        result=self.worker.metrics()
        result['peer_lifecycle']=self.peer.metrics()
        transport=getattr(self,'_native_transport',None)
        if transport is not None:
            result['network_receiver']=transport.metrics()
        from .gc_profile import snapshot
        result['gc_profile']=snapshot();result['resource_samples']=list(self.sampler.samples)
        result['managed']=self.status()
        return result

    def command(self,request):
        token=request.get('token')
        if not isinstance(token,str) or not hmac.compare_digest(token,self.token):
            raise ValueError('Invalid management token')
        op=request['op']
        with self.admin_lock:
            if op=='admin_status':return self.status()
            if op=='admin_network_metrics':
                runtime=getattr(self.peer,'runtime',None)
                if runtime is None:raise ValueError('Lightweight network metrics require native runtime')
                from .metrics import process_snapshot
                return dict(node_id=self.sid,time_ns=time.monotonic_ns(),runtime=runtime.network_metrics(),
                            receiver=self._native_transport.metrics(),process=process_snapshot())
            if op=='admin_snapshot':
                raw=msgpack.packb(self.metrics(),use_bin_type=True)
                self.snapshot_id=uuid.uuid4().hex;self.snapshot_data=raw
                return dict(snapshot_id=self.snapshot_id,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
            if op=='admin_chunk':
                offset=request['offset'];count=request['count']
                if (self.snapshot_data is None or request['snapshot_id']!=self.snapshot_id or type(offset)is not int or
                        type(count)is not int or offset<0 or not 0<count<=262144 or offset>len(self.snapshot_data or b'')):
                    raise ValueError('Invalid metrics chunk')
                return {'data':self.snapshot_data[offset:offset+count]}
            with self.lock:
                txid=request.get('transaction')
                if op=='admin_prepare':
                    if not isinstance(txid,str) or not txid or len(txid)>128:raise ValueError('Invalid transaction ID')
                    candidate=patched(self.c,request['patch'])
                    if self.transaction is not None:
                        t=self.transaction
                        if t['id']==txid and revision(t['candidate_config'])==revision(candidate):return self.status()
                        raise RuntimeError('Another configuration transaction is pending')
                    if request['expected_revision']!=revision(self.c):raise ValueError('Stale expected revision')
                    was_open=self.accepting;self.accepting=False
                    try:
                        if self.readers or not idle(self.peer.metrics()):raise RuntimeError('Node still has in-flight work')
                        new_peer=make_peer(candidate,self.worker)
                    except BaseException:
                        self.accepting=was_open;raise
                    self.transaction=dict(id=txid,phase='prepared',old_peer=self.peer,old_config=self.c,
                        candidate_peer=new_peer,candidate_config=candidate,was_open=was_open)
                elif op in ('admin_commit','admin_abort','admin_finalize'):
                    t=self.transaction
                    if t is None:
                        if self.last_transaction and self.last_transaction['id']==txid:
                            outcome=self.last_transaction['outcome']
                            if (op=='admin_abort') != (outcome=='aborted'):
                                raise RuntimeError('Transaction already resolved differently; use a new transaction')
                            return self.status()
                        if op=='admin_abort':return self.status()
                        raise ValueError('Unknown transaction')
                    if t['id']!=txid:raise ValueError('Transaction identity mismatch')
                    if self.readers:raise RuntimeError('A data request is still active')
                    if op=='admin_commit' and t['phase']=='prepared':
                        save_json(self.state_file,dict(node_id=self.sid,settings=hot_values(t['candidate_config'])))
                        self.c=t['candidate_config'];self.rev=revision(self.c);self.peer=t['candidate_peer'];t['phase']='committed'
                    elif op=='admin_abort':
                        save_json(self.state_file,dict(node_id=self.sid,settings=hot_values(t['old_config'])))
                        self.c=t['old_config'];self.rev=revision(self.c);self.peer=t['old_peer'];t['candidate_peer'].close()
                        self.transaction=None;self.accepting=False
                        self.last_transaction=dict(id=txid,outcome='aborted')
                    elif op=='admin_finalize':
                        if t['phase']!='committed':raise ValueError('Cannot finalize before commit')
                        t['old_peer'].close();self.transaction=None
                        self.last_transaction=dict(id=txid,outcome='committed')
                elif op=='admin_resume':
                    if self.transaction is not None or request['revision']!=revision(self.c):
                        raise ValueError('Cannot resume an uncommitted or different revision')
                    self.accepting=True
                elif op=='admin_pause':self.accepting=False
                elif op=='admin_shutdown':
                    if self.readers or not idle(self.peer.metrics()):raise RuntimeError('Cannot stop a busy node')
                    self.accepting=False
                    threading.Thread(target=self.shutdown,daemon=True).start()
                else:raise ValueError('Unknown management operation')
                return self.status()

    def close(self):
        with self.admin_lock:
            self.accepting=False
            if self.transaction is not None:
                peers={id(p):p for p in (self.peer,self.transaction['old_peer'],self.transaction['candidate_peer'])}
            else:peers={id(self.peer):self.peer}
            for p in peers.values():p.close()
            self.sampler.finish();self.worker.close()


def create_managed_server(c,sid,token,state_file):
    owner=NodeControl(c,sid,token,state_file);limit=c['network']['max_message_bytes']
    from .cpp_runtime import enabled, NativeServer
    if enabled(c):
        def admin(request):
            if request.get('version') != 1:
                raise ValueError('Unsupported protocol')
            result = owner.metrics() if request.get('op') == 'metrics' else owner.command(request)
            return dict(ok=True, request_id=request.get('request_id'), result=result)
        try:
            server = NativeServer(c, sid, owner.peer, admin, owner.close)
            owner._native_transport = server
            server.configure(owner.peer, False, owner.rev)
        except BaseException:
            owner.close()
            raise
        server.control = owner
        # The native reactor signals shutdown after transmitting the admin ACK.
        owner.shutdown = lambda: None
        return server
    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.settimeout(c['network']['timeout_seconds'])
            while True:
                try:request,_=receive(self.request,limit);received=time.monotonic_ns()
                except (OSError,ValueError,msgpack.UnpackException):return
                rid=request.get('request_id');trace=None
                try:
                    op=request.get('op','')
                    if request.get('version')!=1:raise ValueError('Unsupported protocol')
                    if not isinstance(op,str):raise ValueError('Invalid operation')
                    if op.startswith('admin_'):
                        result=owner.command(request)
                        response=dict(ok=True,request_id=rid,result=result)
                    else:
                        with owner.data(request) as peer:
                            if op=='peer_submit':
                                enter=time.monotonic_ns();state=request['state'];result=peer.accept(state)
                                trace=peer.records[(state['query_request_id'],state['branch_id'])].get('execution_timestamps')
                                if trace is not None and not result.get('duplicate'):
                                    trace.update(receiver_message_ready_ns=received,receiver_accept_enter_ns=enter,
                                                 receiver_accept_return_ns=time.monotonic_ns())
                                else:trace=None
                            elif op in ('peer_lease','peer_cancel'):
                                result=peer.lease(request['query_request_id'],request['token'],op=='peer_cancel')
                            elif op=='metrics':result=owner.metrics()
                            else:raise ValueError('Unsupported managed data request')
                        response=dict(ok=True,result=result)
                        if op=='metrics':response['request_id']=rid
                except (ValueError,RuntimeError,KeyError,TypeError,OSError) as exc:
                    response=dict(ok=False,request_id=rid,error=str(exc))
                try:
                    if trace is not None:trace['receiver_ack_send_enter_ns']=time.monotonic_ns()
                    send(self.request,response,limit)
                    if trace is not None:trace['receiver_ack_send_return_ns']=time.monotonic_ns()
                except (OSError,ValueError):return
    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address=True;daemon_threads=True
        request_queue_size=c['network']['listen_backlog']
        def server_close(self):
            super().server_close();owner.close()
    try:
        node=c['cluster']['nodes'][sid];server=Server((node['host'],node['port']),Handler)
    except BaseException:owner.close();raise
    server.control=owner
    owner.shutdown=server.shutdown
    return server
