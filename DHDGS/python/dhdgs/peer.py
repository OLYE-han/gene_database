"""Peer branches plus a control-only head registry. Trusted-cluster protocol."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, Future
from collections import deque
import hashlib
from contextlib import nullcontext
import json
from pathlib import Path
import socket
import socketserver
import threading
import time
import uuid

import msgpack
import numpy as np

from .data import load_complete_npy
from .peer_search import run_branch
from .tracing import trace_lifecycle
from .transport import send, receive


def execution_signature(c):
    fields={k:c[k] for k in ('search','scheduler','quantization','data')}
    fields['peer_protocol']='single_visit_parallel_local_v1'
    fields['peer']={k:v for k,v in c['peer'].items() if not k.startswith('head_')}
    # Entry transport scheduling does not change data-node execution. Omit the
    # optional head-only setting, preserving signatures of archived configs.
    if 'connection_pool' in fields['peer']:
        fields['peer']['connection_pool']={k:v for k,v in fields['peer']['connection_pool'].items()
                                           if k!='head_scheduling'}
    return hashlib.sha256(json.dumps(fields,sort_keys=True).encode()).hexdigest()


def exchange(endpoint, message, limit, timeout):
    from .peer_rpc import rpc_exchange
    return rpc_exchange(endpoint,message,limit,timeout)



class BranchRegistry:
    """Control state only: no frontier, distances or intermediate candidates."""
    def __init__(self, query_id, token, root_id, root_node, budget, slots, node_count):
        self.query_id,self.token,self.root_id=query_id,token,root_id
        self.root_node,self.budget,self.slots,self.node_count=root_node,budget,slots,node_count
        self.nodes={root_node}
        self.reports={}
        self.heartbeat_count=0
        self.last_seen={}
        self.error=None
        self.lock=threading.Lock()

    def update(self, message):
        with self.lock:
            if message.get('query_request_id')!=self.query_id or message.get('token')!=self.token:
                raise ValueError('Invalid query control token')
            sid=message['node_id']
            if type(sid) is not int or not 0<=sid<self.node_count: raise ValueError('Invalid control node')
            involved=message['involved_nodes']
            if any(type(v) is not int or not 0<=v<self.node_count for v in involved):
                raise ValueError('Invalid involved node')
            self.nodes.update(involved); self.nodes.add(sid)
            self.last_seen[sid]=time.monotonic()
            self.heartbeat_count+=1
            if message.get('error'): self.error=message['error']
            for report in message.get('reports',[]):
                if report['node_id']!=sid: raise ValueError('Report origin mismatch')
                bid=report['branch_id']
                if bid in self.reports and self.reports[bid]!=report:
                    raise ValueError('Conflicting duplicate branch result')
                self.reports[bid]=report
            return sorted(self.nodes)

    def complete(self):
        with self.lock:
            if self.error: raise RuntimeError(self.error)
            expected={self.root_id:(None,self.root_node,self.budget,self.slots)}
            visited=set(); visited_nodes=set(); pending=[self.root_id]
            while pending:
                bid=pending.pop()
                if bid in visited: raise ValueError('Branch cycle or duplicate child')
                visited.add(bid)
                report=self.reports.get(bid)
                if report is None: return False
                parent,node,budget,slots=expected[bid]
                path=([node] if parent is None else [*self.reports[parent]['visited_nodes'],node])
                if node in visited_nodes or report['visited_nodes']!=path:
                    raise ValueError('Repeated data node or invalid visited-node path')
                visited_nodes.add(node)
                if (report['parent_branch_id'],report['node_id'],report['work_grant'],report['branch_slots'])!=(parent,node,budget,slots):
                    raise ValueError('Branch credit/parent mismatch')
                spent,unused=report['work_spent'],report['work_unused']
                if type(spent) is not int or type(unused) is not int or min(spent,unused)<0:
                    raise ValueError('Invalid branch credit accounting')
                children=report['children']
                if len(children)>1: raise ValueError('A data node may forward only once')
                if spent+unused+sum(child['work_grant'] for child in children)!=budget:
                    raise ValueError('Global work credit conservation violation')
                if sum(1+child['branch_slots'] for child in children)>slots:
                    raise ValueError('Branch slot conservation violation')
                for child in children:
                    if 'decisions' in report:
                        approvals=[d for d in report['decisions'] if d['decision_id']==child.get('decision_id')
                                   and d['remote'] and d['selected_node']==child['node_id']]
                        if len(approvals)!=1:raise ValueError('Remote child lacks a unique approved decision')
                    cid=child['branch_id']
                    if cid in expected or child['parent_branch_id']!=bid: raise ValueError('Invalid child manifest')
                    if child['work_grant']<1 or child['branch_slots']<0 or not 0<=child['node_id']<self.node_count:
                        raise ValueError('Invalid child credit')
                    expected[cid]=(bid,child['node_id'],child['work_grant'],child['branch_slots'])
                    pending.append(cid)
            if set(self.reports)!=visited: raise ValueError('Orphan final report')
            return True


class HeadSession:
    def __init__(self,c,qid,token,root_id,root_node,hub=None,deferred=False):
        self.c=c
        self.registry=BranchRegistry(qid,token,root_id,root_node,c['search']['max_expansions'],
                                     c['peer']['max_branches']-1,len(c['cluster']['nodes']))
        self.done=threading.Event()
        self.closed=False
        self.control={'rpc_count':0,'request_bytes':0,'response_bytes':0,'errors':0}
        self.control_error_details=[]
        self.control_by_kind={kind:{'rpc_count':0,'request_bytes':0,'response_bytes':0}
                              for kind in ('heartbeat','result','lease','cancel')}
        self.optimized=c['peer']['communication']['mode']=='optimized'
        self.delivery_lock=threading.RLock();self.report_digests={};self.owns_hub=False
        if self.optimized:
            if hub is None:
                from .head_hub import acquire_hub
                hub=acquire_hub(c);self.owns_hub=True
            self.hub=hub;self.receiver_metrics={}
            try:self.endpoint=hub.register(self,deferred=True) if deferred else hub.register(self)
            except Exception:
                if self.owns_hub:
                    from .head_hub import release_hub
                    release_hub(hub)
                raise
            return
        owner=self
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(c['peer']['control_timeout_seconds'])
                try:
                    message,rx=receive(self.request,c['network']['max_message_bytes'])
                    nodes=owner.registry.update(message)
                    reply={'ok':True,'cancelled':owner.closed,'lease_ms':c['peer']['lease_timeout_ms'],
                           'involved_nodes':nodes}
                    tx=send(self.request,reply,c['network']['max_message_bytes'])
                    owner.record_control('result' if message.get('reports') or message.get('error') else 'heartbeat',
                                         {'rpc_count':1,'request_bytes':rx,'response_bytes':tx})
                    owner.done.set()
                except Exception as exc:
                    try: send(self.request,{'ok':False,'error':str(exc)},c['network']['max_message_bytes'])
                    except OSError: pass
        class Server(socketserver.ThreadingTCPServer):
            daemon_threads=False  # close waits for terminal callback accounting to finish
            allow_reuse_address=True
            request_queue_size=c['network'].get('listen_backlog',5)
        self.server=Server((c['peer']['head_bind_host'],c['peer']['head_port']),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.005},daemon=True)
        self.thread.start()
        self.endpoint=[c['peer']['head_advertise_host'],self.server.server_address[1]]

    def record_control(self,kind,counts):
        with self.registry.lock:
            for key in ('rpc_count','request_bytes','response_bytes'):
                self.control[key]+=counts[key]
                self.control_by_kind[kind][key]+=counts[key]

    def record_control_error(self,kind,node,exc):
        with self.registry.lock:
            self.control['errors']+=1
            self.control_error_details.append({'kind':kind,'node_id':node,'error_type':type(exc).__name__,'error':str(exc)})

    def exchange(self,*args):
        if self.optimized:return self.hub.exchange(*args)
        return exchange(*args)

    def wait(self, deadline):
        if self.optimized:
            while True:
                self.done.clear()
                with self.delivery_lock:
                    if self.registry.complete():return list(self.registry.reports.values())
                remaining=deadline-time.monotonic()
                if remaining<=0:raise TimeoutError('Global query deadline exceeded')
                self.done.wait(remaining)
        last=0.
        while True:
            if self.registry.complete(): return list(self.registry.reports.values())
            now=time.monotonic()
            if now>=deadline: raise TimeoutError('Global query deadline exceeded')
            if now-last>=self.c['peer']['heartbeat_interval_ms']/1000:
                with self.registry.lock: nodes=list(self.registry.nodes)
                for sid in nodes:
                    node=self.c['cluster']['nodes'][sid]
                    try:
                        _,counts=exchange([node['host'],node['port']],{
                            'version':1,'op':'peer_lease','query_request_id':self.registry.query_id,
                            'token':self.registry.token},self.c['network']['max_message_bytes'],
                            self.c['peer']['control_timeout_seconds'])
                        self.record_control('lease',counts)
                    except (OSError,RuntimeError,ValueError) as exc:
                        self.record_control_error('lease',sid,exc)
                last=now
            self.done.wait(min(.05,max(0,deadline-time.monotonic())))
            self.done.clear()

    def close(self,success=False):
        if self.optimized:
            with self.delivery_lock:
                if self.closed:return
                if success and not self.registry.complete():raise ValueError('Cannot complete unfinished query')
                self.closed=True
                self.hub.unregister(self,success)
            if not success:
                with self.registry.lock:nodes=list(self.registry.nodes)
                for sid in nodes:
                    node=self.c['cluster']['nodes'][sid]
                    try:
                        _,counts=self.exchange([node['host'],node['port']],{'version':1,'op':'peer_cancel',
                            'query_request_id':self.registry.query_id,'token':self.registry.token},
                            self.c['network']['max_message_bytes'],self.c['peer']['control_timeout_seconds'])
                        self.record_control('cancel',counts)
                    except (OSError,ValueError,RuntimeError) as exc:self.record_control_error('cancel',sid,exc)
            if self.owns_hub:
                from .head_hub import release_hub
                release_hub(self.hub)
            return
        self.closed=True
        with self.registry.lock: nodes=list(self.registry.nodes)
        for sid in nodes:
            node=self.c['cluster']['nodes'][sid]
            try:
                _,counts=exchange([node['host'],node['port']],{'version':1,'op':'peer_cancel',
                    'query_request_id':self.registry.query_id,'token':self.registry.token},
                    self.c['network']['max_message_bytes'],self.c['peer']['control_timeout_seconds'])
                self.record_control('cancel',counts)
            except (OSError,ValueError,RuntimeError) as exc:
                self.record_control_error('cancel',sid,exc)
        self.server.shutdown(); self.server.server_close(); self.thread.join()


class PeerRuntime:
    def __init__(self,c,shard):
        self.c,self.shard=c,shard
        # Protocol/search configuration is immutable for this runtime's life.
        # Apply configuration changes by creating a new runtime, not editing c.
        self.signature=execution_signature(c)
        bank=shard.directory/'code_membership.npy'
        if bank.exists():
            membership=load_complete_npy(bank)
            ids=shard.code_ids
        else:
            # Compatibility for pilot indexes; multi-host deployments must copy the new sidecar.
            root=Path(c['paths']['output'])/'index'
            ids=load_complete_npy(root/'ids.npy');membership=load_complete_npy(root/'membership.npy')
        if membership.shape!=(len(ids),len(c['cluster']['nodes'])): raise ValueError('Invalid peer owner directory')
        # Native SearchShard consumes the compact sidecar directly. A second
        # million-entry Python dict/list copy is unused and increases GC scans.
        self.directory=(None if c['peer']['execution_model']=='native' and bank.exists() else
                        {int(gid):list(map(int,np.flatnonzero(membership[i]))) for i,gid in enumerate(ids)})
        self.lock=threading.Lock()
        self.records={}
        self.active_count=0  # records whose final report/error is not ACKed
        self.tombstones={}
        self.query_visits={}
        self.stop=threading.Event()
        self.control_wakeup=threading.Event()
        self.io=None
        if c['peer']['execution_model']=='native':
            from .native_search import prepare_shard
            prepare_shard(shard,self.directory)
        if c['peer']['execution_model']=='resumable' or (c['peer']['execution_model']=='native' and c['storage']['io']['backend']!='sync_pread'):
            from .async_io import LocalIO
            if 2*c['search']['local_beam']*shard.meta['record_bytes']>c['storage']['io']['buffer_pool_bytes_per_device']:
                raise ValueError('Buffer budget must hold one complete local beam (including copy)')
            self.io=LocalIO(shard,c['storage']['io'],trace=c['metrics']['detailed_trace'] or bool(c['metrics'].get('trace_query_ids')))
        self.optimized=c['peer']['communication']['mode']=='optimized'
        comm=c['peer']['communication']
        self.adaptive_settings=c['peer'].get('adaptive_services',{})
        self.adaptive_enabled=self.adaptive_settings.get('enabled',False)
        self.rpc_pool=None
        if self.optimized and comm['reuse_connections']:
            from .peer_rpc import ConnectionPool
            if self.adaptive_enabled and self.adaptive_settings['connections']:
                from .adaptive_connections import AdaptiveConnectionPool
                self.rpc_pool=AdaptiveConnectionPool(self.adaptive_settings,comm['max_connection_endpoints'])
            elif c['peer']['connection_pool'].get('data_scheduling','broadcast')=='fifo':
                from .peer_rpc import FairConnectionPool
                self.rpc_pool=FairConnectionPool(comm['connections_per_endpoint'],comm['max_connection_endpoints'])
            else:self.rpc_pool=ConnectionPool(comm['connections_per_endpoint'],comm['max_connection_endpoints'],
                                             c['peer']['connection_pool']['probe_mode'])
        if self.rpc_pool is not None:self.rpc_pool.tcp_nodelay=c['network'].get('rpc_tcp_nodelay',False)
        self.queued_senders=self.optimized and c['peer']['sender']['mode']=='queued'
        self.native_controller=None
        if c['peer']['control']['scheduler']=='native':
            from .index import native
            self.native_controller=native().NativeController(c)
        if self.queued_senders:
            from .sender import QueuedSender
            self.control_workers=(QueuedSender(comm['control_workers'],comm['max_pending_control'],'dhdgs-control')
                                  if self.native_controller is None else None)
            if self.adaptive_enabled and self.adaptive_settings['transfer']:
                from .adaptive_service import AdaptiveServicePool
                a=self.adaptive_settings
                self.transfers=AdaptiveServicePool(a,a['transfer_min'],a['transfer_max'],
                    c['peer']['sender']['transfer_queue_capacity'],'dhdgs-transfer')
            else:self.transfers=QueuedSender(comm['transfer_workers'],c['peer']['sender']['transfer_queue_capacity'],'dhdgs-transfer')
        else:
            self.control_workers=(ThreadPoolExecutor(max_workers=comm['control_workers'],thread_name_prefix='dhdgs-control')
                                  if self.optimized and self.native_controller is None else None)
            self.transfers=ThreadPoolExecutor(max_workers=comm['transfer_workers'] if self.optimized else c['search']['max_inflight'],thread_name_prefix='dhdgs-transfer')
        self.control_active=0
        self.control_scheduler_totals=dict(iterations=0,records_scanned=0,selected=0,
            no_capacity_iterations=0,cpu_ms=0.,lock_wait_ms=0.,lock_held_ms=0.)
        self.control_totals={'sent':0,'errors':0,'queue_ms':0.,'wall_ms':0.,'connections_created':0,'connections_reused':0}
        from .executor_trace import TracedComputeExecutor
        self.compute_mode=c['peer']['compute']['mode']
        self.compute_startup={}
        if self.compute_mode=='fixed':
            from .compute_executor import FixedComputeExecutor
            self.workers=FixedComputeExecutor(c['peer']['worker_threads'],c['peer']['compute']['queue_capacity'],'dhdgs-search')
        else:
            self.workers=TracedComputeExecutor(max_workers=c['peer']['worker_threads'],thread_name_prefix='dhdgs-search')
            if self.compute_mode=='prestarted':
                from .compute_executor import prestart_executor
                self.compute_startup=prestart_executor(self.workers,c['peer']['worker_threads'],
                                                       timeout=c['network']['timeout_seconds'])
        self.buffered_admission=c['peer']['admission']['mode']=='buffered'
        self.independent_admission=c['peer']['admission']['queue_lock']=='independent'
        self.admission_queue=deque()
        self.admission_cv=threading.Condition(threading.Lock() if self.independent_admission else self.lock)
        # A separate short accounting lock lets the unique execution owner
        # start a branch without waiting behind native registration/mirroring.
        self.admission_accounting_lock=threading.Lock()
        self.admission_pending=0;self.admission_peak_pending=0;self.admission_rejected=0
        self.control_scheduler=None
        if self.optimized and c['peer']['control']['scheduler']=='event':
            from .control_scheduler import ControlEventScheduler
            self.control_scheduler=ControlEventScheduler(self)
        self.admission_thread=None
        self.admission_threads=[]
        self.admission_pool=None
        if self.buffered_admission and self.adaptive_enabled and self.adaptive_settings['admission']:
            from .adaptive_service import AdaptiveServicePool
            a=self.adaptive_settings
            def downstream_ready():
                m=self.workers.metrics()
                return m['active']<m['workers'] and m['queued']==0
            self.admission_pool=AdaptiveServicePool(a,a['admission_min'],a['admission_max'],
                c['peer']['admission']['queue_capacity'],'dhdgs-admission',downstream_ready)
        elif self.buffered_admission:
            self.admission_threads=[threading.Thread(target=self._admit_loop,
                name=f'dhdgs-admission-{i}',daemon=True)
                for i in range(c['peer']['admission']['dispatch_threads'])]
            self.admission_thread=self.admission_threads[0]  # historical diagnostic alias
            for thread in self.admission_threads:thread.start()
        self.control_thread=threading.Thread(target=self._control,daemon=True)
        if self.native_controller is None:self.control_thread.start()

    def accept(self,state):
        traced=trace_lifecycle(self.c,state.get('query_id'))
        cpu_enter=time.thread_time_ns() if traced else None
        if state['epoch']!=self.shard.meta['epoch'] or state['signature']!=self.signature:
            raise ValueError('Peer epoch or execution configuration mismatch')
        if state['target_node']!=self.shard.meta['shard_id']: raise ValueError('Wrong branch destination')
        path=state['visited_nodes']
        if (not isinstance(path,list) or not path or path[-1]!=state['target_node']
                or path[0]!=state['entry_node'] or len(set(path))!=len(path)
                or any(type(s) is not int or not 0<=s<len(self.c['cluster']['nodes']) for s in path)
                or [a['node_id'] for a in state.get('ancestry',[])]!=path[:-1]):
            raise ValueError('Invalid visited-node path')
        if not 0<state['work_grant']<=self.c['search']['max_expansions']: raise ValueError('Invalid work grant')
        if not 0<=state['branch_slots']<self.c['peer']['max_branches']: raise ValueError('Invalid branch slots')
        if len(state['query_f32'])!=self.shard.meta['dimension']*4: raise ValueError('Wrong query bytes')
        if not state['seeds'] or len(state['seeds'])>self.c['search']['frontier_size']: raise ValueError('Invalid seeds')
        if any(self.shard.meta['shard_id'] not in row[3] for row in state['seeds']): raise ValueError('Nonlocal branch seed')
        carried=state.get('remote_frontier',[])
        if (len(carried)+len(state['seeds'])>self.c['search']['frontier_size']
                or any(not set(row[3])-set(path[:-1]) for row in carried)):
            raise ValueError('Invalid carried frontier')
        key=(state['query_request_id'],state['branch_id'])
        digest=hashlib.sha256(msgpack.packb(state,use_bin_type=True)).hexdigest()
        if self.c['peer'].get('locking',{}).get('split_native_registration',False):
            return self._accept_split_registration(state,key,digest,traced,cpu_enter)
        mirror_enter=time.monotonic_ns() if traced else None
        updates=(self.native_controller.take_updates() if self.native_controller is not None and
                 self.adaptive_settings.get('mirror_outside_lock',False) else None)
        lock_requested=time.monotonic_ns() if traced else None
        with self.lock:
            lock_acquired=time.monotonic_ns() if traced else None
            self._native_updates_locked(updates)
            mirror_applied=time.monotonic_ns() if traced else None
            if self.stop.is_set():raise RuntimeError('Peer runtime closed')
            if key in self.records:
                if self.records[key]['digest']!=digest: raise ValueError('Conflicting branch retry')
                return {'accepted':True,'duplicate':True}
            if key in self.tombstones: raise ValueError('Expired branch cannot be replayed')
            if state['query_request_id'] in self.query_visits:
                raise ValueError('Query already visited this data node')
            if self.active_count>=self.c['peer']['max_active_queries']: raise RuntimeError('Peer branch capacity exceeded')
            if len(self.records)>=self.c['peer']['max_tombstones']+self.c['peer']['max_active_queries']:
                raise RuntimeError('Peer dedup retention capacity exceeded')
            if self.buffered_admission and self.admission_pending>=self.c['peer']['admission']['queue_capacity']:
                self.admission_rejected+=1
                raise RuntimeError('Peer admission queue capacity exceeded')
            rec=self._new_record(state,digest);now=rec['created_at']
            if self.native_controller is not None:
                metadata={name:state[name] for name in ('query_request_id','branch_id','parent_branch_id',
                    'token','head_endpoint','entry_node','target_node','visited_nodes','ancestry','work_grant','branch_slots')}
                metadata.update(accepted_ns=time.monotonic_ns(),created_ns=int(now*1e9),
                                lease=rec['lease'],deadline=rec['deadline'],trace=traced)
                rec['native_ticket']=self.native_controller.register_branch(metadata,rec['native_control'])
            self.records[key]=rec
            self.active_count+=1
            self.query_visits[state['query_request_id']]=key
            if traced:
                rec['execution_timestamps']={'accepted_ns':time.monotonic_ns(),
                    'receiver_thread_id':threading.get_native_id(),
                    'admission_mode':'buffered' if self.buffered_admission else 'direct',
                    'accept_thread_cpu_enter_ns':cpu_enter,
                    'accept_state_lock_enter_ns':lock_requested,
                    'accept_state_lock_acquired_ns':lock_acquired,
                    'accept_mirror_enter_ns':mirror_enter,'accept_mirror_applied_ns':mirror_applied}
                rec['control_events']=[]
            if self.buffered_admission:
                # Publish identity, reserve pending capacity and enqueue atomically.
                with self.admission_accounting_lock:
                    self.admission_pending+=1
                    self.admission_peak_pending=max(self.admission_peak_pending,self.admission_pending)
                # Lock order is state -> queue. Consumer releases queue before
                # touching state/submitting work; no queue -> state inversion.
                if self.admission_pool is not None:
                    if 'execution_timestamps' in rec:
                        rec['execution_timestamps'].update(enqueued_ns=time.monotonic_ns())
                    self.admission_pool.submit(self._adaptive_admit,key)
                else:
                    with self.admission_cv if self.independent_admission else nullcontext():
                        self.admission_queue.append(key)
                        if 'execution_timestamps' in rec:
                            rec['execution_timestamps'].update(enqueued_ns=time.monotonic_ns(),
                                queue_depth_at_enqueue=len(self.admission_queue))
                        self.admission_cv.notify()
        if traced:rec['execution_timestamps']['accept_state_lock_released_ns']=time.monotonic_ns()
        if not self.buffered_admission:
            try:self._submit_branch(key)
            except Exception as exc:
                self._admission_failed(key,exc);raise
        self._control_changed(key)
        if traced:rec['execution_timestamps']['accept_thread_cpu_return_ns']=time.thread_time_ns()
        return {'accepted':True,'duplicate':False}

    def _new_record(self,state,digest):
        now=time.monotonic()
        initial_delay=self.c['peer']['control']['initial_heartbeat_delay_ms']/1000 if self.optimized else 0.
        rec={'state':state,'digest':digest,'status':'queued','lease':now+self.c['peer']['lease_timeout_ms']/1000,
             'deadline':now+self.c['peer']['query_timeout_seconds'],'cancelled':False,'children':[],
             'report':None,'error':None,'acked':False,'touched':now,'peer_rpc_count':0,'peer_request_bytes':0,
             'peer_response_bytes':0,'peer_transfer_errors':0,'created_at':now,
             'created_perf':time.perf_counter(),'pending_start':self.buffered_admission,
             'control_inflight':False,'control_next_at':now+initial_delay if initial_delay else 0.,
             'control_signature':((),False,None) if initial_delay else None}
        if self.c['peer']['execution_model']=='native':
            from .index import native
            rec['native_control']=native().SearchControl(rec['lease'],rec['deadline'])
        return rec

    def _accept_split_registration(self,state,key,digest,traced,cpu_enter):
        """Reserve identity/credits, register without D1, then commit once.

        A matching retry waits for the same registration Future outside D1.
        No successful ACK or searchable task exists before commit. Shutdown
        fences commit and waits for outstanding registrations before closing
        the native controller. A failed reservation releases each credit once.
        """
        rec=self._new_record(state,digest)
        ready=Future();rec['registration_future']=ready;rec['status']='registering'
        if traced:
            rec['execution_timestamps']={'receiver_thread_id':threading.get_native_id(),
                'admission_mode':'buffered','accept_thread_cpu_enter_ns':cpu_enter,
                'accept_mirror_enter_ns':time.monotonic_ns()}
            rec['control_events']=[]
        updates=self.native_controller.take_updates()
        retired=[];duplicate=False;pending=None
        trace=rec.get('execution_timestamps')
        if trace is not None:trace['accept_state_lock_enter_ns']=time.monotonic_ns()
        with self.lock:
            if trace is not None:trace['accept_state_lock_acquired_ns']=time.monotonic_ns()
            self._native_updates_locked(updates,retired)
            if trace is not None:trace['accept_mirror_applied_ns']=time.monotonic_ns()
            if self.stop.is_set():raise RuntimeError('Peer runtime closed')
            old=self.records.get(key)
            if old is not None:
                if old['digest']!=digest:raise ValueError('Conflicting branch retry')
                duplicate=True;pending=old.get('registration_future')
            else:
                if key in self.tombstones:raise ValueError('Expired branch cannot be replayed')
                if state['query_request_id'] in self.query_visits:raise ValueError('Query already visited this data node')
                if self.active_count>=self.c['peer']['max_active_queries']:raise RuntimeError('Peer branch capacity exceeded')
                if len(self.records)>=self.c['peer']['max_tombstones']+self.c['peer']['max_active_queries']:
                    raise RuntimeError('Peer dedup retention capacity exceeded')
                with self.admission_accounting_lock:
                    if self.admission_pending>=self.c['peer']['admission']['queue_capacity']:
                        self.admission_rejected+=1;raise RuntimeError('Peer admission queue capacity exceeded')
                    self.admission_pending+=1
                    self.admission_peak_pending=max(self.admission_peak_pending,self.admission_pending)
                self.records[key]=rec;self.active_count+=1
                self.query_visits[state['query_request_id']]=key
                if trace is not None:trace['accepted_ns']=time.monotonic_ns()
        if trace is not None:trace['accept_state_lock_released_ns']=time.monotonic_ns()
        retired.clear()  # Last Python/native references are dropped without D1.
        if duplicate:
            if pending is not None:pending.result(timeout=self.c['peer']['handoff_timeout_seconds'])
            return {'accepted':True,'duplicate':True}
        try:
            metadata={name:state[name] for name in ('query_request_id','branch_id','parent_branch_id',
                'token','head_endpoint','entry_node','target_node','visited_nodes','ancestry','work_grant','branch_slots')}
            metadata.update(accepted_ns=time.monotonic_ns(),created_ns=int(rec['created_at']*1e9),
                lease=rec['lease'],deadline=rec['deadline'],trace=traced)
            if trace is not None:trace['accept_native_register_enter_ns']=time.monotonic_ns()
            rec['native_ticket']=self.native_controller.register_branch(metadata,rec['native_control'])
            if trace is not None:
                trace['accept_native_register_return_ns']=time.monotonic_ns()
                trace['accept_commit_lock_enter_ns']=time.monotonic_ns()
            with self.lock:
                if trace is not None:trace['accept_commit_lock_acquired_ns']=time.monotonic_ns()
                if self.stop.is_set():raise RuntimeError('Peer runtime closed during registration')
                if rec.get('registration_expired') or rec['cancelled'] or rec['native_control'].cancelled():
                    raise TimeoutError('Branch cancelled or expired during registration')
                rec['status']='queued'
                if self.admission_pool is not None:
                    if trace is not None:trace['enqueued_ns']=time.monotonic_ns()
                    self.admission_pool.submit(self._adaptive_admit,key)
                else:
                    with self.admission_cv if self.independent_admission else nullcontext():
                        self.admission_queue.append(key)
                        if trace is not None:trace.update(enqueued_ns=time.monotonic_ns(),queue_depth_at_enqueue=len(self.admission_queue))
                        self.admission_cv.notify()
            if trace is not None:trace['accept_commit_lock_released_ns']=time.monotonic_ns()
        except BaseException as exc:
            # Native registration may have succeeded: keep that identity and
            # report failure, so a retry cannot start a second branch.
            if 'native_ticket' in rec and not rec.get('registration_expired'):self._admission_failed(key,exc)
            else:
                with self.lock:
                    self._release_pending_locked(rec);self._pop_record_locked(key)
                    if rec.get('registration_expired'):self.tombstones[key]=time.monotonic()
                    elif self.query_visits.get(key[0])==key:self.query_visits.pop(key[0],None)
            ready.set_exception(exc)
            raise
        ready.set_result(None)
        rec.pop('registration_future',None)
        if trace is not None:trace['accept_thread_cpu_return_ns']=time.thread_time_ns()
        return {'accepted':True,'duplicate':False}

    def _mark_acked_locked(self,rec):
        if not rec['acked']:
            rec['acked']=True
            self.active_count-=1

    def _pop_record_locked(self,key):
        rec=self.records.pop(key,None)
        if rec is not None and not rec['acked']:
            self.active_count-=1
        return rec

    def _control_changed(self,key):
        # Safe even under runtime.lock: dirty never obtains that lock.
        if self.native_controller is not None:return
        if self.control_scheduler is not None:self.control_scheduler.dirty(key)
        else:self.control_wakeup.set()

    def _native_updates_locked(self,updates=None,retired=None):
        """Mirror terminal metadata only; native reporting never waits for this."""
        if self.native_controller is None:return
        now=time.monotonic()
        for update in (self.native_controller.take_updates() if updates is None else updates):
            key=(update['query_request_id'],update['branch_id'])
            rec=self.records.get(key)
            if rec is None:continue
            if update['kind']=='acked':
                self._mark_acked_locked(rec)
                rec['status']=update.get('status','complete')
                rec['touched']=now
            elif update['kind']=='expired':
                pending=rec.get('registration_future')
                if pending is not None and not pending.done():
                    rec['registration_expired']=True
                    continue  # Reservation owner performs the rollback once.
                removed=self._pop_record_locked(key)
                if retired is not None and removed is not None:retired.append(removed)
                self.tombstones[key]=now
        while self.tombstones:
            first,stamp=next(iter(self.tombstones.items()))
            if len(self.tombstones)<=self.c['peer']['max_tombstones'] and now-stamp<=self.c['peer']['retention_seconds']:break
            self.tombstones.pop(first)
            if self.query_visits.get(first[0])==first:self.query_visits.pop(first[0],None)

    def _adaptive_admit(self,key):
        trace=self.records[key].get('execution_timestamps')
        if trace is not None:trace.update(dequeued_ns=time.monotonic_ns(),
            admission_thread_id=threading.get_native_id())
        try:self._submit_branch(key)
        except Exception as exc:self._admission_failed(key,exc)

    def _admit_loop(self):
        while True:
            queue_lock_enter=time.monotonic_ns()
            queue_cpu_enter=time.thread_time_ns()
            with self.admission_cv:
                queue_lock_acquired=time.monotonic_ns()
                was_empty=not self.admission_queue
                self.admission_cv.wait_for(lambda:self.admission_queue or self.stop.is_set())
                if not self.admission_queue:return
                key=self.admission_queue.popleft()
                trace=self.records[key].get('execution_timestamps')
                if trace is not None:trace.update(dequeued_ns=time.monotonic_ns(),
                    queue_depth_after_dequeue=len(self.admission_queue),
                    admission_lock_enter_ns=queue_lock_enter,
                    admission_lock_acquired_ns=queue_lock_acquired,
                    admission_lock_cpu_ns=time.thread_time_ns()-queue_cpu_enter,
                    admission_waited_empty=was_empty,
                    admission_thread_id=threading.get_native_id())
            try:self._submit_branch(key)
            except Exception as exc:self._admission_failed(key,exc)

    def _admission_failed(self,key,exc):
        with self.lock:
            rec=self.records[key]
            self._release_pending_locked(rec)
            rec['cancelled']=True
            if 'native_control' in rec:rec['native_control'].cancel()
            if 'task' in rec:
                # A resumable task whose first enqueue failed has no worker
                # that can signal done. Cancellation also fences a late start.
                rec['task'].error=exc;rec['task'].done.set()
            rec['error']=f'{type(exc).__name__}: {exc}'
            rec['status']='failed';rec['touched']=time.monotonic()
            if 'native_ticket' in rec:rec['native_ticket'].fail(rec['error'])
        self._control_changed(key)

    def _release_pending_locked(self,rec):
        with self.admission_accounting_lock:
            if rec.get('pending_start'):
                rec['pending_start']=False;self.admission_pending-=1

    def _submit_branch(self,key):
        trace=self.records[key].get('execution_timestamps')
        # Queue ownership gives exactly one submitter per accepted branch.
        # This block changes no shared counter/map: compute_submit_at belongs
        # to this branch alone and is published before executor submission.
        # Cancellation is advisory here (also checked by native execution),
        # and can happen after this check with either locking strategy.
        bypass=self.adaptive_settings.get('submit_without_state_lock',False)
        if trace is not None:
            trace['submit_state_lock_enter_ns']=time.monotonic_ns()
            trace['submit_state_lock_bypassed']=bypass
        with nullcontext() if bypass else self.lock:
            if trace is not None:trace['submit_state_lock_acquired_ns']=time.monotonic_ns()
            rec=self.records[key];now=time.monotonic()
            expired=(rec['native_control'].cancelled() if 'native_ticket' in rec else now>=min(rec['lease'],rec['deadline']))
            if self.stop.is_set() or rec['cancelled'] or expired:
                raise TimeoutError('Branch cancelled or expired before executor submission')
            rec['compute_submit_at']=now
        if self.io is None or self.c['peer']['execution_model']=='native':
            if trace is not None:
                trace['submit_thread_id']=threading.get_native_id()
                trace['submit_cpu_enter_ns']=time.thread_time_ns()
                trace['submit_enter_ns']=time.monotonic_ns()
            try:self.workers.submit_traced(trace,self._run,key)
            finally:
                # A worker may finish before submit returns. Keep these events
                # in node diagnostics, never mutate an already published report.
                if trace is not None:
                    trace['submit_return_ns']=time.monotonic_ns()
                    trace['submit_cpu_return_ns']=time.thread_time_ns()
        else:
            from .resumable import BranchTask
            task=BranchTask(self,key)
            rec['task']=task
            task.enqueue()

    def _execution_started(self,key,started):
        # Only this branch's executor owner sets its running metadata. Shared
        # pending credits remain serialized by admission_accounting_lock.
        bypass=self.adaptive_settings.get('start_without_state_lock',False)
        with nullcontext() if bypass else self.lock:
            rec=self.records[key];self._release_pending_locked(rec)
            if 'execution_timestamps' in rec:
                rec['execution_timestamps']['worker_state_lock_acquired_ns']=time.monotonic_ns()
                rec['execution_timestamps']['worker_state_lock_bypassed']=bypass
            rec['query_admission_queue_ms']=max(0.,rec['compute_submit_at']-rec['created_at'])*1000
            rec['query_executor_start_wait_ms']=max(0.,started-rec['compute_submit_at'])*1000
            rec['status']='running'
            return rec

    def lease(self,qid,token,cancel=False):
        # single_visit_parallel_local_v1 permits one retained branch per query
        # on this server. Use the existing identity index: scanning retained
        # completed queries under the shared lock makes lease cost grow with
        # run duration and amplifies congestion precisely when queries slow.
        with self.lock:
            key=self.query_visits.get(qid)
            rec=self.records.get(key) if key is not None else None
            if rec is None:return {'found':False}
            if rec['state']['token']!=token:raise ValueError('Invalid lease token')
            rec['lease']=time.monotonic()+self.c['peer']['lease_timeout_ms']/1000
            rec['cancelled'] |= cancel
            if 'native_control' in rec:
                rec['native_control'].renew(rec['lease'])
                if rec['cancelled']:rec['native_control'].cancel()
            if 'native_ticket' in rec:
                rec['native_ticket'].renew(rec['lease'])
                if rec['cancelled']:rec['native_ticket'].cancel()
            return {'found':True}

    def _run(self,key):
        worker_enter_ns=time.monotonic_ns()
        worker_started=time.monotonic()
        trace=self.records[key].get('execution_timestamps')
        if trace is not None:
            trace['worker_enter_ns']=worker_enter_ns
            trace['worker_thread_id']=threading.get_native_id()
        rec=self._execution_started(key,worker_started)
        futures=[]
        def cancelled():
            if 'native_ticket' in rec:return self.stop.is_set() or rec['native_control'].cancelled()
            return self.stop.is_set() or rec['cancelled'] or time.monotonic()>=min(rec['lease'],rec['deadline'])
        def spawn(state,descriptor):
            with self.lock: rec['children'].append(descriptor)
            if 'native_ticket' in rec:rec['native_ticket'].child(descriptor)
            self._control_changed(key)
            future=self.submit_transfer(key,state)
            futures.append(future)
            if 'native_ticket' in rec:
                def transferred(done):
                    error=''
                    try:done.result()
                    except BaseException as exc:error=f'{type(exc).__name__}: {exc}'
                    with self.lock:counts={k:v for k,v in rec.items() if k.startswith('peer_')}
                    rec['native_ticket'].transfer_done(counts,error)
                future.add_done_callback(transferred)
        try:
            if self.c['peer']['execution_model']=='native':
                from .native_search import run_native
                ticket=rec.get('native_ticket')
                if ticket is not None:
                    ticket.start(rec['query_admission_queue_ms'],rec['query_executor_start_wait_ms'],
                                 max(0.,(worker_started-rec['created_at'])*1000))
                report=run_native(rec['state'],self.shard,self.directory,self.c,spawn,cancelled,
                                  control=rec['native_control'],io=self.io,completion=ticket)
                if trace is not None:trace.update(report.get('native_timestamps',{}))
                if ticket is not None:return
                report['stats']['query_ready_queue_ms']=max(0.,(worker_started-rec['touched'])*1000)
            else:
                report=run_branch(rec['state'],self.shard,self.directory,self.c,spawn,cancelled)
                report['execution']={'model':'blocking','io_backend':'sync_pread','fallback_reason':None}
            for name in ('query_admission_queue_ms','query_executor_start_wait_ms'):
                report['stats'][name]=rec[name]
            transfer_started=time.perf_counter()
            if trace is not None:trace['local_search_done_ns']=time.monotonic_ns()
            if self.optimized:
                if self.control_scheduler is not None:
                    # Transfer ownership only. No runtime lock, finalization
                    # or Future callback registration on the compute thread.
                    self.control_scheduler.local_done(key,report,futures,transfer_started)
                    return
                with self.lock:
                    rec['pending_report']=report;rec['transfer_futures']=futures
                    rec['finalize_started']=transfer_started;rec['status']='waiting_transfer'
                self._finalize_ready(key)
                for future in futures:future.add_done_callback(lambda _,key=key:self._finalize_ready(key))
                return
            for future in futures: future.result()
            if self.c['peer']['execution_model']=='native':
                stats=report['stats']
                stats['query_transfer_finalize_wait_ms']=(time.perf_counter()-transfer_started)*1000
                stats['query_branch_wall_ms']=(time.monotonic()-rec['touched'])*1000
                stats['query_state_other_ms']=max(0.,stats['query_branch_wall_ms']-sum(stats[k] for k in
                    ('query_ready_queue_ms','query_compute_slice_wall_ms','query_io_wait_ms','query_transfer_finalize_wait_ms')))
            with self.lock:
                report['peer_transfer']={k:v for k,v in rec.items() if k.startswith('peer_')}
                rec['report']=report;rec['status']='complete';rec['touched']=time.monotonic()
            self._control_changed(key)
        except Exception as exc:
            with self.lock:
                rec['error']=f'{type(exc).__name__}: {exc}';rec['status']='failed';rec['touched']=time.monotonic()
            if 'native_ticket' in rec:rec['native_ticket'].fail(rec['error'])
            self._control_changed(key)

    def _finalize_ready(self,key):
        # Futures are inspected only after done(); this never waits on network I/O.
        finalize_enter_ns=time.monotonic_ns()
        with self.lock:
            finalize_acquired_ns=time.monotonic_ns()
            rec=self.records[key]
            if rec.get('pending_report') is None or not all(f.done() for f in rec['transfer_futures']):return
            if 'execution_timestamps' in rec:
                rec['execution_timestamps'].update(finalize_enter_ns=finalize_enter_ns,
                                                   finalize_acquired_ns=finalize_acquired_ns)
            report=rec.pop('pending_report');futures=rec.pop('transfer_futures')
            try:
                for future in futures:future.result()
                self._publish_report_locked(rec,report,rec['finalize_started'])
            except Exception as exc:
                rec['error']=f'{type(exc).__name__}: {exc}';rec['status']='failed'
            rec['touched']=time.monotonic()
        self._control_changed(key)

    def _publish_report_locked(self,rec,report,started):
        # Caller owns state lock. No locks, Futures, callbacks, queues or socket
        # operations here: both event completion paths share identical checks.
        try:
            if rec['cancelled'] or self.stop.is_set() or time.monotonic()>=min(rec['lease'],rec['deadline']):
                raise TimeoutError('Branch cancelled or expired before publication')
            st=report['stats']
            st['query_transfer_finalize_wait_ms']=(time.perf_counter()-started)*1000
            st['query_branch_wall_ms']=(time.monotonic()-rec['created_at'])*1000
            st['query_state_other_ms']=max(0.,st['query_branch_wall_ms']-sum(st.get(k,0.) for k in
                ('query_ready_queue_ms','query_compute_slice_wall_ms','query_io_wait_ms','query_transfer_finalize_wait_ms')))
            report['peer_transfer']={k:v for k,v in rec.items() if k.startswith('peer_')}
            rec['report']=report;rec['status']='complete'
            if 'execution_timestamps' in rec:rec['execution_timestamps']['report_ready_ns']=time.monotonic_ns()
        except Exception as exc:
            rec['error']=f'{type(exc).__name__}: {exc}';rec['status']='failed'
        rec['touched']=time.monotonic()

    def try_publish_no_transfer(self,key,report,started):
        attempt=time.monotonic_ns()
        if not self.lock.acquire(blocking=False):return False
        trace=None
        try:
            acquired=time.monotonic_ns();rec=self.records.get(key)
            if rec is None or rec['status'] in ('complete','failed'):return True
            if rec.get('pending_report') is not None:
                raise RuntimeError('Local report already owned by completion consumer')
            trace=rec.get('execution_timestamps')
            if trace is not None:
                trace.update(completion_publish_path='try_lock',
                    completion_fast_path_attempt_ns=attempt,completion_fast_path_acquired_ns=acquired,
                    finalize_enter_ns=attempt,finalize_acquired_ns=acquired)
            self._publish_report_locked(rec,report,started)
            return True
        finally:
            self.lock.release()
            if trace is not None:trace['completion_fast_path_return_ns']=time.monotonic_ns()

    def submit_transfer(self,key,state):
        trace=self.records[key].get('execution_timestamps')
        if trace is not None:trace['transfer_enqueue_enter_ns']=time.monotonic_ns()
        try:return self.transfers.submit(self._transfer,key,state,time.monotonic())
        finally:
            if trace is not None:trace['transfer_enqueue_return_ns']=time.monotonic_ns()

    def _transfer(self,key,state,queued_at=None):
        rec=self.records[key];started=time.monotonic()
        trace=rec.get('execution_timestamps')
        if trace is not None:
            trace.update(transfer_worker_enter_ns=time.monotonic_ns(),transfer_thread_id=threading.get_native_id())
        with self.lock:
            rec['peer_transfer_queue_ms']=rec.get('peer_transfer_queue_ms',0.)+max(0.,started-(queued_at or started))*1000
        node=self.c['cluster']['nodes'][state['target_node']]
        try:
            for attempt in range(self.c['network']['max_retries']+1):
                if rec['cancelled'] or self.stop.is_set():raise TimeoutError('Branch transfer cancelled')
                with self.lock:rec['peer_rpc_count']+=1
                try:
                    endpoint=[node['host'],node['port']];message={'version':1,'op':'peer_submit','state':state}
                    pool=getattr(self,'rpc_pool',None)
                    if pool is None:_,counts=exchange(endpoint,message,self.c['network']['max_message_bytes'],self.c['peer']['handoff_timeout_seconds'])
                    else:
                        from .peer_rpc import rpc_exchange
                        _,counts=rpc_exchange(endpoint,message,self.c['network']['max_message_bytes'],self.c['peer']['handoff_timeout_seconds'],pool)
                    with self.lock:
                        rec['peer_request_bytes']+=counts['request_bytes'];rec['peer_response_bytes']+=counts['response_bytes']
                        for k in ('pool_wait_ms','connect_ms','send_ms','response_wait_ms','connections_created','connections_reused'):
                            rec['peer_'+k]=rec.get('peer_'+k,0)+counts.get(k,0)
                    return
                except (OSError,RuntimeError,ValueError):
                    with self.lock:rec['peer_transfer_errors']+=1
                    if attempt==self.c['network']['max_retries']:raise
        finally:
            if trace is not None:trace['transfer_worker_return_ns']=time.monotonic_ns()
            with self.lock:rec['peer_transfer_wall_ms']=rec.get('peer_transfer_wall_ms',0.)+(time.monotonic()-started)*1000

    def _control(self):
        if self.optimized:
            if self.control_scheduler is not None:
                self.control_scheduler.run();return
            self._concurrent_control();return
        interval=self.c['peer']['heartbeat_interval_ms']/1000
        while not self.stop.is_set():
            self.control_wakeup.wait(interval)
            self.control_wakeup.clear()
            now=time.monotonic()
            with self.lock:
                records=list(self.records.items())
                for key,rec in records:
                    if rec['status'] in ('complete','failed') and now-rec['touched']>self.c['peer']['retention_seconds']:
                        self._pop_record_locked(key);self.tombstones[key]=now
                for key,stamp in list(self.tombstones.items()):
                    if now-stamp>self.c['peer']['retention_seconds']:
                        self.tombstones.pop(key,None)
                        if self.query_visits.get(key[0])==key:self.query_visits.pop(key[0],None)
                while len(self.tombstones)>self.c['peer']['max_tombstones']:
                    key=next(iter(self.tombstones));self.tombstones.pop(key)
                    if self.query_visits.get(key[0])==key:self.query_visits.pop(key[0],None)
                # Expired records have already released their admission credit.
                # Never send/ACK stale entries from the pre-cleanup snapshot.
                records=list(self.records.items())
            for key,rec in records:
                if rec['acked'] or self.stop.is_set(): continue
                state=rec['state']
                with self.lock:
                    involved={self.shard.meta['shard_id'],state['entry_node']}
                    involved.update(child['node_id'] for child in rec['children'])
                    involved.update(item['node_id'] for item in state.get('ancestry',[]))
                    message={'query_request_id':state['query_request_id'],'token':state['token'],
                        'node_id':self.shard.meta['shard_id'],'branch_id':state['branch_id'],
                        'status':rec['status'],'involved_nodes':sorted(involved),
                        'reports':[rec['report']] if rec['report'] else [],'error':rec['error']}
                try:
                    reply,_=exchange(state['head_endpoint'],message,self.c['network']['max_message_bytes'],
                                     self.c['peer']['control_timeout_seconds'])
                    with self.lock:
                        rec['lease']=time.monotonic()+reply['lease_ms']/1000
                        rec['cancelled'] |= reply['cancelled']
                        if 'native_control' in rec:
                            rec['native_control'].renew(rec['lease'])
                            if rec['cancelled']:rec['native_control'].cancel()
                        if message['reports'] or message['error']:self._mark_acked_locked(rec)
                except (OSError,RuntimeError,ValueError):
                    pass # lease/deadline bounds compute; final delivery retries until retention expires

    def _concurrent_control(self):
        interval=self.c['peer']['heartbeat_interval_ms']/1000
        capacity=self.c['peer']['communication']['max_pending_control']
        while not self.stop.is_set():
            self.control_wakeup.wait(interval);self.control_wakeup.clear()
            if self.stop.is_set():break
            now=time.monotonic();pending=[]
            scheduling_cpu=time.thread_time_ns();lock_requested=time.monotonic_ns()
            with self.lock:
                lock_acquired=time.monotonic_ns()
                scanned=len(self.records)
                no_capacity=self.control_active>=capacity
                for key,rec in list(self.records.items()):
                    expiry=rec['touched']+self.c['peer']['retention_seconds']
                    if not rec['acked']:expiry=max(expiry,rec['deadline'])
                    if rec['status'] in ('complete','failed') and not rec['control_inflight'] and now>expiry:
                        self._pop_record_locked(key);self.tombstones[key]=now
                for key,stamp in list(self.tombstones.items()):
                    if now-stamp>self.c['peer']['retention_seconds']:
                        self.tombstones.pop(key,None)
                        if self.query_visits.get(key[0])==key:self.query_visits.pop(key[0],None)
                while len(self.tombstones)>self.c['peer']['max_tombstones']:
                    key=next(iter(self.tombstones));self.tombstones.pop(key)
                    if self.query_visits.get(key[0])==key:self.query_visits.pop(key[0],None)
                for key,rec in self.records.items():
                    if rec['acked'] or rec['control_inflight']:continue
                    signature=(tuple(ch['branch_id'] for ch in rec['children']),rec['report'] is not None,rec['error'])
                    if now>=rec['control_next_at'] or signature!=rec['control_signature']:
                        pending.append((not (rec['report'] or rec['error']),rec['control_next_at'],key,signature))
                pending.sort(key=lambda row:(row[0],row[1]))
                selected=pending[:max(0,capacity-self.control_active)]
                for _,_,key,signature in selected:
                    rec=self.records[key];rec['control_inflight']=True;rec['control_signature']=signature
                    self.control_active+=1
                totals=self.control_scheduler_totals
                totals['iterations']+=1;totals['records_scanned']+=scanned;totals['selected']+=len(selected)
                totals['no_capacity_iterations']+=int(no_capacity)
                totals['lock_wait_ms']+=(lock_acquired-lock_requested)/1e6
                totals['lock_held_ms']+=(time.monotonic_ns()-lock_acquired)/1e6
                totals['cpu_ms']+=(time.thread_time_ns()-scheduling_cpu)/1e6
            for _,_,key,signature in selected:
                # Submission never creates a graph branch; it only reports existing state.
                self.control_workers.submit(self._send_control,key,time.monotonic())

    def _send_control(self,key,queued_at):
        started=time.monotonic();counts={};succeeded=False
        event={'queued_ns':int(queued_at*1e9),'sender_enter_ns':time.monotonic_ns(),
               'sender_thread_id':threading.get_native_id()}
        with self.lock:
            rec=self.records[key];state=rec['state']
            involved={self.shard.meta['shard_id'],state['entry_node']}
            involved.update(ch['node_id'] for ch in rec['children'])
            involved.update(a['node_id'] for a in state.get('ancestry',[]))
            message={'query_request_id':state['query_request_id'],'token':state['token'],
                'node_id':self.shard.meta['shard_id'],'branch_id':state['branch_id'],
                'status':rec['status'],'involved_nodes':sorted(involved),
                'reports':[rec['report']] if rec['report'] else [],'error':rec['error']}
            event.update(kind='result' if message['reports'] or message['error'] else 'heartbeat',
                         message_ready_ns=time.monotonic_ns())
            if 'control_events' in rec:rec['control_events'].append(event)
        try:
            if self.stop.is_set():return
            if self.rpc_pool is None:
                reply,counts=exchange(state['head_endpoint'],message,self.c['network']['max_message_bytes'],self.c['peer']['control_timeout_seconds'])
            else:
                from .peer_rpc import rpc_exchange
                reply,counts=rpc_exchange(state['head_endpoint'],message,self.c['network']['max_message_bytes'],self.c['peer']['control_timeout_seconds'],self.rpc_pool)
            with self.lock:
                rec['lease']=time.monotonic()+reply['lease_ms']/1000
                rec['cancelled']|=reply['cancelled']
                if 'native_control' in rec:
                    rec['native_control'].renew(rec['lease'])
                    if rec['cancelled']:rec['native_control'].cancel()
                if message['reports'] or message['error']:self._mark_acked_locked(rec)
            succeeded=True
        except (OSError,RuntimeError,ValueError) as exc:
            counts=getattr(exc,'rpc_metrics',{})
        finally:
            event.update(sender_rpc_return_ns=time.monotonic_ns(),rpc_counts=dict(counts),succeeded=succeeded)
            with self.lock:
                rec['control_inflight']=False
                rec['control_next_at']=time.monotonic()+self.c['peer']['heartbeat_interval_ms']/1000
                self.control_active-=1
                self.control_totals['sent']+=1;self.control_totals['errors']+=int(not succeeded)
                self.control_totals['queue_ms']+=(started-queued_at)*1000
                self.control_totals['wall_ms']+=(time.monotonic()-started)*1000
                for k in ('connections_created','connections_reused'):self.control_totals[k]+=counts.get(k,0)
            self._control_changed(key)

    def close(self):
        with self.lock:
            # Serialize stop with accept's publication. Otherwise an independent
            # queue consumer can see stop+empty and exit before an accept that
            # already passed its stop check appends its reserved task.
            self.stop.set()
            self.control_wakeup.set()
            with self.admission_cv if self.independent_admission else nullcontext():
                self.admission_cv.notify_all()
            for rec in self.records.values():
                if 'native_control' in rec:rec['native_control'].cancel()
            registrations=[r['registration_future'] for r in self.records.values() if 'registration_future' in r]
        for pending in registrations:
            try:pending.result()
            except BaseException:pass
        for thread in self.admission_threads:thread.join()
        if self.admission_pool is not None:self.admission_pool.shutdown(wait=True)
        if self.io is not None:
            self.io.close()
            with self.lock:tasks=[r['task'] for r in self.records.values() if 'task' in r]
            for task in tasks:task.done.wait()
        self.workers.shutdown(wait=True,cancel_futures=False)
        self.transfers.shutdown(wait=True,cancel_futures=True)
        if self.native_controller is None:self.control_thread.join()
        else:self.native_controller.close()
        if self.control_scheduler is not None:self.control_scheduler.finish_stopped()
        if self.control_workers is not None:self.control_workers.shutdown(wait=True,cancel_futures=False)
        if self.rpc_pool is not None:self.rpc_pool.close()

    def metrics(self):
        with self.lock:
            self._native_updates_locked()
            native_snapshot=None
            if self.native_controller is not None:
                native_snapshot=self.native_controller.snapshot()
                self.control_active=native_snapshot['control_active']
                self.control_totals.update(native_snapshot['control_totals'])
                self.control_scheduler_totals.update(native_snapshot['control_scheduler_totals'])
                for item in native_snapshot['branches']:
                    key=(item['query_request_id'],item['branch_id'])
                    rec=self.records.get(key)
                    if rec is None:continue
                    for name in ('status','lease','cancelled'):
                        if name in item:rec[name]=item[name]
                    if 'execution_timestamps' in rec:
                        rec['execution_timestamps'].update(item.get('execution_timestamps',{}))
                        rec['control_events']=item.get('control_events',[])
            with self.admission_cv if self.independent_admission else nullcontext():
                admission_depth=len(self.admission_queue)
            adaptive_admission=self.admission_pool.metrics() if self.admission_pool is not None else None
            if adaptive_admission is not None:admission_depth=adaptive_admission['queued']
            return {'execution_model':self.c['peer']['execution_model'],
                    'compute_mode':self.compute_mode,'compute_startup':dict(self.compute_startup),
                    'compute_queue':self.workers.metrics() if self.compute_mode=='fixed' else None,
                    'execution_timestamps':[
                        {'query_request_id':key[0],'branch_id':key[1],
                         'node_id':self.shard.meta['shard_id'],**dict(rec['execution_timestamps'])}
                        for key,rec in self.records.items() if 'execution_timestamps' in rec],
                    'control_events':[
                        {'query_request_id':key[0],'branch_id':key[1],
                         'node_id':self.shard.meta['shard_id'],**dict(event)}
                        for key,rec in self.records.items() for event in rec.get('control_events',[])],
                    'sender_mode':'queued' if self.queued_senders else 'executor',
                    'send_queues':({'transfer':self.transfers.metrics(),
                                   **({'control':self.control_workers.metrics()} if self.control_workers is not None else {})}
                                   if self.queued_senders else None),
                    'admission_mode':self.c['peer']['admission']['mode'],
                    'admission_dispatch_threads':adaptive_admission['workers'] if adaptive_admission else len(self.admission_threads),
                    'adaptive_admission':adaptive_admission,
                    'adaptive_connections':self.rpc_pool.metrics() if hasattr(self.rpc_pool,'metrics') else None,
                    'admission_queue_lock':self.c['peer']['admission']['queue_lock'],
                    'admission_queue_depth':admission_depth,
                    'admission_pending_start':self.admission_pending,
                    'admission_peak_pending_start':self.admission_peak_pending,
                    'admission_capacity_rejections':self.admission_rejected,
                    'communication_mode':self.c['peer']['communication']['mode'],
                    'control_active':self.control_active,'control_totals':dict(self.control_totals),
                    'control_scheduler_totals':dict(self.control_scheduler_totals),
                    'control_scheduler_mode':self.c['peer']['control']['scheduler'],
                    'native_control':({k:v for k,v in native_snapshot.items() if k!='branches'} if native_snapshot else None),
                    'io':self.io.metrics() if self.io is not None else {'actual_backend':'sync_pread'},
                    'waiting_io':sum(r['status']=='waiting_io' for r in self.records.values()),
                    'waiting_transfer':sum(r['status']=='waiting_transfer' for r in self.records.values()),
                    'retained_branches':len(self.records),'tombstones':len(self.tombstones),
                    'queued':sum(r['status']=='queued' for r in self.records.values()),
                    'running':sum(r['status']=='running' for r in self.records.values()),
                    'unacknowledged':self.active_count,
                    'failed_retained':sum(r['status']=='failed' for r in self.records.values())}
