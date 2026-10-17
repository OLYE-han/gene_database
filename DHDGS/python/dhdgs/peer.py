"""Peer branches plus a control-only head registry. Trusted-cluster protocol."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
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
from .transport import send, receive


def execution_signature(c):
    fields={k:c[k] for k in ('search','scheduler','quantization','data')}
    fields['peer_protocol']='single_visit_parallel_local_v1'
    fields['peer']={k:v for k,v in c['peer'].items() if not k.startswith('head_')}
    return hashlib.sha256(json.dumps(fields,sort_keys=True).encode()).hexdigest()


def exchange(endpoint, message, limit, timeout):
    started=time.perf_counter()
    with socket.create_connection(tuple(endpoint),timeout) as sock:
        tx=send(sock,message,limit)
        response,rx=receive(sock,limit)
    if not response.get('ok'): raise RuntimeError(response.get('error','Peer request failed'))
    return response,{'request_bytes':tx,'response_bytes':rx,'rpc_count':1,
                     'wall_ms':(time.perf_counter()-started)*1000}


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
    def __init__(self,c,qid,token,root_id,root_node):
        self.c=c
        self.registry=BranchRegistry(qid,token,root_id,root_node,c['search']['max_expansions'],
                                     c['peer']['max_branches']-1,len(c['cluster']['nodes']))
        self.done=threading.Event()
        self.closed=False
        self.control={'rpc_count':0,'request_bytes':0,'response_bytes':0,'errors':0}
        self.control_by_kind={kind:{'rpc_count':0,'request_bytes':0,'response_bytes':0}
                              for kind in ('heartbeat','result','lease','cancel')}
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
        self.server=Server((c['peer']['head_bind_host'],c['peer']['head_port']),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.005},daemon=True)
        self.thread.start()
        self.endpoint=[c['peer']['head_advertise_host'],self.server.server_address[1]]

    def record_control(self,kind,counts):
        with self.registry.lock:
            for key in ('rpc_count','request_bytes','response_bytes'):
                self.control[key]+=counts[key]
                self.control_by_kind[kind][key]+=counts[key]

    def wait(self, deadline):
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
                    except (OSError,RuntimeError,ValueError):
                        with self.registry.lock: self.control['errors']+=1
                last=now
            self.done.wait(min(.05,max(0,deadline-time.monotonic())))
            self.done.clear()

    def close(self):
        self.closed=True
        with self.registry.lock: nodes=list(self.registry.nodes)
        for sid in nodes:
            node=self.c['cluster']['nodes'][sid]
            try:
                _,counts=exchange([node['host'],node['port']],{'version':1,'op':'peer_cancel',
                    'query_request_id':self.registry.query_id,'token':self.registry.token},
                    self.c['network']['max_message_bytes'],self.c['peer']['control_timeout_seconds'])
                self.record_control('cancel',counts)
            except (OSError,ValueError,RuntimeError):
                with self.registry.lock: self.control['errors']+=1
        self.server.shutdown(); self.server.server_close(); self.thread.join()


class PeerRuntime:
    def __init__(self,c,shard):
        self.c,self.shard=c,shard
        bank=shard.directory/'code_membership.npy'
        if bank.exists():
            membership=load_complete_npy(bank)
            ids=shard.code_ids
        else:
            # Compatibility for pilot indexes; multi-host deployments must copy the new sidecar.
            root=Path(c['paths']['output'])/'index'
            ids=load_complete_npy(root/'ids.npy');membership=load_complete_npy(root/'membership.npy')
        if membership.shape!=(len(ids),len(c['cluster']['nodes'])): raise ValueError('Invalid peer owner directory')
        self.directory={int(gid):list(map(int,np.flatnonzero(membership[i]))) for i,gid in enumerate(ids)}
        self.lock=threading.Lock()
        self.records={}
        self.tombstones={}
        self.query_visits={}
        self.stop=threading.Event()
        self.control_wakeup=threading.Event()
        self.io=None
        if c['peer']['execution_model']=='resumable':
            from .async_io import LocalIO
            if 2*c['search']['local_beam']*shard.meta['record_bytes']>c['storage']['io']['buffer_pool_bytes_per_device']:
                raise ValueError('Buffer budget must hold one complete local beam (including copy)')
            self.io=LocalIO(shard,c['storage']['io'],trace=c['metrics']['detailed_trace'])
        self.workers=ThreadPoolExecutor(max_workers=c['peer']['worker_threads'])
        self.transfers=ThreadPoolExecutor(max_workers=c['search']['max_inflight'])
        self.control_thread=threading.Thread(target=self._control,daemon=True)
        self.control_thread.start()

    def accept(self,state):
        if state['epoch']!=self.shard.meta['epoch'] or state['signature']!=execution_signature(self.c):
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
        with self.lock:
            if self.stop.is_set():raise RuntimeError('Peer runtime closed')
            if key in self.records:
                if self.records[key]['digest']!=digest: raise ValueError('Conflicting branch retry')
                return {'accepted':True,'duplicate':True}
            if key in self.tombstones: raise ValueError('Expired branch cannot be replayed')
            if state['query_request_id'] in self.query_visits:
                raise ValueError('Query already visited this data node')
            active=sum(not r['acked'] for r in self.records.values())
            if active>=self.c['peer']['max_active_queries']: raise RuntimeError('Peer branch capacity exceeded')
            if len(self.records)>=self.c['peer']['max_tombstones']+self.c['peer']['max_active_queries']:
                raise RuntimeError('Peer dedup retention capacity exceeded')
            now=time.monotonic()
            rec={'state':state,'digest':digest,'status':'queued','lease':now+self.c['peer']['lease_timeout_ms']/1000,
                 'deadline':now+self.c['peer']['query_timeout_seconds'],'cancelled':False,'children':[],
                 'report':None,'error':None,'acked':False,'touched':now,'peer_rpc_count':0,'peer_request_bytes':0,
                 'peer_response_bytes':0,'peer_transfer_errors':0}
            self.records[key]=rec
            self.query_visits[state['query_request_id']]=key
        if self.io is None:
            self.workers.submit(self._run,key)
        else:
            from .resumable import BranchTask
            task=BranchTask(self,key)
            rec['task']=task
            task.enqueue()
        self.control_wakeup.set()
        return {'accepted':True,'duplicate':False}

    def lease(self,qid,token,cancel=False):
        with self.lock:
            found=False
            for (query,_),rec in self.records.items():
                if query==qid:
                    if rec['state']['token']!=token: raise ValueError('Invalid lease token')
                    rec['lease']=time.monotonic()+self.c['peer']['lease_timeout_ms']/1000
                    rec['cancelled'] |= cancel
                    found=True
            return {'found':found}

    def _run(self,key):
        with self.lock:
            rec=self.records[key];rec['status']='running'
        futures=[]
        def cancelled():
            return self.stop.is_set() or rec['cancelled'] or time.monotonic()>=min(rec['lease'],rec['deadline'])
        def spawn(state,descriptor):
            with self.lock: rec['children'].append(descriptor)
            self.control_wakeup.set()
            futures.append(self.transfers.submit(self._transfer,key,state))
        try:
            report=run_branch(rec['state'],self.shard,self.directory,self.c,spawn,cancelled)
            report['execution']={'model':'blocking','io_backend':'sync_pread','fallback_reason':None}
            for future in futures: future.result()
            with self.lock:
                report['peer_transfer']={k:rec[k] for k in ('peer_rpc_count','peer_request_bytes','peer_response_bytes','peer_transfer_errors')}
                rec['report']=report;rec['status']='complete';rec['touched']=time.monotonic()
            self.control_wakeup.set()
        except Exception as exc:
            with self.lock:
                rec['error']=f'{type(exc).__name__}: {exc}';rec['status']='failed';rec['touched']=time.monotonic()
            self.control_wakeup.set()

    def _transfer(self,key,state):
        rec=self.records[key]
        node=self.c['cluster']['nodes'][state['target_node']]
        for attempt in range(self.c['network']['max_retries']+1):
            if rec['cancelled'] or self.stop.is_set(): raise TimeoutError('Branch transfer cancelled')
            with self.lock: rec['peer_rpc_count']+=1
            try:
                _,counts=exchange([node['host'],node['port']],{'version':1,'op':'peer_submit','state':state},
                    self.c['network']['max_message_bytes'],self.c['peer']['handoff_timeout_seconds'])
                with self.lock:
                    rec['peer_request_bytes']+=counts['request_bytes'];rec['peer_response_bytes']+=counts['response_bytes']
                return
            except (OSError,RuntimeError,ValueError):
                with self.lock: rec['peer_transfer_errors']+=1
                if attempt==self.c['network']['max_retries']: raise

    def _control(self):
        interval=self.c['peer']['heartbeat_interval_ms']/1000
        while not self.stop.is_set():
            self.control_wakeup.wait(interval)
            self.control_wakeup.clear()
            now=time.monotonic()
            with self.lock:
                records=list(self.records.items())
                for key,rec in records:
                    if rec['status'] in ('complete','failed') and now-rec['touched']>self.c['peer']['retention_seconds']:
                        self.records.pop(key,None);self.tombstones[key]=now
                for key,stamp in list(self.tombstones.items()):
                    if now-stamp>self.c['peer']['retention_seconds']:
                        self.tombstones.pop(key,None)
                        if self.query_visits.get(key[0])==key:self.query_visits.pop(key[0],None)
                while len(self.tombstones)>self.c['peer']['max_tombstones']:
                    key=next(iter(self.tombstones));self.tombstones.pop(key)
                    if self.query_visits.get(key[0])==key:self.query_visits.pop(key[0],None)
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
                        if message['reports'] or message['error']:rec['acked']=True
                except (OSError,RuntimeError,ValueError):
                    pass # lease/deadline bounds compute; final delivery retries until retention expires

    def close(self):
        self.stop.set()
        self.control_wakeup.set()
        if self.io is not None:
            self.io.close()
            with self.lock:tasks=[r['task'] for r in self.records.values() if 'task' in r]
            for task in tasks:task.done.wait()
        self.workers.shutdown(wait=True,cancel_futures=False)
        self.transfers.shutdown(wait=True,cancel_futures=True)
        self.control_thread.join()

    def metrics(self):
        with self.lock:
            return {'execution_model':self.c['peer']['execution_model'],
                    'io':self.io.metrics() if self.io is not None else {'actual_backend':'sync_pread'},
                    'waiting_io':sum(r['status']=='waiting_io' for r in self.records.values()),
                    'waiting_transfer':sum(r['status']=='waiting_transfer' for r in self.records.values()),
                    'retained_branches':len(self.records),'tombstones':len(self.tombstones),
                    'queued':sum(r['status']=='queued' for r in self.records.values()),
                    'running':sum(r['status']=='running' for r in self.records.values()),
                    'unacknowledged':sum(not r['acked'] for r in self.records.values()),
                    'failed_retained':sum(r['status']=='failed' for r in self.records.values())}
