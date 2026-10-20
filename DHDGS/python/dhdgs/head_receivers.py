"""Query-affine callback processes. No search frontier crosses this IPC channel.

The receiver ACKs and validates callbacks locally. The HNSW process receives one
terminal report bundle per query, plus small node-membership notifications.
Runtime IPC uses msgpack; multiprocessing spawn is used only to start trusted code.
"""
import copy
from concurrent.futures import Future, InvalidStateError
import heapq
import multiprocessing as mp
import os
import queue
import threading
import time

import msgpack

from .head_hub import HeadHub
from .metrics import CLOCK_DOMAIN
from .peer_rpc import ConnectionPool, FairConnectionPool, rpc_exchange


def _resolve(future,value=None,error=None):
    # Cancellation can race with a receiver completion even while the query's
    # delivery lock is held. A cancelled continuation must not kill its reader.
    try:
        if error is None:future.set_result(value)
        else:future.set_exception(error)
    except InvalidStateError:
        if not future.done():raise


def _receiver_main(c, conn, terminal_conn=None, registration=None):
    from .peer import HeadSession
    # Each spawned process has its own GIL. Do not fork an active head.
    import sys
    sys.setswitchinterval(c['runtime']['python_switch_interval_ms']/1000)
    limit=c['network']['max_message_bytes']
    output_lock=threading.Lock()
    terminal_lock=threading.Lock()
    registration_gate=threading.Lock()
    def emit(message):
        data=msgpack.packb(message,use_bin_type=True)
        if len(data)>limit:raise ValueError('Receiver IPC message exceeds max_message_bytes')
        target=terminal_conn if terminal_conn is not None and message['kind']=='terminal' else conn
        with terminal_lock if target is terminal_conn else output_lock:target.send_bytes(data)

    class ReceiverHub(HeadHub):
        def deliver(self,sock,message,rx):
            if registration is not None:
                qid=message.get('query_request_id')
                with registration_gate:
                    with self.lock:known=qid in self.sessions or qid in self.closed
                    if not known:
                        identity=registration.get(qid)
                        if identity is None:raise ValueError('Unknown or expired shared report identity')
                        if identity['token']!=message.get('token'):raise ValueError('Invalid query control token')
                        lazy=HeadSession(c,qid,identity['token'],identity['root_id'],identity['root_node'],self)
                        lazy.registry.budget=identity['budget'];lazy.registry.slots=identity['slots']
                        lazy.receiver_metrics=dict(registered_ns=time.monotonic_ns(),
                            shared_identity_published_ns=identity['published_ns'])
                        lazy.done=CompletionSignal(lazy)
            with self.lock:session=self.sessions.get(message.get('query_request_id'))
            if session is not None:
                with session.delivery_lock:
                    if not session.closed:
                        stamp=time.monotonic_ns()
                        session.receiver_metrics.setdefault('first_callback_dispatch_ns',stamp)
                        session.receiver_metrics['last_callback_dispatch_ns']=stamp
                        session.receiver_metrics['callback_count']=session.receiver_metrics.get('callback_count',0)+1
                    return super().deliver(sock,message,rx)
            return super().deliver(sock,message,rx)

    class CompletionSignal:
        def __init__(self,session):self.session=session;self.nodes=set();self.sent=False
        def set(self):
            s=self.session
            if self.sent:return
            nodes=sorted(s.registry.nodes)
            error=None
            try:complete=s.registry.complete()
            except (ValueError,RuntimeError,KeyError,TypeError) as exc:
                complete=True;error=f'{type(exc).__name__}: {exc}'
            if not complete:
                if set(nodes)!=self.nodes:
                    self.nodes=set(nodes)
                    emit(dict(kind='nodes',qid=s.registry.query_id,nodes=nodes))
                return
            # Terminal ownership changes to a bounded tombstone BEFORE publishing.
            # ACK-loss retries are answered here, never merged a second time.
            self.sent=True;s.closed=True;hub.unregister(s,error is None)
            s.receiver_metrics.update(terminal_publish_ns=time.monotonic_ns(),
                                      receiver_pid=os.getpid(),clock_domain=CLOCK_DOMAIN)
            message=dict(kind='terminal',qid=s.registry.query_id,nodes=nodes,
                      reports=list(s.registry.reports.values()),error=error,
                      control=s.control,control_by_kind=s.control_by_kind,
                      control_error_details=s.control_error_details,
                      metrics=s.receiver_metrics)
            try:emit(message)
            except ValueError:
                # Each report can fit the network limit while their union does
                # not. Fail explicitly rather than leaving a silent timeout.
                message.update(reports=[],error='Terminal report bundle exceeds IPC message limit')
                emit(message)

    def dispatch(s,command):
        enter=time.monotonic_ns()
        try:
            with s.delivery_lock:
                if s.closed:raise RuntimeError('Query cancelled before initial dispatch')
                if time.monotonic()>=command['deadline']:raise TimeoutError('Query expired before initial dispatch')
            state=command['state'];state['head_endpoint']=hub.endpoint
            reply,counts=hub.exchange(command['endpoint'],{'version':1,'op':'peer_submit','state':state},
                                      limit,c['peer']['handoff_timeout_seconds'])
            emit(dict(kind='entry',qid=s.registry.query_id,reply=reply,counts=counts,
                      enter_ns=enter,return_ns=time.monotonic_ns()))
        except (OSError,RuntimeError,ValueError,KeyError) as exc:
            emit(dict(kind='entry',qid=s.registry.query_id,error=f'{type(exc).__name__}: {exc}',
                      enter_ns=enter,return_ns=time.monotonic_ns()))

    hub=None;entries=None
    try:
        hub=ReceiverHub(c,'receiver')
        if c['peer'].get('head_query_workers',0):
            from .sender import QueuedSender
            entries=QueuedSender(c['peer'].get('head_entry_workers',4),c['peer']['max_active_queries'],
                                 'dhdgs-head-entry')
        emit(dict(kind='ready',endpoint=hub.endpoint,pid=os.getpid(),clock_domain=CLOCK_DOMAIN))
        while True:
            command=msgpack.unpackb(conn.recv_bytes(limit),raw=False)
            received_ns=time.monotonic_ns()
            kind=command['kind']
            if kind=='stop':break
            qid=command['qid']
            if kind in ('register','start_query'):
                try:
                    s=HeadSession(c,qid,command['token'],command['root_id'],command['root_node'],hub)
                    # Live search settings can change without restarting the
                    # head/receivers. Credit belongs to this query, not startup.
                    s.registry.budget=command['budget'];s.registry.slots=command['slots']
                    s.receiver_metrics={'registered_ns':time.monotonic_ns(),
                        'register_receiver_received_ns':received_ns,
                        'register_parent_enqueued_ns':command['enqueued_ns'],
                        'register_parent_dequeued_ns':command['dequeued_ns']}
                    s.done=CompletionSignal(s)
                    if kind=='start_query':
                        if entries is None:raise RuntimeError('Receiver entry workers disabled')
                        s.receiver_metrics.update(registration_ready_ms=(s.receiver_metrics['registered_ns']-
                            command['register_enter_ns'])/1e6,entry_enqueued_ns=time.monotonic_ns())
                        entries.submit(dispatch,s,command)
                    else:
                        emit(dict(kind='registered',qid=qid,metrics={**s.receiver_metrics,
                            'register_ack_publish_ns':time.monotonic_ns()}))
                except (ValueError,RuntimeError) as exc:
                    emit(dict(kind='entry' if kind=='start_query' else 'registered',qid=qid,error=str(exc)))
            elif kind=='unregister':
                with registration_gate:
                    with hub.lock:s=hub.sessions.get(qid)
                    if s is not None:
                        with s.delivery_lock:
                            if not s.closed:s.closed=True;hub.unregister(s,False)
            else:raise ValueError('Invalid receiver command')
    except (EOFError,BrokenPipeError,OSError):pass
    finally:
        if entries is not None:entries.shutdown(wait=False,cancel_futures=True)
        if hub is not None:hub.close()
        conn.close()
        if terminal_conn is not None:terminal_conn.close()


class ProcessHeadHub:
    """Parent-side endpoint selector and entry connection pool.

    Only the reader delivers terminal state. A bounded command writer queue keeps
    session teardown from blocking while it holds the per-query delivery lock.
    """
    def __init__(self,c,key):
        self.c,self.key,self.references=c,key,0
        self.lock=threading.Lock();self.sessions={};self.workers=[];self.next_worker=0
        self.deadlines=[];self.deadline_cv=threading.Condition(self.lock);self.watchdog=None
        self.stopping=False
        comm=c['peer']['communication'];settings=c['peer']['connection_pool']
        self.pool=None
        if comm['reuse_connections']:
            self.pool=(FairConnectionPool(comm['connections_per_endpoint'],comm['max_connection_endpoints'])
                       if settings.get('head_scheduling','broadcast')=='fifo' else
                       ConnectionPool(comm['connections_per_endpoint'],comm['max_connection_endpoints'],settings['probe_mode']))
        try:
            context=mp.get_context('spawn')
            for i in range(c['peer']['head_receiver_processes']):
                child_c=copy.deepcopy(c);child_c['peer']['head_receiver_processes']=0
                if child_c['peer']['head_port']:child_c['peer']['head_port']+=i
                parent,child=context.Pipe(duplex=True)
                terminal_parent,terminal_child=(context.Pipe(duplex=False)
                    if c['peer'].get('head_receiver_ipc','shared')=='split' else (None,None))
                registration=None
                direct=c['peer'].get('head_direct',{})
                if direct.get('enabled') and direct.get('queued_reports') and direct.get('report_registration','ipc')=='shared':
                    from .report_registry import ReportRegistry
                    registration=ReportRegistry(context,c['peer']['max_active_queries'])
                proc=context.Process(target=_receiver_main,args=(child_c,child,terminal_child,registration),name=f'dhdgs-head-rx-{i}',daemon=True)
                w=dict(conn=parent,process=proc,queue=queue.Queue(c['peer']['max_active_queries']*2+1),
                       ready=threading.Event(),error=None,pending={},index=i,terminal_conn=terminal_parent,registration=registration,
                       lock=threading.Lock(),sessions={})
                self.workers.append(w)
                proc.start();child.close()
                if terminal_child is not None:terminal_child.close()
                w['reader']=threading.Thread(target=self._read,args=(w,),name=f'dhdgs-rx-read-{i}',daemon=True)
                w['writer']=threading.Thread(target=self._write,args=(w,),name=f'dhdgs-rx-write-{i}',daemon=True)
                w['reader'].start();w['writer'].start()
                if terminal_parent is not None:
                    w['terminal_reader']=threading.Thread(target=self._read,args=(w,terminal_parent),
                        name=f'dhdgs-rx-terminal-{i}',daemon=True)
                    w['terminal_reader'].start()
            for w in self.workers:
                if not w['ready'].wait(c['network']['timeout_seconds']):raise TimeoutError('Receiver startup timeout')
                if w['error']:raise RuntimeError(w['error'])
            self.endpoint=self.workers[0]['endpoint']
            if c['peer'].get('head_query_workers',0):
                self.watchdog=threading.Thread(target=self._watch_deadlines,name='dhdgs-head-deadlines',daemon=True)
                self.watchdog.start()
        except BaseException:
            self.close();raise

    def _failed(self,w,exc):
        error=f'Report receiver {w["index"]} unavailable: {exc}'
        with w['lock']:
            if w['error']:return
            w['error']=error
            pending=list(w['pending'].values())
            sessions=list(w['sessions'].values())
        w['ready'].set()
        for item in pending:item['error']=error;item['event'].set()
        for s in sessions:
            with s.delivery_lock:
                s.registry.error=error
                # If receiver died before publishing involvement, cancel every
                # possible node. This sends control only, never starts search.
                s.registry.nodes.update(range(len(self.c['cluster']['nodes'])))
                s.done.set()
        for item in pending:
            future=item.get('future')
            if future is not None:
                with item['session'].delivery_lock:
                    if not future.done():_resolve(future,error=RuntimeError(error))

    def _enqueue(self,w,message):
        if w['error']:raise RuntimeError(w['error'])
        message['enqueued_ns']=time.monotonic_ns()
        w['queue'].put_nowait(message)

    def _write(self,w):
        try:
            while True:
                message=w['queue'].get()
                if message is None:return
                message['dequeued_ns']=time.monotonic_ns()
                data=msgpack.packb(message,use_bin_type=True)
                if len(data)>self.c['network']['max_message_bytes']:raise ValueError('Receiver command too large')
                w['conn'].send_bytes(data)
                if message['kind']=='stop':return
        except (OSError,ValueError) as exc:self._failed(w,exc)

    def _read(self,w,conn=None):
        conn=w['conn'] if conn is None else conn
        try:
            while True:
                message=msgpack.unpackb(conn.recv_bytes(self.c['network']['max_message_bytes']),raw=False)
                received=time.monotonic_ns();kind=message['kind']
                if kind=='ready':
                    if CLOCK_DOMAIN is None or message['clock_domain']!=CLOCK_DOMAIN:raise ValueError('Receiver clock domain mismatch')
                    w.update(endpoint=message['endpoint'],pid=message['pid']);w['ready'].set();continue
                with w['lock']:
                    item=w['pending'].get(message['qid'])
                    session=w['sessions'].get(message['qid'])
                    pair=(session,w) if session is not None else None
                if kind=='registered':
                    if item is not None:
                        item['metrics']={**message.get('metrics',{}),
                            'register_ack_parent_received_ns':received,
                            'register_ack_parent_applied_ns':time.monotonic_ns()}
                        item['error']=message.get('error');item['event'].set()
                    continue
                if pair is None:continue
                s,owner=pair
                if owner is not w:raise ValueError('Wrong receiver ownership')
                with s.delivery_lock:
                    if s.closed:continue
                    if kind=='entry':
                        if item is None or 'future' not in item:continue
                        item['entry']=message
                        if message.get('error'):s.registry.error=message['error']
                        self._finish_async(s,item)
                        continue
                    s.registry.nodes.update(message['nodes'])
                    if kind=='terminal':
                        metrics=message['metrics']
                        if metrics['clock_domain']!=CLOCK_DOMAIN:raise ValueError('Terminal clock domain mismatch')
                        s.registry.reports={r['branch_id']:r for r in message['reports']}
                        s.registry.error=message['error']
                        if getattr(s,'receiver_control_additive',False):
                            for control_kind,counts in message['control_by_kind'].items():s.record_control(control_kind,counts)
                            s.control['errors']+=message['control']['errors']
                            s.control_error_details.extend(message['control_error_details'])
                        else:
                            s.control=message['control'];s.control_by_kind=message['control_by_kind']
                            s.control_error_details=message['control_error_details']
                        s.receiver_metrics.update(metrics,terminal_parent_received_ns=received,
                            terminal_parent_applied_ns=time.monotonic_ns(),receiver_index=w['index'])
                        s.done.set()
                        if item is not None and 'future' in item:
                            item['terminal']=True;self._finish_async(s,item)
        except (EOFError,OSError,ValueError,KeyError,TypeError) as exc:self._failed(w,exc)

    def register(self,s,deferred=False):
        started=time.monotonic_ns();qid=s.registry.query_id
        item=dict(event=threading.Event(),error=None,session=s)
        with self.lock:
            if self.stopping:raise RuntimeError('Head receivers closed')
            if qid in self.sessions:raise ValueError('Duplicate head query identity')
            if len(self.sessions)>=self.c['peer']['max_active_queries']:
                raise RuntimeError('Head receiver query capacity exceeded')
            w=self.workers[self.next_worker%len(self.workers)];self.next_worker+=1
            self.sessions[qid]=(s,w)
        try:
            with w['lock']:
                if w['error']:raise RuntimeError(w['error'])
                w['sessions'][qid]=s;w['pending'][qid]=item
            s.receiver_metrics['receiver_index']=w['index']
            if not deferred and w['registration'] is not None:
                if w['error']:raise RuntimeError(w['error'])
                published=time.monotonic_ns()
                w['registration'].put(dict(qid=qid,token=s.registry.token,root_id=s.registry.root_id,
                    root_node=s.registry.root_node,budget=s.registry.budget,slots=s.registry.slots,published_ns=published))
                s.receiver_metrics.update(register_enter_ns=started,register_return_ns=time.monotonic_ns(),
                    shared_identity_published_ns=published,registration_ms=(time.monotonic_ns()-started)/1e6)
                return w['endpoint']
            if deferred:
                item.update(future=Future(),entry=None,terminal=False)
                s.receiver_metrics.update(register_enter_ns=started,registration_ms=0.,
                                          registration_mode='receiver_continuation')
                return w['endpoint']
            self._enqueue(w,dict(kind='register',qid=qid,token=s.registry.token,
                                 root_id=s.registry.root_id,root_node=s.registry.root_node,
                                 budget=s.registry.budget,slots=s.registry.slots))
            if not item['event'].wait(self.c['peer']['control_timeout_seconds']):
                raise TimeoutError('Report receiver registration timeout')
            if item['error']:raise RuntimeError(item['error'])
            s.receiver_metrics.update(item.get('metrics',{}),register_enter_ns=started,
                register_return_ns=time.monotonic_ns(),registration_ms=(time.monotonic_ns()-started)/1e6)
            return w['endpoint']
        except BaseException:
            self.unregister(s,False);raise
        finally:
            if not deferred:
                with w['lock']:w['pending'].pop(qid,None)

    def submit_query(self,s,state,endpoint,deadline):
        qid=s.registry.query_id
        with self.deadline_cv:
            _,w=self.sessions[qid];item=w['pending'][qid]
            item['deadline']=deadline
            heapq.heappush(self.deadlines,(deadline,qid))
            if self.watchdog is not None:self.deadline_cv.notify()
        try:
            self._enqueue(w,dict(kind='start_query',qid=qid,token=s.registry.token,
                root_id=s.registry.root_id,root_node=s.registry.root_node,
                budget=s.registry.budget,slots=s.registry.slots,state=state,endpoint=endpoint,
                register_enter_ns=s.receiver_metrics['register_enter_ns'],deadline=deadline))
        except (RuntimeError,queue.Full) as exc:
            with s.delivery_lock:
                if not item['future'].done():_resolve(item['future'],error=exc)
        return item['future']

    def _finish_async(self,s,item):
        future=item['future']
        if future.done():return
        if s.registry.error:_resolve(future,error=RuntimeError(s.registry.error));return
        if item['entry'] is None or not item['terminal']:return
        try:
            if not s.registry.complete():raise ValueError('Incomplete receiver terminal bundle')
        except (ValueError,RuntimeError,KeyError,TypeError) as exc:_resolve(future,error=exc)
        else:
            s.receiver_metrics['query_ready_ns']=time.monotonic_ns()
            _resolve(future,item['entry'])

    def _watch_deadlines(self):
        while True:
            with self.deadline_cv:
                if self.stopping:return
                if not self.deadlines:self.deadline_cv.wait();continue
                deadline,qid=self.deadlines[0]
                pair=self.sessions.get(qid)
                if pair is None:heapq.heappop(self.deadlines);continue
                remaining=deadline-time.monotonic()
                if remaining>0:self.deadline_cv.wait(remaining);continue
                heapq.heappop(self.deadlines)
                item=pair[1]['pending'].get(qid)
            if item is not None:
                with pair[0].delivery_lock:
                    future=item.get('future')
                    if future is not None and not future.done():
                        _resolve(future,error=TimeoutError('Global query deadline exceeded'))

    def unregister(self,s,success):
        entered=time.monotonic_ns()
        with self.deadline_cv:
            acquired=time.monotonic_ns()
            pair=self.sessions.pop(s.registry.query_id,None)
            if len(self.deadlines)>2*self.c['peer']['max_active_queries']:
                self.deadlines=[(d,q) for d,q in self.deadlines if q in self.sessions]
                heapq.heapify(self.deadlines)
            if self.watchdog is not None:self.deadline_cv.notify()
            leaving=time.monotonic_ns()
        s.receiver_metrics['receiver_unregister_lock_wait_ms']=(acquired-entered)/1e6
        s.receiver_metrics['receiver_unregister_lock_held_ms']=(leaving-acquired)/1e6
        if pair is not None:
            with pair[1]['lock']:
                pair[1]['pending'].pop(s.registry.query_id,None)
                pair[1]['sessions'].pop(s.registry.query_id,None)
        if pair is not None and pair[1]['registration'] is not None:pair[1]['registration'].remove(s.registry.query_id)
        if pair is None or success:return  # completed receiver already holds tombstone
        try:self._enqueue(pair[1],dict(kind='unregister',qid=s.registry.query_id))
        except (RuntimeError,queue.Full):
            # A failed/overloaded receiver cannot safely retain live identities.
            pair[1]['process'].terminate()

    def exchange(self,*args):return rpc_exchange(*args,self.pool)

    def status(self):
        with self.lock:
            return [dict(index=w['index'],pid=w.get('pid'),endpoint=w.get('endpoint'),
                         alive=w['process'].is_alive(),error=w['error'],
                         command_queue_depth=w['queue'].qsize(),
                         active_queries=sum(owner is w for _,owner in self.sessions.values()))
                    for w in self.workers]

    def close(self):
        with self.deadline_cv:self.stopping=True;self.deadline_cv.notify_all()
        if self.watchdog is not None:self.watchdog.join(3)
        for w in self.workers:
            try:self._enqueue(w,dict(kind='stop'))
            except (RuntimeError,queue.Full):pass
        for w in self.workers:
            p=w['process']
            if p.pid is not None:
                p.join(3)
                if p.is_alive():p.terminate();p.join(3)
            w['conn'].close()
            if w.get('terminal_conn') is not None:w['terminal_conn'].close()
            try:w['queue'].put_nowait(None)
            except queue.Full:pass
            for name in ('reader','writer','terminal_reader'):
                if name in w:w[name].join(3)
        if self.pool is not None:self.pool.close()
