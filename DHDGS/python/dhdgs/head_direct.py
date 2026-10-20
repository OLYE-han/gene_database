"""One distribution pool, asynchronous confirmation threads, process result pool."""
from collections import OrderedDict
import copy
from concurrent.futures import Future,ThreadPoolExecutor,ProcessPoolExecutor,CancelledError
import asyncio
import json
import multiprocessing as mp
import os
import queue
from pathlib import Path
import socket
import struct
import threading
import time
import msgpack

from .head_hub import HeadHub
from .pipeline_hub import PipelineHub,resolve
from .transport import send


async def read_frame(reader,limit):
    size=struct.unpack('!I',await reader.readexactly(4))[0]
    if not 0<size<=limit:raise ValueError('Invalid frame length')
    value=msgpack.unpackb(await reader.readexactly(size),raw=False)
    if not isinstance(value,dict):raise ValueError('Expected mapping')
    return value,size+4


class DirectHub(HeadHub):
    callback=PipelineHub.callback

    def __init__(self,c,key):
        self.c=c;self.key=key;self.references=0;self.lock=threading.Lock();self.cv=threading.Condition(self.lock)
        self.sessions={};self.pending={};self.closed=OrderedDict();self.stopping=False;self.serial=0;self.slots=[]
        settings=c['peer']['head_direct'];self.queued_reports=settings.get('queued_reports',False)
        self.report_slots=[];self.completion_threads=[];self.report_hub=None
        self.process_reports=self.queued_reports and settings.get('report_backend','thread')=='process'
        # At most one completion notification per admitted query, including active workers.
        self.completions=queue.Queue(c['peer']['max_active_queries'])
        if self.queued_reports:
            for i in range(settings.get('completion_threads',2)):
                t=threading.Thread(target=self._complete,name=f'dhdgs-complete-{i}',daemon=True)
                self.completion_threads.append(t);t.start()
        for i in range(c['peer']['head_direct']['confirmation_threads']):
            ready=threading.Event();slot=dict(ready=ready,index=i,writers=set())
            thread=threading.Thread(target=self._run,args=(slot,),name=f'dhdgs-confirm-{i}',daemon=True)
            slot['thread']=thread;self.slots.append(slot);thread.start()
            if not ready.wait(c['peer']['head_direct']['startup_timeout_seconds']):
                self.close();raise TimeoutError('Confirmation startup timeout')
            if slot.get('error'):
                self.close();raise RuntimeError(slot['error'])
        if self.queued_reports and not self.process_reports:
            for i in range(settings.get('result_channels',8)):
                slot=dict(ready=threading.Event(),index=i,writers=set(),report=True,
                          received=0,peak=0,backpressure=0)
                t=threading.Thread(target=self._run,args=(slot,),name=f'dhdgs-report-{i}',daemon=True)
                slot['thread']=t;self.report_slots.append(slot);t.start()
                if not slot['ready'].wait(settings['startup_timeout_seconds']) or slot.get('error'):
                    self.close();raise RuntimeError(slot.get('error','Report receiver startup timeout'))
        if self.process_reports:
            from .head_receivers import ProcessHeadHub
            child=copy.deepcopy(c);p=child['peer']
            p.update(head_receiver_processes=settings.get('result_channels',8),head_receiver_ipc='split',head_query_workers=0)
            if p['head_port']:p['head_port']+=len(self.slots)
            try:self.report_hub=ProcessHeadHub(child,key+'-report-channels')
            except BaseException:self.close();raise

    def _finish(self,s,item):
        if not self.queued_reports:return PipelineHub._finish(self,s,item)
        if self.process_reports:
            # Early entry ACK is not a terminal event. Do not flood the completion
            # queue with notifications that cannot possibly finish a query.
            if not s.registry.error and (item['entry'] is None or 'terminal_parent_applied_ns' not in s.receiver_metrics):return
            # The terminal pipe reader is already a consumer of an independent
            # result channel, separate from TCP ACK. Avoid another queue hop.
            if (self.c['peer']['head_direct'].get('completion_dispatch','queued')=='inline'
                    and threading.current_thread().name.startswith('dhdgs-rx-')):
                s.receiver_metrics['completion_queue_ms']=0.
                s.receiver_metrics['completion_started_ns']=time.monotonic_ns()
                return PipelineHub._finish(self,s,item)
        # Called with delivery_lock held. Coalescing bounds this notification queue
        # by admitted queries. Completion/serialization never runs on a socket loop.
        if self.stopping or item['future'].done() or item.get('completion_queued'):return
        item['completion_queued']=True
        try:self.completions.put_nowait((s,item,time.monotonic_ns()))
        except queue.Full:
            # Cancellation can leave stale notifications while new queries arrive.
            # Fail explicitly rather than losing a terminal notification silently.
            item['completion_queued']=False
            resolve(item['future'],error=RuntimeError('Head completion queue capacity exceeded'))

    def _complete(self):
        while True:
            work=self.completions.get()
            if work is None:return
            s,item,enqueued=work
            with s.delivery_lock:
                item['completion_queued']=False
                if s.closed or item['future'].done():continue
                started=time.monotonic_ns()
                s.receiver_metrics['completion_queue_ms']=s.receiver_metrics.get('completion_queue_ms',0.)+(started-enqueued)/1e6
                s.receiver_metrics['completion_started_ns']=started
                PipelineHub._finish(self,s,item)

    async def _consume_reports(self,slot):
        while True:
            message,rx,enqueued,response=await slot['inbox'].get()
            started=time.monotonic_ns()
            try:reply,after=self.callback(message,rx)
            except (ValueError,RuntimeError,KeyError,TypeError) as exc:reply,after=dict(ok=False,error=str(exc)),None
            validated=time.monotonic_ns()
            # Once admitted, a disconnected sender does not discard the report.
            if not response.cancelled():response.set_result((reply,after,started,validated))
            elif after:after(0)
            slot['inbox'].task_done()

    def _run(self,slot):
        async def main():
            slot['loop']=asyncio.get_running_loop();slot['stop']=asyncio.Event()
            p=self.c['peer'];offset=slot['index']+(len(self.slots) if slot.get('report') else 0)
            port=p['head_port']+offset if p['head_port'] else 0
            try:
                server=await asyncio.start_server(lambda r,w:self._client(slot,r,w),p['head_bind_host'],port,
                    backlog=self.c['network']['listen_backlog'])
                slot['endpoint']=[p['head_advertise_host'],server.sockets[0].getsockname()[1]]
            except Exception as exc:slot['error']=str(exc);slot['ready'].set();return
            if slot.get('report'):
                slot['inbox']=asyncio.Queue(p['head_direct'].get('report_queue_capacity',64))
                background=asyncio.create_task(self._consume_reports(slot))
            else:background=asyncio.create_task(self._heartbeat(slot))
            slot['ready'].set()
            await slot['stop'].wait();server.close();background.cancel()
            # Python 3.12 Server.wait_closed also waits for accepted transports.
            # Close persistent callback sockets before waiting for server teardown.
            for w in list(slot['writers']):w.close()
            tasks=[t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for t in tasks:t.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
            await server.wait_closed()
        asyncio.run(main())

    def register(self,s,deferred=False):
        now=time.monotonic_ns()
        with self.lock:
            qid=s.registry.query_id
            if self.stopping or len(self.sessions)>=self.c['peer']['max_active_queries']:raise RuntimeError('Head capacity exceeded')
            if qid in self.sessions or qid in self.closed:raise ValueError('Duplicate query identity')
            slot=self.slots[self.serial%len(self.slots)]
            report_slot=self.report_slots[self.serial%len(self.report_slots)] if self.report_slots else slot
            self.serial+=1
            self.sessions[qid]=s;self.pending[qid]=dict(future=Future(),entry=None,slot=slot,
                deadline=time.monotonic()+self.c['peer']['query_timeout_seconds'],next_lease=time.monotonic()+self.c['peer']['heartbeat_interval_ms']/1000)
        s.receiver_metrics.update(registration_mode='direct_async_confirm',registered_ns=now,
            registration_ms=0.,registration_ready_ms=(time.monotonic_ns()-now)/1e6,
            confirmation_slot=slot['index'],result_channel=report_slot['index'])
        if self.report_hub is not None:
            hub=self
            class TerminalSignal(threading.Event):
                def set(signal):
                    super().set()
                    with s.delivery_lock:
                        with hub.lock:item=hub.pending.get(qid)
                        if item is not None:hub._finish(s,item)
            s.done=TerminalSignal();s.receiver_control_additive=True
            try:
                endpoint=self.report_hub.register(s)
                owner=s.receiver_metrics['receiver_index']
                s.receiver_metrics.update(registration_mode='direct_process_channels',result_channel=owner)
                return endpoint
            except BaseException:
                with self.lock:self.sessions.pop(qid,None);self.pending.pop(qid,None)
                raise
        return report_slot['endpoint']

    def submit_query(self,s,state,endpoint,deadline):
        """Called by distribution worker. Send bytes; transfer socket ownership without recv."""
        qid=s.registry.query_id
        with self.lock:item=self.pending[qid]
        item['deadline']=deadline;message=dict(version=1,op='peer_submit',state=state)
        enter=time.monotonic_ns();s.receiver_metrics['entry_submit_enter_ns']=enter
        sock=None;counts=dict(rpc_count=0,request_bytes=0,response_bytes=0,connections_created=0,connections_reused=0)
        error=None
        try:
            timeout=min(self.c['peer']['handoff_timeout_seconds'],max(.001,deadline-time.monotonic()))
            sock=socket.create_connection(endpoint,timeout);counts['connections_created']+=1
            counts['rpc_count']+=1;counts['request_bytes']+=send(sock,message,self.c['network']['max_message_bytes'])
            sock.setblocking(False)
        except (OSError,ValueError) as exc:
            error=exc
            if sock:sock.close();sock=None
        sent=time.monotonic_ns();s.receiver_metrics['entry_send_done_ns']=sent
        asyncio.run_coroutine_threadsafe(self._confirm(s,item,sock,message,endpoint,enter,sent,counts,error),item['slot']['loop'])
        return item['future']

    async def _confirm(self,s,item,sock,message,endpoint,enter,sent,counts,error):
        p=self.c['peer'];limit=self.c['network']['max_message_bytes'];writer=None
        try:
            for attempt in range(p['head_direct']['submit_retries']+1):
                if item['future'].done() or s.closed:return
                try:
                    timeout=min(p['head_direct']['ack_timeout_ms']/1000,item['deadline']-time.monotonic())
                    if timeout<=0:raise TimeoutError('Global deadline before entry confirmation')
                    async with asyncio.timeout(timeout):
                        if attempt==0:
                            if error:raise error
                            reader,writer=await asyncio.open_connection(sock=sock);sock=None
                        else:
                            reader,writer=await asyncio.open_connection(*endpoint)
                            counts['connections_created']+=1;counts['rpc_count']+=1
                            body=msgpack.packb(message,use_bin_type=True)
                            writer.write(struct.pack('!I',len(body))+body);counts['request_bytes']+=len(body)+4
                            await writer.drain()
                        reply,rx=await read_frame(reader,limit);counts['response_bytes']+=rx
                        if not reply.get('ok'):raise ValueError('Entry rejected: '+str(reply.get('error')))
                    end=time.monotonic_ns()
                    counts.update(wall_ms=(end-enter)/1e6,wall_including_release_ms=(end-enter)/1e6,
                        response_wait_ms=(end-sent)/1e6,send_ms=(sent-enter)/1e6,pool_wait_ms=0.,connect_ms=0.)
                    with s.delivery_lock:
                        item['entry']=dict(reply=reply,counts=dict(counts),enter_ns=enter,return_ns=end)
                        s.receiver_metrics.update(entry_ack_received_ns=end,entry_submit_attempts=attempt+1)
                        self._finish(s,item)
                    return
                except (OSError,EOFError,asyncio.IncompleteReadError) as exc:
                    if attempt>=p['head_direct']['submit_retries']:raise
                    s.receiver_metrics['entry_retry_reason']=str(exc)
                finally:
                    if writer:writer.close();writer=None
            raise RuntimeError('Entry acknowledgement missing')
        except Exception as exc:resolve(item['future'],error=exc)
        finally:
            if sock:sock.close()
            if writer:writer.close()

    async def _client(self,slot,reader,writer):
        if slot.get('report') and len(slot['writers'])>=self.c['peer']['head_direct'].get('report_connections_per_channel',64):
            writer.close();return
        slot['writers'].add(writer)
        try:
            while not self.stopping:
                message,rx=await asyncio.wait_for(read_frame(reader,self.c['network']['max_message_bytes']),self.c['network']['timeout_seconds'])
                received=time.monotonic_ns()
                if slot.get('report'):
                    response=asyncio.get_running_loop().create_future()
                    if slot['inbox'].full():slot['backpressure']+=1
                    await slot['inbox'].put((message,rx,received,response))
                    enqueued=time.monotonic_ns();slot['received']+=1
                    slot['peak']=max(slot['peak'],slot['inbox'].qsize())
                    reply,after,started,validated=await response
                else:
                    try:reply,after=self.callback(message,rx)
                    except (ValueError,RuntimeError,KeyError,TypeError) as exc:reply,after=dict(ok=False,error=str(exc)),None
                body=msgpack.packb(reply,use_bin_type=True);writer.write(struct.pack('!I',len(body))+body)
                try:await writer.drain()
                finally:
                    # Validation has transferred ownership to the registry. An ACK
                    # write failure must not prevent completion; retries are idempotent.
                    if slot.get('report'):
                        ack=time.monotonic_ns()
                        with self.lock:s=self.sessions.get(message.get('query_request_id'))
                        if s is not None:
                            with s.delivery_lock:
                                if not s.closed:
                                    s.receiver_metrics.setdefault('report_receipts',[]).append(dict(
                                        channel=slot['index'],kind='result' if message.get('reports') else 'heartbeat',
                                        received_ns=received,enqueued_ns=enqueued,validation_start_ns=started,
                                        validated_ns=validated,ack_write_finished_ns=ack,bytes=rx))
                    if after:after(len(body)+4)
        except (OSError,ValueError,asyncio.IncompleteReadError,TimeoutError,msgpack.UnpackException):pass
        finally:slot['writers'].discard(writer);writer.close()

    async def _rpc(self,endpoint,message):
        writer=None
        try:
            async with asyncio.timeout(self.c['peer']['control_timeout_seconds']):
                reader,writer=await asyncio.open_connection(*endpoint);body=msgpack.packb(message,use_bin_type=True)
                writer.write(struct.pack('!I',len(body))+body);await writer.drain()
                reply,rx=await read_frame(reader,self.c['network']['max_message_bytes'])
                if not reply.get('ok'):raise RuntimeError(str(reply.get('error')))
                return reply,dict(rpc_count=1,request_bytes=len(body)+4,response_bytes=rx)
        finally:
            if writer:writer.close()

    async def _lease(self,s,item,sid):
        node=self.c['cluster']['nodes'][sid]
        try:
            _,counts=await self._rpc([node['host'],node['port']],dict(version=1,op='peer_lease',query_request_id=s.registry.query_id,token=s.registry.token))
            with s.delivery_lock:
                if not s.closed:s.record_control('lease',counts)
        except (OSError,ValueError,RuntimeError) as exc:
            with s.delivery_lock:
                if not s.closed:s.record_control_error('lease',sid,exc)

    async def _heartbeat(self,slot):
        interval=self.c['peer']['heartbeat_interval_ms']/1000
        while True:
            await asyncio.sleep(min(interval,.05))
            with self.lock:rows=[(self.sessions[q],i) for q,i in self.pending.items() if i['slot'] is slot]
            now=time.monotonic()
            for s,item in rows:
                if item['future'].done():continue
                if now>=item['deadline']:resolve(item['future'],error=TimeoutError('Global query deadline exceeded'));continue
                if now<item['next_lease']:continue
                item['next_lease']=now+interval
                with s.delivery_lock:nodes=s.registry.nodes-{r['node_id'] for r in s.registry.reports.values()}
                for sid in nodes:asyncio.create_task(self._lease(s,item,sid))

    def exchange(self,endpoint,message,*args):
        # Error cleanup must not block a confirmation event loop on a synchronous RPC.
        slot=self.slots[0];future=asyncio.run_coroutine_threadsafe(self._rpc(endpoint,message),slot['loop'])
        if threading.current_thread() in [x['thread'] for x in self.slots]:
            future.add_done_callback(lambda f:f.exception() if not f.cancelled() else None)
            return dict(ok=True),dict(rpc_count=0,request_bytes=0,response_bytes=0)
        return future.result(self.c['peer']['control_timeout_seconds']+1)

    def unregister(self,s,success):
        if self.report_hub is not None:self.report_hub.unregister(s,success)
        super().unregister(s,success)
        with self.lock:self.pending.pop(s.registry.query_id,None)

    def status(self):return self.report_hub.status() if self.report_hub is not None else []

    def close(self):
        with self.lock:self.stopping=True;items=list(self.pending.values())
        for item in items:resolve(item['future'],error=RuntimeError('Direct head closed'))
        for slot in self.slots+self.report_slots:
            if slot.get('loop') and not slot['loop'].is_closed():slot['loop'].call_soon_threadsafe(slot['stop'].set)
        for slot in self.slots+self.report_slots:slot['thread'].join()
        if self.report_hub is not None:self.report_hub.close();self.report_hub=None
        for _ in self.completion_threads:self.completions.put(None)
        for t in self.completion_threads:t.join()
        self.completion_threads=[]

    def report_status(self):
        if self.report_hub is not None:return self.report_hub.status()
        return [dict(channel=s['index'],endpoint=s.get('endpoint'),received=s['received'],
                     queue_peak=s['peak'],backpressure=s['backpressure']) for s in self.report_slots]


def result_ready():return os.getpid()


def initialize_result_worker(ready,blas_threads,switch_ms):
    import sys
    from .head_merge import aggregate_reports
    from threadpoolctl import threadpool_limits
    global _result_blas_limit
    _result_blas_limit=threadpool_limits(limits=blas_threads)
    sys.setswitchinterval(switch_ms/1000)
    ready.put(os.getpid())


def process_result(packet):
    from .head_merge import aggregate_reports
    started=time.monotonic_ns();cpu=time.thread_time_ns()
    c=packet['config'];reports=packet['reports'];transfer=packet['transfer'];control=packet['control']
    ranked,stats=aggregate_reports(c,reports,transfer,packet['involved'])
    if stats['expanded']>c['search']['max_expansions']:raise ValueError('Global budget exceeded')
    stats.update(control_rpc_count=control['rpc_count'],control_request_bytes=control['request_bytes'],
        control_response_bytes=control['response_bytes'],control_errors=control['errors'],
        control_by_kind=packet['control_by_kind'],control_error_details=packet['control_error_details'],
        control_wire_bytes=control['request_bytes']+control['response_bytes'],
        all_protocol_bytes=stats['wire_bytes']+control['request_bytes']+control['response_bytes'],
        all_protocol_rpc_count=stats['rpc_count']+control['rpc_count'],head_receiver=packet['receiver'],entry_rpc=transfer,
        metrics_complete=stats['rpc_retries']==0 and control['errors']==0)
    timing=packet['timing'];timing['result_started_ns']=started
    stats['head_direct']=timing
    stats['head_distribution_queue_ms']=(timing['distribution_started_ns']-timing['submitted_ns'])/1e6
    stats['head_result_queue_ms']=(started-timing['result_enqueued_ns'])/1e6
    stats['head_stage_queue_ms']=stats['head_distribution_queue_ms']
    stats['head_stage_resume_queue_ms']=stats['head_result_queue_ms']
    stats['result_process_pid']=os.getpid()
    stats['result_backend']=c['peer']['head_direct'].get('result_backend','process')
    stats['entry_submit_retries']=max(0,transfer['rpc_count']-1)
    stats['metrics_complete'] &= stats['entry_submit_retries']==0
    result=dict(status='ok',request_id=packet['request_id'],query_id=packet['query_id'],
        ids=[gid for gid,_ in ranked],squared_distances=[d for _,d in ranked],epoch=packet['epoch'],
        metric=c['data']['metric'],stats=stats,branch_reports=reports)
    if packet['trace'] is not None:stats['head_timestamps']=packet['trace']
    stats['coordinator_thread_cpu_ms']=packet['route_cpu_ms']+(time.thread_time_ns()-cpu)/1e6
    if c['peer']['head_direct']['save_results']:
        root=Path(c['peer']['head_direct']['results_directory']);root.mkdir(parents=True,exist_ok=True)
        path=root/f'results-{os.getpid()}.jsonl';before=time.monotonic_ns()
        # The worker persists pre-return results. Final write/IPC timing is in caller metrics.
        with path.open('a') as f:f.write(json.dumps(result,allow_nan=False)+'\n')
        stats['result_write_ms']=(time.monotonic_ns()-before)/1e6
        stats['result_log']=str(path)
    else:stats['result_write_ms']=0.
    ended=time.monotonic_ns();timing['result_finished_ns']=ended
    latency=(ended-timing['submitted_ns'])/1e6
    stats['latency_ms']=latency
    parts=dict(distribution_queue_ms=stats['head_distribution_queue_ms'],
        route_and_send_ms=(timing['distribution_send_done_ns']-timing['distribution_started_ns'])/1e6,
        confirmation_and_reports_ms=(timing['result_enqueued_ns']-timing['distribution_send_done_ns'])/1e6,
        result_queue_ms=stats['head_result_queue_ms'],result_processing_ms=(ended-started)/1e6)
    stats['latency_breakdown_ms']=parts;stats['latency_breakdown_fraction']={k:v/latency for k,v in parts.items()}
    return result


class DirectFlow:
    def __init__(self,c):
        self.c=c;p=c['peer']['head_direct'];self.cv=threading.Condition();self.records={};self.closed=False
        self.distributors=ThreadPoolExecutor(p['distribution_threads'],thread_name_prefix='dhdgs-distribute')
        self.http_slots=threading.BoundedSemaphore(c['peer']['max_active_queries'])
        self.result_slots=threading.BoundedSemaphore(p['result_queue_capacity'])
        self.inline_results=p.get('result_backend','process')=='inline'
        self.results=None;self.ready_queue=None;self.worker_pids=set()
        if self.inline_results:
            # Only terminal consumers/completion workers reach merge in this mode.
            # Data-network ACKs are handled by independent receiver processes.
            if not (p.get('queued_reports') and p.get('report_backend')=='process'):
                self.distributors.shutdown();raise ValueError('Inline results require process report receivers')
            self.ingress=self
            return
        context=mp.get_context('spawn');self.ready_queue=context.Queue()
        self.results=ProcessPoolExecutor(p['result_processes'],mp_context=context,
            initializer=initialize_result_worker,initargs=(self.ready_queue,c['runtime']['blas_threads'],c['runtime']['python_switch_interval_ms']))
        # Start workers before benchmark; importing NumPy must not be charged to first queries.
        futures=[self.results.submit(result_ready) for _ in range(p['result_processes'])]
        try:
            self.worker_pids={self.ready_queue.get(timeout=p['startup_timeout_seconds']) for _ in range(p['result_processes'])}
            if len(self.worker_pids)!=p['result_processes']:raise RuntimeError('Result workers not fully initialized')
            for f in futures:f.result(p['startup_timeout_seconds'])
        except BaseException:self.results.shutdown(cancel_futures=True);self.distributors.shutdown();raise
        self.ingress=self

    def status(self):
        return dict(mode='direct',distribution_threads=self.c['peer']['head_direct']['distribution_threads'],
            confirmation_threads=self.c['peer']['head_direct']['confirmation_threads'],
            result_backend='inline' if self.inline_results else 'process',
            result_processes=[] if self.results is None else [p.pid for p in self.results._processes.values()],active_queries=len(self.records))

    def submit(self,task,*args):
        # HTTP ingress calls submit(fn,*args); search admission calls submit(generator,timing).
        if callable(task):
            if not self.http_slots.acquire(blocking=False):raise RuntimeError('Head HTTP admission capacity exceeded')
            def receive():
                try:return task(*args)
                finally:self.http_slots.release()
            try:return self.distributors.submit(receive)
            except BaseException:self.http_slots.release();raise
        timing=args[0];timing['submitted_ns']=time.monotonic_ns();out=Future()
        rec=dict(generator=task,out=out,timing=timing,waiting=None)
        with self.cv:
            if self.closed or len(self.records)>=self.c['peer']['max_active_queries']:
                task.close();out.set_exception(RuntimeError('Head capacity exceeded'));return out
            self.records[id(rec)]=rec
        out.add_done_callback(lambda f:rec['waiting'].cancel() if f.cancelled() and rec['waiting'] is not None else None)
        if threading.current_thread().name.startswith('dhdgs-distribute'):
            self._step(rec,None)
        else:self.distributors.submit(self._step,rec,None)
        return out

    def _step(self,rec,operation):
        g,out=rec['generator'],rec['out']
        try:
            if out.cancelled():waiting=g.throw(CancelledError())
            elif operation is None:
                rec['timing']['distribution_started_ns']=time.monotonic_ns();waiting=next(g)
            else:
                try:value=operation.result()
                except BaseException as exc:waiting=g.throw(exc)
                else:waiting=g.send(value)
            if not isinstance(waiting,Future):raise TypeError('Expected stage Future')
            rec['waiting']=waiting
            if operation is None:rec['timing']['distribution_released_ns']=time.monotonic_ns()
            waiting.add_done_callback(lambda f:self._step(rec,f))
            if out.cancelled():waiting.cancel()
        except StopIteration as done:
            with self.cv:self.records.pop(id(rec),None);self.cv.notify_all()
            if out.set_running_or_notify_cancel():out.set_result(done.value)
        except BaseException as exc:
            g.close()
            with self.cv:self.records.pop(id(rec),None);self.cv.notify_all()
            if out.set_running_or_notify_cancel():out.set_exception(exc)

    def merge(self,packet):
        if not self.result_slots.acquire(blocking=False):raise RuntimeError('Result queue capacity exceeded')
        if self.inline_results:
            future=Future()
            try:future.set_result(process_result(packet))
            except BaseException as exc:future.set_exception(exc)
            finally:self.result_slots.release()
            return future
        try:future=self.results.submit(process_result,packet)
        except BaseException:self.result_slots.release();raise
        future.add_done_callback(lambda _:self.result_slots.release());return future

    def close(self):
        with self.cv:self.closed=True;records=list(self.records.values())
        for r in records:r['out'].cancel()
        self.distributors.shutdown(wait=True)
        with self.cv:self.cv.wait_for(lambda:not self.records)
        if self.results is not None:self.results.shutdown(wait=True,cancel_futures=True)
        if self.ready_queue is not None:self.ready_queue.close();self.ready_queue.join_thread()
