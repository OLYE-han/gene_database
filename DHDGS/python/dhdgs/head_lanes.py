"""Query-affine head processes: one input and one final-result IPC per query.

Each worker owns routing, leases, branch verification and merge. Data nodes still
decide every graph expansion. No intermediate search state returns to the parent.
"""
from concurrent.futures import Future, InvalidStateError
import copy
import json
import multiprocessing as mp
from pathlib import Path
import queue
import struct
import threading
import time
import uuid

import msgpack
import numpy as np

from .metrics import CLOCK_DOMAIN

IPC_TIMING = struct.Struct('!4sQQQQ')
IPC_MAGIC = b'DHL1'


def resolve(future, value=None, error=None):
    try:
        if error is None:future.set_result(value)
        else:future.set_exception(error)
    except InvalidStateError:
        if not future.done():raise


def worker_config(c, index):
    c=copy.deepcopy(c);p=c['peer'];s=p['head_lanes']
    s['enabled']=False
    p.update(head_receiver_processes=0,head_query_workers=0)
    if p['head_port']:p['head_port']+=index
    if 'head_direct' in p:p['head_direct']['enabled']=False
    p['head_pipeline']=dict(enabled=True,receive_min=1,receive_max=1,
        receive_backlog=8,receive_idle_ms=1000,route_workers=s['route_threads'],
        merge_workers=1,dispatch_workers=s['dispatch_threads'],
        callback_connections=s['callback_connections'],ack_inline=True)
    return c


def lane_main(c,index,conn):
    from .search import Engine
    from .index import native
    from threadpoolctl import threadpool_limits
    limit=c['network']['max_message_bytes'];write_lock=threading.Lock()
    active={};active_lock=threading.Lock();engine=None
    settings=c['peer']['head_lanes']
    result_path=None
    if settings.get('save_results',True):
        root=Path(settings['results_directory']);root.mkdir(parents=True,exist_ok=True)
        result_path=root/f'lane-{index}.jsonl'
    def emit(message):
        pack_enter=time.monotonic_ns()
        data=msgpack.packb(message,use_bin_type=True)
        pack_done=time.monotonic_ns()
        traced=message.get('kind')=='result' and 'worker_finished_ns' in message
        if len(data)+(IPC_TIMING.size if traced else 0)>limit:raise ValueError('Head lane result exceeds IPC limit')
        lock_enter=time.monotonic_ns()
        with write_lock:
            lock_acquired=time.monotonic_ns()
            if traced:data=IPC_TIMING.pack(IPC_MAGIC,pack_enter,pack_done,lock_enter,lock_acquired)+data
            conn.send_bytes(data)
    def done(key,future):
        try:
            try:
                result=future.result();before=time.monotonic_ns()
                if result_path is not None:
                    with result_path.open('a') as f:f.write(json.dumps(result,allow_nan=False)+'\n')
                result['stats']['head_lane_write_ms']=(time.monotonic_ns()-before)/1e6
                value=dict(kind='result',key=key,result=result,worker_finished_ns=time.monotonic_ns())
            except BaseException as exc:
                value=dict(kind='result',key=key,error=f'{type(exc).__name__}: {exc}',
                           context=getattr(exc,'dhdgs_metrics',{}),worker_finished_ns=time.monotonic_ns())
            with active_lock:active.pop(key,None)
            try:emit(value)
            except ValueError:emit(dict(kind='result',key=key,error='Head lane result exceeds IPC limit'))
        except (OSError,EOFError):pass # Parent has closed; shutdown cancels remaining work.
    try:
        threadpool_limits(limits=c['runtime']['blas_threads'])
        engine=Engine(worker_config(c,index))
        contiguous=(len(engine.owners)==engine.manifest['rows'] and all(gid==i for i,gid in enumerate(engine.owners)))
        emit(dict(kind='ready',manifest=engine.manifest,source_file=__file__,contiguous_ids=contiguous,clock_domain=CLOCK_DOMAIN,
                  endpoint=engine.head_hub.endpoint))
        while True:
            message=msgpack.unpackb(conn.recv_bytes(limit),raw=False)
            kind=message['kind'];key=message.get('key')
            if kind=='stop':break
            if kind=='query':
                received=time.monotonic_ns()
                if message['dtype'] not in ('<f4','<f8'):raise ValueError('Invalid vector dtype')
                vector=np.frombuffer(message['vector'],dtype=message['dtype'])
                future=engine.search_async(vector,message['query_id'])
                with active_lock:active[key]=future
                def completed(f,key=key,received=received):
                    if not f.cancelled() and f.exception() is None:
                        f.result()['stats']['head_lane_worker_received_ns']=received
                    done(key,f)
                future.add_done_callback(completed)
            elif kind=='cancel':
                with active_lock:future=active.get(key)
                if future is not None:future.cancel()
            elif kind=='configure':
                try:
                    with active_lock:
                        if active:raise RuntimeError('Cannot configure active head lane')
                    candidate=worker_config(message['config'],index)
                    if candidate['routing']['ef_search']!=engine.c['routing']['ef_search']:
                        m=engine.manifest;r=candidate['routing']
                        router=native().Router(m['dimension'],m['route_samples'],r['hnsw_m'],r['ef_construction'],candidate['seed'])
                        router.load(str(Path(candidate['paths']['output'])/'index/router.hnsw'))
                        engine.router=router
                    engine.c=candidate
                    emit(dict(kind='configured',key=key))
                except Exception as exc:emit(dict(kind='configured',key=key,error=str(exc)))
            else:raise ValueError('Unknown head lane command')
    except BaseException as exc:
        try:emit(dict(kind='failed',error=f'{type(exc).__name__}: {exc}'))
        except (OSError,ValueError):pass
    finally:
        if engine is not None:
            try:
                a=c['peer'].get('adaptive_services',{})
                if a.get('enabled') and (a.get('head_dispatch') or a.get('head_connections')) and result_path is not None:
                    result_path.with_suffix('.services.json').write_text(json.dumps(
                        dict(dispatch=engine.head_hub.dispatcher.metrics(),connections=engine.head_hub.pool.metrics() if hasattr(engine.head_hub.pool,'metrics') else None),indent=2))
            finally:engine.close()
        conn.close()


class LaneIngress:
    """HTTP parsing uses a small bounded ingress, independent of active queries."""
    def __init__(self,owner):
        from .head_pipeline import AdaptiveIngress
        self.owner=owner
        self.http=AdaptiveIngress(dict(receive_min=1,receive_max=2,receive_backlog=8,receive_idle_ms=1000),
                                  owner.c['peer']['max_active_queries'])
        self.ingress=self
    def submit(self,*args):return self.http.submit(*args)
    def status(self):return dict(mode='lanes',workers=self.owner.status(),http=self.http.status())
    def close(self):self.http.close()


class LaneEngine:
    def __init__(self,c):
        from .runtime import apply_process_runtime
        apply_process_runtime(c)
        self.c=c;self.router=None;self.head_hub=self;self.lock=threading.Lock()
        self.records={};self.commands={};self.workers=[];self.serial=0;self.stopping=False
        self.wake=threading.Event();self.watchdog=None;self.head_flow=None
        context=mp.get_context('spawn');settings=c['peer']['head_lanes']
        try:
            for index in range(settings['processes']):
                parent,child=context.Pipe()
                proc=context.Process(target=lane_main,args=(c,index,child),name=f'dhdgs-head-lane-{index}',daemon=True)
                w=dict(index=index,process=proc,conn=parent,queue=queue.Queue(c['peer']['max_active_queries']*2+8),
                       ready=threading.Event(),error=None,active=0)
                self.workers.append(w);proc.start();child.close()
                w['reader']=threading.Thread(target=self._read,args=(w,),name=f'dhdgs-lane-read-{index}',daemon=True)
                w['writer']=threading.Thread(target=self._write,args=(w,),name=f'dhdgs-lane-write-{index}',daemon=True)
                w['reader'].start();w['writer'].start()
            deadline=time.monotonic()+settings['startup_timeout_seconds']
            for w in self.workers:
                if not w['ready'].wait(max(0,deadline-time.monotonic())):raise TimeoutError('Head lane startup timeout')
                if w['error']:raise RuntimeError(w['error'])
            self.manifest=self.workers[0]['manifest']
            self.corpus_ids_contiguous=all(w['contiguous_ids'] for w in self.workers)
            if any(w['manifest']!=self.manifest for w in self.workers):raise ValueError('Head lane index mismatch')
            self.head_flow=LaneIngress(self)
            self.watchdog=threading.Thread(target=self._deadlines,name='dhdgs-lane-deadlines',daemon=True)
            self.watchdog.start()
        except BaseException:self.close();raise

    def _failed(self,w,error):
        with self.lock:
            if w['error']:return
            w['error']=f'{type(error).__name__}: {error}'
            items=[(k,r) for k,r in self.records.items() if r['worker'] is w]
            for k,r in items:self.records.pop(k,None)
            keys=[key for key in self.commands if key[0]==w['index']]
            commands=[self.commands.pop(key) for key in keys]
            w['active']=0
        if not self.stopping and w['process'].pid is not None and w['process'].is_alive():
            w['process'].terminate()
        w['ready'].set()
        for _,r in items:resolve(r['future'],error=RuntimeError('Head lane failed: '+str(error)))
        for f in commands:resolve(f,error=RuntimeError('Head lane failed: '+str(error)))

    def _write(self,w):
        try:
            while True:
                message=w['queue'].get()
                if message is None:return
                data=msgpack.packb(message,use_bin_type=True)
                if len(data)>self.c['network']['max_message_bytes']:raise ValueError('Head lane input exceeds IPC limit')
                w['conn'].send_bytes(data)
                if message['kind']=='stop':return
        except (OSError,ValueError) as exc:self._failed(w,exc)

    def _read(self,w):
        try:
            while True:
                receive_enter=time.monotonic_ns()
                frame=w['conn'].recv_bytes(self.c['network']['max_message_bytes'])
                receive_done=time.monotonic_ns();ipc={}
                if frame.startswith(IPC_MAGIC):
                    _,pack_enter,pack_done,lock_enter,lock_acquired=IPC_TIMING.unpack_from(frame)
                    ipc=dict(ipc_pack_enter_ns=pack_enter,ipc_pack_done_ns=pack_done,
                             ipc_write_lock_enter_ns=lock_enter,ipc_write_lock_acquired_ns=lock_acquired)
                    frame=memoryview(frame)[IPC_TIMING.size:]
                message=msgpack.unpackb(frame,raw=False,strict_map_key=False)
                now=time.monotonic_ns();kind=message['kind']
                if kind=='ready':
                    if not CLOCK_DOMAIN or message['clock_domain']!=CLOCK_DOMAIN:raise ValueError('Head lane clock domain mismatch')
                    w.update(manifest=message['manifest'],endpoint=message['endpoint'],contiguous_ids=message['contiguous_ids'],source_file=message['source_file']);w['ready'].set();continue
                if kind=='failed':raise RuntimeError(message['error'])
                if kind=='configured':
                    with self.lock:f=self.commands.pop((w['index'],message['key']),None)
                    if f is not None:resolve(f,error=RuntimeError(message['error']) if message.get('error') else None)
                    continue
                if kind!='result':raise ValueError('Invalid head lane response')
                with self.lock:
                    rec=self.records.get(message['key'])
                    if rec is not None:
                        if rec['worker'] is not w:raise ValueError('Wrong head lane result ownership')
                        self.records.pop(message['key']);w['active']-=1
                if rec is None:continue
                if message.get('error'):
                    exc=RuntimeError(message['error']);exc.dhdgs_metrics=message.get('context',{})
                    resolve(rec['future'],error=exc);continue
                result=message['result'];stats=result['stats'];start=rec['start']
                stats['head_lane']=dict(index=w['index'],submitted_ns=start,
                    worker_received_ns=stats.pop('head_lane_worker_received_ns'),
                    worker_finished_ns=message['worker_finished_ns'],parent_received_ns=now,
                    ipc_receive_enter_ns=receive_enter,ipc_receive_done_ns=receive_done,ipc_decode_done_ns=now,**ipc)
                t=stats['head_lane']
                parts=dict(input_queue_ipc_ms=(t['worker_received_ns']-start)/1e6,
                           worker_pipeline_ms=(t['worker_finished_ns']-t['worker_received_ns'])/1e6,
                           output_ipc_ms=(now-t['worker_finished_ns'])/1e6)
                stats['worker_latency_breakdown_ms']=stats['latency_breakdown_ms']
                stats['latency_breakdown_ms']=parts;stats['latency_ms']=(now-start)/1e6
                stats['latency_breakdown_fraction']={k:v/stats['latency_ms'] for k,v in parts.items()}
                resolve(rec['future'],result)
        except (EOFError,OSError,ValueError,KeyError,TypeError,RuntimeError) as exc:self._failed(w,exc)

    def search_async(self,vector,query_id=None):
        start=time.monotonic_ns();out=Future()
        try:
            raw=np.asarray(vector)
            if raw.shape!=(self.manifest['dimension'],):raise ValueError('Wrong search query dimension')
            if not np.isfinite(raw).all():raise ValueError('Query contains nonfinite values')
            if query_id is not None and (type(query_id)is not int or not 0<=query_id<2**64):raise ValueError('query_id must be uint64 or null')
            dtype='<f4' if raw.dtype==np.dtype('float32') else '<f8'
            payload=np.asarray(raw,dtype=dtype).tobytes();key=uuid.uuid4().hex
            with self.lock:
                if self.stopping:raise RuntimeError('Head lanes closed')
                if len(self.records)>=self.c['peer']['max_active_queries']:raise RuntimeError('Head lane query capacity exceeded')
                healthy=[w for w in self.workers if not w['error']]
                if not healthy:raise RuntimeError('No healthy head lanes')
                w=min(healthy,key=lambda w:(w['active'],(w['index']-self.serial)%len(self.workers)))
                self.serial=(w['index']+1)%len(self.workers)
                rec=dict(future=out,worker=w,start=start,deadline=time.monotonic()+self.c['peer']['query_timeout_seconds'],expired=False)
                self.records[key]=rec;w['active']+=1
                try:w['queue'].put_nowait(dict(kind='query',key=key,query_id=query_id,vector=payload,dtype=dtype))
                except queue.Full:self.records.pop(key);w['active']-=1;raise RuntimeError('Head lane command capacity exceeded')
            def cancelled(f):
                if f.cancelled():self._cancel(w,key)
            out.add_done_callback(cancelled);self.wake.set()
        except Exception as exc:resolve(out,error=exc)
        return out

    def search(self,vector,query_id=None):return self.search_async(vector,query_id).result()

    def _cancel(self,w,key):
        try:w['queue'].put_nowait(dict(kind='cancel',key=key))
        except queue.Full:
            # Do not forget accepted work: a failed lane loses its leases and
            # all affected queries fail explicitly. Never reissue search.
            self._failed(w,'Cancellation queue overflow');w['process'].terminate()

    def _deadlines(self):
        while not self.stopping:
            self.wake.clear();now=time.monotonic()
            with self.lock:
                due=[(k,r) for k,r in self.records.items() if not r['expired'] and r['deadline']<=now]
                for _,r in due:r['expired']=True
                deadline=min((r['deadline'] for r in self.records.values() if not r['expired']),default=now+1)
            for k,r in due:
                self._cancel(r['worker'],k)
                resolve(r['future'],error=TimeoutError('Global query deadline exceeded in head lane'))
            self.wake.wait(max(.001,min(1,deadline-time.monotonic())))

    def apply_configuration(self,c,router=None):
        with self.lock:
            if self.records:raise RuntimeError('Cannot reconfigure active head lanes')
            requests=[]
            for w in self.workers:
                if w['error']:raise RuntimeError(w['error'])
                key=uuid.uuid4().hex;f=Future();self.commands[w['index'],key]=f;requests.append(f)
                w['queue'].put_nowait(dict(kind='configure',key=key,config=c))
        for f in requests:f.result(self.c['peer']['head_lanes']['startup_timeout_seconds'])
        self.c=c

    def status(self):
        with self.lock:return [dict(index=w['index'],pid=w['process'].pid,role='head_lane',
            alive=w['process'].is_alive(),error=w['error'],active_queries=w['active'],endpoint=w.get('endpoint'),source_file=w.get('source_file')) for w in self.workers]

    def close(self):
        with self.lock:self.stopping=True;items=list(self.records.items())
        self.wake.set()
        if self.watchdog is not None:self.watchdog.join(2)
        if self.head_flow is not None:self.head_flow.close()
        for k,r in items:r['future'].cancel()
        for w in self.workers:
            try:w['queue'].put_nowait(dict(kind='stop'))
            except queue.Full:w['process'].terminate()
        for w in self.workers:
            p=w['process']
            if p.pid is not None:
                p.join(5)
                if p.is_alive():p.terminate();p.join(3)
            w['conn'].close()
            try:w['queue'].put_nowait(None)
            except queue.Full:pass
            for k in ('reader','writer'):
                if k in w:w[k].join(2)
