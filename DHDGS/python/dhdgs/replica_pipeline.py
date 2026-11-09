"""Versioned, bounded replica transfer. No search worker executes this module."""
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import hashlib
import os
from pathlib import Path
import queue
import shutil
import threading
import time


class PipelineReceiver:
    def __init__(self, service):
        self.service=service
        self.slots=threading.BoundedSemaphore(service.b.get('replica_write_workers',2))
        self.closed=threading.Event()
        self.janitor=threading.Thread(target=self.expire,name='replica-expiration',daemon=True)
        self.janitor.start()

    def begin(self, version, request):
        from .replicas import CONTRACT, digest, holds_reservation
        from .index import native
        s=self.service;b=s.b
        contract={k:s.owner.worker.meta[k] for k in CONTRACT}
        if request['contract']!=contract or request['centers_sha256']!=digest(s.owner.worker.directory/'centers.npy'):
            raise ValueError('Replica graph/quantization contract mismatch')
        amount=request['reserve_bytes']
        if type(amount)is not int or amount<=0:raise ValueError('Invalid replica reservation')
        fingerprint={k:request[k] for k in ('contract','centers_sha256','reserve_bytes')}
        order=request.get('record_order','sorted_v1')
        if order not in ('sorted_v1','admission_v1'):raise ValueError('Invalid replica record order')
        if order=='admission_v1':fingerprint['record_order']=order
        with s.lock:
            if version in s.states:
                if s.states[version].get('_begin')!=fingerprint:raise ValueError('Conflicting replica retry')
                return s.status(version)
            active=[x for x in s.states.values()if holds_reservation(x)]
            if len(active)>=b['max_replicas']:raise ValueError('Replica capacity exceeded')
            used=sum(x.get('reserved_bytes',x.get('manifest',{}).get('bytes',0))for x in active)
            meta=s.owner.worker.meta
            cap=int(b.get('max_target_shard_fraction',1.)*(meta['header_bytes']+meta['rows']*meta['record_bytes']))
            amount=min(amount,b['max_region_bytes'],cap)
            from .replica_budget import target_limit
            if used+amount>target_limit(b):raise ValueError('Total target replica byte budget exceeded')
            if amount<=0 or shutil.disk_usage(s.root).free<amount+b['reserve_disk_bytes']:raise ValueError('Replica destination lacks space')
            path=s.root/version;path.mkdir(exist_ok=False)
            state=dict(id=version,role='target',phase='RECEIVING',protocol=2,events=[],reserved_bytes=amount,
                received_bytes=0,declared_bytes=0,files={},fds={},blocks={},lock=threading.RLock(),cancel=threading.Event(),
                _writers={},_begin=fingerprint,_native_cancel=native().ReplicaCancel(b['operation_timeout_s']),
                _deadline=time.monotonic()+b['operation_timeout_s'],_cleaned=False,
                _growing=order=='admission_v1',_nodes_sealed=False,_node_lengths={})
            s.states[version]=state;s.event(state,'RECEIVING')
        return s.status(version)

    def open_file(self, version, name, length):
        from .replicas import FILES
        s=self.service;state=s.states[version]
        with state['lock']:
            self.receiving(state)
            growing=state['_growing'] and name=='nodes.bin'
            if name not in FILES or type(length)is not int or (length!=0 if growing else length<=0):
                raise ValueError('Invalid replica file declaration')
            if name in state['files']:
                if not growing and state['files'][name]!=length:raise ValueError('Conflicting replica file declaration')
                return dict(opened=True)
            if state['declared_bytes']+length>state['reserved_bytes']:raise ValueError('Replica reservation exceeded')
            code_bytes=sum(v for k,v in state['files'].items()if k!='nodes.bin')+(length if name!='nodes.bin' else 0)
            if code_bytes>s.b['max_code_bytes']:raise ValueError('Replica code dependency budget exceeded')
            fd=os.open(s.root/version/name,os.O_CREAT|os.O_EXCL|os.O_RDWR,0o600)
            try:
                if length:os.posix_fallocate(fd,0,length)
            except BaseException:
                os.close(fd);(s.root/version/name).unlink(missing_ok=True);raise
            state['files'][name]=length;state['fds'][name]=fd;state['declared_bytes']+=length
        return dict(opened=True)

    def receiving(self,state):
        if self.service.stop.is_set() or state['cancel'].is_set() or state['phase']!='RECEIVING' or time.monotonic()>state['_deadline']:
            raise ValueError('Replica not receiving/cancelled')

    def chunk(self,version,request):
        s=self.service;state=s.states[version];name=request['file'];off=request['offset'];raw=request['data'];claimed=request['sha256']
        if not isinstance(raw,bytes) or type(off)is not int or not raw:raise ValueError('Invalid replica chunk')
        # Hash outside state lock. The sender has a bounded in-flight window.
        checksum=hashlib.sha256(raw).hexdigest()
        if checksum!=claimed:raise ValueError('Replica chunk checksum mismatch')
        key=(name,off)
        with state['lock']:
            self.receiving(state);length=state['files'].get(name,0)
            growing=state['_growing'] and name=='nodes.bin' and name in state['files']
            if off<0 or off%s.b['chunk_bytes'] or (len(raw)>s.b['chunk_bytes'] if growing else
                    off>=length or len(raw)!=min(s.b['chunk_bytes'],length-off)):
                raise ValueError('Invalid replica chunk range')
            if key in state['blocks']:
                if state['blocks'][key]!=checksum:raise ValueError('Conflicting replica chunk')
                return dict(duplicate=True)
            if key in state['_writers']:
                if state['_writers'][key]!=checksum:raise ValueError('Conflicting in-flight replica chunk')
                return dict(pending=True)
            if growing:
                if state['_nodes_sealed']:raise ValueError('Growing replica already sealed')
                extra=max(0,off+len(raw)-length)
                if state['declared_bytes']+extra>state['reserved_bytes']:
                    raise ValueError('Growing replica reservation exceeded')
                state['declared_bytes']+=extra;state['files'][name]=max(length,off+len(raw))
                state['_node_lengths'][off]=len(raw)
            state['_writers'][key]=checksum;fd=state['fds'][name]
        acquired=False
        try:
            while not acquired:
                if state['cancel'].is_set() or s.stop.is_set() or time.monotonic()>state['_deadline']:raise RuntimeError('Replica write cancelled')
                acquired=self.slots.acquire(timeout=.05)
            view=memoryview(raw);written=0
            while written<len(raw):
                if state['cancel'].is_set() or s.stop.is_set():raise RuntimeError('Replica write cancelled')
                n=os.pwrite(fd,view[written:],off+written)
                if n<=0:raise OSError('Short replica write')
                written+=n
            with state['lock']:
                self.receiving(state);state['blocks'][key]=checksum;state['received_bytes']+=len(raw)
                state.setdefault('first_chunk_ns',time.monotonic_ns());state['last_chunk_ns']=time.monotonic_ns()
            return dict(duplicate=False,bytes=len(raw))
        finally:
            if acquired:self.slots.release()
            with state['lock']:state['_writers'].pop(key,None)

    def seal_nodes(self,version,length,header):
        from .index import HEADER, MAGIC
        s=self.service;state=s.states[version]
        with state['lock']:
            fingerprint=(length,header)
            if state.get('_node_seal') is not None:
                if state['_node_seal']!=fingerprint:raise ValueError('Conflicting growing seal')
                return dict(sealed=True)
            self.receiving(state)
            if not state['_growing'] or type(length)is not int or not isinstance(header,bytes):
                raise ValueError('Invalid growing seal')
            meta=s.owner.worker.meta
            if len(header)!=meta['header_bytes'] or len(header)<HEADER.size:
                raise ValueError('Invalid growing header')
            magic,ver,dim,rows,stride=HEADER.unpack_from(header)
            if (magic!=MAGIC or ver!=1 or dim!=meta['dimension'] or stride!=meta['record_bytes']
                    or not 0<rows<=s.b['max_region_rows'] or length!=len(header)+rows*stride):
                raise ValueError('Invalid growing record layout')
            if state['files'].get('nodes.bin')!=length or state['_writers']:
                raise ValueError('Growing replica transfer incomplete')
            for off in range(0,length,s.b['chunk_bytes']):
                if (('nodes.bin',off)not in state['blocks'] or
                        state['_node_lengths'].get(off)!=min(s.b['chunk_bytes'],length-off)):
                    raise ValueError('Growing replica missing/short chunk')
            done=0
            while done<len(header):
                n=os.pwrite(state['fds']['nodes.bin'],header[done:],done)
                if n<=0:raise OSError('Growing header short write')
                done+=n
            state['_nodes_sealed']=True;state['_node_seal']=fingerprint
            state['growth_sealed_ns']=time.monotonic_ns()
            return dict(sealed=True)

    def finish(self,version,manifest):
        s=self.service;state=s.states[version];s.check_manifest(manifest)
        with state['lock']:
            if 'manifest'in state:
                if state['manifest']!=manifest:raise ValueError('Conflicting replica commit')
                return s.status(version)
            self.receiving(state)
            if state['_growing'] and not state['_nodes_sealed']:raise ValueError('Growing replica not sealed')
            if manifest['contract']!=state['_begin']['contract'] or manifest['centers_sha256']!=state['_begin']['centers_sha256']:
                raise ValueError('Replica final contract mismatch')
            if {k:v['bytes']for k,v in manifest['files'].items()}!=state['files']:raise ValueError('Replica final file set mismatch')
            if state['_writers'] or state['received_bytes']!=manifest['bytes']:raise ValueError('Replica transfer incomplete')
            for name,length in state['files'].items():
                if any((name,o)not in state['blocks']for o in range(0,length,s.b['chunk_bytes'])):raise ValueError('Replica missing chunk')
            state['manifest']=manifest;state['reserved_bytes']=manifest['bytes']
            s.event(state,'SEALING');state['future']=s.jobs.submit(self.commit,state)
        return s.status(version)

    def commit(self,state):
        from .replicas import digest
        s=self.service;hashed=flushed=0.
        try:
            for name,info in state['manifest']['files'].items():
                if state['cancel'].is_set() or s.stop.is_set():raise RuntimeError('Replica commit cancelled')
                begin=time.monotonic()
                if digest(s.root/state['id']/name)!=info['sha256']:raise ValueError('Replica file checksum mismatch')
                hashed+=time.monotonic()-begin;begin=time.monotonic();os.fsync(state['fds'][name]);flushed+=time.monotonic()-begin
            with state['lock']:
                if state['cancel'].is_set() or s.stop.is_set():raise RuntimeError('Replica commit cancelled')
                for fd in state['fds'].values():os.close(fd)
                state['fds'].clear();s.event(state,'VERIFIED',hash_seconds=hashed,fsync_seconds=flushed)
                s.event(state,'WARMING')
            s.warm(state)
        except BaseException as e:
            with state['lock']:
                state['cancel'].set();state['_native_cancel'].cancel();s.event(state,'FAILED',error=str(e))

    def abort(self,version):
        state=self.service.states[version]
        with state['lock']:
            state['cancel'].set();state['_native_cancel'].cancel()
            self.service.owner.worker.native_search.retire_replica(version)
            if state['phase']not in ('FAILED','RETIRED'):self.service.event(state,'DRAINING')
        return dict(cancelled=True)

    def reap(self,version):
        s=self.service;state=s.states[version]
        with state['lock']:
            if state['phase']=='RETIRED':return dict(reclaimed=True)
            if state['phase']not in ('DRAINING','FAILED'):raise ValueError('Replica is still accepting')
            if state['_writers'] or (state.get('future')is not None and not state['future'].done()):return dict(reclaimed=False)
            if not s.owner.worker.native_search.replica_released(version):return dict(reclaimed=False)
            for fd in state['fds'].values():os.close(fd)
            state['fds'].clear();shutil.rmtree(s.root/version,ignore_errors=True);state['_cleaned']=True;s.event(state,'RETIRED')
        return dict(reclaimed=True)

    def expire(self):
        s=self.service
        while not self.closed.wait(.1):
            with s.lock:states=list(s.states.values())
            for state in states:
                if state.get('protocol')!=2 or state.get('_cleaned'):continue
                with state['lock']:
                    if state['phase']in ('RECEIVING','SEALING','VERIFIED','WARMING') and time.monotonic()>state['_deadline']:
                        state['cancel'].set();state['_native_cancel'].cancel();s.event(state,'FAILED',error='Replica lifecycle deadline exceeded')
                    if state['phase']not in ('FAILED','DRAINING') or state['_writers']:continue
                    future=state.get('future')
                    if future is not None and not future.done():continue
                    if not s.owner.worker.native_search.replica_released(state['id']):continue
                    for fd in state['fds'].values():os.close(fd)
                    state['fds'].clear();shutil.rmtree(s.root/state['id'],ignore_errors=True);state['_cleaned']=True
                    if state['phase']=='DRAINING':s.event(state,'RETIRED')
            s.trim_terminal()

    def close(self):
        self.closed.set();self.janitor.join()


def send_pipeline(service,state,*,produce_package=None,growing=False):
    """Native record extraction produces immutable spool ranges; one sender drains."""
    from .replicas import package, CONTRACT, digest
    s=service;b=state.get('_pack_settings',s.b);plan=state['plan'];target=s.c['cluster']['nodes'][plan['target']]
    path=s.root/(state['id']+'_source');started=time.monotonic();deadline=started+b['operation_timeout_s'];cancel=threading.Event()
    q=queue.Queue(maxsize=max(1,b.get('producer_queue_bytes',67108864)//b['chunk_bytes']))
    peak=0;first=last=0;producer_done=0;blocked=0.;sent=0;pending=deque();throttled=checks=0;next_send=check_at=started;pressure=False
    def stopped():return cancel.is_set() or s.stop.is_set() or state.get('_cancel',cancel).is_set() or time.monotonic()>deadline
    def reliable(op,**fields):
        # Retry only missing transport replies. A remote validation error is final.
        while not stopped():
            try:return s.rpc(target,op,**fields)
            except (TimeoutError,ConnectionError,OSError):cancel.wait(.01)
        raise TimeoutError('Replica maintenance deadline exceeded: '+op)
    def emit(event):
        nonlocal peak,blocked
        begin=time.monotonic()
        while True:
            if stopped():raise RuntimeError('Replica producer cancelled/deadline exceeded')
            try:q.put(event,timeout=.05);break
            except queue.Full:pass
        blocked+=time.monotonic()-begin;peak=max(peak,q.qsize())
    positions={};lengths={}
    def progress(total,ready):
        name='nodes.bin'
        if name not in positions:
            positions[name]=0;lengths[name]=total
            emit(('open',name,0 if growing else total))
        end=ready if total and ready==total else ready//b['chunk_bytes']*b['chunk_bytes']
        while positions[name]<end:
            off=positions[name];n=min(b['chunk_bytes'],end-off);emit(('chunk',name,off,n));positions[name]+=n
        if growing and total and ready==total:
            with (path/name).open('rb') as f:header=f.read(s.owner.worker.meta['header_bytes'])
            emit(('seal',total,header))
    def complete_file(name):
        if name=='nodes.bin':return
        size=(path/name).stat().st_size;emit(('open',name,size))
        for off in range(0,size,b['chunk_bytes']):emit(('chunk',name,off,min(b['chunk_bytes'],size-off)))
    def produce():
        nonlocal producer_done
        try:
            m=(produce_package(path,stopped,progress,complete_file) if produce_package else
               package(s.owner.worker,plan['ids'],plan['entries'],path,b,stopped,progress,complete_file))
            producer_done=time.monotonic_ns()
            s.event(state,'TRANSFERRING',manifest=m,producer_done_ns=producer_done)
            emit(('done',m))
        except BaseException as e:
            import traceback
            state['_producer_traceback']=traceback.format_exc()
            try:emit(('error',e))
            except BaseException:pass
            raise
    def request_chunk(name,off,raw):
        checksum=hashlib.sha256(raw).hexdigest()
        while not stopped():
            try:
                reply=reliable('pipeline_chunk',id=state['id'],file=name,offset=off,data=raw,sha256=checksum)
                if not reply.get('pending'):
                    if growing:
                        ack=time.monotonic_ns()
                        with s.lock:
                            state.setdefault('first_data_ack_ns',ack)
                            state['last_data_ack_ns']=ack
                            if ack<state.get('admission_finished_ns',ack+1):
                                state['acknowledged_bytes_before_admission_close']=state.get('acknowledged_bytes_before_admission_close',0)+len(raw)
                    return reply
            except (TimeoutError,ConnectionError,OSError):
                pass # Same version/file/range/hash; receiver is idempotent.
            cancel.wait(.01)
        raise TimeoutError('Replica chunk deadline exceeded')
    executor=None;producer=None;producer_pool=None;fds={};begun=False
    try:
        begun=True # Even a lost begin ACK must eventually abort its reservation.
        reliable('pipeline_begin',id=state['id'],contract={k:s.owner.worker.meta[k]for k in CONTRACT},
              centers_sha256=digest(s.owner.worker.directory/'centers.npy'),reserve_bytes=state.get('_pack_settings',b)['max_region_bytes'],
              **({'record_order':'admission_v1'} if growing else {}))
        producer_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='replica-producer')
        try:
            producer=producer_pool.submit(produce)
            with ThreadPoolExecutor(max_workers=b['inflight_chunks'],thread_name_prefix='replica-stream')as executor:
                try:
                    while True:
                        if stopped():raise TimeoutError('Replica stream deadline exceeded/cancelled')
                        try:event=q.get(timeout=.05)
                        except queue.Empty:
                            if producer.done():producer.result();raise RuntimeError('Replica producer ended without manifest')
                            continue
                        if event[0]=='error':raise event[1]
                        if event[0]=='done':manifest=event[1];break
                        if event[0]=='seal':
                            for future in pending:future.result()
                            pending.clear()
                            reliable('pipeline_seal_nodes',id=state['id'],bytes=event[1],header=event[2])
                            continue
                        _,name,*args=event
                        if event[0]=='open':
                            reliable('pipeline_open',id=state['id'],file=name,bytes=args[0]);fds[name]=os.open(path/name,os.O_RDONLY)
                            if 'transfer_started_ns'not in state:s.event(state,'TRANSFERRING',transfer_started_ns=time.monotonic_ns())
                            continue
                        off,n=args
                        if len(pending)>=b['inflight_chunks']:pending.popleft().result()
                        now=time.monotonic()
                        if now>=check_at:
                            local=s.owner.peer.runtime.network_metrics()['compute_queue']['queued']
                            remote=s.rpc(target,'load')['runtime']['compute_queue']['queued']
                            pressure=max(local if b.get('copy_pressure_source_queue',True)else 0,remote)>=b['copy_pressure_queue_tasks']
                            checks+=1;check_at=time.monotonic()+b['copy_pressure_check_ms']/1000
                        factor=b['copy_pressure_rate_factor']if pressure else 1.
                        next_send=(s.growth_rate_slot(n,factor) if growing else max(next_send,now)+n/(b['copy_mib_per_s']*1048576*factor))
                        if cancel.wait(max(0,next_send-time.monotonic())):raise RuntimeError('Replica stream cancelled')
                        raw=os.pread(fds[name],n,off)
                        if len(raw)!=n:raise ValueError('Replica spool short read')
                        first=first or time.monotonic_ns();last=time.monotonic_ns();sent+=n;throttled+=n if pressure else 0
                        pending.append(executor.submit(request_chunk,name,off,raw))
                    for f in pending:f.result()
                    producer.result()
                except BaseException:
                    cancel.set();raise
            streamed=time.monotonic()
            s.event(state,'TRANSFERRING',manifest=manifest)
            reliable('pipeline_finish',id=state['id'],manifest=manifest)
            s.event(state,'SENT',sent_bytes=sent,transfer_seconds=time.monotonic()-started,stream_seconds=streamed-started,
                commit_seconds=time.monotonic()-streamed,schedule_install_seconds=0.,foreground_pressure_checks=checks,throttled_bytes=throttled,
                producer_done_ns=producer_done,first_chunk_ns=first,last_chunk_ns=last,producer_backpressure_seconds=blocked,
                producer_queue_peak=peak,pipeline=True,growing=growing)
        finally:
            cancel.set()
            producer_pool.shutdown(wait=True,cancel_futures=True)
    except BaseException as e:
        cancel.set();s.event(state,'FAILED',error=str(e))
        if begun:
            try:s.rpc(target,'pipeline_abort',id=state['id'])
            except BaseException:pass
    finally:
        cancel.set()
        for fd in fds.values():os.close(fd)
        if path.exists():shutil.rmtree(path)
