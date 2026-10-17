"""Bounded node-local record reads. No search policy or network dependencies.

One reactor owns admission, tokens and completion accounting. Each query has at
most one selected round; round buffers stay reserved until CPU consumption.
"""
from concurrent.futures import Future, ThreadPoolExecutor
from collections import deque, Counter
import os
import select
import threading
import time


class ThreadReader:
    def __init__(self, fd, workers, notify=lambda:None):
        self.fd=os.dup(fd);self.notify=notify
        self.pool=ThreadPoolExecutor(max_workers=workers,thread_name_prefix='dhdgs-io')
        self.pending={}
        self.submission_calls=0

    def submit(self, requests):
        self.submission_calls+=1
        for token,offset,length in requests:
            self.pending[token]=self.pool.submit(self._read,offset,length)
            self.pending[token].add_done_callback(lambda _:self.notify())

    def _read(self, offset, length):
        start=time.perf_counter()
        try:
            raw=os.pread(self.fd,length,offset)
            return raw,('' if len(raw)==length else 'Short node read'),(time.perf_counter()-start)*1000
        except OSError as exc:
            return b'',str(exc),(time.perf_counter()-start)*1000

    def poll(self, limit):
        result=[]
        for token,future in list(self.pending.items()):
            if future.done():
                result.append((token,*future.result()));del self.pending[token]
                if len(result)>=limit:break
        return result

    def close(self):
        self.pool.shutdown(wait=True);os.close(self.fd)


class ReadBatch:
    def __init__(self, scheduler, job):
        self.records=job['records'];self.stats=job['stats'];self.trace=job['trace'];self.job=job
        self.scheduler=scheduler;self.released=False

    def release(self):
        if not self.released:
            self.released=True
            with self.scheduler.cv:
                if self.scheduler.stopping or self.scheduler.fatal:
                    self.records.clear();return
                self.scheduler.commands.append(('release',self.job))
                self.scheduler._notify()


class LocalIO:
    def __init__(self, shard, config, trace=False):
        self.shard,self.c=shard,config
        self.trace=trace;self.batch_id=0
        self.late_reads=deque(maxlen=config['max_pending_batches'])
        self.stride=shard.meta['record_bytes']
        if self.stride>config['max_inflight_bytes_per_device']:
            raise ValueError('I/O byte quota cannot hold one record')
        self.wakeup_fd=os.eventfd(0,os.EFD_NONBLOCK|os.EFD_CLOEXEC)
        self.fallback_reason=None
        self.backend=config['backend']
        if self.backend=='io_uring':
            try:
                from .index import native
                core=native()
                if not core.io_uring_compiled:raise RuntimeError('liburing was not compiled')
                self.reader=core.AsyncReader(shard.fd,config['max_inflight_per_device'],self.wakeup_fd)
            except Exception as exc:
                if config['fallback_backend']!='thread_pool':
                    os.close(self.wakeup_fd);raise
                self.backend='thread_pool';self.fallback_reason=str(exc)
        if self.backend=='thread_pool':
            self.reader=ThreadReader(shard.fd,config['worker_threads'],self._notify)
        self.cv=threading.Condition()
        self.commands=deque();self.jobs=[];self.inflight={};self.token=0
        self.live=0;self.reserved=0;self.stopping=False;self.fatal=None
        self.counts=Counter();self.batch_sizes=Counter();self.batch_queries=Counter()
        self.thread=threading.Thread(target=self._run,name='dhdgs-io-reactor',daemon=True)
        self.thread.start()

    def _notify(self):
        try:os.eventfd_write(self.wakeup_fd,1)
        except BlockingIOError:pass # An unread notification already guarantees wakeup.

    def submit(self, ids, epoch, query_key, cancelled):
        future=Future();future.set_running_or_notify_cancel();ids=list(dict.fromkeys(map(int,ids)))
        reserve=2*len(ids)*self.stride  # includes copy from native buffer to Python bytes
        if epoch!=self.shard.meta['epoch']:
            future.set_exception(ValueError('Index epoch mismatch'));return future
        if reserve>self.c['buffer_pool_bytes_per_device']:
            future.set_exception(ValueError('Buffer budget cannot hold a complete selected round'));return future
        job={'future':future,'key':query_key,'ids':ids,'pending':deque(ids),'records':{},'inflight':0,
             'reserve':reserve,'admitted':None,'start':time.perf_counter(),'cancelled':cancelled,
             'error':None,'released':False,'trace':None,'stats':dict(file_read_bytes=0,pread_calls=0,cache_hits=0,
                 pread_ms=0.,async_read_requests=0,io_read_residence_ms=0.,io_queue_wait_ms=0.)}
        with self.cv:
            if self.stopping or self.fatal:
                future.set_exception(RuntimeError(self.fatal or 'I/O scheduler closed'))
            elif self.live>=self.c['max_pending_batches']:
                future.set_exception(RuntimeError('I/O batch capacity exceeded'))
            else:
                self.batch_id+=1
                if self.trace:job['trace']={'batch_id':self.batch_id,'query_branch_key':query_key,
                    'epoch':epoch,'ids':ids,'start_ns':time.perf_counter_ns(),'reads':[]}
                self.live+=1;self.commands.append(('submit',job));self._notify()
        return future

    def _fail(self, job, exc):
        if job['error'] is None:job['error']=exc
        job['pending'].clear()
        if not job['future'].done():job['future'].set_exception(job['error'])
        # Already-delivered batches remain CPU-owned until release().
        if job['future'].cancelled() or job['future'].exception() is not None:job['released']=True

    def _iteration(self):
        with self.cv:
            commands=list(self.commands);self.commands.clear()
        for op,job in commands:
            if op=='submit':self.jobs.append(job)
            else:job['released']=True
        for job in self.jobs:
            if self.stopping or job['cancelled']() or job['future'].cancelled():
                self._fail(job,TimeoutError('Query cancelled during local I/O'))
        events=self.reader.poll(self.c['max_completion_events_per_poll'])
        for token,raw,error,read_ms in events:
            job,gid,submitted=self.inflight.pop(token);job['inflight']-=1
            if job['trace'] is not None:
                job['trace']['reads'].append({'request_token':token,'gid':gid,
                    'offset':self.shard.meta['header_bytes']+self.shard.location[gid]*self.stride,
                    'requested_bytes':self.stride,'returned_bytes':len(raw),'submit_ns':int(submitted*1e9),
                    'completion_ns':time.perf_counter_ns(),'error':error})
            stats=job['stats'];stats['file_read_bytes']+=len(raw)
            stats['io_read_residence_ms']+=(time.perf_counter()-submitted)*1000
            self.counts['completed_reads']+=1;self.counts['file_read_bytes']+=len(raw)
            if self.backend=='thread_pool':stats['pread_ms']+=read_ms
            if job['error'] is not None:
                self.counts['late_read_bytes']+=len(raw)
                self.late_reads.append({'query_branch_key':job['key'],'request_token':token,
                                       'gid':gid,'bytes':len(raw),'error':error})
            elif error:
                self._fail(job,OSError(error))
            else:
                job['records'][gid]=raw;self.shard.cache_record(gid,raw)
        for job in list(self.jobs):
            if job['released'] and not job['inflight']:
                if job['admitted'] is not None:self.reserved-=job['reserve']
                job['records'].clear();self.jobs.remove(job)
                with self.cv:self.live-=1
        for job in self.jobs:
            if job['error'] or job['future'].done():continue
            if job['admitted'] is None:
                if self.reserved+job['reserve']>self.c['buffer_pool_bytes_per_device']:continue
                job['admitted']=time.perf_counter();self.reserved+=job['reserve']
                job['stats']['io_capacity_wait_ms']=(job['admitted']-job['start'])*1000
                remaining=deque()
                for gid in job['pending']:
                    try:raw=self.shard.cached_record(gid)
                    except Exception as exc:self._fail(job,exc);break
                    if raw is None:remaining.append(gid)
                    else:job['records'][gid]=raw;job['stats']['cache_hits']+=1
                if job['error'] is None:job['pending']=remaining
            if not job['pending'] and not job['inflight'] and not job['error']:
                job['stats']['io_batch_wall_ms']=(time.perf_counter()-job['start'])*1000
                if job['trace'] is not None:job['trace']['ready_ns']=time.perf_counter_ns()
                job['future'].set_result(ReadBatch(self,job))
        requests=[];query_keys=set()
        # All records have the same stride. One read per round-robin turn is
        # byte-fair DRR with quantum=stride, without a second configurable policy.
        scanned=0
        while self.jobs and len(requests)<self.c['max_submit_requests']:
            if len(self.inflight)>=self.c['max_inflight_per_device']:break
            if (len(self.inflight)+1)*self.stride>self.c['max_inflight_bytes_per_device']:break
            job=self.jobs.pop(0);self.jobs.append(job)
            eligible=(job['admitted'] is not None and job['pending'] and not job['error']
                      and job['inflight']<self.c['max_inflight_per_query'])
            if not eligible:
                scanned+=1
                if scanned>=len(self.jobs):break
                continue
            scanned=0;gid=job['pending'].popleft();self.token+=1
            now=time.perf_counter();self.inflight[self.token]=(job,gid,now);job['inflight']+=1
            job['stats']['io_queue_wait_ms']+=(now-job['admitted'])*1000
            job['stats']['async_read_requests' if self.backend=='io_uring' else 'pread_calls']+=1
            requests.append((self.token,self.shard.meta['header_bytes']+self.shard.location[gid]*self.stride,self.stride))
            query_keys.add(job['key'])
        if requests:
            self.reader.submit(requests)
            self.batch_sizes[len(requests)]+=1;self.batch_queries[len(query_keys)]+=1
            self.counts['submission_batches']+=1
        self.counts['peak_inflight_requests']=max(self.counts['peak_inflight_requests'],len(self.inflight))
        self.counts['peak_reserved_bytes']=max(self.counts['peak_reserved_bytes'],self.reserved)
        held=sum(len(j['records'])*self.stride for j in self.jobs if j['future'].done() and not j['released'])
        self.counts['peak_completed_unconsumed_bytes']=max(self.counts['peak_completed_unconsumed_bytes'],held)
        # Immediately submit more eligible work; otherwise sleep only until next
        # poll or producer/release notification, never wait to fill a batch.
        return bool(requests or events)

    def _run(self):
        try:
            while True:
                progress=self._iteration()
                with self.cv:
                    if self.stopping and not self.inflight:break
                    wait=not progress and not self.commands
                if wait:
                    select.select([self.wakeup_fd],[],[],self.c['completion_poll_ms']/1000 if self.jobs else None)
                    try:os.eventfd_read(self.wakeup_fd)
                    except BlockingIOError:pass
        except Exception as exc:
            with self.cv:
                self.fatal=f'{type(exc).__name__}: {exc}'
                commands=list(self.commands);self.commands.clear()
            for op,job in commands:
                if op=='submit':self.jobs.append(job)
            for job in self.jobs:self._fail(job,RuntimeError(self.fatal))
        finally:
            self.reader.close()  # owns a duplicate fd; outstanding writes finish before buffers die
            with self.cv:
                self.inflight.clear();self.jobs.clear();self.live=0;self.reserved=0
                os.close(self.wakeup_fd)

    def metrics(self):
        with self.cv:
            return {'actual_backend':self.backend,'fallback_reason':self.fallback_reason,'fatal_error':self.fatal,
                    'late_reads_recent':list(self.late_reads),
                    'scope':'one data-node process; allocate shared-device quotas across processes',
                    'active_batches':self.live,'reserved_bytes':self.reserved,
                    'inflight_requests':len(self.inflight),'counters':dict(self.counts),
                    'submission_batch_sizes':{str(k):v for k,v in self.batch_sizes.items()},
                    'submission_query_counts':{str(k):v for k,v in self.batch_queries.items()},
                    'backend_submission_calls':self.reader.submission_calls}

    def close(self):
        with self.cv:
            if not self.stopping:
                self.stopping=True
                if self.thread.is_alive() and self.fatal is None:self._notify()
        self.thread.join()
