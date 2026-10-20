"""Single-owner branch coroutine driver; I/O callbacks only enqueue CPU work."""
import time
import threading
from .peer_search import branch_steps


class BranchTask:
    def __init__(self, runtime, key):
        self.runtime=runtime;self.key=key;self.rec=runtime.records[key];self.started=self.rec['created_perf']
        self.generator=branch_steps(self.rec['state'],runtime.shard,runtime.directory,
                                    runtime.c,self.spawn,self.cancelled)
        self.request=None;self.cpu_ms=0.;self.ready_ms=0.;self.slices=0
        self.slice_wall_ms=0.;self.io_wall_ms=0.;self.pending_transfers=0
        self.report=None;self.error=None;self.compute_done=False;self.finalized=False
        self.transfer_wait_start=None;self.done=threading.Event()

    def cancelled(self):
        r=self.rec
        return (self.runtime.stop.is_set() or r['cancelled'] or self.error is not None
                or time.monotonic()>=min(r['lease'],r['deadline']))

    def enqueue(self, future=None):
        queued=time.perf_counter()
        # A completion cannot run the generator inline on the reactor thread.
        self.runtime.workers.submit(self.step,future,queued)

    def spawn(self, state, descriptor):
        if not state.get('dispatch_decision_id') or state['dispatch_decision_id']!=descriptor.get('decision_id'):
            raise ValueError('Remote branch lacks an approved search decision')
        with self.runtime.lock:
            self.rec['children'].append(descriptor);self.pending_transfers+=1
        self.runtime._control_changed(self.key)
        try:future=self.runtime.submit_transfer(self.key,state)
        except Exception:
            with self.runtime.lock:self.pending_transfers-=1
            raise
        future.add_done_callback(self.transferred)

    def transferred(self, future):
        with self.runtime.lock:
            self.pending_transfers-=1
            try:future.result()
            except Exception as exc:self.error=exc
            self._finish_locked()

    def _finish_locked(self):
        if self.finalized or not self.compute_done or self.pending_transfers:return
        self.finalized=True
        if self.error is not None:
            self.rec['error']=f'{type(self.error).__name__}: {self.error}';self.rec['status']='failed'
        else:
            self.report['peer_transfer']={k:v for k,v in self.rec.items() if k.startswith('peer_')}
            self.report['execution']={'model':'resumable','io_backend':self.runtime.io.backend,
                                     'fallback_reason':self.runtime.io.fallback_reason}
            self.report['stats'].update(server_thread_cpu_ms=self.cpu_ms,query_ready_queue_ms=self.ready_ms,
                query_admission_queue_ms=self.rec['query_admission_queue_ms'],
                query_executor_start_wait_ms=self.rec['query_executor_start_wait_ms'],
                query_compute_slice_wall_ms=self.slice_wall_ms,query_io_wait_ms=self.io_wall_ms,
                query_resume_count=self.slices,
                query_transfer_finalize_wait_ms=(time.perf_counter()-self.transfer_wait_start)*1000)
            st=self.report['stats']
            st['query_branch_wall_ms']=(time.perf_counter()-self.started)*1000
            st['query_state_other_ms']=max(0.,st['query_branch_wall_ms']-sum(st[k] for k in (
                'query_ready_queue_ms','query_compute_slice_wall_ms','query_io_wait_ms','query_transfer_finalize_wait_ms')))
            self.rec['report']=self.report;self.rec['status']='complete'
        self.rec['touched']=time.monotonic();self.runtime._control_changed(self.key);self.done.set()

    def step(self, future, queued):
        start=time.perf_counter();cpu=time.thread_time();self.ready_ms+=(start-queued)*1000
        if self.slices==0:
            self.runtime._execution_started(self.key,time.monotonic())
            self.ready_ms+=max(0.,queued-self.started)*1000
        self.slices+=1;batch=None;next_future=None
        with self.runtime.lock:self.rec['status']='running'
        try:
            if future is not None:
                self.io_wall_ms+=(queued-self.io_started)*1000
                batch=future.result()
            if self.cancelled():raise TimeoutError('Query cancelled before CPU resume')
            reply=None
            if batch is not None:
                q,ids,epoch=self.request
                reply=self.runtime.shard.expand(q,ids,epoch,records=batch.records,read_stats=batch.stats)
                if batch.trace is not None:reply['io_trace']=batch.trace
                batch.release();batch=None
            self.request=self.generator.send(reply)
            _,ids,epoch=self.request
            next_future=self.runtime.io.submit(ids,epoch,self.key,self.cancelled)
            with self.runtime.lock:self.rec['status']='waiting_io'
        except StopIteration as done:
            self.report=done.value
        except Exception as exc:
            self.error=exc
            self.generator.close()
        finally:
            if batch is not None:batch.release()
            self.cpu_ms+=(time.thread_time()-cpu)*1000
            self.slice_wall_ms+=(time.perf_counter()-start)*1000
        # All actor mutations finish before registering an immediately-ready callback.
        if next_future is not None:
            self.io_started=time.perf_counter()
            next_future.add_done_callback(self.enqueue)
        else:
            with self.runtime.lock:
                self.compute_done=True
                self.transfer_wait_start=time.perf_counter()
                self.rec['status']='waiting_transfer'
                self._finish_locked()
