"""Single-process head: bounded admission, separate route/merge queues, no IPC."""
from collections import deque
from concurrent.futures import Future, CancelledError, ThreadPoolExecutor
import threading
import time


class AdaptiveIngress:
    """Lock-protected receive queue; scale only between configured 1..4 workers."""
    def __init__(self, settings, capacity, backlog=lambda:0):
        self.settings=settings;self.capacity=capacity;self.backlog=backlog
        self.cv=threading.Condition();self.queue=deque();self.workers=set();self.threads=[]
        self.closed=False;self.serial=0;self.peak_workers=0;self.peak_queue=0
        with self.cv:
            for _ in range(settings['receive_min']):self._spawn()

    def _spawn(self):
        self.threads=[t for t in self.threads if t.is_alive()]
        i=self.serial;self.serial+=1;self.workers.add(i)
        t=threading.Thread(target=self._run,args=(i,),name=f'dhdgs-head-receive-{i}',daemon=True)
        self.threads.append(t);self.peak_workers=max(self.peak_workers,len(self.workers));t.start()

    def submit(self,fn,*args):
        out=Future()
        with self.cv:
            if self.closed or len(self.queue)>=self.capacity:raise RuntimeError('Head receive queue capacity exceeded')
            self.queue.append((out,fn,args));self.peak_queue=max(self.peak_queue,len(self.queue))
            target=min(self.settings['receive_max'],self.settings['receive_min']+
                       max(len(self.queue),self.backlog())//self.settings['receive_backlog'])
            while len(self.workers)<target:self._spawn()
            self.cv.notify()
        return out

    def _run(self,i):
        idle=self.settings['receive_idle_ms']/1000
        last_busy=time.monotonic()
        try:
            while True:
                with self.cv:
                    if not self.queue:
                        if self.closed:self.workers.discard(i);return
                        excess=len(self.workers)>self.settings['receive_min']
                        self.cv.wait(max(.001,idle-(time.monotonic()-last_busy)) if excess else None)
                        if not self.queue:
                            if self.closed or (len(self.workers)>self.settings['receive_min'] and
                                    time.monotonic()-last_busy>=idle and self.backlog()<self.settings['receive_backlog']):
                                self.workers.discard(i);return
                            last_busy=time.monotonic()
                            continue
                    out,fn,args=self.queue.popleft()
                if out.set_running_or_notify_cancel():
                    try:out.set_result(fn(*args))
                    except BaseException as exc:out.set_exception(exc)
                last_busy=time.monotonic()
        finally:
            with self.cv:self.workers.discard(i)

    def status(self):
        with self.cv:return dict(workers=len(self.workers),queued=len(self.queue),
                                 peak_workers=self.peak_workers,peak_queue=self.peak_queue)

    def close(self):
        with self.cv:self.closed=True;self.cv.notify_all()
        for t in self.threads:t.join()


class PipelineFlow:
    """A suspended query owns a record, never a route or merge worker."""
    def __init__(self,c):
        p=c['peer']['head_pipeline'];self.capacity=c['peer']['max_active_queries']
        self.cv=threading.Condition();self.records={};self.closed=False;self.route_queued=0
        self.route=ThreadPoolExecutor(p['route_workers'],thread_name_prefix='dhdgs-route')
        self.merge=ThreadPoolExecutor(p['merge_workers'],thread_name_prefix='dhdgs-merge')
        self.ingress=AdaptiveIngress(p,self.capacity,lambda:self.route_queued)

    def submit(self,generator,timing=None):
        timing={} if timing is None else timing
        out=Future();rec=dict(generator=generator,out=out,waiting=None,cancel=False,timing=timing)
        timing['receive_enqueued_ns']=time.monotonic_ns()
        with self.cv:
            if self.closed or len(self.records)>=self.capacity:
                generator.close();out.set_exception(RuntimeError('Head query capacity exceeded'));return out
            self.records[id(rec)]=rec
        def cancelled(done):
            if done.cancelled():
                with self.cv:rec['cancel']=True;waiting=rec['waiting']
                if waiting is not None:waiting.cancel()
        out.add_done_callback(cancelled)
        try:self.ingress.submit(self._accept,rec)
        except BaseException as exc:
            generator.close();out.set_exception(exc)
            with self.cv:self.records.pop(id(rec),None);self.cv.notify_all()
        return out

    def _accept(self,rec):
        rec['timing']['receive_started_ns']=time.monotonic_ns()
        with self.cv:self.route_queued+=1
        rec['timing']['route_enqueued_ns']=time.monotonic_ns()
        self.route.submit(self._step,rec,None)

    def _resume(self,rec,operation):
        rec['timing']['merge_enqueued_ns']=time.monotonic_ns()
        self.merge.submit(self._step,rec,operation)

    def _step(self,rec,operation):
        g,out=rec['generator'],rec['out']
        if operation is None:
            with self.cv:self.route_queued-=1
            rec['timing']['route_started_ns']=time.monotonic_ns()
        else:rec['timing']['merge_started_ns']=time.monotonic_ns()
        try:
            if rec['cancel']:waiting=g.throw(CancelledError())
            elif operation is None:waiting=next(g)
            else:
                try:value=operation.result()
                except BaseException as exc:waiting=g.throw(exc)
                else:waiting=g.send(value)
            if not isinstance(waiting,Future):raise TypeError('Pipeline stage must yield Future')
            with self.cv:rec['waiting']=waiting;cancel=rec['cancel']
            waiting.add_done_callback(lambda f:self._resume(rec,f))
            if cancel:waiting.cancel()
        except StopIteration as done:
            # Completion callbacks may immediately submit the next query. Return
            # capacity before invoking them, including at max_active_queries=1.
            with self.cv:self.records.pop(id(rec),None);self.cv.notify_all()
            if out.set_running_or_notify_cancel():out.set_result(done.value)
        except BaseException as exc:
            try:g.close()
            finally:
                with self.cv:self.records.pop(id(rec),None);self.cv.notify_all()
                if out.set_running_or_notify_cancel():out.set_exception(exc)
        finally:
            if g.gi_frame is None:
                with self.cv:self.records.pop(id(rec),None);self.cv.notify_all()

    def close(self):
        with self.cv:self.closed=True;records=list(self.records.values())
        for r in records:r['out'].cancel()
        self.ingress.close()
        with self.cv:self.cv.wait_for(lambda:not self.records)
        self.route.shutdown();self.merge.shutdown()
