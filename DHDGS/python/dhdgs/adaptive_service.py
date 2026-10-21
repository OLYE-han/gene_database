"""Bounded adaptive workers for control services, never for search computation."""
from collections import deque
from concurrent.futures import Future
import math
import threading
import time


class AdaptiveServicePool:
    """Age-triggered, bounded service parallelism with slow idle retirement.

    downstream_ready must be a cheap snapshot, with no acquisition of this
    pool's lock. Worker callbacks and the predicate run outside the queue lock.
    A running task is never cancelled by downscaling. Only already approved
    tasks can enter this pool; scaling cannot create graph expansion work.
    """
    def __init__(self, settings, minimum, maximum, capacity, name, downstream_ready=lambda: True):
        self.settings=settings;self.minimum=minimum;self.maximum=maximum
        self.capacity=capacity;self.name=name;self.downstream_ready=downstream_ready
        self.cv=threading.Condition();self.queue=deque();self.threads={};self.serial=0
        self.closed=False;self.active=0;self.completed=0;self.arrivals=0
        self.peak_queued=0;self.peak_workers=0;self.rejected=0;self.callback_errors=0
        self.target=minimum;self.service_ewma=0.;self.last_busy=time.monotonic()
        self.events=deque(maxlen=settings['history_size']);self.samples=0;self.blocked_growth=0
        self.halt=threading.Event()
        with self.cv:
            for _ in range(minimum):self._spawn_locked()
        self.monitor=threading.Thread(target=self._monitor,name=name+'-governor',daemon=True)
        self.monitor.start()

    def _spawn_locked(self):
        i=self.serial;self.serial+=1
        t=threading.Thread(target=self._run,args=(i,),name=f'{self.name}-{i}',daemon=True)
        self.threads[i]=t
        try:t.start()
        except BaseException:
            self.threads.pop(i,None)
            raise
        self.peak_workers=max(self.peak_workers,len(self.threads))

    def submit(self, fn, *args, **kwargs):
        f=Future()
        with self.cv:
            if self.closed:raise RuntimeError('Adaptive service closed')
            if len(self.queue)>=self.capacity:
                self.rejected+=1;raise RuntimeError('Adaptive service queue capacity exceeded')
            self.queue.append((time.monotonic(),f,fn,args,kwargs));self.arrivals+=1
            self.peak_queued=max(self.peak_queued,len(self.queue));self.last_busy=time.monotonic()
            self.cv.notify()
        return f

    def _run(self,i):
        try:
            while True:
                with self.cv:
                    self.cv.wait_for(lambda:self.queue or self.closed or len(self.threads)>self.target)
                    if not self.queue:
                        if self.closed or len(self.threads)>self.target:
                            self.threads.pop(i,None);return
                        continue
                    _,f,fn,args,kwargs=self.queue.popleft();self.active+=1
                started=time.monotonic()
                try:
                    if f.set_running_or_notify_cancel():
                        try:value=fn(*args,**kwargs)
                        except BaseException as exc:f.set_exception(exc)
                        else:f.set_result(value)
                except BaseException:
                    with self.cv:self.callback_errors+=1
                finally:
                    elapsed=time.monotonic()-started
                    with self.cv:
                        self.active-=1;self.completed+=1;self.last_busy=time.monotonic()
                        alpha=self.settings['ewma_weight']
                        self.service_ewma=(alpha*elapsed+(1-alpha)*self.service_ewma if self.service_ewma else elapsed)
        finally:
            with self.cv:self.threads.pop(i,None);self.cv.notify_all()

    def _monitor(self):
        period=self.settings['sample_interval_ms']/1000
        cooldown=self.settings['cooldown_ms']/1000
        target_wait=self.settings['target_wait_ms']/1000
        idle=self.settings['downscale_idle_ms']/1000
        last_change=last_sample=time.monotonic();last_arrivals=0
        while not self.halt.wait(period):
            ready=self.downstream_ready()
            now=time.monotonic()
            with self.cv:
                if self.closed:return
                self.samples+=1
                rate=(self.arrivals-last_arrivals)/max(now-last_sample,1e-9)
                last_arrivals=self.arrivals;last_sample=now
                q=len(self.queue);age=now-self.queue[0][0] if q else 0.
                old=self.target;reason=None
                if now-last_change>=cooldown:
                    if q and age>target_wait and self.active>=len(self.threads):
                        if ready and self.target<self.maximum:
                            needed=math.ceil((rate+q/target_wait)*self.service_ewma/self.settings['target_utilization'])
                            # Bounded additive probe prevents a single burst
                            # from immediately consuming the entire allowance.
                            self.target=min(self.maximum,max(old+1,min(needed,old+self.settings['growth_step'])))
                            reason='oldest_wait'
                        elif not ready:self.blocked_growth+=1
                    elif not q and not self.active and now-self.last_busy>=idle and self.target>self.minimum:
                        self.target-=1;reason='idle'
                if self.target!=old:
                    last_change=now
                    self.events.append(dict(monotonic_ns=time.monotonic_ns(),old=old,target=self.target,
                        reason=reason,queued=q,oldest_wait_ms=age*1000,active=self.active,
                        completed=self.completed,arrival_rate=rate,service_ewma_ms=self.service_ewma*1000))
                    while len(self.threads)<self.target:self._spawn_locked()
                    self.cv.notify_all()

    def metrics(self):
        with self.cv:
            return dict(workers=len(self.threads),target_workers=self.target,min_workers=self.minimum,
                max_workers=self.maximum,active=self.active,queued=len(self.queue),queue_capacity=self.capacity,
                peak_queued=self.peak_queued,peak_workers=self.peak_workers,capacity_rejections=self.rejected,
                completed=self.completed,arrivals=self.arrivals,callback_errors=self.callback_errors,
                service_ewma_ms=self.service_ewma*1000,closed=self.closed,samples=self.samples,
                blocked_growth=self.blocked_growth,events=list(self.events))

    def shutdown(self,wait=True,*,cancel_futures=False):
        cancelled=[]
        with self.cv:
            self.closed=True;self.halt.set()
            if cancel_futures:
                while self.queue:cancelled.append(self.queue.popleft()[1])
            threads=list(self.threads.values());self.cv.notify_all()
        for f in cancelled:
            try:f.cancel()
            except BaseException:
                with self.cv:self.callback_errors+=1
        if wait:
            if threading.current_thread() is not self.monitor:self.monitor.join()
            for t in threads:
                if t is not threading.current_thread():t.join()
