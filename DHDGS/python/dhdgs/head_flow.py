"""Bounded query continuations: workers run ready stages, never wait on IPC."""
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
import threading


class QueryFlow:
    def __init__(self,workers,capacity):
        self.pool=ThreadPoolExecutor(max_workers=workers,thread_name_prefix='dhdgs-head-stage')
        self.capacity=capacity;self.cv=threading.Condition();self.records={};self.closed=False

    def submit(self,generator):
        output=Future()
        rec=dict(generator=generator,output=output,waiting=None,cancel=False)
        with self.cv:
            if self.closed or len(self.records)>=self.capacity:
                generator.close();output.set_exception(RuntimeError('Head query stage capacity exceeded'));return output
            self.records[id(rec)]=rec
        def cancel(done):
            if not done.cancelled():return
            with self.cv:
                rec['cancel']=True;waiting=rec['waiting']
            if waiting is not None:waiting.cancel()
        output.add_done_callback(cancel)
        self.pool.submit(self._step,rec,None)
        return output

    def _step(self,rec,operation):
        g,out=rec['generator'],rec['output']
        try:
            if rec['cancel']:waiting=g.throw(CancelledError())
            elif operation is None:waiting=next(g)
            else:
                try:value=operation.result()
                except BaseException as exc:waiting=g.throw(exc)
                else:waiting=g.send(value)
            if not isinstance(waiting,Future):raise TypeError('Query stage must yield Future')
            with self.cv:rec['waiting']=waiting;cancel=rec['cancel']
            # Callback only queues a ready continuation; it never merges results
            # or performs network calls in an IPC reader thread.
            waiting.add_done_callback(lambda done:self.pool.submit(self._step,rec,done))
            if cancel:waiting.cancel()
            return
        except StopIteration as done:
            if not out.done():out.set_result(done.value)
        except BaseException as exc:
            try:g.close()
            finally:
                if not out.done():out.set_exception(exc)
        finally:
            # A suspended generator retains its query record/concurrency slot.
            if g.gi_frame is None:
                with self.cv:self.records.pop(id(rec),None);self.cv.notify_all()

    def close(self):
        with self.cv:self.closed=True;records=list(self.records.values())
        for rec in records:rec['output'].cancel()
        with self.cv:self.cv.wait_for(lambda:not self.records)
        self.pool.shutdown(wait=True)
