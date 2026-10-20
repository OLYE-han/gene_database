"""Prestarted communication workers; callers only enqueue, never run socket work."""
from collections import deque
from concurrent.futures import Future
import threading


class QueuedSender:
    """Bound the waiting queue independently of currently executing requests.

    submit never waits for queue capacity and never creates a worker. Saturation
    is explicit, so an approved remote branch cannot silently disappear.
    """
    def __init__(self, workers, queue_capacity, name):
        if workers < 1 or queue_capacity < 1:
            raise ValueError('Sender worker count and queue capacity must be positive')
        self.cv = threading.Condition()
        self.queue = deque()
        self.capacity = queue_capacity
        self.closed = False
        self.active = self.peak_queued = self.rejected = 0
        self.ready = 0
        self.threads = [threading.Thread(target=self._run, name=f'{name}-{i}', daemon=True)
                        for i in range(workers)]
        for thread in self.threads:
            thread.start()
        with self.cv:
            self.cv.wait_for(lambda: self.ready == workers)

    def submit(self, fn, *args, **kwargs):
        future = Future()
        with self.cv:
            if self.closed:
                raise RuntimeError('Communication sender closed')
            if len(self.queue) >= self.capacity:
                self.rejected += 1
                raise RuntimeError('Communication sender queue capacity exceeded')
            self.queue.append((future, fn, args, kwargs))
            self.peak_queued = max(self.peak_queued, len(self.queue))
            self.cv.notify()
        return future

    def _run(self):
        with self.cv:
            self.ready += 1
            self.cv.notify_all()
        while True:
            with self.cv:
                self.cv.wait_for(lambda: self.closed or self.queue)
                if not self.queue:
                    return
                future, fn, args, kwargs = self.queue.popleft()
                self.active += 1
            try:
                if future.set_running_or_notify_cancel():
                    try:
                        result = fn(*args, **kwargs)
                    except BaseException as exc:
                        future.set_exception(exc)
                    else:
                        future.set_result(result)
            finally:
                with self.cv:
                    self.active -= 1

    def shutdown(self, wait=True, *, cancel_futures=False):
        cancelled = []
        with self.cv:
            self.closed = True
            if cancel_futures:
                while self.queue:
                    cancelled.append(self.queue.popleft()[0])
            self.cv.notify_all()
        # Future callbacks may acquire runtime locks; never run them under cv.
        for future in cancelled:
            future.cancel()
        if wait:
            for thread in self.threads:
                thread.join()

    def metrics(self):
        with self.cv:
            return {'queued': len(self.queue), 'active': self.active,
                    'queue_capacity': self.capacity, 'peak_queued': self.peak_queued,
                    'capacity_rejections': self.rejected, 'workers': len(self.threads),
                    'closed': self.closed}
