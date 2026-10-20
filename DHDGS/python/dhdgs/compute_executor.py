"""Prestarted compute workers with a short, bounded submission path."""
from collections import deque
from concurrent.futures import Future
import threading
import time


class FixedComputeExecutor:
    """Future-compatible executor; saturation raises without waiting for space.

    Capacity counts queued calls, excluding running calls. The peer admission
    limit remains responsible for bounding accepted branches across stages.
    No user function or Future callback runs under the queue condition.
    """

    def __init__(self, max_workers, queue_capacity, thread_name_prefix='dhdgs-search'):
        if type(max_workers) is not int or max_workers < 1:
            raise ValueError('Compute worker count must be a positive integer')
        if type(queue_capacity) is not int or queue_capacity < 1:
            raise ValueError('Compute queue capacity must be a positive integer')
        self._max_workers = max_workers
        self._capacity = queue_capacity
        self._cv = threading.Condition()
        self._queue = deque()
        self._closed = False
        self._ready = self._active = self._peak_queued = self._rejected = 0
        self._callback_errors = 0
        self._threads = []
        try:
            for i in range(max_workers):
                worker = threading.Thread(target=self._run,
                    name=f'{thread_name_prefix}-{i}', daemon=True)
                worker.start()
                self._threads.append(worker)
            with self._cv:
                self._cv.wait_for(lambda: self._ready == max_workers)
        except BaseException:
            # Starting many threads may fail partway through initialization.
            # None are left waiting on a condition belonging to a failed pool.
            self.shutdown(wait=True, cancel_futures=True)
            raise

    def submit(self, fn, /, *args, **kwargs):
        return self._submit(None, fn, args, kwargs)

    def submit_traced(self, trace, fn, /, *args, **kwargs):
        return self._submit(trace, fn, args, kwargs)

    def _submit(self, trace, fn, args, kwargs):
        future = Future()
        with self._cv:
            if self._closed:
                raise RuntimeError('Compute executor closed')
            if len(self._queue) >= self._capacity:
                self._rejected += 1
                raise RuntimeError('Compute queue capacity exceeded')
            if trace is not None:
                trace['executor_enqueue_ns'] = time.monotonic_ns()
            self._queue.append((future, fn, args, kwargs, trace))
            self._peak_queued = max(self._peak_queued, len(self._queue))
            self._cv.notify()
        return future

    def _run(self):
        with self._cv:
            self._ready += 1
            self._cv.notify_all()
        while True:
            with self._cv:
                self._cv.wait_for(lambda: self._closed or self._queue)
                if not self._queue:
                    return
                future, fn, args, kwargs, trace = self._queue.popleft()
                if trace is not None:
                    trace['executor_dequeue_ns'] = time.monotonic_ns()
                self._active += 1
            try:
                if future.set_running_or_notify_cancel():
                    try:
                        result = fn(*args, **kwargs)
                    except BaseException as exc:
                        future.set_exception(exc)
                    else:
                        future.set_result(result)
                    finally:
                        if trace is not None:
                            trace['worker_call_return_ns'] = time.monotonic_ns()
            except BaseException:
                # Future callbacks can raise even BaseException. Future state
                # is already terminal when callbacks execute; retain the
                # worker and expose a diagnostic instead of losing capacity.
                with self._cv:
                    self._callback_errors += 1
            finally:
                with self._cv:
                    self._active -= 1
                    if trace is not None:
                        trace['worker_available_ns'] = time.monotonic_ns()

    def shutdown(self, wait=True, *, cancel_futures=False):
        cancelled = []
        with self._cv:
            self._closed = True
            if cancel_futures:
                while self._queue:
                    cancelled.append(self._queue.popleft()[0])
            self._cv.notify_all()
        for future in cancelled:
            try:
                future.cancel()
            except BaseException:
                # A callback must not prevent cancellation of later futures
                # or joining workers during shutdown.
                with self._cv:
                    self._callback_errors += 1
        if wait:
            current = threading.current_thread()
            for worker in self._threads:
                if worker is not current:
                    worker.join()

    def metrics(self):
        with self._cv:
            return {'queued': len(self._queue), 'active': self._active,
                    'queue_capacity': self._capacity, 'peak_queued': self._peak_queued,
                    'capacity_rejections': self._rejected, 'workers': len(self._threads),
                    'ready_workers': self._ready, 'closed': self._closed,
                    'callback_errors': self._callback_errors}


def prestart_executor(executor, max_workers, timeout=30.0):
    """Start a standard executor's entire pool before an ablation measurement.

    Each submitted initializer stays behind a gate until all workers have
    entered. Idle-worker reuse therefore cannot masquerade as full prestart.
    Submission retains the executor's original locking implementation.
    """
    if type(max_workers) is not int or max_workers < 1:
        raise ValueError('Compute worker count must be a positive integer')
    if getattr(executor, '_max_workers', max_workers) != max_workers:
        raise ValueError('Prestart worker count must match executor capacity')
    if timeout <= 0:
        raise ValueError('Prestart timeout must be positive')
    cv = threading.Condition()
    release = threading.Event()
    ready = 0
    futures = []
    started = time.monotonic()

    def initialize():
        nonlocal ready
        with cv:
            ready += 1
            cv.notify_all()
        release.wait()

    try:
        for _ in range(max_workers):
            futures.append(executor.submit(initialize))
        with cv:
            if not cv.wait_for(lambda: ready == max_workers,
                               timeout=max(0.0, timeout - (time.monotonic() - started))):
                raise TimeoutError('Compute workers did not all start before timeout')
    finally:
        release.set()
        # Wait for initialization only, never leave barrier tasks in a measured
        # work queue. Also releases all started tasks if a later submit fails.
        for future in futures:
            future.result()
    return {'workers': max_workers, 'startup_ms': (time.monotonic() - started) * 1000}
