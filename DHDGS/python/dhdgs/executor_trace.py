"""Observe the existing executor without replacing its submission algorithm."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time


class TracedComputeExecutor(ThreadPoolExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._trace_local = threading.local()

    def submit_traced(self, trace, fn, *args, **kwargs):
        self._trace_local.current = trace
        try:
            return self.submit(fn, *args, **kwargs)
        finally:
            self._trace_local.current = None

    def _adjust_thread_count(self):
        trace = getattr(self._trace_local, 'current', None)
        if trace is not None:
            trace['executor_adjust_enter_ns'] = time.monotonic_ns()
            trace['executor_workers_before'] = len(self._threads)
        try:
            return super()._adjust_thread_count()
        finally:
            if trace is not None:
                trace['executor_adjust_return_ns'] = time.monotonic_ns()
                trace['executor_workers_after'] = len(self._threads)
