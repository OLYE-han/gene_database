"""Bounded process-wide cyclic-GC diagnostics; never changes GC policy."""
from collections import deque
import gc
import time


class GCProfile:
    def __init__(self, capacity):
        self.events = deque(maxlen=capacity)
        self.starts = {}
        self.count = 0
        self.wall_ms = self.cpu_ms = 0.
        self.callback = self._observe
        gc.callbacks.append(self.callback)

    def _observe(self, phase, info):
        gen = info['generation']
        if phase == 'start':
            self.starts[gen] = (time.monotonic_ns(), time.thread_time_ns())
        elif gen in self.starts:
            end, cpu_end = time.monotonic_ns(), time.thread_time_ns()
            start, cpu_start = self.starts.pop(gen)
            wall, cpu = (end-start)/1e6, (cpu_end-cpu_start)/1e6
            self.count += 1
            self.wall_ms += wall
            self.cpu_ms += cpu
            self.events.append(dict(generation=gen, start_ns=start, end_ns=end,
                wall_ms=wall, cpu_ms=cpu, collected=info['collected'],
                uncollectable=info['uncollectable']))

    def snapshot(self):
        return dict(count=self.count, wall_ms=self.wall_ms, cpu_ms=self.cpu_ms,
                    events=list(self.events), capacity=self.events.maxlen)

    def close(self):
        if self.callback in gc.callbacks:
            gc.callbacks.remove(self.callback)


_profile = None

def configure(enabled, capacity):
    global _profile
    if enabled:
        if _profile is None:_profile = GCProfile(capacity)
    elif _profile is not None:
        _profile.close()
        _profile = None


def snapshot():
    return _profile.snapshot() if _profile is not None else None
