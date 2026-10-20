"""Control messages scheduled by changed records and deadlines, never graph expansion.

Dirty notifications are coalesced. A single consumer owns ready queues/timer
heaps; shared branch state is inspected under the runtime lock. Socket work
remains on the independent control sending workers.
"""
from collections import OrderedDict, deque
import heapq
from queue import Empty, SimpleQueue
import threading
import time


class ControlEventScheduler:
    def __init__(self, runtime):
        self.runtime = runtime
        self.limit = runtime.c['peer']['control'].get('max_events_per_turn', 0)
        if type(self.limit) is not int or self.limit < 0:
            raise ValueError('max_events_per_turn must be a nonnegative integer')
        self.interval = runtime.c['peer']['heartbeat_interval_ms'] / 1000
        self.capacity = runtime.c['peer']['communication']['max_pending_control']
        self._local_done_queue = SimpleQueue()
        self._published_queue = SimpleQueue()
        self.fastpath = runtime.c['peer']['control'].get('completion_fast_path', False)
        self._dirty_lock = threading.Lock()
        self._dirty_queue = deque()
        self._dirty_set = set()
        self._ready_result = OrderedDict()
        self._ready_regular = OrderedDict()
        self._timers = {}
        self._heap = []
        self._serial = 0
        self.stats = runtime.control_scheduler_totals
        self.stats.update(dirty_records=0, timer_fires=0, submit_errors=0, heap_rebuilds=0,
                          local_done_process_ms=0., max_turn_wall_ms=0.,
                          completion_fast_path_hits=0, completion_fast_path_lock_busy=0,
                          completion_fast_path_future_fallback=0,
                          max_local_done_per_turn=0, max_dirty_per_turn=0, max_timer_events_per_turn=0)

    def dirty(self, key):
        # Can be called with runtime.lock held. The consumer never holds this
        # lock while obtaining runtime.lock, avoiding a reversed lock order.
        with self._dirty_lock:
            if key not in self._dirty_set:
                self._dirty_set.add(key)
                self._dirty_queue.append(key)
        self.runtime.control_wakeup.set()

    def local_done(self, key, report, futures, started):
        # Only the optional no-transfer path attempts the state lock, and only
        # nonblocking. Notification never takes dirty_lock on a compute worker.
        fallback = 'disabled'
        if self.fastpath:
            if futures:
                fallback = 'futures'
            elif self.runtime.try_publish_no_transfer(key, report, started):
                self._published_queue.put((key, time.monotonic_ns()))
                self.runtime.control_wakeup.set()
                return
            else:
                fallback = 'lock_busy'
        self._local_done_queue.put((key, report, futures, started, time.monotonic_ns(), fallback))
        self.runtime.control_wakeup.set()

    def _drain_published(self, limit=None):
        out = []
        while limit is None or len(out) < limit:
            try:
                out.append(self._published_queue.get_nowait())
            except Empty:
                break
        self.stats['completion_fast_path_hits'] += len(out)
        return out

    def _consume_local_done(self, limit=None):
        r = self.runtime
        completed = []
        count = 0
        while limit is None or count < limit:
            try:
                key, report, futures, started, enqueued, fallback = self._local_done_queue.get_nowait()
            except Empty:
                break
            count += 1
            if fallback == 'futures':
                self.stats['completion_fast_path_future_fallback'] += 1
            elif fallback == 'lock_busy':
                self.stats['completion_fast_path_lock_busy'] += 1
            dequeued = time.monotonic_ns()
            with r.lock:
                rec = r.records.get(key)
                if rec is None:
                    # Normal runtime retention cannot remove running branches.
                    # A removed key on shutdown has no live execution owner.
                    continue
                rec['pending_report'] = report
                rec['transfer_futures'] = futures
                rec['finalize_started'] = started
                rec['status'] = 'waiting_transfer'
                trace = rec.get('execution_timestamps')
                if trace is not None:
                    trace.update(local_done_enqueued_ns=enqueued, local_done_dequeued_ns=dequeued,
                                 local_done_consumed_ns=time.monotonic_ns(),
                                 completion_publish_path='queued', completion_fast_path_fallback=fallback)
            completed.append(key)
            for future in futures:
                future.add_done_callback(lambda _, key=key: self.dirty(key))
        return completed, count

    def _finalize_pending(self, keys):
        r = self.runtime
        for key in keys:
            with r.lock:
                pending = r.records.get(key, {}).get('pending_report') is not None
            if pending:
                r._finalize_ready(key)

    def finish_stopped(self):
        """After compute/transfers drained AND control thread joined, at close.

        Late local completion events must be consumed even after run() exits.
        The existing finalizer sees stop and publishes explicit failure while
        reclaiming pending_report/futures. This method never sends a message.
        """
        if not self.runtime.stop.is_set():
            raise RuntimeError('Control final drain requires stopped runtime')
        completed, _ = self._consume_local_done()
        self._drain_published()  # Already published; reclaim notification keys only.
        self._finalize_pending(dict.fromkeys([*completed, *self._drain_dirty()]))

    def _drain_dirty(self, limit=None):
        with self._dirty_lock:
            batch = []
            while self._dirty_queue and (limit is None or len(batch) < limit):
                key = self._dirty_queue.popleft()
                self._dirty_set.remove(key)
                batch.append(key)
        return batch

    def _set_timer(self, kind, key, due):
        token = (kind, key)
        old = self._timers.get(token)
        if old is not None and old[0] == due:
            return
        self._serial += 1
        stamp = (due, self._serial)
        self._timers[token] = stamp
        heapq.heappush(self._heap, (due, self._serial, kind, key))

    def _cancel_timer(self, kind, key):
        self._timers.pop((kind, key), None)

    def _trim_heap(self):
        while self._heap:
            due, serial, kind, key = self._heap[0]
            if self._timers.get((kind, key)) == (due, serial):
                break
            heapq.heappop(self._heap)
        # Repeated state changes cannot grow stale heap entries indefinitely.
        if len(self._heap) > 2 * len(self._timers) + 64:
            self._heap = [(due, serial, kind, key)
                          for (kind, key), (due, serial) in self._timers.items()]
            heapq.heapify(self._heap)
            self.stats['heap_rebuilds'] += 1

    def _forget_ready(self, key):
        self._ready_result.pop(key, None)
        self._ready_regular.pop(key, None)

    @staticmethod
    def _signature(rec):
        return (tuple(child['branch_id'] for child in rec['children']),
                rec['report'] is not None, rec['error'])

    def _refresh_locked(self, key, now):
        r = self.runtime
        self.stats['records_scanned'] += 1
        rec = r.records.get(key)
        if rec is None:
            self._forget_ready(key)
            self._cancel_timer('heartbeat', key)
            self._cancel_timer('retention', key)
            return
        terminal = rec['status'] in ('complete', 'failed')
        if terminal and not rec['control_inflight']:
            expiry = rec['touched'] + r.c['peer']['retention_seconds']
            if not rec['acked']:
                expiry = max(expiry, rec['deadline'])
            if now >= expiry:
                self._expire_record_locked(key, now)
                return
            self._set_timer('retention', key, expiry)
        else:
            # Completion will re-arm retention. In-flight records must not be
            # removed while a sender still owns their lookup key.
            self._cancel_timer('retention', key)
        if rec['acked'] or rec['control_inflight']:
            self._forget_ready(key)
            self._cancel_timer('heartbeat', key)
            return
        signature = self._signature(rec)
        if now >= rec['control_next_at'] or signature != rec['control_signature']:
            self._cancel_timer('heartbeat', key)
            if rec['report'] or rec['error']:
                self._ready_regular.pop(key, None)
                self._ready_result.setdefault(key, None)
            else:
                self._ready_result.pop(key, None)
                self._ready_regular.setdefault(key, None)
        else:
            self._forget_ready(key)
            self._set_timer('heartbeat', key, rec['control_next_at'])

    def _expire_record_locked(self, key, now):
        r = self.runtime
        r._pop_record_locked(key)
        self._forget_ready(key)
        self._cancel_timer('heartbeat', key)
        self._cancel_timer('retention', key)
        r.tombstones[key] = now
        self._set_timer('tombstone', key, now + r.c['peer']['retention_seconds'])
        while len(r.tombstones) > r.c['peer']['max_tombstones']:
            self._expire_tombstone_locked(next(iter(r.tombstones)))

    def _expire_tombstone_locked(self, key):
        r = self.runtime
        r.tombstones.pop(key, None)
        self._cancel_timer('tombstone', key)
        if r.query_visits.get(key[0]) == key:
            r.query_visits.pop(key[0], None)

    def _timers_due_locked(self, now, limit=None):
        self._trim_heap()
        count = 0
        while self._heap and self._heap[0][0] <= now and (limit is None or count < limit):
            due, serial, kind, key = heapq.heappop(self._heap)
            if self._timers.get((kind, key)) != (due, serial):
                continue
            self._timers.pop((kind, key))
            self.stats['timer_fires'] += 1
            count += 1
            if kind == 'tombstone':
                self._expire_tombstone_locked(key)
            else:
                self._refresh_locked(key, now)
            self._trim_heap()
        return count

    def _select_locked(self, now, limit=None):
        r = self.runtime
        selected = []
        while (r.control_active < self.capacity and (self._ready_result or self._ready_regular)
               and (limit is None or len(selected) < limit)):
            queue = self._ready_result if self._ready_result else self._ready_regular
            key, _ = queue.popitem(last=False)
            rec = r.records.get(key)
            if rec is None or rec['acked'] or rec['control_inflight']:
                continue
            signature = self._signature(rec)
            if now < rec['control_next_at'] and signature == rec['control_signature']:
                self._refresh_locked(key, now)
                continue
            rec['control_inflight'] = True
            rec['control_signature'] = signature
            trace = rec.get('execution_timestamps')
            if trace is not None:
                trace['control_selected_ns'] = time.monotonic_ns()
                if rec['report'] or rec['error']:
                    trace['control_result_selected_ns'] = trace['control_selected_ns']
            r.control_active += 1
            self._cancel_timer('heartbeat', key)
            self._cancel_timer('retention', key)
            selected.append(key)
        return selected

    def _submit_selected(self, selected):
        r = self.runtime
        for key in selected:
            try:
                r.control_workers.submit(r._send_control, key, time.monotonic())
            except Exception:
                with r.lock:
                    rec = r.records.get(key)
                    if rec is not None:
                        rec['control_inflight'] = False
                        rec['control_next_at'] = time.monotonic() + self.interval
                    r.control_active -= 1
                self.stats['submit_errors'] += 1
                self.dirty(key)

    def step(self):
        """One bounded turn; limits apply independently to each event source."""
        r = self.runtime
        cpu = time.thread_time()
        turn_started = time.monotonic()
        limit = self.limit or None
        # Already-ready reports must not wait behind newly arrived completion
        # events. Do not acquire any socket or Future wait under runtime.lock.
        before = time.monotonic()
        published = self._drain_published(limit)
        with r.lock:
            acquired = time.monotonic()
            for key, enqueued in published:
                rec = r.records.get(key)
                trace = rec.get('execution_timestamps') if rec is not None else None
                if trace is not None:
                    trace.update(completion_ready_enqueued_ns=enqueued,
                                 completion_ready_consumed_ns=time.monotonic_ns())
                self._refresh_locked(key, acquired)
            preselected = self._select_locked(acquired, limit)
        self.stats['lock_wait_ms'] += (acquired - before) * 1000
        self.stats['lock_held_ms'] += (time.monotonic() - acquired) * 1000
        self._submit_selected(preselected)

        local_started = time.monotonic()
        completed, consumed = self._consume_local_done(limit)
        dirty = self._drain_dirty(limit)
        # A completion consumed this turn must not be put behind thousands of
        # unrelated accepted/heartbeat notifications in the ordinary queue.
        work = list(dict.fromkeys([*completed, *dirty]))
        self._finalize_pending(work)
        self.stats['local_done_process_ms'] += (time.monotonic() - local_started) * 1000
        before = time.monotonic()
        with r.lock:
            acquired = time.monotonic()
            now = acquired
            self.stats['no_capacity_iterations'] += int(r.control_active >= self.capacity)
            for key in work:
                self._refresh_locked(key, now)
            timer_count = self._timers_due_locked(now, limit)
            selected = self._select_locked(now, limit)
        unlocked = time.monotonic()
        self.stats['iterations'] += 1
        self.stats['dirty_records'] += len(dirty)
        self.stats['selected'] += len(preselected) + len(selected)
        self.stats['lock_wait_ms'] += (acquired - before) * 1000
        self.stats['lock_held_ms'] += (unlocked - acquired) * 1000
        self.stats['max_local_done_per_turn'] = max(self.stats['max_local_done_per_turn'], consumed)
        self.stats['max_dirty_per_turn'] = max(self.stats['max_dirty_per_turn'], len(dirty))
        self.stats['max_timer_events_per_turn'] = max(self.stats['max_timer_events_per_turn'], timer_count)
        self._submit_selected(selected)
        self._trim_heap()
        self.stats['cpu_ms'] += (time.thread_time() - cpu) * 1000
        self.stats['max_turn_wall_ms'] = max(self.stats['max_turn_wall_ms'],
                                           (time.monotonic() - turn_started) * 1000)
        with self._dirty_lock:
            backlog = bool(self._dirty_queue)
        if backlog or not self._local_done_queue.empty() or not self._published_queue.empty():
            return 0.
        if (self._ready_result or self._ready_regular) and r.control_active < self.capacity:
            return 0.
        if self._heap:
            return max(0., self._heap[0][0] - time.monotonic())
        return None

    def run(self):
        r = self.runtime
        # Startup-only seed supports a scheduler attached to an existing empty
        # or populated runtime. Normal turns never scan all retained records.
        with r.lock:
            initial = list(r.records)
            tombstones = list(r.tombstones.items())
        for key in initial:
            self.dirty(key)
        for key, stamp in tombstones:
            self._set_timer('tombstone', key, stamp + r.c['peer']['retention_seconds'])
        while not r.stop.is_set():
            # Clear before draining: producers racing with step leave wakeup
            # set, so wait cannot lose a state change or released send slot.
            r.control_wakeup.clear()
            timeout = self.step()
            if r.stop.is_set():
                break
            r.control_wakeup.wait(timeout)
