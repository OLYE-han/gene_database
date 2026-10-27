"""Python API boundary for the C++ entry, control registry and result merger.

The completion thread converts only final results to Python. Routing, entry
RPCs, leases, branch validation and merging never call back into Python.
"""
from concurrent.futures import Future
import json
from pathlib import Path
import threading
import time

import numpy as np

from .config import index_settings
from .data import load_complete_npy
from .index import native
from .peer import execution_signature


class NativeEngine:
    def __init__(self, c, *, external_completion=False):
        self.external_completion = external_completion
        from .runtime import apply_process_runtime
        self.runtime_settings = apply_process_runtime(c)
        self.c = c
        self.root = Path(c['paths']['output'])/'index'
        self.manifest = json.loads((self.root/'manifest.json').read_text())
        m = self.manifest
        if (m.get('build_settings') != index_settings(c) or m['version'] != 1
                or m['metric'] != c['data']['metric'] or m['dimension'] != c['data']['dimension']
                or len(m['shards']) != len(c['cluster']['nodes'])):
            raise ValueError('Search configuration differs from built index')
        self.ids = load_complete_npy(self.root/'ids.npy')
        self.membership = load_complete_npy(self.root/'membership.npy')
        if self.ids.dtype != np.uint64 or self.membership.dtype != np.bool_:
            raise ValueError('Invalid global location directory types')
        self.corpus_ids_contiguous = (len(self.ids) == m['rows'] and len(self.ids) > 0
                                     and int(self.ids[0]) == 0 and int(self.ids[-1]) == m['rows']-1)
        with np.load(self.root/'route_codes.npz', allow_pickle=False) as bank:
            self.route_bank = {key: np.array(bank[key]) for key in bank.files}
        self.quantizer = native().Quantizer(self.route_bank['centers'], m['bits'],
                                           m['rotation_seed'], (self.root/'rotation.bin').read_bytes())
        self.router = self._router(c)
        self.head_hub = None
        self.head_flow = None
        self.lock = threading.Lock()
        self.pending = {}
        self.closed = False
        self.load_balancer = None
        self._start()

    def start_load_balancer(self, token, directory):
        if self.load_balancer is not None:
            raise RuntimeError('Replica controller already started')
        from .load_balance import LoadBalancer
        self.load_balancer = LoadBalancer(self, token, directory)
        return self.load_balancer

    def _router(self, c):
        m, r = self.manifest, c['routing']
        router = native().Router(m['dimension'], m['route_samples'], r['hnsw_m'], r['ef_construction'], c['seed'])
        router.load(str(self.root/'router.hnsw'))
        return router

    def _start(self):
        self.runtime = native().NativeHead(self.c, self.manifest, execution_signature(self.c), self.router,
            self.quantizer, self.route_bank, self.ids, self.membership)
        self.stopping = threading.Event()
        self.disk_mode = self.c.get('native_runtime', {}).get('result_sink', {}).get('enabled', False)
        self.consumer = None
        if self.disk_mode or self.external_completion:
            return
        self.consumer = threading.Thread(target=self._consume, name='dhdgs-final-api', daemon=True)
        self.consumer.start()

    def _consume(self):
        while True:
            results = self.runtime.completed_records(32, 50)
            with self.lock:
                jobs = [self.pending.pop(record.request_id) for record in results]
            for record, (future, offered, capture) in zip(results, jobs):
                if capture:
                    # Transfer ownership only. Even MessagePack encoding is
                    # deferred until the capture buffer drains after completion.
                    future.set_result((record, offered, time.monotonic_ns()))
                    continue
                result = record.decode()
                now = time.monotonic_ns()
                stats = result['stats']
                if result['status'] == 'ok':
                    native_ready = stats['native_head_timestamps']['native_ready_ns']
                    stats['api_result_delivery_ms'] = (now-native_ready)/1e6
                    stats['api_submit_ms'] = (stats['native_head_timestamps']['search_enter_ns']-offered)/1e6
                    stats['latency_ms'] = (now-offered)/1e6
                    parts = stats['latency_breakdown_ms']
                    parts['api_result_delivery_ms'] = stats['api_result_delivery_ms']
                    parts['api_submit_ms'] = stats['api_submit_ms']
                    stats['latency_breakdown_fraction'] = {k: v/stats['latency_ms'] for k, v in parts.items()}
                    stats['head_timestamps']['search_return_ns'] = now
                    future.set_result(result)
                else:
                    error = RuntimeError(result['error'])
                    error.dhdgs_metrics = result
                    future.set_exception(error)
            with self.lock:
                if self.stopping.is_set() and not self.pending:
                    return

    def search_async(self, vector, query_id=None, *, capture=False):
        if self.disk_mode:
            raise RuntimeError('SSD result mode uses run_to_disk; process files after the experiment')
        if self.external_completion:
            raise RuntimeError('Use the network search API in external completion mode')
        offered = time.monotonic_ns()
        if query_id is not None and (type(query_id) is not int or not 0 <= query_id < 2**64):
            raise ValueError('query_id must be uint64 or null')
        query = np.asarray(vector, dtype=np.float64)
        future = Future()
        # Cancellation is a runtime operation, not removal of an active Future.
        future.set_running_or_notify_cancel()
        with self.lock:
            if self.closed or self.stopping.is_set():
                raise RuntimeError('Native head closed')
            request_id = self.runtime.submit(query, query_id)
            self.pending[request_id] = (future, offered, capture)
        return future

    def search_capture_async(self, vector, query_id=None):
        return self.search_async(vector, query_id, capture=True)

    def search(self, vector, query_id=None):
        return self.search_async(vector, query_id).result()

    def run_to_disk(self, vectors, ids, directory, concurrency):
        if not self.disk_mode or self.closed:
            raise RuntimeError('Native SSD result mode is not active')
        # Arrays remain alive throughout the native call, which releases the GIL.
        x = np.ascontiguousarray(vectors, dtype=np.float64)
        qids = np.ascontiguousarray(ids, dtype=np.uint64)
        with self.lock:
            if self.pending:
                raise RuntimeError('Queries already active')
            self.pending['disk_run'] = None
        try:
            return self.runtime.run_to_disk(x, qids, concurrency, str(directory),
                                           self.c['native_runtime']['result_sink'])
        finally:
            with self.lock:
                self.pending.pop('disk_run')

    def _stop(self):
        self.stopping.set()
        self.runtime.close()
        if self.consumer is not None:
            self.consumer.join()

    def apply_configuration(self, c, router=None):
        with self.lock:
            if self.pending:
                raise RuntimeError('Cannot reconfigure active native head')
        self.stop_load_balancer()
        old_c, old_router = self.c, self.router
        self._stop()
        self.c = c
        self.router = router if router is not None else self._router(c)
        try:
            self._start()
        except BaseException:
            self.c, self.router = old_c, old_router
            self._start()
            raise

    def stop_load_balancer(self):
        if self.load_balancer is not None:
            controller, self.load_balancer = self.load_balancer, None
            controller.close()

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:self.stop_load_balancer()
        finally:self._stop()
