"""Protocol/I/O adapters for a fixed-owner, GIL-released C++ search loop.

No candidate maintenance or expansion decisions occur in these adapters.
"""
import time
import uuid

import numpy as np

from .data import load_complete_npy
from .index import native
from .metrics import CLOCK_DOMAIN
from .tracing import trace_query, trace_lifecycle
from .peer_search import COUNTERS


def prepare_shard(shard, directory):
    if getattr(shard, 'native_search', None) is not None:
        return
    path = shard.directory/'code_membership.npy'
    if path.exists():
        membership = load_complete_npy(path)
    else:
        width = max(n for owners in directory.values() for n in owners)+1
        membership = np.zeros((len(shard.code_ids), width), dtype='u1')
        for i, gid in enumerate(shard.code_ids):
            membership[i, directory[int(gid)]] = 1
    shard.native_search = native().SearchShard(shard, membership, CLOCK_DOMAIN)


def run_native(state, shard, directory, c, spawn, cancelled, *, control=None, io=None, completion=None):
    prepare_shard(shard, directory)
    if cancelled():
        raise TimeoutError('Query cancelled before native search')
    tracing = trace_query(c, state.get("query_id"))
    lifecycle = trace_lifecycle(c, state.get("query_id"))
    cpu = time.thread_time()
    started = time.perf_counter()
    if control is None:
        deadline = time.monotonic()+c['peer']['query_timeout_seconds']
        control = native().SearchControl(deadline, deadline)
    rng = np.random.default_rng(state['decision_seed'])
    draws = rng.random(state['work_grant'])
    pending = None
    io_traces = []

    def approved_spawn(payload):
        payload = dict(payload)
        draws_used = payload.pop('draws_used')
        child_rng = np.random.default_rng(state['decision_seed'])
        child_rng.random(draws_used)
        child = {**state, **payload, 'branch_id': str(uuid.uuid4()),
                 'parent_branch_id': state['branch_id'],
                 'visited_nodes': [*state['visited_nodes'], payload['target_node']],
                 'ancestry': [*state.get('ancestry', []),
                              {'branch_id': state['branch_id'], 'node_id': state['target_node']}],
                 'decision_seed': int(child_rng.integers(0, 2**32))}
        for key in ('replica_version','replica_access_kind','replica_fallback_source','replica_unavailable_fallback'):
            if key not in payload:child.pop(key,None)
        if c['peer'].get('logical_region_visits',False):
            region=lambda s:'replica:'+s['replica_version'] if 'replica_version' in s else 'base:'+str(s['target_node'])
            path=state.get('visited_regions',[f'base:{n}' for n in state['visited_nodes']])
            path=[*path[:-1],region(state)]
            child['logical_region']=region(child);child['visited_regions']=[*path,region(child)]
        descriptor = {'decision_id': child['dispatch_decision_id'],
                      'branch_id': child['branch_id'], 'node_id': child['target_node'],
                      'work_grant': child['work_grant'], 'branch_slots': child['branch_slots'],
                      'parent_branch_id': state['branch_id']}
        if 'logical_region' in child:descriptor['logical_region']=child['logical_region']
        if 'replica_fallback_source' in child:descriptor['fallback_node']=child['replica_fallback_source']
        spawn(child, descriptor)
        return descriptor

    def read_batch(ids):
        nonlocal pending
        if pending is not None:
            raise RuntimeError('Native query submitted a second unconsumed I/O round')
        # result() releases the GIL while waiting. The same OS worker thread
        # resumes this call; the reactor never computes distances or candidates.
        pending = io.submit(ids, state['epoch'],
                            (state['query_request_id'], state['branch_id']),
                            control.cancelled).result()
        if tracing:
            io_traces.append(pending.trace)
        return [pending.records[gid] for gid in ids], pending.stats

    def release_batch():
        nonlocal pending
        if pending is not None:
            pending.release()
            pending = None

    try:
        native_call_ns = time.monotonic_ns()
        report = shard.native_search.run({**state, "trace_enabled": tracing,
                                          "lifecycle_trace_enabled": lifecycle}, c, draws, control,
                                         read_batch if io is not None else None,
                                         release_batch, approved_spawn, completion)
        native_return_ns = time.monotonic_ns()
        if lifecycle:
            report['native_timestamps'].update(native_call_ns=native_call_ns,
                                               native_return_ns=native_return_ns)
    finally:
        release_batch()
    if completion is not None:
        # The native controller already owns the immutable report. Do not
        # perform a second Python publication or post-success cancel check.
        return report
    if cancelled() or control.cancelled():
        raise TimeoutError('Query cancelled before native result publication')
    stats = report['stats']
    for key in (*COUNTERS, 'batches', 'frontier_dropped', 'fallback_entries',
                'improved_topk_batches', 'visited_node_candidates_dropped',
                'remote_candidates_dropped_after_forward'):
        stats.setdefault(key, 0)
    stats['server_thread_cpu_ms'] = (time.thread_time()-cpu)*1000
    stats['query_branch_wall_ms'] = (time.perf_counter()-started)*1000
    stats['query_compute_slice_wall_ms'] = max(0., stats['query_branch_wall_ms']-stats['query_io_wait_ms'])
    if io is not None and tracing:
        for batch, trace in zip(report['local_batch_trace'], io_traces, strict=True):
            batch['io_trace'] = trace
    report['execution'] = {'model': 'native',
                           'io_backend': io.backend if io is not None else 'sync_pread',
                           'fallback_reason': io.fallback_reason if io is not None else None,
                           'query_parallelism': 'one_fixed_compute_thread_per_node_branch'}
    return report
