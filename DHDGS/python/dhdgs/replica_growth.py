"""Copy admitted records while a region grows; seal before any route can use it.

A planning thread produces IDs only. One native packer and the existing bounded
maintenance sender own all data I/O. The source spool and destination use the
same append order, so closing the plan never rewrites the vector records.
"""
import copy
import hashlib
import json
import queue
import threading
import time

from .index import native, HEADER, MAGIC
from .replicas import package, ident

CLAIM = ('source', 'target', 'ids', 'entries', 'region_proof', 'reserved_package_bytes',
         'source_capacity_bytes')


def claim_digest(plan):
    fields={k:plan[k] for k in CLAIM}
    fields['replica_tier']=plan.get('replica_tier','small')
    fields['certified_entries']=plan.get('certified_entries',plan['entries'])
    return hashlib.sha256(json.dumps(fields,sort_keys=True,separators=(',',':')).encode()).hexdigest()


class GrowingReplica:
    def __init__(self, service, version, target, capacity, cfg, executor):
        self.service, self.version = service, ident(version)
        self.cfg, self.capacity = cfg, capacity
        self.pending = queue.Queue(maxsize=cfg['growth_queue_batches'])
        self.cancelled = threading.Event()
        self.started = time.monotonic_ns()
        self.rows = self.batches = self.peak = 0
        self.blocked = 0.
        self.finished = False
        s = service
        # Never silently fall back after a growing transfer has reserved space.
        capabilities = s.rpc(s.c['cluster']['nodes'][target], 'capabilities')
        if 3 not in capabilities.get('replica_protocols', []):
            raise ValueError('Target does not support growing replica protocol')
        state = dict(id=version, role='source', phase='BUILDING', events=[],
                     plan=dict(target=target), _cancel=self.cancelled,
                     _pack_settings=dict(s.b, halo_hops=0, max_region_bytes=capacity,
                         inflight_chunks=max(1,s.b['inflight_chunks']//cfg['growth_copy_workers']),
                         producer_queue_bytes=max(s.b['chunk_bytes'],s.b.get('producer_queue_bytes',67108864)//cfg['growth_copy_workers'])))
        with s.lock:
            if version in s.states:
                raise ValueError('Growing version already exists')
            s.states[version] = self.state = state
            s.event(state, 'BUILDING', growth_started_ns=self.started, growing=True)
            from .replica_pipeline import send_pipeline
            self.future = state['future'] = executor.submit(send_pipeline, s, state,
                                           produce_package=self.produce, growing=True)

    def check(self):
        if self.cancelled.is_set() or self.service.stop.is_set():
            raise RuntimeError('Growing replica cancelled')
        if self.future.done():
            self.future.result()
            if self.state['phase'] != 'SENT':
                raise RuntimeError(self.state.get('error', 'Growing sender failed'))

    def emit(self, event):
        begin = time.monotonic()
        while True:
            self.check()
            if time.monotonic()>self.cfg.get('_planning_deadline',float('inf')):
                raise TimeoutError('Growing admission exceeded total planning deadline')
            try:
                self.pending.put(event, timeout=.05)
                break
            except queue.Full:
                pass
        self.blocked += time.monotonic()-begin
        self.peak = max(self.peak, self.pending.qsize())

    def admit(self, ids):
        if self.finished or not 0 < len(ids) <= self.cfg['growth_batch_rows']:
            raise ValueError('Invalid growing admission batch')
        self.emit(('ids', list(ids)))
        self.rows += len(ids); self.batches += 1

    def finish(self, plan):
        if self.finished:
            raise ValueError('Growing plan already closed')
        if plan['reserved_package_bytes'] > self.capacity:
            raise ValueError('Growing plan exceeds reserved capacity')
        plan['_stream_version'] = self.version
        final = copy.deepcopy(plan)
        final['source'] = self.service.owner.sid
        with self.service.lock:
            self.state['plan'] = final
            self.state['_adopt_claim'] = claim_digest(final)
            self.state.update(admission_finished_ns=time.monotonic_ns(), admitted_rows=self.rows,
                              admission_batches=self.batches, admission_queue_peak=self.peak,
                              admission_backpressure_seconds=self.blocked)
        self.emit(('final', final))
        self.finished = True

    def abort(self):
        self.cancelled.set()

    def produce(self, path, stopped, progress, file_ready):
        from .community import verify
        s = self.service; shard = s.owner.worker; m = shard.meta
        path.mkdir(exist_ok=False)
        header = HEADER.pack(MAGIC, 1, m['dimension'], 0, m['record_bytes'])
        header += bytes(m['header_bytes']-len(header))
        settings = dict(self.state['_pack_settings'])
        writer = native().ReplicaGrowingWriter(shard.fd, shard.ids, m['dimension'], m['record_bytes'],
            m['header_bytes'], m['max_degree'], settings['max_region_rows'], self.capacity,
            str(path/'nodes.bin'), header, settings.get('compute_region_quality',True))
        io_seconds = 0.; wait_seconds = 0.; first = 0
        try:
            while True:
                if stopped():
                    raise RuntimeError('Growing record producer cancelled/deadline')
                begin = time.monotonic()
                try:event = self.pending.get(timeout=.05)
                except queue.Empty:
                    wait_seconds += time.monotonic()-begin
                    continue
                wait_seconds += time.monotonic()-begin
                if event[0] == 'final':
                    plan = event[1]
                    break
                begin = time.monotonic()
                ready = writer.append(event[1])
                io_seconds += time.monotonic()-begin
                first = first or time.monotonic_ns()
                progress(0, ready)
            # Independent publication verification: build-local memo is NOT used.
            before = time.monotonic()
            proof = verify(shard, plan, settings)
            if proof is None or proof['verification_stats']['estimated_bytes'] > plan['reserved_package_bytes']:
                raise ValueError('Growing publication proof/capacity mismatch')
            settings.update(_region_proof=proof, max_region_bytes=plan['reserved_package_bytes'])
            packed = writer.finish(plan['ids'])
            progress(m['header_bytes']+len(packed['ids'])*m['record_bytes'],
                     m['header_bytes']+len(packed['ids'])*m['record_bytes'])
            with s.lock:
                self.state.update(region_verification_seconds=time.monotonic()-before,
                    first_records_ready_ns=first, record_append_seconds=io_seconds,
                    record_wait_for_admission_seconds=wait_seconds, source_sealed_ns=time.monotonic_ns())
            if stopped():raise RuntimeError('Growing replica cancelled before package seal')
            return package(shard, plan['ids'], plan['entries'], path, settings, stopped,
                           file_ready=file_ready, prepacked=packed)
        finally:
            writer.close()


def reserve_portfolio(controller, source, layout):
    """Reserve aggregate head capacity before the first partial byte is copied."""
    import uuid
    c=controller;b=c.b
    if not b.get('locality',{}).get('enabled') or b.get('replica_transfer_mode')!='pipeline_v2':
        raise ValueError('Growing copy requires locality capacity accounting and pipeline_v2')
    from .replica_budget import check_layout, available, reservations, region_limit
    check_layout(b,layout)
    from .replica_layout import pipeline_settings
    workers=pipeline_settings(layout)['growth_copy_workers']
    if workers>b['inflight_chunks'] or (4*(b['inflight_chunks']//workers)+2)*b['chunk_bytes']*workers>b.get('transfer_buffer_budget_bytes',134217728):
        raise ValueError('Growing transfer buffers exceed shared budget')
    targets=sorted(map(int,layout['target_weights']))
    if source in targets or any(t<0 or t>=len(c.c['cluster']['nodes']) for t in targets):
        raise ValueError('Invalid growing target')
    actual=c.rpc(source,'load')['source_index_bytes']
    budget=min(int(actual*layout['total_alpha']),b['max_total_replica_bytes'])
    budget=min(budget,layout.get('capacity_bytes',budget))
    cap=min(region_limit(b,actual,layout.get('replica_tier','small')),budget//len(targets))
    if cap<=65536:raise ValueError('Growing capacity too small')
    # Check protocol on every participant before any target receives a byte.
    for target in targets:
        if 3 not in c.rpc(target,'capabilities').get('replica_protocols',[]):
            raise ValueError('Growing target protocol unavailable')
        free=c.rpc(target,'load')['free_bytes']
        with c.lock:c.loads.setdefault(target,{})['free_bytes']=free
    with c.lock:
        if c.in_progress is not None or getattr(c,'partition_pending',{}):
            raise RuntimeError('Replica build/reservation in progress')
        active=reservations(c)
        if len(active)+len(targets)>b['max_replicas']:
            raise ValueError('Growing portfolio exceeds replica slots')
        if sum(r['bytes'] for r in active)+cap*len(targets)>b['max_total_replica_bytes']:
            raise ValueError('Growing portfolio exceeds aggregate capacity')
        if cap*len(targets)>available(c,source,actual):
            raise ValueError('Growing portfolio exceeds aggregate source capacity')
        for target in targets:
            if cap>available(c,source,actual,target):
                raise ValueError('Growing portfolio exceeds target capacity/free space')
        streams={str(t):dict(id='r_'+uuid.uuid4().hex,bytes=cap) for t in targets}
        c.partition_pending={v['id']:dict(id=v['id'],bytes=cap,source=source,target=int(t)) for t,v in streams.items()}
    c.log('GROWING_PORTFOLIO_RESERVED',source=source,streams=streams,bytes=cap*len(targets))
    return streams


def cleanup_portfolio(controller, streams, published, source_quiescent):
    """Do not free capacity if a delayed producer could still create a target."""
    c=controller;errors=[]
    for target,desc in streams.items():
        version=desc['id']
        if version in published:continue  # Existing retire/drain owns its reservation.
        if not source_quiescent:
            errors.append(version+': source cancellation not acknowledged; reservation retained')
            continue
        try:
            c.rpc(int(target),'retire',id=version)
            until=time.monotonic()+c.b['operation_timeout_s']
            while not c.rpc(int(target),'reap',id=version)['reclaimed']:
                if time.monotonic()>=until:raise TimeoutError('Growing target cleanup pending')
                time.sleep(.05)
            with c.lock:c.partition_pending.pop(version,None)
        except BaseException as exc:
            errors.append(version+': '+str(exc))
    return errors
