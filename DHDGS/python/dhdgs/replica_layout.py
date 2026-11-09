"""Plan portal ownership from complete local neighborhoods, off the query path."""
import math


PIPELINE_DEFAULTS = dict(enabled=False, adjacency_cache=False, io_threads=2,
    prefetch_rows=128, queue_reads=256, cache_bytes=268435456,
    memoize_verification=True, verification_cache_entries=32, pending_plans=1,
    copy_during_expansion=False, growth_batch_rows=1024, growth_queue_batches=4, growth_copy_workers=2)


def pipeline_settings(layout):
    cfg = dict(PIPELINE_DEFAULTS, **layout.get('pipeline', {}))
    if set(cfg) != set(PIPELINE_DEFAULTS):
        raise ValueError('Unknown partition pipeline setting')
    if any(type(cfg[k]) is not bool for k in ('enabled','adjacency_cache','memoize_verification','copy_during_expansion')):
        raise ValueError('Invalid partition pipeline switch')
    limits = dict(io_threads=(0,16), prefetch_rows=(1,65536), queue_reads=(1,65536),
                  cache_bytes=(1024,2**40), verification_cache_entries=(1,1024), pending_plans=(1,16),
                  growth_batch_rows=(1,65536), growth_queue_batches=(1,64), growth_copy_workers=(1,4))
    if any(type(cfg[k]) is not int or not lo<=cfg[k]<=hi for k,(lo,hi) in limits.items()):
        raise ValueError('Invalid partition pipeline limits')
    if cfg['copy_during_expansion'] and not cfg['enabled']:
        raise ValueError('Growing copy requires completed-plan pipeline')
    return cfg


def build_partitioned_regions(shard, hot_ids, weights, settings, layout, *, on_plan=None, context_ready=None, stream_factory=None):
    """Deliver each fully verified plan; no partially expanded graph is emitted.

    on_plan may have side effects before a subsequent failure. Callers needing
    publication rollback should use build_and_publish_partitioned_regions.
    """
    import time
    import threading
    cfg = pipeline_settings(layout)
    planning = None
    started=time.monotonic();deadline=started+layout['deadline_s'];timer=None
    settings=dict(settings,_planning_deadline=deadline)
    if cfg['adjacency_cache']:
        from .planning_context import PlanningContext
        planning = PlanningContext(shard, settings, cfg, deadline)
        # Native stages release the GIL; cancellation also covers a stage
        # that does not return to Python before the global deadline.
        timer=threading.Timer(max(0.,deadline-time.monotonic()),planning.cancel)
        timer.daemon=True;timer.start()
    try:
        if context_ready:
            context_ready(planning)
        plans, report = _build_partitioned_regions(shard, hot_ids, weights, settings, layout,
                                                  planning=planning, on_plan=on_plan, stream_factory=stream_factory)
        if planning:
            planning.check()
            report['planning_cache'] = planning.stats()
        return plans, report
    finally:
        if timer:timer.cancel();timer.join()
        if planning:
            planning.close()


class PartitionPlanWorker:
    """Source-side bounded producer. Poll sequence numbers make lost replies safe."""
    def __init__(self, shard, settings, stream_factory=None):
        import threading
        from concurrent.futures import ThreadPoolExecutor
        self.shard, self.settings = shard, settings
        self.stream_factory = stream_factory
        self.lock = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='partition-plan')
        self.state = None
        self.closed = False

    def start(self, job, hot_ids, weights, layout, streams=None, preflight=None):
        import copy
        import queue
        import threading
        import json
        if not isinstance(job,str) or not job.isalnum() or not 1<=len(job)<=64:
            raise ValueError('Invalid partition job ID')
        if not 0<len(hot_ids)<=self.settings['community_top_k'] or len(hot_ids)!=len(weights):
            raise ValueError('Invalid partition seed count')
        cfg=pipeline_settings(layout)
        if cfg['copy_during_expansion'] and (not self.stream_factory or not streams):
            raise ValueError('Growing copy requires reserved live streams')
        if not cfg['copy_during_expansion'] and streams:raise ValueError('Unexpected growing streams')
        args=json.dumps([hot_ids,weights,layout,streams],sort_keys=True)
        with self.lock:
            if self.closed:raise RuntimeError('Partition planner closed')
            if self.state and self.state['job']==job:
                if self.state['arguments']!=args:raise ValueError('Partition job ID conflict')
                return dict(job=job,accepted=True)
            if self.state and (not self.state['future'].done() or
                    any(not v.future.done() for v in self.state['streams']) or
                    not (self.state['terminal_polled'] or self.state['cancel'].is_set())):
                raise RuntimeError('Partition planner busy or result not acknowledged')
            if preflight:preflight()
            s=dict(job=job,arguments=args,queue=queue.Queue(maxsize=cfg['pending_plans']),
                   cancel=threading.Event(),context=None,last=None,sequence=-1,terminal_polled=False,streams=[])
            self.state=s
            def attach(context):
                s['context']=context
                if s['cancel'].is_set() and context:context.cancel()
            def emit(plan):
                while not s['cancel'].is_set():
                    if s['context']:s['context'].check()
                    try:s['queue'].put(plan,timeout=.05);return
                    except queue.Full:pass
                raise RuntimeError('Partition planning cancelled')
            def factory(target,capacity):
                if s['cancel'].is_set():raise RuntimeError('Partition planning cancelled')
                stream_cfg=dict(cfg)
                if s['context']:stream_cfg['_planning_deadline']=s['context'].deadline
                stream=self.stream_factory(streams[str(target)],target,capacity,stream_cfg,s['stream_pool'])
                s['streams'].append(stream)
                if s['cancel'].is_set():stream.abort();raise RuntimeError('Partition planning cancelled')
                return stream
            def run():
                from concurrent.futures import ThreadPoolExecutor
                s['stream_pool']=ThreadPoolExecutor(max_workers=cfg['growth_copy_workers'],thread_name_prefix='growth-copy') if cfg['copy_during_expansion'] else None
                try:
                    plans,report=build_partitioned_regions(self.shard,list(hot_ids),list(weights),
                        self.settings,copy.deepcopy(layout),on_plan=emit if cfg['enabled'] else None,
                        context_ready=attach,stream_factory=factory if cfg['copy_during_expansion'] else None)
                    if not cfg['enabled']:
                        for plan in plans:emit(plan)
                    return json.loads(json.dumps(report))
                except BaseException:
                    for stream in s['streams']:stream.abort()
                    raise
                finally:
                    if s['stream_pool']:s['stream_pool'].shutdown(wait=True,cancel_futures=False)
            s['future']=self.pool.submit(run)
        return dict(job=job,accepted=True)

    def poll(self, job, sequence):
        import queue
        if type(sequence) is not int or sequence<0:raise ValueError('Invalid partition sequence')
        with self.lock:
            s=self.state
            if s is None or s['job']!=job:raise ValueError('Unknown partition job')
            future=s['future']
            if future.done() and future.exception() is not None:future.result()
            if sequence==s['sequence']:
                return dict(job=job,sequence=sequence,done=False,plan=s['last'])
            if sequence!=s['sequence']+1:raise ValueError('Partition sequence skipped or stale')
            try:plan=s['queue'].get_nowait()
            except queue.Empty:
                if future.done():
                    s['terminal_polled']=True
                    return dict(job=job,sequence=sequence,done=True,report=future.result())
                return dict(job=job,sequence=sequence,done=False,plan=None)
            s.update(last=plan,sequence=sequence)
            return dict(job=job,sequence=sequence,done=False,plan=plan)

    def cancel(self, job):
        with self.lock:
            s=self.state
            if s is None or s['job']!=job:raise ValueError('Unknown partition job')
            s['cancel'].set()
            for stream in s['streams']:stream.abort()
            if s['context']:s['context'].cancel()
        return dict(cancelled=True,quiescent=s['future'].done() and all(v.future.done() for v in s['streams']))

    def close(self):
        with self.lock:
            self.closed=True
            if self.state:
                self.state['cancel'].set()
                for stream in self.state['streams']:stream.abort()
                if self.state['context']:self.state['context'].cancel()
        self.pool.shutdown(wait=True)


def _build_partitioned_regions(shard, hot_ids, weights, settings, layout, *, planning=None, on_plan=None, stream_factory=None):
    """Build capacity-certified regions for exclusive historical portal groups.

    Source-local complete h-balls are indivisible. External edges remain exits.
    Capacity includes overlaps and quantization dependencies at each target.
    This off-path planner returns plans; publication still uses the normal
    validated package pipeline. It never initiates a remote search.
    """
    import copy
    import os
    import struct
    import time
    from .community import seeded_core
    from .hop_expansion import breadth_expand, rebind_historical_entries, open_covered_entries
    from .locality import source_index_bytes
    cfg=pipeline_settings(layout)
    if cfg['copy_during_expansion'] and stream_factory is None:
        raise ValueError('Growing copy requires online publication orchestration')
    alpha = layout['total_alpha']; hops = layout['hops']
    targets = {int(k): float(v) for k, v in layout['target_weights'].items()}
    if (not 0 < alpha <= max(1,len(targets)) or type(hops) is not int or not 1 <= hops <= settings['community_hops']
            or len(hot_ids) != len(weights) or len(set(hot_ids)) != len(hot_ids)
            or not targets or any(not math.isfinite(v) or v <= 0 for v in targets.values())
            or layout['deadline_s'] <= 0 or layout['max_edge_scans'] <= 0):
        raise ValueError('Invalid partitioned region settings')
    work = dict(zip(map(int, hot_ids), map(float, weights)))
    if not work or any(k not in shard.location for k in work) or any(not math.isfinite(v) or v <= 0 for v in work.values()):
        raise ValueError('Invalid partitioned region hotspots')
    historical_entries=layout.get('historical_entries')
    covered=layout.get('covered_route_entries')
    if covered is not None:
        if (not isinstance(covered,list) or not covered or len(covered)>len(shard.ids)
                or any(type(e)is not int or e not in shard.location for e in covered)
                or covered!=sorted(set(covered)) or not set(work)<=set(covered)
                or historical_entries is not None):
            raise ValueError('Invalid complete covered routing directory')
    if historical_entries is not None:
        if (not isinstance(historical_entries,list) or len(targets)!=1 or not 0<len(historical_entries)<=4096
                or any(type(e)is not int or e not in shard.location for e in historical_entries)
                or len(set(historical_entries))!=len(historical_entries) or not set(work)<=set(historical_entries)):
            raise ValueError('Invalid bounded historical entry candidates')
    source_bytes = source_index_bytes(shard)
    budget = min(int(source_bytes * alpha), settings['max_total_replica_bytes'])
    requested=layout.get('capacity_bytes',budget)
    if type(requested)is not int or not 65536<requested<=settings['max_total_replica_bytes']:
        raise ValueError('Invalid explicit partition capacity')
    budget=min(budget,requested)
    start = time.monotonic(); deadline = settings.get('_planning_deadline',start + layout['deadline_s'])
    reserve=float(layout.get('finish_reserve_s',0.))
    if not math.isfinite(reserve) or not 0<=reserve<layout['deadline_s']:
        raise ValueError('Invalid partition finish reserve')
    scans = reads = 0; cache = {}; balls = {}; meta = shard.meta
    def adjacency(gid):
        nonlocal scans, reads
        if time.monotonic() > deadline: raise TimeoutError('Partition planning deadline')
        if gid not in cache:
            offset = meta['header_bytes'] + shard.location[gid] * meta['record_bytes'] + 4 * meta['dimension']
            raw = os.pread(shard.fd, 4 + 8 * meta['max_degree'], offset)
            if len(raw) < 4: raise ValueError('Short partition adjacency')
            count = struct.unpack_from('<I', raw)[0]
            if count > meta['max_degree'] or len(raw) < 4 + 8 * count: raise ValueError('Invalid partition degree')
            cache[gid] = {v[0] for v in struct.iter_unpack('<Q', raw[4:4+8*count]) if v[0] in shard.location}
            reads += 1
            if reads * (4 + 8 * meta['max_degree']) > settings['community_adjacency_bytes']:
                raise ValueError('Partition adjacency cache limit')
        scans += len(cache[gid])
        if scans > layout['max_edge_scans']: raise ValueError('Partition edge scan limit')
        return cache[gid]
    if planning is not None:
        roots = sorted(work)
        result = planning.balls([[root] for root in roots], hops, settings, layout['max_edge_scans'])
        balls = dict(zip(roots, map(set, result['members'])))
        scans = result['edge_scans']
    else:
        for root in sorted(work):
            seen = {root}; front = {root}
            for _ in range(hops):
                front = set().union(*(adjacency(gid) for gid in sorted(front))) - seen if front else set()
                seen.update(front)
            balls[root] = seen
    planned = partition_entries(balls, work, targets, layout['balance_weight'])
    planned['layout_seconds'] = time.monotonic() - start
    del cache, balls
    plans = []; phases = []
    for index,(target, entries) in enumerate(planned['groups'].items()):
        cap = min(settings['max_region_bytes'], budget // len(targets))
        if cap <= 65536: raise ValueError('Partition metadata exceeds capacity')
        b = copy.deepcopy(settings)
        # Reserve route proof and package metadata before expanding, so adding
        # all portals cannot overflow the same physical byte budget afterwards.
        route_reserve=128*len(covered) if covered is not None else 0
        if cap<=65536+route_reserve:raise ValueError('Covered route metadata exceeds partition capacity')
        b.update(max_region_bytes=cap-65536-route_reserve, max_total_replica_bytes=cap)
        b['locality']['source_capacity_fraction'] = cap / source_bytes
        t = time.monotonic()
        left = deadline-t
        if left <= 0: raise TimeoutError('Partition core deadline')
        b['community_max_plan_ms'] = min(b['community_max_plan_ms'], max(1, int(left*1000)))
        core = seeded_core(shard, entries, [work[e] for e in entries], b, seed_count=len(entries), hops=hops, planning=planning)
        core_seconds = time.monotonic()-t; t = time.monotonic()
        left = deadline-t
        if left <= 0: raise TimeoutError('Partition expansion deadline')
        b['community_max_plan_ms'] = min(b['community_max_plan_ms'], max(1, int(left*1000)))
        if reserve:
            # Split remaining time among unfinished regions, reserving proof time.
            b['_expansion_stop_at']=time.monotonic()+max(0.,left/(len(targets)-index)-reserve)
        stream=stream_factory(target,cap) if stream_factory else None
        out = breadth_expand(shard, core, b, planning=planning,
            on_admit=stream.admit if stream else None, batch_rows=cfg['growth_batch_rows'])
        original_ids=out['ids']
        candidates=covered if covered is not None else (entries if historical_entries is None else historical_entries)
        promote=open_covered_entries if covered is not None else rebind_historical_entries
        out = promote(shard, out, dict(b, max_region_bytes=cap), candidates, planning=planning)
        if out['ids']!=original_ids:
            raise ValueError('Historical entry promotion changed region membership')
        out.update(target=target, source_capacity_bytes=source_bytes, source_capacity_limit=budget)
        if 'replica_tier' in layout:out['replica_tier']=layout['replica_tier']
        if out['reserved_package_bytes'] > cap: raise ValueError('Partition exceeds physical capacity')
        if stream:stream.finish(out)
        plans.append(out)
        phases.append(dict(target=target, core_seconds=core_seconds, expansion_verify_seconds=time.monotonic()-t,
                           rows=len(out['ids']), entries=len(out['entries']), reserved_bytes=out['reserved_package_bytes'], capacity_bytes=cap,
                           plan_ready_seconds=time.monotonic()-start,seed_entries=len(entries),
                           entry_candidates=len(candidates),added_entries=len(set(out['entries'])-set(entries))))
        if on_plan is not None:
            on_plan(copy.deepcopy(out))
    claimed = [e for p in plans for e in p['entries']]
    if covered is None and len(claimed) != len(set(claimed)): raise ValueError('Partition portal collision')
    used = sum(p['reserved_package_bytes'] for p in plans)
    if used > budget: raise ValueError('Partition aggregate capacity exceeded')
    planned.update(phases=phases, reserved_bytes=used, budget_bytes=budget,
                   physical_rows=sum(len(p['ids']) for p in plans),
                   unique_rows=len(set().union(*(set(p['ids']) for p in plans))),
                   total_seconds=time.monotonic()-start, adjacency_reads=reads, edge_scans=scans)
    if time.monotonic() > deadline: raise TimeoutError('Partition verification deadline')
    return plans, planned


def partition_entries(neighborhoods, work, target_weights, balance_weight=1.):
    """Greedy incremental overlap/cost heuristic, not an optimal graph cut.

    Each portal has one target. Neighborhood records may overlap, and must be
    charged once per physical target by the subsequent certified packer.
    The objective trades incremental union size against squared work load.
    Final byte capacity and graph quality are independent acceptance gates.
    """
    if not work or set(neighborhoods) != set(work) or not target_weights:
        raise ValueError('Incomplete layout inputs')
    if (not math.isfinite(balance_weight) or balance_weight < 0
            or any(not math.isfinite(x) or x <= 0 for x in work.values())
            or any(not math.isfinite(x) or x <= 0 for x in target_weights.values())):
        raise ValueError('Invalid layout weights')
    if any(root not in members for root, members in neighborhoods.items()):
        raise ValueError('Neighborhood must contain its portal')
    total_work = sum(work.values())
    weight_sum = sum(target_weights.values())
    goals = {k: total_work*w/weight_sum for k, w in target_weights.items()}
    unions = {k:set() for k in goals}; loads = {k:0. for k in goals}
    groups = {k:[] for k in goals}
    normalizer = max(1, len(set().union(*neighborhoods.values())))
    for entry in sorted(work, key=lambda k:(-work[k], k)):
        def cost(target):
            before = loads[target]/goals[target]
            after = (loads[target]+work[entry])/goals[target]
            return (len(neighborhoods[entry]-unions[target])/normalizer
                    + balance_weight*(after*after-before*before), target)
        target = min(goals, key=cost)
        groups[target].append(entry)
        loads[target] += work[entry]
        unions[target].update(neighborhoods[entry])
    if any(not entries for entries in groups.values()):
        raise ValueError('Layout left an empty target; use fewer targets')
    return dict(groups={k:sorted(v) for k,v in groups.items()}, work=loads,
                core_rows={k:len(v) for k,v in unions.items()},
                duplicated_core_rows=sum(map(len, unions.values()))-len(set().union(*unions.values())),
                scope='Exclusive portals; overlapping complete neighborhoods; validate final bytes and recall separately')


def build_and_publish_partitioned_regions(shard, hot_ids, weights, settings, layout,
                                          publish, retire, *, cancelled=lambda: False):
    """One planner + one bounded publisher; roll back this build on failure.

    publish(plan) returns a handle only after that whole replica is published.
    It must clean up its own partially failed publication; retire(handle) drains
    successful publications. Earlier preexisting replicas are never retired.
    No record of a still-expanding region is handed to the publisher.
    """
    import queue
    import threading
    import time
    cfg = pipeline_settings(layout)
    pending = queue.Queue(maxsize=cfg['pending_plans'])
    failed = threading.Event()
    done = threading.Event()
    publications, errors, events = [], [], []
    started = time.monotonic()

    def check():
        if cancelled():
            raise RuntimeError('Partition publication cancelled')
        if failed.is_set():
            raise RuntimeError('Partition publisher failed') from errors[0]

    def consume():
        while True:
            try:
                plan = pending.get(timeout=.05)
            except queue.Empty:
                if done.is_set():
                    return
                continue
            try:
                if failed.is_set() or cancelled():
                    continue
                begin = time.monotonic()-started
                handle = publish(plan)
                publications.append(handle)
                events.append(dict(target=plan['target'], publish_started_seconds=begin,
                                   published_seconds=time.monotonic()-started))
            except BaseException as exc:
                errors.append(exc)
                failed.set()
            finally:
                pending.task_done()

    def enqueue(plan):
        while True:
            check()
            try:
                pending.put(plan, timeout=.05)
                return
            except queue.Full:
                pass

    worker = threading.Thread(target=consume, name='partition-publish', daemon=False)
    worker.start()
    try:
        plans, report = build_partitioned_regions(shard, hot_ids, weights, settings, layout,
                                                  on_plan=enqueue if cfg['enabled'] else None)
        if not cfg['enabled']:
            for plan in plans:
                enqueue(plan)
        done.set()
        worker.join()
        check()
        report.update(publications=events, publication_pipeline=cfg['enabled'],
                      first_published_seconds=min(e['published_seconds'] for e in events),
                      all_published_seconds=time.monotonic()-started)
        return plans, report, publications
    except BaseException as exc:
        failed.set()
        done.set()
        worker.join()
        cleanup_errors = []
        for handle in reversed(publications):
            try:
                retire(handle)
            except BaseException as cleanup:
                cleanup_errors.append(str(cleanup))
        if cleanup_errors:
            raise RuntimeError('Partition build failed; retirement remains pending: '+
                               '; '.join(cleanup_errors)) from exc
        raise
