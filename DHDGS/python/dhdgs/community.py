"""Complete source-local graph regions; scores are explicitly structural proxies."""
from collections import defaultdict
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import os
import struct
import time
from .index import native
from .planning_context import limited_settings


class PlanWorker:
    """One bounded source-side job; maintenance handlers never wait for planning.

    Retain the last result for idempotent polling until the next job replaces it.
    Search does not take this lock. The owner joins this worker before closing
    the immutable source shard's descriptors.
    """
    def __init__(self, shard, settings):
        self.shard, self.settings = shard, settings
        self.lock = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='community-plan')
        self.closed = False
        self.job = self.arguments = self.future = None

    def start(self, job, hot_ids, weights, capacity_bytes=None, deadline_s=None):
        if not isinstance(job, str) or not 1 <= len(job) <= 64 or not job.isalnum():
            raise ValueError('Invalid community job ID')
        if not 0 < len(hot_ids) <= self.settings['community_top_k'] or len(hot_ids) != len(weights):
            raise ValueError('Invalid community seed count')
        import copy
        settings=copy.deepcopy(self.settings)
        if capacity_bytes is not None:
            if type(capacity_bytes)is not int or not 65536<capacity_bytes<=settings['max_region_bytes']:
                raise ValueError('Invalid source planning capacity')
            from .locality import source_index_bytes
            from .replica_budget import region_limit
            if settings.get('locality',{}).get('enabled') and capacity_bytes>region_limit(settings,source_index_bytes(self.shard)):
                raise ValueError('Source planning capacity exceeds alpha')
            settings['max_region_bytes']=capacity_bytes
        if deadline_s is not None:
            if not np.isfinite(deadline_s) or not 0<deadline_s<=settings['operation_timeout_s']:
                raise ValueError('Invalid total community deadline')
            settings['_planning_deadline']=time.monotonic()+deadline_s
        arguments = (tuple(hot_ids), tuple(weights),capacity_bytes,deadline_s)
        with self.lock:
            if self.closed:raise RuntimeError('Community planner closed')
            if self.job == job:
                if arguments != self.arguments:raise ValueError('Community job ID conflict')
            else:
                if self.future is not None and not self.future.done():raise RuntimeError('Community planner busy')
                planner=select
                if self.settings.get('selection_policy')=='entry_geometry':
                    from .geometry import select as planner
                self.future = self.pool.submit(planner, self.shard, list(hot_ids), list(weights), settings)
                self.job, self.arguments = job, arguments
            return dict(job=job, accepted=True)

    def poll(self, job):
        with self.lock:
            if job != self.job or self.future is None:raise ValueError('Unknown community job')
            future = self.future
        if not future.done():return dict(job=job, done=False)
        # done() is true: retrieving or propagating the result cannot block.
        return dict(job=job, done=True, region=future.result())

    def close(self):
        with self.lock:self.closed = True
        self.pool.shutdown(wait=True, cancel_futures=True)


def hotspots(observations, source, settings):
    groups=defaultdict(list)
    for q in observations:
        if q['source']==source and (q['replica'] is None or settings.get('_include_replica_observations')) and q['query_id'] is not None:
            groups[int(q['entry'])].append(q)
    ranked=[]
    for entry,qs in groups.items():
        if len({q['query_id'] for q in qs})<settings['min_entry_queries']:continue
        work=sum(v['cpu_ms'] for q in qs for v in q['branches'] if v['node']==source or settings.get('_include_replica_observations'))
        if work>0:ranked.append((entry,work))
    ranked.sort(key=lambda x:(-x[1],x[0]))
    ranked=ranked[:settings['community_top_k']]
    if not ranked:raise ValueError('No sampled hot entries for community planner')
    return ranked,groups


def metadata(shard):
    m=dict(shard.meta)
    per=sum(getattr(shard,k).dtype.itemsize*int(np.prod(getattr(shard,k).shape[1:])) for k in ('codes','extra','factors','center_ids'))
    # Include ID, membership and conservative per-array/header/proof overhead.
    membership=np.load(shard.directory/'code_membership.npy',mmap_mode='r',allow_pickle=False)
    m['community_code_bytes']=per+8+membership.dtype.itemsize*int(np.prod(membership.shape[1:]))
    m['community_fixed_bytes']=sum((shard.directory/n).stat().st_size for n in ('centers.npy','rotation.bin'))+65536
    return m


def seeded_core(shard, hot_ids, weights, settings, *, seed_count, hops, planning=None):
    """Complete multi-entry core without repeated PPR candidate evaluation.

    All selected hot roots receive their full directed source-local h-ball.
    The union may have several components, each reachable from a route root.
    No row truncation is allowed: over-budget cores fail as a whole. The native
    independent verifier checks reachability, dependency bytes and membership.
    """
    if (type(seed_count) is not int or seed_count <= 0 or type(hops) is not int
            or not 1 <= hops <= settings['community_hops']
            or len(hot_ids) != len(weights) or not hot_ids
            or len(set(hot_ids)) != len(hot_ids)
            or any(not np.isfinite(w) or w <= 0 for w in weights)):
        raise ValueError('Invalid multi-entry core inputs')
    chosen = sorted(zip(hot_ids, weights), key=lambda x: (-x[1], x[0]))[:seed_count]
    roots = [int(x[0]) for x in chosen]
    if any(x not in shard.location for x in roots):
        raise ValueError('Nonlocal multi-entry seed')
    started = time.monotonic()
    deadline = started + settings['community_max_plan_ms'] / 1000
    members = set(roots)
    frontier = set(roots)
    scans = reads = 0
    meta = shard.meta
    if planning is not None:
        result = planning.balls([roots], hops, settings)
        members = set(result['members'][0])
        scans = result['edge_scans']
    else:
        for _ in range(hops):
            following = set()
            for gid in sorted(frontier):
                if time.monotonic() > deadline:
                    raise ValueError('Multi-entry core deadline')
                off = meta['header_bytes'] + shard.location[gid]*meta['record_bytes'] + 4*meta['dimension']
                raw = os.pread(shard.fd, 4 + 8*meta['max_degree'], off)
                if len(raw) < 4:
                    raise ValueError('Short core adjacency header')
                degree = struct.unpack_from('<I', raw)[0]
                if degree > meta['max_degree'] or len(raw) < 4+8*degree:
                    raise ValueError('Invalid core adjacency')
                reads += 1; scans += degree
                if scans > settings['community_max_edge_scans']:
                    raise ValueError('Multi-entry core edge budget')
                for item in np.frombuffer(raw, dtype='<u8', count=degree, offset=4):
                    v = int(item)
                    if v in shard.location and v not in members:
                        following.add(v)
                if len(members)+len(following) > settings['max_region_rows']:
                    raise ValueError('Complete multi-entry core exceeds row budget')
            members.update(following)
            frontier = following
    ids = sorted(members)
    proof = dict(kind='complete_hot_entry_balls_v1', scope='source_local_records',
                 epoch=meta['epoch'], hops=hops, seeds=sorted(roots),
                 hot_ids=roots, weights=[float(x[1]) for x in chosen], hot_closure=True,
                 members_sha256=hashlib.sha256(np.asarray(ids, dtype='<u8').tobytes()).hexdigest())
    out = dict(ids=ids, entries=sorted(roots), seeds=sorted(roots), hops=hops, region_proof=proof)
    result = verify(shard, out, settings, planning=planning)
    out['stats'] = dict(result['verification_stats'], seed_selection='top_historical_work',
                        seed_count=len(roots), enumeration_reads=reads, enumeration_scans=scans,
                        core_with_verification_seconds=time.monotonic()-started)
    out['reserved_package_bytes'] = out['stats']['estimated_bytes']
    return out


def select(shard, hot_ids, weights, settings):
    settings=limited_settings(settings)
    enabled=settings.get('hop_expansion',{}).get('enabled',False)
    core_settings=dict(settings)
    if enabled:core_settings['max_region_bytes']=int(settings['max_region_bytes']*settings['hop_expansion']['core_fraction'])
    fast_core=settings.get('fast_core',{})
    if fast_core.get('enabled',False):
        result=seeded_core(shard,hot_ids,weights,core_settings,
                           seed_count=fast_core['seed_count'],hops=fast_core['hops'])
    else:
        result=native().replica_community_plan(shard.fd,shard.ids,hot_ids,weights,core_settings,metadata(shard))
        result['region_proof']=dict(kind='complete_hot_entry_balls_v1',scope='source_local_records',
            epoch=shard.meta['epoch'],hops=result['hops'],seeds=result['seeds'],hot_ids=hot_ids,weights=weights,
            hot_closure=settings['community_hot_closure'],
            members_sha256=hashlib.sha256(np.asarray(result['ids'],dtype='<u8').tobytes()).hexdigest())
    if enabled:
        from .hop_expansion import expand,fast_capacity_expand
        fast=settings['hop_expansion'].get('fast',{})
        if fast.get('enabled',False):
            result=fast_capacity_expand(shard,result,settings,dict(zip(hot_ids,weights)),
                **{k:fast[k] for k in ('cut_budget_ms','cut_rounds','cut_candidates')})
        else:
            result=expand(shard,result,settings)
        if settings['hop_expansion'].get('promote_covered_entries',False):
            from .hop_expansion import rebind_historical_entries
            result=rebind_historical_entries(shard,result,settings,hot_ids)
    if settings.get("boundary_augmentation",{}).get("enabled"):
        from .boundary_portals import augment
        result=augment(shard,result,settings)
    return result


def verify(shard, plan, settings, planning=None):
    if planning is not None:
        return planning.verified('community', plan, settings,
                                 lambda: _verify(shard, plan, settings, planning))
    return _verify(shard, plan, settings)


def _verify(shard, plan, settings, planning=None):
    # An augmented certificate binds the active and certified entry sets
    # separately. Let its verifier validate the embedded original core before
    # applying the legacy certified-entry normalization below.
    if (plan.get('region_proof') or {}).get('kind')=='boundary_augmented_v1':
        from .boundary_portals import verify as verify_boundary
        return verify_boundary(shard,plan,settings)
    if 'certified_entries' in plan:
        certified=plan['certified_entries']
        if not certified or len(certified)!=len(set(certified)) or not set(plan['entries'])<=set(certified):
            raise ValueError('Routable entries exceed the certified structural contract')
        plan=dict(plan,entries=certified)
    proof=plan.get('region_proof')
    if proof is None:
        if settings.get('selection_policy') in ('hot_entry_community','entry_geometry'):raise ValueError('Missing complete region proof')
        return None
    if proof.get('kind')=='complete_attached_hop_balls_v1':
        from .hop_expansion import verify_expansion
        return verify_expansion(shard,plan,settings,planning=planning)
    if proof.get('kind')=='complete_entry_geometry_v1':
        from .geometry import verify as verify_geometry
        return verify_geometry(shard,plan,settings)
    if proof['kind'] not in ('complete_hot_entry_balls_v1','complete_locality_balls_v2') or proof['scope']!='source_local_records' or proof['epoch']!=shard.meta['epoch']:
        raise ValueError('Invalid community region contract')
    ids=plan['ids']
    if ids!=sorted(set(ids)) or hashlib.sha256(np.asarray(ids,dtype='<u8').tobytes()).hexdigest()!=proof['members_sha256']:
        raise ValueError('Invalid community membership digest/order')
    if proof['kind']=='complete_hot_entry_balls_v1' and set(plan['entries']) != set(proof['hot_ids']).intersection(ids):
        raise ValueError('Community entries differ from covered hot entries')
    b=dict(settings,community_hot_closure=proof['hot_closure'])
    kwargs={}
    if proof['kind']=='complete_locality_balls_v2':
        entries=plan['entries']
        if not entries or len(entries)!=len(set(entries)):raise ValueError('Missing/duplicate locality entries')
        if not set(entries)<=set(proof['routing_candidates']):raise ValueError('Unknown locality routing entry')
        kwargs=dict(route_ids=proof['routing_candidates'],verify_entry_ids=entries)
        b['community_generalization_weight']=proof['generalization_weight']
    result=native().replica_community_plan(shard.fd,shard.ids,proof['hot_ids'],proof['weights'],limited_settings(b),planning.metadata() if planning else metadata(shard),ids,proof['seeds'],proof['hops'],**kwargs)
    return dict(proof,verified=True,verification_stats=result['stats'])
