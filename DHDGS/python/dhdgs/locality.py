"""Prebuilt, complete local-region portfolios from historical entries and index geometry.

No evaluation query or nearest-neighbor truth is an input. A portfolio counts
physical package bytes, including overlap and repeated quantization metadata.
"""
import hashlib
import math
import time
import numpy as np
from .community import metadata, verify
from .index import native


def source_index_bytes(shard):
    from .replicas import FILES
    return sum((shard.directory / name).stat().st_size for name in FILES if (shard.directory / name).is_file())


def select_regions(shard, hot_ids, weights, route_ids, settings):
    c = settings['locality']
    if not c['enabled']:
        raise ValueError('Locality portfolio selection disabled')
    if len(hot_ids) != len(weights) or not hot_ids:
        raise ValueError('Invalid historical hotspot list')
    source_bytes = source_index_bytes(shard)
    from .replica_budget import per_region, source_limit, region_limit
    budget = min(source_limit(settings,source_bytes), settings['max_total_replica_bytes'])
    routes = sorted(set(map(int, route_ids)))
    local = set(map(int, shard.ids))
    if not set(routes) <= local:
        raise ValueError('Routing candidates must belong to source shard')
    remaining = dict(zip(map(int, hot_ids), map(float, weights)))
    if len(remaining) != len(hot_ids) or any(not math.isfinite(w) or w <= 0 for w in remaining.values()):
        raise ValueError('Invalid/duplicate hotspot weight')
    regions, rejected, used_entries = [], [], set()
    total, started = 0, time.monotonic()
    for slot in range(min(c['max_regions'], settings['max_replicas'])):
        if not remaining:
            break
        left = budget - total
        block_cap = min(left,region_limit(settings,source_bytes)) if per_region(settings) else min(left, settings['max_region_bytes'], int(budget * c['per_region_fraction']))
        if block_cap <= 0:
            break
        ids = sorted(remaining, key=lambda v: (-remaining[v], v))
        cfg = dict(settings, max_region_bytes=block_cap,
                   community_generalization_weight=c['generalization_weight'])
        core_cfg=dict(cfg)
        if settings.get('hop_expansion',{}).get('enabled',False):
            core_cfg['max_region_bytes']=int(block_cap*settings['hop_expansion']['core_fraction'])
        meta=metadata(shard)
        meta['community_fixed_bytes'] += c['metadata_reserve_bytes']
        try:
            region = native().replica_community_plan(
                shard.fd, shard.ids, ids, [remaining[v] for v in ids], core_cfg, meta,
                route_ids=[v for v in routes if v not in used_entries])
        except ValueError as error:
            rejected.append(dict(slot=slot, reason=str(error)))
            break
        entries = region['entries']
        if not entries:
            rejected.append(dict(slot=slot, reason='No entry with complete local one-hop neighborhood'))
            break
        estimate = int(region['stats']['estimated_bytes'])
        if estimate > block_cap or total + estimate > budget:
            raise AssertionError('Native complete-region capacity contract violated')
        proof = dict(kind='complete_locality_balls_v2', scope='source_local_records',
                     epoch=shard.meta['epoch'], hops=region['hops'], seeds=region['seeds'],
                     hot_ids=ids, weights=[remaining[v] for v in ids],
                     routing_candidates=[v for v in routes if v not in used_entries],
                     generalization_weight=c['generalization_weight'],
                     hot_closure=settings['community_hot_closure'],
                     members_sha256=hashlib.sha256(np.asarray(region['ids'], dtype='<u8').tobytes()).hexdigest())
        region.update(region_proof=proof, region_stats=region['stats'],
                      source_capacity_bytes=source_bytes, source_capacity_limit=budget,
                      reserved_package_bytes=estimate, selection_policy='hot_entry_community')
        verify(shard, region, cfg)
        if settings.get('hop_expansion',{}).get('enabled',False):
            from .hop_expansion import expand
            region=expand(shard,region,cfg)
            if settings['hop_expansion'].get('promote_covered_entries',False):
                from .hop_expansion import rebind_historical_entries
                region=rebind_historical_entries(shard,region,cfg,
                    [v for v in routes if v not in used_entries])
            estimate=int(region['reserved_package_bytes'])
            if total+estimate>budget:raise AssertionError('Expanded portfolio capacity exceeded')
            entries=region['entries']
        if settings.get('boundary_augmentation',{}).get('enabled'):
            from .boundary_portals import augment
            region=augment(shard,region,settings)
            estimate=region['reserved_package_bytes']
            if total+estimate>settings['max_total_replica_bytes']:raise ValueError('Boundary portfolio physical capacity exceeded')
        regions.append(region)
        total += estimate
        used_entries.update(entries)
        remaining = {v: w for v, w in remaining.items() if v not in used_entries}
    return dict(regions=regions, source_index_bytes=source_bytes, capacity_bytes=budget,
                estimated_physical_bytes=total, remaining_hot_entries=len(remaining),
                selected_routing_entries=len(used_entries), rejected=rejected,
                planning_and_verification_seconds=time.monotonic() - started)
