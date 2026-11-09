"""Bounded automatic small-region maintenance, separate from layout advice."""
import copy
import time


def covered_route_entries(controller, source):
    import numpy as np
    engine=controller.engine
    ids=np.asarray(engine.route_bank['ids'],dtype=np.uint64)
    positions=np.searchsorted(engine.ids,ids)
    return sorted(map(int,ids[engine.membership[positions,source]]))


def remaining_capacity(controller, source, source_bytes=None, tier='small', targets=1):
    """Cheap preflight under the controller lock; prepare rechecks before use.

    Draining copies still occupy space. Missing index metadata only permits a
    global-budget precheck, never a substitute for prepare's source RPC.
    """
    from .replica_budget import available, per_region
    c = controller; b = c.b
    budget = available(c, source, source_bytes)
    if source_bytes is not None:
        alpha = (b['capacity_policy'][tier+'_alpha']*(targets if tier=='small' else 1)
                 if per_region(b) else b['partitioned_regions']['total_alpha'])
        budget = min(budget, int(source_bytes*alpha))
    return budget


def prepare(controller, source, preferred, observations, *, tier='small'):
    from .community import hotspots
    c = controller; b = c.b; policy = b['automatic_pipeline']
    candidate_limit=0 if b.get('entry_admission',{}).get('enabled') else policy.get('max_entry_candidates',0)
    if candidate_limit and (not policy['max_seed_entries']<=candidate_limit<=4096 or policy['max_targets']!=1):
        raise ValueError('Automatic entry promotion requires one target and bounded historical candidates')
    ranking_settings=dict(b,community_top_k=max(b['community_top_k'],candidate_limit,
                          b.get('online_policy',{}).get('large_seed_entries',0) if tier=='large' else 0),
                          _include_replica_observations=tier=='large')
    ranked, groups = hotspots(observations, source, ranking_settings)
    historical_entries=[e for e,w in ranked[:candidate_limit]] if candidate_limit else []
    online=b.get('online_policy',{})
    seed_limit=online['large_seed_entries'] if tier=='large' else policy['max_seed_entries']
    ranked = ranked[:seed_limit]
    training=sorted({q['query_id'] for e,w in ranked for q in groups[e]})
    if len(training)<b['min_group_queries']:
        raise ValueError('Insufficient automatic hotspot query coverage')
    source_bytes = c.rpc(source, 'load')['source_index_bytes']
    now = time.monotonic()
    with c.lock:
        if not c.pair_eligible(source, preferred, now):
            raise ValueError('Automatic source/target pressure changed before planning')
        from .replica_budget import available, reservations, per_region, region_limit
        active = reservations(c)
        max_targets=online['large_max_targets'] if tier=='large' else policy['max_targets']
        budget = remaining_capacity(c, source, source_bytes, tier, max_targets)
        if budget < policy['min_capacity_bytes']:
            raise ValueError('Automatic remaining replica capacity too small')
        eligible = [n for n in c.loads if n != source and c.pair_eligible(source,n,now)]
        eligible.sort(key=lambda n:(n != preferred, c.loads[n]['util'], n))
        eligible=[n for n in eligible if available(c,source,source_bytes,n)>=policy['min_capacity_bytes']]
        targets = eligible[:min(max_targets, b['max_replicas']-len(active), len(ranked))]
        if not targets:
            raise ValueError('Automatic replica has no target or slot')
        budget = min(budget, remaining_capacity(c,source,source_bytes,tier,len(targets)))
        cap = min(region_limit(b,source_bytes,tier), budget//len(targets),
                  *(available(c,source,source_bytes,n) for n in targets))
        budget=cap*len(targets)
        if cap<policy['min_capacity_bytes']:
            raise ValueError('Automatic target remaining capacity too small')
        if any(c.loads[n]['free_bytes'] < cap+b['reserve_disk_bytes'] for n in targets):
            raise ValueError('Automatic target free space insufficient')
        epochs = {n:c.loads[n]['epoch'] for n in [source]+targets}
    layout = copy.deepcopy(b['partitioned_regions'])
    layout.update(total_alpha=budget/source_bytes,
                  target_weights={str(n):1. for n in targets},
                  deadline_s=online['large_planning_seconds'] if tier=='large' else policy['planning_seconds'],
                  finish_reserve_s=policy['finish_reserve_seconds'])
    if per_region(b):layout['replica_tier']=tier
    # Source checks this explicit budget against both alpha and its own index.
    layout['capacity_bytes'] = budget
    if b.get('entry_admission',{}).get('enabled'):
        # All source-owned HNSW samples, independent of hot history and holdout.
        # Hot IDs choose the graph; membership alone opens its route portals.
        layout['covered_route_entries']=covered_route_entries(c,source)
    elif candidate_limit and len(targets)==1:
        # Candidates affect routing metadata only, never core seeds or expansion.
        layout['historical_entries']=historical_entries
    history = dict(hot_ids=[e for e,w in ranked], weights=[w for e,w in ranked],
                   training_query_ids=training)
    if candidate_limit:
        history['training_query_ids']=sorted({q['query_id'] for e in historical_entries for q in groups[e]})
    return history, layout, epochs


def run(controller, source, target, observations, *, tier='small', replace_versions=()):
    c = controller; started = time.monotonic()
    history, layout, epochs = prepare(c,source,target,observations,tier=tier)
    if tier=='large':
        layout.update(defer_publication=True,require_validation=True)
    c.log('AUTO_PIPELINE_STARTED',source=source,targets=list(layout['target_weights']),
          entries=len(history['hot_ids']),capacity_bytes=layout['capacity_bytes'],tier=tier,
          planning_seconds=layout['deadline_s'])

    def gate(plan):
        now=time.monotonic()
        with c.lock:
            nodes=(source,plan['target'])
            if not all(c.fresh(n,now) and c.loads[n]['epoch']==epochs[n] for n in nodes):
                raise ValueError('Automatic publication has stale load or changed epoch')
            dest=c.loads[plan['target']]
            online=c.b.get('online_policy',{})
            from .online_replica_policy import pressure
            queue=pressure(dest) if online.get('enabled') else dest.get('backlog_estimate_ms',dest['queue_ms'])
            if queue is None or dest['util']>c.b['target_util_limit'] or queue>c.b['target_queue_limit_ms']:
                if tier=='small' and online.get('enabled') and online.get('publish_busy_target_paused') and c.b.get('publication_mode')=='ready':
                    plan['_initial_fraction']=0.
                    plan['_pressure_start_paused']=True
                else:raise ValueError('Automatic publication target pressure too high')
        c.log('AUTO_PUBLICATION_CHECK',source=source,target=plan['target'],
              elapsed_seconds=now-started,reserved_bytes=plan['reserved_package_bytes'])

    report,versions=c.build_partitioned_online(source,history,layout,_automatic=True,
                                               _publication_gate=gate)
    if tier=='large':
        try:c.activate_portfolio(versions,replace_versions)
        except BaseException:
            for version in versions:c.retire(version,wait=False)
            raise
    c.log('AUTO_PIPELINE_COMPLETED',source=source,versions=versions,
          elapsed_seconds=time.monotonic()-started,report=report)
    return report
