"""Complete h-core + attached complete one-hop balls, selected by closure cuts.

The linear anchor prize is a surrogate. No recall or submodular guarantee is
claimed. The certificate proves original adjacency and attachment independently.
"""
import copy
import hashlib
import json
import time
import numpy as np
from .index import native
from .community import metadata
from .planning_context import limited_settings


def digest(ids):
    return hashlib.sha256(np.asarray(ids, dtype='<u8').tobytes()).hexdigest()


def planning_metadata(shard, core_ids, core_proof, settings, planning=None):
    m=planning.metadata() if planning else metadata(shard)
    # The complete certificate is persisted in meta.json. Count its copied core
    # and bounded anchor list, not just records and quantization dependencies.
    c=settings['hop_expansion']
    m['community_fixed_bytes']+=len(json.dumps(core_proof).encode())+24*len(core_ids)+24*c['candidates']*c['rounds']+65536
    return m


def expand(shard, core, settings, work_weights=None):
    from .community import verify
    verify(shard, core, settings)
    proof = core['region_proof']
    weights = work_weights or dict(zip(proof['hot_ids'], proof['weights']))
    weights = sorted((int(k), float(v)) for k, v in weights.items())
    result = native().replica_hop_expand(shard.fd, shard.ids, core['ids'],
                [k for k, v in weights], [v for k, v in weights], limited_settings(settings), planning_metadata(shard,core['ids'],proof,settings))
    out = copy.deepcopy(core)
    out.update(ids=result['ids'], stats=result['stats'], region_stats=result['stats'],
               reserved_package_bytes=result['stats']['estimated_bytes'])
    out['region_proof'] = dict(kind='complete_attached_hop_balls_v1',
        scope='source_local_records', epoch=shard.meta['epoch'],
        core_ids=core['ids'], core_entries=core['entries'], core_proof=proof,
        anchors=result['anchors'], members_sha256=digest(result['ids']))
    verify_expansion(shard, out, settings)
    return out


def breadth_expand(shard, core, settings, planning=None, on_admit=None, batch_rows=1024):
    """Capacity-bounded breadth extension of a complete core, in whole balls."""
    from .community import verify
    verify(shard, core, settings, planning=planning)
    proof = core['region_proof']
    result = native().replica_hop_breadth_expand(shard.fd, shard.ids, core['ids'],
        limited_settings(settings), planning_metadata(shard, core['ids'], proof, settings, planning), on_admit, batch_rows)
    out = copy.deepcopy(core)
    out.update(ids=result['ids'], stats=result['stats'], region_stats=result['stats'],
               reserved_package_bytes=result['stats']['estimated_bytes'])
    out['region_proof'] = dict(kind='complete_attached_hop_balls_v1',
        scope='source_local_records', epoch=shard.meta['epoch'],
        core_ids=core['ids'], core_entries=core['entries'], core_proof=proof,
        anchors=result['anchors'], members_sha256=digest(result['ids']))
    verify_expansion(shard, out, settings, planning=planning)
    return out


def capacity_diagnostics(shard, plan, settings, potential_entries, planning=None):
    """Report complete added shells separately from a partially filled exterior."""
    proof = plan['region_proof']
    return native().replica_hop_capacity_inspect(shard.fd, shard.ids,
        proof['core_ids'], plan['ids'], sorted(set(potential_entries)), limited_settings(settings),
        planning_metadata(shard, proof['core_ids'], proof['core_proof'], settings, planning))


def rebind_historical_entries(shard, plan, settings, historical_entries, planning=None):
    """Reuse an existing complete region for additional historical portals.

    Membership and the attached-ball certificate remain identical. Every new
    portal must have its entire source-local one-hop neighborhood in the
    region. This avoids routing an entry to a tiny overlapping replica merely
    because that replica was planned first. It does not assert better recall;
    callers must validate quality and publish/retire mappings atomically.
    """
    candidates = list(historical_entries)
    if any(type(v) is not int or v < 0 or v >= 2**64 for v in candidates):
        raise ValueError('Invalid historical entry ID')
    proof = plan['region_proof']
    if proof.get('kind') != 'complete_attached_hop_balls_v1':
        raise ValueError('Entry rebinding requires an attached-hop region')
    if proof.get('epoch') != shard.meta['epoch']:
        raise ValueError('Entry rebinding epoch mismatch')
    out = copy.deepcopy(plan)
    old_bytes = len(json.dumps(plan).encode())
    candidates = sorted(set(candidates) | set(proof.get('routing_candidates', plan['entries'])))
    out['region_proof']['routing_candidates'] = candidates
    out['entries'] = capacity_diagnostics(shard, out, settings, candidates, planning)['safe_entries']
    if not set(plan['entries']) <= set(out['entries']):
        raise ValueError('Entry rebinding would remove an existing portal')
    # Reserve the extra metadata even though the usual proof reserve already
    # provides headroom. Actual package bytes are still checked on publication.
    extra = max(0, len(json.dumps(out).encode()) - old_bytes)
    out['reserved_package_bytes'] = plan['reserved_package_bytes'] + extra
    if out['reserved_package_bytes'] > settings['max_region_bytes']:
        raise ValueError('Entry rebinding metadata exceeds capacity')
    verify_expansion(shard, out, settings, planning=planning)
    return out


def open_covered_entries(shard, plan, settings, route_entries, planning=None):
    """Open every included routing sample, including incomplete boundary balls.

    The complete-core/attached-ball certificate still verifies the graph.
    Membership admission deliberately makes no residence or recall promise.
    """
    out=copy.deepcopy(plan)
    candidates=sorted(set(map(int,route_entries)))
    entries=sorted(set(plan['ids']).intersection(candidates))
    if not set(plan['region_proof']['core_entries'])<=set(entries):
        raise ValueError('Covered routing directory omits core entries')
    old_bytes=len(json.dumps(plan).encode())
    out['region_proof'].update(routing_candidates=candidates,
                              routing_policy='covered_membership_v1')
    out.update(entries=entries,entry_admission='covered_membership_v1')
    out['reserved_package_bytes']+=max(0,len(json.dumps(out).encode())-old_bytes)
    if out['reserved_package_bytes']>settings['max_region_bytes']:
        raise ValueError('Covered entry metadata exceeds capacity')
    verify_expansion(shard,out,settings,planning=planning)
    return out


def capacity_expand(shard, core, settings, work_weights, candidate_schedule):
    """Closure cuts at decreasing granularity, then whole-ball budget filling.

    A Lagrangian cut can leave a discrete budget gap. The final breadth pass is
    explicitly measured; it is not claimed to optimize the closure objective.
    One flat certificate retains the original core and every attached ball.
    """
    from .community import verify
    verify(shard, core, settings)
    limit=settings['hop_expansion']['candidates']
    if not candidate_schedule or any(type(n)is not int or not 0<n<=limit for n in candidate_schedule):
        raise ValueError('Invalid capacity candidate schedule')
    proof=core['region_proof'];meta=planning_metadata(shard,core['ids'],proof,settings)
    weights=sorted((int(k),float(v))for k,v in work_weights.items())
    ids=core['ids'];anchors=[];phases=[];started=time.monotonic()
    deadline=started+settings['community_max_plan_ms']/1000
    result=None
    for count in candidate_schedule:
        remaining=deadline-time.monotonic()
        if remaining<30:break
        cfg=copy.deepcopy(settings);cfg['hop_expansion']['candidates']=count
        cfg['community_max_plan_ms']=max(1,int(remaining*1000))
        result=native().replica_hop_expand(shard.fd,shard.ids,ids,
            [k for k,v in weights],[v for k,v in weights],cfg,meta)
        phases.append(dict(method='closure_cut',candidate_limit=count,added_rows=len(result['ids'])-len(ids),stats=dict(result['stats'])))
        ids=result['ids'];anchors.extend(result['anchors'])
        if settings['max_region_bytes']-result['stats']['estimated_bytes']<shard.meta['record_bytes']+8:break
    # Independently bounded completion phase; construction/verification timings
    # expose this extra cost instead of presenting it as a cut-optimal solution.
    result=native().replica_hop_breadth_expand(shard.fd,shard.ids,ids,settings,meta)
    phases.append(dict(method='breadth_fill',added_rows=len(result['ids'])-len(ids),stats=dict(result['stats'])))
    anchors.extend(result['anchors'])
    stats=dict(result['stats'],core_rows=len(core['ids']),selected_balls=len(anchors),
        planning_seconds=time.monotonic()-started,capacity_phases=phases,
        breadth_fill_rows=phases[-1]['added_rows'])
    out=copy.deepcopy(core);out.update(ids=result['ids'],stats=stats,region_stats=stats,
        reserved_package_bytes=stats['estimated_bytes'])
    out['region_proof']=dict(kind='complete_attached_hop_balls_v1',scope='source_local_records',
        epoch=shard.meta['epoch'],core_ids=core['ids'],core_entries=core['entries'],core_proof=proof,
        anchors=anchors,members_sha256=digest(result['ids']))
    verify_expansion(shard,out,settings)
    return out


def fast_capacity_expand(shard, core, settings, work_weights, *, cut_budget_ms=10000,
                         cut_rounds=4, cut_candidates=256):
    """Bound the quality-improvement pass, then fill with complete attached balls.

    The cut budget excludes core construction, breadth filling and independent
    verification. It is a cooperative deadline, not an end-to-end time promise.
    """
    from .community import verify
    for value in (cut_budget_ms, cut_rounds, cut_candidates):
        if type(value) is not int or value <= 0:
            raise ValueError('Fast expansion limits must be positive integers')
    if (cut_rounds > settings['hop_expansion']['rounds'] or
            cut_candidates > settings['hop_expansion']['candidates']):
        raise ValueError('Fast cut exceeds certificate bounds')
    verify(shard, core, settings)
    started=time.monotonic();proof=core['region_proof']
    meta=planning_metadata(shard,core['ids'],proof,settings)
    cfg=copy.deepcopy(settings);cfg['community_max_plan_ms']=cut_budget_ms
    cfg['hop_expansion'].update(rounds=cut_rounds,candidates=cut_candidates)
    weights=sorted((int(k),float(v)) for k,v in work_weights.items())
    cut=native().replica_hop_expand(shard.fd,shard.ids,core['ids'],
        [k for k,v in weights],[v for k,v in weights],cfg,meta)
    cut_done=time.monotonic()
    fill=native().replica_hop_breadth_expand(shard.fd,shard.ids,cut['ids'],settings,meta)
    anchors=cut['anchors']+fill['anchors']
    if len(anchors)>settings['hop_expansion']['rounds']*settings['hop_expansion']['candidates']:
        raise ValueError('Combined fast expansion exceeds certificate bounds')
    out=copy.deepcopy(core)
    stats=dict(fill['stats'],core_rows=len(core['ids']),selected_balls=len(anchors),
        cut_seconds=cut_done-started,fill_seconds=time.monotonic()-cut_done,
        cut_stats=dict(cut['stats']),breadth_fill_rows=len(fill['ids'])-len(cut['ids']))
    out.update(ids=fill['ids'],stats=stats,region_stats=stats,reserved_package_bytes=stats['estimated_bytes'])
    out['region_proof']=dict(kind='complete_attached_hop_balls_v1',scope='source_local_records',
        epoch=shard.meta['epoch'],core_ids=core['ids'],core_entries=core['entries'],
        core_proof=proof,anchors=anchors,members_sha256=digest(fill['ids']))
    before=time.monotonic();verify_expansion(shard,out,settings)
    stats['verification_seconds']=time.monotonic()-before
    stats['fast_expansion_seconds']=time.monotonic()-started
    return out


def verify_expansion(shard, plan, settings, planning=None):
    if planning is not None:
        return planning.verified('attached', plan, settings,
                                 lambda: _verify_expansion(shard, plan, settings, planning))
    return _verify_expansion(shard, plan, settings)


def _verify_expansion(shard, plan, settings, planning=None):
    from .community import verify
    p = plan['region_proof']
    if p.get('scope') != 'source_local_records' or p.get('epoch') != shard.meta['epoch']:
        raise ValueError('Invalid attached hop epoch/scope')
    if plan['ids'] != sorted(set(plan['ids'])) or digest(plan['ids']) != p['members_sha256']:
        raise ValueError('Invalid attached hop members')
    # Additional historical entries require an explicit, independently checked
    # complete-one-hop contract; old proofs retain their exact entry set.
    if 'routing_candidates' not in p:
        if plan['entries'] != p['core_entries']:
            raise ValueError('Attached hop entry contract mismatch')
    elif (p['routing_candidates'] != sorted(set(p['routing_candidates'])) or
          not set(p['core_entries']) <= set(plan['entries']) or
          not set(plan['entries']) <= set(p['routing_candidates'])):
        raise ValueError('Invalid expanded entry candidates')
    if p['core_proof']['kind'] == 'complete_attached_hop_balls_v1':
        raise ValueError('Nested expansion certificates are not supported')
    verify(shard, dict(ids=p['core_ids'], entries=p['core_entries'],
                     region_proof=p['core_proof']), settings, planning=planning)
    result = native().replica_hop_expand(shard.fd, shard.ids, p['core_ids'], [], [],
                   limited_settings(settings), planning_metadata(shard,p['core_ids'],p['core_proof'],settings,planning), p['anchors'], True)
    if result['ids'] != plan['ids']:
        raise ValueError('Attached hop completeness mismatch')
    if 'routing_candidates' in p:
        if p.get('routing_policy')=='covered_membership_v1':
            portals=sorted(set(plan['ids']).intersection(p['routing_candidates']))
        else:
            if 'routing_policy' in p:raise ValueError('Unknown routing admission proof')
            portals = capacity_diagnostics(shard, plan, settings, p['routing_candidates'], planning)['safe_entries']
        if plan['entries'] != portals:
            raise ValueError('Expanded entries violate routing admission contract')
    return dict(p, verified=True, verification_stats=result['stats'])
