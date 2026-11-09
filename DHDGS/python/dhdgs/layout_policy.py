"""Auditable replica portfolio planning; never publishes or starts a copy.

Costs are measured for a whole global request, including all return branches.
Every alternative is a complete certified portfolio, not an independently
additive piece of a graph. Immutable graph construction stays in replica_layout.
"""
from __future__ import annotations
import math


def _number(value, name, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError('Invalid '+name)
    return float(value)


def validate_policy(config):
    required={'small_alpha','large_alpha','capacity_headroom','observation_max_age_ms','cost_max_age_ms',
              'large_sustained_windows','large_recurring_windows','cooldown_ms','source_queue_trigger_ms',
              'target_queue_limit_ms','recall_min','coordinate_passes','share_step','min_capacity_gain','build_amortization_factor'}
    if set(config)!=required:raise ValueError('Incomplete or unknown layout policy keys')
    for key,value in config.items():_number(value,'layout '+key)
    if (not 0<config['small_alpha']<=.4 or not config['small_alpha']<=config['large_alpha']<=.8
            or not 0<config['capacity_headroom']<=1 or not 0<config['share_step']<=1
            or not 0<config['recall_min']<=1 or not 1<=config['coordinate_passes']<=100
            or config['large_sustained_windows']<1 or config['large_recurring_windows']<1
            or any(type(config[k])is not int for k in ['coordinate_passes','large_sustained_windows','large_recurring_windows'])):
        raise ValueError('Invalid replica layout policy')


def plan_layout(observation, candidates, config, now_ms):
    """Rank supplied certified portfolios and optimize exclusive entry shares.

    observation.resources maps resource ID to available units/second (e.g.
    cpu:0=effective cores, slots:0=workers, net:0=bytes/s). Costs use matching
    units/query. Fractions of all entries must sum to one, including an explicit
    unmodelled/other cohort. Nodes' free_bytes are already net of reservations.
    No quality, lifecycle, demand or cost observation is inferred from missing data.
    """
    validate_policy(config)
    now=_number(now_ms,'now');cfg=config
    age=now-_number(observation['observed_ms'],'observed_ms')
    if age<0 or age>cfg['observation_max_age_ms']:
        return dict(action='hold',reason='stale_observation',candidates=[])
    if not observation.get('healthy',False):
        return dict(action='hold',reason='unhealthy_observation',candidates=[])
    resources={str(k):_number(v,'resource '+str(k),True)*cfg['capacity_headroom'] for k,v in observation['resources'].items()}
    entries=observation['entries'];source=str(observation['source_resource'])
    if source not in resources or not resources:raise ValueError('Unknown source resource')
    def costs(values):
        if set(values)!=set(resources):raise ValueError('Incomplete resource cost vector')
        return {k:_number(v,'cost '+k)for k,v in values.items()}
    baseline={str(e['id']):costs(e['costs'])for e in entries}
    weights={str(e['id']):_number(e['fraction'],'entry fraction')for e in entries}
    if len(weights)!=len(entries) or abs(sum(weights.values())-1)>1e-6:raise ValueError('Invalid cohort distribution')
    def evaluate(routes,shares):
        demand={r:0. for r in resources}
        for entry,base in baseline.items():
            choices=[j for j,x in enumerate(routes)if str(x['entry'])==entry]
            total=sum(shares[j]for j in choices)
            if total>1+1e-9:raise ValueError('Entry share exceeds one')
            for resource in resources:
                demand[resource]+=weights[entry]*((1-total)*base[resource]+sum(shares[j]*routes[j]['costs'][resource]for j in choices))
        limits={r:resources[r]/d for r,d in demand.items()if d>0}
        if not limits:raise ValueError('Zero measured work')
        bottleneck=min(limits,key=limits.get)
        return dict(qps_capacity=limits[bottleneck],demand=demand,bottleneck=bottleneck)
    current=evaluate([],[])
    if 'current' in observation:
        # Same resource/cost contract as candidate portfolios; optional current
        # routing avoids attributing an existing replica's gain to a new copy.
        current_routes=observation['current']['routes']
        for route in current_routes:
            if str(route['entry']) not in baseline:raise ValueError('Unknown current entry')
            costs(route['costs'])
        current=evaluate(current_routes,observation['current']['shares'])
    source_bytes=_number(observation['source_bytes'],'source_bytes',True)
    horizon=_number(observation['benefit_horizon_s'],'horizon')
    offered=_number(observation['offered_qps'],'offered qps')
    source_queue=_number(observation['source_runnable_p95_ms'],'source runnable')
    rows=[]
    for candidate in candidates:
        row=dict(id=candidate['id'],kind=candidate['kind'],eligible=False,reasons=[])
        reason=row['reasons'];tier=candidate['tier'];routes=candidate['routes']
        if tier not in ('small','large'):raise ValueError('Unknown replica tier')
        if candidate['kind'] not in ('single','identical_copies','distinct_small','partitioned_large'):raise ValueError('Unknown layout kind')
        # Portfolio bytes include existing retained copies. Peak explicitly
        # includes old draining versions and staged replacements.
        physical={str(k):_number(v,'physical bytes')for k,v in candidate['physical_bytes_by_node'].items()}
        peak={str(k):_number(v,'peak bytes')for k,v in candidate['peak_bytes_by_node'].items()}
        if set(physical)!=set(peak) or any(peak[k]<v for k,v in physical.items()):raise ValueError('Invalid transition bytes')
        cap=source_bytes*cfg[tier+'_alpha']
        if sum(peak.values())>cap:reason.append('aggregate_transition_capacity')
        for node,value in peak.items():
            free=observation['node_free_bytes'].get(node)
            retained=observation.get('current_bytes_by_node',{}).get(node,0)
            if free is None or max(0,value-retained)>free:reason.append('target_capacity:'+node)
        if tier=='large' and not (observation['sustained_windows']>=cfg['large_sustained_windows'] or observation['recurring_windows']>=cfg['large_recurring_windows']):reason.append('hotspot_not_persistent')
        if now-observation.get('last_change_ms',-1e30)<cfg['cooldown_ms']:reason.append('cooldown')
        if source_queue<cfg['source_queue_trigger_ms']:reason.append('source_not_queued')
        if not candidate.get('qualified',False):reason.append('unqualified_graph')
        measured=_number(candidate['cost_observed_ms'],'cost timestamp')
        if measured>now or now-measured>cfg['cost_max_age_ms']:reason.append('stale_cost')
        for route in routes:
            if str(route['entry']) not in baseline:raise ValueError('Unknown route entry')
            costs(route['costs'])
            if not 0<=_number(route['max_share'],'max share')<=1:raise ValueError('Invalid max share')
            if _number(route['recall'],'route recall')<cfg['recall_min']:reason.append('route_recall')
            queue=observation['target_runnable_p95_ms'].get(str(route['target']))
            if queue is None or queue>cfg['target_queue_limit_ms']:reason.append('target_pressure:'+str(route['target']))
        shares=[0.]*len(routes)
        # Deterministic bounded coordinate search; model heuristic, not a global
        # optimum. Downward moves allow moving a shared entry between targets.
        state=evaluate(routes,shares)
        def score(x):return (x['qps_capacity'],-x['demand'][source])
        for _ in range(cfg['coordinate_passes']):
            improved=False
            for j,route in enumerate(routes):
                total=sum(shares[k]for k,x in enumerate(routes)if str(x['entry'])==str(route['entry'])and k!=j)
                upper=min(route['max_share'],1-total)
                options={0.,upper,shares[j]}
                steps=math.ceil(upper/cfg['share_step'])
                options.update(min(upper,k*cfg['share_step'])for k in range(steps+1))
                best,chosen=state,shares[j]
                for fraction in sorted(options):
                    trial=shares.copy();trial[j]=fraction;value=evaluate(routes,trial)
                    if score(value)>score(best):best,chosen=value,fraction
                if chosen!=shares[j]:shares[j]=chosen;state=best;improved=True
            if not improved:break
        gain=state['qps_capacity']/current['qps_capacity']-1
        saved=current['demand'][source]-state['demand'][source]
        build=_number(candidate['build_seconds'],'build seconds')
        build_source=_number(candidate['build_source_work'],'build source work')
        active=max(0,horizon-build)
        saving_rate=min(offered,state['qps_capacity'])*max(0,saved)
        paid=saving_rate*active
        if gain<cfg['min_capacity_gain']:reason.append('insufficient_capacity_gain')
        if saved<=0:reason.append('no_net_source_unload')
        if not active or paid<cfg['build_amortization_factor']*build_source:reason.append('not_amortized')
        net_completions=(min(offered,state['qps_capacity'])-min(offered,current['qps_capacity']))*active-_number(candidate['build_lost_queries'],'build lost queries')
        if net_completions<0:reason.append('negative_foreground_benefit')
        row.update(eligible=not reason,shares=[dict(entry=x['entry'],target=x['target'],fraction=v)for x,v in zip(routes,shares)],
                   predicted=state,capacity_gain=gain,source_work_saved_per_query=saved,
                   build_payback_s=build_source/saving_rate if saving_rate else None,
                   net_completions=net_completions,total_physical_bytes=sum(physical.values()),peak_physical_bytes=sum(peak.values()))
        rows.append(row)
    eligible=[x for x in rows if x['eligible']]
    best=max(eligible,key=lambda x:(x['net_completions'],x['predicted']['qps_capacity'],-x['total_physical_bytes']))if eligible else None
    return dict(action='review_candidate'if best else 'hold',selected=best['id']if best else None,
                current=current,candidates=rows,scope='Advisory only; measured cost model, not validated online optimality')
