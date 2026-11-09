"""Low-frequency, deterministic replica policy; no query or RPC work here.

A healthy-observation clock prevents outages from masquerading as cold demand.
Physical installation/drain and routing remain the controller's responsibility.
"""
from dataclasses import dataclass
import hashlib
import json
import math


def family_key(plan):
    """Equivalent content and entry contract, independent of physical version."""
    proof = plan.get('region_proof', {})
    content = proof.get('members_sha256')
    if content is None:
        content = hashlib.sha256(json.dumps(sorted(plan['ids']), separators=(',', ':')).encode()).hexdigest()
    identity = [plan['source'], proof.get('epoch', plan.get('epoch', 'unknown')),
                content, sorted(plan['entries']), plan['min_score'], plan['max_score']]
    return hashlib.sha256(json.dumps(identity, separators=(',', ':')).encode()).hexdigest()


@dataclass
class Heat:
    source: int
    epoch: str
    entries: frozenset
    lower: float
    upper: float
    created: float
    touched: float
    active: bool = True
    fast: float = 0.
    slow: float = 0.
    peak: float = 0.
    cooling: float | None = None
    withdrawn: float | None = None
    cold_run: float = 0.
    gap_ms: float = 0.
    rebounds: float = 0.
    ready_ms: float = 0.
    hold_ms: float = 0.
    samples: int = 0
    state: str = 'HOT'


class AdaptiveDecay:
    def __init__(self, settings):
        self.c = settings
        self.states = {}
        self.clock = 0.
        self.last_tick = None
        self.unknown_windows = 0

    def register(self, plan, now, ready_seconds, family=None):
        key = family or family_key(plan)
        h = self.states.get(key)
        epoch = plan.get('region_proof', {}).get('epoch', plan.get('epoch', 'unknown'))
        if h is not None and h.active:
            h.entries = h.entries | frozenset(plan['entries'])
            h.ready_ms = max(h.ready_ms, max(0., ready_seconds)*1000)
            h.touched = now
            return key
        if h is None:
            self.prune(now)
            if len(self.states) >= self.c['history_limit']:
                inactive = [(s.touched, k) for k, s in self.states.items() if not s.active]
                if not inactive:
                    raise ValueError('Adaptive replica history full of active families')
                del self.states[min(inactive)[1]]
            entries = frozenset(plan['entries'])
            matches = [(len(entries & old.entries)/max(1, len(entries | old.entries)), old)
                       for old in self.states.values()
                       if not old.active and old.source == plan['source'] and old.epoch == epoch]
            match = max(matches, key=lambda v:v[0], default=(0., None))
            h = Heat(plan['source'], epoch, entries, plan['min_score'],
                     plan['max_score'], self.clock, now)
            if match[0] >= self.c['history_entry_overlap']:
                old = match[1]
                h.gap_ms, h.rebounds, h.ready_ms = old.gap_ms, old.rebounds, old.ready_ms
                h.cooling, h.withdrawn, h.cold_run = old.cooling, old.withdrawn, old.cold_run
                h.active = False
                h.rebounds *= math.exp(-math.log(2)*max(0.,now-old.touched)*1000/self.c['peak_half_life_ms'])
                self._rebound(h)
            self.states[key] = h
        elif not h.active:
            h.rebounds *= math.exp(-math.log(2)*max(0.,now-h.touched)*1000/self.c['peak_half_life_ms'])
            self._rebound(h)
            h.created = self.clock
        h.active = True
        h.state = 'HOT'
        h.cooling = None
        h.withdrawn = None
        h.cold_run = 0.
        h.touched = now
        h.ready_ms = max(h.ready_ms, max(0., ready_seconds) * 1000)
        return key

    def _rebound(self, h):
        if h.cooling is not None:
            gap = max(0., self.clock - h.cooling) * 1000
            if not h.active and h.withdrawn is not None:
                absence = max(0., self.clock-h.withdrawn)*1000
                if absence > self.c['rebound_window_ms']:return
                gap = h.cold_run*1000+absence
            elif gap > self.c['rebound_window_ms']:
                return
            w = self.c['history_ewma_weight']
            h.gap_ms = gap if not h.gap_ms else (1-w)*h.gap_ms + w*gap
            h.rebounds = min(self.c['max_rebound_score'], h.rebounds + 1.)

    def withdraw(self, key, now):
        h = self.states[key]
        h.active = False
        h.state = 'WITHDRAWN'
        h.withdrawn = self.clock
        h.touched = now
        if h.cooling is None:
            h.cooling = self.clock

    def prune(self, now):
        expired = [k for k, h in self.states.items()
                   if not h.active and now-h.touched > self.c['history_ttl_ms']/1000]
        for k in expired:
            del self.states[k]

    def counts(self, observations):
        """Count original entry demand, including requests routed to replicas."""
        out = {k: 0 for k, h in self.states.items() if h.active}
        for q in observations:
            for k in out:
                h = self.states[k]
                if q['source'] == h.source and q['entry'] in h.entries and h.lower <= q['score'] <= h.upper:
                    out[k] += 1
        return out

    def tick(self, now, counts, *, healthy, sample_every, pressure=0.):
        previous, self.last_tick = self.last_tick, now
        self.prune(now)
        if previous is None:
            return []
        dt = now-previous
        if not healthy or dt <= 0 or dt*1000 > self.c['max_sample_gap_ms']:
            self.unknown_windows += 1
            return []
        self.clock += dt
        c = self.c
        alpha = lambda half_ms: math.exp(-math.log(2)*dt*1000/half_ms)
        af, ass, ap = (alpha(c[k]) for k in ('fast_half_life_ms', 'slow_half_life_ms', 'peak_half_life_ms'))
        actions = []
        for key, h in self.states.items():
            if not h.active:
                continue
            rate = counts.get(key, 0)*sample_every/dt
            h.samples += counts.get(key, 0)
            h.fast = af*h.fast+(1-af)*rate
            h.slow = ass*h.slow+(1-ass)*rate
            h.peak = max(h.fast, ap*h.peak)
            h.rebounds *= ap
            cold = max(c['min_keep_qps'], c['cold_ratio']*h.peak)
            hot = max(c['reheat_qps'], c['reheat_ratio']*h.peak)
            is_cold = max(h.fast, h.slow) <= cold
            if h.fast >= hot:
                if h.cooling is not None:
                    self._rebound(h)
                    actions.append(dict(kind='reheat', family=key, gap_ms=h.gap_ms))
                h.cooling = None
                h.cold_run = 0.
                h.state = 'HOT'
            elif is_cold:
                if h.cooling is None:
                    h.cooling = self.clock
                    h.state = 'COOLING'
                    actions.append(dict(kind='cooling', family=key))
                h.cold_run += dt
            else:
                # Ambiguous middle band is not continuous cold evidence.
                h.cold_run = 0.
            confidence = min(1., h.rebounds / c['rebound_confidence_events'])
            raw = c['base_hold_ms'] + c['build_cost_weight']*h.ready_ms + c['rebound_weight']*h.gap_ms*confidence
            h.hold_ms = max(c['hold_min_ms'], min(c['hold_max_ms'], raw*(1-c['pressure_discount']*max(0., min(1., pressure)))))
            if h.cooling is not None and h.cold_run*1000 >= h.hold_ms and (self.clock-h.created)*1000 >= c['min_residency_ms']:
                actions.append(dict(kind='retire', family=key, fast_qps=h.fast, slow_qps=h.slow,
                                    hold_ms=h.hold_ms, cold_ms=h.cold_run*1000))
        return actions

    def snapshot(self):
        return dict(healthy_clock_seconds=self.clock, unknown_windows=self.unknown_windows,
                    families={k: dict(state=h.state, active=h.active, fast_qps=h.fast,
                                      slow_qps=h.slow, peak_qps=h.peak, hold_ms=h.hold_ms,
                                      cold_ms=h.cold_run*1000, gap_ms=h.gap_ms,
                                      rebound_score=h.rebounds, ready_ms=h.ready_ms)
                              for k, h in self.states.items()})


def expansion_decision(regions, loads, rate, settings, max_fraction, now):
    """Conservative extra-copy proposal, never a query fanout instruction.

    Costs are calibration observations on existing copies. A prospective target
    is explicitly priced using a safety-multiplied sibling cost, then must pass
    its own validation before publication. None/negative costs do not imply zero.
    """
    if not regions or rate <= 0:
        return dict(action='hold', reason='no_demand')
    source = regions[0]['source']
    validation = [r.get('validation', {}) for r in regions]
    if settings['require_qualified'] and any(not r.get('qualified', False) for r in regions):
        return dict(action='hold', reason='unqualified_region')
    if any(v.get('count', 0) < settings['min_validation_queries'] or
           v.get('source_saved_s') is None or v.get('target_added_s') is None for v in validation):
        return dict(action='hold', reason='insufficient_cost_evidence')
    if any(now-r.get('validated_at',r['published']) > settings['cost_max_age_ms']/1000 for r in regions):
        return dict(action='hold', reason='stale_cost_evidence')
    gain = min(v['source_saved_s'] for v in validation)
    cost = max(v['target_added_s'] for v in validation)
    if not math.isfinite(gain) or not math.isfinite(cost) or gain < settings['min_saved_s'] or cost <= 0:
        return dict(action='hold', reason='no_positive_source_saving')
    a = loads[source]
    total = sum(r['fraction'] for r in regions)
    need = max(0., (a['util']-settings['source_goal'])*a['workers'])/(rate*gain)
    spare = max(0., max_fraction-total)
    # At the family cap, another copy can MOVE existing traffic away from an
    # overloaded sibling. It must not create a second root search for a query.
    def queue(load):
        value=load.get('worker_pressure_ms')
        return value if value is not None else load.get('backlog_estimate_ms',load.get('queue_ms',0))
    donors={r['id']:min(r['fraction'],settings['fraction_step']) for r in regions
            if queue(loads[r['target']])>settings['target_queue_ms'] or loads[r['target']]['util']>settings['target_goal']}
    transferable=sum(donors.values())
    queue_need=queue(a)>settings['target_queue_ms']
    if queue_need:need=max(need,settings['fraction_step'])
    if need < settings['min_fraction'] and transferable < settings['min_fraction']:
        return dict(action='hold', reason='no_residual_need_or_fraction')
    available=min(spare,need)+transferable
    if available < settings['min_fraction']:
        return dict(action='hold', reason='no_residual_need_or_fraction')
    def take_donors(amount):
        out={};left=max(0.,amount-min(spare,need))
        for version,share in sorted(donors.items()):
            value=min(left,share)
            if value>0:out[version]=value;left-=value
        return out
    # Use current targets before spending on another physical copy.
    existing = []
    for r in regions:
        load = loads[r['target']]
        room = max(0., (settings['target_goal']-load['util'])*load['workers'])/(rate*cost)
        if r['id'] not in donors and queue(load) <= settings['target_queue_ms']:
            existing.append((room, r['id']))
    if existing and max(existing)[0] >= settings['min_fraction']:
        room, version = max(existing)
        fraction=min(available,room,settings['fraction_step'])
        transfer=take_donors(fraction)
        return dict(action='redistribute' if transfer else 'increase', version=version,
                    fraction=fraction, donors=transfer)
    if len(regions) >= settings['max_copies_per_family']:
        return dict(action='hold', reason='family_copy_cap')
    estimate_ready = max(r.get('copy_ready_seconds', r.get('ready_seconds', 0.)) for r in regions)
    if estimate_ready*1000*settings['amortization_factor'] > settings['decision_horizon_ms']:
        return dict(action='hold', reason='copy_too_slow_for_horizon')
    occupied = {source} | {r['target'] for r in regions}
    targets = []
    for sid, load in loads.items():
        if sid in occupied or queue(load) > settings['target_queue_ms']:
            continue
        room = max(0., (settings['target_goal']-load['util'])*load['workers'])/(rate*cost*settings['new_target_cost_margin'])
        fraction = min(available, room, settings['fraction_step'])
        if fraction >= settings['min_fraction']:
            targets.append((fraction, -load['util'], sid))
    if not targets:
        return dict(action='hold', reason='no_target_capacity')
    fraction, _, target = max(targets)
    return dict(action='copy', target=target, fraction=fraction, donors=take_donors(fraction), estimated_source_relief_cores=rate*max(0.,fraction-sum(take_donors(fraction).values()))*gain,
                target_cost_kind='sibling_calibration_with_margin', estimated_copy_seconds=estimate_ready)
