"""Low-frequency replica controller; native routing uses immutable snapshots.

Static creation and routing are exposed separately for controlled experiments.
Dynamic mode adds sustained imbalance detection and bounded proportional steps.
Publication policy is explicit: validated or structurally ready. Neither
readiness nor unlabelled shadow agreement is a guarantee of search recall.
"""
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
from pathlib import Path
import threading
import time
import uuid

import numpy as np

from .index import native
from .replicas import maintenance_config
from .load_pressure import search_pressure


def candidate(observations, source, target, b, heat=None):
    """Greedily add complete sampled work sets grouped by existing route entry."""
    groups = defaultdict(dict)
    for q in observations:
        if q['source'] != source or q['replica'] is not None or q['query_id'] is None:
            continue
        groups[q['entry']][q['query_id']] = q
    ranked = []
    for entry, queries in groups.items():
        if len(queries) < b['min_entry_queries']: continue
        ids, cpu, total = {entry}, 0., 0.
        for q in queries.values():
            total += sum(v['cpu_ms'] for v in q['branches'])
            for v in q['branches']:
                if v['node'] == source:
                    ids.update(v['expanded_ids']); cpu += v['cpu_ms']
        if not cpu or cpu / max(total, 1e-9) < b['min_coverage']: continue
        benefit=cpu if heat is None or b.get('selection_policy')=='cpu_worksets' else sum(heat.get(gid,0.) for gid in sorted(ids))
        if benefit<=0:continue
        ranked.append((benefit / len(ids), entry, ids, list(queries.values())))
    ids, entries, selected = set(), [], []
    portals=b.get('portal_budget_fraction',0.)
    core_cap=max(1,int(b['max_core_rows']*(1-portals)))
    for _, entry, extra, queries in sorted(ranked, key=lambda r: (-r[0], r[1])):
        # Do not copy the growing core for every candidate. The difference is
        # bounded by this workset, and yields exactly the same capacity test.
        if len(ids) + len(extra - ids) > core_cap: continue
        ids.update(extra); entries.append(entry); selected.extend(queries)
    # Attach observed routing portals without requiring every training workset
    # to fit. Publication policy separately decides whether to shadow-validate.
    attached=0
    if portals and ids:
        chosen=set(entries)
        order=sorted(groups.items(),key=lambda item:(-sum(v['cpu_ms'] for q in item[1].values() for v in q['branches'] if v['node']==source),item[0]))
        for entry,queries in order:
            if entry in chosen:continue
            if entry not in ids and len(ids)>=b['max_core_rows']:continue
            ids.add(entry);entries.append(entry);chosen.add(entry);selected.extend(queries.values());attached+=1
    if len({q['query_id'] for q in selected}) < b['min_group_queries']:

        raise ValueError('Insufficient independent, entry-eligible hotspot traces')
    scores = [q['score'] for q in selected]
    slack = max((max(scores) - min(scores)) * .25, 1e-6)
    return dict(source=source, target=target, entries=sorted(entries), ids=sorted(ids),
        training_query_ids=sorted({q['query_id'] for q in selected}),
        min_score=min(scores) - slack, max_score=max(scores) + slack,
        sampled_cpu_ms=sum(v['cpu_ms'] for q in selected for v in q['branches'] if v['node'] == source),
        selection_policy=b.get('selection_policy','entry_worksets'), candidate_entries=len(ranked),attached_portals=attached)


def queue_p95(hist):
    total = sum(hist)
    if not total: return None
    limit, acc = math.ceil(total * .95), 0
    for i, n in enumerate(hist):
        acc += n
        if acc >= limit: return (2 ** (i + 1)) / 1000  # histogram upper bound, ms


class LoadBalancer:
    @property
    def active(self):
        """Compatibility view for single-region callers; policy uses regions."""
        return next(iter(getattr(self, 'regions', {}).values()), None)

    @active.setter
    def active(self, value):
        self.regions = {} if value is None else {value['id']: value}

    def __init__(self, engine, token, directory):
        self.engine, self.c, self.token = engine, engine.c, token
        self.b = self.c['load_balance']
        if not self.b['enabled']: raise ValueError('Load balancing disabled')
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.client = native().MaintenanceClient(maintenance_config(self.c))
        self.lock = threading.RLock()
        self.history = deque(maxlen=self.b['max_history'])
        self.previous, self.loads, self.rates = {}, {}, defaultdict(float)
        self.active, self.in_progress = None, None
        self.partition_pending = {}
        self.build_reservations = {}
        self.cooldown_until, self.streak, self.last_pair = 0., 0, None
        self.last_control, self.last_adjust = 0., 0.
        self.stop = threading.Event()
        self.builder = ThreadPoolExecutor(max_workers=1, thread_name_prefix='replica-controller-build')
        self.planner = ThreadPoolExecutor(max_workers=1, thread_name_prefix='replica-planner')
        self.plan_future = None
        self.plan_context = None
        self.events = []
        self.counters = defaultdict(int)
        self.region_counters = {}
        from .replica_policy import AdaptiveDecay
        decay = self.b.get('adaptive_decay', {})
        self.decay = AdaptiveDecay(decay) if decay.get('enabled', False) else None
        self.draining, self.reclaim_futures, self.reclaim_retry = {}, {}, {}
        self.reclaimer = ThreadPoolExecutor(max_workers=decay.get('reclaim_workers', 1),
                                            thread_name_prefix='replica-reclaim')
        self.observation_drops = 0
        self.expansion_streaks, self.expansion_cooldown, self.shrink_streaks = {}, {}, {}
        self.family_rates = {}
        from .online_replica_policy import OnlinePolicy
        online=self.b.get('online_policy',{})
        self.online_policy=OnlinePolicy(online) if online.get('enabled') else None
        self.calibrator=ThreadPoolExecutor(max_workers=1,thread_name_prefix='replica-calibration')
        self.calibration_future=None
        self.last_policy=0.
        self.policy_reasons={}
        self.rejected_entries={}
        self.last_entry_admission=0.
        self.thread = threading.Thread(target=self.run, name='replica-observer', daemon=True)
        self.thread.start()

    def log(self, event, **fields):
        row = dict(event=event, monotonic_ns=time.monotonic_ns(), **fields)
        with self.lock:
            self.events.append(row)
            counter={'ENTRY_STAGED':'cut_attempts','CUT_COMPLETED':'cut_completed','COPY_STARTED':'copy_attempts',
                'SOURCE_SENT':'copy_completed','READY_UNVALIDATED':'ready',
                'CANARY':'published','FAILED':'failed','RETIRED':'retired'}.get(event)
            if counter:self.counters[counter]+=1
            with (self.directory / 'events.jsonl').open('a') as f:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')

    def rpc(self, node, op, **fields):
        n = self.c['cluster']['nodes'][node]
        reply = self.client.request(n['host'], n['port'] + self.b['port_offset'],
                                    dict(op=op, token=self.token, **fields))['response']
        if not reply.get('ok'): raise RuntimeError(reply.get('error', 'Replica operation failed'))
        return reply['result']

    def poll_entry_admission(self, now):
        cfg=self.b.get('entry_admission',{})
        if not cfg.get('enabled') or now-getattr(self,'last_entry_admission',0.)<cfg['interval_seconds']:return
        self.last_entry_admission=now
        from .entry_admission import evaluate
        snapshot=self.engine.runtime.replica_entry_statistics()
        records=[];changed=False
        # Maintenance-only use of the existing lifecycle lock. Native query
        # telemetry writers do not acquire it or any new statistics mutex.
        with self.lock:
            for raw in snapshot:
                r=self.regions.get(raw['version'])
                if r is None:continue
                item=evaluate(raw,cfg);entry=int(item['entry'])
                closed=r.setdefault('closed_entries',[])
                if item['close'] and entry not in closed:
                    closed.append(entry);closed.sort();changed=True
                    self.log('ENTRY_CLOSED',id=r['id'],entry=entry,returns=item['returns'],
                             completed=item.get('completed'),return_probability=item['return_probability'],
                             expected_remaining_budget=item['expected_remaining_budget'],
                             score_mode=item['score_mode'],score=item['score'],threshold=item['threshold'],
                             mean_remaining=item['mean_remaining'],initial_budget=item['initial_budget'])
                item['closed']=entry in closed
                records.append(item)
            # Export once per control window, never in the search/report writer.
            (self.directory/'entry_admission.json').write_text(json.dumps(
                dict(monotonic_ns=time.monotonic_ns(),entries=records),indent=2))
            with (self.directory/'entry_admission_windows.jsonl').open('a') as f:
                f.write(json.dumps(dict(monotonic_ns=time.monotonic_ns(),entries=records))+'\n')
            if changed:self.publish(now)

    def observe(self):
        now = time.monotonic()
        rows = self.engine.runtime.replica_observations()
        with self.lock:
            self.history.extend(rows)
            counts = defaultdict(int)
            family_counts = defaultdict(int)
            # One set per observation batch, not a linear portal-list scan for
            # every sampled query. Keep the region record for last_use updates.
            region_entries=[(r,set(r['entries'])-set(r.get('closed_entries',[]))) for r in self.regions.values()]
            for q in rows:
                matched = set()
                for r,entries in region_entries:
                    if q['source'] == r['source'] and q['entry'] in entries and r['min_score'] <= q['score'] <= r['max_score']:
                        r['last_use'] = now
                        if r.get('family_id'):matched.add(r['family_id'])
                for family in matched:family_counts[family] += 1
                counts[q['entry']] += 1
            dt = max(now - getattr(self, 'last_observe', now), .001)
            for entry in set(self.rates) | set(counts):
                self.rates[entry] = .8 * self.rates[entry] + .2 * counts[entry] * self.b['sample_every'] / dt
            self.family_rates = {r['family_id']:.8*self.family_rates.get(r['family_id'],0.)+.2*family_counts[r['family_id']]*self.b['sample_every']/dt
                                 for r in self.regions.values() if r.get('family_id')}
            self.last_observe = now
        for n in self.c['cluster']['nodes']:
            sid = n['id']; started = time.monotonic()
            r = self.rpc(sid, 'load'); ended = time.monotonic()
            old = self.previous.get(sid); self.previous[sid] = r
            if old is None: continue
            dt = (r['time_ns'] - old['time_ns']) / 1e9
            if dt <= 0: continue
            v, before = r['runtime'], old['runtime']
            workers = v['compute_queue']['workers']
            cpu = (v['compute_cpu_ns'] - before['compute_cpu_ns']) / 1e9
            completed = v['compute_completed'] - before['compute_completed']
            try:
                pressure = search_pressure(v, before, self.loads.get(sid, {}).get('service_ms', 1.))
            except ValueError:
                with self.lock:
                    if sid in self.loads: self.loads[sid]['usable'] = False
                continue
            queue = v['compute_queue']
            load = dict(util=max(0., cpu / dt / workers), **pressure,
                workers=workers, queued=queue['queued'], active=queue['active'],
                completed=completed, cpu_seconds=cpu, window_seconds=dt,
                updated=ended, rpc_ms=(ended-started)*1000,
                usable=ended-started <= self.b['max_load_age_ms']/1000,
                free_bytes=r['free_bytes'])
            load['epoch']=r['meta']['epoch']
            load['source_index_bytes']=r.get('source_index_bytes')
            load['hotness']=r.get('hotness',{})
            with self.lock: self.loads[sid] = load
        with self.lock:
            sample = dict(monotonic_ns=time.monotonic_ns(), nodes=self.loads,
                          replica=None if self.active is None else dict(id=self.active['id'], fraction=self.active['fraction']),
                          replicas=[dict(id=r['id'],source=r['source'],target=r['target'],fraction=r['fraction'],
                                         entries=len(r['entries']),
                                         closed_entries=len(set(r['entries']) & set(r.get('closed_entries',[]))),
                                         open_entries=len(set(r['entries']) - set(r.get('closed_entries',[]))))
                                    for r in self.regions.values()])
            with (self.directory/'loads.jsonl').open('a') as f: f.write(json.dumps(sample)+'\n')
        if self.decay is not None and self.b['mode'] == 'dynamic':
            self.observe_decay(rows, time.monotonic())
        self.poll_reclaims(time.monotonic())

    def observe_decay(self, rows, now):
        head = self.engine.runtime.metrics()
        dropped = head.get('replica_observation_dropped', 0)
        with self.lock:
            healthy = dropped == self.observation_drops and all(
                self.fresh(n, now) for n in self.loads) and len(self.loads) == len(self.c['cluster']['nodes'])
            self.observation_drops = dropped
            # Completions can temporarily disappear while work is still queued.
            if not rows and any(x.get('queued', 0) or x.get('active', 0) for x in self.loads.values()):
                healthy = False
            self.policy_observations_healthy=healthy
            used = sum(r.get('bytes', 0) for r in list(self.regions.values())+list(self.draining.values()))
            cap = self.b.get('max_total_replica_bytes', self.b['max_region_bytes']*self.b['max_replicas'])
            pressure = min(1., used/max(1, cap))
            for r in self.regions.values():
                free = self.loads.get(r['target'], {}).get('free_bytes')
                if free is not None:
                    disk = (self.b['reserve_disk_bytes']+self.b['max_region_bytes']-free)/max(1,self.b['max_region_bytes'])
                    pressure = max(pressure, max(0., min(1., disk)))
            actions = self.decay.tick(now, self.decay.counts(rows), healthy=healthy,
                                      sample_every=self.b['sample_every'], pressure=pressure)
            for action in actions:
                self.log('DECAY_'+action['kind'].upper(), **{k:v for k,v in action.items() if k != 'kind'})
            retiring = {a['family'] for a in actions if a['kind'] == 'retire'}
            versions = [r['id'] for r in self.regions.values() if r.get('family_id') in retiring]
            (self.directory/'decay.json').write_text(json.dumps(self.decay.snapshot(), indent=2))
        for version in versions:
            self.retire(version, wait=False)

    def register_family(self, region, started):
        from .replica_policy import family_key
        now = time.monotonic()
        region['family_id'] = region.get('_replica_family') or family_key(region)
        region['copy_ready_seconds'] = now-started
        region['ready_seconds'] = now-started+(0. if region.get('replica_of') else
            region.get('_planning_elapsed_seconds', region.get('region_stats', {}).get('planning_seconds', 0.)))
        if self.decay is not None:
            self.decay.register(region, now, region['ready_seconds'], region['family_id'])
            self.withdraw_unused_families(now)

    def withdraw_unused_families(self, now):
        if self.decay is None:return
        live = {r.get('family_id') for r in self.regions.values()}
        for key, heat in self.decay.states.items():
            if heat.active and key not in live:
                self.decay.withdraw(key, now)

    def fresh(self, node, now):
        s = self.loads.get(node)
        return s is not None and s['usable'] and now - s['updated'] <= self.b['max_load_age_ms']/1000

    def plan_snapshot(self,source,include_existing=False):
        with self.lock:
            used=set() if include_existing else {e for r in self.regions.values() if r['source']==source
                                                 for e in r.get('_candidate_entries',r['entries'])}
            rows=[q for q in self.history if q['source']==source and q['entry'] not in used]
            if self.b.get('online_policy',{}).get('enabled'):
                from .online_replica_policy import held_out
                rows=[q for q in rows if q['query_id'] is not None and not held_out(q['query_id'],self.b['online_policy']['holdout_modulus'])]
                if not include_existing:
                    now=time.monotonic()
                    self.rejected_entries={k:t for k,t in getattr(self,'rejected_entries',{}).items() if t>now}
                    rows=[q for q in rows if (source,q['entry']) not in self.rejected_entries]
            return rows

    def propose(self, source, target, observations=None):
        if observations is None:observations=self.plan_snapshot(source)
        if self.b.get('selection_policy') in ('hot_entry_community','entry_geometry'):
            from .community import hotspots
            ranked,groups=hotspots(observations,source,self.b)
            job=uuid.uuid4().hex
            actual=self.rpc(source,'load')['source_index_bytes']
            with self.lock:
                from .replica_budget import available, region_limit
                capacity=min(self.b['max_region_bytes'],available(self,source,actual))
                if self.b.get('locality',{}).get('enabled'):
                    capacity=min(capacity,region_limit(self.b,actual))
            if capacity<=65536:raise ValueError('Insufficient source planning capacity')
            self.rpc(source,'community_plan_start',job=job,hot_ids=[v for v,w in ranked],weights=[w for v,w in ranked],
                     capacity_bytes=capacity,deadline_s=min(self.b['operation_timeout_s'],self.b['planner_max_age_ms']/1000))
            deadline=time.monotonic()+self.b['operation_timeout_s']
            while True:
                if self.stop.is_set():raise RuntimeError('Community planning cancelled')
                result=self.rpc(source,'community_plan_poll',job=job)
                if result['done']:
                    region=result['region'];break
                if time.monotonic()>=deadline:raise RuntimeError('Community planning timed out')
                self.stop.wait(self.b.get('community_poll_ms',50)/1000)
            qs=[q for entry in region['entries'] for q in groups[entry]]
            if len({q['query_id'] for q in qs})<self.b['min_group_queries']:raise ValueError('Insufficient community query coverage')
            scores=[q['score'] for q in qs];slack=max((max(scores)-min(scores))*.25,1e-6)
            return dict(source=source,target=target,entries=region['entries'],ids=region['ids'],
                training_query_ids=sorted({q['query_id'] for q in qs}),min_score=min(scores)-slack,max_score=max(scores)+slack,
                selection_policy=self.b['selection_policy'],region_proof=region['region_proof'],region_stats=region['stats'],
                reserved_package_bytes=region.get('reserved_package_bytes',region['stats']['estimated_bytes']),
                source_capacity_bytes=actual,
                sampled_cpu_ms=sum(v['cpu_ms'] for q in qs for v in q['branches'] if v['node']==source))
        scores=None
        if self.c.get('hotness',{}).get('enabled') and self.b.get('selection_policy')=='hotness_worksets':
            heat=self.rpc(source,'hotness')
            (self.directory/'latest_hotness_candidates.json').write_text(json.dumps(heat))
            if self.b.get('selection_policy','entry_worksets') in ('hotness_worksets','cpu_worksets'):
                scores={int(r['id']):r['lower_bound'] for r in heat.get('top',[])}
        fn=native().replica_candidate if self.b.get('planner_mode','inline')=='native_async' else candidate
        return fn(observations, source, target, self.b,scores)

    def pair_eligible(self,a,b,now):
        if self.b.get('online_policy',{}).get('enabled'):
            from .online_replica_policy import pressure
            if not self.fresh(a,now) or not self.fresh(b,now):return False
            pa,pb=pressure(self.loads[a]),pressure(self.loads[b])
            return (pa is not None and pb is not None and pa>=self.b['queue_high_ms']
                    and pb<=self.b['queue_low_ms'] and self.loads[a]['util']>=self.b['source_util_high']
                    and self.loads[b]['util']<=self.b['target_util_low']
                    and self.loads[a]['util']-self.loads[b]['util']>=self.b['util_gap'])
        return (self.fresh(a,now) and self.fresh(b,now) and
            self.loads[a]['util']>=self.b['source_util_high'] and self.loads[a]['queue_ms']>=self.b['queue_high_ms'] and
            self.loads[b]['util']<=self.b['target_util_low'] and
            self.loads[b].get('backlog_estimate_ms',self.loads[b]['queue_ms'])<=self.b['queue_low_ms'] and
            self.loads[a]['util']-self.loads[b]['util']>=self.b['util_gap'])

    def poll_plan(self,now):
        future=self.plan_future
        if future is None or not future.done():return
        context=self.plan_context;self.plan_future=self.plan_context=None
        if context.get('automatic_pipeline'):
            try:future.result()
            except Exception as exc:self.log('AUTO_PIPELINE_FAILED',source=context['source'],error=str(exc))
            self.cooldown_until=now+self.b['cooldown_ms']/1000
            return
        try:
            plan=future.result();a,b=plan['source'],plan['target']
            with self.lock:
                used={e for r in self.regions.values()if r['source']==a for e in r['entries']}
                reason=('stopped' if self.stop.is_set() else
                    'expired' if now-context['started']>self.b.get('planner_max_age_ms',500)/1000 else
                    'load_changed' if not self.pair_eligible(a,b,now) else
                    'epoch_changed' if any(self.loads[n].get('epoch')!=epoch for n,epoch in context['epochs'].items()) else
                    'capacity_changed' if self.in_progress is not None or len(self.regions)+len(getattr(self,'draining',{}))>=self.b['max_replicas'] else
                    'entry_already_replicated' if used.intersection(plan['entries']) else None)
            if reason:self.log('PLAN_DISCARDED',reason=reason,planning_seconds=now-context['started']);return
            self.log('PLAN_COMPLETED',planning_seconds=now-context['started'],entries=len(plan['entries']))
            plan = dict(plan, _planning_elapsed_seconds=now-context['started'])
            self.create(plan)
        except (ValueError,RuntimeError) as e:
            self.log('NO_CANDIDATE',reason=str(e));self.cooldown_until=now+self.b['cooldown_ms']/1000

    def create(self, plan, *, publication_gate=None):
        n = len(self.c['cluster']['nodes'])
        if not (0 <= plan['source'] < n and 0 <= plan['target'] < n and plan['source'] != plan['target']):
            raise ValueError('Invalid replica source/target')
        reservation=self.b['max_region_bytes']
        loc=self.b.get('locality',{})
        if loc.get('enabled'):
            actual=self.rpc(plan['source'],'load')['source_index_bytes']
            reservation=plan.get('reserved_package_bytes',reservation)
            from .replica_budget import package_limit, per_region
            limit=package_limit(self.b,actual,plan) if per_region(self.b) else self.b['max_region_bytes']
            if type(reservation) is not int or reservation<=0 or reservation>limit:
                raise ValueError('Invalid package reservation')
            if plan.get('source_capacity_bytes',actual)!=actual:
                raise ValueError('Source index capacity identity mismatch')
            plan=dict(plan,source_capacity_bytes=actual,reserved_package_bytes=reservation)
            if per_region(self.b) and not plan.get('_stream_version'):
                free=self.rpc(plan['target'],'load')['free_bytes']
                with self.lock:self.loads.setdefault(plan['target'],{})['free_bytes']=free
        growth=self.c['peer'].get('demand_growth',{})
        growth_reservation=0
        if growth.get('enabled'):
            if not loc.get('enabled'):
                actual=self.rpc(plan['source'],'load')['source_index_bytes']
            growth_reservation=int(growth['growth_fraction']*actual)
        plan=dict(plan,_demand_reserved_bytes=growth_reservation)
        # Core alpha applies to the package. Aggregate source/target/global
        # quotas additionally charge each physical copy's entire growth claim.
        reservation+=growth_reservation
        with self.lock:
            pending=getattr(self,'partition_pending',{})
            streamed=plan.get('_stream_version')
            own=pending.get(streamed)
            if streamed and (not own or own['source']!=plan['source'] or own['target']!=plan['target'] or reservation>own['bytes']):
                raise ValueError('Growing publication lacks matching reservation')
            if pending and not streamed:raise RuntimeError('Growing partition build in progress')
            if self.b.get('online_policy',{}).get('enabled') and not self.b.get('entry_admission',{}).get('enabled') and not plan.get('replica_of') and not plan.get('_pending_activation'):
                owned={e for r in self.regions.values() if r['source']==plan['source']
                       for e in r.get('_candidate_entries',r['entries'])}
                if owned.intersection(plan['entries']):
                    raise ValueError('New small region conflicts with an existing entry contract')
            others=[r for k,r in pending.items() if k!=streamed and k not in self.regions and k not in self.draining]
            if loc.get('enabled'):
                from .replica_budget import available, per_region
                if reservation>available(self,plan['source'],actual,exclude=streamed):
                    raise ValueError('Aggregate source replica capacity exceeded')
                if per_region(self.b) and reservation>available(self,plan['source'],actual,plan['target'],exclude=streamed):
                    raise ValueError('Target replica capacity/free space exceeded')
            if plan.get('replica_of'):
                parent = self.regions.get(plan['replica_of'])
                if not self.b.get('adaptive_expansion', {}).get('enabled') or parent is None:
                    raise ValueError('Replica expansion parent unavailable/disabled')
                if any(plan.get(k) != parent.get(k) for k in ('source','ids','entries','min_score','max_score','region_proof','epoch','certified_entries')) or plan.get('_replica_family') != parent['family_id']:
                    raise ValueError('Replica expansion changed region contract')
                fraction=plan.get('_initial_fraction',0.)
                total=sum(r['fraction'] for r in self.regions.values() if r.get('family_id')==parent['family_id'])
                donors=plan.get('_fraction_donors',{})
                if any(k not in self.regions or self.regions[k].get('family_id')!=parent['family_id']
                       or type(v) not in (int,float) or not math.isfinite(v) or not 0<=v<=self.regions[k]['fraction'] for k,v in donors.items()):
                    raise ValueError('Replica fraction donor unavailable')
                if not isinstance(fraction,(int,float)) or not math.isfinite(fraction) or not 0<fraction<=self.b['max_fraction']-total+sum(donors.values())+1e-12:
                    raise ValueError('Replica expansion exceeds family fraction budget')
                if any(r['target']==plan['target'] and r.get('family_id')==parent['family_id'] for r in list(self.regions.values())+list(self.draining.values())):
                    raise ValueError('Replica family already present on target')
                if sum(r.get('family_id')==parent['family_id'] for r in list(self.regions.values())+list(self.draining.values())) >= self.b['adaptive_expansion']['max_copies_per_family']:
                    raise ValueError('Replica family copy capacity exceeded')
            if len(self.regions)+len(getattr(self,'draining',{}))+len(others)>=self.b['max_replicas'] or self.in_progress is not None:
                raise RuntimeError('Replica controller capacity/build in progress')
            used=sum(r.get('bytes',0) for r in list(self.regions.values())+list(getattr(self,'draining',{}).values()))
            if used+sum(r['bytes'] for r in others)+reservation>self.b.get('max_total_replica_bytes',self.b['max_region_bytes']*self.b['max_replicas']):
                raise ValueError('Total replica byte reservation exceeded')
            version = streamed or 'r_' + uuid.uuid4().hex
            self.in_progress = version
            if not hasattr(self,'build_reservations'):self.build_reservations={}
            self.build_reservations[version]=dict(id=version,source=plan['source'],target=plan['target'],bytes=reservation)
        try:
            future = (self.builder.submit(self.build, version, plan, publication_gate) if publication_gate
                      else self.builder.submit(self.build, version, plan))
        except BaseException:
            with self.lock:
                self.in_progress=None;self.build_reservations.pop(version,None)
            raise
        return future

    def build_partitioned(self, shard, source, history, layout=None):
        """Explicit maintenance operation; one whole graph publishes at a time.

        The caller pins the immutable source shard for this operation. Normal
        create/build still checks live source/target capacity and independently
        verifies every graph. No automatic layout choice is introduced here.
        """
        from .replica_layout import build_and_publish_partitioned_regions
        import copy
        if self.b['mode'] != 'static':
            raise ValueError('Partition maintenance requires static controller mode')
        cfg = copy.deepcopy(layout if layout is not None else self.b['partitioned_regions'])
        if self.b.get('entry_admission',{}).get('enabled'):
            from .automatic_replica import covered_route_entries
            cfg.pop('historical_entries',None)
            cfg['covered_route_entries']=covered_route_entries(self,source)
        if source in {int(k) for k in cfg['target_weights']}:
            raise ValueError('Replica target must differ from source')
        training = list(history['training_query_ids'])

        def publish(plan):
            plan.update(source=source, training_query_ids=training,
                        min_score=-1e300, max_score=1e300)
            self.log('PARTITION_PLAN_READY', target=plan['target'], rows=len(plan['ids']),
                     reserved_bytes=plan['reserved_package_bytes'])
            return self.create(plan).result()

        return build_and_publish_partitioned_regions(
            shard, history['hot_ids'], history['weights'], self.b, cfg,
            publish, lambda version: self.retire(version, wait=True), cancelled=self.stop.is_set)

    def build_partitioned_online(self, source, history, layout=None, *, _automatic=False, _publication_gate=None):
        """Plan on the source data server while completed replicas are copied.

        Polls transfer whole immutable plans only. A repeated sequence returns
        the same plan; a transport timeout is retried without another create.
        Source and destination checks are the normal create/build path.
        """
        import copy
        from .replica_layout import pipeline_settings
        if self.b['mode']!='static' and not (_automatic and self.b['mode']=='dynamic' and self.b.get('automatic_pipeline',{}).get('enabled')):
            raise ValueError('Partition maintenance requires static mode or enabled automatic pipeline')
        cfg=copy.deepcopy(layout if layout is not None else self.b['partitioned_regions'])
        if self.b.get('entry_admission',{}).get('enabled'):
            from .automatic_replica import covered_route_entries
            cfg.pop('historical_entries',None)
            cfg['covered_route_entries']=covered_route_entries(self,source)
        pcfg=pipeline_settings(cfg)
        job=uuid.uuid4().hex;versions=[];events=[];sequence=0;started=time.monotonic();start_rejected=False
        deadline=started+cfg['deadline_s']+len(cfg['target_weights'])*self.b['operation_timeout_s']
        def rpc(op,**fields):
            nonlocal start_rejected
            while not self.stop.is_set() and time.monotonic()<deadline:
                node=self.c['cluster']['nodes'][source]
                try:
                    response=self.client.request(node['host'],node['port']+self.b['port_offset'],
                        dict(op=op,token=self.token,job=job,**fields))['response']
                except (RuntimeError,TimeoutError,ConnectionError,OSError):
                    self.stop.wait(self.b['community_poll_ms']/1000)
                    continue
                if not response.get('ok'):
                    if op=='partition_plan_start':start_rejected=True
                    raise RuntimeError(response.get('error','Partition maintenance failed'))
                return response['result']
            raise TimeoutError('Partition maintenance cancelled/deadline')
        streams={}
        if pcfg['copy_during_expansion']:
            from .replica_growth import reserve_portfolio
            streams=reserve_portfolio(self,source,cfg)
        try:
            rpc('partition_plan_start',hot_ids=history['hot_ids'],weights=history['weights'],layout=cfg,
                **({'streams':streams} if streams else {}))
            while True:
                reply=rpc('partition_plan_poll',sequence=sequence)
                if reply['done']:
                    report=reply['report'];break
                plan=reply['plan']
                if plan is None:
                    self.stop.wait(self.b['community_poll_ms']/1000);continue
                if streams and plan.get('_stream_version')!=streams[str(plan['target'])]['id']:
                    raise ValueError('Source returned unknown growing version')
                plan.update(source=source,training_query_ids=list(history['training_query_ids']),
                            min_score=-1e300,max_score=1e300)
                if cfg.get('defer_publication'):plan['_pending_activation']=True
                if cfg.get('require_validation'):plan['_require_validation']=True
                if _publication_gate:_publication_gate(plan)
                begin=time.monotonic()-started
                version=(self.create(plan,publication_gate=_publication_gate) if _publication_gate else self.create(plan)).result()
                versions.append(version)
                with self.lock:self.partition_pending.pop(version,None)
                events.append(dict(target=plan['target'],version=version,publish_started_seconds=begin,
                                   published_seconds=time.monotonic()-started))
                sequence+=1
            report.update(publications=events,publication_pipeline=cfg.get('pipeline',{}).get('enabled',False),
                          first_published_seconds=min(e['published_seconds'] for e in events),
                          all_published_seconds=time.monotonic()-started)
            self.log('PARTITION_PORTFOLIO_STAGED' if cfg.get('defer_publication') else 'PARTITION_PORTFOLIO_PUBLISHED',job=job,report=report)
            return report,versions
        except BaseException as exc:
            cancelled_source=start_rejected
            try:
                until=time.monotonic()+self.b['operation_timeout_s']
                while not cancelled_source:
                    reply=self.rpc(source,'partition_plan_cancel',job=job)
                    if reply.get('quiescent',not streams):cancelled_source=True;break
                    if time.monotonic()>=until:raise TimeoutError('Growing source cancellation still active')
                    time.sleep(.05)
            except BaseException as cancel:self.log('PARTITION_CANCEL_PENDING',job=job,error=str(cancel))
            pending=[]
            for version in reversed(versions):
                try:self.retire(version,wait=True)
                except BaseException as cleanup:pending.append(str(cleanup))
            if streams:
                from .replica_growth import cleanup_portfolio
                pending.extend(cleanup_portfolio(self,streams,versions,cancelled_source))
            if pending:raise RuntimeError('Partition retirement remains pending: '+'; '.join(pending)) from exc
            raise

    def build(self, version, plan, publication_gate=None):
        started = time.monotonic()
        (self.directory/(version+'_plan.json')).write_text(json.dumps(plan, indent=2))
        self.log('ENTRY_STAGED', id=version, source=plan['source'], target=plan['target'], entries=len(plan['entries']), rows=len(plan['ids']))
        deadline = time.monotonic() + self.b['operation_timeout_s']
        try:
            self.rpc(plan['source'], 'source_adopt' if plan.get('_stream_version') else 'source_start', id=version, plan=plan)
            self.log('COPY_STARTED',id=version)
            last = None; packaged=False
            while not self.stop.is_set():
                if time.monotonic() > deadline: raise TimeoutError('Replica lifecycle deadline exceeded')
                source = self.rpc(plan['source'], 'status', id=version)
                if source['phase'] == 'FAILED': raise RuntimeError(source['error'])
                if not packaged and 'manifest' in source:
                    self.log('CUT_COMPLETED',id=version,rows=source['manifest']['rows'],bytes=source['manifest']['bytes']);packaged=True
                if source['phase'] != last:
                    self.log('SOURCE_'+source['phase'], id=version); last = source['phase']
                if source['phase'] == 'SENT':
                    target = self.rpc(plan['target'], 'status', id=version)
                    if target['phase'] == 'FAILED': raise RuntimeError(target['error'])
                    if target['phase'] == 'READY': break
                self.stop.wait(self.b.get('lifecycle_poll_ms',100)/1000)
            if self.stop.is_set(): raise RuntimeError('Replica controller stopped during build')
            self.log('READY_UNVALIDATED', id=version, source=source, target=target)
            if publication_gate:publication_gate(plan)
            quality=source['manifest'].get('quality',{})
            plan=dict(plan)
            plan.setdefault('certified_entries',list(plan['entries']))
            plan.pop('_stream_version',None)  # Transfer handle must not leak into later replica-family clones.
            plan=dict(plan,entry_weights=quality.get('entry_weights',{}),peer_entries=quality.get('peer_entries',[]),epoch=source['manifest']['contract']['epoch'])
            self.log('STRUCTURE',id=version,bytes=source['manifest']['bytes']+plan.get('_demand_reserved_bytes',0),core_bytes=source['manifest']['bytes'],rows=source['manifest']['rows'],quality=quality)
            if self.b.get('publication_mode','validated')=='ready' and not plan.get('replica_of') and not plan.get('_require_validation'):
                # Data/contract/dependency validation and immutable installation
                # have completed on the destination. Do not replay searches on
                # this path or label untested entries as quality-qualified.
                now=time.monotonic()
                validation=dict(passed=None,quality_validated=False,gate='readiness_only',
                                source_saved_s=None,target_added_s=None,count=0)
                with self.lock:
                    self.regions[version]=dict(plan,id=version,bytes=source['manifest']['bytes']+plan.get('_demand_reserved_bytes',0),core_bytes=source['manifest']['bytes'],
                        fraction=plan.get('_initial_fraction',self.b['initial_fraction']),published=now,last_use=now,
                        validation=validation,qualified=False,publication_mode='ready')
                    self.register_family(self.regions[version], started)
                self.publish(now)
                self.log('CANARY',id=version,entries=len(plan['entries']),validation=validation,
                         publication_mode='ready',fraction=self.regions[version]['fraction'],pressure_start_paused=plan.get('_pressure_start_paused',False))
                return version
            def publish_validated(v):
                if not v.get('passed'):
                    with self.lock:removed=self.regions.pop(version,None)
                    if removed:
                        with self.lock:self.withdraw_unused_families(time.monotonic())
                        self.log('CANARY_PAUSED',id=version,reason=v.get('reason'));self.publish(time.monotonic())
                    return
                qualified=set(v['qualified_entries'])
                with self.lock:
                    if plan.get('replica_of') and plan['replica_of'] not in self.regions:
                        raise ValueError('Expansion parent withdrawn during construction')
                    old=self.regions.get(version)
                    self.regions[version]=dict(plan,id=version,bytes=source['manifest']['bytes']+plan.get('_demand_reserved_bytes',0),core_bytes=source['manifest']['bytes'],
                        entries=sorted(qualified),entry_weights={k:w for k,w in plan.get('entry_weights',{}).items() if int(k) in qualified},
                        fraction=old['fraction'] if old else plan.get('_initial_fraction',self.b['initial_fraction']),
                        published=old['published'] if old else time.monotonic(),last_use=time.monotonic(),validation=v,qualified=True)
                    self.register_family(self.regions[version], started)
                self.log('CANARY_UPDATED' if old else 'CANARY',id=version,validation=v,progressive=True,entries=len(qualified))
                self.publish(time.monotonic())
            progress=publish_validated if self.b.get('progressive_validation',False) and not (plan.get('replica_of') or plan.get('_pending_activation')) else None
            validation = self.validate(plan, version, progress)
            (self.directory/(version+'_validation.json')).write_text(json.dumps(validation, indent=2))
            if not validation['passed']: raise ValueError('Replica quality/unloading gate failed: '+validation['reason'])
            if 'qualified_entries' in validation and not plan.get('replica_of'):
                qualified=set(validation['qualified_entries'])
                self.log('ENTRY_QUALITY_FILTER',id=version,before=len(plan['entries']),after=len(qualified))
                plan['entries']=sorted(qualified)
                plan['entry_weights']={k:v for k,v in plan['entry_weights'].items() if int(k) in qualified}
            now = time.monotonic()
            with self.lock:
                if plan.get('replica_of') and plan['replica_of'] not in self.regions:
                    raise ValueError('Expansion parent withdrawn during construction')
                old=self.regions.get(version)
                fraction=old['fraction'] if old else plan.get('_initial_fraction',self.b['initial_fraction'])
                if plan.get('replica_of'):
                    # Recheck after the asynchronous build. Adjustment/decay may
                    # have reduced a donor since the proposal was made.
                    family=plan['_replica_family']
                    if self.b.get('entry_admission',{}).get('enabled'):
                        plan['closed_entries']=sorted(set(plan.get('closed_entries',[])) |
                            set(self.regions[plan['replica_of']].get('closed_entries',[])))
                    donors=plan.get('_fraction_donors',{})
                    if any(k not in self.regions or self.regions[k].get('family_id')!=family for k in donors):
                        raise ValueError('Expansion donor withdrawn during build')
                    released=sum(min(v,self.regions[k]['fraction']) for k,v in donors.items())
                    total=sum(r['fraction'] for r in self.regions.values() if r.get('family_id')==family)
                    fraction=min(fraction,max(0.,self.b['max_fraction']-total+released))
                    if fraction<self.b['adaptive_expansion']['min_fraction']:
                        raise ValueError('Expansion no longer has a useful routing share')
                    plan['_applied_donors']={k:min(v,self.regions[k]['fraction']) for k,v in donors.items()}
                    for k,v in plan['_applied_donors'].items():self.regions[k]['fraction']-=v
                self.regions[version] = dict(plan, id=version, bytes=source['manifest']['bytes']+plan.get('_demand_reserved_bytes',0),core_bytes=source['manifest']['bytes'],fraction=old['fraction'] if old else plan.get('_initial_fraction',self.b['initial_fraction']),
                    published=old['published'] if old else now, last_use=now, validation=validation, qualified=True,validated_at=now)
                self.regions[version]['fraction']=fraction
                self.register_family(self.regions[version], started)
                self.in_progress = None
            self.log('STAGED_VALIDATED' if plan.get('_pending_activation') else ('CANARY_UPDATED' if old else 'CANARY'), id=version, validation=validation, fraction=self.regions[version]['fraction'])
            self.publish(now)
            return version
        except BaseException as e:
            with self.lock:
                self.regions.pop(version,None)
                family=plan.get('_replica_family')
                for k,v in plan.get('_applied_donors',{}).items():
                    if k in self.regions:
                        total=sum(r['fraction'] for r in self.regions.values() if r.get('family_id')==family)
                        self.regions[k]['fraction']+=min(v,max(0.,self.b['max_fraction']-total))
                self.withdraw_unused_families(time.monotonic())
            self.publish(time.monotonic())
            self.log('FAILED', id=version, error=str(e))
            try:
                until=time.monotonic()+self.b['operation_timeout_s']
                while not self.rpc(plan['source'],'source_cancel',id=version)['quiescent']:
                    if time.monotonic()>=until:raise TimeoutError('Source cancellation not quiescent')
                    time.sleep(.05)
                self.rpc(plan['target'], 'retire', id=version)
                cleanup_until = time.monotonic()+self.b['operation_timeout_s']
                while not self.rpc(plan['target'], 'reap', id=version)['reclaimed']:
                    if time.monotonic() >= cleanup_until: raise TimeoutError('Replica cleanup still pinned')
                    time.sleep(.05)
            except BaseException as cleanup:
                with self.lock:
                    claim=getattr(self,'build_reservations',{}).get(version)
                    if claim:self.draining[version]=dict(claim,id=version,_source_cleanup_pending=True)
                self.log('CLEANUP_PENDING', id=version, error=str(cleanup),capacity_still_reserved=True)
            raise
        finally:
            with self.lock:
                self.in_progress = None
                getattr(self,'build_reservations',{}).pop(version,None)
                self.cooldown_until = time.monotonic()+self.b['cooldown_ms']/1000

    def validate(self, plan, version, progress=None):
        if self.b.get('validation_mode','ground_truth')=='shadow':return self.validate_shadow(plan,version,progress)
        path = self.b['calibration_file']
        if not path: return dict(passed=False, reason='No independent ground-truth calibration file')
        with np.load(path, allow_pickle=False) as data:
            queries = np.array(data['queries'], dtype='float64')
            ids = np.array(data['ids'], dtype='uint64')
            truth = np.array(data['truth'], dtype='uint64')
        if queries.ndim != 2 or len(ids) != len(queries) or len(truth) != len(ids) or truth.shape[1] < self.c['search']['top_k']:
            raise ValueError('Invalid replica calibration arrays')
        entries, training = set(plan['entries']), set(plan['training_query_ids'])
        selected = []
        for i, (q, qid) in enumerate(zip(queries, ids)):
            if self.stop.is_set(): raise RuntimeError('Replica validation cancelled')
            if int(qid) in training: continue
            info = self.engine.runtime.route_info(q, int(qid))
            if info['source'] == plan['source'] and info['entry_id'] in entries and plan['min_score'] <= info['score'] <= plan['max_score']:
                selected.append((i, info))
                if len(selected)>=self.b.get('max_validation_queries',4096):break
        if len(selected) < self.b['min_validation_queries']:
            return dict(passed=False, reason='Too few independent matching calibration queries', matched=len(selected))
        def evaluate(item):
            i, info = item
            descriptor = dict(entry_id=info['entry_id'], source=plan['source'], target=plan['source'], version='')
            baseline = self.engine.runtime.validate_replica(queries[i], int(ids[i]), descriptor)
            descriptor.update(target=plan['target'], version=version)
            replica = self.engine.runtime.validate_replica(queries[i], int(ids[i]), descriptor)
            if baseline['status'] != 'ok' or replica['status'] != 'ok':
                raise ValueError('Replica calibration query failed: '+str(replica.get('error',baseline.get('error'))))
            k = self.c['search']['top_k']; expected = set(map(int, truth[i, :k]))
            def cpu(result, sid):
                return sum(b['stats']['server_thread_cpu_ms']/1000 for b in result['branch_reports'] if b['node_id'] == sid)
            return dict(id=int(ids[i]), baseline_recall=len(set(baseline['ids']) & expected)/k,
                replica_recall=len(set(replica['ids']) & expected)/k,
                source_saved_s=cpu(baseline,plan['source'])-cpu(replica,plan['source']),
                target_added_s=cpu(replica,plan['target'])-cpu(baseline,plan['target']),
                baseline_work=baseline['stats']['expanded'], replica_work=replica['stats']['expanded'],
                replica_nodes=[b['node_id'] for b in replica['branch_reports']])
        with ThreadPoolExecutor(max_workers=self.b['validation_workers']) as pool:
            samples = list(pool.map(evaluate, selected))
        base, recall, saved, added = [float(np.mean([r[key] for r in samples])) for key in
            ('baseline_recall','replica_recall','source_saved_s','target_added_s')]
        passed = recall >= self.b['recall_min'] and recall+self.b['recall_margin'] >= base and saved > 0 and added > 0
        return dict(passed=passed, reason='passed' if passed else 'Recall or net source unloading below gate',
            count=len(samples), baseline_recall=base, replica_recall=recall, source_saved_s=saved,
            target_added_s=added, samples=samples)

    def validate_shadow(self, plan, version, progress=None):
        # Online controller may use observed, UNLABELLED queries. No test truth
        # is read here. Agreement with an original search is a guard, not proof
        # of recall; the complete workload is evaluated independently offline.
        entries=set(plan['entries'])-set(plan.get('closed_entries',[]));training=set(plan.get('training_query_ids',[]))
        online=self.b.get('online_policy',{}).get('enabled',False)
        from .online_replica_policy import held_out
        with self.lock:
            pool={q['query_id']:q for q in self.history if q['source']==plan['source'] and
                  q['entry'] in entries and 'query_f32' in q and (online or q['replica'] is None) and q['query_id'] not in training
                  and (not online or (q['query_id'] is not None and held_out(q['query_id'],self.b['online_policy']['holdout_modulus'])))}
            references=[dict(r,entry_set=set(r['entries'])) for r in self.regions.values()
                        if r['source']==plan['source'] and r['id']!=version and not r.get('_pending_activation')]
        probes=list(pool.values())[-self.b.get('max_validation_queries',32):]
        if online:
            # At least several independent IDs per admitted entry; one lucky
            # replay must not certify an entire entry's future traffic.
            groups=defaultdict(list)
            for q in pool.values():groups[q['entry']].append(q)
            minimum=self.b.get('min_entry_validation_queries',1)
            limit=self.b.get('max_validation_queries',32)
            selected=sorted((e for e,qs in groups.items() if len(qs)>=minimum),
                            key=lambda e:(-len(groups[e]),e))[:max(1,limit//minimum)]
            for e in selected:groups[e].sort(key=lambda q:(int(q['query_id'])*0x9e3779b97f4a7c15)&((1<<64)-1))
            probes=[];index=0
            while len(probes)<limit:
                batch=[groups[e][index] for e in selected if index<len(groups[e])]
                if not batch:break
                probes.extend(batch[:limit-len(probes)]);index+=1
        if len(probes)<self.b['min_validation_queries']:
            return dict(passed=False,reason='Too few unlabelled shadow probes',matched=len(probes))
        def evaluate(q):
            query=np.frombuffer(q['query_f32'],dtype='<f4').astype('float64');qid=int(q['query_id'])
            d=dict(entry_id=q['entry'],source=plan['source'],target=plan['source'],version='')
            cached=not online and self.b.get('reuse_observed_baseline',False) and 'result_ids' in q
            base=None if cached else self.engine.runtime.validate_replica(query,qid,d)
            d.update(target=plan['target'],version=version)
            rep=self.engine.runtime.validate_replica(query,qid,d)
            if (base is not None and base['status']!='ok') or rep['status']!='ok':raise ValueError('Shadow query failed')
            def cpu(r,node):return sum(b['stats']['server_thread_cpu_ms']/1000 for b in r['branch_reports'] if b['node_id']==node)
            root=next(b for b in rep['branch_reports'] if b['parent_branch_id'] is None)
            original_cpu=lambda node:sum(b['cpu_ms']/1000 for b in q['branches'] if b['node']==node) if cached else cpu(base,node)
            sample=dict(id=qid,entry=q['entry'],baseline_cached=cached,agreement=len(set(q['result_ids'] if cached else base['ids']) & set(rep['ids']))/self.c['search']['top_k'],
                source_saved_s=original_cpu(plan['source'])-cpu(rep,plan['source']),
                target_added_s=cpu(rep,plan['target'])-original_cpu(plan['target']),
                local_expansions=root['work_spent'],early_exit=bool(root['children']) and root['work_spent']<self.b.get('min_replica_expansions',32))
            if plan.get('replica_tier')=='large' and not plan.get('replica_of') and not cached:
                # Compare the staged upgrade with the current layout as well as
                # the original source, on the SAME held-out query. Expected
                # current source cost includes source-bound probability mass.
                current=original_cpu(plan['source'])
                for old in references:
                    if q['entry'] not in old['entry_set'] or not old['min_score']<=q['score']<=old['max_score'] or old['fraction']<=0:continue
                    prev=self.engine.runtime.validate_replica(query,qid,dict(entry_id=q['entry'],source=plan['source'],target=old['target'],version=old['id']))
                    if prev['status']!='ok':raise ValueError('Current-layout calibration failed')
                    current+=old['fraction']*(cpu(prev,plan['source'])-original_cpu(plan['source']))
                sample['incremental_source_saved_s']=current-cpu(rep,plan['source'])
            return sample
        samples=[]
        with ThreadPoolExecutor(max_workers=self.b['validation_workers']) as pool:
            futures=[pool.submit(evaluate,q) for q in probes]
            for future in as_completed(futures):
                samples.append(future.result())
                if progress and len(samples)%self.b.get('validation_publish_batch',32)==0:
                    progress(self.shadow_summary(samples))
        if plan.get('replica_of') or plan.get('_fixed_contract') or self.b.get('entry_admission',{}).get('enabled'):
            # A fixed family cannot silently route rejected entries on the new
            # target. Evaluate ALL probes; entry filtering is for new contracts.
            avg=lambda key:float(np.mean([s[key] for s in samples]))
            agreement,saved,added,early=map(avg,('agreement','source_saved_s','target_added_s','early_exit'))
            passed=(agreement>=self.b.get('shadow_overlap_min',.95) and saved>0 and added>0
                    and early<=self.b.get('max_early_exit_fraction',.2))
            return dict(passed=passed,reason='passed' if passed else 'Fixed family agreement or net-saving guard failed',
                        gate='observed_unlabelled_fixed_contract_not_recall',count=len(samples),agreement=agreement,
                        source_saved_s=saved,target_added_s=added,early_exit_fraction=early,samples=samples)
        result=self.shadow_summary(samples)
        if samples and 'incremental_source_saved_s' in samples[0] and result.get('passed'):
            accepted=set(result['qualified_entries'])
            gain=float(np.mean([s['incremental_source_saved_s'] for s in samples if s['entry'] in accepted]))
            result['incremental_source_saved_s']=gain
            if gain<=0:result.update(passed=False,reason='Upgrade does not unload source beyond the current layout')
        return result

    def shadow_summary(self, samples):
        all_samples=samples
        groups=defaultdict(list)
        for s in samples:groups[s['entry']].append(s)
        qualified=[]
        for entry,rows in groups.items():
            avg=lambda key:float(np.mean([s[key] for s in rows]))
            if len(rows)>=self.b.get('min_entry_validation_queries',1) and avg('agreement')>=self.b.get('shadow_overlap_min',.95) and avg('early_exit')<=self.b.get('max_early_exit_fraction',.2) and avg('source_saved_s')>0 and avg('target_added_s')>0:
                qualified.append(entry)
        samples=[s for s in all_samples if s['entry'] in qualified]
        if len(samples)<self.b['min_validation_queries']:
            return dict(passed=False,reason='Too few probes at qualified entries',qualified_entries=qualified,
                qualified_count=len(samples),count=len(all_samples),samples=all_samples)
        avg=lambda key:float(np.mean([s[key] for s in samples]))
        agreement,saved,added,early=map(avg,('agreement','source_saved_s','target_added_s','early_exit'))
        passed=agreement>=self.b.get('shadow_overlap_min',.95) and saved>0 and added>0 and early<=self.b.get('max_early_exit_fraction',.2)
        return dict(passed=passed,reason='passed' if passed else 'Shadow agreement, residence or unloading guard failed',
            gate='observed_unlabelled_replay_not_recall',count=len(samples),agreement=agreement,
            source_saved_s=saved,target_added_s=added,early_exit_fraction=early,
            mean_local_expansions=avg('local_expansions'),qualified_entries=qualified,total_probes=len(all_samples),samples=all_samples)

    def save_counters(self):
        head=self.engine.runtime.metrics().get('replica_access_counts',{})
        peer={str(n['id']):self.rpc(n['id'],'counters') for n in self.c['cluster']['nodes']}
        with self.lock:
            snapshot=dict(time_ns=time.monotonic_ns(),lifecycle=dict(self.counters),regions=head,peer=peer,
                          routing_funnel=self.engine.runtime.metrics().get('replica_routing_funnel',{}))
            (self.directory/'counters.json').write_text(json.dumps(snapshot,indent=2))
        return snapshot

    def set_fraction(self, fraction):
        if not 0 <= fraction <= self.b['max_fraction']: raise ValueError('Replica fraction exceeds configured cap')
        with self.lock:
            if self.active is None: raise ValueError('No validated replica')
            family = self.active.get('family_id')
            other = sum(r['fraction'] for r in self.regions.values() if family and r is not self.active and r.get('family_id') == family)
            if other+fraction > self.b['max_fraction']+1e-12:
                raise ValueError('Total family fraction exceeds configured cap')
            self.active['fraction'] = fraction
            self.log('FRACTION', id=self.active['id'], fraction=fraction)

    def set_family_fractions(self, family, fractions):
        """Set every live target share atomically; publish() installs one directory.

        Requiring the complete target set prevents a caller from accidentally
        retaining an old share and exceeding the family's routing budget.
        Invalid updates leave all existing shares unchanged.
        """
        if not isinstance(family, str) or not family or not isinstance(fractions, dict):
            raise ValueError('Invalid replica family fractions')
        if not fractions or any(type(k) is not int or type(v) not in (int, float)
                or not math.isfinite(v) or v < 0 for k, v in fractions.items()):
            raise ValueError('Invalid target or replica fraction')
        with self.lock:
            members = [r for r in self.regions.values() if r.get('family_id') == family]
            targets = {r['target'] for r in members}
            if not members or len(targets) != len(members) or targets != set(fractions):
                raise ValueError('Family update must cover every live target exactly once')
            if sum(fractions.values()) > self.b['max_fraction'] + 1e-12:
                raise ValueError('Total family fraction exceeds configured cap')
            for r in members:
                r['fraction'] = float(fractions[r['target']])
            self.log('FAMILY_FRACTIONS', family=family, fractions=fractions)

    def publish(self, now):
        with self.lock:
            covered=self.b.get('entry_admission',{}).get('enabled',False)
            fractions = defaultdict(float)
            for r in self.regions.values():fractions[r.get('family_id',r['id'])] += r['fraction']
            if any(p > self.b['max_fraction']+1e-12 for p in fractions.values()):
                raise ValueError('Replica family fractions exceed cap')
            rows=[];peer=[];claimed=set()
            for r in self.regions.values():
                if r.get('_pending_activation'):continue
                usable=self.fresh(r['source'],now) and self.fresh(r['target'],now)
                if r.get('epoch') and any(self.loads.get(n,{}).get('epoch',r['epoch']) != r['epoch'] for n in (r['source'],r['target'])):usable=False
                if not usable and not covered:continue
                row=dict(source=r['source'],target=r['target'],version=r['id'],entries=r['entries'],fraction=r['fraction'] if usable else 0.,
                         min_score=r['min_score'],max_score=r['max_score'],ttl_ms=self.b['max_load_age_ms'])
                if r.get('family_id'):row['family']=r['family_id']
                if self.b.get('quality_weight_routing',True) and r.get('entry_weights'):row['entry_weights']=r['entry_weights']
                if covered:
                    closed=set(r.get('closed_entries',[]))
                    row['entry_weights']={str(e):0. if e in closed else 1. for e in r['entries']}
                rows.append(row)
                # Deterministic first ownership avoids ambiguous peer candidates.
                weighted=[x for x in r.get('peer_entries',[]) if x[0] not in claimed]
                claimed.update(x[0] for x in weighted)
                if weighted:peer.append(dict(source=r['source'],target=r['target'],version=r['id'],fraction=r['fraction'],
                    ttl_ms=self.b['max_load_age_ms'],weighted_entries=weighted))
            self.engine.runtime.replica_routes(rows)
        if self.b.get('peer_routes_enabled',False):
            for n in self.c['cluster']['nodes']:self.rpc(n['id'],'routes',routes=peer)

    def activate_portfolio(self, versions, replaced):
        """One directory switch after the entire new portfolio passed its gates.

        Keep old entries that the new validated contract did not cover. Physical
        retirement follows the switch and remains charged until reap succeeds.
        """
        now=time.monotonic()
        with self.lock:
            new=[self.regions[v] for v in versions]
            if not new or any(not r.get('_pending_activation') or not r.get('qualified') for r in new):
                raise ValueError('Portfolio is not completely staged and validated')
            source=new[0]['source']
            if any(r['source']!=source or not all(self.fresh(n,now) for n in (source,r['target']))
                   or self.loads[source].get('epoch')!=r.get('epoch') for r in new):
                raise ValueError('Portfolio activation has stale source/target or epoch')
            from .online_replica_policy import pressure
            if self.b.get('online_policy',{}).get('enabled') and any(
                    pressure(self.loads[r['target']]) is None
                    or pressure(self.loads[r['target']])>self.b['target_queue_limit_ms']
                    or self.loads[r['target']]['util']>self.b['target_util_limit'] for r in new):
                raise ValueError('Portfolio activation target pressure changed')
            claimed=[e for r in new for e in r['entries']]
            if not self.b.get('entry_admission',{}).get('enabled') and len(claimed)!=len(set(claimed)):raise ValueError('Portfolio entry ownership collision')
            claimed=set(claimed)
            old=[self.regions[v] for v in replaced if v in self.regions]
            if any(r['source']!=source for r in old):raise ValueError('Portfolio replacement source mismatch')
            # Reject concurrent unrelated owners; never let insertion order
            # silently choose which family receives an overlapping entry.
            if not self.b.get('entry_admission',{}).get('enabled') and any(claimed.intersection(r['entries']) for k,r in self.regions.items()
                   if k not in set(versions)|set(replaced) and r['source']==source):
                raise ValueError('Portfolio conflicts with an unowned live contract')
            before={r['id']:dict(r) for r in new+old}
            retiring=[]
            try:
                for r in old:
                    r['entries']=sorted(set(r['entries'])-claimed)
                    if '_candidate_entries' in r:r['_candidate_entries']=sorted(set(r['_candidate_entries'])-claimed)
                    r['qualified']=False  # Changed cohort requires new costs.
                    if not r['entries']:
                        retiring.append(r['id']);self.draining[r['id']]=self.regions.pop(r['id'])
                for r in new:r['_pending_activation']=False
                self.publish(now)
            except BaseException:
                for version,r in before.items():
                    self.draining.pop(version,None);self.regions[version]=r
                self.publish(now)
                raise
            self.withdraw_unused_families(now)
        self.log('PORTFOLIO_ACTIVATED',versions=versions,replaced=list(replaced),retiring=retiring,entries=len(claimed))
        self.poll_reclaims(now)

    def poll_calibration(self, now):
        """One bounded background replay, with no RPC under the controller lock."""
        if not self.b.get('online_policy',{}).get('enabled'):return
        future=self.calibration_future
        if future is not None and future.done():
            version=self.calibration_version;self.calibration_future=None
            try:result=future.result()
            except Exception as exc:
                self.log('CALIBRATION_FAILED',id=version,error=str(exc));result=None
            if result is not None:
                retiring=[]
                with self.lock:
                    r=self.regions.get(version)
                    if r is not None:
                        r['last_calibration_attempt']=now
                        if self.b.get('entry_admission',{}).get('enabled'):
                            # User-selected residence admission owns portal
                            # closure. Shadow overlap remains an explicit
                            # diagnostic; never certify recall or zero a family.
                            r['shadow_diagnostic']=result
                            if result.get('passed'):
                                r.update(validation=result,qualified=True,validated_at=now)
                            else:r['qualified']=False
                        elif result.get('passed'):
                            members=[x for x in self.regions.values() if x.get('family_id')==r.get('family_id')]
                            qualified=set(result.get('qualified_entries',r['entries']))
                            if len(members)==1 and not self.in_progress:
                                r.setdefault('_candidate_entries',list(r['entries']))
                                r['entries']=sorted(qualified)
                            r.update(validation=result,qualified=True,validated_at=now)
                            r['_calibration_failures']=0
                        elif result.get('count',0)>=self.b['min_validation_queries']:
                            # Failed quality/net-saving evidence pauses new
                            # traffic; lack of probes is unknown, not a failure.
                            family=r.get('family_id')
                            for member in self.regions.values():
                                if member.get('family_id')==family:
                                    member.update(qualified=False,fraction=0.)
                            r['validation']=result
                            r['_calibration_failures']=r.get('_calibration_failures',0)+1
                            if r['_calibration_failures']>=self.b['online_policy'].get('calibration_failure_limit',2):
                                retiring=[member['id'] for member in self.regions.values() if member.get('family_id')==family]
                                if not hasattr(self,'rejected_entries'):self.rejected_entries={}
                                for e in r.get('_candidate_entries',r['entries']):
                                    self.rejected_entries[(r['source'],e)]=now+self.b['online_policy'].get('rejected_entry_cooldown_seconds',120.)
                self.log('CALIBRATION_COMPLETED',id=version,validation=result)
                for v in retiring:self.retire(v,wait=False)
        if self.calibration_future is not None or self.in_progress is not None:return
        cfg=self.b['online_policy']
        with self.lock:
            candidates=[r for r in self.regions.values() if not r.get('_pending_activation') and
                        now-r.get('last_calibration_attempt',r['published'])>=cfg['calibration_interval_seconds']
                        and all(self.fresh(n,now) for n in (r['source'],r['target']))]
            if not candidates:return
            r=min(candidates,key=lambda v:v.get('last_calibration_attempt',v['published']))
            r['last_calibration_attempt']=now
            fixed=sum(x.get('family_id')==r.get('family_id') for x in self.regions.values())>1
            plan=dict(r,entries=r['entries'] if fixed else r.get('_candidate_entries',r['entries']),_fixed_contract=fixed)
            self.calibration_version=r['id']
            self.calibration_future=self.calibrator.submit(self.validate,plan,r['id'])
        self.log('CALIBRATION_STARTED',id=r['id'])

    def adjust(self, now):
        retire=[];reserved=defaultdict(float)
        with self.lock:
            for r in list(self.regions.values()):
                if r.get('_pending_activation'):continue
                if not self.fresh(r['source'],now) or not self.fresh(r['target'],now):continue
                a,b=self.loads[r['source']],self.loads[r['target']]
                rate=sum(self.rates[e] for e in r['entries']);old=p=r['fraction']
                target_queue=b.get('backlog_estimate_ms',b['queue_ms'])
                online=self.b.get('online_policy',{})
                if online.get('enabled'):
                    from .online_replica_policy import pressure
                    target_queue=pressure(b)
                    if target_queue is None:continue
                overloaded=b['util']>self.b['target_util_limit'] or target_queue>self.b.get('target_queue_limit_ms',self.b['queue_high_ms'])
                if overloaded:
                    if online.get('enabled'):
                        since=r.setdefault('_overload_started',now)
                        if now-since>=online['target_backoff_seconds']:
                            p*=.5;r['_overload_started']=now
                    else:p*=.5
                elif self.b.get('entry_admission',{}).get('enabled') and r.get('publication_mode')=='ready':
                    if rate>1 and a['util']>b['util'] and b['util']<=self.b['target_util_low'] and target_queue<=self.b['queue_low_ms']:
                        family=r.get('family_id')
                        other=sum(x['fraction'] for x in self.regions.values() if x is not r and x.get('family_id')==family)
                        p=min(self.b['initial_fraction'],max(0.,self.b['max_fraction']-other),p+self.b['fraction_step'])
                elif self.b.get('adaptive_expansion', {}).get('enabled'):
                    pass  # Family controller owns all increases; fractions must add up.
                elif r.get('publication_mode')=='ready':
                    # No shadow-derived cost estimates exist. Use observed
                    # pressure; recover only to the explicitly configured
                    # initial fraction, never extrapolate a fictitious saving.
                    if rate>1 and a['util']>b['util'] and b['util']<=self.b['target_util_low'] and target_queue<=self.b['queue_low_ms']:
                        p=min(self.b['max_fraction'],self.b['initial_fraction'],p+self.b['fraction_step'])
                    elif a['util']<b['util']:p=max(0.,p-self.b['fraction_step'])
                elif rate>1 and a['util']>b['util']:
                    d,c=r['validation']['source_saved_s'],r['validation']['target_added_s']
                    if d>0 and c>0:
                        equilibrium=p+(a['util']-b['util'])/(rate*(d/a['workers']+c/b['workers']))
                        spare=max(0.,(self.b['target_util_limit']-b['util'])*b['workers']-reserved[r['target']])
                        cap=p+spare/(rate*c)
                        p=max(0.,min(self.b['max_fraction'],p+self.b['fraction_step'],equilibrium,cap))
                        reserved[r['target']]+=max(0.,p-old)*rate*c
                elif a['util']<b['util']:p=max(0.,p-self.b['fraction_step'])
                if not overloaded:r.pop('_overload_started',None)
                r['fraction']=p
                if abs(p-old)>1e-9:self.log('ADJUST',id=r['id'],old=old,fraction=p,eligible_rate=rate,
                    estimated_source_relief_cores=None if r.get('publication_mode')=='ready' else rate*p*r['validation']['source_saved_s'])
                if getattr(self,'decay',None) is None and now-r['last_use']>self.b['idle_retire_ms']/1000 and now-r['published']>self.b['min_residency_ms']/1000:retire.append(r['id'])
        for version in retire:
            self.retire(version, wait=False)

    def retire(self, version=None, wait=True):
        with self.lock:
            removed=list(self.regions.values()) if version is None else ([self.regions[version]] if version in self.regions else [])
            for r in removed:
                self.regions.pop(r['id'])
                self.draining[r['id']] = r
                family = r.get('family_id')
                if self.decay is not None and family and not any(x.get('family_id') == family for x in self.regions.values()):
                    self.decay.withdraw(family, time.monotonic())
        self.publish(time.monotonic())
        for r in removed:
            self.log('WITHDRAWN', id=r['id'], reserved_bytes=r.get('bytes', 0))
        self.poll_reclaims(time.monotonic(), force=wait)
        if wait:
            with self.lock:
                futures = list(self.reclaim_futures.values()) if version is None else ([self.reclaim_futures[version]] if version in self.reclaim_futures else [])
            for future in futures:
                future.result()

    def reclaim(self, r):
        try:
            if r.get('_source_cleanup_pending'):
                if not self.rpc(r['source'],'source_cancel',id=r['id'])['quiescent']:
                    raise RuntimeError('Failed source producer still active')
                r['_source_cleanup_pending']=False
            self.rpc(r['target'], 'retire', id=r['id'])
            self.log('DRAINING', id=r['id'])
            deadline = time.monotonic()+self.b['operation_timeout_s']
            while time.monotonic() < deadline:
                if self.rpc(r['target'], 'reap', id=r['id'])['reclaimed']:
                    with self.lock:
                        self.draining.pop(r['id'], None)
                        family = r.get('family_id')
                        if family and not any(x.get('family_id')==family for x in list(self.regions.values())+list(self.draining.values())):
                            for table in ('expansion_streaks','expansion_cooldown','shrink_streaks','family_rates'):
                                getattr(self,table,{}).pop(family,None)
                    self.log('RETIRED', id=r['id'], released_bytes=r.get('bytes', 0))
                    return
                time.sleep(self.b.get('lifecycle_poll_ms', 100)/1000)
            raise TimeoutError('Replica still pinned after drain deadline')
        except BaseException as exc:
            with self.lock:
                self.reclaim_retry[r['id']] = time.monotonic()+self.b.get('adaptive_decay', {}).get('reclaim_retry_ms', 1000)/1000
            self.log('CLEANUP_PENDING', id=r['id'], error=str(exc), capacity_still_reserved=True)
            raise

    def poll_reclaims(self, now, force=False):
        with self.lock:
            for version in list(self.reclaim_futures):
                if version not in self.draining and self.reclaim_futures[version].done():
                    self.reclaim_futures.pop(version)
                    self.reclaim_retry.pop(version, None)
            for version, region in self.draining.items():
                future = self.reclaim_futures.get(version)
                if future is not None and not future.done():
                    continue
                if force or now >= self.reclaim_retry.get(version, 0):
                    self.reclaim_futures[version] = self.reclaimer.submit(self.reclaim, region)

    def run(self):
        while not self.stop.is_set():
            try:
                self.observe(); now = time.monotonic()
                self.poll_calibration(now)
                self.poll_entry_admission(now)
                self.publish(now)
                if self.b['mode'] == 'dynamic':
                    if self.b.get('planner_mode','inline')=='native_async':self.poll_plan(now)
                    if now-self.last_adjust >= self.b['route_adjust_ms']/1000:
                        self.adjust(now); self.last_adjust=now
                    if now-self.last_control >= self.b['control_interval_ms']/1000:
                        self.last_control=now
                        self.control(now)
            except BaseException as e:
                self.log('OBSERVER_ERROR', error=str(e))
                try: self.engine.runtime.replica_routes([])
                except BaseException: pass
            self.stop.wait(self.b['load_interval_ms']/1000)

    def control_expansion(self, now):
        from .replica_policy import expansion_decision
        cfg = self.b['adaptive_expansion']
        with self.lock:
            busy=self.in_progress is not None or getattr(self,'plan_future',None) is not None
            groups = defaultdict(list)
            for r in self.regions.values():groups[r['family_id']].append(r)
            fresh = {sid:v for sid,v in self.loads.items() if self.fresh(sid,now)}
            for family, members in groups.items():
                if now < self.expansion_cooldown.get(family,0):continue
                if any(n not in fresh for r in members for n in (r['source'],r['target'])):
                    self.expansion_streaks.pop(family,None);continue
                if any(r.get('epoch') and fresh[r['source']].get('epoch',r['epoch'])!=r['epoch'] for r in members):
                    self.expansion_streaks.pop(family,None);continue
                rate = self.family_rates.get(family,0.)
                decision = expansion_decision(members,fresh,rate,cfg,self.b['max_fraction'],now)
                heat = self.decay.states.get(family) if self.decay is not None else None
                if heat is not None and heat.state != 'HOT':
                    decision = dict(action='hold',reason='family_cooling')
                if fresh[members[0]['source']].get('queue_ms',0)<self.b['queue_high_ms']:
                    decision = dict(action='hold',reason='source_queue_low')
                self.policy_log('FAMILY_DECISION',family,**decision)
                if decision['action'] in ('increase','redistribute'):
                    r = self.regions[decision['version']]
                    for donor,share in decision.get('donors',{}).items():self.regions[donor]['fraction']-=share
                    r['fraction'] += decision['fraction']
                    self.log('FAMILY_FRACTION_INCREASE',family=family,**decision)
                    self.expansion_streaks.pop(family,None)
                    return True
                if decision['action'] == 'copy':
                    if busy:continue
                    target=decision['target']
                    previous,count=self.expansion_streaks.get(family,(None,0))
                    count=count+1 if previous==target else 1
                    self.expansion_streaks[family]=(target,count)
                    if count < cfg['sustained_windows']:return True
                    reserve=self.b.get('online_policy',{}).get('transition_slots',0) if self.b.get('online_policy',{}).get('enabled') else 0
                    if len(self.regions)+len(self.draining)>=self.b['max_replicas']-reserve:
                        self.policy_log('FAMILY_CAPACITY_BLOCKED',family,reason='transition_slots_reserved');continue
                    from .replica_budget import available
                    parent=members[0]
                    required=parent.get('reserved_package_bytes',parent['bytes'])
                    if required>available(self,parent['source'],parent.get('source_capacity_bytes'),target):
                        self.policy_log('FAMILY_CAPACITY_BLOCKED',family,required=required,target=target)
                        continue
                    if any(r.get('family_id')==family and r['target']==target for r in self.draining.values()):return True
                    plan={k:parent[k] for k in ('source','ids','entries','min_score','max_score','region_proof','region_stats','selection_policy','training_query_ids','epoch','source_capacity_bytes','replica_tier','certified_entries','entry_admission','closed_entries') if k in parent}
                    if 'closed_entries' in plan:plan['closed_entries']=list(plan['closed_entries'])
                    plan.update(target=target,replica_of=parent['id'],_replica_family=family,_initial_fraction=decision['fraction'],
                                reserved_package_bytes=required,_fraction_donors=decision.get('donors',{}))
                    self.log('FAMILY_COPY_PROPOSED',family=family,source=parent['source'],**decision)
                    self.expansion_cooldown[family]=now+cfg['cooldown_ms']/1000
                    self.expansion_streaks.pop(family,None)
                    try:self.create(plan)
                    except (ValueError,RuntimeError) as exc:self.log('FAMILY_COPY_REJECTED',family=family,error=str(exc))
                    return True
                self.expansion_streaks.pop(family,None)
                if busy:continue  # A build may pin a parent; do not shrink it.
                # Remove an extra copy only when surviving copies can absorb
                # its current share with headroom and the source is not hot.
                if len(members)<2 or rate<=0 or fresh[members[0]['source']]['util']>cfg['source_goal']:
                    self.shrink_streaks.pop(family,None);continue
                drop=min(members,key=lambda r:(r['fraction'],r['id']))
                if now-drop['published']<self.b['min_residency_ms']/1000:continue
                capacities={}
                for r in members:
                    if r is drop:continue
                    cost=r.get('validation',{}).get('target_added_s')
                    if not cost or cost<=0 or now-r.get('validated_at',r['published'])>cfg['cost_max_age_ms']/1000:continue
                    load=fresh[r['target']]
                    if load.get('backlog_estimate_ms',load.get('queue_ms',0))>cfg['target_queue_ms']:continue
                    capacities[r['id']]=max(0.,(cfg['target_goal']-load['util'])*load['workers'])/(rate*cost)
                capacity=sum(capacities.values())
                if capacity < drop['fraction']*(1+cfg['scale_down_margin']) or capacity<=0:
                    self.shrink_streaks.pop(family,None);continue
                old,count=self.shrink_streaks.get(family,(None,0));count=count+1 if old==drop['id'] else 1
                self.shrink_streaks[family]=(drop['id'],count)
                if count < cfg['scale_down_windows']:continue
                for version,room in capacities.items():self.regions[version]['fraction']+=drop['fraction']*room/capacity
                drop['fraction']=0.
                self.log('FAMILY_SCALE_DOWN',family=family,id=drop['id'])
                self.expansion_cooldown[family]=now+cfg['cooldown_ms']/1000
                self.shrink_streaks.pop(family,None)
                self.retire(drop['id'],wait=False)
                return True
        return False

    def control(self, now):
        if self.b.get('online_policy',{}).get('enabled'):
            self.control_online(now)
            return
        if self.b.get('adaptive_expansion', {}).get('enabled') and self.control_expansion(now):
            return
        with self.lock:
            possible=[(a,b) for a in self.loads for b in self.loads if a!=b and self.fresh(a,now) and self.fresh(b,now)
                and self.loads[a]['util']>=self.b['source_util_high'] and self.loads[a]['queue_ms']>=self.b['queue_high_ms']
                and self.loads[b]['util']<=self.b['target_util_low'] and self.loads[b].get('backlog_estimate_ms',self.loads[b]['queue_ms'])<=self.b['queue_low_ms']
                and self.loads[a]['util']-self.loads[b]['util']>=self.b['util_gap']]
            if self.b.get('automatic_pipeline',{}).get('enabled'):
                from .automatic_replica import remaining_capacity
                blocked={a:remaining_capacity(self,a,self.loads[a].get('source_index_bytes'))
                         for a,_ in possible}
                blocked={a:cap for a,cap in blocked.items()
                         if cap<self.b['automatic_pipeline']['min_capacity_bytes']}
                # Exhausted capacity is a stable policy state, not a failed
                # planning job. Reevaluate on each window so reclamation can
                # immediately make a source eligible again.
                if blocked and blocked!=getattr(self,'capacity_blocked',{}):
                    self.log('AUTO_CAPACITY_BLOCKED',remaining_bytes=blocked)
                self.capacity_blocked=blocked
                possible=[p for p in possible if p[0] not in blocked]
            pair=max(possible,key=lambda p:self.loads[p[0]]['util']-self.loads[p[1]]['util']) if possible else None
            self.streak=self.streak+1 if pair and pair==self.last_pair else int(pair is not None);self.last_pair=pair
            trigger=pair and self.streak>=self.b['sustained_windows'] and len(self.regions)+len(getattr(self,'draining',{}))<self.b['max_replicas'] and self.in_progress is None and getattr(self,'plan_future',None) is None and now>=self.cooldown_until
        if trigger:
            self.log('IMBALANCED',source=pair[0],target=pair[1],loads=self.loads,windows=self.streak)
            if self.b.get('automatic_pipeline',{}).get('enabled'):
                from .automatic_replica import run
                snapshot=self.plan_snapshot(pair[0])
                self.plan_context=dict(automatic_pipeline=True,source=pair[0],started=now)
                self.plan_future=self.planner.submit(run,self,*pair,snapshot)
                self.log('PLAN_SUBMITTED',source=pair[0],target=pair[1],observations=len(snapshot),path='automatic_partition_pipeline')
                return
            if self.b.get('planner_mode','inline')=='native_async':
                snapshot=self.plan_snapshot(pair[0])
                self.plan_context=dict(started=time.monotonic(),epochs={n:self.loads[n].get('epoch')for n in pair})
                self.plan_future=self.planner.submit(self.propose,*pair,snapshot)
                self.log('PLAN_SUBMITTED',source=pair[0],target=pair[1],observations=len(snapshot))
                return
            try:
                started=time.monotonic()
                plan=self.propose(*pair)
                self.create(dict(plan,_planning_elapsed_seconds=time.monotonic()-started))
            except ValueError as e:self.log('NO_CANDIDATE',reason=str(e));self.cooldown_until=now+self.b['cooldown_ms']/1000

    def policy_log(self, event, key, **details):
        if not hasattr(self,'policy_reasons'):self.policy_reasons={}
        signature=json.dumps(details,sort_keys=True)
        token=(event,key)
        if self.policy_reasons.get(token)!=signature:
            self.policy_reasons[token]=signature
            self.log(event,key=key,**details)

    def control_online(self, now):
        """Queue-aware, elapsed-time decisions, serialized maintenance only."""
        from .online_replica_policy import pressure
        from .automatic_replica import run, remaining_capacity
        from .replica_budget import reservations
        from .community import hotspots
        cfg=self.b['online_policy']
        if now-self.last_policy<cfg['decision_interval_ms']/1000:return
        self.last_policy=now
        with self.lock:
            healthy=(getattr(self,'policy_observations_healthy',True) and len(self.loads)==len(self.c['cluster']['nodes'])
                     and all(self.fresh(n,now) for n in self.loads))
            pairs=[(a,b) for a in self.loads for b in self.loads if a!=b and self.pair_eligible(a,b,now)]
            # Target availability controls admission, not the age of source
            # congestion. A temporarily busy destination cannot erase a hotspot.
            hot_sources={a for a,v in self.loads.items() if self.fresh(a,now) and pressure(v) is not None
                         and pressure(v)>=self.b['queue_high_ms'] and v['util']>=self.b['source_util_high']}
            entries={}
            for source in hot_sources:
                try:
                    ranked,_=hotspots(self.plan_snapshot(source,True),source,
                                     dict(self.b,_include_replica_observations=True))
                    entries[source]=[e for e,w in ranked[:self.b['automatic_pipeline']['max_seed_entries']]]
                except ValueError:entries[source]=[]
            self.online_policy.tick(now,self.loads,entries,hot_sources,healthy)
            busy=self.in_progress is not None or self.plan_future is not None or bool(self.partition_pending)
            if not healthy:return
        # Same-content capacity moves are cheaper than constructing a new graph.
        # A pending copy streak may delay a new graph only for bounded windows.
        if self.control_expansion(now):return
        if busy or now<self.cooldown_until:return
        with self.lock:
            slots=self.b['max_replicas']-len(reservations(self))
            if slots<=0:
                self.policy_log('ONLINE_HOLD','capacity',reason='replica_slots_full')
                return
            pairs.sort(key=lambda p:(-pressure(self.loads[p[0]]),pressure(self.loads[p[1]]),self.loads[p[1]]['util'],p))
            for source,target in pairs:
                members=[r for r in self.regions.values() if r['source']==source and not r.get('_pending_activation')]
                reason=self.online_policy.large_reason(source,now)
                if not reason and any(r.get('replica_tier')=='large' for r in members):reason='large_layout_already_live'
                usable=[r for r in members if r.get('qualified') and
                        now-r.get('validated_at',r['published'])<=self.b['adaptive_expansion']['cost_max_age_ms']/1000 and
                        (r.get('validation',{}).get('source_saved_s') or 0)>self.b['adaptive_expansion']['min_saved_s']]
                if not reason and not usable:reason='large_missing_fresh_positive_calibration'
                if not reason:
                    # Conservative size-scaled wall-time prediction, explicitly
                    # a proxy, not an unmeasured QPS gain assertion.
                    bsource=self.loads[source]['source_index_bytes']
                    estimate=max(r.get('ready_seconds',0.)*bsource*self.b['capacity_policy']['large_alpha']/max(1,r['bytes']) for r in usable)
                    if estimate*cfg['build_amortization_factor']>cfg['benefit_horizon_seconds']:
                        reason='large_build_does_not_amortize'
                if not reason and remaining_capacity(self,source,self.loads[source].get('source_index_bytes'),'large')<self.b['automatic_pipeline']['min_capacity_bytes']:
                    reason='large_transition_capacity'
                if not reason:
                    snapshot=self.plan_snapshot(source,True)
                    self.online_policy.attempted(source,now)
                    self.plan_context=dict(automatic_pipeline=True,source=source,started=now,tier='large')
                    self.plan_future=self.planner.submit(run,self,source,target,snapshot,tier='large',replace_versions=[r['id'] for r in members])
                    self.log('LARGE_PLAN_SUBMITTED',source=source,target=target,estimated_build_seconds=estimate,
                             evidence=self.online_policy.snapshot(),replaced=[r['id'] for r in members])
                    return
                self.policy_log('LARGE_PLAN_HELD',source,reason=reason)
                if slots<=cfg['transition_slots']:
                    self.policy_log('SMALL_PLAN_HELD',source,reason='transition_slots_reserved');continue
                # Disjoint entry contracts may each own a full .3 region. An
                # existing region never consumes another region's alpha.
                snapshot=self.plan_snapshot(source)
                try:ranked,groups=hotspots(snapshot,source,self.b)
                except ValueError:
                    self.policy_log('SMALL_PLAN_HELD',source,reason='no_uncovered_hot_entries');continue
                if len({q['query_id'] for e,w in ranked for q in groups[e]})<self.b['min_group_queries']:
                    self.policy_log('SMALL_PLAN_HELD',source,reason='insufficient_uncovered_history');continue
                if remaining_capacity(self,source,self.loads[source].get('source_index_bytes'))<self.b['automatic_pipeline']['min_capacity_bytes']:
                    self.policy_log('SMALL_PLAN_HELD',source,reason='physical_capacity');continue
                heat=self.online_policy.states.get(source)
                if heat is None or heat.run<cfg['min_episode_seconds']:continue
                self.plan_context=dict(automatic_pipeline=True,source=source,started=now,tier='small')
                self.plan_future=self.planner.submit(run,self,source,target,snapshot)
                self.log('PLAN_SUBMITTED',source=source,target=target,observations=len(snapshot),path='online_distinct_small')
                return

    def close(self):
        self.stop.set();self.thread.join()
        self.planner.shutdown(wait=True,cancel_futures=True)
        self.calibrator.shutdown(wait=True,cancel_futures=True)
        self.builder.shutdown(wait=True)
        try:
            self.save_counters()
            self.retire()
        finally:
            self.reclaimer.shutdown(wait=True)
            self.client.close()
