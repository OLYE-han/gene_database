"""Low-frequency replica controller; native routing uses immutable snapshots.

Static creation and routing are exposed separately for controlled experiments.
Dynamic mode adds sustained imbalance detection and bounded proportional steps.
Publication requires configured quality calibration; shadow agreement is not recall.
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
        benefit=cpu if heat is None or b.get('selection_policy')=='cpu_worksets' else sum(heat.get(gid,0.) for gid in ids)
        if benefit<=0:continue
        ranked.append((benefit / len(ids), entry, ids, list(queries.values())))
    ids, entries, selected = set(), [], []
    portals=b.get('portal_budget_fraction',0.)
    core_cap=max(1,int(b['max_core_rows']*(1-portals)))
    for _, entry, extra, queries in sorted(ranked, key=lambda r: (-r[0], r[1])):
        if len(ids | extra) > core_cap: continue
        ids.update(extra); entries.append(entry); selected.extend(queries)
    # Attach observed routing portals without requiring every training workset
    # to fit. They are never published without independent shadow validation.
    attached=0
    if portals and ids:
        order=sorted(groups.items(),key=lambda item:(-sum(v['cpu_ms'] for q in item[1].values() for v in q['branches'] if v['node']==source),item[0]))
        for entry,queries in order:
            if entry in entries:continue
            if entry not in ids and len(ids)>=b['max_core_rows']:continue
            ids.add(entry);entries.append(entry);selected.extend(queries.values());attached+=1
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
        self.cooldown_until, self.streak, self.last_pair = 0., 0, None
        self.last_control, self.last_adjust = 0., 0.
        self.stop = threading.Event()
        self.builder = ThreadPoolExecutor(max_workers=1, thread_name_prefix='replica-controller-build')
        self.events = []
        self.counters = defaultdict(int)
        self.region_counters = {}
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

    def observe(self):
        now = time.monotonic()
        rows = self.engine.runtime.replica_observations()
        with self.lock:
            self.history.extend(rows)
            counts = defaultdict(int)
            for q in rows:
                for r in self.regions.values():
                    if q['entry'] in r['entries'] and r['min_score'] <= q['score'] <= r['max_score']: r['last_use'] = now
                counts[q['entry']] += 1
            dt = max(now - getattr(self, 'last_observe', now), .001)
            for entry in set(self.rates) | set(counts):
                self.rates[entry] = .8 * self.rates[entry] + .2 * counts[entry] * self.b['sample_every'] / dt
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
            work = (v['compute_wall_ns'] - before['compute_wall_ns']) / 1e6
            mean = work / completed if completed else self.loads.get(sid, {}).get('service_ms', 1.)
            hist = [max(0, a - b) for a, b in zip(v['compute_wait_hist_us_log2'], before['compute_wait_hist_us_log2'])]
            q95 = queue_p95(hist)
            queue = v['compute_queue']
            # If no task starts, pending work still signals service pressure.
            backlog = queue['queued'] * mean / workers
            load = dict(util=max(0., cpu / dt / workers), queue_ms=max(q95 or 0., backlog),
                queue_p95_ms=q95, backlog_estimate_ms=backlog, service_ms=mean,
                workers=workers, queued=queue['queued'], active=queue['active'],
                completed=completed, cpu_seconds=cpu, window_seconds=dt,
                updated=ended, rpc_ms=(ended-started)*1000,
                usable=ended-started <= self.b['max_load_age_ms']/1000,
                free_bytes=r['free_bytes'])
            load['hotness']=r.get('hotness',{})
            with self.lock: self.loads[sid] = load
        with self.lock:
            sample = dict(monotonic_ns=time.monotonic_ns(), nodes=self.loads,
                          replica=None if self.active is None else dict(id=self.active['id'], fraction=self.active['fraction']),
                          replicas=[dict(id=r['id'],source=r['source'],target=r['target'],fraction=r['fraction'],entries=len(r['entries'])) for r in self.regions.values()])
            with (self.directory/'loads.jsonl').open('a') as f: f.write(json.dumps(sample)+'\n')

    def fresh(self, node, now):
        s = self.loads.get(node)
        return s is not None and s['usable'] and now - s['updated'] <= self.b['max_load_age_ms']/1000

    def propose(self, source, target):
        with self.lock:
            used={e for r in self.regions.values() if r['source']==source for e in r['entries']}
            observations = [q for q in self.history if q['entry'] not in used]
        scores=None
        if self.c.get('hotness',{}).get('enabled'):
            heat=self.rpc(source,'hotness')
            (self.directory/'latest_hotness_candidates.json').write_text(json.dumps(heat))
            if self.b.get('selection_policy','entry_worksets') in ('hotness_worksets','cpu_worksets'):
                scores={int(r['id']):r['lower_bound'] for r in heat.get('top',[])}
        return candidate(observations, source, target, self.b,scores)

    def create(self, plan):
        n = len(self.c['cluster']['nodes'])
        if not (0 <= plan['source'] < n and 0 <= plan['target'] < n and plan['source'] != plan['target']):
            raise ValueError('Invalid replica source/target')
        with self.lock:
            if len(self.regions)>=self.b['max_replicas'] or self.in_progress is not None:
                raise RuntimeError('Replica controller capacity/build in progress')
            used=sum(r.get('bytes',0) for r in self.regions.values())
            if used+self.b['max_region_bytes']>self.b.get('max_total_replica_bytes',self.b['max_region_bytes']*self.b['max_replicas']):
                raise ValueError('Total replica byte reservation exceeded')
            version = 'r_' + uuid.uuid4().hex
            self.in_progress = version
        future = self.builder.submit(self.build, version, plan)
        return future

    def build(self, version, plan):
        (self.directory/(version+'_plan.json')).write_text(json.dumps(plan, indent=2))
        self.log('ENTRY_STAGED', id=version, source=plan['source'], target=plan['target'], entries=len(plan['entries']), rows=len(plan['ids']))
        deadline = time.monotonic() + self.b['operation_timeout_s']
        try:
            self.rpc(plan['source'], 'source_start', id=version, plan=plan)
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
            quality=source['manifest'].get('quality',{})
            plan=dict(plan,entry_weights=quality.get('entry_weights',{}),peer_entries=quality.get('peer_entries',[]))
            self.log('STRUCTURE',id=version,bytes=source['manifest']['bytes'],rows=source['manifest']['rows'],quality=quality)
            def publish_validated(v):
                if not v.get('passed'):
                    with self.lock:removed=self.regions.pop(version,None)
                    if removed:
                        self.log('CANARY_PAUSED',id=version,reason=v.get('reason'));self.publish(time.monotonic())
                    return
                qualified=set(v['qualified_entries'])
                with self.lock:
                    old=self.regions.get(version)
                    self.regions[version]=dict(plan,id=version,bytes=source['manifest']['bytes'],
                        entries=sorted(qualified),entry_weights={k:w for k,w in plan.get('entry_weights',{}).items() if int(k) in qualified},
                        fraction=old['fraction'] if old else self.b['initial_fraction'],
                        published=old['published'] if old else time.monotonic(),last_use=time.monotonic(),validation=v,qualified=True)
                self.log('CANARY_UPDATED' if old else 'CANARY',id=version,validation=v,progressive=True,entries=len(qualified))
                self.publish(time.monotonic())
            progress=publish_validated if self.b.get('progressive_validation',False) else None
            validation = self.validate(plan, version, progress)
            (self.directory/(version+'_validation.json')).write_text(json.dumps(validation, indent=2))
            if not validation['passed']: raise ValueError('Replica quality/unloading gate failed: '+validation['reason'])
            if 'qualified_entries' in validation:
                qualified=set(validation['qualified_entries'])
                self.log('ENTRY_QUALITY_FILTER',id=version,before=len(plan['entries']),after=len(qualified))
                plan['entries']=sorted(qualified)
                plan['entry_weights']={k:v for k,v in plan['entry_weights'].items() if int(k) in qualified}
            now = time.monotonic()
            with self.lock:
                old=self.regions.get(version)
                self.regions[version] = dict(plan, id=version, bytes=source['manifest']['bytes'],fraction=old['fraction'] if old else self.b['initial_fraction'],
                    published=old['published'] if old else now, last_use=now, validation=validation, qualified=True)
                self.in_progress = None
            self.log('CANARY_UPDATED' if old else 'CANARY', id=version, validation=validation, fraction=self.regions[version]['fraction'])
            return version
        except BaseException as e:
            with self.lock:self.regions.pop(version,None)
            self.publish(time.monotonic())
            self.log('FAILED', id=version, error=str(e))
            try:
                self.rpc(plan['target'], 'retire', id=version)
                cleanup_until = time.monotonic()+self.b['operation_timeout_s']
                while not self.rpc(plan['target'], 'reap', id=version)['reclaimed']:
                    if time.monotonic() >= cleanup_until: raise TimeoutError('Replica cleanup still pinned')
                    time.sleep(.05)
            except BaseException as cleanup: self.log('CLEANUP_PENDING', id=version, error=str(cleanup))
            raise
        finally:
            with self.lock:
                self.in_progress = None
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
        entries=set(plan['entries']);training=set(plan.get('training_query_ids',[]))
        with self.lock:
            pool={q['query_id']:q for q in self.history if q['source']==plan['source'] and
                  q['entry'] in entries and 'query_f32' in q and q['replica'] is None and q['query_id'] not in training}
        probes=list(pool.values())[-self.b.get('max_validation_queries',32):]
        if len(probes)<self.b['min_validation_queries']:
            return dict(passed=False,reason='Too few unlabelled shadow probes',matched=len(probes))
        def evaluate(q):
            query=np.frombuffer(q['query_f32'],dtype='<f4').astype('float64');qid=int(q['query_id'])
            d=dict(entry_id=q['entry'],source=plan['source'],target=plan['source'],version='')
            cached=self.b.get('reuse_observed_baseline',False) and 'result_ids' in q
            base=None if cached else self.engine.runtime.validate_replica(query,qid,d)
            d.update(target=plan['target'],version=version)
            rep=self.engine.runtime.validate_replica(query,qid,d)
            if (base is not None and base['status']!='ok') or rep['status']!='ok':raise ValueError('Shadow query failed')
            def cpu(r,node):return sum(b['stats']['server_thread_cpu_ms']/1000 for b in r['branch_reports'] if b['node_id']==node)
            root=next(b for b in rep['branch_reports'] if b['parent_branch_id'] is None)
            original_cpu=lambda node:sum(b['cpu_ms']/1000 for b in q['branches'] if b['node']==node) if cached else cpu(base,node)
            return dict(id=qid,entry=q['entry'],baseline_cached=cached,agreement=len(set(q['result_ids'] if cached else base['ids']) & set(rep['ids']))/self.c['search']['top_k'],
                source_saved_s=original_cpu(plan['source'])-cpu(rep,plan['source']),
                target_added_s=cpu(rep,plan['target'])-original_cpu(plan['target']),
                local_expansions=root['work_spent'],early_exit=bool(root['children']) and root['work_spent']<self.b.get('min_replica_expansions',32))
        samples=[]
        with ThreadPoolExecutor(max_workers=self.b['validation_workers']) as pool:
            futures=[pool.submit(evaluate,q) for q in probes]
            for future in as_completed(futures):
                samples.append(future.result())
                if progress and len(samples)%self.b.get('validation_publish_batch',32)==0:
                    progress(self.shadow_summary(samples))
        return self.shadow_summary(samples)

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
            self.active['fraction'] = fraction
            self.log('FRACTION', id=self.active['id'], fraction=fraction)

    def publish(self, now):
        with self.lock:
            rows=[];peer=[];claimed=set()
            for r in self.regions.values():
                if not self.fresh(r['source'],now) or not self.fresh(r['target'],now):continue
                row=dict(source=r['source'],target=r['target'],version=r['id'],entries=r['entries'],fraction=r['fraction'],
                         min_score=r['min_score'],max_score=r['max_score'],ttl_ms=self.b['max_load_age_ms'])
                if self.b.get('quality_weight_routing',True) and r.get('entry_weights'):row['entry_weights']=r['entry_weights']
                rows.append(row)
                # Deterministic first ownership avoids ambiguous peer candidates.
                weighted=[x for x in r.get('peer_entries',[]) if x[0] not in claimed]
                claimed.update(x[0] for x in weighted)
                if weighted:peer.append(dict(source=r['source'],target=r['target'],version=r['id'],fraction=r['fraction'],
                    ttl_ms=self.b['max_load_age_ms'],weighted_entries=weighted))
        self.engine.runtime.replica_routes(rows)
        if self.b.get('peer_routes_enabled',False):
            for n in self.c['cluster']['nodes']:self.rpc(n['id'],'routes',routes=peer)

    def adjust(self, now):
        retire=[];reserved=defaultdict(float)
        with self.lock:
            for r in list(self.regions.values()):
                if not self.fresh(r['source'],now) or not self.fresh(r['target'],now):continue
                a,b=self.loads[r['source']],self.loads[r['target']]
                rate=sum(self.rates[e] for e in r['entries']);old=p=r['fraction']
                target_queue=b.get('backlog_estimate_ms',b['queue_ms'])
                if b['util']>self.b['target_util_limit'] or target_queue>self.b.get('target_queue_limit_ms',self.b['queue_high_ms']):p*=.5
                elif rate>1 and a['util']>b['util']:
                    d,c=r['validation']['source_saved_s'],r['validation']['target_added_s']
                    if d>0 and c>0:
                        equilibrium=p+(a['util']-b['util'])/(rate*(d/a['workers']+c/b['workers']))
                        spare=max(0.,(self.b['target_util_limit']-b['util'])*b['workers']-reserved[r['target']])
                        cap=p+spare/(rate*c)
                        p=max(0.,min(self.b['max_fraction'],p+self.b['fraction_step'],equilibrium,cap))
                        reserved[r['target']]+=max(0.,p-old)*rate*c
                elif a['util']<b['util']:p=max(0.,p-self.b['fraction_step'])
                r['fraction']=p
                if abs(p-old)>1e-9:self.log('ADJUST',id=r['id'],old=old,fraction=p,eligible_rate=rate,
                    estimated_source_relief_cores=rate*p*r['validation']['source_saved_s'])
                if now-r['last_use']>self.b['idle_retire_ms']/1000 and now-r['published']>self.b['min_residency_ms']/1000:retire.append(r['id'])
        for version in retire:
            if len(self.regions)==1:self.retire()
            else:self.retire(version)

    def retire(self, version=None):
        with self.lock:
            removed=list(self.regions.values()) if version is None else [self.regions[version]]
            for r in removed:self.regions.pop(r['id'])
        self.publish(time.monotonic())
        for r in removed:
            self.rpc(r['target'],'retire',id=r['id']);self.log('DRAINING',id=r['id'])
            deadline=time.monotonic()+self.b['operation_timeout_s']
            while time.monotonic()<deadline:
                if self.rpc(r['target'],'reap',id=r['id'])['reclaimed']:
                    self.log('RETIRED',id=r['id']);break
                time.sleep(.05)
            else:raise TimeoutError('Replica still pinned after drain deadline')

    def run(self):
        while not self.stop.is_set():
            try:
                self.observe(); now = time.monotonic()
                self.publish(now)
                if self.b['mode'] == 'dynamic':
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

    def control(self, now):
        with self.lock:
            possible=[(a,b) for a in self.loads for b in self.loads if a!=b and self.fresh(a,now) and self.fresh(b,now)
                and self.loads[a]['util']>=self.b['source_util_high'] and self.loads[a]['queue_ms']>=self.b['queue_high_ms']
                and self.loads[b]['util']<=self.b['target_util_low'] and self.loads[b].get('backlog_estimate_ms',self.loads[b]['queue_ms'])<=self.b['queue_low_ms']
                and self.loads[a]['util']-self.loads[b]['util']>=self.b['util_gap']]
            pair=max(possible,key=lambda p:self.loads[p[0]]['util']-self.loads[p[1]]['util']) if possible else None
            self.streak=self.streak+1 if pair and pair==self.last_pair else int(pair is not None);self.last_pair=pair
            trigger=pair and self.streak>=self.b['sustained_windows'] and len(self.regions)<self.b['max_replicas'] and self.in_progress is None and now>=self.cooldown_until
        if trigger:
            self.log('IMBALANCED',source=pair[0],target=pair[1],loads=self.loads,windows=self.streak)
            try:self.create(self.propose(*pair))
            except ValueError as e:self.log('NO_CANDIDATE',reason=str(e));self.cooldown_until=now+self.b['cooldown_ms']/1000

    def close(self):
        self.stop.set();self.thread.join()
        self.builder.shutdown(wait=True)
        try:
            self.save_counters()
            self.retire()
        finally: self.client.close()
