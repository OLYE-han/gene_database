"""Demand, rebound and drain invariants; simulated policy time is explicit."""
from pathlib import Path
import copy
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
import yaml
from dhdgs.replica_policy import AdaptiveDecay, family_key
from dhdgs.load_balance import LoadBalancer

ROOT = Path(__file__).resolve().parents[1]


def settings():
    c = yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())['load_balance']['adaptive_decay']
    c.update(enabled=True, fast_half_life_ms=50., slow_half_life_ms=200., peak_half_life_ms=5000.,
             max_sample_gap_ms=300., base_hold_ms=300., hold_min_ms=100., hold_max_ms=2000.,
             min_residency_ms=200., history_ttl_ms=20000.)
    return c


def plan(target=1):
    return dict(source=0, target=target, ids=[1,2,3], entries=[1,2], min_score=-10., max_score=10., epoch='test')


class DecayTests(unittest.TestCase):
    def setUp(self):
        self.p = AdaptiveDecay(settings())
        self.key = self.p.register(plan(), 0., .02)
        self.now = 0.
        self.p.tick(0., {}, healthy=True, sample_every=1)

    def run_window(self, count, steps=1, healthy=True):
        actions = []
        for _ in range(steps):
            self.now += .1
            actions += self.p.tick(self.now, {self.key:count}, healthy=healthy, sample_every=1)
        return actions

    def test_hot_and_short_dip_keep_copy_long_cold_retires(self):
        self.assertFalse(any(x['kind']=='retire' for x in self.run_window(100, 20)))
        self.assertFalse(any(x['kind']=='retire' for x in self.run_window(0, 2)))
        self.run_window(100, 10)
        actions = self.run_window(0, 40)
        self.assertTrue(any(x['kind']=='retire' for x in actions))
        self.assertGreaterEqual(self.p.states[self.key].cold_run, .3)

    def test_observation_loss_and_long_gap_are_not_zero_demand(self):
        self.run_window(100, 10)
        clock = self.p.clock
        self.assertEqual(self.run_window(0, 100, healthy=False), [])
        self.assertAlmostEqual(self.p.clock, clock)
        self.now += 100
        self.assertEqual(self.p.tick(self.now, {}, healthy=True, sample_every=1), [])
        self.assertAlmostEqual(self.p.clock, clock)
        self.assertEqual(self.p.states[self.key].state, 'HOT')

    def test_demand_is_original_source_entry_not_copy_hits(self):
        rows = [dict(source=0,entry=1,score=0,replica=None),
                dict(source=0,entry=1,score=0,replica='v1'),
                dict(source=2,entry=1,score=0,replica='v2'),
                dict(source=0,entry=1,score=100,replica='v1')]
        self.assertEqual(self.p.counts(rows)[self.key], 2)
        other = dict(plan(2), id='other')
        self.assertEqual(self.p.register(other, 1., .03), self.key)
        self.assertEqual(self.p.counts(rows)[self.key], 2)

    def test_rebound_and_recopy_cost_increase_bounded_hold(self):
        self.run_window(100, 20)
        self.run_window(0, 8)
        before = self.p.states[self.key].hold_ms
        self.run_window(100, 3)
        h = self.p.states[self.key]
        self.assertGreater(h.rebounds, 0)
        self.assertGreater(h.hold_ms, before)
        self.p.withdraw(self.key, self.now)
        self.now += 1
        self.p.tick(self.now, {}, healthy=False, sample_every=1)
        self.p.register(plan(), self.now, 20.)
        self.run_window(0, 1)
        self.assertEqual(h.hold_ms, settings()['hold_max_ms'])

    def test_rebuilt_different_contents_share_only_entry_history(self):
        self.run_window(100, 10)
        self.run_window(0, 8)
        self.p.withdraw(self.key, self.now)
        changed = dict(plan(), ids=[1,2,3,4], min_score=-9.)
        key = self.p.register(changed, self.now, .03)
        self.assertNotEqual(key, self.key)
        self.assertGreater(self.p.states[key].rebounds, 0)
        self.assertEqual(self.p.states[key].fast, 0)
        different_epoch = dict(changed, epoch='next')
        self.p.withdraw(key, self.now)
        k = self.p.register(different_epoch, self.now, .03)
        self.assertEqual(self.p.states[k].rebounds, 0)

    def test_inactive_history_is_bounded_and_has_no_graph_arrays(self):
        self.p.c['history_limit'] = 3
        self.p.withdraw(self.key, 0.)
        for i in range(8):
            key = self.p.register(dict(plan(), entries=[100+i]), i+1., .1)
            self.p.withdraw(key, i+1.)
        self.assertLessEqual(len(self.p.states), 3)
        self.assertTrue(all(not hasattr(v, 'ids') for v in self.p.states.values()))
        self.p.prune(100.)
        self.assertFalse(self.p.states)


class DrainTests(unittest.TestCase):
    def controller(self):
        x = object.__new__(LoadBalancer)
        x.b = dict(max_replicas=1,max_region_bytes=10,max_total_replica_bytes=10,
                   operation_timeout_s=.5,lifecycle_poll_ms=5,adaptive_decay=dict(reclaim_retry_ms=5))
        x.c = dict(cluster=dict(nodes=[{},{}]))
        x.regions = {'r':dict(id='r',source=0,target=1,bytes=10,entries=[1])}
        x.draining, x.reclaim_futures, x.reclaim_retry = {}, {}, {}
        x.lock = threading.RLock()
        x.in_progress = None
        x.decay = None
        x.reclaimer = ThreadPoolExecutor(1)
        self.addCleanup(x.reclaimer.shutdown, wait=True)
        x.events = []
        x.log = lambda event, **fields:x.events.append((event,fields))
        x.publish = lambda now:x.events.append(('DIRECTORY',list(x.regions)))
        return x

    def test_async_drain_keeps_capacity_and_does_not_block_observer(self):
        x = self.controller();released = threading.Event();retired = threading.Event()
        def rpc(node, op, **kw):
            if op=='retire':retired.set();return {}
            return dict(reclaimed=released.is_set())
        x.rpc = rpc
        begin = time.monotonic();x.retire('r', wait=False)
        self.assertLess(time.monotonic()-begin,.1)
        self.assertFalse(x.regions)
        self.assertIn('r',x.draining)
        self.assertTrue(retired.wait(.2))
        with self.assertRaisesRegex(RuntimeError,'capacity'):
            x.create(plan())
        self.assertEqual(x.events[0],('DIRECTORY',[]))
        released.set()
        x.retire(wait=True)
        self.assertFalse(x.draining)
        self.assertTrue(any(e=='RETIRED'for e,_ in x.events))

    def test_reclaim_rpc_failure_does_not_free_reserved_bytes(self):
        x=self.controller()
        def rpc(*a, **k):raise ConnectionError('offline')
        x.rpc=rpc
        with self.assertRaises(ConnectionError):x.retire('r')
        self.assertEqual(x.draining['r']['bytes'],10)
        x.rpc=lambda node,op,**kw:dict(reclaimed=True)
        x.retire(wait=True)
        self.assertFalse(x.draining)


class ExpansionPolicyTests(unittest.TestCase):
    def setUp(self):
        from dhdgs.replica_policy import expansion_decision
        self.decide = expansion_decision
        self.c = yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())['load_balance']['adaptive_expansion']
        self.r = dict(id='v1',source=0,target=1,fraction=.2,published=100.,qualified=True,
                      copy_ready_seconds=2.,validation=dict(count=100,source_saved_s=.001,target_added_s=.001))
        self.loads = {0:dict(util=.99,workers=4,queue_ms=10),1:dict(util=.8,workers=4,queue_ms=0),
                      2:dict(util=.1,workers=4,queue_ms=0)}

    def test_uses_existing_capacity_before_building(self):
        self.loads[1]['util']=.1
        d=self.decide([self.r],self.loads,1000,self.c,.8,101.)
        self.assertEqual(d['action'],'increase')
        self.assertLessEqual(self.r['fraction']+d['fraction'],.8)

    def test_pressure_can_add_second_destination_without_copying_query(self):
        d=self.decide([self.r],self.loads,1000,self.c,.8,101.)
        self.assertEqual((d['action'],d['target']),('copy',2))
        self.assertGreater(d['estimated_source_relief_cores'],0)
        self.assertLessEqual(d['fraction'],.8-self.r['fraction'])
        self.assertIn('sibling',d['target_cost_kind'])

    def test_unmeasured_stale_negative_cost_and_slow_copy_are_blocked(self):
        for override in [dict(qualified=False),dict(published=0),dict(copy_ready_seconds=100),
                         dict(validation=dict(count=100,source_saved_s=None,target_added_s=None)),
                         dict(validation=dict(count=100,source_saved_s=-.001,target_added_s=.001))]:
            self.assertEqual(self.decide([dict(self.r,**override)],self.loads,1000,self.c,.8,101.)['action'],'hold')


class MultiReplicaRuntimeTests(unittest.TestCase):
    """Real local three-node transfer/search; policy loads are synthetic inputs."""
    @classmethod
    def setUpClass(cls):
        from test_replicas import ReplicaTests
        ReplicaTests.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        from test_replicas import ReplicaTests
        ReplicaTests.setUp(self)

    def configure_replica_test(self,c):
        c['peer'].update(region_tasks=True,concurrent_region_admission=True,logical_region_visits=True,
                         allow_node_revisit=True,deduplicate_node_expansions=True,max_excluded_region_ids=1000)
        c['load_balance'].update(publication_mode='ready',compute_region_quality=False,
                                quality_weight_routing=False,peer_routes_enabled=False,warm_bytes=0,
                                native_replica_pack=True,max_replicas=2,initial_fraction=.4,max_fraction=.8,
                                max_load_age_ms=10000)
        c['load_balance']['adaptive_expansion']['enabled']=True

    def info(self,i=0):
        return self.engine.runtime.route_info(self.x[i].astype(float),i)

    def rpc(self,node,op,**kw):
        from test_replicas import ReplicaTests
        return ReplicaTests.rpc(self,node,op,**kw)

    def test_two_destinations_single_root_fraction_expiry_and_drain(self):
        import numpy as np
        a=self.info()['source'];targets=[i for i in range(3)if i!=a]
        ids=np.load(Path(self.c['paths']['output'])/'index'/f'shard_{a}'/'ids.npy').tolist()
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'multi')
        controller.stop.set();controller.thread.join();controller.stop.clear()
        controller.loads={n:dict(updated=time.monotonic(),usable=True)for n in range(3)}
        p=dict(source=a,ids=ids,entries=ids,training_query_ids=[],min_score=-1e300,max_score=1e300)
        versions=[controller.create(dict(p,target=t)).result(20)for t in targets]
        controller.publish(time.monotonic())
        family=controller.regions[versions[0]]['family_id']
        self.assertEqual(family,controller.regions[versions[1]]['family_id'])
        def draw(rid):
            h=1469598103934665603
            for ch in rid.encode():h=((h^ch)*1099511628211)&((1<<64)-1)
            return (h>>11)/9007199254740992
        counts={v:0 for v in versions};counts[None]=0
        truth=set(map(int,np.argsort(np.sum((self.x.astype(float)-self.x[0])**2,axis=1))[1:6]))
        for _ in range(150):
            q=self.engine.search_async(self.x[0],0).result(10)
            self.assertEqual(q['status'],'ok')
            d=draw(q['request_id']);expected=versions[0]if d<.4 else versions[1]if d<.8 else None
            self.assertEqual(q['stats'].get('replica_version'),expected)
            counts[expected]+=1
            self.assertEqual(sum(b['parent_branch_id']is None for b in q['branch_reports']),1)
            self.assertEqual(set(q['ids']),truth)
            self.assertLessEqual(q['stats']['expanded'],256)
        self.assertTrue(all(v>0 for v in counts.values()),counts)
        rows=[dict(source=a,target=t,version=v,family=family,entries=ids,fraction=.4,
                   min_score=-1e300,max_score=1e300,ttl_ms=1 if i==0 else 10000)
              for i,(t,v)in enumerate(zip(targets,versions))]
        self.engine.runtime.replica_routes(rows);time.sleep(.02)
        for _ in range(40):
            q=self.engine.search_async(self.x[0],0).result(10);d=draw(q['request_id'])
            self.assertEqual(q['stats'].get('replica_version'),versions[1]if .4<=d<.8 else None)
        invalid=copy.deepcopy(rows);invalid[1]['fraction']=.7
        with self.assertRaisesRegex(Exception,'fractions exceed'):self.engine.runtime.replica_routes(invalid)
        invalid=copy.deepcopy(rows);invalid[1]['family']='different'
        with self.assertRaisesRegex(Exception,'Ambiguous'):self.engine.runtime.replica_routes(invalid)
        controller.retire()
        self.assertFalse(controller.draining)
        for t,v in zip(targets,versions):
            self.assertEqual(self.rpc(t,'status',id=v)['phase'],'RETIRED')
            self.assertFalse((Path(self.work.name)/str(t)/'replicas'/v).exists())

    def test_controller_pressure_build_scale_down_and_cold_reclaim(self):
        """Real copies/search/release; injected loads and calibration are not performance evidence."""
        import numpy as np
        a=self.info()['source'];targets=[i for i in range(3)if i!=a]
        ids=np.load(Path(self.c['paths']['output'])/'index'/f'shard_{a}'/'ids.npy').tolist()
        ctl=self.engine.start_load_balancer(self.token,Path(self.work.name)/'transitions')
        ctl.stop.set();ctl.thread.join();ctl.stop.clear()
        ctl.b['adaptive_expansion'].update(sustained_windows=2,scale_down_windows=2,cooldown_ms=1.,
            min_validation_queries=2,max_copies_per_family=2)
        ctl.b['min_residency_ms']=1
        ctl.decay=AdaptiveDecay(settings())
        now=time.monotonic()
        ctl.loads={n:dict(updated=now,usable=True,util=.1,workers=4,queue_ms=0.,free_bytes=2**40)for n in range(3)}
        p=dict(source=a,target=targets[0],ids=ids,entries=ids,training_query_ids=[],min_score=-1e300,max_score=1e300)
        v=ctl.create(p).result(20);family=ctl.regions[v]['family_id']
        # Costs are synthetic, explicitly controlled to exercise the real transition.
        evidence=dict(passed=True,count=100,source_saved_s=.001,target_added_s=.001,qualified_entries=ids)
        ctl.regions[v].update(qualified=True,validation=evidence)
        ctl.validate=lambda plan,version,progress=None:dict(evidence)
        ctl.family_rates[family]=1000.
        ctl.loads[a].update(util=.99,queue_ms=10.)
        ctl.loads[targets[0]]['util']=.8
        ctl.control_expansion(time.monotonic());self.assertEqual(len(ctl.regions),1)
        ctl.control_expansion(time.monotonic())
        ctl.builder.submit(lambda:None).result(20)
        self.assertEqual(len(ctl.regions),2,ctl.events)
        self.assertEqual({r['target']for r in ctl.regions.values()},set(targets))
        self.assertTrue(all(r['family_id']==family for r in ctl.regions.values()))
        self.assertLessEqual(sum(r['fraction']for r in ctl.regions.values()),ctl.b['max_fraction'])
        ctl.publish(time.monotonic())
        qs=[self.engine.search_async(self.x[0],0).result(10)for _ in range(30)]
        self.assertTrue(all(q['status']=='ok'for q in qs))
        self.assertTrue(all(sum(b['parent_branch_id']is None for b in q['branch_reports'])==1 for q in qs))
        self.assertTrue(all(q['stats']['expanded']<=256 for q in qs))
        for n in ctl.loads.values():n.update(util=.1,queue_ms=0.,updated=time.monotonic())
        ctl.expansion_cooldown.clear()
        ctl.control_expansion(time.monotonic());ctl.control_expansion(time.monotonic())
        for future in list(ctl.reclaim_futures.values()):future.result(10)
        self.assertEqual(len(ctl.regions),1,ctl.events)
        self.assertFalse(ctl.draining)
        # Observe a healthy, genuinely empty node set on an accelerated policy clock.
        now=time.monotonic();ctl.decay.tick(now,{},healthy=True,sample_every=1)
        for _ in range(60):
            now+=.1
            for n in ctl.loads.values():n['updated']=now
            ctl.observe_decay([],now)
            if not ctl.regions:break
        self.assertFalse(ctl.regions,ctl.decay.snapshot())
        ctl.retire(wait=True);self.assertFalse(ctl.draining)
        self.assertFalse(ctl.decay.states[family].active)
        self.assertTrue(any(e['event']=='FAMILY_COPY_PROPOSED'for e in ctl.events))
        self.assertTrue(any(e['event']=='FAMILY_SCALE_DOWN'for e in ctl.events))
        self.assertTrue(any(e['event']=='DECAY_RETIRE'for e in ctl.events))
        q=self.engine.search_async(self.x[0],0).result(10)
        self.assertEqual(q['status'],'ok');self.assertNotIn('replica_version',q['stats'])
        for t in targets:
            root=self.servers[t].control.replica_service.root
            self.assertFalse([p for p in root.glob('r_*')if p.is_dir()])

    def test_terminal_metadata_trim_preserves_retry_then_expires_safely(self):
        from unittest.mock import patch
        import numpy as np
        info=self.info();a=info['source'];b=(a+1)%3
        ids=np.load(Path(self.c['paths']['output'])/'index'/f'shard_{a}'/'ids.npy').tolist()
        p=dict(ids=ids,entries=[info['entry_id']],target=b)
        self.rpc(a,'source_start',id='trimmed',plan=p)
        deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            if self.rpc(a,'status',id='trimmed')['phase']=='SENT' and self.rpc(b,'status',id='trimmed')['phase']=='READY':break
            time.sleep(.01)
        else:self.fail('copy timeout')
        src=self.servers[a].control.replica_service;dst=self.servers[b].control.replica_service
        src.trim_terminal();self.assertNotIn('plan',src.states['trimmed'])
        self.assertEqual(self.rpc(a,'source_start',id='trimmed',plan=p)['phase'],'SENT')
        with self.assertRaisesRegex(RuntimeError,'Conflicting'):self.rpc(a,'source_start',id='trimmed',plan=dict(p,ids=[]))
        self.rpc(b,'retire',id='trimmed');self.assertTrue(self.rpc(b,'reap',id='trimmed')['reclaimed'])
        with patch('dhdgs.replicas.time.monotonic_ns',return_value=time.monotonic_ns()+int(30e9)):
            src.trim_terminal();dst.trim_terminal()
        self.assertNotIn('trimmed',src.states);self.assertNotIn('trimmed',dst.states)
        self.assertTrue(self.rpc(b,'reap',id='trimmed')['reclaimed'])
        self.assertTrue(self.rpc(b,'retire',id='trimmed')['tombstone_expired'])

    def test_native_search_pin_blocks_reap_until_branch_finishes(self):
        """Hold a real native search at its approved boundary callback during retirement."""
        import numpy as np
        from test_replicas import ReplicaTests
        from dhdgs.native_search import run_native
        info,a,b=ReplicaTests.copy_source(self,'pinned')
        worker=self.servers[b].control.worker
        members=set(np.load(Path(self.c['paths']['output'])/'index'/f'shard_{a}'/'ids.npy').tolist())
        remote=next(int(i)for i in range(len(self.x))if i not in members)
        owner=next(n for n in range(3)if n!=b and remote in set(np.load(Path(self.c['paths']['output'])/'index'/f'shard_{n}'/'ids.npy').tolist()))
        seed=int(info['entry_id']);cfg=copy.deepcopy(self.c)
        cfg['peer'].update(region_tasks=False,concurrent_region_admission=False,logical_region_visits=False,
                           allow_node_revisit=False,min_expansions_after_cross=0)
        cfg['scheduler'].update(mode='reference',remote_weight=100.,max_wait_ms=.000001)
        state=dict(query_request_id='pin',branch_id='pin',parent_branch_id=None,target_node=b,entry_node=b,
                   visited_nodes=[b],epoch=worker.meta['epoch'],query_f32=self.x[0].tobytes(),query_id=0,
                   work_grant=64,branch_slots=2,seeds=[[seed,1e6,0.,[a],None,0,0,seed,0.]],
                   seen=[seed,remote],remote_frontier=[[remote,0.,0.,[owner],None,0,0,seed,0.]],
                   fallback_entries=[],scale=1.,decision_seed=42,replica_version='pinned',original_entry_node=a)
        entered=threading.Event();released=threading.Event()
        def spawn(*args):entered.set();self.assertTrue(released.wait(5))
        with ThreadPoolExecutor(1)as pool:
            f=pool.submit(run_native,state,worker,None,cfg,spawn,lambda:False)
            try:
                self.assertTrue(entered.wait(3),str(f.exception())if f.done()else'no boundary')
                self.rpc(b,'retire',id='pinned')
                self.assertFalse(self.rpc(b,'reap',id='pinned')['reclaimed'])
                root=self.servers[b].control.replica_service.root/'pinned'
                self.assertTrue(root.is_dir())
            finally:released.set()
            f.result(5)
        self.assertTrue(self.rpc(b,'reap',id='pinned')['reclaimed'])
        self.assertFalse(root.exists())
