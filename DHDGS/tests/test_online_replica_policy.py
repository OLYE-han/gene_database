import copy
from concurrent.futures import Future
from pathlib import Path
import threading
import time
import unittest
from types import SimpleNamespace

import yaml

from dhdgs.automatic_replica import prepare, remaining_capacity
from dhdgs.replica_budget import available, reservations, check_layout
from dhdgs.online_replica_policy import OnlinePolicy, held_out
from dhdgs.replica_policy import expansion_decision
from dhdgs.load_balance import LoadBalancer
from test_automatic_replica import BudgetAndControlTests

ROOT=Path(__file__).resolve().parents[1]
DEFAULT=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())['load_balance']


class IndependentCapacityTests(unittest.TestCase):
    def controller(self):
        c=BudgetAndControlTests().controller()
        c.b.update(capacity_policy=dict(mode='per_region',small_alpha=.3,large_alpha=.8,
                    max_source_total_bytes=0,max_target_replica_bytes=9000000),max_total_replica_bytes=18000000)
        c.rpc=lambda node,op,**kw:dict(source_index_bytes=6000000,free_bytes=10000000)
        return c

    def test_first_small_region_does_not_exhaust_other_regions_alpha(self):
        c=self.controller();c.b['automatic_pipeline']['max_targets']=1
        c.regions={'a':dict(id='a',source=1,target=2,bytes=1800000)}
        self.assertEqual(remaining_capacity(c,1,6000000),1800000)
        _,layout,_=prepare(c,1,0,BudgetAndControlTests().observations())
        self.assertEqual(layout['capacity_bytes'],1800000)
        self.assertAlmostEqual(layout['total_alpha'],.3)

    def test_two_targets_each_have_small_alpha_not_a_shared_half(self):
        c=self.controller()
        _,layout,_=prepare(c,1,2,BudgetAndControlTests().observations())
        self.assertEqual(layout['capacity_bytes'],3600000)
        self.assertAlmostEqual(layout['total_alpha'],.6)

    def test_adopted_pending_claim_charged_once_at_larger_size(self):
        c=self.controller()
        c.regions={'a':dict(id='a',source=1,target=2,bytes=1000000)}
        c.partition_pending={'a':dict(id='a',source=1,target=2,bytes=1800000)}
        c.draining={'b':dict(id='b',source=1,target=0,bytes=1200000)}
        c.build_reservations={'c':dict(id='c',source=1,target=2,bytes=1800000)}
        self.assertEqual(len(reservations(c)),3)
        self.assertEqual(available(c,1,6000000),13200000)
        self.assertEqual(available(c,1,6000000,2),5400000)

    def test_independent_source_bytes_and_target_bytes_are_enforced(self):
        c=self.controller();c.b['capacity_policy']['max_source_total_bytes']=2400000
        c.regions={'a':dict(source=1,target=2,bytes=1800000)}
        self.assertEqual(remaining_capacity(c,1,6000000),600000)
        c.b['capacity_policy']['max_source_total_bytes']=0
        c.b['capacity_policy']['max_target_replica_bytes']=2000000
        self.assertEqual(available(c,1,6000000,2),200000)

    def test_small_and_large_construction_limits_are_distinct(self):
        c=self.controller()
        check_layout(c.b,dict(total_alpha=.6,target_weights={'0':1,'2':1}))
        with self.assertRaisesRegex(ValueError,'alpha'):
            check_layout(c.b,dict(total_alpha=.6,target_weights={'0':1}))
        check_layout(c.b,dict(total_alpha=.8,target_weights={'0':1},replica_tier='large'))
        with self.assertRaisesRegex(ValueError,'alpha'):
            check_layout(c.b,dict(total_alpha=.9,target_weights={'0':1,'2':1},replica_tier='large'))


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.cfg=dict(DEFAULT['online_policy'],large_sustained_seconds=10.,min_episode_seconds=2.,
                      episode_cool_seconds=2.,recurrence_count=3,max_observation_gap_seconds=2.,recurrence_window_seconds=60.)
        self.p=OnlinePolicy(self.cfg);self.now=0.
        self.loads={0:dict(epoch='v1',worker_pressure_ms=10.)}
        self.p.tick(0.,self.loads,{0:[1,2,3]},[0])

    def tick(self, hot=True, healthy=True, entries=(1,2,3)):
        self.now+=1
        self.p.tick(self.now,self.loads,{0:list(entries)},[0] if hot else [],healthy)

    def test_short_spike_not_large_sustained_elapsed_time_is(self):
        for _ in range(3):self.tick()
        self.assertIsNotNone(self.p.large_reason(0,self.now))
        for _ in range(7):self.tick()
        self.assertIsNone(self.p.large_reason(0,self.now))

    def test_missing_observation_freezes_clock_and_epoch_resets(self):
        for _ in range(5):self.tick()
        for _ in range(20):self.tick(healthy=False)
        self.assertEqual(self.p.states[0].run,5)
        self.assertEqual(self.p.states[0].unknown,20)
        self.loads[0]['epoch']='v2';self.tick()
        self.assertEqual(self.p.states[0].run,1)

    def test_recurrence_requires_separate_cold_episodes_same_hotspot(self):
        for _ in range(2):
            for i in range(3):self.tick()
            for i in range(2):self.tick(hot=False)
        for _ in range(2):self.tick()
        self.assertIsNone(self.p.large_reason(0,self.now))
        self.tick(entries=(9,10,11))
        self.assertIsNotNone(self.p.large_reason(0,self.now))
        self.assertEqual(len(self.p.states[0].episodes),0)

    def test_holdout_is_deterministic_and_disjoint(self):
        held={i for i in range(1000) if held_out(i,5)}
        self.assertTrue(150<len(held)<250)
        self.assertEqual(held,{i for i in range(1000) if held_out(i,5)})
        self.assertFalse(held & {i for i in range(1000) if not held_out(i,5)})


class FractionTransferTests(unittest.TestCase):
    def setUp(self):
        self.cfg=DEFAULT['adaptive_expansion']
        self.r=dict(id='a',source=0,target=1,fraction=.9,published=0.,validated_at=100.,qualified=True,
                    copy_ready_seconds=2.,validation=dict(count=100,source_saved_s=.001,target_added_s=.001))
        self.loads={0:dict(util=.9,workers=64,worker_pressure_ms=10),
                    1:dict(util=.8,workers=64,worker_pressure_ms=30),
                    2:dict(util=.1,workers=64,worker_pressure_ms=.1)}

    def test_full_family_can_transfer_to_new_copy_using_fresh_calibration(self):
        d=expansion_decision([self.r],self.loads,1000,self.cfg,.9,101.)
        self.assertEqual(d['action'],'copy')
        self.assertAlmostEqual(sum(d['donors'].values()),d['fraction'])
        self.assertEqual(d['estimated_source_relief_cores'],0)
        self.assertAlmostEqual(.9-sum(d['donors'].values())+d['fraction'],.9)

    def test_existing_spare_copy_receives_share_before_building(self):
        b=dict(self.r,id='b',target=2,fraction=.1)
        a=dict(self.r,fraction=.8)
        d=expansion_decision([a,b],self.loads,1000,self.cfg,.9,101.)
        self.assertEqual((d['action'],d['version']),('redistribute','b'))
        self.assertEqual(set(d['donors']),{'a'})

    def test_stale_cost_does_not_gain_freshness_from_recent_publication(self):
        r=dict(self.r,published=100.,validated_at=0.)
        self.assertEqual(expansion_decision([r],self.loads,1000,self.cfg,.9,101.)['reason'],'stale_cost_evidence')


class PortfolioActivationTests(unittest.TestCase):
    def controller(self):
        c=LoadBalancer.__new__(LoadBalancer);c.lock=threading.RLock()
        c.b={}
        c.regions={k:dict(id=k,source=0,target=1,entries=entries,qualified=True,
                         _pending_activation=stage,bytes=300,epoch='v1',fraction=.9)
                   for k,entries,stage in [('old',[1,2],False),('new',[1,3],True)]}
        c.draining={};c.loads={0:dict(epoch='v1'),1:{}}
        c.fresh=lambda n,now:True;c.decay=None;c.log=lambda *a,**kw:None;c.poll_reclaims=lambda now:None
        c.publish=lambda now:None
        return c

    def test_switch_preserves_old_entries_not_qualified_on_new_graph(self):
        c=self.controller();c.activate_portfolio(['new'],['old'])
        self.assertEqual(c.regions['old']['entries'],[2])
        self.assertFalse(c.regions['new']['_pending_activation'])

    def test_failed_directory_switch_restores_old_routes_and_staged_status(self):
        c=self.controller();calls=[]
        def fail_once(now):
            calls.append(1)
            if len(calls)==1:raise RuntimeError('injected directory failure')
        c.publish=fail_once
        with self.assertRaisesRegex(RuntimeError,'directory'):c.activate_portfolio(['new'],['old'])
        self.assertEqual(c.regions['old']['entries'],[1,2])
        self.assertTrue(c.regions['new']['_pending_activation'])
        self.assertEqual(c.draining,{})

    def test_uncertified_or_stale_portfolio_cannot_replace(self):
        c=self.controller();c.regions['new']['qualified']=False
        with self.assertRaises(ValueError):c.activate_portfolio(['new'],['old'])
        self.assertEqual(c.regions['old']['entries'],[1,2])


class CalibrationTests(unittest.TestCase):
    def test_refresh_records_cost_time_without_resetting_residency(self):
        c=LoadBalancer.__new__(LoadBalancer);c.lock=threading.RLock()
        c.b=dict(online_policy=dict(enabled=True,calibration_interval_seconds=30),min_validation_queries=2)
        c.regions={'r':dict(id='r',source=0,target=1,entries=[1],published=5.,family_id='f',fraction=.5)}
        c.calibration_future=Future();c.calibration_version='r';c.in_progress=None
        c.calibration_future.set_result(dict(passed=True,count=10,qualified_entries=[1],source_saved_s=.01,target_added_s=.02))
        c.log=lambda *a,**kw:None;c.fresh=lambda *a:True
        c.poll_calibration(100.)
        self.assertEqual(c.regions['r']['published'],5.)
        self.assertEqual(c.regions['r']['validated_at'],100.)
        self.assertTrue(c.regions['r']['qualified'])

    def test_quality_failure_pauses_family_but_missing_probes_do_not(self):
        for count,fraction in [(0,.5),(10,0.)]:
            c=LoadBalancer.__new__(LoadBalancer);c.lock=threading.RLock()
            c.b=dict(online_policy=dict(enabled=True,calibration_interval_seconds=30),min_validation_queries=2)
            c.regions={'r':dict(id='r',source=0,target=1,entries=[1],published=5.,family_id='f',fraction=.5)}
            c.calibration_future=Future();c.calibration_version='r';c.in_progress=None
            c.calibration_future.set_result(dict(passed=False,count=count))
            c.log=lambda *a,**kw:None;c.fresh=lambda *a:True
            c.poll_calibration(100.)
            self.assertEqual(c.regions['r']['fraction'],fraction)


class FullShareRuntimeTests(unittest.TestCase):
    """Real native graph copies and searches; synthetic load/cost gates only."""
    from test_replica_lifecycle import MultiReplicaRuntimeTests as _fixture
    setUpClass=classmethod(_fixture.setUpClass.__func__)
    tearDownClass=classmethod(_fixture.tearDownClass.__func__)
    setUp=_fixture.setUp
    info=_fixture.info
    rpc=_fixture.rpc
    def configure_replica_test(self,c):
        self._fixture.configure_replica_test(self,c)
        c['load_balance'].update(initial_fraction=.8,max_fraction=.8)
    test_full_share_copy_search_single_root_and_drain=_fixture.test_controller_pressure_build_scale_down_and_cold_reclaim


class StagedGrowthRuntimeTests(unittest.TestCase):
    """Real record transfer/install; synthetic capacity base and cost validation."""
    from test_replica_growth import GrowingReplicaTests as _fixture
    setUpClass=classmethod(_fixture.setUpClass.__func__)
    tearDownClass=classmethod(_fixture.tearDownClass.__func__)
    setUp=_fixture.setUp
    configure_replica_test=_fixture.configure_replica_test
    info=_fixture.info
    rpc=_fixture.rpc
    service=_fixture.service
    inputs=_fixture.inputs

    def test_large_portfolio_stays_unroutable_until_atomic_activation(self):
        from unittest.mock import patch
        a,others,shard,history,layout=self.inputs()
        ctl=self.engine.start_load_balancer(self.token,Path(self.work.name)/'staged')
        layout.update(replica_tier='large',total_alpha=.6,defer_publication=True,require_validation=True)
        ctl.validate=lambda plan,version,progress=None:dict(passed=True,count=32,qualified_entries=plan['entries'],source_saved_s=.001,target_added_s=.002)
        with patch('dhdgs.locality.source_index_bytes',return_value=10000000):
            report,versions=ctl.build_partitioned_online(a,history,layout)
        self.assertEqual(len(versions),2)
        self.assertTrue(all(ctl.regions[v]['_pending_activation'] for v in versions))
        ctl.publish(time.monotonic())
        q=self.engine.search_async(self.x[0],0).result(10)
        self.assertFalse(q['stats'].get('replica_version'))
        now=time.monotonic()
        for node in [a]+others:
            ctl.loads.setdefault(node,{}).update(updated=now,usable=True,epoch=shard.meta['epoch'])
        ctl.activate_portfolio(versions,[])
        self.assertTrue(all(not ctl.regions[v]['_pending_activation'] for v in versions))
        self.assertEqual(ctl.partition_pending,{})
        self.assertEqual(ctl.build_reservations,{})
        self.assertTrue(any(e['event']=='PORTFOLIO_ACTIVATED'for e in ctl.events))
        ctl.retire(wait=True)
        self.assertEqual(ctl.draining,{})

    def test_source_cancel_fences_a_late_start(self):
        a,others,shard,history,layout=self.inputs()
        self.assertTrue(self.rpc(a,'source_cancel',id='cancelled_late')['quiescent'])
        with self.assertRaisesRegex(RuntimeError,'Conflicting source retry'):
            self.rpc(a,'source_start',id='cancelled_late',plan=dict(source=a,target=others[0]))

    def test_calibrated_subset_can_copy_without_weakening_full_core_proof(self):
        from unittest.mock import patch
        a,others,shard,history,layout=self.inputs()
        layout['target_weights']={str(others[0]):1.}
        ctl=self.engine.start_load_balancer(self.token,Path(self.work.name)/'subset_copy')
        ctl.stop.set();ctl.thread.join();ctl.stop.clear()
        ctl.b['adaptive_expansion']['enabled']=True
        ctl.validate=lambda plan,version,progress=None:dict(passed=True,count=32,source_saved_s=.001,target_added_s=.002)
        with patch('dhdgs.locality.source_index_bytes',return_value=10000000):
            report,versions=ctl.build_partitioned_online(a,history,layout)
            r=ctl.regions[versions[0]]
            self.assertGreater(len(r['certified_entries']),1)
            r['entries']=r['entries'][:1];r['fraction']=0.
            plan={k:r[k] for k in ('source','ids','entries','min_score','max_score','region_proof','region_stats','selection_policy','training_query_ids','epoch','source_capacity_bytes','certified_entries') if k in r}
            plan.update(target=others[1],replica_of=r['id'],_replica_family=r['family_id'],
                        _initial_fraction=.05,reserved_package_bytes=r['reserved_package_bytes'])
            clone=ctl.create(plan).result(15)
        self.assertEqual(ctl.regions[clone]['entries'],r['entries'])
        self.assertEqual(ctl.regions[clone]['certified_entries'],r['certified_entries'])
        ctl.retire(wait=True)


class EntryOwnershipTests(unittest.TestCase):
    def test_calibration_filter_does_not_reoffer_reserved_entries(self):
        c=LoadBalancer.__new__(LoadBalancer);c.lock=threading.RLock()
        c.b=dict(online_policy=dict(enabled=True,holdout_modulus=5))
        c.regions={'r':dict(source=0,entries=[1],_candidate_entries=[1,2])}
        qid=next(i for i in range(100) if not held_out(i,5))
        c.history=[dict(source=0,entry=i,query_id=qid)for i in [1,2,3]]
        self.assertEqual([q['entry']for q in c.plan_snapshot(0)],[3])
        self.assertEqual(len(c.plan_snapshot(0,True)),3)

    def test_transient_target_overload_does_not_repeatedly_halve_share(self):
        c=LoadBalancer.__new__(LoadBalancer);c.lock=threading.RLock()
        c.b=dict(DEFAULT,online_policy=dict(DEFAULT['online_policy'],enabled=True),
                 adaptive_expansion=dict(DEFAULT['adaptive_expansion'],enabled=True))
        c.regions={'r':dict(id='r',source=0,target=1,entries=[1],fraction=.8,published=0.,last_use=10.,publication_mode='ready')}
        c.loads={0:dict(util=.9,queue_ms=30,worker_pressure_ms=30),1:dict(util=.9,queue_ms=30,worker_pressure_ms=30)}
        c.rates={1:100};c.fresh=lambda n,now:True;c.log=lambda *a,**kw:None;c.decay=object()
        c.adjust(10.);c.adjust(11.)
        self.assertEqual(c.regions['r']['fraction'],.8)
        c.adjust(12.);self.assertEqual(c.regions['r']['fraction'],.4)
        c.adjust(12.1);self.assertEqual(c.regions['r']['fraction'],.4)


class OnlineControlWiringTests(unittest.TestCase):
    def controller(self):
        c=IndependentCapacityTests().controller()
        c.b['online_policy']=dict(DEFAULT['online_policy'],enabled=True,min_episode_seconds=1.,large_sustained_seconds=2.)
        c.b['adaptive_expansion']=dict(DEFAULT['adaptive_expansion'])
        c.b['automatic_pipeline']['max_targets']=1
        c.online_policy=OnlinePolicy(c.b['online_policy'])
        c.plan_future=None;c.last_policy=0.;c.cooldown_until=0.;c.policy_observations_healthy=True
        c.history=BudgetAndControlTests().observations()
        for sid,l in c.loads.items():l.update(updated=100.,worker_pressure_ms=4. if sid==1 else .1,source_index_bytes=6000000)
        c.regions={'old':dict(id='old',source=1,target=2,entries=[0],bytes=1800000,published=99.,
                         qualified=True,ready_seconds=1.,validation=dict(source_saved_s=.001))}
        entries=[q['entry'] for q in c.plan_snapshot(1,True)]
        c.online_policy.tick(97.,c.loads,{1:entries},[1])
        for t in [98.,99.]:c.online_policy.tick(t,c.loads,{1:entries},[1])
        c.control_expansion=lambda now:False
        c.submitted=[]
        c.planner=SimpleNamespace(submit=lambda *args,**kw:c.submitted.append((args,kw)) or Future())
        return c

    def test_persistent_positive_region_submits_real_large_pipeline_with_replacement(self):
        c=self.controller();c.control_online(100.)
        self.assertEqual(len(c.submitted),1)
        self.assertEqual(c.submitted[0][1],dict(tier='large',replace_versions=['old']))
        self.assertEqual(c.plan_context['tier'],'large')

    def test_unqualified_region_cannot_upgrade_but_may_build_disjoint_small_graph(self):
        c=self.controller();c.regions['old']['qualified']=False;c.control_online(100.)
        self.assertEqual(len(c.submitted),1)
        self.assertNotEqual(c.submitted[0][1].get('tier'),'large')
        self.assertEqual(c.plan_context['tier'],'small')

    def test_missing_observation_freezes_decisions(self):
        c=self.controller();c.policy_observations_healthy=False;c.control_online(100.)
        self.assertEqual(c.submitted,[])


class BuildAndSharesTests(unittest.TestCase):
    def test_existing_share_can_grow_while_an_unrelated_plan_is_building(self):
        c=LoadBalancer.__new__(LoadBalancer);c.lock=threading.RLock()
        c.b=dict(DEFAULT,adaptive_expansion=dict(DEFAULT['adaptive_expansion'],enabled=True))
        c.in_progress='another_graph';c.plan_future=Future()
        c.regions={'a':dict(id='a',source=0,target=1,entries=[1],family_id='f',fraction=.1,
                           published=100.,qualified=True,validation=dict(count=100,source_saved_s=.001,target_added_s=.001))}
        c.loads={0:dict(util=.99,workers=64,queue_ms=10.,updated=101.,usable=True),
                 1:dict(util=.1,workers=64,queue_ms=0.,updated=101.,usable=True)}
        c.family_rates={'f':1000.};c.expansion_cooldown={};c.expansion_streaks={};c.shrink_streaks={};c.decay=None
        c.log=lambda *a,**kw:None
        self.assertTrue(c.control_expansion(101.))
        self.assertGreater(c.regions['a']['fraction'],.1)
        self.assertEqual(c.in_progress,'another_graph')


class LegacyCapacityCompatibilityTests(unittest.TestCase):
    def test_omitted_optional_global_budget_preserves_previous_fallback(self):
        from dhdgs.replica_budget import total_limit,target_limit
        b=dict(max_region_bytes=1000,max_replicas=3)
        self.assertEqual(total_limit(b),3000)
        self.assertEqual(target_limit(b),3000)
