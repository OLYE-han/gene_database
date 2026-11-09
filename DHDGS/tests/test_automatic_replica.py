import copy
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from concurrent.futures import Future

from dhdgs.automatic_replica import prepare
from dhdgs.load_balance import LoadBalancer
from dhdgs.planning_context import limited_settings


class BudgetAndControlTests(unittest.TestCase):
    def controller(self):
        c=LoadBalancer.__new__(LoadBalancer)
        c.b=dict(automatic_pipeline=dict(max_seed_entries=4,max_targets=2,planning_seconds=20.,
                    finish_reserve_seconds=5.,min_capacity_bytes=100000),
                 community_top_k=4,min_entry_queries=1,min_group_queries=1,
                 partitioned_regions=dict(total_alpha=.3,hops=1,pipeline={}),
                 locality=dict(enabled=True,source_capacity_fraction=.3),
                 max_total_replica_bytes=3000000,max_region_bytes=3000000,max_replicas=4,
                 reserve_disk_bytes=0,max_load_age_ms=1000,source_util_high=.5,queue_high_ms=2.,
                 target_util_low=.5,queue_low_ms=2.,util_gap=.1)
        c.lock=threading.RLock();c.regions={};c.draining={};c.in_progress=None;c.partition_pending={}
        c.c={'cluster':{'nodes':[{}, {}, {}]}}
        c.loads={n:dict(util=.1 if n!=1 else .9,queue_ms=0. if n!=1 else 4.,
                       backlog_estimate_ms=0.,usable=True,updated=time.monotonic(),free_bytes=10000000,epoch='x')for n in range(3)}
        c.rpc=lambda node,op,**kw:dict(source_index_bytes=6000000)
        c.log=lambda *a,**kw:None
        return c

    def observations(self):
        return [dict(source=1,replica=None,query_id=i,entry=i,branches=[dict(node=1,cpu_ms=10-i)])for i in range(4)]

    def test_smaller_source_and_existing_copies_bound_aggregate_reservation(self):
        c=self.controller();c.regions={'a':dict(source=1,bytes=600000)}
        history,layout,epochs=prepare(c,1,2,self.observations())
        self.assertEqual(layout['capacity_bytes'],1200000)
        self.assertEqual(set(layout['target_weights']),{'0','2'})
        self.assertNotIn('1',layout['target_weights'])
        self.assertEqual(history['hot_ids'],[0,1,2,3])
        self.assertLessEqual(layout['total_alpha']*6000000,1200000)

    def test_no_capacity_or_stale_pressure_never_plans(self):
        c=self.controller();c.regions={'a':dict(source=1,bytes=1800000)}
        with self.assertRaisesRegex(ValueError,'capacity'):prepare(c,1,2,self.observations())
        c.regions={};c.loads[2]['updated']-=5
        with self.assertRaisesRegex(ValueError,'pressure'):prepare(c,1,2,self.observations())

    def test_capacity_gate_skips_work_and_resumes_after_reclaim(self):
        for scope in ('global','source'):
            with self.subTest(scope=scope):
                c=self.controller();c.b['automatic_pipeline']['enabled']=True
                c.b['sustained_windows']=1;c.streak=0;c.last_pair=None
                c.plan_future=None;c.cooldown_until=0.;jobs=[];events=[]
                c.plan_snapshot=lambda source: jobs.append(('snapshot',source)) or []
                c.planner=SimpleNamespace(submit=lambda *args:jobs.append(('submit',args)) or Future())
                c.log=lambda event,**kw:events.append(event)
                used=2950000 if scope=='global' else 1750000
                c.draining={'old':dict(source=1,bytes=used)}
                if scope=='source':c.loads[1]['source_index_bytes']=6000000
                c.control(time.monotonic());c.control(time.monotonic())
                self.assertEqual(jobs,[])
                self.assertEqual(events,['AUTO_CAPACITY_BLOCKED'])
                c.draining.clear();c.control(time.monotonic())
                self.assertEqual([j[0]for j in jobs],['snapshot','submit'])

    def test_capacity_change_after_preflight_is_rechecked(self):
        from dhdgs.automatic_replica import remaining_capacity
        c=self.controller()
        with c.lock:self.assertGreater(remaining_capacity(c,1,6000000),100000)
        c.regions={'new':dict(source=1,bytes=1800000)}
        with self.assertRaisesRegex(ValueError,'capacity'):
            prepare(c,1,2,self.observations())

    def test_automatic_path_preserves_minimum_distinct_query_evidence(self):
        c=self.controller();c.b['min_group_queries']=5
        with self.assertRaisesRegex(ValueError,'query coverage'):
            prepare(c,1,2,self.observations()*3)

    def test_create_uses_actual_plan_reservation_for_smaller_source(self):
        c=self.controller();c.builder=SimpleNamespace(submit=lambda *a:a)
        p=dict(source=1,target=2,reserved_package_bytes=1000000,source_capacity_bytes=6000000)
        out=c.create(p)
        self.assertEqual(out[2]['reserved_package_bytes'],1000000)
        c.in_progress=None
        with self.assertRaisesRegex(ValueError,'source replica capacity'):
            c.create(dict(source=1,target=2))

    def test_legacy_propose_keeps_certificate_capacity(self):
        c=self.controller();c.b.update(selection_policy='hot_entry_community',operation_timeout_s=10,
            planner_max_age_ms=10000,community_poll_ms=1)
        c.stop=threading.Event()
        region=dict(entries=[0],ids=[0,1],region_proof={},stats={'estimated_bytes':700000},reserved_package_bytes=800000)
        calls=[]
        def rpc(n,op,**kw):
            calls.append((op,kw))
            return dict(source_index_bytes=6000000)if op=='load'else dict(done=True,region=region)
        c.rpc=rpc
        obs=self.observations()
        for q in obs:q['score']=1.
        plan=c.propose(1,2,obs)
        self.assertEqual(plan['reserved_package_bytes'],800000)
        self.assertEqual(calls[1][1]['capacity_bytes'],1800000)

    def test_total_deadline_is_consumed_not_reset_per_stage(self):
        b={'community_max_plan_ms':300000,'_planning_deadline':25.}
        with patch('dhdgs.planning_context.time.monotonic',return_value=5.):
            self.assertEqual(limited_settings(b)['community_max_plan_ms'],20000)
        with patch('dhdgs.planning_context.time.monotonic',return_value=24.):
            self.assertEqual(limited_settings(b)['community_max_plan_ms'],1000)
        with patch('dhdgs.planning_context.time.monotonic',return_value=26.):
            with self.assertRaises(TimeoutError):limited_settings(b)

    def test_completed_automatic_future_does_not_duplicate_create(self):
        c=self.controller();c.plan_future=Future();c.plan_future.set_result({'published':True})
        c.plan_context={'automatic_pipeline':True,'source':1};c.b['cooldown_ms']=100
        c.create=lambda *a,**k:self.fail('duplicate create')
        c.poll_plan(time.monotonic());self.assertIsNone(c.plan_future)


class DeadlineGraphTests(unittest.TestCase):
    def test_zero_soft_expansion_budget_keeps_complete_core_and_valid_proof(self):
        from test_partition_pipeline import PartitionPipelineTests
        from dhdgs.community import seeded_core
        from dhdgs.hop_expansion import breadth_expand,verify_expansion
        f=PartitionPipelineTests();f.setUp();self.addCleanup(f.doCleanups)
        b=dict(f.b,expansion_soft_ms=0)
        core=seeded_core(f.shard,[1],[1.],b,seed_count=1,hops=1)
        emitted=[]
        plan=breadth_expand(f.shard,core,b,on_admit=lambda ids:emitted.extend(ids),batch_rows=2)
        self.assertEqual(plan['ids'],core['ids'])
        self.assertEqual(sorted(emitted),plan['ids'])
        self.assertTrue(plan['stats']['soft_deadline_stopped'])
        self.assertTrue(verify_expansion(f.shard,plan,b)['verified'])

    def test_expired_total_deadline_never_emits_a_plan(self):
        from test_partition_pipeline import PartitionPipelineTests
        f=PartitionPipelineTests();f.setUp();self.addCleanup(f.doCleanups)
        cfg=copy.deepcopy(f.layout);cfg['deadline_s']=1e-9
        emitted=[]
        with self.assertRaises((RuntimeError,TimeoutError,ValueError)):
            f.build(cfg,on_plan=emitted.append)
        self.assertEqual(emitted,[])
