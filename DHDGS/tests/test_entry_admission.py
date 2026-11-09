"""All-covered portals, cumulative return credit and native concurrent writes."""
import copy
from concurrent.futures import Future
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import threading
import time
import unittest

from dhdgs.entry_admission import evaluate
from dhdgs.load_balance import LoadBalancer
from dhdgs.replica_layout import build_partitioned_regions
from dhdgs.community import verify
from test_partition_pipeline import PartitionPipelineTests as _Partition
from test_hop_return import PullRuntimeTests as _Pull

SETTINGS=dict(enabled=True,interval_seconds=5.,min_returns=32,
              max_mean_remaining_fraction=.5,counter_stripes=16)


class CoveredGraphTests(unittest.TestCase):
    def fixture(self):
        f=_Partition();f.setUp();self.addCleanup(f.doCleanups)
        return f

    def test_candidate_directory_is_not_limited_to_observed_hot_entries(self):
        import numpy as np
        from test_automatic_replica import BudgetAndControlTests
        from dhdgs.automatic_replica import prepare
        f=BudgetAndControlTests();c=f.controller()
        c.b['entry_admission']=dict(SETTINGS)
        c.b['automatic_pipeline'].update(max_targets=1,max_seed_entries=2)
        membership=np.zeros((8,3),dtype=bool);membership[[0,1,2,3,5],1]=True
        c.engine=SimpleNamespace(ids=np.arange(8,dtype=np.uint64),membership=membership,
                                 route_bank={'ids':np.arange(8,dtype=np.uint64)})
        history,layout,_=prepare(c,1,2,f.observations())
        self.assertEqual(len(history['hot_ids']),2)
        self.assertEqual(layout['covered_route_entries'],[0,1,2,3,5])

    def test_boundary_and_unobserved_portals_open_without_changing_members(self):
        f=self.fixture();layout=copy.deepcopy(f.layout)
        layout.update(target_weights={1:1.},hops=1)
        settings=dict(f.b,expansion_soft_ms=0)
        base,_=build_partitioned_regions(f.shard,[1],[1.],settings,layout)
        layout['covered_route_entries']=[1,2,4,10]
        expanded,report=build_partitioned_regions(f.shard,[1],[1.],settings,layout)
        self.assertEqual(expanded[0]['ids'],base[0]['ids'])
        self.assertEqual(expanded[0]['entries'],[1,2])  # 2 points outside to 3.
        self.assertNotIn(3,expanded[0]['ids'])
        verify(f.shard,expanded[0],settings)
        self.assertLessEqual(expanded[0]['reserved_package_bytes'],report['budget_bytes'])
        bad=copy.deepcopy(expanded[0]);bad['entries'].append(10)
        with self.assertRaises(ValueError):verify(f.shard,bad,settings)

    def test_overlapping_partitions_register_all_contained_portals(self):
        f=self.fixture();layout=copy.deepcopy(f.layout)
        layout['covered_route_entries']=[1,2,3,4,10]
        plans,_=build_partitioned_regions(f.shard,[1,3],[1.,1.],f.b,layout)
        for plan in plans:
            self.assertEqual(plan['entries'],sorted(set(plan['ids'])&set(layout['covered_route_entries'])))
            verify(f.shard,plan,f.b)
        self.assertTrue(set(plans[0]['entries'])&set(plans[1]['entries']))


class EntryPolicyTests(unittest.TestCase):
    def test_expected_budget_weights_rare_returns_and_strict_boundary(self):
        cfg=dict(SETTINGS,score_mode='expected_remaining_per_query',
                 min_returns=20,max_expected_remaining_budget=10.)
        row=dict(returns=20,remaining_sum=3200,initial_budget=200,completed=400)
        value=evaluate(row,cfg)
        self.assertEqual(value['return_probability'],.05)
        self.assertEqual(value['mean_remaining'],160.)
        self.assertEqual(value['score'],8.)
        self.assertFalse(value['close'])
        self.assertFalse(evaluate(dict(row,completed=320),cfg)['close'])
        self.assertTrue(evaluate(dict(row,completed=319),cfg)['close'])
        self.assertFalse(evaluate(dict(row,completed=19,returns=19,remaining_sum=3040),cfg)['close'])

    def test_expected_empty_zero_return_and_incomplete_snapshots(self):
        cfg=dict(SETTINGS,score_mode='expected_remaining_per_query',
                 min_returns=20,max_expected_remaining_budget=0.)
        row=dict(returns=0,remaining_sum=0,initial_budget=200,completed=0)
        self.assertEqual(evaluate(row,cfg)['score'],0.)
        self.assertFalse(evaluate(dict(row,completed=100),cfg)['close'])
        row.update(returns=20,remaining_sum=4000,completed=19)
        self.assertEqual(evaluate(row,cfg)['reason'],'incomplete_snapshot')
        self.assertFalse(evaluate(dict(row,completed=20,saturated=1),cfg)['close'])

    def test_expected_configuration_and_legacy_compatibility(self):
        from dhdgs.config import load_config,validate
        c=load_config(Path(__file__).resolve().parents[1]/'configs/dhdgs.yaml')
        c={k:v for k,v in c.items() if not k.startswith('_')}
        cfg=dict(SETTINGS,score_mode='expected_remaining_per_query',max_expected_remaining_budget=80.)
        cfg.pop('max_mean_remaining_fraction')
        cfg['enabled']=False
        c['load_balance']['entry_admission']=cfg
        validate(c)
        for bad in [-1.,float('nan'),float('inf')]:
            cfg['max_expected_remaining_budget']=bad
            with self.assertRaises(ValueError):validate(c)
        cfg['max_expected_remaining_budget']=80.;cfg['score_mode']='unknown'
        with self.assertRaises(ValueError):validate(c)
        c['load_balance']['entry_admission']=dict(SETTINGS,enabled=False)
        validate(c)

    def test_return_only_mean_strict_threshold_and_minimum(self):
        row=dict(returns=31,remaining_sum=31*200,initial_budget=200,completed=1000)
        self.assertFalse(evaluate(row,SETTINGS)['close'])
        row.update(returns=32,remaining_sum=32*100)
        self.assertFalse(evaluate(row,SETTINGS)['close'])
        row['remaining_sum']+=1
        self.assertTrue(evaluate(row,SETTINGS)['close'])
        row.update(returns=0,remaining_sum=0)
        self.assertIsNone(evaluate(row,SETTINGS)['mean_remaining'])

    def test_controller_closes_only_bad_entry_and_keeps_unknown_open(self):
        with tempfile.TemporaryDirectory() as folder:
            c=LoadBalancer.__new__(LoadBalancer);c.lock=threading.RLock();c.directory=Path(folder)
            c.b=dict(entry_admission=SETTINGS);c.regions={'v':dict(id='v',entries=[1,2,3])}
            rows=[dict(version='v',entry=e,returns=n,remaining_sum=n*r,initial_budget=200,completed=n,saturated=0)
                  for e,n,r in [(1,32,101),(2,32,99),(3,31,200)]]
            c.engine=SimpleNamespace(runtime=SimpleNamespace(replica_entry_statistics=lambda:rows))
            c.log=lambda *args,**kw:None;updates=[];c.publish=lambda now:updates.append(now)
            c.poll_entry_admission(100.)
            self.assertEqual(c.regions['v']['closed_entries'],[1]);self.assertEqual(updates,[100.])
            # Cumulative snapshots are not deltas. Re-reading cannot add samples.
            c.poll_entry_admission(101.);c.poll_entry_admission(105.)
            self.assertEqual(updates,[100.])
            self.assertEqual(json.loads((Path(folder)/'entry_admission.json').read_text())['entries'][2]['returns'],31)

    def test_shadow_diagnostic_does_not_zero_or_filter_open_contract(self):
        c=LoadBalancer.__new__(LoadBalancer);c.lock=threading.RLock()
        c.b=dict(entry_admission=SETTINGS,online_policy=dict(enabled=True,calibration_interval_seconds=30),min_validation_queries=32)
        c.regions={'v':dict(id='v',source=0,target=1,entries=[1,2],published=5.,family_id='f',fraction=.9)}
        c.calibration_future=Future();c.calibration_version='v';c.in_progress=None
        c.calibration_future.set_result(dict(passed=False,count=64,qualified_entries=[]))
        c.log=lambda *a,**kw:None;c.fresh=lambda *a:True
        c.poll_calibration(100.)
        self.assertEqual(c.regions['v']['fraction'],.9)
        self.assertEqual(c.regions['v']['entries'],[1,2])
        self.assertFalse(c.regions['v']['qualified'])


class NativeEntryTelemetryTests(unittest.TestCase):
    setUpClass=classmethod(_Pull.setUpClass.__func__)
    tearDownClass=classmethod(_Pull.tearDownClass.__func__)
    setUp=_Pull.setUp
    rpc=_Pull.rpc
    info=_Pull.info
    install_entry=_Pull.install_entry

    def configure_replica_test(self,c):
        _Pull.configure_replica_test(self,c)
        c['peer']['replica_pull']['enabled']=False
        c['load_balance'].update(publication_mode='ready',quality_weight_routing=False,
                                  peer_routes_enabled=False,entry_admission=dict(SETTINGS))

    def test_native_pre_split_credit_concurrent_queries_refresh_and_closure(self):
        source,target=self.install_entry();entry=self.info()['entry_id']
        route=dict(source=source,target=target,version='pull_test',family='family1',
                   entries=[entry],fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=10000)
        self.engine.runtime.replica_routes([route])
        count=64
        jobs=[self.engine.search_async(self.x[0],0) for _ in range(count)]
        self.engine.runtime.replica_routes([route])  # Must retain live counters.
        results=[f.result(20) for f in jobs]
        expected=[]
        for result in results:
            forks=[e for b in result['branch_reports'] if b.get('replica_version')=='pull_test'
                   for e in b.get('fork_events',[]) if e['target']==source]
            if forks:expected.append(min(forks,key=lambda e:e['dispatch_submitted_ns'])['remaining_before'])
        self.assertGreaterEqual(len(expected),32)
        snapshot=self.engine.runtime.replica_entry_statistics()
        self.assertEqual(len(snapshot),1);row=snapshot[0]
        self.assertEqual(row['completed'],count)
        self.assertEqual(row['returns'],len(expected))
        self.assertEqual(row['remaining_sum'],sum(expected))
        self.assertEqual(row['saturated'],0)
        self.assertTrue(evaluate(row,SETTINGS)['close'])
        expected_cfg=dict(SETTINGS,score_mode='expected_remaining_per_query',
                          max_expected_remaining_budget=100.)
        self.assertEqual(evaluate(row,expected_cfg)['score'],sum(expected)/count)
        # Shadow replay must not pollute live admission statistics.
        self.engine.runtime.validate_replica(self.x[0].astype(float),0,
            dict(entry_id=entry,source=source,target=target,version='pull_test'))
        self.assertEqual(self.engine.runtime.replica_entry_statistics(),snapshot)
        controller=LoadBalancer.__new__(LoadBalancer)
        controller.lock=threading.RLock();controller.b=self.c['load_balance']
        controller.b['max_fraction']=1.
        controller.c=self.c;controller.directory=Path(self.work.name)
        controller.engine=self.engine
        controller.loads={source:dict(updated=time.monotonic(),usable=True),
                          target:dict(updated=time.monotonic(),usable=True)}
        controller.regions={'pull_test':dict(route,id='pull_test',family_id='family1')}
        controller.log=lambda *a,**kw:None
        controller.poll_entry_admission(time.monotonic())
        self.assertEqual(controller.regions['pull_test']['closed_entries'],[entry])
        result=self.engine.search_async(self.x[0],0).result(20)
        self.assertNotIn('replica_version',result['stats'])
        self.assertEqual(self.engine.runtime.replica_entry_statistics(),snapshot)
        if os.environ.get('DHDGS_ENTRY_ADMISSION_EVIDENCE'):
            Path(os.environ['DHDGS_ENTRY_ADMISSION_EVIDENCE']).write_text(json.dumps(dict(
                concurrent_queries=count,source=source,target=target,entry=entry,
                telemetry=row,policy=evaluate(row,SETTINGS),expected_returns=len(expected),
                expected_remaining_sum=sum(expected),shadow_excluded=True,
                closure_next_query_on_source=True),indent=2)+'\n')

    def test_overlapping_families_select_one_root_and_closed_owner_yields(self):
        source,target=self.install_entry();entry=self.info()['entry_id']
        # First family is disabled; second is the installed replica. Selection
        # must not try both families or sum their independent .9 allocations.
        base=dict(source=source,target=target,entries=[entry],fraction=.9,
                  min_score=-1e300,max_score=1e300,ttl_ms=10000)
        routes=[dict(base,version='closed_family',family='f0',entry_weights={str(entry):0.}),
                dict(base,version='pull_test',family='f1',fraction=1.)]
        self.engine.runtime.replica_routes(routes)
        result=self.engine.search_async(self.x[0],0).result(20)
        roots=[b for b in result['branch_reports'] if b['parent_branch_id'] is None]
        self.assertEqual(len(roots),1);self.assertEqual(roots[0]['replica_version'],'pull_test')


class NativePullTelemetryTests(unittest.TestCase):
    setUpClass=classmethod(_Pull.setUpClass.__func__)
    tearDownClass=classmethod(_Pull.tearDownClass.__func__)
    setUp=_Pull.setUp
    rpc=_Pull.rpc
    info=_Pull.info
    install_entry=_Pull.install_entry

    def configure_replica_test(self,c):
        _Pull.configure_replica_test(self,c)
        c['load_balance'].update(publication_mode='ready',quality_weight_routing=False,
                                  peer_routes_enabled=False,entry_admission=dict(SETTINGS))

    def test_data_pull_is_not_counted_as_compute_return(self):
        source,target=self.install_entry()
        result=self.engine.search_async(self.x[0],0).result(20)
        self.assertGreater(result['stats']['pull_expanded'],0)
        forks=[e for b in result['branch_reports'] if b.get('replica_version')=='pull_test'
               for e in b.get('fork_events',[]) if e['target']==source]
        row=self.engine.runtime.replica_entry_statistics()[0]
        self.assertEqual(row['completed'],1)
        self.assertEqual(row['returns'],int(bool(forks)))
        self.assertEqual(row['remaining_sum'],min(forks,key=lambda e:e['dispatch_submitted_ns'])['remaining_before'] if forks else 0)
