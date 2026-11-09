"""Readiness publication is explicit and never masquerades as quality proof."""
import copy
from pathlib import Path
import time
import unittest
from unittest.mock import patch
import numpy as np
from dhdgs.config import validate
from dhdgs.load_balance import LoadBalancer, candidate
from dhdgs.replicas import package
from dhdgs.index import Shard
from test_replicas import ControllerPolicyTests, ReplicaTests


class CandidatePlanningTests(unittest.TestCase):
    def test_shared_worksets_and_portal_capacity_are_preserved(self):
        settings=dict(min_entry_queries=1,min_group_queries=1,min_coverage=.5,
                      selection_policy='cpu_worksets',max_core_rows=4,portal_budget_fraction=.25)
        observations=[dict(source=0,replica=None,query_id=i,entry=entry,score=.5,
                           branches=[dict(node=0,cpu_ms=cpu,expanded_ids=ids)])
                      for i,(entry,cpu,ids) in enumerate([(1,6.,[1,2]),(3,4.,[2,3]),(4,3.,[4,5,6])])]
        plan=candidate(observations,0,1,settings)
        self.assertEqual(plan['ids'],[1,2,3,4])
        self.assertEqual(plan['entries'],[1,3,4])
        self.assertEqual(plan['attached_portals'],1)
        self.assertEqual(plan['training_query_ids'],[0,1,2])


class ReadyPolicyTests(unittest.TestCase):
    controller = ControllerPolicyTests.controller

    def test_feedback_recovers_only_to_configured_fraction_and_backs_off(self):
        x=self.controller();now=100
        x.b.update(initial_fraction=.3,max_fraction=1.,fraction_step=.2)
        x.loads={0:dict(util=.99,queue_ms=80,updated=now,usable=True,workers=64),
                 1:dict(util=.1,queue_ms=0,updated=now,usable=True,workers=64)}
        x.regions={'r':dict(id='r',source=0,target=1,entries=[1],fraction=.1,
            last_use=now,published=now,publication_mode='ready',
            validation=dict(quality_validated=False,source_saved_s=None,target_added_s=None))}
        x.rates[1]=10000
        x.adjust(now);self.assertEqual(x.active['fraction'],.3)
        x.adjust(now);self.assertEqual(x.active['fraction'],.3)
        self.assertIsNone(x.logged[-1][1]['estimated_source_relief_cores'])
        x.loads[1]['queue_ms']=100
        x.adjust(now);self.assertEqual(x.active['fraction'],.15)


class ReadyRuntimeTests(unittest.TestCase):
    setUpClass=classmethod(ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(ReplicaTests.tearDownClass.__func__)
    setUp=ReplicaTests.setUp
    config=ReplicaTests.config if hasattr(ReplicaTests,'config') else None
    info=ReplicaTests.info
    rpc=ReplicaTests.rpc

    def configure_replica_test(self,c):
        c['load_balance'].update(publication_mode='ready',warm_bytes=0,
            compute_region_quality=False,quality_weight_routing=False,peer_routes_enabled=False,
            native_replica_pack=True,initial_fraction=1.,max_fraction=1.)

    def test_ready_publication_skips_replay_but_preserves_validation_and_search(self):
        info=self.info();a=info['source'];b=(a+1)%3
        ids=np.load(Path(self.c['paths']['output'])/'index'/f'shard_{a}'/'ids.npy').tolist()
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'ready')
        controller.stop.set();controller.thread.join();controller.stop.clear()
        now=time.monotonic()
        controller.loads={n:dict(updated=now,usable=True) for n in [a,b]}
        plan=dict(source=a,target=b,ids=ids,entries=ids,training_query_ids=[],
                  min_score=-1e300,max_score=1e300)
        with patch.object(controller,'validate',side_effect=AssertionError('Unexpected quality replay')):
            version=controller.create(plan).result(15)
        r=controller.active
        self.assertFalse(r['qualified'])
        self.assertFalse(r['validation']['quality_validated'])
        self.assertIsNone(r['validation']['passed'])
        target=self.rpc(b,'status',id=version)
        self.assertEqual(target['phase'],'READY')
        self.assertEqual(target['warm_read_bytes'],0)
        self.assertGreater(target['validation_read_bytes'],0)
        self.assertFalse(target['manifest']['quality']['computed'])
        now=time.monotonic()
        for v in controller.loads.values():v['updated']=now
        controller.publish(now)
        result=self.engine.search_async(self.x[0],0).result(10)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(result['stats'].get('replica_version'),version)
        controller.retire()
        self.assertIsNone(controller.active)
        self.assertEqual(self.rpc(b,'status',id=version)['phase'],'RETIRED')

    def test_skipping_scores_does_not_change_records_or_dependencies(self):
        shard=Shard(Path(self.c['paths']['output'])/'index/shard_0',0)
        self.addCleanup(shard.close)
        settings=copy.deepcopy(self.c['load_balance'])
        ids=list(map(int,shard.ids[:8]))
        settings['compute_region_quality']=True
        a=package(shard,ids,ids[:2],Path(self.work.name)/'scores',settings)
        settings['compute_region_quality']=False
        b=package(shard,ids,ids[:2],Path(self.work.name)/'no_scores',settings)
        self.assertEqual(a['files'],b['files'])
        self.assertEqual(a['rows'],b['rows'])
        self.assertEqual(a['code_rows'],b['code_rows'])

    def test_invalid_policy_and_score_dependencies_rejected(self):
        c=copy.deepcopy(self.c)
        c['load_balance']['publication_mode']='none'
        with self.assertRaisesRegex(ValueError,'publication_mode'):validate({k:v for k,v in c.items()if not k.startswith('_')})
        c['load_balance']['publication_mode']='ready'
        c['load_balance']['quality_weight_routing']=True
        with self.assertRaisesRegex(ValueError,'Region scores'):validate({k:v for k,v in c.items()if not k.startswith('_')})
