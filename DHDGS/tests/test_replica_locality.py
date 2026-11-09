import copy
from pathlib import Path
from types import SimpleNamespace
import threading
import unittest
from unittest.mock import patch, Mock
import numpy as np
import yaml
from dhdgs.index import native
from dhdgs.locality import select_regions
from dhdgs.load_balance import LoadBalancer
from test_replica_community import CommunityTests, ROOT


class LocalityTests(unittest.TestCase):
    def setUp(self):
        self.fixture=CommunityTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.b=self.fixture.b

    def test_unseen_entry_requires_complete_local_neighborhood(self):
        f,ids,m=self.fixture.graph({1:[2],2:[3],3:[4],4:[]})
        cfg=dict(self.b,community_hops=2,community_min_hops=2)
        r=native().replica_community_plan(f.fileno(),ids,[1],[1.],cfg,m,route_ids=[1,2,3,4])
        self.assertEqual(r['ids'],[1,2,3])
        self.assertEqual(r['entries'],[1,2])
        with self.assertRaisesRegex(ValueError,'complete local neighborhood'):
            native().replica_community_plan(f.fileno(),ids,[1],[1.],cfg,m,
                r['ids'],r['seeds'],2,route_ids=[1,2,3,4],verify_entry_ids=[3])

    def test_multiple_disconnected_localities_remain_distinct_and_bounded(self):
        f,ids,m=self.fixture.graph({1:[2],2:[1],10:[11],11:[10],20:[21],21:[20]})
        shard=SimpleNamespace(fd=f.fileno(),ids=ids,meta=dict(epoch='fixture'))
        cfg=copy.deepcopy(self.b)
        cfg.update(max_replicas=3,max_region_bytes=3000000,max_total_replica_bytes=5000000,
                   max_code_bytes=3000000,community_seed_starts=3,
                   locality=dict(enabled=True,source_capacity_fraction=.5,max_regions=3,
                                 per_region_fraction=.5,generalization_weight=.5,metadata_reserve_bytes=1048576))
        with patch('dhdgs.locality.source_index_bytes',return_value=10000000), \
             patch('dhdgs.locality.metadata',side_effect=lambda s:dict(m)), \
             patch('dhdgs.community.metadata',return_value=m):
            out=select_regions(shard,[1,10,20],[3.,2.,1.],[1,2,10,11,20,21],cfg)
        self.assertEqual(len(out['regions']),3)
        self.assertEqual([set(r['ids'])for r in out['regions']],[{1,2},{10,11},{20,21}])
        self.assertLessEqual(out['estimated_physical_bytes'],out['capacity_bytes'])
        self.assertEqual(out['estimated_physical_bytes'],sum(r['reserved_package_bytes']for r in out['regions']))
        entries=[v for r in out['regions']for v in r['entries']]
        self.assertEqual(len(entries),len(set(entries)))

    def test_source_total_includes_draining_and_all_targets(self):
        ctl=object.__new__(LoadBalancer)
        ctl.b=dict(max_region_bytes=600,max_total_replica_bytes=5000,max_replicas=4,
                   locality=dict(enabled=True,source_capacity_fraction=.5))
        ctl.c={'cluster':{'nodes':[{}, {}, {}]}}
        ctl.lock=threading.RLock()
        ctl.regions={'a':dict(source=0,target=1,bytes=400)}
        ctl.draining={'b':dict(source=0,target=2,bytes=400)}
        ctl.in_progress=None
        ctl.rpc=Mock(return_value={'source_index_bytes':2000})
        ctl.builder=Mock()
        p=dict(source=0,target=1,reserved_package_bytes=300)
        with self.assertRaisesRegex(ValueError,'Aggregate source'):
            ctl.create(p)
        ctl.builder.submit.assert_not_called()
        ctl.create(dict(p,reserved_package_bytes=200))
        ctl.builder.submit.assert_called_once()

    def test_expanded_portals_are_consumed_before_planning_the_next_region(self):
        f,ids,m=self.fixture.graph({1:[2],2:[3],3:[4],4:[5],5:[]})
        shard=SimpleNamespace(fd=f.fileno(),ids=ids,meta=dict(epoch='fixture'))
        cfg=copy.deepcopy(self.b)
        cfg.update(community_hot_closure=False,max_region_bytes=3000000,
            max_total_replica_bytes=5000000,max_code_bytes=3000000,max_replicas=3,
            locality=dict(enabled=True,source_capacity_fraction=.5,max_regions=3,
                per_region_fraction=1.,generalization_weight=0.,metadata_reserve_bytes=0))
        cfg['hop_expansion'].update(enabled=True,promote_covered_entries=True,
            core_fraction=.5,rounds=4,candidates=16,history_weight=1.)
        with patch('dhdgs.locality.source_index_bytes',return_value=10000000), \
             patch('dhdgs.locality.metadata',side_effect=lambda s:dict(m)), \
             patch('dhdgs.community.metadata',return_value=m), \
             patch('dhdgs.hop_expansion.planning_metadata',return_value=m):
            out=select_regions(shard,[1,3],[10.,1.],[1,3],cfg)
        self.assertEqual(len(out['regions']),1)
        self.assertEqual(out['regions'][0]['entries'],[1,3])
        self.assertEqual(out['remaining_hot_entries'],0)
        self.assertEqual(out['selected_routing_entries'],2)
        self.assertEqual(out['estimated_physical_bytes'],out['regions'][0]['reserved_package_bytes'])

    def test_capacity_fraction_has_strict_bounds(self):
        from dhdgs.config import validate
        c=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())
        for alpha in [0.,1.,-0.1,1.1]:
            c['load_balance']['locality']['source_capacity_fraction']=alpha
            with self.assertRaises(ValueError):validate(c)


if __name__=='__main__':unittest.main()
