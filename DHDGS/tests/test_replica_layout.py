import unittest
from dhdgs.replica_layout import partition_entries


class ReplicaLayoutTests(unittest.TestCase):
    def test_every_portal_once_and_duplicate_records_charged(self):
        balls={1:{1,2,3},2:{2,3,4},10:{10,11,12},11:{11,12,13}}
        r=partition_entries(balls,{x:1. for x in balls},{1:1.,2:1.})
        self.assertEqual(sorted(sum(r['groups'].values(),[])),sorted(balls))
        unions=[set().union(*(balls[x]for x in roots)) for roots in r['groups'].values()]
        self.assertEqual(r['duplicated_core_rows'],sum(map(len,unions))-len(set().union(*unions)))
        self.assertEqual(sorted(r['work'].values()),[2.,2.])

    def test_invalid_inputs_and_no_empty_physical_copy(self):
        with self.assertRaises(ValueError):partition_entries({1:{1}},{1:float('nan')},{1:1.})
        with self.assertRaises(ValueError):partition_entries({1:{1}},{1:1.},{1:1.,2:1.})


class PartitionBuildTests(unittest.TestCase):
    def test_complete_cores_and_physical_budget(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from test_replica_community import CommunityTests
        from dhdgs.replica_layout import build_partitioned_regions
        fixture=CommunityTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        f,ids,meta=fixture.graph({1:[2],2:[3,999],3:[1],10:[11],11:[12],12:[10]})
        meta['epoch']='fixture'
        shard=SimpleNamespace(fd=f.fileno(),ids=ids,meta=meta,location={int(x):i for i,x in enumerate(ids)})
        b=fixture.b;b.update(max_total_replica_bytes=1000000,community_hops=2,community_max_plan_ms=10000)
        layout=dict(total_alpha=.3,hops=2,target_weights={1:1.,2:1.},balance_weight=1.,deadline_s=10,max_edge_scans=10000)
        with patch('dhdgs.locality.source_index_bytes',return_value=3000000),patch('dhdgs.community.metadata',side_effect=lambda _:dict(meta)),patch('dhdgs.hop_expansion.metadata',side_effect=lambda _:dict(meta)):
            plans,info=build_partitioned_regions(shard,[1,10],[1.,1.],b,layout)
        self.assertEqual(sorted(e for p in plans for e in p['entries']),[1,10])
        self.assertLessEqual(info['reserved_bytes'],900000)
        for p in plans:
            core=set(p['region_proof']['core_ids'])
            self.assertTrue({1,2,3}<=core or {10,11,12}<=core)
            self.assertNotIn(999,p['ids'])
        with patch('dhdgs.locality.source_index_bytes',return_value=100):
            with self.assertRaisesRegex(ValueError,'capacity'):
                build_partitioned_regions(shard,[1,10],[1.,1.],b,layout)
