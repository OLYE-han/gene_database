"""Extra historical portals must not change the graph or bypass proof/budget."""
import copy
import unittest

from test_automatic_replica import BudgetAndControlTests
from test_partition_pipeline import PartitionPipelineTests
from dhdgs.automatic_replica import prepare
from dhdgs.replica_layout import build_partitioned_regions


class HistoricalEntryTests(unittest.TestCase):
    def test_controller_keeps_seeds_and_capacity_with_larger_portal_pool(self):
        f=BudgetAndControlTests();c=f.controller()
        c.b['automatic_pipeline'].update(max_targets=1,max_seed_entries=2)
        original=prepare(c,1,2,f.observations())
        c.b['automatic_pipeline']['max_entry_candidates']=4
        promoted=prepare(c,1,2,f.observations())
        self.assertEqual(original[0]['hot_ids'],promoted[0]['hot_ids'])
        self.assertEqual(original[0]['weights'],promoted[0]['weights'])
        self.assertEqual(original[1]['capacity_bytes'],promoted[1]['capacity_bytes'])
        self.assertEqual(promoted[1]['historical_entries'],[0,1,2,3])
        self.assertEqual(promoted[0]['training_query_ids'],[0,1,2,3])

    def test_extra_entry_requires_complete_existing_neighborhood(self):
        f=PartitionPipelineTests();f.setUp();self.addCleanup(f.doCleanups)
        cfg=copy.deepcopy(f.layout);cfg['target_weights']={1:1.};cfg['hops']=1
        # Disable optional exterior growth so boundary 2 lacks its neighbor 3.
        b=dict(f.b,expansion_soft_ms=0)
        base,_=build_partitioned_regions(f.shard,[1],[1.],b,cfg)
        cfg['historical_entries']=[1,2,4,10]
        promoted,report=build_partitioned_regions(f.shard,[1],[1.],b,cfg)
        self.assertEqual(base[0]['ids'],promoted[0]['ids'])
        self.assertEqual(base[0]['region_proof']['core_ids'],promoted[0]['region_proof']['core_ids'])
        self.assertEqual(promoted[0]['entries'],[1])
        self.assertLessEqual(promoted[0]['reserved_package_bytes'],report['budget_bytes'])

    def test_covered_entries_are_added_without_adding_records(self):
        f=PartitionPipelineTests();f.setUp();self.addCleanup(f.doCleanups)
        cfg=copy.deepcopy(f.layout);cfg['target_weights']={1:1.}
        base,_=build_partitioned_regions(f.shard,[1],[1.],f.b,cfg)
        cfg['historical_entries']=[1,2,3,4,10]
        promoted,report=build_partitioned_regions(f.shard,[1],[1.],f.b,cfg)
        self.assertEqual(base[0]['ids'],promoted[0]['ids'])
        self.assertEqual(promoted[0]['entries'],[1,2,3,4])
        self.assertNotIn(10,promoted[0]['entries'])
        self.assertGreaterEqual(promoted[0]['reserved_package_bytes'],base[0]['reserved_package_bytes'])
        self.assertEqual(report['phases'][0]['added_entries'],3)

    def test_invalid_candidates_and_multitarget_promotion_rejected(self):
        f=PartitionPipelineTests();f.setUp();self.addCleanup(f.doCleanups)
        for values in ([1,1], [999], [1,True], [], [1,2]):
            cfg=copy.deepcopy(f.layout);cfg['historical_entries']=values
            with self.assertRaisesRegex(ValueError,'historical entry'):
                build_partitioned_regions(f.shard,[1,10],[1.,1.],f.b,cfg)
