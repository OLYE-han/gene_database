from types import SimpleNamespace
from unittest.mock import patch
import unittest
from test_replica_community import CommunityTests
from dhdgs.community import seeded_core


class SeededCoreTests(unittest.TestCase):
    setUp = CommunityTests.setUp
    graph = CommunityTests.graph

    def build(self, adj, roots, weights, seeds=2, hops=2):
        f, ids, meta = self.graph(adj)
        meta['epoch'] = 'fixture'
        self.b['community_hops'] = hops
        shard = SimpleNamespace(fd=f.fileno(), ids=ids, meta=meta,
                                location={int(x): i for i, x in enumerate(ids)})
        with patch('dhdgs.community.metadata', return_value=meta):
            return seeded_core(shard, roots, weights, self.b, seed_count=seeds, hops=hops)

    def test_complete_multi_source_cycles_and_external_exit(self):
        graph = {1:[2], 2:[3,999], 3:[4], 4:[1], 10:[11], 11:[12], 12:[13], 13:[]}
        r = self.build(graph, [1,10], [2.,1.])
        self.assertEqual(r['ids'], [1,2,3,10,11,12])
        self.assertEqual(r['entries'], [1,10])
        self.assertEqual(r['region_proof']['hops'], 2)

    def test_order_by_work_and_never_truncate_complete_core(self):
        graph = {1:[2],2:[3],3:[],10:[11],11:[12],12:[]}
        self.assertEqual(self.build(graph,[1,10],[1.,2.],seeds=1)['entries'],[10])
        self.b['max_region_rows'] = 4
        with self.assertRaisesRegex(ValueError,'row budget'):
            self.build(graph,[1,10],[1.,2.])

    def test_dependency_bytes_checked_by_native_verifier(self):
        self.b['max_code_bytes'] = 270
        with self.assertRaisesRegex((ValueError,RuntimeError),'byte budget'):
            self.build({1:[2,999],2:[]}, [1], [1.], seeds=1, hops=1)
