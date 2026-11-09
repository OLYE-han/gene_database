import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from dhdgs.index import native
import test_replica_community as fixture

class HopCapacityTests(unittest.TestCase):
    def setUp(self):
        self.f=fixture.CommunityTests();self.f.setUp();self.addCleanup(self.f.doCleanups)

    def test_whole_ball_rejection_keeps_core_and_does_not_add_partial_layer(self):
        f,ids,meta=self.f.graph({1:[2],2:[3],3:[4,5],4:[],5:[],90:[91],91:[]})
        cfg=copy.deepcopy(self.f.b);cfg['max_region_rows']=4
        result=native().replica_hop_breadth_expand(f.fileno(),ids,[1,2],cfg,meta)
        self.assertEqual(result['ids'],[1,2])
        self.assertEqual(result['anchors'],[])
        self.assertGreater(result['stats']['rejections'],0)

    def test_capacity_includes_missing_neighbors_codes_and_shared_dependencies(self):
        f,ids,meta=self.f.graph({1:[2],2:[3],3:[4],4:[5],5:[]})
        cfg=copy.deepcopy(self.f.b)
        cfg['max_region_bytes']=meta['community_fixed_bytes']+4*(meta['record_bytes']+8)+5*meta['community_code_bytes']
        result=native().replica_hop_breadth_expand(f.fileno(),ids,[1,2],cfg,meta)
        self.assertEqual(result['ids'],[1,2,3,4])
        self.assertEqual(result['stats']['estimated_bytes'],cfg['max_region_bytes'])
        self.assertEqual(result['stats']['code_rows'],5)
        replay=native().replica_hop_expand(f.fileno(),ids,[1,2],[],[],cfg,meta,result['anchors'],True)
        self.assertEqual(replay['ids'],result['ids'])
        d=native().replica_hop_capacity_inspect(f.fileno(),ids,[1,2],result['ids'],[1,2,4],cfg,meta)
        self.assertEqual(d['complete_shells_beyond_core'],2)
        self.assertEqual(d['max_core_distance'],2)
        self.assertEqual(d['potential_safe_entries'],2)
        self.assertFalse(d['source_closure_complete'])

    def test_reachable_closure_and_code_bound(self):
        f,ids,meta=self.f.graph({1:[2,3],2:[4],3:[4],4:[],90:[91],91:[]})
        cfg=copy.deepcopy(self.f.b)
        result=native().replica_hop_breadth_expand(f.fileno(),ids,[1],cfg,meta)
        self.assertEqual(result['ids'],[1,2,3,4])
        self.assertEqual(result['stats']['code_rows'],4)
        d=native().replica_hop_capacity_inspect(f.fileno(),ids,[1],result['ids'],[1],cfg,meta)
        self.assertTrue(d['source_closure_complete'])
        cfg['max_code_bytes']=meta['community_fixed_bytes']+3*meta['community_code_bytes']
        result=native().replica_hop_breadth_expand(f.fileno(),ids,[1],cfg,meta)
        self.assertEqual(result['ids'],[1])

    def test_initial_core_cannot_exceed_rows_or_bytes(self):
        f,ids,meta=self.f.graph({1:[2],2:[]})
        cfg=copy.deepcopy(self.f.b);cfg['max_region_rows']=1
        with self.assertRaisesRegex(ValueError, 'row bound'):native().replica_hop_breadth_expand(f.fileno(),ids,[1,2],cfg,meta)
        cfg['max_region_rows']=2;cfg['max_region_bytes']=1
        with self.assertRaisesRegex(RuntimeError, 'byte budget'):native().replica_hop_breadth_expand(f.fileno(),ids,[1,2],cfg,meta)

    def test_new_historical_entries_require_complete_neighborhoods_and_replay(self):
        from dhdgs.hop_expansion import breadth_expand, capacity_diagnostics, verify_expansion, digest
        f,ids,meta=self.f.graph({1:[2],2:[3],3:[4],4:[5],5:[]})
        cfg=copy.deepcopy(self.f.b);cfg['max_region_rows']=4
        shard=SimpleNamespace(fd=f.fileno(),ids=ids,meta=dict(epoch='fixture'))
        core=dict(ids=[1,2],entries=[1],region_proof=dict(kind='complete_hot_entry_balls_v1',
            scope='source_local_records',epoch='fixture',hops=1,seeds=[1],hot_ids=[1],
            weights=[1.],hot_closure=False,members_sha256=digest([1,2])))
        with patch('dhdgs.community.metadata',return_value=meta), \
             patch('dhdgs.hop_expansion.planning_metadata',return_value=meta):
            out=breadth_expand(shard,core,cfg)
            out['region_proof']['routing_candidates']=[1,2,3,4,5]
            out['entries']=capacity_diagnostics(shard,out,cfg,[1,2,3,4,5])['safe_entries']
            self.assertEqual(out['entries'],[1,2,3])
            self.assertTrue(verify_expansion(shard,out,cfg)['verified'])
            bad=copy.deepcopy(out);bad['entries'].append(4)
            with self.assertRaisesRegex(ValueError,'complete local'):verify_expansion(shard,bad,cfg)
            bad=copy.deepcopy(out);bad['entries'].remove(1)
            with self.assertRaisesRegex(ValueError,'entry candidates'):verify_expansion(shard,bad,cfg)

    def test_discrete_cut_gap_completion_has_one_flat_verifiable_certificate(self):
        from dhdgs.hop_expansion import capacity_expand,verify_expansion,digest
        f,ids,meta=self.f.graph({1:[2],2:[3],3:[4],4:[5],5:[]})
        cfg=copy.deepcopy(self.f.b);cfg['max_region_rows']=4
        cfg['community_max_plan_ms']=60000
        cfg['hop_expansion'].update(history_weight=1.,candidates=2,rounds=2)
        shard=SimpleNamespace(fd=f.fileno(),ids=ids,meta=dict(epoch='fixture',record_bytes=meta['record_bytes']))
        core=dict(ids=[1,2],entries=[1],region_proof=dict(kind='complete_hot_entry_balls_v1',
            scope='source_local_records',epoch='fixture',hops=1,seeds=[1],hot_ids=[1],
            weights=[1.],hot_closure=False,members_sha256=digest([1,2])))
        with patch('dhdgs.community.metadata',return_value=meta), \
             patch('dhdgs.hop_expansion.planning_metadata',return_value=meta):
            out=capacity_expand(shard,core,cfg,{},[2,1])
            self.assertEqual(out['ids'],[1,2,3,4])
            self.assertEqual(out['stats']['breadth_fill_rows'],2)
            self.assertEqual(out['region_proof']['core_ids'],[1,2])
            self.assertTrue(verify_expansion(shard,out,cfg)['verified'])

    def test_rebinding_only_promotes_complete_portals_without_growing_membership(self):
        from dhdgs.hop_expansion import breadth_expand, rebind_historical_entries, digest
        f, ids, meta = self.f.graph({1:[2],2:[3],3:[4],4:[5],5:[]})
        cfg = copy.deepcopy(self.f.b); cfg['max_region_rows'] = 4
        shard = SimpleNamespace(fd=f.fileno(), ids=ids, meta=dict(epoch='fixture'))
        core = dict(ids=[1,2],entries=[1],region_proof=dict(kind='complete_hot_entry_balls_v1',
            scope='source_local_records',epoch='fixture',hops=1,seeds=[1],hot_ids=[1],
            weights=[1.],hot_closure=False,members_sha256=digest([1,2])))
        with patch('dhdgs.community.metadata',return_value=meta), \
             patch('dhdgs.hop_expansion.planning_metadata',return_value=meta):
            original = breadth_expand(shard,core,cfg)
            bound = rebind_historical_entries(shard,original,cfg,[2,3,4,5])
            self.assertEqual(bound['ids'],original['ids'])
            self.assertEqual(bound['region_proof']['anchors'],original['region_proof']['anchors'])
            self.assertEqual(bound['entries'],[1,2,3])
            self.assertEqual(original['entries'],[1])
            self.assertGreater(bound['reserved_package_bytes'],original['reserved_package_bytes'])
            with self.assertRaisesRegex(ValueError,'Invalid historical'):
                rebind_historical_entries(shard,original,cfg,[True])
            limited = copy.deepcopy(cfg)
            limited['max_region_bytes'] = original['reserved_package_bytes']
            with self.assertRaisesRegex(ValueError,'metadata exceeds'):
                rebind_historical_entries(shard,original,limited,[2,3])
