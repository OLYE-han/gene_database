import copy
import os
from pathlib import Path
import struct
import tempfile
import unittest
import numpy as np
import yaml
from dhdgs.index import native
from dhdgs.config import validate
from dhdgs.community import hotspots

ROOT=Path(__file__).resolve().parents[1]


class CommunityWorkerTests(unittest.TestCase):
    def test_pending_job_is_bounded_idempotent_and_does_not_block_polling(self):
        import threading
        from unittest.mock import patch
        from dhdgs.community import PlanWorker
        entered, release = threading.Event(), threading.Event()
        def slow(*args):
            entered.set()
            if not release.wait(10):raise RuntimeError('test worker timed out')
            return dict(ids=[1,2])
        with patch('dhdgs.community.select', side_effect=slow) as select:
            worker=PlanWorker(None,dict(community_top_k=2))
            try:
                worker.start('one',[1],[1.]);self.assertTrue(entered.wait(2))
                self.assertFalse(worker.poll('one')['done'])
                self.assertTrue(worker.start('one',[1],[1.])['accepted'])
                with self.assertRaisesRegex(ValueError,'conflict'):worker.start('one',[2],[1.])
                with self.assertRaisesRegex(RuntimeError,'busy'):worker.start('two',[2],[1.])
                with self.assertRaisesRegex(ValueError,'Unknown'):worker.poll('missing')
                release.set();worker.future.result(2)
                self.assertEqual(worker.poll('one')['region'],dict(ids=[1,2]))
                self.assertEqual(worker.poll('one'),worker.poll('one'))
                self.assertEqual(select.call_count,1)
                worker.start('two',[2],[1.]);worker.future.result(2)
                with self.assertRaisesRegex(ValueError,'Unknown'):worker.poll('one')
            finally:
                release.set();worker.close()
            with self.assertRaisesRegex(RuntimeError,'closed'):worker.start('three',[1],[1.])

    def test_failure_is_preserved_until_replacement(self):
        from unittest.mock import patch
        from dhdgs.community import PlanWorker
        with patch('dhdgs.community.select',side_effect=ValueError('no complete region')):
            worker=PlanWorker(None,dict(community_top_k=2))
            try:
                worker.start('failed',[1],[1.])
                with self.assertRaisesRegex(ValueError,'no complete region'):worker.future.result(2)
                for _ in range(2):
                    with self.assertRaisesRegex(ValueError,'no complete region'):worker.poll('failed')
            finally:worker.close()

class CommunityTests(unittest.TestCase):
    def setUp(self):
        self.b=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())['load_balance']
        self.b.update(community_hops=1,community_seed_starts=1,community_ppr_rounds=0,
            community_max_plan_ms=5000,max_region_rows=100,max_region_bytes=1000000,max_code_bytes=500000)
    def graph(self,adj):
        f=tempfile.TemporaryFile();self.addCleanup(f.close)
        ids=np.array(list(adj),dtype='uint64');degree=max(map(len,adj.values()),default=0)
        stride=4+4+8*degree
        for v,neighbors in adj.items():
            f.write(struct.pack('<fI',0.,len(neighbors)))
            f.write(np.array(neighbors,dtype='<u8').tobytes());f.write(bytes(8*(degree-len(neighbors))))
        f.flush()
        return f,ids,dict(dimension=1,record_bytes=stride,header_bytes=0,max_degree=degree,community_code_bytes=32,community_fixed_bytes=256)
    def run_graph(self,adj,hot,**kwargs):
        f,ids,m=self.graph(adj)
        return native().replica_community_plan(f.fileno(),ids,hot,[1.]*len(hot),self.b,m,**kwargs)
    def test_hot_boundary_resets_depth_and_prior_expansion_continues(self):
        r=self.run_graph({1:[2,3],2:[4],3:[5],4:[6],5:[],6:[]},[1,2])
        self.assertEqual(r['ids'],[1,2,3,4]);self.assertEqual(r['seeds'],[1,2])
    def test_multiple_depth_arrivals_and_cycles(self):
        self.b['community_hops']=2
        adj={1:[2,3],2:[4],3:[5],4:[6],5:[4],6:[7],7:[1]}
        r=self.run_graph(adj,[1,5])
        self.assertEqual(r['ids'],[1,2,3,4,5,6]);self.assertEqual(r['hops'],2)
    def test_no_partial_hot_closure_under_capacity(self):
        self.b['max_region_rows']=3
        with self.assertRaisesRegex(ValueError,'No complete community'):
            self.run_graph({1:[2],2:[3],3:[4],4:[]},[1,2,3])
    def test_missing_interior_record_rejected_independently(self):
        with self.assertRaisesRegex(ValueError,'completeness'):
            self.run_graph({1:[2],2:[3],3:[]},[1,2],verify_ids=[1,2],verify_seeds=[1],verify_hops=1)
    def test_disconnected_complete_balls_rejected_without_reachable_entry(self):
        with self.assertRaisesRegex(ValueError,'unreachable from routing entries'):
            self.run_graph({1:[2],2:[],5:[6],6:[]},[1],
                verify_ids=[1,2,5,6],verify_seeds=[1,5],verify_hops=1)
    def test_external_neighbors_are_exits_not_fake_local_members(self):
        r=self.run_graph({1:[2,999],2:[]},[1])
        self.assertEqual(r['ids'],[1,2]);self.assertEqual(r['stats']['exit_edges'],1)
        self.assertEqual(r['stats']['code_rows'],3)
    def test_ppr_expansion_keeps_complete_ball_and_out_mass(self):
        self.b.update(community_ppr_rounds=1,community_byte_price=0.)
        r=self.run_graph({1:[2],2:[3,999],3:[2]},[1])
        self.assertEqual(r['ids'],[1,2,3]);self.assertGreater(r['stats']['last_ppr_out_mass'],0.)
        self.run_graph({1:[2],2:[3,999],3:[2]},[1],verify_ids=r['ids'],verify_seeds=r['seeds'],verify_hops=r['hops'])
    def test_byte_budget_includes_external_codes(self):
        self.b['max_code_bytes']=270
        with self.assertRaisesRegex(ValueError,'No complete community'):
            self.run_graph({1:[2,999],2:[]},[1])
    def test_nonpositive_structural_proxy_does_not_create_replica(self):
        self.b['community_byte_price']=1e9
        with self.assertRaisesRegex(ValueError,'No positive community'):
            self.run_graph({1:[2],2:[]},[1])
    def test_core_comparison_and_winning_expansion_are_reported(self):
        self.b.update(community_seed_starts=2,community_ppr_rounds=1,community_byte_price=0.)
        r=self.run_graph({1:[2],2:[3,999],3:[2],10:[11],11:[12,998],12:[11]},[1,10])
        self.assertEqual(r['stats']['completed_cores'],2)
        self.assertEqual(r['stats']['selected_core_rows'],2)
        self.assertEqual(r['stats']['selected_ppr_added_balls'],1)
    def test_sparse_uint64_identity(self):
        x=2**63+13;r=self.run_graph({x:[x+1],x+1:[]},[x]);self.assertEqual(r['ids'],[x,x+1])
    def test_no_hotness_multiplication_from_duplicate_vertex(self):
        q=dict(source=0,replica=None,query_id=1,entry=1,branches=[dict(node=0,cpu_ms=2.,expanded_ids=[1])])
        ranked,groups=hotspots([q,q],0,dict(self.b,min_entry_queries=1))
        self.assertEqual(ranked,[(1,4.)]) # two occurrences, even with same vector ID
    def test_invalid_runtime_combo_rejected(self):
        c=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text());b=c['load_balance']
        b.update(selection_policy='hot_entry_community',planner_mode='native_async',halo_hops=1)
        with self.assertRaisesRegex(ValueError,'halo'):validate(c)


if __name__=='__main__':unittest.main()

from test_replicas import ReplicaTests
class CommunityRuntimeTests(unittest.TestCase):
    setUpClass=classmethod(ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(ReplicaTests.tearDownClass.__func__)
    setUp=ReplicaTests.setUp
    info=ReplicaTests.info
    rpc=ReplicaTests.rpc
    def configure_replica_test(self,c):
        c['load_balance'].update(selection_policy='hot_entry_community',planner_mode='native_async',
            halo_hops=0,portal_budget_fraction=0.,community_hops=1,community_ppr_rounds=1,
            community_max_plan_ms=10000,publication_mode='ready',quality_weight_routing=False,
            compute_region_quality=False,peer_routes_enabled=False,native_replica_pack=True,
            replica_transfer_mode='pipeline_v2',replica_loader='native',warm_bytes=0,
            initial_fraction=1.,max_fraction=1.)
    def test_source_plan_verified_pipeline_and_native_search(self):
        import time
        info=self.info();source=info['source'];target=(source+1)%3
        self.rpc(source,'community_plan_start',job='integration1',hot_ids=[info['entry_id']],weights=[1.])
        deadline=time.monotonic()+15
        while True:
            response=self.rpc(source,'community_plan_poll',job='integration1')
            if response['done']:
                region=response['region'];break
            self.assertLess(time.monotonic(),deadline);time.sleep(.01)
        plan=dict(region,source=source,target=target,training_query_ids=[],min_score=-1e300,max_score=1e300)
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'community')
        controller.stop.set();controller.thread.join();controller.stop.clear()
        controller.loads={n:dict(updated=time.monotonic(),usable=True)for n in (source,target)}
        version=controller.create(plan).result(20)
        state=self.rpc(target,'status',id=version)
        self.assertEqual(state['phase'],'READY')
        result=self.engine.search_async(self.x[0],0).result(10)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(result['stats'].get('replica_version'),version)
        controller.retire()
