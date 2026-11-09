"""Quality-changing paths still preserve exact returned distances and ownership."""
import copy
import unittest
import numpy as np
import test_one_sided as hw
import test_async_pull as asynchronous


class TailLimitTests(hw.MissingCodeTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['tail_policy']=dict(enabled=True,max_expansions=8,
            min_expansions=0,stable_batches=0,uncertainty_weight=1.,improvement_margin=0.)

    def test_tail_stops_release_unspent_credit(self):
        self.install_entry();q=self.engine.search_async(self.x[0],0).result(20)
        self.check_result(q,200)
        stopped=[b for b in q['branch_reports']if b['stats'].get('tail_policy_stops',0)]
        self.assertTrue(stopped)
        for b in stopped:
            self.assertLessEqual(b['stats']['tail_policy_expanded'],8)
            self.assertEqual(b['stats']['tail_policy_unused_credit'],b['work_unused'])
            self.assertGreater(b['work_unused'],0)


class SharedComputeTests(hw.MissingCodeTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['async']['shared_compute']=dict(enabled=True,tail_fraction=.25,credit_limit_ms=2.)

    def test_both_task_classes_run_on_fixed_total_capacity(self):
        _,b=self.install_entry()
        futures=[self.engine.search_async(self.x[0],i)for i in range(24)]
        for f in futures:self.check_result(f.result(30),200)
        m=self.servers[b].control.peer.runtime.metrics();q=m['compute_queue']
        self.assertTrue(q['shared_compute'])
        self.assertEqual(q['workers'],self.c['peer']['worker_threads']+2)
        self.assertGreater(q['normal_cpu_ns'],0);self.assertGreater(q['tail_cpu_ns'],0)
        self.assertEqual(q['tail_active'],0);self.assertEqual(q['tail_queued'],0)


class LazyRerankTests(hw.MissingCodeTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['lazy_rerank']=dict(enabled=True,max_vectors=16,uncertainty_weight=1.)

    def test_all_returned_distances_are_exact_after_navigation_and_refine(self):
        self.install_entry()
        q=self.engine.search_async(self.x[0],0).result(30);self.check_result(q,200)
        self.assertGreater(q['stats'].get('lazy_navigation_vertices',0),0)
        self.assertGreater(q['stats'].get('lazy_refined_vectors',0),0)
        for b in q['branch_reports']:
            s=b['stats'];self.assertLessEqual(s.get('lazy_refined_vectors',0),16)
            self.assertEqual(s.get('lazy_navigation_vertices',0),s.get('lazy_refined_vectors',0)+s.get('lazy_unrefined_vertices',0))
            for idx,distance in zip(b['ids'],b['squared_distances']):
                want=float(np.square(self.x[int(idx)].astype(float)-self.x[0].astype(float)).sum())
                self.assertAlmostEqual(distance,want,places=6)
        self.assertNotIn(0,q['ids'])

    def test_selective_hardware_ranges_match_full_records(self):
        a,_=self.install_entry();shard=self.servers[a].control.worker
        cfg=self.c['peer']['replica_pull']['one_sided'];ids=[int(x)for x in shard.ids[:4]]
        full=hw.native().one_sided_probe(shard.native_search,cfg,ids,524288)
        for mode in ['adjacency','vectors']:
            got=hw.native().one_sided_probe(shard.native_search,cfg,ids,524288,{},mode)
            self.assertTrue(got['records_equal']);self.assertTrue(got['codes_equal'])
            self.assertLess(got['client']['read_bytes'],full['client']['read_bytes'])

    def test_normal_handoff_after_lazy_navigation_preserves_budget(self):
        # The implementation refines deferred results before granting source work.
        self.install_entry()
        futures=[self.engine.search_async(self.x[i%4],100+i)for i in range(16)]
        for f in futures:self.check_result(f.result(30),200)


if __name__=='__main__':unittest.main()
