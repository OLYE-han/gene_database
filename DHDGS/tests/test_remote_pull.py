"""Real RDMA, ordinary regions, no installed replicas."""
import copy
import threading
import time
from pathlib import Path
import unittest
import yaml
from test_replicas import ReplicaTests
from test_region_tasks import RegionRuntimeTests, mode
from test_one_sided import active_ib, enable


@unittest.skipUnless(active_ib(), 'Requires an active InfiniBand verbs device')
class RemotePullTests(unittest.TestCase):
    setUpClass=classmethod(ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(ReplicaTests.tearDownClass.__func__)
    setUp=ReplicaTests.setUp
    check_result=RegionRuntimeTests.check_result

    def configure_replica_test(self,c):
        mode(c)
        c['load_balance']['enabled']=False
        c['search'].update(max_expansions=200,local_beam=11)
        c['peer']['remote_pull']=dict(enabled=True,pull_enabled=True,switch_after_batches=2)
        c['peer']['replica_pull']=copy.deepcopy(yaml.safe_load((Path(__file__).resolve().parents[1]/'configs/dhdgs.yaml').read_text())['peer']['replica_pull'])
        p=c['peer']['replica_pull']
        p.update(enabled=True,require_rdma=False,max_remaining=1,max_rounds=64,
                 batch_records=1,batch_bytes=524288,max_bytes=32*524288)
        p['async'].update(enabled=True,tail_threads=0,max_contexts=64,max_inflight_per_source=8,
                          response_bytes=64*524288,source_read_threads=0,target_nodes=[],source_nodes=[])
        p['async']['shared_compute']=dict(enabled=True,tail_fraction=.5,credit_limit_ms=2.)
        enable(c)
        p['one_sided'].update(doorbell_batch=True,missing_codes_only=True)

    def test_real_reads_without_replicas_and_conserved_budget(self):
        rows=[self.engine.search_async(self.x[i],i).result(20) for i in range(12)]
        for q in rows:
            self.check_result(q,200)
            self.assertTrue(all('replica_version' not in b for b in q['branch_reports']))
            for b in q['branch_reports']:
                self.assertLessEqual(b['stats'].get('remote_pull_max_streak',0),2)
                self.assertEqual(b['work_spent']+b['work_unused']+sum(x['work_grant'] for x in b['children']),b['work_grant'])
        self.assertGreater(sum(q['stats'].get('one_sided_read_ops',0)for q in rows),0)
        self.assertGreater(sum(q['stats'].get('pull_expanded',0)for q in rows),0)
        for server in self.servers:
            metrics=server.control.peer.runtime.metrics()
            self.assertEqual(metrics['pull_scheduler']['tail_workers'],0)
            self.assertEqual(metrics['pull_scheduler']['one_sided_client']['errors'],0)

    def test_concurrent_queries_drain_and_reuse_fixed_pool(self):
        futures=[self.engine.search_async(self.x[i%12],i%12)for i in range(48)]
        for f in futures:self.check_result(f.result(30),200)
        for server in self.servers:
            s=server.control.peer.runtime.metrics()['pull_scheduler']
            self.assertEqual(s['contexts'],0)
            self.assertEqual(s['pending'],0)
            self.assertEqual(sum(s['inflight']),0)

    def test_invalid_options_rejected(self):
        from dhdgs.config import validate
        base={k:v for k,v in self.c.items() if not k.startswith('_')}
        for patch in [dict(switch_after_batches=0),dict(switch_after_batches=1025)]:
            c=copy.deepcopy(base);c['peer']['remote_pull'].update(patch)
            with self.assertRaisesRegex(ValueError,'remote_pull'):validate(c)
        c=copy.deepcopy(base);c['peer']['replica_pull']['async']['enabled']=False
        with self.assertRaisesRegex(ValueError,'remote_pull|shared compute'):validate(c)


@unittest.skipUnless(active_ib(), 'Requires an active InfiniBand verbs device')
class DirectPushTests(RemotePullTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['remote_pull']['pull_enabled']=False

    def test_real_reads_without_replicas_and_conserved_budget(self):
        rows=[self.engine.search_async(self.x[i],i).result(20)for i in range(8)]
        for q in rows:
            self.check_result(q,200)
            self.assertEqual(q['stats'].get('pull_expanded',0),0)
        self.assertGreater(sum(q['stats'].get('remote_first_pushes',0)for q in rows),0)


@unittest.skipUnless(active_ib(), 'Requires an active InfiniBand verbs device')
class ParallelIoTests(RemotePullTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['one_sided']['io_threads']=2
        c['peer']['replica_pull']['async']['dedicated_dispatcher']=True

    def test_each_engine_progresses_and_total_workers_stay_fixed(self):
        self.test_concurrent_queries_drain_and_reuse_fixed_pool()
        for server in self.servers:
            metrics=server.control.peer.runtime.metrics()
            p=metrics['pull_scheduler'];self.assertEqual(p['main_workers'],self.c['peer']['worker_threads'])
            self.assertEqual(p['one_sided_client']['io_threads'],2)
            self.assertEqual(len(p['one_sided_client']['engines']),2)
        active=[s.control.peer.runtime.metrics()['pull_scheduler']['one_sided_client']for s in self.servers]
        self.assertTrue(any(all(e['completed_batches']>0 for e in m['engines'])for m in active))

    def test_close_drains_fifo_and_each_read_engine(self):
        tasks=[self.engine.search_async(self.x[i%12],i%12)for i in range(256)]
        runtimes=[s.control.peer.runtime for s in self.servers]
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            if any(r.metrics()['pull_scheduler']['contexts'] for r in runtimes):break
            time.sleep(.001)
        else:self.fail('Expected an outstanding pull before cancellation')
        closers=[threading.Thread(target=r.close,daemon=True)for r in runtimes]
        for t in closers:t.start()
        for t in closers:t.join(10)
        self.assertFalse(any(t.is_alive()for t in closers),'Close stranded a FIFO or CQ continuation')
        self.engine.close()
        self.assertTrue(all(t.done()for t in tasks))
        for runtime in runtimes:
            p=runtime.metrics()['pull_scheduler']
            self.assertEqual(p['contexts']+p['pending']+sum(p['inflight']),0)
            self.assertEqual(p['compute']['active']+p['compute']['queued'],0)
            self.assertEqual(p['one_sided_client']['pending'],0)

if __name__=='__main__':unittest.main()
