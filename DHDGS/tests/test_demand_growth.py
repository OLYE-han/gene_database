"""Actual native search + RNIC integration; performance is measured separately."""
import copy
import time
import unittest
import uuid
import tempfile
import gc
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import yaml
from dhdgs.index import native
import test_replicas as replicas
ROOT=replicas.ROOT
import test_region_tasks as regions
from test_one_sided import enable, active_ib


class DemandCacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.config=copy.deepcopy(yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())['peer']['demand_growth'])
        self.config.update(cache_records=4,promotion_hot_vertices=2,growth_fraction=.1)
        self.graph={'0':[1,2,3,4,5,6,7],**{str(i):[]for i in range(1,8)}}

    def make(self,graph=None,source_bytes=1000000):
        return native().DemandGrowthTestStore(self.config,str(Path(self.tmp.name)/'growth.bin'),graph or self.graph,[0],source_bytes)

    def wait(self,store,predicate):
        end=time.monotonic()+3
        while time.monotonic()<end:
            m=store.metrics()
            if predicate(m):return m
            time.sleep(.01)
        self.fail(str(store.metrics()))

    def fill(self,p):
        self.assertTrue(p.request([1,2],[[0,1],[0,2]]));p.touch([1,2],2)
        self.assertTrue(p.request([3,4],[[0,3],[0,4]]));p.touch([3,4],1)

    def test_strict_keeps_hot_cache_and_replaces_singletons(self):
        p=self.make();self.fill(p);time.sleep(.04)
        self.assertEqual(p.metrics()['promotion_batches'],0)
        self.assertTrue(p.request([5],[[0,5]]));self.assertTrue(p.contains(1));self.assertTrue(p.contains(2))
        self.assertEqual(p.metrics()['evictions'],1)

    def test_strict_promotes_when_all_records_are_repeated(self):
        p=self.make();self.fill(p);p.touch([3,4],1)
        m=self.wait(p,lambda m:m['promotion_batches']>0)
        self.assertGreaterEqual(m['durable_records'],2);self.assertFalse(m['error'])

    def test_hot_batch_persists_without_waiting_for_singletons(self):
        self.config['promotion_trigger']='full_hot_batch';p=self.make();self.fill(p)
        m=self.wait(p,lambda m:m['promotion_batches']>0)
        self.assertEqual(m['hot_vertices'],2);self.assertFalse(m['error'])

    def test_connector_is_paid_and_durable_bytes_survive_reopen(self):
        self.config.update(cache_records=3,promotion_trigger='full_hot_batch')
        graph={'0':[5],'5':[1,2],'1':[],'2':[]};p=self.make(graph)
        self.assertTrue(p.request([5,1,2],[[0,5],[0,5,1],[0,5,2]]));p.touch([5],1);p.touch([1,2],2)
        m=self.wait(p,lambda m:m['promotion_batches']>0)
        self.assertEqual(m['durable_records'],3);self.assertEqual(m['connector_vertices'],1)
        self.assertGreater(m['committed_bytes'],3*4096)
        before=p.record(1);p.seal();del p;gc.collect()
        p=self.make(graph);self.assertEqual(p.record(1),before);self.assertTrue(p.contains(5))
        self.assertEqual(p.metrics()['committed_bytes'],m['committed_bytes'])

    def test_uncommitted_tail_is_discarded_on_recovery(self):
        self.config.update(cache_records=2,promotion_trigger='full_hot_batch')
        p=self.make();self.assertTrue(p.request([1,2],[[0,1],[0,2]]));p.touch([1,2],2)
        m=self.wait(p,lambda m:m['promotion_batches']==1);del p;gc.collect()
        path=Path(self.tmp.name)/'growth.bin'
        with path.open('r+b')as f:f.truncate(m['committed_bytes']-4)
        p=self.make();self.assertEqual(p.metrics()['durable_records'],0)
        self.assertEqual(p.metrics()['committed_bytes'],64)

    def test_corrupt_committed_batch_is_rejected(self):
        self.config.update(cache_records=2,promotion_trigger='full_hot_batch')
        p=self.make();self.assertTrue(p.request([1,2],[[0,1],[0,2]]));p.touch([1,2],2)
        self.wait(p,lambda m:m['promotion_batches']==1);del p;gc.collect()
        path=Path(self.tmp.name)/'growth.bin'
        with path.open('r+b')as f:
            f.seek(112);value=f.read(1);f.seek(112);f.write(bytes([value[0]^1]))
        with self.assertRaisesRegex(RuntimeError,'Corrupt'):self.make()

    def test_journal_cannot_be_reopened_on_a_different_target(self):
        self.config['target_identity']=1;p=self.make();del p;gc.collect()
        self.config['target_identity']=2
        with self.assertRaisesRegex(RuntimeError,'identity'):self.make()

    def test_pinned_singleton_is_not_replaced(self):
        self.config.update(cache_records=1,promotion_hot_vertices=1,persist=False);p=self.make()
        self.assertTrue(p.request([1],[[0,1]]));p.touch([1],1);p.hold(1)
        self.assertFalse(p.request([2],[[0,2]]));self.assertTrue(p.contains(1))
        p.release();self.assertTrue(p.request([2],[[0,2]]))

    def test_small_last_commit_never_exceeds_beta(self):
        self.config.update(cache_records=2,promotion_trigger='full_hot_batch');p=self.make(source_bytes=70000)
        self.assertTrue(p.request([1,2],[[0,1],[0,2]]));p.touch([1,2],2)
        m=self.wait(p,lambda m:not m['learning'])
        self.assertEqual(m['durable_records'],1);self.assertLessEqual(m['committed_bytes'],7000)

    def test_invalid_directed_path_does_not_publish(self):
        self.config.update(cache_records=1,promotion_hot_vertices=1);p=self.make({'0':[5],'5':[1],'1':[]})
        self.assertTrue(p.request([1],[[0,1]]));p.touch([1],2)
        m=self.wait(p,lambda m:bool(m['error']))
        self.assertIn('edge',m['error']);self.assertEqual(m['durable_records'],0)

    def test_freeze_releases_temporary_cache_and_forbids_reads(self):
        self.config['persist']=False;p=self.make();self.fill(p);p.seal()
        self.wait(p,lambda m:m['cache_records']==0)
        self.assertFalse(p.request([1],[[0,1]]));self.assertFalse(p.contains(1))

    def test_parallel_hits_do_not_overflow_slots(self):
        self.config['persist']=False;p=self.make();self.assertTrue(p.request([1],[[0,1]]))
        with ThreadPoolExecutor(8)as pool:list(pool.map(lambda _:p.touch([1],10),range(64)))
        self.assertEqual(p.metrics()['expanded'],640);self.assertEqual(p.metrics()['cache_records'],1)

    def test_promotion_while_rank_counters_change(self):
        # The old live-atomic comparator can violate std::sort's ordering
        # contract. Exercise real concurrent expansions and journal commits,
        # including strict and early promotion, not a mock ranking function.
        ids=list(range(1,101));graph={'0':ids,**{str(i):[] for i in ids}}
        for trigger in ['full_no_singleton','full_hot_batch']:
            for repeat in range(4):
                with self.subTest(trigger=trigger,repeat=repeat):
                    cfg=dict(self.config,cache_records=100,promotion_hot_vertices=20,
                             promotion_trigger=trigger)
                    p=native().DemandGrowthTestStore(cfg,
                        str(Path(self.tmp.name)/f'{trigger}_{repeat}.bin'),graph,[0],30000000)
                    self.assertTrue(p.request(ids,[[0,i]for i in ids]))
                    gate=threading.Barrier(9)
                    def touch(worker):
                        order=ids.copy();random.Random(worker+repeat*100).shuffle(order)
                        gate.wait()
                        for _ in range(20):p.touch(order,100)
                    with ThreadPoolExecutor(8)as pool:
                        futures=[pool.submit(touch,i)for i in range(8)];gate.wait()
                        for f in futures:f.result()
                    m=self.wait(p,lambda m:m['promotion_batches']>0)
                    self.assertFalse(m['error'])
                    self.assertEqual(m['expanded'],8*20*100*100)
                    self.assertLessEqual(m['occupancy_peak'],100)
                    self.assertLessEqual(m['committed_bytes'],m['reserved_bytes'])
                    self.assertEqual(m['hot_vertices']+m['connector_vertices'],m['durable_records'])
                    p.seal();del p;gc.collect()



class DemandPipelineTests(DemandCacheTests):
    """Same journal/quality invariants through the new indexed, pipelined path."""
    def setUp(self):
        super().setUp()
        self.config.update(indexed_directory=True,persistence_pipeline=True,
            persistence_queue_batches=2,persistence_queue_bytes=134217728,
            maintenance_interval_ms=2,lock_sample_every=1)

    def test_stalled_writer_does_not_block_reads_and_prepares_next_batch(self):
        self.config.update(cache_records=4,promotion_trigger='full_hot_batch')
        p=self.make();p.pause_writer(True)
        try:
            self.assertTrue(p.request([1,2,3,4],[[0,i]for i in range(1,5)]))
            p.touch([1,2,3,4],2)
            m=self.wait(p,lambda m:m['prepared_batches']==2)
            self.assertTrue(p.writer_entered());self.assertEqual(m['durable_records'],0)
            self.assertEqual(m['persistence_queue_peak'],2)
            # Completion while the writer is blocked is the invariant: no
            # fragile sub-millisecond speed assertion or disk-speed assumption.
            with ThreadPoolExecutor(4)as pool:
                jobs=[pool.submit(p.touch,[i],1000)for i in range(1,5)]
                for job in jobs:job.result(2)
            for i in range(1,5):self.assertEqual(len(p.record(i)),4096)
            self.assertEqual(p.metrics()['promotion_batches'],0)
            p.seal() # Accepted batches must still drain after seal.
        finally:p.pause_writer(False)
        m=self.wait(p,lambda m:m['promotion_batches']==2 and not m['promoting'])
        self.assertEqual(m['durable_records'],4);self.assertFalse(m['error'])
        p.close();del p;gc.collect()
        p=self.make();self.assertEqual(p.metrics()['durable_records'],4)

    def test_queued_closures_share_connector_once_and_recover(self):
        self.config.update(cache_records=5,promotion_trigger='full_hot_batch')
        graph={'0':[5],'5':[1,2,3,4],**{str(i):[]for i in range(1,5)}}
        p=self.make(graph);p.pause_writer(True)
        try:
            self.assertTrue(p.request([5,1,2,3,4],[[0,5]]+[[0,5,i]for i in range(1,5)]))
            p.touch([5],1);p.touch([1,2,3,4],2)
            self.wait(p,lambda m:m['prepared_batches']==2)
            self.assertEqual(p.metrics()['durable_records'],0)
        finally:p.pause_writer(False)
        m=self.wait(p,lambda m:m['promotion_batches']==2 and not m['promoting'])
        self.assertEqual(m['durable_records'],5)
        self.assertEqual((m['hot_vertices'],m['connector_vertices']),(4,1))
        before={i:p.record(i)for i in range(1,6)}
        p.close();del p;gc.collect();p=self.make(graph)
        self.assertEqual(p.metrics()['durable_records'],5)
        for i in before:self.assertEqual(p.record(i),before[i])

    def test_negative_filter_never_hides_admitted_or_persisted_records(self):
        self.config.update(persist=False)
        p=self.make();self.assertFalse(p.contains(1))
        self.assertTrue(p.request([1,2],[[0,1],[0,2]]))
        self.assertTrue(p.contains(1));self.assertTrue(p.contains(2))
        p.hold(1);p.seal();self.assertFalse(p.contains(1));p.release()
        self.wait(p,lambda m:m['cache_records']==0)

    def test_new_cache_admission_of_pending_connector_is_not_promoted_twice(self):
        self.config.update(cache_records=6,promotion_trigger='full_hot_batch',persistence_queue_batches=3)
        graph={'0':[5,6,7],'5':[1,2,3,4],**{str(i):[]for i in [1,2,3,4,6,7]}}
        p=self.make(graph);p.pause_writer(True)
        try:
            self.assertTrue(p.request([1,2,3,4,6,7],[[0,5,i]for i in range(1,5)]+[[0,6],[0,7]]))
            p.touch([1,2,3,4],2);p.touch([6,7],1)
            self.wait(p,lambda m:m['prepared_batches']==2)
            self.assertTrue(p.request([5],[[0,5]]));p.touch([5],3);p.touch([7],1)
            time.sleep(.03) # A broken pending flag would admit a duplicate third frame.
            self.assertEqual(p.metrics()['prepared_batches'],2)
            self.assertEqual(p.metrics()['promotion_batches'],0)
        finally:p.pause_writer(False)
        m=self.wait(p,lambda m:m['promotion_batches']==2 and not m['promoting'])
        self.assertFalse(m['error']);self.assertEqual(m['durable_records'],5)
        self.assertEqual((m['hot_vertices'],m['connector_vertices']),(4,1))


class DemandReservationTests(unittest.TestCase):
    def controller(self):
        import threading
        from types import SimpleNamespace
        from dhdgs.load_balance import LoadBalancer
        x=LoadBalancer.__new__(LoadBalancer)
        x.c=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())
        x.b=x.c['load_balance'];x.c['peer']['demand_growth'].update(enabled=True,growth_fraction=.1)
        x.b.update(max_region_bytes=100000,max_total_replica_bytes=1000000,max_replicas=3,reserve_disk_bytes=0)
        x.b['locality'].update(enabled=True)
        x.b['capacity_policy'].update(mode='per_region',small_alpha=.3,max_source_total_bytes=1000000,max_target_replica_bytes=1000000)
        x.b['online_policy']['enabled']=False
        x.lock=threading.RLock();x.regions={};x.draining={};x.loads={};x.in_progress=None
        x.rpc=lambda *a,**k:dict(source_index_bytes=1000000,free_bytes=1000000)
        x.builder=SimpleNamespace(submit=lambda fn,version,plan:(version,plan))
        return x

    def test_head_build_reserves_core_plus_beta(self):
        x=self.controller();v,p=x.create(dict(source=0,target=1,entries=[1],reserved_package_bytes=80000))
        self.assertEqual(x.build_reservations[v]['bytes'],180000)
        self.assertEqual(p['reserved_package_bytes'],80000)
        self.assertEqual(p['_demand_reserved_bytes'],100000)

    def test_head_global_quota_rejects_growth_from_another_source(self):
        x=self.controller();x.b['max_total_replica_bytes']=250000
        x.regions['existing']=dict(source=2,target=1,bytes=100000,entries=[2])
        with self.assertRaisesRegex(ValueError,'capacity|reservation'):
            x.create(dict(source=0,target=1,entries=[1],reserved_package_bytes=80000))


@unittest.skipUnless(active_ib(), 'Requires an active InfiniBand device')
class DemandRuntimeTests(unittest.TestCase):
    setUpClass=classmethod(replicas.ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(replicas.ReplicaTests.tearDownClass.__func__)
    setUp=replicas.ReplicaTests.setUp
    rpc=replicas.ReplicaTests.rpc
    info=replicas.ReplicaTests.info
    check_result=regions.RegionRuntimeTests.check_result

    def configure_replica_test(self,c):
        regions.mode(c)
        defaults=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())
        c['peer']['replica_pull']=copy.deepcopy(defaults['peer']['replica_pull'])
        c['peer']['replica_pull'].update(enabled=True,require_rdma=False,max_remaining=128)
        c['peer']['replica_pull']['async'].update(enabled=True,tail_threads=2,source_read_threads=0)
        enable(c)
        c['peer']['demand_growth']=copy.deepcopy(defaults['peer']['demand_growth'])
        c['peer']['demand_growth'].update(enabled=True,cache_records=8,promotion_hot_vertices=2,
            growth_fraction=1.,promotion_trigger='full_hot_batch',training_seconds=0.)
        c['peer']['replica_pull']['one_sided'].update(missing_codes_only=True,doorbell_batch=True)
        c['load_balance'].update(replica_transfer_mode='staged_v1',native_replica_pack=True)
        c['load_balance']['capacity_policy'].update(mode='per_region',max_source_total_bytes=1<<30)
        c['search']['max_expansions']=128

    def install(self):
        info=self.info();a=info['source'];b=(a+1)%3;entry=info['entry_id'];version='growth_'+uuid.uuid4().hex[:12]
        self.rpc(a,'source_start',id=version,plan=dict(ids=[entry],entries=[entry],target=b))
        end=time.monotonic()+20
        while time.monotonic()<end:
            state=self.rpc(a,'status',id=version)
            if state['phase']=='FAILED':self.fail(state['error'])
            if state['phase']=='SENT':
                target=self.rpc(b,'status',id=version)
                if target['phase']=='FAILED':self.fail(target['error'])
                if target['phase']=='READY':break
            time.sleep(.01)
        else:self.fail('Demand replica not ready')
        self.addCleanup(Path(target['growth_file']).unlink,missing_ok=True)
        self.engine.runtime.replica_routes([dict(source=a,target=b,version=version,entries=[entry],fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=60000)])
        return a,b

    def test_shared_rdma_record_includes_own_code_not_just_neighbor_codes(self):
        a,_=self.install();shard=self.servers[a].control.worker
        cfg=self.c['peer']['replica_pull']['one_sided'];ids=[int(shard.ids[0])]
        old=native().one_sided_probe(shard.native_search,cfg,ids,524288)
        full=native().one_sided_probe(shard.native_search,cfg,ids,524288,{},'full',True)
        self.assertTrue(full['codes_equal']);self.assertTrue(full['records_equal'])
        self.assertEqual(full['returned_code_rows'],old['returned_code_rows']+1)

    def test_real_read_reuse_budget_and_freeze(self):
        a,b=self.install()
        results=[self.engine.search_async(self.x[0],0).result(20) for _ in range(12)]
        for r in results:self.check_result(r,128)
        metrics=self.rpc(b,'growth_status')[0]
        self.assertGreater(metrics['read_batches'],0,metrics)
        self.assertGreater(metrics['expanded'],0,metrics)
        self.assertEqual(metrics['read_errors'],0,metrics)
        self.assertLessEqual(metrics['occupancy_peak'],8)
        self.assertLessEqual(metrics['committed_bytes'],metrics['reserved_bytes'])
        m=self.servers[b].control.peer.runtime.metrics()['pull_scheduler']['one_sided_client']
        self.assertGreater(m['read_operations'],0)
        self.assertEqual(m['errors'],0)
        self.rpc(b,'growth_seal')
        before=self.rpc(b,'growth_status')[0]['read_batches']
        for _ in range(3):self.check_result(self.engine.search_async(self.x[0],0).result(20),128)
        self.assertEqual(self.rpc(b,'growth_status')[0]['read_batches'],before)

    def test_concurrent_admission_falls_back_without_budget_loss(self):
        _,b=self.install()
        jobs=[self.engine.search_async(self.x[i%10],i%10) for i in range(48)]
        for job in jobs:self.check_result(job.result(30),128)
        m=self.rpc(b,'growth_status')[0]
        self.assertLessEqual(m['occupancy_peak'],8)
        self.assertEqual(m['read_errors'],0,m)
        self.assertFalse(m['error'],m)
