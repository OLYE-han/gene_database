import copy
import os
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from contextlib import ExitStack
from dhdgs.replica_layout import (build_partitioned_regions,
    build_and_publish_partitioned_regions, PIPELINE_DEFAULTS, pipeline_settings)
from dhdgs.planning_context import PlanningContext
from test_replica_community import CommunityTests


class PartitionPipelineTests(unittest.TestCase):
    def setUp(self):
        f = CommunityTests(); f.setUp(); self.addCleanup(f.doCleanups)
        handle, ids, meta = f.graph({1:[2],2:[3,999],3:[4],4:[1],10:[11],11:[12],12:[13],13:[10]})
        meta['epoch'] = 'pinned-test-index'
        self.shard = SimpleNamespace(fd=handle.fileno(), ids=ids, meta=meta,
                                    location={int(x):i for i,x in enumerate(ids)})
        self.b = f.b
        self.b.update(max_total_replica_bytes=1000000, community_hops=2, community_max_plan_ms=10000)
        self.layout = dict(total_alpha=.3, hops=2, target_weights={1:1.,2:1.},
            balance_weight=1.,deadline_s=10,max_edge_scans=10000,
            pipeline=dict(PIPELINE_DEFAULTS, enabled=True, adjacency_cache=True))
        # Data-node test environments include Python 3.10, before enterContext.
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch('dhdgs.locality.source_index_bytes', return_value=3000000))
        for name in ('dhdgs.community.metadata','dhdgs.hop_expansion.metadata'):
            stack.enter_context(patch(name, side_effect=lambda _:dict(meta)))

    def build(self, layout=None, **kwargs):
        return build_partitioned_regions(self.shard,[1,10],[1.,1.],self.b,layout or self.layout,**kwargs)

    def test_native_cached_core_and_plan_equal_original(self):
        old=copy.deepcopy(self.layout);old['pipeline']['adjacency_cache']=False
        a,_=self.build(old);b,report=self.build()
        for x,y in zip(a,b):
            for key in ('ids','entries','region_proof','reserved_package_bytes','target'):
                self.assertEqual(x[key],y[key],key)
        stats=report['planning_cache']
        self.assertLessEqual(stats['adjacency_reads'],len(self.shard.ids))
        self.assertGreater(stats['cache_hits'],0)
        self.assertGreater(stats['verification_cache_hits'],0)
        self.assertLessEqual(stats['queue_peak'],self.layout['pipeline']['queue_reads'])

    def test_serial_and_pipeline_publish_identical_plans(self):
        seen=[]
        def publish(plan):seen.append(plan['target']);return plan['target']
        for enabled in (False,True):
            cfg=copy.deepcopy(self.layout);cfg['pipeline']['enabled']=enabled
            plans,report,handles=build_and_publish_partitioned_regions(
                self.shard,[1,10],[1.,1.],self.b,cfg,publish,lambda _:self.fail('unexpected rollback'))
            self.assertEqual(handles,[1,2]);self.assertEqual(report['publication_pipeline'],enabled)
            self.assertEqual([p['target'] for p in plans],[1,2])
        self.assertEqual(seen,[1,2,1,2])

    def test_first_publication_overlaps_next_complete_plan(self):
        entered=threading.Event();second=threading.Event()
        from dhdgs.community import seeded_core
        calls=0
        def core(*args,**kwargs):
            nonlocal calls
            calls+=1
            if calls==2:
                self.assertTrue(entered.wait(3),'first publisher not running during second plan')
                second.set()
            return seeded_core(*args,**kwargs)
        def publish(plan):
            if plan['target']==1:
                entered.set();self.assertTrue(second.wait(3))
            return plan['target']
        with patch('dhdgs.community.seeded_core',side_effect=core):
            _,_,handles=build_and_publish_partitioned_regions(self.shard,[1,10],[1.,1.],self.b,self.layout,publish,lambda _:None)
        self.assertEqual(handles,[1,2])

    def test_later_plan_failure_retires_already_published_graph(self):
        published=threading.Event();retired=[]
        from dhdgs.community import seeded_core
        calls=0
        def core(*args,**kwargs):
            nonlocal calls
            calls+=1
            if calls==2:
                self.assertTrue(published.wait(3));raise ValueError('second complete core failed')
            return seeded_core(*args,**kwargs)
        def publish(plan):published.set();return plan['target']
        with patch('dhdgs.community.seeded_core',side_effect=core):
            with self.assertRaisesRegex(ValueError,'second complete core'):
                build_and_publish_partitioned_regions(self.shard,[1,10],[1.,1.],self.b,self.layout,publish,retired.append)
        self.assertEqual(retired,[1])

    def test_publication_failure_rolls_back_and_surfaces_cleanup_failure(self):
        for fail_cleanup in (False,True):
            retired=[]
            def publish(plan):
                if plan['target']==2:raise ValueError('target failed')
                return plan['target']
            def retire(handle):
                retired.append(handle)
                if fail_cleanup:raise ValueError('still pinned')
            with self.assertRaisesRegex(RuntimeError,'pending' if fail_cleanup else 'publisher failed'):
                build_and_publish_partitioned_regions(self.shard,[1,10],[1.,1.],self.b,self.layout,publish,retire)
            self.assertEqual(retired,[1])

    def test_cancel_before_publication(self):
        with self.assertRaisesRegex(RuntimeError,'cancelled'):
            build_and_publish_partitioned_regions(self.shard,[1,10],[1.,1.],self.b,self.layout,
                lambda p:self.fail('published after cancellation'),lambda _:None,cancelled=lambda:True)

    def test_identity_change_and_cancel_cannot_reuse_cached_reads(self):
        import time
        ctx=PlanningContext(self.shard,self.b,self.layout['pipeline'],time.monotonic()+5)
        self.addCleanup(ctx.close)
        ctx.balls([[1]],1,self.b)
        old=self.shard.meta['epoch'];self.shard.meta['epoch']='different'
        with self.assertRaisesRegex(ValueError,'identity'):ctx.check()
        self.shard.meta['epoch']=old
        os.ftruncate(self.shard.fd, os.fstat(self.shard.fd).st_size+1)
        with self.assertRaisesRegex(ValueError,'identity'):ctx.check()

    def test_wrong_member_digest_never_uses_cached_verification(self):
        import time
        from dhdgs.community import seeded_core,verify
        ctx=PlanningContext(self.shard,self.b,self.layout['pipeline'],time.monotonic()+5)
        self.addCleanup(ctx.close)
        p=seeded_core(self.shard,[1],[1.],self.b,seed_count=1,hops=2,planning=ctx)
        verify(self.shard,p,self.b,planning=ctx)
        p['ids'].append(999)
        with self.assertRaisesRegex(ValueError,'digest'):verify(self.shard,p,self.b,planning=ctx)

    def test_limits_and_legacy_configuration(self):
        self.assertFalse(pipeline_settings({})['enabled'])
        for key,value in [('io_threads',-1),('pending_plans',0),('cache_bytes',1),('enabled',1),('unknown',1)]:
            with self.assertRaises(ValueError):pipeline_settings({'pipeline':{key:value}})

    def test_remote_poll_is_idempotent_ordered_and_bounded(self):
        import time
        from dhdgs.replica_layout import PartitionPlanWorker
        worker=PartitionPlanWorker(self.shard,self.b);self.addCleanup(worker.close)
        worker.start('stream',[1,10],[1.,1.],self.layout)
        self.assertTrue(worker.start('stream',[1,10],[1.,1.],self.layout)['accepted'])
        with self.assertRaisesRegex(ValueError,'conflict'):
            worker.start('stream',[1,10],[2.,1.],self.layout)
        seen=[]
        for sequence in range(3):
            deadline=time.monotonic()+5
            while True:
                reply=worker.poll('stream',sequence)
                from dhdgs.index import native
                native().native_wire_pack(reply)
                if reply['done'] or reply['plan'] is not None:break
                self.assertLess(time.monotonic(),deadline);time.sleep(.001)
            if sequence<2:
                self.assertEqual(reply,worker.poll('stream',sequence))
                with self.assertRaisesRegex(ValueError,'sequence'):
                    worker.poll('stream',sequence+2)
                seen.append(reply['plan']['target'])
            else:self.assertTrue(reply['done'])
        self.assertEqual(seen,[1,2])
        self.assertLessEqual(worker.state['queue'].qsize(),1)
        self.assertIsNone(worker.state['context'].graph)

    def test_source_worker_cancel_releases_producer_blocked_on_full_queue(self):
        import time
        from dhdgs.replica_layout import PartitionPlanWorker
        worker=PartitionPlanWorker(self.shard,self.b);self.addCleanup(worker.close)
        worker.start('cancelled',[1,10],[1.,1.],self.layout)
        deadline=time.monotonic()+5
        while worker.state['queue'].empty():
            self.assertLess(time.monotonic(),deadline);time.sleep(.001)
        worker.cancel('cancelled')
        with self.assertRaises((RuntimeError,ValueError)):
            worker.state['future'].result(5)

    def test_cache_exhaustion_does_not_emit_an_incomplete_plan(self):
        cfg=copy.deepcopy(self.layout);cfg['pipeline']['cache_bytes']=1024
        emitted=[]
        with self.assertRaisesRegex(RuntimeError,'cache limit'):
            self.build(cfg,on_plan=emitted.append)
        self.assertEqual(emitted,[])

    def test_yaml_schema_accepts_new_and_legacy_layout(self):
        import yaml
        from pathlib import Path
        from dhdgs.config import validate
        c=yaml.safe_load((Path(__file__).resolve().parents[1]/'configs/dhdgs.yaml').read_text())
        validate(c)
        c['load_balance']['partitioned_regions']['pipeline']['io_threads']=-1
        with self.assertRaises(ValueError):validate(c)
        del c['load_balance']['partitioned_regions']['pipeline']
        validate(c)


if __name__ == '__main__':unittest.main()
