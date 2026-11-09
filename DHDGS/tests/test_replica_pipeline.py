"""Transfer, cancellation, native loading and async plan correctness."""
import copy
from concurrent.futures import Future, ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch
import numpy as np
import yaml
from dhdgs.index import Shard, native
from dhdgs.replicas import package, digest, CONTRACT, ReplicaService, ReplicaTransportError
from dhdgs.load_balance import candidate
import test_replicas as fixtures


class PlannerTests(unittest.TestCase):
    def test_native_plan_matches_python_with_duplicates_and_portals(self):
        b=yaml.safe_load((fixtures.ROOT/'configs/dhdgs.yaml').read_text())['load_balance']
        b.update(min_entry_queries=1,min_group_queries=1,min_coverage=.1,max_core_rows=45,portal_budget_fraction=.25)
        rng=np.random.default_rng(82)
        rows=[dict(source=0,replica=None,query_id=i//2,entry=i//4,score=float(rng.random()),
                   branches=[dict(node=0,cpu_ms=float(rng.random()*5),expanded_ids=list(map(int,rng.choice(100,8,replace=False)))),
                             dict(node=1,cpu_ms=.2,expanded_ids=[201])])for i in range(200)]
        rows+=copy.deepcopy(rows[:20]);rows[-1]['score']=.5
        for policy in ['cpu_worksets','entry_worksets','hotness_worksets']:
            b['selection_policy']=policy;heat={i:float(i%13)for i in range(100)}
            expected=candidate(rows,0,1,b,heat);actual=native().replica_candidate(rows,0,1,b,heat)
            self.assertEqual(expected,actual)
        with self.assertRaisesRegex(ValueError,'Insufficient'):native().replica_candidate([],0,1,b,None)

    def test_completed_plan_rechecked_for_age_epoch_and_overlap(self):
        for reason in ['expired','epoch_changed','entry_already_replicated',None]:
            x=fixtures.ControllerPolicyTests.controller(self);x.stop=threading.Event()
            x.b.update(planner_mode='native_async',planner_max_age_ms=500,max_replicas=2)
            now=time.monotonic();x.loads={0:dict(util=.99,queue_ms=99,usable=True,updated=now,epoch='e'),
                                        1:dict(util=.1,queue_ms=0,usable=True,updated=now,epoch='e')}
            x.plan_context=dict(started=now-(1 if reason=='expired'else .01),epochs={0:'bad'if reason=='epoch_changed'else 'e',1:'e'})
            if reason=='entry_already_replicated':x.regions={'old':dict(source=0,entries=[7])}
            x.plan_future=Future();x.plan_future.set_result(dict(source=0,target=1,entries=[7],ids=[7]))
            x.poll_plan(now)
            self.assertIsNone(x.plan_future)
            if reason:self.assertEqual(x.logged[-1][1]['reason'],reason);self.assertIsNone(x.in_progress)
            else:self.assertEqual(x.in_progress,'test')


class PipelineTests(unittest.TestCase):
    setUpClass=classmethod(fixtures.ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.ReplicaTests.tearDownClass.__func__)
    setUp=fixtures.ReplicaTests.setUp
    info=fixtures.ReplicaTests.info
    rpc=fixtures.ReplicaTests.rpc
    copy_source=fixtures.ReplicaTests.copy_source

    def configure_replica_test(self,c):
        c['load_balance'].update(replica_transfer_mode='pipeline_v2',replica_loader='native',native_replica_pack=True,
            chunk_bytes=4096,producer_queue_bytes=8192,inflight_chunks=2,publication_mode='ready',warm_bytes=0,
            compute_region_quality=False,quality_weight_routing=False,peer_routes_enabled=False)

    def service(self,node):return self.servers[node].control.replica_service

    def prepared_package(self):
        info=self.info();a=info['source'];b=(a+1)%3
        shard=Shard(Path(self.c['paths']['output'])/'index'/f'shard_{a}',0);self.addCleanup(shard.close)
        path=Path(self.work.name)/'package'
        manifest=package(shard,shard.ids.tolist(),[info['entry_id']],path,self.c['load_balance'])
        return a,b,path,manifest

    def begin(self,b,manifest,name='manual'):
        return self.rpc(b,'pipeline_begin',id=name,contract=manifest['contract'],centers_sha256=manifest['centers_sha256'],reserve_bytes=self.c['load_balance']['max_region_bytes'])

    def test_pipeline_search_retire_and_byte_equivalence(self):
        info,a,b=self.copy_source('stream')
        service=self.service(b);state=self.rpc(b,'status',id='stream')
        self.assertEqual(state['loader'],'native');self.assertEqual(state['warm_read_bytes'],0)
        self.assertGreater(state['validation_read_bytes'],0)
        shard=Shard(Path(self.c['paths']['output'])/'index'/f'shard_{a}',0);self.addCleanup(shard.close)
        expected=package(shard,shard.ids.tolist(),[info['entry_id']],Path(self.work.name)/'old',self.c['load_balance'])
        self.assertEqual(expected['files'],state['manifest']['files'])
        for name,desc in expected['files'].items():self.assertEqual(digest(service.root/'stream'/name),desc['sha256'])
        self.engine.runtime.replica_routes([dict(source=a,target=b,version='stream',entries=[info['entry_id']],fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=10000)])
        result=self.engine.search_async(self.x[0],0).result(10)
        self.assertEqual(result['status'],'ok');self.assertEqual(result['stats']['replica_version'],'stream')
        self.assertLessEqual(result['stats']['expanded'],256)
        self.assertEqual(set(result['ids']),set(map(int,np.argsort(np.sum((self.x.astype(float)-self.x[0])**2,axis=1))[1:6])))
        self.engine.runtime.replica_routes([]);self.rpc(b,'retire',id='stream')
        self.assertTrue(self.rpc(b,'reap',id='stream')['reclaimed'])
        self.assertFalse((service.root/'stream').exists())

    def test_lost_ack_retries_begin_open_chunk_and_finish(self):
        original=ReplicaService.rpc;lost=set()
        operations={'pipeline_begin','pipeline_open','pipeline_chunk','pipeline_finish'}
        def lose_reply(service,node,op,**fields):
            reply=original(service,node,op,**fields)
            if op in operations and op not in lost:
                lost.add(op)
                raise ReplicaTransportError('Injected lost ACK after successful operation')
            return reply
        with patch.object(ReplicaService,'rpc',lose_reply):
            info,a,b=self.copy_source('lost_ack')
        self.assertEqual(lost,operations)
        state=self.rpc(b,'status',id='lost_ack')
        self.assertEqual(state['phase'],'READY')
        self.assertEqual(state['received_bytes'],state['manifest']['bytes'])
        self.assertEqual(sum(e['phase']=='SEALING'for e in state['events']),1)

    def test_capability_fallback_before_v2_reservation(self):
        original=ReplicaService.rpc
        def legacy(service,node,op,**fields):
            if op=='capabilities':return dict(replica_protocols=[1])
            if op.startswith('pipeline_'):self.fail('V2 operation after fallback')
            return original(service,node,op,**fields)
        with patch.object(ReplicaService,'rpc',legacy):
            info,a,b=self.copy_source('fallback')
        self.assertEqual(self.rpc(a,'status',id='fallback')['transfer_fallback'],'staged_v1')
        self.assertEqual(self.rpc(b,'status',id='fallback')['loader'],'native')

    def test_chunks_reorder_duplicate_incomplete_and_conflict(self):
        a,b,path,m=self.prepared_package();self.begin(b,m)
        raw=(path/'nodes.bin').read_bytes();self.rpc(b,'pipeline_open',id='manual',file='nodes.bin',bytes=len(raw))
        with self.assertRaisesRegex(RuntimeError,'file set|incomplete'):self.rpc(b,'pipeline_finish',id='manual',manifest=m)
        chunks=[(off,raw[off:off+4096])for off in range(0,len(raw),4096)]
        for off,data in reversed(chunks):
            args=dict(id='manual',file='nodes.bin',offset=off,data=data,sha256=hashlib.sha256(data).hexdigest())
            self.assertFalse(self.rpc(b,'pipeline_chunk',**args)['duplicate'])
            self.assertTrue(self.rpc(b,'pipeline_chunk',**args)['duplicate'])
        altered=b'x'+chunks[0][1][1:]
        with self.assertRaisesRegex(RuntimeError,'Conflicting'):self.rpc(b,'pipeline_chunk',id='manual',file='nodes.bin',offset=0,data=altered,sha256=hashlib.sha256(altered).hexdigest())
        with self.assertRaisesRegex(RuntimeError,'checksum'):self.rpc(b,'pipeline_chunk',id='manual',file='nodes.bin',offset=0,data=raw[:4096],sha256='bad')
        with self.assertRaisesRegex(RuntimeError,'declaration'):self.rpc(b,'pipeline_open',id='manual',file='../bad',bytes=10)
        self.assertEqual(self.rpc(b,'status',id='manual')['received_bytes'],len(raw))
        self.rpc(b,'pipeline_abort',id='manual');self.assertTrue(self.rpc(b,'reap',id='manual')['reclaimed'])

    def test_failed_writer_retains_reservation_until_reaped(self):
        a,b,path,m=self.prepared_package();self.begin(b,m,'reserved')
        service=self.service(b);service.pipeline.close();service.b['max_replicas']=1
        state=service.states['reserved']
        state['phase']='FAILED';state['_writers'][('nodes.bin',0)]='inflight'
        with self.assertRaisesRegex(RuntimeError,'capacity'):self.begin(b,m,'new_version')
        self.assertFalse(service.pipeline.reap('reserved')['reclaimed'])
        state['_writers'].clear();self.assertTrue(service.pipeline.reap('reserved')['reclaimed'])
        self.begin(b,m,'new_version')

    def test_abort_does_not_close_descriptor_owned_by_writer(self):
        a,b,path,m=self.prepared_package();self.begin(b,m,'racing')
        data=(path/'nodes.bin').read_bytes()[:4096];self.rpc(b,'pipeline_open',id='racing',file='nodes.bin',bytes=m['files']['nodes.bin']['bytes'])
        receiver=self.service(b).pipeline;entered=threading.Event();release=threading.Event()
        import os
        original=os.pwrite
        def blocked(fd,buf,off):
            entered.set();self.assertTrue(release.wait(3));return original(fd,buf,off)
        request=dict(file='nodes.bin',offset=0,data=data,sha256=hashlib.sha256(data).hexdigest())
        with ThreadPoolExecutor(1)as pool:
            with patch('dhdgs.replica_pipeline.os.pwrite',blocked):
                future=pool.submit(receiver.chunk,'racing',request)
                try:
                    self.assertTrue(entered.wait(3));self.assertTrue(receiver.chunk('racing',request)['pending'])
                    receiver.abort('racing');self.assertFalse(receiver.reap('racing')['reclaimed'])
                finally:release.set()
                with self.assertRaisesRegex((ValueError,RuntimeError),'cancelled'):future.result(3)
        self.assertTrue(receiver.reap('racing')['reclaimed'])

    def test_native_loader_rejects_invalid_arrays_and_cancelled_publication(self):
        a,b,path,m=self.prepared_package();search=self.service(b).owner.worker.native_search
        meta=json.loads((path/'manifest.json').read_text())
        def prepare(token):return search.prepare_replica_directory('direct',str(path),meta,m['centers_sha256'],m['entries'],0,0,token)
        good=native().ReplicaCancel(20);p=prepare(good);good.cancel()
        with self.assertRaisesRegex(RuntimeError,'cancelled'):search.publish_replica(p)
        del p
        factors=np.load(path/'factors.npy');np.save(path/'factors.npy',factors.astype(np.float64))
        with self.assertRaisesRegex(ValueError,'dtype'):prepare(native().ReplicaCancel(20))
        np.save(path/'factors.npy',factors);owners=np.load(path/'code_membership.npy').astype(np.uint8);owners[0,0]=2;np.save(path/'code_membership.npy',owners)
        with self.assertRaisesRegex(ValueError,'membership'):prepare(native().ReplicaCancel(20))

    def test_commit_hash_failure_never_publishes_and_can_be_reaped(self):
        a,b,path,m=self.prepared_package();self.begin(b,m,'bad_hash')
        for name,desc in m['files'].items():
            self.rpc(b,'pipeline_open',id='bad_hash',file=name,bytes=desc['bytes'])
            raw=(path/name).read_bytes()
            for off in range(0,len(raw),4096):
                data=raw[off:off+4096]
                self.rpc(b,'pipeline_chunk',id='bad_hash',file=name,offset=off,data=data,sha256=hashlib.sha256(data).hexdigest())
        service=self.service(b)
        with (service.root/'bad_hash/nodes.bin').open('r+b')as f:f.seek(100);f.write(b'bad')
        self.rpc(b,'pipeline_finish',id='bad_hash',manifest=m)
        self.rpc(b,'pipeline_finish',id='bad_hash',manifest=m) # Idempotent unknown ACK retry.
        end=time.monotonic()+5
        while time.monotonic()<end:
            state=self.rpc(b,'status',id='bad_hash')
            if state['phase']=='FAILED':break
            time.sleep(.01)
        self.assertEqual(state['phase'],'FAILED');self.assertIn('checksum',state['error'])
        self.assertTrue(service.owner.worker.native_search.replica_released('bad_hash'))
        self.assertTrue(self.rpc(b,'reap',id='bad_hash')['reclaimed'])
