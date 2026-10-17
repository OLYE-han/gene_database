import copy
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import numpy as np
from threadpoolctl import threadpool_limits

from dhdgs.async_io import LocalIO, ThreadReader
from dhdgs.index import Shard, build
from dhdgs.peer import PeerRuntime, execution_signature
from dhdgs.peer_search import branch_steps, COUNTERS
from test_system import fixture


class RecordShard:
    def __init__(self, root):
        self.path=root/'records.bin';self.path.write_bytes(b'HEAD'+b'aaaabbbbccccdddd')
        self.fd=os.open(self.path,os.O_RDONLY)
        self.meta={'record_bytes':4,'header_bytes':4,'epoch':'e'}
        self.location={i:i for i in range(4)}
    def cached_record(self,gid):
        if gid not in self.location:raise ValueError('Nonlocal ID')
        return None
    def cache_record(self,gid,raw):pass
    def close(self):os.close(self.fd)


class IOTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.c,_=fixture(self.root)
        self.c=self.c['storage']['io'];self.c.update(backend='thread_pool',max_inflight_per_query=1,
            max_inflight_per_device=2,max_inflight_bytes_per_device=8,buffer_pool_bytes_per_device=64)
        self.shard=RecordShard(self.root)
    def tearDown(self):self.shard.close();self.temp.cleanup()

    def test_round_larger_than_depth_and_capacity_progress(self):
        self.c['buffer_pool_bytes_per_device']=32
        io=LocalIO(self.shard,self.c)
        try:
            f1=io.submit([0,1,2,3],'e','a',lambda:False)
            f2=io.submit([3,2,1,0],'e','b',lambda:False)
            b1=f1.result(3)
            self.assertEqual(b1.records,{0:b'aaaa',1:b'bbbb',2:b'cccc',3:b'dddd'})
            self.assertFalse(f2.done()) # complete-but-unconsumed round holds reservation
            b1.release();b2=f2.result(3)
            self.assertEqual(b2.stats['file_read_bytes'],16);self.assertEqual(b2.stats['pread_calls'],4)
            b2.release()
            self.assertLessEqual(io.metrics()['counters']['peak_reserved_bytes'],32)
        finally:io.close()

    def test_cancel_inflight_drains_without_resurrection(self):
        entered,release,cancel=threading.Event(),threading.Event(),threading.Event()
        original=ThreadReader._read
        def delayed(reader,offset,length):
            entered.set()
            if not release.wait(3):raise TimeoutError('test barrier')
            return original(reader,offset,length)
        with patch.object(ThreadReader,'_read',delayed):
            io=LocalIO(self.shard,self.c)
            try:
                f=io.submit([0,1],'e','q',cancel.is_set)
                self.assertTrue(entered.wait(2));cancel.set()
                with self.assertRaises(TimeoutError):f.result(2)
                self.assertEqual(io.metrics()['inflight_requests'],1)
                release.set()
            finally:release.set();io.close()
            self.assertEqual(io.metrics()['counters']['late_read_bytes'],4)
            self.assertEqual(io.metrics()['inflight_requests'],0)

    def test_backpressure_invalid_epoch_and_short_read(self):
        self.c['max_pending_batches']=1
        io=LocalIO(self.shard,self.c)
        try:
            b=io.submit([0],'e','a',lambda:False).result(2)
            with self.assertRaisesRegex(RuntimeError,'capacity'):io.submit([1],'e','b',lambda:False).result(2)
            with self.assertRaisesRegex(ValueError,'epoch'):io.submit([0],'wrong','x',lambda:False).result(2)
            b.release()
        finally:io.close()
        self.shard.path.write_bytes(b'HEADx')
        io=LocalIO(self.shard,self.c)
        try:
            with self.assertRaisesRegex(OSError,'Short'):io.submit([0],'e','q',lambda:False).result(2)
        finally:io.close()

    def test_mixed_query_submission_and_out_of_order_completion(self):
        release=threading.Event();original=ThreadReader._read
        def delayed(reader,offset,length):
            if offset==4 and not release.wait(3):raise TimeoutError('test barrier')
            return original(reader,offset,length)
        with patch.object(ThreadReader,'_read',delayed):
            io=LocalIO(self.shard,self.c)
            try:
                with io.cv:
                    f1=io.submit([0,1],'e','a',lambda:False)
                    f2=io.submit([3,2],'e','b',lambda:False)
                b2=f2.result(2)
                self.assertFalse(f1.done()) # second query completes before slow first query
                self.assertEqual(b2.records[3],b'dddd');b2.release();release.set()
                b1=f1.result(2);self.assertEqual(b1.records[1],b'bbbb');b1.release()
                self.assertGreater(io.metrics()['submission_query_counts'].get('2',0),0)
            finally:release.set();io.close()

    def test_backend_probe_failure_is_explicit(self):
        from dhdgs import _native
        self.c['backend']='io_uring'
        with patch.object(_native,'AsyncReader',side_effect=RuntimeError('probe refused')):
            with self.assertRaisesRegex(RuntimeError,'probe refused'):LocalIO(self.shard,self.c)
            self.c['fallback_backend']='thread_pool'
            io=LocalIO(self.shard,self.c)
            try:
                self.assertEqual(io.metrics()['actual_backend'],'thread_pool')
                self.assertIn('probe refused',io.metrics()['fallback_reason'])
            finally:io.close()

    def test_native_real_reads_when_enabled(self):
        if os.environ.get('DHDGS_TEST_URING')!='1':self.skipTest('enable on a host permitting io_uring')
        self.c.update(backend='io_uring',fallback_backend='error')
        io=LocalIO(self.shard,self.c)
        try:
            batch=io.submit([3,1,0,2],'e','q',lambda:False).result(3)
            self.assertEqual(batch.records,{0:b'aaaa',1:b'bbbb',2:b'cccc',3:b'dddd'})
            self.assertEqual(batch.stats['async_read_requests'],4)
            self.assertEqual(batch.stats['pread_calls'],0)
            batch.release()
        finally:io.close()


class ResumeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.c,self.x=fixture(self.root)
        self.c['search'].update(execution='peer',max_expansions=16,local_beam=2)
        self.c['network']['transport']='tcp'
        self.c['peer'].update(execution_model='resumable',worker_threads=1,lease_timeout_ms=10000)
        self.c['storage']['io'].update(backend='thread_pool',worker_threads=2)
        with threadpool_limits(limits=1):build(self.c)
        self.shard=Shard(Path(self.c['paths']['output'])/'index/shard_0',0)
    def tearDown(self):self.shard.close();self.temp.cleanup()

    def state(self,qid):
        gid=int(self.shard.ids[0])
        return dict(query_request_id=qid,branch_id=qid,parent_branch_id=None,token='token',
            visited_nodes=[0],entry_node=0,target_node=0,epoch=self.shard.meta['epoch'],
            signature=execution_signature(self.c),query_f32=self.x[0].tobytes(),
            work_grant=16,branch_slots=0,seeds=[[gid,0.,0.,[0],None,0,0,gid,0.]],
            seen=[gid],scale=1.,decision_seed=42,query_id=None)

    def test_one_cpu_worker_overlaps_queries_and_preserves_results(self):
        first,second,release=threading.Event(),threading.Event(),threading.Event()
        original=ThreadReader._read;count=[0];lock=threading.Lock()
        def delayed(reader,offset,length):
            with lock:
                count[0]+=1
                (first if count[0]==1 else second).set()
            if not release.wait(3):raise TimeoutError('test I/O barrier')
            return original(reader,offset,length)
        with patch.object(PeerRuntime,'_control'),patch.object(ThreadReader,'_read',delayed):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state('a'));self.assertTrue(first.wait(2))
                runtime.accept(self.state('b'));self.assertTrue(second.wait(2))
                # Both query rounds reached disk despite only one CPU worker.
                self.assertEqual(runtime.metrics()['waiting_io'],2)
                release.set()
                for key in [('a','a'),('b','b')]:
                    task=runtime.records[key]['task'];self.assertTrue(task.done.wait(5))
                    self.assertIsNone(runtime.records[key]['error'])
                a=runtime.records[('a','a')]['report'];b=runtime.records[('b','b')]['report']
                self.assertEqual(a['ids'],b['ids'])
                from dhdgs.peer_search import run_branch
                reference=run_branch(self.state('baseline'),self.shard,runtime.directory,self.c,
                                     lambda *_:self.fail('No branch slots'),lambda:False)
                self.assertEqual(a['expanded_ids'],reference['expanded_ids'])
                self.assertEqual(a['squared_distances'],reference['squared_distances'])
                self.assertGreater(a['stats']['query_io_wait_ms'],0)
                self.assertGreater(a['stats']['server_thread_cpu_ms'],0)
                self.assertEqual(a['work_grant'],a['work_spent']+a['work_unused'])
            finally:release.set();runtime.close()

    def test_actual_record_compute_equivalence_and_cached_completion(self):
        ids=list(map(int,self.shard.ids[:4]));epoch=self.shard.meta['epoch']
        sync=self.shard.expand(self.x[0],ids,epoch)
        self.shard.cache_bytes=65536
        io=LocalIO(self.shard,self.c['storage']['io'])
        try:
            b=io.submit(ids,epoch,'q',lambda:False).result(3)
            result=self.shard.expand(self.x[0],ids,epoch,records=b.records,read_stats=b.stats)
            for field in ('expanded','neighbors','parents'):self.assertEqual(sync[field],result[field])
            b.release()
            cached=io.submit(ids,epoch,'q2',lambda:False).result(3)
            self.assertEqual(cached.stats['cache_hits'],4)
            self.assertEqual(cached.stats['pread_calls'],0);cached.release()
        finally:io.close()

    def test_io_wait_cannot_trigger_remote_even_after_threshold(self):
        class FakeShard:
            meta={'shard_id':0}
        c=copy.deepcopy(self.c)
        c['search'].update(local_beam=1,fallback_enabled=False)
        c['scheduler'].update(mode='adaptive',remote_weight=0,max_wait_ms=20.)
        state=self.state('q');state.update(work_grant=16,branch_slots=2,
            seeds=[[0,0.,0.,[0],None,0,0,0,0.]],seen=[0])
        forks=[]
        steps=branch_steps(state,FakeShard(),{0:[0],1:[0],2:[0],100:[1]},c,
                           lambda child,desc:forks.append((child,desc)),lambda:False)
        def reply(gid,neighbors):
            stats={k:0 for k in COUNTERS};stats['service_ms']=.01
            return {'expanded':[[gid,0.]],'neighbors':[[n,0.,.1] for n in neighbors],
                    'parents':[[n,gid] for n in neighbors],'stats':stats,'service_trace':{}}
        self.assertEqual(next(steps)[1],[0])
        self.assertEqual(steps.send(reply(0,[1,100]))[1],[1])
        time.sleep(.06) # remote max_wait elapsed; the local round is still suspended
        self.assertEqual(forks,[])
        steps.send(reply(1,[2]))
        self.assertEqual(len(forks),1)
        child,desc=forks[0]
        self.assertEqual(child['dispatch_decision_id'],desc['decision_id'])
        steps.close()

    def test_three_process_resumable_protocol(self):
        import socket
        from dhdgs.config import snapshot
        from dhdgs.cluster_test import run_local_cluster
        modes=['thread_pool']+(['io_uring'] if os.environ.get('DHDGS_TEST_URING')=='1' else [])
        for backend in modes:
            c=copy.deepcopy(self.c)
            c['storage']['io']['backend']=backend
            c['scheduler'].update(mode='adaptive',remote_weight=2)
            c['search']['max_expansions']=96
            c['experiment'].update(benchmark_concurrency=2,benchmark_repetitions=2)
            reservations=[socket.socket() for _ in range(3)]
            try:
                for node,sock in zip(c['cluster']['nodes'],reservations):
                    sock.bind(('127.0.0.1',0));node['port']=sock.getsockname()[1]
            finally:
                for sock in reservations:sock.close()
            directory=self.root/backend;snapshot(c,directory)
            report=run_local_cluster(c,directory)
            self.assertEqual(report['failed_queries'],0)
            self.assertTrue(all(s['stopped'] for s in report['local_cluster']['services']))
            for row in report['queries']:
                st=row['stats']
                self.assertEqual(st['node_revisit_count'],0)
                self.assertEqual(st['router_expand_rpc_count'],0)
                self.assertEqual(st['io_backends'],[backend])
                self.assertLessEqual(st['expanded'],96)
                for branch in row['branch_reports']:
                    stats=branch['stats']
                    parts=sum(stats[k] for k in ('query_ready_queue_ms','query_compute_slice_wall_ms',
                        'query_io_wait_ms','query_transfer_finalize_wait_ms','query_state_other_ms'))
                    self.assertAlmostEqual(parts,stats['query_branch_wall_ms'],places=5)
                    for child in branch['children']:
                        matches=[d for d in branch['decisions'] if d['decision_id']==child['decision_id']]
                        self.assertEqual(len(matches),1)
                        self.assertTrue(matches[0]['remote'])
