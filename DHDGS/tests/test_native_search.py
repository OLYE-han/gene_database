import copy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import os
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import numpy as np
from threadpoolctl import threadpool_limits

from dhdgs import _native
from dhdgs.async_io import LocalIO, ThreadReader
from dhdgs.config import validate, snapshot
from dhdgs.cluster_test import run_local_cluster
from dhdgs.index import build, Shard
from dhdgs.native_search import prepare_shard, run_native
from dhdgs.peer_search import run_branch
from dhdgs.peer import PeerRuntime
from dhdgs.search import Engine
from test_system import fixture


class NativeSearchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.base, cls.x = fixture(cls.root)
        with threadpool_limits(limits=1):
            build(cls.base)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.c = copy.deepcopy(self.base)
        self.c['search'].update(execution='peer', max_expansions=32, fallback_enabled=False)
        self.c['peer']['execution_model'] = 'native'
        self.c['storage']['io']['backend'] = 'sync_pread'
        self.c['scheduler'].update(mode='reference', max_wait_ms=1e9)
        self.shard = Shard(self.root/'out/index/shard_0', 0)
        self.addCleanup(self.shard.close)
        ids = np.load(self.root/'out/index/ids.npy')
        membership = np.load(self.root/'out/index/membership.npy')
        self.directory = {int(gid): list(map(int, np.flatnonzero(membership[i]))) for i, gid in enumerate(ids)}
        prepare_shard(self.shard, self.directory)

    def state(self, key='q'):
        gid = int(self.shard.ids[0])
        return {'query_request_id':key, 'branch_id':key, 'parent_branch_id':None,
                'target_node':0, 'entry_node':0, 'visited_nodes':[0], 'epoch':self.shard.meta['epoch'],
                'query_f32':self.x[0].tobytes(), 'query_id':0, 'work_grant':32,
                'branch_slots':0, 'seeds':[[gid,0.,0.,self.directory[gid],None,0,0,gid,0.]],
                'seen':[gid], 'remote_frontier':[], 'fallback_entries':[], 'scale':1., 'decision_seed':42}

    def test_native_matches_python_local_expansion_and_quantization(self):
        state = self.state()
        no_fork = lambda *_: self.fail('Unexpected fork')
        expected = run_branch(state, self.shard, self.directory, self.c, no_fork, lambda:False)
        before = self.shard.metrics()['counters']
        result = run_native(state, self.shard, self.directory, self.c, no_fork, lambda:False)
        self.assertEqual(result['expanded_ids'], expected['expanded_ids'])
        self.assertEqual(result['ids'], expected['ids'])
        self.assertEqual(result['result_paths'], expected['result_paths'])
        np.testing.assert_allclose(result['squared_distances'], expected['squared_distances'], rtol=1e-12)
        for key in ('file_read_bytes','pread_calls','scanned_edges','rabitq_evaluations','exact_distance_evaluations'):
            self.assertEqual(result['stats'][key], expected['stats'][key])
        for a,b in zip(result['local_batch_trace'],expected['local_batch_trace'],strict=True):
            self.assertEqual(a['ids'],b['ids'])
        self.assertEqual(result['compute_thread_id'], threading.get_native_id())
        self.assertEqual(result['compute_thread_migrations'], 0)
        self.assertTrue(result['native_search_loop'])
        after = self.shard.metrics()['counters']
        self.assertEqual(after['requests']-before['requests'], result['stats']['batches'])
        self.assertEqual(after['completed']-before['completed'], result['stats']['batches'])
        self.assertEqual(after['active'], 0)

    def test_native_sidecar_runtime_needs_no_python_owner_directory(self):
        # A fresh shard ensures prepare_shard cannot reuse setUp's native copy.
        shard=Shard(self.root/'out/index/shard_0',0)
        self.assertTrue((shard.directory/'code_membership.npy').exists())
        with patch.object(PeerRuntime,'_control'):
            runtime=PeerRuntime(self.c,shard)
            try:
                self.assertIsNone(runtime.directory)
                report=run_native(self.state(),shard,runtime.directory,self.c,
                                  lambda *_:self.fail('unexpected fork'),lambda:False)
                expected=run_native(self.state(),self.shard,self.directory,self.c,
                                    lambda *_:None,lambda:False)
                self.assertEqual(report['ids'],expected['ids'])
                self.assertEqual(report['expanded_ids'],expected['expanded_ids'])
            finally:runtime.close();shard.close()

    def test_skip_seen_estimates_preserves_local_search(self):
        no_fork=lambda *_: self.fail('Unexpected fork')
        original=run_native(self.state(),self.shard,self.directory,self.c,no_fork,lambda:False)
        tuned=copy.deepcopy(self.c);tuned['search']['skip_seen_estimates']=True
        validate({k:v for k,v in tuned.items() if not k.startswith('_')})
        optimized=run_native(self.state(),self.shard,self.directory,tuned,no_fork,lambda:False)
        for key in ('expanded_ids','ids','result_paths','squared_distances','work_spent'):
            self.assertEqual(original[key],optimized[key])
        for key in ('pread_calls','scanned_edges','exact_distance_evaluations'):
            self.assertEqual(original['stats'][key],optimized['stats'][key])
        self.assertGreater(optimized['stats']['rabitq_skipped_seen'],0)
        self.assertEqual(original['stats']['rabitq_evaluations'],
                         optimized['stats']['rabitq_evaluations']+optimized['stats']['rabitq_skipped_seen'])

    def test_striped_statistics_with_parallel_native_searches(self):
        shard=Shard(self.root/'out/index/shard_0',0,counter_stripes=128)
        self.addCleanup(shard.close);prepare_shard(shard,self.directory)
        before=shard.counters.snapshot()
        def run(i):
            return run_native(self.state(str(i)),shard,self.directory,self.c,
                              lambda *_:self.fail('Unexpected fork'),lambda:False)
        with ThreadPoolExecutor(16) as pool:reports=list(pool.map(run,range(64)))
        after=shard.counters.snapshot()
        self.assertTrue(all(r['ids']==reports[0]['ids'] for r in reports))
        self.assertEqual(after['active'],0)
        self.assertEqual(after['completed']-before['completed'],sum(r['stats']['batches']for r in reports))
        for k in ('pread_calls','file_read_bytes','rabitq_evaluations','exact_distance_evaluations'):
            self.assertEqual(after[k]-before[k],sum(r['stats'][k]for r in reports))

    def test_bounded_dense_id_lookup_preserves_search_and_falls_back(self):
        reference=run_native(self.state(),self.shard,self.directory,self.c,lambda *_:None,lambda:False)
        for limit,mode in ((1,'hash'),(1<<20,'dense')):
            shard=Shard(self.root/'out/index/shard_0',0,id_lookup_max_bytes=limit)
            self.addCleanup(shard.close);prepare_shard(shard,self.directory)
            layout=shard.native_search.id_lookup_info()
            self.assertEqual(layout['mode'],mode);self.assertLessEqual(layout['dense_bytes'],limit)
            actual=run_native(self.state(),shard,self.directory,self.c,lambda *_:None,lambda:False)
            for key in ('expanded_ids','ids','squared_distances','result_paths','work_spent'):
                self.assertEqual(actual[key],reference[key])
            for key in ('pread_calls','scanned_edges','rabitq_evaluations','exact_distance_evaluations'):
                self.assertEqual(actual['stats'][key],reference['stats'][key])
        # Widely spaced uint64 identifiers cannot overflow into a small table.
        sparse=Shard(self.root/'out/index/shard_0',0,id_lookup_max_bytes=1<<20)
        self.addCleanup(sparse.close);sparse.code_ids=np.array(sparse.code_ids,copy=True)
        sparse.code_ids[0]=0;sparse.code_ids[-1]=np.iinfo(np.uint64).max
        prepare_shard(sparse,self.directory)
        self.assertEqual(sparse.native_search.id_lookup_info()['mode'],'hash')

    def test_selected_query_trace_preserves_aggregate_metrics(self):
        self.c['metrics'].update(detailed_trace=False,trace_query_ids=[])
        state=self.state()
        plain=run_native(state,self.shard,self.directory,self.c,lambda *_:None,lambda:False)
        self.assertNotIn('native_timestamps',plain)
        self.assertNotIn('local_batch_trace',plain)
        self.c['metrics']['trace_query_ids']=[0]
        traced=run_native(state,self.shard,self.directory,self.c,lambda *_:None,lambda:False)
        self.assertTrue(traced['local_batch_trace'])
        self.assertEqual(plain['ids'],traced['ids'])
        self.assertEqual(plain['expanded_ids'],traced['expanded_ids'])
        for key in ('file_read_bytes','pread_calls','rabitq_evaluations','exact_distance_evaluations'):
            self.assertEqual(plain['stats'][key],traced['stats'][key])
        t=traced['native_timestamps']
        chain=[t[k] for k in ('native_search_start_ns','native_loop_done_ns',
                             'native_gil_reacquired_ns','native_report_ready_ns','native_return_ns')]
        self.assertEqual(chain,sorted(chain))

    def test_native_start_timestamps_bracket_batches(self):
        self.c['metrics']['detailed_trace']=True
        result=run_native(self.state(),self.shard,self.directory,self.c,lambda *_:None,lambda:False)
        t=result['native_timestamps']
        stamps=[t[k] for k in ('native_call_ns','native_entry_ns','native_search_start_ns','native_return_ns')]
        self.assertEqual(stamps,sorted(stamps))
        for batch in result['local_batch_trace']:
            b=batch['service_trace']
            self.assertEqual(t['clock_domain'],b['clock_domain'])
            self.assertLessEqual(t['native_search_start_ns'],b['start_ns'])
            self.assertLessEqual(b['end_ns'],t['native_return_ns'])
        self.c['metrics']['detailed_trace']=False
        result=run_native(self.state(),self.shard,self.directory,self.c,lambda *_:None,lambda:False)
        self.assertNotIn('native_timestamps',result)

    def test_lifecycle_trace_without_batch_trace(self):
        self.c['metrics'].update(detailed_trace=False,trace_query_ids=[],lifecycle_trace=True)
        result=run_native(self.state(),self.shard,self.directory,self.c,lambda *_:None,lambda:False)
        self.assertNotIn('local_batch_trace',result)
        stamps=result['native_timestamps']
        times=[stamps[k] for k in ('native_call_ns','native_search_start_ns','native_loop_done_ns',
                                  'native_gil_reacquired_ns','native_report_ready_ns','native_return_ns')]
        self.assertEqual(times,sorted(times))
        self.assertGreater(result['stats']['exact_distance_evaluations'],0)

    def test_native_remote_protocol_budget_and_no_node_revisits(self):
        self.c['search'].update(max_expansions=192, fallback_enabled=True)
        self.c['scheduler']['max_wait_ms'] = 1.
        engine = Engine(self.c)
        try:
            for index in (0,35,70):
                result = engine.search(self.x[index], index)
                self.assertEqual(len(result['ids']),5)
                self.assertEqual(result['stats']['node_revisit_count'],0)
                self.assertEqual(result['stats']['router_expand_rpc_count'],0)
                self.assertLessEqual(result['stats']['expanded'],192)
                for report in result['branch_reports']:
                    self.assertEqual(report['work_grant'],report['work_spent']+report['work_unused']+sum(ch['work_grant'] for ch in report['children']))
                    for child in report['children']:
                        self.assertEqual(sum(d['remote'] and d['decision_id']==child['decision_id'] and d['selected_node']==child['node_id'] for d in report['decisions']),1)
                    self.assertTrue(report['native_search_loop'])
        finally:
            engine.close()

    def test_atomic_cancellation_and_validation(self):
        state=self.state();control=_native.SearchControl(time.monotonic()+10,time.monotonic()+10)
        control.cancel()
        with self.assertRaises(TimeoutError):
            run_native(state,self.shard,self.directory,self.c,lambda *_:None,lambda:False,control=control)
        with self.assertRaisesRegex(ValueError,'epoch'):
            run_native({**state,'epoch':'bad'},self.shard,self.directory,self.c,lambda *_:None,lambda:False)
        self.c['storage']['cache_bytes_per_shard']=1
        with self.assertRaisesRegex(ValueError,'cache'):
            validate({k:v for k,v in self.c.items() if not k.startswith('_')})

    def test_async_aggregation_has_fixed_compute_owner_and_same_results(self):
        cfg=copy.deepcopy(self.c['storage']['io'])
        cfg.update(backend='thread_pool', worker_threads=4, max_inflight_per_device=16,
                   max_submit_requests=8, max_inflight_per_query=4)
        first_submit=threading.Event();release=threading.Event()
        original=ThreadReader.submit
        submits=[]
        def gated(reader, requests):
            submits.append(threading.get_native_id())
            if not first_submit.is_set():
                first_submit.set()
                if not release.wait(5):raise TimeoutError('Test gate not released')
            return original(reader,requests)
        with patch.object(ThreadReader,'submit',gated):
            io=LocalIO(self.shard,cfg,trace=True)
            try:
                def query(i):
                    tid=threading.get_native_id()
                    result=run_native(self.state(str(i)),self.shard,self.directory,self.c,lambda *_:None,lambda:False,io=io)
                    self.assertEqual(result['compute_thread_id'],tid)
                    return result
                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures=[pool.submit(query,i) for i in range(4)]
                    try:
                        self.assertTrue(first_submit.wait(5))
                        deadline=time.monotonic()+5
                        while io.metrics()['active_batches']<4 and time.monotonic()<deadline:time.sleep(.001)
                        self.assertEqual(io.metrics()['active_batches'],4)
                    finally:release.set()
                    reports=[f.result(10) for f in futures]
                expected=run_native(self.state(),self.shard,self.directory,self.c,lambda *_:None,lambda:False)
                tids={r['compute_thread_id'] for r in reports}
                self.assertEqual(len(tids),4)
                self.assertTrue(tids.isdisjoint(submits))
                for r in reports:
                    self.assertEqual(r['expanded_ids'],expected['expanded_ids'])
                    self.assertEqual(r['ids'],expected['ids'])
                    self.assertEqual(r['compute_thread_migrations'],0)
                    self.assertGreater(r['stats']['query_io_wait_ms'],0)
                self.assertTrue(any(int(k)>1 and v>0 for k,v in io.metrics()['submission_query_counts'].items()))
            finally:release.set();io.close()

    def test_io_uring_native_equivalence(self):
        if os.environ.get('DHDGS_TEST_URING')!='1':self.skipTest('opt-in native io_uring')
        cfg=copy.deepcopy(self.c['storage']['io']);cfg.update(backend='io_uring',fallback_backend='error')
        io=LocalIO(self.shard,cfg,trace=True)
        try:
            result=run_native(self.state(),self.shard,self.directory,self.c,lambda *_:None,lambda:False,io=io)
            expected=run_native(self.state(),self.shard,self.directory,self.c,lambda *_:None,lambda:False)
            self.assertEqual(result['expanded_ids'],expected['expanded_ids'])
            self.assertEqual(result['squared_distances'],expected['squared_distances'])
            self.assertGreater(result['stats']['async_read_requests'],0)
        finally:io.close()

    def test_native_cancellation_during_io_drains_without_search_resume(self):
        cfg=copy.deepcopy(self.c['storage']['io']);cfg['backend']='thread_pool'
        entered,release=threading.Event(),threading.Event()
        original=ThreadReader._read
        def delayed(reader,offset,length):
            entered.set()
            if not release.wait(5):raise TimeoutError('Test did not release I/O')
            return original(reader,offset,length)
        control=_native.SearchControl(time.monotonic()+10,time.monotonic()+10)
        before=self.shard.metrics()['counters']
        with patch.object(ThreadReader,'_read',delayed):
            io=LocalIO(self.shard,cfg)
            try:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future=pool.submit(run_native,self.state(),self.shard,self.directory,self.c,
                                       lambda *_:self.fail('Unexpected dispatch'),lambda:False,
                                       control=control,io=io)
                    try:
                        self.assertTrue(entered.wait(5));control.cancel()
                        with self.assertRaises(TimeoutError):future.result(5)
                    finally:release.set()
            finally:release.set();io.close()
        after=self.shard.metrics()['counters']
        self.assertEqual(after['completed'],before['completed'])
        self.assertEqual(after['failed']-before['failed'],1)
        self.assertEqual(after['active'],0)
        self.assertEqual(io.metrics()['reserved_bytes'],0)

    def test_tcp_native_three_nodes_and_metrics(self):
        c=copy.deepcopy(self.c)
        c['network']['transport']='tcp'
        c['storage']['io']['backend']='thread_pool'
        c['search'].update(max_expansions=192,fallback_enabled=True)
        c['peer'].update(heartbeat_interval_ms=10,lease_timeout_ms=1000,worker_threads=4)
        c['experiment'].update(benchmark_queries=3,benchmark_concurrency=2,warmup_queries=0)
        sockets=[]
        try:
            for node in c['cluster']['nodes']:
                sock=socket.socket();sock.bind(('127.0.0.1',0));sockets.append(sock)
                node.update(host='127.0.0.1',port=sock.getsockname()[1])
            with tempfile.TemporaryDirectory() as tmp:
                run=Path(tmp)/'run';snapshot(c,run)
                for sock in sockets:sock.close()
                report=run_local_cluster(c,run)
        finally:
            for sock in sockets:sock.close()
        self.assertEqual(report['failed_queries'],0)
        self.assertEqual(report['recall_at_k'],1.)
        for q in report['queries']:
            self.assertTrue(q['stats']['metrics_complete'])
            self.assertEqual(q['stats']['node_revisit_count'],0)
            for branch in q['branch_reports']:
                self.assertEqual(branch['execution']['model'],'native')
                self.assertEqual(branch['compute_thread_migrations'],0)


    def test_native_control_tcp_preserves_budget_results_and_diagnostics(self):
        c=copy.deepcopy(self.c)
        c['network']['transport']='tcp'
        c['peer']['control']['scheduler']='native'
        c['peer']['communication']['mode']='optimized'
        c['peer']['control']['initial_heartbeat_delay_ms']=10
        c['peer']['compute']['mode']='fixed'
        c['peer'].update(heartbeat_interval_ms=10,lease_timeout_ms=1000,worker_threads=2)
        c['search'].update(max_expansions=192,fallback_enabled=True)
        c['metrics'].update(lifecycle_trace=True,detailed_trace=True)
        c['experiment'].update(benchmark_queries=3,benchmark_concurrency=2,warmup_queries=0)
        sockets=[]
        try:
            for node in c['cluster']['nodes']:
                sock=socket.socket();sock.bind(('127.0.0.1',0));sockets.append(sock)
                node.update(host='127.0.0.1',port=sock.getsockname()[1])
            with tempfile.TemporaryDirectory() as tmp:
                run=Path(tmp)/'run';snapshot(c,run)
                for sock in sockets:sock.close()
                report=run_local_cluster(c,run)
        finally:
            for sock in sockets:sock.close()
        self.assertEqual(report['failed_queries'],0)
        self.assertEqual(report['recall_at_k'],1.)
        for q in report['queries']:
            self.assertTrue(q['stats']['metrics_complete'])
            self.assertEqual(q['stats']['node_revisit_count'],0)
            for b in q['branch_reports']:
                self.assertEqual(b['execution']['completion_backend'],'native')
                self.assertEqual(b['compute_thread_migrations'],0)
                self.assertEqual(b['work_grant'],b['work_spent']+b['work_unused']+sum(x['work_grant'] for x in b['children']))
                self.assertIn('local_batch_trace',b)

    def test_native_control_rejects_unsupported_backends(self):
        c={k:copy.deepcopy(v) for k,v in self.c.items() if not k.startswith('_')}
        c['peer']['control']['scheduler']='native'
        with self.assertRaisesRegex(ValueError,'Native control requires'):validate(c)
        c['network']['transport']='tcp'
        c['storage']['io']['backend']='thread_pool'
        with self.assertRaisesRegex(ValueError,'Native control requires'):validate(c)
        c['storage']['io']['backend']='sync_pread'
        c['peer']['control']['completion_fast_path']=True
        with self.assertRaisesRegex(ValueError,'completion_fast_path'):validate(c)
        c['peer']['control']['completion_fast_path']=False
        c['peer']['communication']['reuse_connections']=False
        with self.assertRaisesRegex(ValueError,'reuse_connections'):validate(c)


if __name__=='__main__':unittest.main()
