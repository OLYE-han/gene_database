import tempfile,unittest,socket,threading
from pathlib import Path
import numpy as np
import test_cpp_runtime as fixtures
from dhdgs.cpp_head import NativeEngine
from dhdgs.index import native
from dhdgs.network_api import api_options,records
from dhdgs.transport import send,receive

class NetworkSearchApiTests(unittest.TestCase):
    setUpClass=classmethod(fixtures.NativeRuntimeTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.NativeRuntimeTests.tearDownClass.__func__)
    config=fixtures.NativeRuntimeTests.config
    start=fixtures.NativeRuntimeTests.start
    def setup_api(self,response_batch_size=0):
        c=self.config(True,True);self.start(c);engine=NativeEngine(c,external_completion=True);self.addCleanup(engine.close)
        settings={'api':dict(host='127.0.0.1',port=0,handlers=2,queue_capacity=64,max_connections=128,max_message_bytes=1048576,reactors=1,response_batch_size=response_batch_size)}
        port=engine.runtime.start_api(api_options(c,settings));return engine,dict(host='127.0.0.1',port=port,lanes=2,connections=16,timeout_ms=10000,max_message_bytes=1048576,consumer_queue_records=64,consumer_queue_bytes=64<<20)
    def test_stream_consumption_and_no_head_retention(self):
        engine,opts=self.setup_api();root=Path(tempfile.mkdtemp(dir=self.root))
        m=native().run_network_load(self.x[:48],np.arange(48,dtype=np.uint64),16,opts,str(root/'results'))
        self.assertFalse(m['error']);rows=list(records(root/'results'));self.assertEqual({q['job'] for q in rows},set(range(48)))
        for q in rows:
            self.assertTrue(q['transport_ok']);self.assertEqual(q['result']['status'],'ok');self.assertLessEqual(q['client_done_ns'],q['consume_started_ns'])
            self.assertLessEqual(q['result']['stats']['expanded'],engine.c['search']['max_expansions'])
        self.assertEqual(engine.runtime.metrics()['completed_pending'],0);self.assertEqual(engine.runtime.metrics()['active'],0)
        self.assertEqual(engine.runtime.metrics()['successful_queries'],48)
        self.assertEqual(engine.runtime.metrics()['failed_queries'],0)
        self.assertEqual(engine.runtime.metrics()['recent_failures'],[])
        self.assertEqual(sum(v['consumed'] for v in m['lanes']),48)
        for lane in m['lanes']:
            self.assertGreaterEqual(lane['output_sync_started_ns'],m['last_received_ns'])
            self.assertGreaterEqual(lane['output_sync_barrier_ms'],0)
    def test_consumer_capacity_failure_is_explicit(self):
        engine,opts=self.setup_api();opts['consumer_queue_bytes']=1
        root=Path(tempfile.mkdtemp(dir=self.root));m=native().run_network_load(self.x[:32],np.arange(32,dtype=np.uint64),16,opts,str(root/'results'))
        self.assertIn('capacity',m['error']);self.assertEqual(engine.runtime.metrics()['active'],0)

    def test_response_path_timestamps_and_counters(self):
        engine,opts=self.setup_api();root=Path(tempfile.mkdtemp(dir=self.root))
        m=native().run_network_load(self.x[:48],np.arange(48,dtype=np.uint64),16,opts,str(root/'results'))
        self.assertFalse(m['error'])
        for q in records(root/'results'):
            self.assertTrue(q['transport_ok']);t=q['result']['api_timestamps']
            stamps=[t[k] for k in ['completion_callback_ns','reply_deliver_ns',
                'reply_mailbox_acquired_ns','reply_queue_requested_ns','reply_enqueued_ns','reply_encode_started_ns']]
            stamps += [q[k] for k in ['response_first_byte_ns','response_frame_complete_ns',
                                      'response_decode_done_ns','client_done_ns']]
            self.assertEqual(stamps,sorted(stamps));self.assertTrue(all(stamps))
        lanes=engine.runtime.api_metrics()['network_module']['lanes']
        self.assertEqual(sum(v['response_timing']['count'] for v in lanes),48)
        self.assertEqual(sum(v['response_timing']['sent'] for v in lanes),48)

    def test_head_shared_affinity_uses_full_configured_cpu_set(self):
        import os
        cpus=sorted(os.sched_getaffinity(0))[:2]
        if len(cpus)<2:self.skipTest('Needs two allowed CPUs to distinguish shared affinity')
        c=self.config(True,True);c['native_runtime'].update(head_pin_threads=False,compute_cpu_ids=cpus)
        self.start(c);engine=NativeEngine(c,external_completion=True);self.addCleanup(engine.close)
        roles=[]
        for t in Path('/proc/self/task').iterdir():
            try:
                if t.joinpath('comm').read_text().strip() in ['dhdgs-route','dhdgs-merge']:
                    roles.append(os.sched_getaffinity(int(t.name)))
            except FileNotFoundError:pass
        self.assertEqual(len(roles),4)
        self.assertTrue(all(mask==set(cpus) for mask in roles))

    def test_bounded_response_batches_drain_all_results(self):
        engine,opts=self.setup_api(response_batch_size=1);opts['connections']=128
        root=Path(tempfile.mkdtemp(dir=self.root));x=np.tile(self.x,(3,1));ids=np.tile(np.arange(len(self.x),dtype=np.uint64),3)
        m=native().run_network_load(x,ids,64,opts,str(root/'results'))
        self.assertFalse(m['error']);self.assertEqual(m['received'],len(x))
        rows=list(records(root/'results'));self.assertEqual(len(rows),len(x))
        self.assertEqual({q['job']for q in rows},set(range(len(x))))
        self.assertTrue(all(q['transport_ok']and q['result']['status']=='ok'for q in rows))
        for lane in engine.runtime.api_metrics()['network_module']['lanes']:
            self.assertEqual(lane['response_batch_size'],1)
            self.assertEqual(lane['response_timing']['completed_queued'],0)

    def test_profiled_head_distinguishes_registry_wait_and_route_queue(self):
        c=self.config(True,True);c['metrics']['lifecycle_trace']=True
        self.start(c);engine=NativeEngine(c,external_completion=True);self.addCleanup(engine.close)
        settings={'api':dict(host='127.0.0.1',port=0,handlers=2,queue_capacity=64,
                            max_connections=128,max_message_bytes=1048576,reactors=1)}
        port=engine.runtime.start_api(api_options(c,settings))
        opts=dict(host='127.0.0.1',port=port,lanes=2,connections=16,timeout_ms=10000,
                  max_message_bytes=1048576,consumer_queue_records=64,consumer_queue_bytes=64<<20)
        root=Path(tempfile.mkdtemp(dir=self.root))
        m=native().run_network_load(self.x[:48],np.arange(48,dtype=np.uint64),16,opts,str(root/'results'))
        self.assertFalse(m['error'])
        for row in records(root/'results'):
            self.assertTrue(row['transport_ok']);result=row['result'];self.assertEqual(result['status'],'ok')
            stats=result['stats'];t=stats['native_head_timestamps']
            points=[t[k]for k in ['search_enter_ns','submit_registry_requested_ns',
                     'submit_registry_acquired_ns','submit_registry_released_ns','route_enqueue_ns','route_started_ns']]
            self.assertEqual(points,sorted(points))
            for field in ['report_registry_lock_wait_ms','report_query_lock_wait_ms',
                          'report_receive_to_handler_ms','report_validation_under_query_lock_ms','hnsw_cpu_ms']:
                self.assertGreaterEqual(stats['head_stage_timings_ms'][field],0)
        metrics=engine.runtime.metrics();self.assertTrue(metrics['profile_head_enabled'])
        self.assertGreaterEqual(metrics['maintenance_registry_hold_ns'],metrics['maintenance_registry_hold_max_ns'])
    def test_bad_dimensions_do_not_poison_connection(self):
        engine,opts=self.setup_api()
        with socket.create_connection((opts['host'],opts['port'])) as s:
            send(s,dict(version=1,op='search',encoding='float64_le',vector=b'x',query_id=0),1048576)
            reply,_=receive(s,1048576);self.assertFalse(reply['ok'])
            send(s,dict(version=1,op='search',encoding='float64_le',vector=self.x[0].astype('<f8').tobytes(),query_id=0),1048576)
            reply,_=receive(s,1048576);self.assertEqual(reply['status'],'ok')

    def test_disconnect_and_close_with_outstanding_queries(self):
        engine,opts=self.setup_api()
        for i in range(8):
            with socket.create_connection((opts['host'],opts['port'])) as sock:
                send(sock,dict(version=1,op='search',encoding='float64_le',vector=self.x[i].astype('<f8').tobytes(),query_id=i),1048576)
        engine.runtime.close()
        self.assertEqual(engine.runtime.metrics()['active'],0)

    def test_metrics_during_unbounded_receive_scaling_and_queries(self):
        c=self.config(True,True)
        c['native_runtime']['network_module'].update(
            enabled=True,transport='tcp',send_min_reactors=1,send_max_reactors=0,
            receive_min_reactors=1,receive_max_reactors=0,
            connections_per_endpoint_per_reactor=0,max_receive_connections=0,
            receive_queue_high=1,high_samples=1,sample_interval_ms=2,
            cooldown_ms=4,rebalance_connections=True)
        self.start(c);engine=NativeEngine(c,external_completion=True);self.addCleanup(engine.close)
        settings={'api':dict(host='127.0.0.1',port=0,handlers=1,queue_capacity=256,
            max_connections=0,max_message_bytes=1048576,reactors=1,max_reactors=0)}
        port=engine.runtime.start_api(api_options(c,settings))
        opts=dict(host='127.0.0.1',port=port,lanes=2,connections=64,timeout_ms=10000,
            max_message_bytes=1048576,consumer_queue_records=256,consumer_queue_bytes=64<<20)
        stop=threading.Event();snapshots=[];errors=[]
        def monitor():
            try:
                while not stop.is_set():
                    snapshots.append((engine.runtime.metrics(),engine.runtime.api_metrics()))
                    stop.wait(.002)
            except BaseException as exc:errors.append(exc)
        thread=threading.Thread(target=monitor);thread.start()
        root=Path(tempfile.mkdtemp(dir=self.root))
        try:
            m=native().run_network_load(self.x[:96],np.arange(96,dtype=np.uint64),64,opts,str(root/'results'))
        finally:
            stop.set();thread.join(10)
        self.assertFalse(thread.is_alive());self.assertFalse(errors);self.assertTrue(snapshots)
        self.assertFalse(m['error']);rows=list(records(root/'results'))
        self.assertEqual({row['job'] for row in rows},set(range(96)))
        failed=[row for row in rows if not row['transport_ok'] or row['result']['status']!='ok']
        self.assertFalse(failed, failed[:2])
        self.assertEqual(engine.runtime.metrics()['active'],0)
        self.assertEqual(engine.runtime.api_metrics()['network_module']['max_reactors'],0)

    def test_paced_arrivals_are_timed_without_python_consumer(self):
        engine,opts=self.setup_api();root=Path(tempfile.mkdtemp(dir=self.root))
        opts.update(target_qps=200.,pacing_startup_ms=20)
        m=native().run_network_load(self.x[:48],np.arange(48,dtype=np.uint64),16,opts,str(root/'results'))
        self.assertFalse(m['error']);self.assertEqual(m['received'],48)
        rows=sorted(records(root/'results'),key=lambda r:r['job'])
        self.assertEqual(m['target_qps'],200.)
        for row in rows:
            self.assertEqual(row['client_scheduled_ns'],m['offered_ns']+row['job']*5000000)
            self.assertGreaterEqual(row['client_start_ns'],row['client_scheduled_ns'])
        self.assertGreaterEqual(m['last_received_ns']-m['offered_ns'],47*5000000)
        for value in [-1.,float('inf'),float('nan')]:
            with self.assertRaisesRegex(Exception,'Invalid offered query rate'):native().run_network_load(self.x[:1],np.arange(1,dtype=np.uint64),2,dict(opts,target_qps=value),str(root/('invalid_'+str(value))))
