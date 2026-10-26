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
    def setup_api(self):
        c=self.config(True,True);self.start(c);engine=NativeEngine(c,external_completion=True);self.addCleanup(engine.close)
        settings={'api':dict(host='127.0.0.1',port=0,handlers=2,queue_capacity=64,max_connections=128,max_message_bytes=1048576,reactors=1)}
        port=engine.runtime.start_api(api_options(c,settings));return engine,dict(host='127.0.0.1',port=port,lanes=2,connections=16,timeout_ms=10000,max_message_bytes=1048576,consumer_queue_records=64,consumer_queue_bytes=64<<20)
    def test_stream_consumption_and_no_head_retention(self):
        engine,opts=self.setup_api();root=Path(tempfile.mkdtemp(dir=self.root))
        m=native().run_network_load(self.x[:48],np.arange(48,dtype=np.uint64),16,opts,str(root/'results'))
        self.assertFalse(m['error']);rows=list(records(root/'results'));self.assertEqual({q['job'] for q in rows},set(range(48)))
        for q in rows:
            self.assertTrue(q['transport_ok']);self.assertEqual(q['result']['status'],'ok');self.assertLessEqual(q['client_done_ns'],q['consume_started_ns'])
            self.assertLessEqual(q['result']['stats']['expanded'],engine.c['search']['max_expansions'])
        self.assertEqual(engine.runtime.metrics()['completed_pending'],0);self.assertEqual(engine.runtime.metrics()['active'],0)
        self.assertEqual(sum(v['consumed'] for v in m['lanes']),48)
        for lane in m['lanes']:
            self.assertGreaterEqual(lane['output_sync_started_ns'],m['last_received_ns'])
            self.assertGreaterEqual(lane['output_sync_barrier_ms'],0)
    def test_consumer_capacity_failure_is_explicit(self):
        engine,opts=self.setup_api();opts['consumer_queue_bytes']=1
        root=Path(tempfile.mkdtemp(dir=self.root));m=native().run_network_load(self.x[:32],np.arange(32,dtype=np.uint64),16,opts,str(root/'results'))
        self.assertIn('capacity',m['error']);self.assertEqual(engine.runtime.metrics()['active'],0)
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
        self.assertTrue(all(row['transport_ok'] and row['result']['status']=='ok' for row in rows))
        self.assertEqual(engine.runtime.metrics()['active'],0)
        self.assertEqual(engine.runtime.api_metrics()['network_module']['max_reactors'],0)
