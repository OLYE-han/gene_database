"""Optional real-hardware RDMA tests; no software emulation or silent fallback."""
import os,time,unittest
from dhdgs.index import native
from dhdgs.network_api import api_options
from test_native_network import policy

class RdmaTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('DHDGS_TEST_RDMA_HOST'),'Requires actual IB')
    def test_unbounded_real_rdma(self):
        host=os.environ['DHDGS_TEST_RDMA_HOST'];cfg=policy(transport='rdma',
            send_max_reactors=0,receive_max_reactors=0,connections_per_endpoint_per_reactor=0,
            max_receive_connections=0,async_connect=True,min_connections_per_endpoint_per_reactor=1,
            rebalance_connections=True,idle_ms=100)
        cfg['probe_host']=host
        server=native().NetworkProbeServer(cfg,5);client=native().NetworkProbeClient(cfg)
        try:
            for i in range(160):self.assertTrue(client.submit(host,server.port(),{'request_id':i}))
            got=[];deadline=time.monotonic()+15
            while len(got)<160 and time.monotonic()<deadline:got.extend(client.poll());time.sleep(.005)
            self.assertEqual(len(got),160);self.assertTrue(all(v['ok'] for v in got))
            self.assertEqual(client.metrics()['max_reactors'],0)
            self.assertEqual(server.metrics()['max_connections'],0)
            deadline=time.monotonic()+10
            while time.monotonic()<deadline and (client.metrics()['reactors']!=1 or server.metrics()['network_module']['reactors']!=1):time.sleep(.02)
            self.assertEqual(client.metrics()['reactors'],1)
            self.assertEqual(server.metrics()['network_module']['reactors'],1)
        finally:client.close();server.close()
    def test_transport_selection_rejects_typo(self):
        with self.assertRaises(ValueError):native().NetworkProbeClient(policy(transport='rdam'))
    def test_public_api_remains_tcp(self):
        config=policy(transport='rdma')
        settings={'api':dict(reactors=2,handlers=1,queue_capacity=32,max_connections=32)}
        self.assertEqual(api_options(config,settings)['native_runtime']['network_module']['transport'],'tcp')
        self.assertEqual(config['native_runtime']['network_module']['transport'],'rdma')
    @unittest.skipUnless(os.environ.get('DHDGS_TEST_RDMA_HOST'),'Requires explicit IB IPv4 for hardware test')
    def test_real_rdma_multireactor_framing_and_drain(self):
        self.assertTrue(native().rdma_available())
        host=os.environ['DHDGS_TEST_RDMA_HOST']
        cfg=policy(transport='rdma',send_min_reactors=4,send_max_reactors=4,receive_min_reactors=8,receive_max_reactors=8)
        cfg['probe_host']=host
        server=native().NetworkProbeServer(cfg);client=native().NetworkProbeClient(cfg)
        try:
            sizes=[1,128,20480,131072]*64
            for i,size in enumerate(sizes):self.assertTrue(client.submit(host,server.port(),{'request_id':i,'payload':b'x'*size}))
            got=[];deadline=time.monotonic()+20
            while len(got)<len(sizes) and time.monotonic()<deadline:
                got.extend(client.poll());time.sleep(.002)
            self.assertEqual(len(got),len(sizes));self.assertTrue(all(x['ok'] for x in got),got[:1])
            for x in got:self.assertEqual(x['response']['payload'],b'x'*sizes[x['response']['request_id']])
            self.assertEqual(server.metrics()['transport'],'rdma');self.assertEqual(client.metrics()['transport'],'rdma')
            self.assertEqual(client.metrics()['pending'],0)
        finally:client.close();server.close()
    @unittest.skipUnless(os.environ.get('DHDGS_TEST_RDMA_HOST'),'Requires explicit IB IPv4 for hardware test')
    def test_real_rdma_dynamic_warm_rebalance(self):
        host=os.environ['DHDGS_TEST_RDMA_HOST']
        cfg=policy(transport='rdma',async_connect=True,send_min_reactors=1,send_max_reactors=3,
                   receive_min_reactors=1,receive_max_reactors=3,
                   connections_per_endpoint_per_reactor=16,
                   min_connections_per_endpoint_per_reactor=8,max_connecting_per_reactor=2,
                   rebalance_connections=True,rebalance_interval_ms=2,rebalance_batch=4,
                   send_pressure_mode='queued',idle_ms=100)
        cfg['probe_host']=host
        server=native().NetworkProbeServer(cfg,3);client=native().NetworkProbeClient(cfg)
        try:
            got=[]
            for i in range(220):self.assertTrue(client.submit(host,server.port(),{'request_id':i,'payload':b'x'*20480}))
            deadline=time.monotonic()+15
            while len(got)<220 and time.monotonic()<deadline:got.extend(client.poll());time.sleep(.002)
            self.assertEqual(len(got),220);self.assertTrue(all(x['ok']for x in got),got[:1])
            self.assertEqual(len({x['response']['request_id']for x in got}),220)
            self.assertGreater(client.metrics()['peak_reactors'],1)
            self.assertGreater(server.metrics()['network_module']['peak_reactors'],1)
            self.assertGreater(server.metrics()['migrated_connections'],0)
            self.assertLessEqual(server.metrics()['connections'],64)
            self.assertEqual(client.metrics()['pending'],0)
            deadline=time.monotonic()+5
            while server.metrics()['network_module']['reactors']!=1 and time.monotonic()<deadline:time.sleep(.01)
            self.assertEqual(server.metrics()['network_module']['reactors'],1)
            self.assertGreater(server.metrics()['connections'],0)
            self.assertTrue(client.submit(host,server.port(),{'request_id':220}))
            again=[];deadline=time.monotonic()+5
            while not again and time.monotonic()<deadline:again.extend(client.poll());time.sleep(.002)
            self.assertEqual(len(again),1);self.assertTrue(again[0]['ok'])
        finally:client.close();server.close()
