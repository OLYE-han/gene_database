"""Native network scaling, global limits, framing, drain and lifecycle checks."""
import time
import unittest
import os
from pathlib import Path
from dhdgs.index import native


def policy(**overrides):
    p=dict(enabled=True,send_min_reactors=1,send_max_reactors=3,receive_min_reactors=1,
           receive_max_reactors=3,handlers_per_receive_reactor=1,
           connections_per_endpoint_per_reactor=8,send_queue_capacity=256,
           receive_queue_capacity_per_reactor=256,max_receive_connections=64,
           send_pending_high=2,receive_queue_high=1,high_samples=1,
           sample_interval_ms=2,cooldown_ms=4,idle_ms=20)
    p.update(overrides)
    return {'native_runtime':{'network_module':p}}


class NetworkModuleTests(unittest.TestCase):
    def test_unbounded_connections_and_reactors_grow_and_reclaim(self):
        cfg=policy(send_max_reactors=0,receive_max_reactors=0,
                   connections_per_endpoint_per_reactor=0,max_receive_connections=0,
                   min_connections_per_endpoint_per_reactor=1,async_connect=True,
                   rebalance_connections=True,idle_ms=100)
        c,s=self.make(cfg,delay=10);done=[]
        for i in range(120):self.assertTrue(c.submit('127.0.0.1',s.port(),{'request_id':i}))
        self.wait(lambda:(done.extend(c.poll()) or len(done)==120),timeout=10)
        self.assertTrue(all(x['ok'] for x in done))
        self.assertEqual(len({x['response']['request_id'] for x in done}),120)
        self.assertEqual(c.metrics()['max_reactors'],0)
        self.assertEqual(s.metrics()['max_connections'],0)
        self.assertGreater(c.metrics()['peak_reactors'],1)
        self.assertGreater(s.metrics()['network_module']['peak_reactors'],1)
        self.wait(lambda:c.metrics()['reactors']==1,timeout=10)
        self.wait(lambda:sum(w['ready_connections'] for w in c.metrics()['wire_lanes'])<=1,timeout=10)
        self.wait(lambda:s.metrics()['network_module']['reactors']==1,timeout=10)
        self.assertTrue(c.submit('127.0.0.1',s.port(),{'request_id':999}))
        done.clear();self.wait(lambda:(done.extend(c.poll()) or bool(done)))
        self.assertTrue(done[0]['ok'])
    def setUp(self):self.resources=[]
    def tearDown(self):
        for obj in reversed(self.resources):obj.close()
    def make(self,c=None,delay=0):
        c=c or policy();server=native().NetworkProbeServer(c,delay);self.resources.append(server)
        client=native().NetworkProbeClient(c);self.resources.append(client)
        return client,server
    def wait(self,fn,timeout=4):
        until=time.monotonic()+timeout
        while time.monotonic()<until:
            if fn():return
            time.sleep(.005)
        self.fail('network condition timed out')
    def test_send_receive_scale_and_drain_without_loss(self):
        c,s=self.make(delay=10);done=[];accepted=set()
        for i in range(120):
            if c.submit('127.0.0.1',s.port(),{'request_id':i,'payload':b'x'*128}):accepted.add(i)
            time.sleep(.001)
        def finished():done.extend(c.poll());return len(done)==len(accepted)
        self.wait(finished)
        self.assertTrue(all(r['ok'] for r in done))
        self.assertEqual({r['response']['request_id'] for r in done},accepted)
        self.assertEqual(len(done),len(accepted))
        self.assertGreater(c.metrics()['peak_reactors'],1)
        self.assertGreater(s.metrics()['network_module']['peak_reactors'],1)
        self.assertLessEqual(s.metrics()['connections'],64)
        self.wait(lambda:c.metrics()['reactors']==1)
        c.close()
        self.wait(lambda:s.metrics()['network_module']['reactors']==1)
        self.assertGreater(s.metrics()['network_module']['scale_downs'],0)
        # Listener still serves on the identical port after contraction.
        other=native().NetworkProbeClient(policy());self.resources.append(other)
        self.assertTrue(other.submit('127.0.0.1',s.port(),{'request_id':999}))
        got=[]
        self.wait(lambda:(got.extend(other.poll()) or bool(got)))
        self.assertTrue(got[0]['ok']);self.assertEqual(got[0]['response']['request_id'],999)
    @unittest.skipUnless(hasattr(os, 'sched_getaffinity'), 'Linux affinity required')
    def test_network_cpu_reservation_and_invalid_cpu_cleanup(self):
        cpu=min(os.sched_getaffinity(0))
        before={int(p.name) for p in Path('/proc/self/task').iterdir()}
        c,s=self.make(policy(cpu_ids=[cpu],async_connect=True,send_max_reactors=1,receive_max_reactors=1))
        created={int(p.name) for p in Path('/proc/self/task').iterdir()}-before
        self.assertGreaterEqual(len(created),4)
        for tid in created:self.assertEqual(os.sched_getaffinity(tid),{cpu})
        self.assertTrue(c.submit('127.0.0.1',s.port(),{'request_id':71}))
        got=[];self.wait(lambda:(got.extend(c.poll()) or bool(got)))
        self.assertTrue(got[0]['ok'])
        c.close();s.close()
        for cls in (native().NetworkProbeClient,native().NetworkProbeServer):
            with self.assertRaises((ValueError,RuntimeError)):
                cls(policy(cpu_ids=[-1]))
    def test_async_connector_failure_recovery_and_close(self):
        import socket
        cfg=policy(async_connect=True,send_min_reactors=1,send_max_reactors=1,
                   max_connecting_per_reactor=2)
        c,s=self.make(cfg,delay=20)
        with socket.socket() as unused:
            unused.bind(('127.0.0.1',0));unused_port=unused.getsockname()[1]
            self.assertTrue(c.submit('127.0.0.1',unused_port,{'request_id':0}))
            got=[];self.wait(lambda:(got.extend(c.poll()) or bool(got)))
        self.assertFalse(got[0]['ok'])
        for i in range(1,41):self.assertTrue(c.submit('127.0.0.1',s.port(),{'request_id':i}))
        self.wait(lambda:(got.extend(c.poll()) or len(got)==41))
        self.assertTrue(all(r['ok'] for r in got[1:]))
        self.assertEqual(len({r['response']['request_id'] for r in got[1:]}),40)
        for i in range(41,61):self.assertTrue(c.submit('127.0.0.1',s.port(),{'request_id':i}))
        c.close();got.extend(c.poll());self.assertEqual(len(got),61)
        self.assertEqual(c.metrics()['pending'],0)
        self.assertEqual(c.metrics()['wire_lanes'][0]['connecting'],0)
    def test_send_limit_is_global_across_reactors(self):
        c,s=self.make(policy(send_min_reactors=3,send_max_reactors=3,send_queue_capacity=4),delay=100)
        accepted=sum(c.submit('127.0.0.1',s.port(),{'request_id':i}) for i in range(20))
        self.assertEqual(accepted,4);self.assertEqual(c.metrics()['pending'],4)
        c.close();done=c.poll();self.assertEqual(len(done),accepted)
        self.assertFalse(c.submit('127.0.0.1',s.port(),{'request_id':100}))
        c.close()
    def test_disabled_module_uses_one_reactor(self):
        c,s=self.make(policy(enabled=False));self.assertEqual(c.metrics()['reactors'],1)
        self.assertEqual(s.metrics()['network_module']['reactors'],1)
        self.assertTrue(c.submit('127.0.0.1',s.port(),{'request_id':'abc','nested':[1,2,3]}))
        got=[];self.wait(lambda:(got.extend(c.poll()) or bool(got)))
        self.assertTrue(got[0]['ok']);self.assertEqual(got[0]['response']['nested'],[1,2,3])
    def test_rejects_invalid_limits_and_overlapping_controllers(self):
        for cfg in [policy(send_min_reactors=4,send_max_reactors=2),policy(receive_min_reactors=0)]:
            with self.assertRaises(ValueError):native().NetworkProbeClient(cfg)
        cfg=policy();cfg['native_runtime']['adaptive_control']={'enabled':True}
        with self.assertRaises(ValueError):native().NetworkProbeClient(cfg)

    def test_receive_connection_limit_is_shared_by_reactors(self):
        import socket
        c,s=self.make(policy(receive_min_reactors=3,receive_max_reactors=3,max_receive_connections=2))
        sockets=[]
        try:
            for _ in range(10):sockets.append(socket.create_connection(('127.0.0.1',s.port()),timeout=1))
            self.wait(lambda:s.metrics()['rejected']>0)
            self.assertLessEqual(s.metrics()['connections'],2)
        finally:
            for sock in sockets:sock.close()

    def test_warm_connections_and_persistent_receive_rebalance(self):
        cfg=policy(async_connect=True,async_connect_progress=False,send_min_reactors=1,send_max_reactors=1,
                   min_connections_per_endpoint_per_reactor=16,
                   connections_per_endpoint_per_reactor=16,max_connecting_per_reactor=2,
                   rebalance_connections=True,rebalance_interval_ms=2,rebalance_batch=4,
                   idle_ms=200,send_pressure_mode='queued')
        c,s=self.make(cfg,delay=8)
        self.assertTrue(c.submit('127.0.0.1',s.port(),{'request_id':0}))
        got=[];self.wait(lambda:(got.extend(c.poll()) or bool(got)))
        self.wait(lambda:c.metrics()['wire_lanes'][0]['ready_connections']==16)
        for i in range(1,161):self.assertTrue(c.submit('127.0.0.1',s.port(),{'request_id':i}))
        self.wait(lambda:(got.extend(c.poll()) or len(got)==161))
        self.assertTrue(all(r['ok']for r in got),got[:1])
        self.assertEqual(len({r['response']['request_id']for r in got}),161)
        self.assertGreater(s.metrics()['migrated_connections'],0)
        self.assertEqual(c.metrics()['wire_lanes'][0]['connection_attempts'],16)
        self.assertEqual(c.metrics()['wire_lanes'][0]['connection_failures'],0)
        # Keep the client's persistent sockets OPEN while the server contracts.
        # Balanced retiring lanes must be evacuated, not left stuck draining.
        self.wait(lambda:s.metrics()['network_module']['reactors']==1)
        self.assertEqual(s.metrics()['connections'],16)
        self.assertTrue(c.submit('127.0.0.1',s.port(),{'request_id':161}))
        last=[];self.wait(lambda:(last.extend(c.poll()) or bool(last)))
        self.assertTrue(last[0]['ok'])
        c.close();self.wait(lambda:s.metrics()['connections']==0)
