"""Native network scaling, global limits, framing, drain and lifecycle checks."""
import time
import unittest
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
