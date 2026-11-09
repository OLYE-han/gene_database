"""Real three-node native first-hop routing and read-only load snapshots."""
import copy
import time
import unittest
import test_replicas as fixture

class EntryRoutingTests(unittest.TestCase):
    setUpClass=classmethod(fixture.ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(fixture.ReplicaTests.tearDownClass.__func__)
    setUp=fixture.ReplicaTests.setUp
    rpc=fixture.ReplicaTests.rpc
    info=fixture.ReplicaTests.info
    copy_source=fixture.ReplicaTests.copy_source

    def configure_replica_test(self,c):
        c['load_balance']['entry_routing']=dict(mode='fixed',telemetry=True,poll_ms=2,ttl_ms=200,
            temperature_ms=5.,prior_ms=5.,cpu_threshold=.9,cpu_penalty_ms=2.)

    def mode(self,name):
        options=copy.deepcopy(self.c['load_balance']['entry_routing']);options['mode']=name
        self.engine.runtime.entry_routing(options)

    def fresh(self):
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            m=self.engine.runtime.entry_routing_metrics()
            if all(n['polls']>0 and n['snapshot_age_ms']<100 for n in m['nodes']):return m
            time.sleep(.01)
        self.fail(str(m))

    def test_live_selection_and_budget(self):
        info,a,b=self.copy_source('route')
        route=dict(source=a,target=b,version='route',entries=[info['entry_id']],fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=10000)
        self.engine.runtime.replica_routes([route]);self.fresh()
        for mode in ['fixed','least_queue','cost']:
            self.mode(mode)
            results=[self.engine.search_async(self.x[0],0).result(10) for _ in range(24)]
            targets=[next(r['node_id']for r in q['branch_reports']if r['parent_branch_id'] is None)for q in results]
            self.assertTrue(set(targets)<={a,b})
            if mode=='fixed':self.assertEqual(set(targets),{b})
            for q in results:self.assertLessEqual(q['stats']['expanded'],256)
            self.assertTrue(all(n['outstanding_roots']==0 for n in self.engine.runtime.entry_routing_metrics()['nodes']))
        route['fraction']=0;self.engine.runtime.replica_routes([route]);self.mode('least_queue')
        q=self.engine.search_async(self.x[0],0).result(10)
        self.assertNotIn('replica_version',q['stats'])
        route['fraction']=1.;route['ttl_ms']=1;self.engine.runtime.replica_routes([route]);time.sleep(.01)
        q=self.engine.search_async(self.x[0],0).result(10);self.assertNotIn('replica_version',q['stats'])

    def test_missing_replica_fallback_and_reconfiguration(self):
        info=self.info();a=info['source'];b=(a+1)%3;self.fresh()
        self.engine.runtime.replica_routes([dict(source=a,target=b,version='missing',entries=[info['entry_id']],fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=10000)])
        q=self.engine.search_async(self.x[0],0).result(10)
        self.assertTrue(q['stats']['replica_unavailable_fallback'])
        self.assertTrue(all(n['outstanding_roots']==0 for n in self.engine.runtime.entry_routing_metrics()['nodes']))
        for option in [dict(self.c['load_balance']['entry_routing'],mode='bad'),dict(self.c['load_balance']['entry_routing'],poll_ms=3)]:
            with self.assertRaises(Exception):self.engine.runtime.entry_routing(option)
