import copy
import json
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import numpy as np
from threadpoolctl import threadpool_limits

from dhdgs.config import snapshot, validate
from dhdgs.index import build, Shard
from dhdgs.peer import BranchRegistry, PeerRuntime, execution_signature
from dhdgs.peer_search import run_branch
from dhdgs.scheduler import remote_probability
from dhdgs.search import Engine
from dhdgs.cluster_test import run_local_cluster
from test_system import fixture


class ProbabilityTests(unittest.TestCase):
    def test_weight_monotonic_and_limits(self):
        values=[remote_probability(2,4,w,1) for w in (0,.1,1,10,100)]
        self.assertEqual(values,sorted(values))
        self.assertEqual(values[0],0)
        self.assertAlmostEqual(remote_probability(2,2,1,1),.5)
        self.assertAlmostEqual(remote_probability(2,4,1,1),1/3)
        self.assertEqual(remote_probability(1e300,1e-300,1,1),1.)
        for temperature in (.25,1.,4.):
            self.assertAlmostEqual(remote_probability(2,2,1,temperature),.5)

    def test_manifest_waits_for_children_and_enforces_budget(self):
        reg=BranchRegistry('q','t','root',0,10,2,3)
        root={'branch_id':'root','parent_branch_id':None,'node_id':0,'work_grant':10,'branch_slots':2,
              'visited_nodes':[0],
              'work_spent':4,'work_unused':0,'children':[{'branch_id':'child','parent_branch_id':'root',
                  'node_id':1,'work_grant':6,'branch_slots':1}]}
        child={'branch_id':'child','parent_branch_id':'root','node_id':1,'work_grant':6,'branch_slots':1,
               'visited_nodes':[0,1],
               'work_spent':6,'work_unused':0,'children':[]}
        def put(report):
            reg.update({'query_request_id':'q','token':'t','node_id':report['node_id'],
                        'involved_nodes':[0,1],'reports':[report]})
        put(child);self.assertFalse(reg.complete())
        put(root);self.assertTrue(reg.complete())
        put(root);self.assertTrue(reg.complete())
        reg.reports['child']['work_spent']=7
        with self.assertRaisesRegex(ValueError,'conservation'):reg.complete()


class PeerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.c,self.x=fixture(self.root)
        self.c['search'].update(execution='peer',max_expansions=192,max_inflight=3)
        self.c['scheduler'].update(mode='adaptive',remote_weight=2)
        self.c['peer'].update(max_branches=16,heartbeat_interval_ms=10,lease_timeout_ms=1000,
                              query_timeout_seconds=10.,control_timeout_seconds=.2)
        with threadpool_limits(limits=1):build(self.c)

    def tearDown(self):self.temp.cleanup()

    def runtime_state(self,shard):
        gid=int(shard.ids[0])
        return {'query_request_id':'q','branch_id':'root','parent_branch_id':None,'token':'secret',
                'visited_nodes':[0],
                'entry_node':0,'target_node':0,'epoch':shard.meta['epoch'],
                'signature':execution_signature(self.c),'query_f32':self.x[0].tobytes(),
                'work_grant':12,'branch_slots':1,'seeds':[[gid,0.,0.,[0],None,0,0,gid,0.]],
                'seen':[gid],'scale':1.,'decision_seed':42,'query_id':None}

    def test_zero_weight_config(self):
        c=copy.deepcopy(self.c);c['scheduler']['remote_weight']=0
        validate({k:v for k,v in c.items() if not k.startswith('_')})

    def test_submit_idempotency_conflict_and_capacity(self):
        shard=Shard(Path(self.c['paths']['output'])/'index/shard_0',0)
        started,release=threading.Event(),threading.Event()
        def blocked(*args):
            started.set()
            if not release.wait(3):raise TimeoutError('test worker was not released')
            return {}
        self.c['peer']['max_active_queries']=1
        try:
            with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',side_effect=blocked) as run:
                runtime=PeerRuntime(self.c,shard)
                try:
                    state=self.runtime_state(shard)
                    self.assertFalse(runtime.accept(state)['duplicate'])
                    self.assertTrue(started.wait(2))
                    self.assertTrue(runtime.accept(copy.deepcopy(state))['duplicate'])
                    conflict={**state,'work_grant':11}
                    with self.assertRaisesRegex(ValueError,'Conflicting'):runtime.accept(conflict)
                    with self.assertRaisesRegex(ValueError,'token'):runtime.lease('q','wrong')
                    with self.assertRaisesRegex(ValueError,'already visited'):runtime.accept({**state,'branch_id':'other'})
                    with self.assertRaisesRegex(RuntimeError,'capacity'):runtime.accept({**state,'query_request_id':'other-query'})
                    self.assertEqual(run.call_count,1)
                    self.assertTrue(runtime.lease('q','secret',cancel=True)['found'])
                    self.assertTrue(runtime.records[('q','root')]['cancelled'])
                finally:
                    release.set();runtime.close()
        finally:shard.close()

    def test_expired_lease_fails_branch(self):
        shard=Shard(Path(self.c['paths']['output'])/'index/shard_0',0)
        self.c['peer']['lease_timeout_ms']=30
        running=threading.Event()
        def check_lease(state,shard,directory,c,spawn,cancelled):
            running.set()
            deadline=time.monotonic()+2
            while not cancelled() and time.monotonic()<deadline:time.sleep(.002)
            if cancelled():raise TimeoutError('head lease expired')
            self.fail('Lease did not expire')
        try:
            with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',side_effect=check_lease):
                runtime=PeerRuntime(self.c,shard)
                try:
                    runtime.accept(self.runtime_state(shard))
                    self.assertTrue(running.wait(2))
                    runtime.workers.shutdown(wait=True)
                    rec=runtime.records[('q','root')]
                    self.assertEqual(rec['status'],'failed')
                    self.assertIn('lease expired',rec['error'])
                    self.assertIsNone(rec['report'])
                finally:runtime.close()
        finally:shard.close()

    def test_transfer_retries_same_identity_then_fails_explicitly(self):
        runtime=PeerRuntime.__new__(PeerRuntime)
        runtime.c=self.c;runtime.lock=threading.Lock();runtime.stop=threading.Event()
        rec={'cancelled':False,'peer_rpc_count':0,'peer_request_bytes':0,
             'peer_response_bytes':0,'peer_transfer_errors':0}
        runtime.records={('q','r'):rec}
        state={'target_node':1,'branch_id':'child'}
        with patch('dhdgs.peer.exchange',side_effect=TimeoutError('lost ACK')) as transfer:
            with self.assertRaisesRegex(TimeoutError,'lost ACK'):runtime._transfer(('q','r'),state)
            self.assertEqual(transfer.call_count,self.c['network']['max_retries']+1)
            self.assertTrue(all(call.args[1]['state'] is state for call in transfer.call_args_list))
        self.assertEqual(rec['peer_transfer_errors'],rec['peer_rpc_count'])

    def test_local_branch_credits_and_topk_merge(self):
        engine=Engine(self.c)
        try:
            r=engine.search(self.x[0],0)
            s=r['stats']
            self.assertLessEqual(s['expanded'],192)
            self.assertEqual(s['router_expand_rpc_count'],0)
            self.assertEqual(s['rpc_count'],0)
            self.assertGreater(s['branch_count'],1)
            self.assertLessEqual(s['branch_count'],3)
            self.assertEqual(s['node_revisit_count'],0)
            self.assertEqual(len(set(r['ids'])),len(r['ids']))
            self.assertNotIn(0,r['ids'])
            combined={}
            for report in r['branch_reports']:
                self.assertEqual(report['work_spent']+report['work_unused']+
                    sum(ch['work_grant'] for ch in report['children']),report['work_grant'])
                combined.update(zip(report['ids'],report['squared_distances']))
            expected=sorted(combined,key=lambda gid:(combined[gid],gid))[:self.c['search']['top_k']]
            self.assertEqual(r['ids'],expected)
        finally:engine.close()

    def test_data_loop_continues_after_fork(self):
        # Deterministic graph-like shard: a remote seed is emitted while local work remains.
        class FakeShard:
            meta={'shard_id':0}
            def expand(self,q,ids,epoch):
                neighbors=[];parents=[]
                for gid in ids:
                    for target in (gid+1,100+gid):
                        neighbors.append([target,float(target%100),.1]);parents.append([target,gid])
                from dhdgs.peer_search import COUNTERS
                stats={k:0 for k in COUNTERS};stats['service_ms']=.01;stats['exact_distance_evaluations']=len(ids)
                return {'expanded':[[gid,float(gid)] for gid in ids], 'neighbors':neighbors,
                        'parents':parents,'stats':stats,'service_trace':{}}
        c=copy.deepcopy(self.c);c['search'].update(local_beam=1,max_expansions=12,fallback_enabled=False)
        c['scheduler'].update(remote_weight=1e9,max_wait_ms=100.)
        state={'query_f32':np.zeros(64,dtype='f4').tobytes(),'query_id':None,'epoch':'e','branch_id':'r',
               'visited_nodes':[0],
               'parent_branch_id':None,'work_grant':12,'branch_slots':5,'seeds':[[0,0.,0.,[0],None,0,0,0,0.]],
               'seen':[0],'scale':1.,'decision_seed':42}
        forks=[]
        report=run_branch(state,FakeShard(),{i:([0] if i<100 else [1]) for i in range(1000)},c,
                          lambda child,desc:forks.append(child),lambda:False)
        self.assertTrue(forks)
        self.assertEqual(len(forks),1)
        self.assertEqual(forks[0]['visited_nodes'],[0,1])
        self.assertTrue(all(1 in seed[3] for seed in forks[0]['seeds']))
        first=report['fork_events'][0]['local_expansions_before']
        self.assertGreater(report['work_spent'],first)

    def test_three_process_peer_protocol(self):
        c=copy.deepcopy(self.c);c['network']['transport']='tcp'
        reservations=[socket.socket() for _ in range(3)]
        try:
            for node,sock in zip(c['cluster']['nodes'],reservations):
                sock.bind(('127.0.0.1',0));node['port']=sock.getsockname()[1]
        finally:
            for sock in reservations:sock.close()
        c['experiment'].update(benchmark_concurrency=2,benchmark_repetitions=1)
        directory=self.root/'run';snapshot(c,directory)
        report=run_local_cluster(c,directory)
        self.assertEqual(report['failed_queries'],0)
        self.assertEqual(report['successful_queries'],3)
        self.assertTrue(all(s['stopped'] for s in report['local_cluster']['services']))
        for q in report['queries']:
            s=q['stats']
            self.assertEqual(s['router_submit_rpc_count'],1)
            self.assertEqual(s['router_expand_rpc_count'],0)
            self.assertLessEqual(s['expanded'],192)
            self.assertGreater(s['control_rpc_count'],0)
            self.assertEqual(s['control_rpc_count'],sum(v['rpc_count'] for v in s['control_by_kind'].values()))
            self.assertEqual(s['control_by_kind']['cancel']['rpc_count'],0)
            self.assertEqual(s['control_by_kind']['lease']['rpc_count'],0)
            self.assertEqual(s['all_protocol_bytes'],s['wire_bytes']+s['control_wire_bytes'])
            self.assertEqual(s['peer_branch_rpc_count'],s['branch_count']-1)
            self.assertLessEqual(s['peer_branch_rpc_count'],2)
            self.assertEqual(s['node_revisit_count'],0)
            self.assertEqual(len(s['visited_nodes']),len(set(s['visited_nodes'])))
            self.assertTrue(set(s['shards_visited'])<=set(s['keepalive_nodes']))
            self.assertEqual(len(q['ids']),5)

    def test_cycle_is_filtered_but_local_replica_remains_searchable(self):
        from dhdgs.peer_search import COUNTERS
        class ShardWithCycle:
            def __init__(self,sid):self.meta={'shard_id':sid}
            def expand(self,q,ids,epoch):
                edges={0:[1,100,200],1:[2,100],2:[],100:[101,3],101:[102],
                       102:[],200:[201,4,103],201:[],3:[],4:[],103:[]}
                neighbors=[];parents=[]
                for gid in ids:
                    for target in edges[gid]:
                        neighbors.append([target,0.,.1]);parents.append([target,gid])
                stats={k:0 for k in COUNTERS};stats['service_ms']=.01
                stats['exact_distance_evaluations']=len(ids)
                return {'expanded':[[gid,float(gid)] for gid in ids],'neighbors':neighbors,
                        'parents':parents,'stats':stats,'service_trace':{}}
        owners={0:[0],1:[0],2:[0],3:[0],4:[0],100:[1],101:[1],102:[1],103:[1,2],200:[2],201:[2]}
        c=copy.deepcopy(self.c)
        c['search'].update(local_beam=1,max_expansions=32,fallback_enabled=False)
        c['scheduler'].update(remote_weight=1e12,max_wait_ms=100.)
        root={'query_f32':np.zeros(64,dtype='f4').tobytes(),'query_id':None,'epoch':'e','branch_id':'r',
              'parent_branch_id':None,'work_grant':32,'branch_slots':15,'target_node':0,
              'visited_nodes':[0],'seeds':[[0,0.,0.,[0],None,0,0,0,0.]],'seen':[0],
              'scale':1.,'decision_seed':42}
        pending=[root];reports=[];states=[]
        while pending:
            state=pending.pop(0);states.append(state)
            reports.append(run_branch(state,ShardWithCycle(state['target_node']),owners,c,
                          lambda child,desc:pending.append(child),lambda:False))
        self.assertEqual([r['node_id'] for r in reports],[0,1,2])
        self.assertEqual([s['visited_nodes'] for s in states],[[0],[0,1],[0,1,2]])
        self.assertIn(200,[row[0] for row in states[1]['remote_frontier']])
        self.assertNotIn(1,[row[0] for row in states[1]['remote_frontier']])
        self.assertIn(2,reports[0]['expanded_ids'])  # Original node still completes local work.
        self.assertNotIn(3,reports[1]['expanded_ids'])
        self.assertNotIn(4,reports[2]['expanded_ids'])
        self.assertIn(103,reports[2]['expanded_ids']) # Replica is local despite its other owner being visited.
        self.assertGreater(sum(r['stats']['visited_node_candidates_dropped'] for r in reports),0)
        self.assertLessEqual(sum(r['work_spent'] for r in reports),32)
        for report in reports:
            self.assertLessEqual(len(report['children']),1)
            self.assertEqual(report['work_spent']+report['work_unused']+
                             sum(ch['work_grant'] for ch in report['children']),report['work_grant'])
