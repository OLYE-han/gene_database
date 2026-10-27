"""Physical revisits must preserve branch identity, credits and dwell rules."""
import copy
import json
import socket
import time
import unittest
import numpy as np
from dhdgs.index import native
from dhdgs.native_search import run_native
from dhdgs.search import Engine
from dhdgs.peer import execution_signature
from dhdgs.transport import send, receive
from dhdgs.replicas import region_quality
from test_cpp_runtime import NativeRuntimeTests
from test_native_search import NativeSearchTests

class RevisitSearchTests(unittest.TestCase):
    setUpClass=classmethod(NativeSearchTests.setUpClass.__func__)
    tearDownClass=classmethod(NativeSearchTests.tearDownClass.__func__)
    setUp=NativeSearchTests.setUp
    state=NativeSearchTests.state

    def run_case(self,n,escape=False,exhaust=False):
        self.c['peer'].update(allow_node_revisit=True,min_expansions_after_cross=n,cross_on_local_exhaustion=escape)
        self.c['search'].update(local_beam=1)
        state=self.state();state.update(visited_nodes=[0,1,0],branch_slots=3,work_grant=32)
        remote=next(gid for gid,owners in self.directory.items() if 0 not in owners and 1 in owners)
        state['remote_frontier']=[[remote,-1e9,0.,[1],None,0,0,remote,0.]]
        state['seen'].append(remote)
        if exhaust:state['ancestor_expanded']=[int(x) for x in self.shard.ids]
        children=[]
        r=run_native(state,self.shard,self.directory,self.c,lambda s,d:children.append((s,d)),lambda:False)
        return r,children

    def test_revisit_and_minimum_successful_expansions(self):
        r,children=self.run_case(8)
        self.assertTrue(children)
        self.assertGreaterEqual(r['fork_events'][0]['local_expansions_before'],8)
        self.assertEqual(children[0][0]['visited_nodes'],[0,1,0,1])
        history=children[0][0]['ancestor_expanded']
        self.assertEqual(len(history),8)
        self.assertEqual(r['work_spent']+r['work_unused']+sum(d['work_grant'] for _,d in children),32)
        self.assertNotIn(children[0][0]['seeds'][0][0],history)

    def test_frontier_exhaustion_exception_is_explicit(self):
        strict,_=self.run_case(8,False,True)
        self.assertEqual(strict['stats']['stop_reason'],'minimum_local_expansions_unmet')
        escaped,children=self.run_case(8,True,True)
        self.assertEqual(len(children),1)
        self.assertEqual(escaped['work_spent'],0)
        self.assertEqual(escaped['stats']['cross_dwell_exhaustion_escapes'],1)

    def test_boundary_entry_gets_less_weight_without_changing_edges(self):
        adjacency={0:[1],1:[2],2:[3],3:[99]};old=copy.deepcopy(adjacency)
        q=region_quality([0,1,2,3],adjacency,[0,3],dict(quality_walk_steps=4,entry_weight_floor=.1,max_peer_entries=4))
        self.assertGreater(q['entry_weights']['0'],q['entry_weights']['3'])
        self.assertEqual(old,adjacency)
        self.assertTrue(all(0<w<=1 for _,w in q['peer_entries']))

class RevisitRuntimeTests(unittest.TestCase):
    setUpClass=classmethod(NativeRuntimeTests.setUpClass.__func__)
    tearDownClass=classmethod(NativeRuntimeTests.tearDownClass.__func__)
    config=NativeRuntimeTests.config
    start=NativeRuntimeTests.start

    def test_concurrent_revisits_finish_and_conserve_budget(self):
        c=self.config(True,True)
        c['peer'].update(allow_node_revisit=True,min_expansions_after_cross=4,cross_on_local_exhaustion=True)
        c['search'].update(max_expansions=200)
        c['peer']['max_active_queries']=4096
        c['peer']['deduplicate_node_expansions']=True
        c['scheduler'].update(remote_weight=.25)
        self.start(c);engine=Engine(c);self.addCleanup(engine.close)
        futures=[engine.search_async(x,i) for i,x in enumerate(self.x)]
        results=[f.result(15) for f in futures]
        self.assertTrue(all(q['status']=='ok' for q in results))
        self.assertTrue(any(q['stats']['node_revisit_count'] for q in results))
        for q in results:
            rows=q['branch_reports'];root=next(r for r in rows if r['parent_branch_id'] is None)
            self.assertLessEqual(q['stats']['expanded'],200)
            self.assertTrue(native().validate_manifests(rows,root['branch_id'],root['node_id'],200,root['branch_slots'],3,True))
            self.assertEqual(len(rows),len({r['branch_id'] for r in rows}))
            for node in range(3):
                expanded=[v for r in rows if r['node_id']==node for v in r['expanded_ids']]
                self.assertEqual(len(expanded),len(set(expanded)), 'Repeated physical-node expansion')
        self.assertEqual(engine.runtime.metrics()['active'],0)

    def test_two_visits_retry_conflict_query_wide_lease_and_cancel(self):
        c=self.config(True,True);c['peer'].update(allow_node_revisit=True,min_expansions_after_cross=1)
        servers=self.start(c)
        ids=np.load(self.root/'out/index/shard_0/ids.npy');gid=int(ids[0]);member=np.load(self.root/'out/index/membership.npy')
        epoch=json.loads((self.root/'out/index/manifest.json').read_text())['epoch']
        # No functioning completion endpoint is required for admission/lease.
        with socket.socket() as s:s.bind(('127.0.0.1',0));unused=s.getsockname()[1]
        state=dict(query_request_id='same-query',branch_id='r',parent_branch_id=None,token='test-token',
            head_endpoint=['127.0.0.1',unused],entry_node=0,target_node=0,epoch=epoch,signature=execution_signature(c),
            query_f32=self.x[0].tobytes(),query_id=0,work_grant=1,branch_slots=0,seeds=[[gid,0.,0.,list(map(int,np.flatnonzero(member[gid]))),None,0,0,gid,0.]],
            seen=[gid],fallback_entries=[],visited_nodes=[0],remote_frontier=[],fallback_remaining=0,scale=1.,ancestry=[],decision_seed=42)
        n=c['cluster']['nodes'][0]
        with socket.create_connection((n['host'],n['port']),3) as sock:
            def rpc(r):send(sock,dict(version=1,**r),1<<20);return receive(sock,1<<20)[0]
            self.assertTrue(rpc(dict(op='peer_submit',state=state))['result']['accepted'])
            second=copy.deepcopy(state);second.update(branch_id='r2',parent_branch_id='b',visited_nodes=[0,1,0],ancestry=[dict(branch_id='r',node_id=0),dict(branch_id='b',node_id=1)])
            self.assertFalse(rpc(dict(op='peer_submit',state=second))['result']['duplicate'])
            self.assertTrue(rpc(dict(op='peer_submit',state=second))['result']['duplicate'])
            bad=copy.deepcopy(second);bad['decision_seed']=1
            self.assertFalse(rpc(dict(op='peer_submit',state=bad))['ok'])
            for op in ('peer_lease','peer_cancel'):
                result=rpc(dict(op=op,query_request_id='same-query',token='test-token'))
                self.assertEqual(result['result']['branches'],2)

    def test_manifest_revisit_does_not_allow_branch_cycles_or_extra_credit(self):
        def report(bid,parent,node,path,grant,slots,spent,child=None):
            return dict(branch_id=bid,parent_branch_id=parent,node_id=node,visited_nodes=path,work_grant=grant,branch_slots=slots,work_spent=spent,work_unused=0,children=[] if child is None else [child])
        child=lambda b,p,n,g,s:dict(branch_id=b,parent_branch_id=p,node_id=n,work_grant=g,branch_slots=s,decision_id=b)
        rows=[report('r',None,0,[0],30,2,10,child('b','r',1,20,1)),report('b','r',1,[0,1],20,1,10,child('c','b',0,10,0)),report('c','b',0,[0,1,0],10,0,10)]
        self.assertTrue(native().validate_manifests(rows,'r',0,30,2,3,True))
        with self.assertRaises(ValueError):native().validate_manifests(rows,'r',0,30,2,3,False)
        bad=copy.deepcopy(rows);bad[-1]['work_spent']=11
        with self.assertRaises(ValueError):native().validate_manifests(bad,'r',0,30,2,3,True)
        bad=copy.deepcopy(rows);bad[1]['children'][0]['branch_id']='r'
        with self.assertRaises(ValueError):native().validate_manifests(bad,'r',0,30,2,3,True)

# Do not export imported TestCase classes: unittest discovery should execute
# only this module's tests rather than silently rerun their complete suites.
del NativeSearchTests, NativeRuntimeTests
