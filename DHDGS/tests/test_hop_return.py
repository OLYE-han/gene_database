import copy
import time
import unittest
from dhdgs.index import native
from test_replica_community import CommunityTests
import test_replica_community as _community
from test_replicas import ReplicaTests
from test_region_tasks import RegionRuntimeTests, mode


class HopCutTests(unittest.TestCase):
    def setUp(self):
        self.f = _community.CommunityTests(); self.f.setUp(); self.addCleanup(self.f.doCleanups)

    def test_attached_ball_keeps_all_original_neighbors_and_certificate(self):
        f, ids, meta = self.f.graph({1:[2],2:[3,4],3:[5],4:[],5:[],90:[91],91:[]})
        cfg=self.f.b
        out=native().replica_hop_expand(f.fileno(),ids,[1,2],[5],[1.],cfg,meta)
        self.assertTrue(set([1,2,3,4,5])<=set(out['ids']))
        check=native().replica_hop_expand(f.fileno(),ids,[1,2],[],[],cfg,meta,out['anchors'],True)
        self.assertEqual(out['ids'],check['ids'])
        self.assertNotIn(90,out['ids'])
        with self.assertRaisesRegex(ValueError,'not attached'):
            native().replica_hop_expand(f.fileno(),ids,[1,2],[],[],cfg,meta,[90],True)

    def test_budget_rejects_whole_ball_and_preserves_core(self):
        f,ids,m=self.f.graph({1:[2],2:[3],3:[4,5,6],4:[],5:[],6:[]})
        cfg=copy.deepcopy(self.f.b)
        core=native().replica_hop_expand(f.fileno(),ids,[1,2],[],[],cfg,m,[],True)
        cfg['max_region_bytes']=core['stats']['estimated_bytes']+1
        out=native().replica_hop_expand(f.fileno(),ids,[1,2],[3],[1.],cfg,m)
        self.assertEqual(out['ids'],[1,2]);self.assertLessEqual(out['stats']['estimated_bytes'],cfg['max_region_bytes'])

    def test_shared_dependencies_paid_once(self):
        f,ids,m=self.f.graph({1:[2,3],2:[4],3:[4],4:[]})
        cfg=self.f.b
        out=native().replica_hop_expand(f.fileno(),ids,[1],[2,3],[1.,1.],cfg,m)
        self.assertEqual(out['ids'],[1,2,3,4]);self.assertEqual(out['stats']['code_rows'],4)
        self.assertEqual(out['stats']['estimated_bytes'],m['header_bytes']+4*(m['record_bytes']+8+m['community_code_bytes'])+m['community_fixed_bytes'])

    def test_native_entry_rejects_an_oversized_core(self):
        f, ids, meta = self.f.graph({1:[2],2:[]})
        cfg = copy.deepcopy(self.f.b)
        cfg['max_region_rows'] = 1
        for verify in (False, True):
            with self.assertRaisesRegex(ValueError, 'core row budget'):
                native().replica_hop_expand(f.fileno(),ids,[1,2],[],[],cfg,meta,[],verify)


class PullRuntimeTests(unittest.TestCase):
    setUpClass=classmethod(ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(ReplicaTests.tearDownClass.__func__)
    setUp=ReplicaTests.setUp
    rpc=ReplicaTests.rpc
    info=ReplicaTests.info
    check_result=RegionRuntimeTests.check_result

    def configure_replica_test(self,c):
        mode(c)
        c['peer']['replica_pull']=dict(enabled=True,require_rdma=False,max_remaining=200,max_rounds=64,max_bytes=64*524288,batch_bytes=524288,batch_records=4)

    def install_entry(self):
        info=self.info();a=info['source'];b=(a+1)%3;entry=info['entry_id']
        self.rpc(a,'source_start',id='pull_test',plan=dict(ids=[entry],entries=[entry],target=b))
        end=time.monotonic()+15
        while time.monotonic()<end:
            s=self.rpc(a,'status',id='pull_test')
            if s['phase']=='FAILED':self.fail(s['error'])
            if s['phase']=='SENT' and self.rpc(b,'status',id='pull_test')['phase']=='READY':break
            time.sleep(.01)
        else:self.fail('Pull fixture not ready')
        self.engine.runtime.replica_routes([dict(source=a,target=b,version='pull_test',entries=[entry],fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=10000)])
        return a,b

    def test_pull_computes_on_replica_preserves_credit_and_dependencies(self):
        a,b=self.install_entry()
        q=self.engine.search_async(self.x[0],0).result(15)
        self.check_result(q,200)
        self.assertGreater(q['stats']['pull_expanded'],0,q['stats'])
        reps=[r for r in q['branch_reports']if r.get('replica_version')=='pull_test']
        self.assertTrue(all(r['node_id']==b for r in reps))
        self.assertTrue(all(r['stats']['expanded']==r['stats'].get('replica_record_reads',0)+r['stats'].get('pull_expanded',0)for r in reps))
        self.assertGreater(q['stats']['pull_code_bytes'],0)
        self.assertGreater(q['stats']['pull_source_cpu_ms'],0)
        self.assertGreater(q['stats']['pull_wire_bytes'],q['stats']['pull_record_bytes'])

class PullBoundTests(PullRuntimeTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['max_rounds']=1

    def test_pull_exhaustion_falls_back_without_resetting_budget(self):
        a,b=self.install_entry();q=self.engine.search_async(self.x[0],0).result(15)
        self.check_result(q,200)
        self.assertGreater(q['stats']['pull_expanded'],0)
        self.assertGreater(q['stats']['pull_limit_rejections'],0)
        self.assertTrue(any(r['node_id']==a and r['work_spent']>0 for r in q['branch_reports']))


class PullBatchTests(PullRuntimeTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['batch_records']=8
        c['search']['local_beam']=1

    def test_remote_record_buffer_can_exceed_local_beam(self):
        self.install_entry();q=self.engine.search_async(self.x[0],0).result(15)
        self.check_result(q,200)
        events=[e for b in q['branch_reports']for e in b.get('pull_events',[])if e['success']]
        self.assertTrue(any(e['records']>1 for e in events))


class PullFailureTests(unittest.TestCase):
    setUpClass=PullRuntimeTests.__dict__['setUpClass']
    tearDownClass=PullRuntimeTests.__dict__['tearDownClass']
    setUp=PullRuntimeTests.setUp
    rpc=PullRuntimeTests.rpc
    info=PullRuntimeTests.info
    check_result=PullRuntimeTests.check_result
    install_entry=PullRuntimeTests.install_entry

    def configure_replica_test(self,c):
        PullRuntimeTests.configure_replica_test(self,c)
        c['peer']['replica_pull']['batch_bytes']=1

    def test_rejected_read_does_not_spend_credit_or_drop_candidates(self):
        a,b=self.install_entry();q=self.engine.search_async(self.x[0],0).result(15)
        self.check_result(q,200)
        self.assertEqual(q['stats']['pull_expanded'],0)
        self.assertGreater(q['stats']['pull_fallbacks'],0)
        self.assertTrue(any(r['node_id']==a and r['work_spent']>0 for r in q['branch_reports']))


class PullProtocolTests(unittest.TestCase):
    setUpClass=classmethod(RegionRuntimeTests.setUpClass.__func__)
    tearDownClass=classmethod(RegionRuntimeTests.tearDownClass.__func__)
    config=RegionRuntimeTests.config
    start=RegionRuntimeTests.start
    rpc=RegionRuntimeTests.rpc
    state=RegionRuntimeTests.state

    def test_read_is_guarded_by_epoch_token_batch_and_source_actor(self):
        c=mode(self.config(True,True));PullRuntimeTests.configure_replica_test(self,c);self.start(c)
        state=self.state(c);fields={k:state[k]for k in ['query_request_id','token','epoch','signature']}
        fields.update(owner='replica:alpha',directory=dict(opened=[],reserved=[]),ids=[state['seeds'][0][0]])
        reply=self.rpc(c,0,'peer_replica_read',**fields)
        self.assertTrue(reply['accepted']);self.assertEqual(reply['ids'],fields['ids'])
        denied=self.rpc(c,0,'peer_replica_read',**dict(fields,owner='replica:beta'))
        self.assertFalse(denied['accepted'])
        # The source actor may not yet exist when the same owner has already
        # announced a source handoff. Reject a pull during that ACK window.
        for directory in [dict(opened=[],pending=['base:0'],reserved=[]),
                          dict(opened=['base:0'],reserved=[])]:
            self.rpc(c,0,'peer_region_update',**dict(fields,directory=directory))
            self.assertFalse(self.rpc(c,0,'peer_replica_read',**fields)['accepted'])
        for bad in [dict(fields,epoch='wrong'),dict(fields,token='wrong'),dict(fields,ids=fields['ids']*5)]:
            with self.assertRaises(AssertionError):self.rpc(c,0,'peer_replica_read',**bad)
        authorized=self.state(c,bid='source',origin='replica:alpha')
        self.rpc(c,0,'peer_submit',state=authorized)
        self.assertFalse(self.rpc(c,0,'peer_replica_read',**fields)['accepted'])
        self.rpc(c,0,'peer_cancel',query_request_id=state['query_request_id'],token=state['token'])

del CommunityTests,ReplicaTests,RegionRuntimeTests
