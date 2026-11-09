"""Replica roots use the same physical branch, inbox and continuous READ policy."""
import copy
import time
import unittest

from dhdgs.config import validate
from test_replicas import ReplicaTests
from test_region_tasks import RegionRuntimeTests, mode
from test_remote_pull import RemotePullTests
from test_hop_return import PullRuntimeTests
from test_one_sided import active_ib


class ReplicaCandidateTests(unittest.TestCase):
    setUpClass=classmethod(ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(ReplicaTests.tearDownClass.__func__)
    setUp=ReplicaTests.setUp
    rpc=ReplicaTests.rpc
    info=ReplicaTests.info
    install_entry=PullRuntimeTests.install_entry

    def configure_replica_test(self,c):
        mode(c)
        c['load_balance']['peer_routes_enabled']=False
        c['peer']['candidate_sync']=dict(enabled=True,interval_expansions=10,max_records=64,
            nonblocking_start=True,early_result=True,early_budget_report=True,
            coalesce_pending=True,max_buffered_records=4096,completion_threads=0)

    def check_result(self,q,budget=200):
        RegionRuntimeTests.check_result(self,q,budget)
        reports=q['branch_reports']
        self.assertEqual(len(reports),len({r['node_id']for r in reports}))
        for r in reports:
            self.assertEqual(r['work_grant'],r['work_spent']+r['work_unused']+sum(c['work_grant']for c in r['children']))
            if 'replica_version' in r:
                self.assertEqual(r['work_spent'],r['stats'].get('replica_overlay_expansions',0)+
                    r['stats'].get('replica_host_base_expansions',0)+r['stats'].get('pull_expanded',0))

    def test_replica_root_has_one_physical_branch_and_local_union(self):
        _,target=self.install_entry()
        rows=[self.engine.search_async(self.x[0],0).result(20)for _ in range(8)]
        for q in rows:self.check_result(q)
        roots=[next(b for b in q['branch_reports']if b['parent_branch_id']is None)for q in rows]
        self.assertTrue(all(b['node_id']==target and b.get('replica_version')=='pull_test' for b in roots))
        self.assertGreater(sum(b['stats'].get('replica_host_base_expansions',0)for b in roots),0)
        self.assertGreater(sum(q['stats'].get('candidate_sync_candidates_sent',0)for q in rows),0)

    def test_concurrent_replica_queries_retire_and_drain(self):
        _,target=self.install_entry()
        tasks=[self.engine.search_async(self.x[0],0)for _ in range(48)]
        for task in tasks:self.check_result(task.result(30))
        self.engine.runtime.replica_routes([])
        self.rpc(target,'retire',id='pull_test')
        deadline=time.monotonic()+5
        while not self.rpc(target,'reap',id='pull_test')['reclaimed']:
            self.assertLess(time.monotonic(),deadline,'Pinned replica not released after completion')
            time.sleep(.01)
        q=self.engine.search_async(self.x[0],0).result(15);self.check_result(q)
        self.assertFalse(any('replica_version' in b for b in q['branch_reports']))
        self.assertEqual(self.engine.runtime.metrics()['active'],0)

    def test_peer_replica_routing_remains_rejected(self):
        c=copy.deepcopy({k:v for k,v in self.c.items()if not k.startswith('_')})
        c['load_balance']['peer_routes_enabled']=True
        with self.assertRaisesRegex(ValueError,'root-only'):validate(c)


@unittest.skipUnless(active_ib(),'Requires an active InfiniBand verbs device')
class ReplicaContinuousPullTests(ReplicaCandidateTests):
    def configure_replica_test(self,c):
        RemotePullTests.configure_replica_test(self,c)
        c['load_balance']['enabled']=True
        ReplicaCandidateTests.configure_replica_test(self,c)
        c['peer']['replica_pull']['async']['dedicated_dispatcher']=True
        c['peer']['replica_pull']['one_sided']['io_threads']=2

    def test_replica_root_has_one_physical_branch_and_local_union(self):
        ReplicaCandidateTests.test_replica_root_has_one_physical_branch_and_local_union(self)

    def test_continuous_replica_read_does_not_reserve_whole_source(self):
        s=RegionRuntimeTests.state(self,self.c,qid='continuous-owner')
        fields={k:s[k]for k in ('query_request_id','token','epoch','signature')}
        request=dict(fields,owner='replica:continuous',directory=dict(opened=[],reserved=[]),
                     ids=[s['seeds'][0][0]],one_sided=True)
        reply=RegionRuntimeTests.rpc(self,self.c,0,'peer_replica_read',**request)
        self.assertTrue(reply['accepted']);self.assertFalse(reply['directory']['reserved'])
        claim=RegionRuntimeTests.rpc(self,self.c,0,'peer_candidate_claim',owner='another-branch',**fields)
        self.assertTrue(claim['claimed'])
        # Announced first start excludes a subsequent data pull, before compute.
        reply=RegionRuntimeTests.rpc(self,self.c,0,'peer_replica_read',**request)
        self.assertFalse(reply['accepted'])

    def test_continuous_read_ignores_legacy_remaining_gate_then_starts_source(self):
        source,target=self.install_entry()
        rows=[self.engine.search_async(self.x[0],0).result(20)for _ in range(12)]
        for q in rows:self.check_result(q)
        roots=[next(b for b in q['branch_reports']if b['parent_branch_id']is None)for q in rows]
        # Legacy max_remaining=1 in this fixture: successful larger grants
        # prove the replica actually uses the general continuous READ path.
        self.assertTrue(any(e['success'] and e['remaining_before']>1 for b in roots for e in b['pull_events']))
        self.assertGreater(sum(b['stats'].get('one_sided_read_ops',0)for b in roots),0)
        self.assertGreater(sum(b['stats'].get('remote_pull_streak_pushes',0)for b in roots),0)
        self.assertTrue(any(any(b['node_id']==source and b['work_spent'] for b in q['branch_reports'])for q in rows))
        for q in rows:
            for b in q['branch_reports']:self.assertLessEqual(b['stats'].get('remote_pull_max_streak',0),2)
        metrics=self.servers[target].control.peer.runtime.metrics()['pull_scheduler']
        self.assertEqual(metrics['contexts']+metrics['pending']+sum(metrics['inflight']),0)
        self.assertEqual(metrics['one_sided_client']['errors'],0)


@unittest.skipUnless(active_ib(),'Requires an active InfiniBand verbs device')
class ReplicaDirectPushTests(ReplicaCandidateTests):
    def configure_replica_test(self,c):
        ReplicaContinuousPullTests.configure_replica_test(self,c)
        c['peer']['remote_pull']['pull_enabled']=False
        # Deliberately allow the legacy tail gate. Disabling continuous reads
        # must not silently re-enable that path for a replica root.
        c['peer']['replica_pull']['max_remaining']=200

    def test_read_disabled_keeps_unique_branches_and_no_legacy_tail(self):
        self.install_entry()
        rows=[self.engine.search_async(self.x[0],0).result(20)for _ in range(8)]
        for q in rows:
            self.check_result(q)
            self.assertEqual(q['stats'].get('pull_expanded',0),0)
            self.assertEqual(q['stats'].get('one_sided_read_ops',0),0)
        self.assertGreater(sum(q['stats'].get('remote_first_pushes',0)for q in rows),0)
        for server in self.servers:
            p=server.control.peer.runtime.metrics()['pull_scheduler']
            self.assertEqual(p['one_sided_client']['read_operations'],0)


if __name__=='__main__':unittest.main()
