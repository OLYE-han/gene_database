"""The replica/source protocol must not leak to ordinary graph branches."""
import copy
import unittest
from collections import defaultdict
from dhdgs.index import native
from dhdgs.config import validate
from test_replica_candidate_sync import ReplicaCandidateTests, ReplicaContinuousPullTests
from test_replicas import ReplicaTests
from test_one_sided import active_ib

@unittest.skipUnless(active_ib(), 'Requires active InfiniBand')
class ReplicaPairTests(unittest.TestCase):
    setUpClass=classmethod(ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(ReplicaTests.tearDownClass.__func__)
    setUp=ReplicaTests.setUp
    rpc=ReplicaTests.rpc
    info=ReplicaTests.info
    install_entry=ReplicaCandidateTests.install_entry

    def configure_replica_test(self,c):
        ReplicaContinuousPullTests.configure_replica_test(self,c)
        c['peer']['candidate_sync']['scope']='replica_source'
        c['search']['max_expansions']=128

    def check(self,q,source=None,host=None):
        self.assertEqual(q['status'],'ok',q.get('error'))
        rows=q['branch_reports'];root=next(r for r in rows if r['parent_branch_id'] is None)
        self.assertTrue(native().validate_manifests(rows,root['branch_id'],root['node_id'],128,root['branch_slots'],3,True,True))
        ids=defaultdict(list);pair=[]
        for r in rows:
            ids[r['node_id']].extend(r['expanded_ids'])
            self.assertEqual(r['work_grant'],r['work_spent']+r['work_unused']+sum(c['work_grant']for c in r['children']))
            if 'replica_pair_version' in r:
                pair.append(r['node_id']);self.assertIn(r['node_id'],(source,host))
                self.assertEqual(r['replica_pair_source'],source)
                for e in r.get('pull_events',[]):self.assertEqual((r['node_id'],e['source']),(host,source))
            else:
                for k in ('one_sided_read_ops','pull_batches','candidate_sync_candidates_sent','candidate_sync_resumes','candidate_early_results'):
                    self.assertEqual(r['stats'].get(k,0),0,(r['node_id'],k))
        self.assertEqual(len(pair),len(set(pair)))
        for expanded in ids.values():self.assertEqual(len(expanded),len(set(expanded)))
        self.assertLessEqual(q['stats']['expanded'],128)

    def test_no_replica_has_no_read_or_sync(self):
        for i in range(12):self.check(self.engine.search_async(self.x[i],i).result(20))
        for server in self.servers:
            m=server.control.peer.runtime.metrics()
            self.assertEqual(m['candidate_sync']['sent'],0)
            self.assertEqual(m['candidate_sync']['claims'],0)
            self.assertEqual(m['pull_scheduler']['one_sided_client']['read_operations'],0)

    def test_pair_reads_and_sync_with_ordinary_children(self):
        source,host=self.install_entry()
        rows=[self.engine.search_async(self.x[0],0).result(20)for _ in range(12)]
        for q in rows:self.check(q,source,host)
        self.assertGreater(sum(q['stats'].get('one_sided_read_ops',0)for q in rows),0)
        self.assertGreater(sum(q['stats'].get('candidate_sync_candidates_sent',0)for q in rows),0)
        self.assertTrue(any('replica_pair_version' not in r for q in rows for r in q['branch_reports']))

    def test_concurrent_pair_and_ordinary_queries_drain(self):
        source,host=self.install_entry()
        tasks=[self.engine.search_async(self.x[i%8],i%8)for i in range(48)]
        for f in tasks:self.check(f.result(30),source,host)
        self.assertEqual(self.engine.runtime.metrics()['active'],0)
        for server in self.servers:
            m=server.control.peer.runtime.metrics()['pull_scheduler']
            self.assertEqual(m['contexts']+m['pending']+sum(m['inflight']),0)

    def test_invalid_scope_rejected(self):
        c=copy.deepcopy({k:v for k,v in self.c.items()if not k.startswith('_')})
        c['peer']['candidate_sync']['scope']='anything'
        with self.assertRaisesRegex(ValueError,'scope'):validate(c)
        c['peer']['candidate_sync'].update(scope='replica_source',nonblocking_start=False)
        with self.assertRaisesRegex(ValueError,'nonblocking'):validate(c)

if __name__=='__main__':unittest.main()
