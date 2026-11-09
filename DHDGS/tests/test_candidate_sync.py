"""Canonical branch integration; no new grants for candidate delivery."""
import copy
import unittest
import numpy as np
from dhdgs.search import Engine
from dhdgs.config import validate
from test_region_tasks import RegionRuntimeTests, mode


class CandidateSyncTests(unittest.TestCase):
    setUpClass=classmethod(RegionRuntimeTests.setUpClass.__func__)
    tearDownClass=classmethod(RegionRuntimeTests.tearDownClass.__func__)
    config=RegionRuntimeTests.config
    start=RegionRuntimeTests.start
    rpc=RegionRuntimeTests.rpc
    state=RegionRuntimeTests.state

    def configuration(self):
        c=mode(self.config(True,True))
        c['peer']['candidate_sync']=dict(enabled=True,interval_expansions=10,max_records=64,skip_exhausted_targets=True)
        c['peer']['region_visit_broadcast']=True
        c['peer']['query_timeout_seconds']=5.
        validate({k:v for k,v in c.items() if not k.startswith('_')})
        return c

    def test_unique_branch_fixed_budget_and_drain(self):
        c=self.configuration();self.start(c)
        e=Engine(c);self.addCleanup(e.close)
        tasks=[e.search_async(x,i)for i,x in enumerate(self.x[:16])]
        rows=[f.result(15)for f in tasks]
        for q in rows:
            self.assertEqual(q['status'],'ok',q.get('error'))
            reports=q['branch_reports']
            if len(reports)==1:
                self.assertFalse(any(x['kind']=='idle' for x in reports[0].get('candidate_pause_events',[])),
                                 'Sole-root termination must not use a distributed barrier')
            self.assertEqual(len(reports),len({r['node_id']for r in reports}))
            self.assertLessEqual(q['stats']['expanded'],200)
            for r in reports:
                self.assertEqual(r['work_grant'],r['work_spent']+r['work_unused']+sum(x['work_grant']for x in r['children']))
                self.assertEqual(len(r['expanded_ids']),len(set(r['expanded_ids'])))
        self.assertTrue(any(len(q['branch_reports'])>1 for q in rows))
        self.assertGreater(sum(q['stats'].get('candidate_sync_candidates_sent',0)for q in rows),0)
        self.assertGreater(sum(q['stats'].get('candidate_sync_rpc_count',0)for q in rows),0)
        self.assertEqual(e.runtime.metrics()['active'],0)

    def test_claim_is_idempotent_and_exclusive(self):
        from concurrent.futures import ThreadPoolExecutor
        c=self.configuration();self.start(c);s=self.state(c)
        fields={k:s[k]for k in ('query_request_id','token','epoch','signature')}
        with ThreadPoolExecutor(8)as pool:
            rows=list(pool.map(lambda i:self.rpc(c,0,'peer_candidate_claim',owner=str(i),**fields),range(8)))
        self.assertEqual(sum(r['claimed']for r in rows),1)
        winner=next(i for i,r in enumerate(rows)if r['claimed'])
        retry=self.rpc(c,0,'peer_candidate_claim',owner=str(winner),**fields)
        self.assertEqual(retry,rows[winner])
        self.assertEqual(len({r['branch_id']for r in rows}),1)

    def test_queued_mailbox_accepts_deltas_before_search_and_deduplicates(self):
        c=self.configuration();self.start(c);s=self.state(c)
        fields={k:s[k]for k in ('query_request_id','token','epoch','signature')}
        self.rpc(c,0,'peer_candidate_claim',owner='sender',**fields)
        packet=dict(sender_branch='sender',sequence=1,rows=s['seeds'],
                    admission_bound=None,bound_valid=False,expansions=10,**fields)
        first=self.rpc(c,0,'peer_candidate_delta',**packet)
        self.assertTrue(first['accepted'])
        self.assertEqual(self.rpc(c,0,'peer_candidate_delta',**packet),first)
        packet['rows']=[]
        with self.assertRaises(AssertionError):self.rpc(c,0,'peer_candidate_delta',**packet)

    def test_single_compute_worker_high_concurrency_finishes(self):
        c=self.configuration();c['peer']['worker_threads']=1;self.start(c)
        e=Engine(c);self.addCleanup(e.close)
        tasks=[e.search_async(self.x[i%96],i%96)for i in range(256)]
        for task in tasks:
            row=task.result(15);self.assertEqual(row['status'],'ok',row.get('error'))
            self.assertEqual(len(row['branch_reports']),len({r['node_id']for r in row['branch_reports']}))
        self.assertEqual(e.runtime.metrics()['active'],0)

    def test_single_participant_never_enters_quiescence_protocol(self):
        c=self.configuration();c['peer']['max_branches']=1;self.start(c)
        e=Engine(c);self.addCleanup(e.close)
        for i in range(4):
            q=e.search_async(self.x[i],i).result(10)
            self.assertEqual(q['status'],'ok',q.get('error'))
            self.assertEqual(len(q['branch_reports']),1)
            self.assertFalse(q['branch_reports'][0].get('candidate_pause_events'))

    def test_exhausted_branch_reports_and_rejects_late_work_without_new_budget(self):
        import socketserver,threading
        from dhdgs.transport import send,receive
        reported=threading.Event()
        class Server(socketserver.ThreadingTCPServer):
            daemon_threads=True
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(2)
                try:
                    while True:
                        message,_=receive(self.request,1<<20)
                        if message.get('reports'):reported.set()
                        send(self.request,dict(ok=True,cancelled=False,lease_ms=60000,involved_nodes=[0]),1<<20)
                except (OSError,EOFError):pass
        head=Server(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=head.serve_forever,daemon=True);thread.start()
        self.addCleanup(head.server_close);self.addCleanup(head.shutdown)
        c=self.configuration();self.start(c);s=self.state(c)
        fields={k:s[k]for k in ('query_request_id','token','epoch','signature')}
        claim=self.rpc(c,0,'peer_candidate_claim',owner='test-sender',**fields)
        s.update(branch_id=claim['branch_id'],work_grant=1,head_endpoint=list(head.server_address))
        self.rpc(c,0,'peer_submit',state=s)
        self.assertTrue(reported.wait(3),'Budget-zero branch must report without a head prepare')
        gid=int(np.load(self.root/'out/index/shard_0/ids.npy')[1]);row=copy.deepcopy(s['seeds'][0]);row[0]=gid
        packet=dict(sender_branch='test-sender',sequence=1,rows=[row],admission_bound=None,bound_valid=False,expansions=10,**fields)
        result=self.rpc(c,0,'peer_candidate_delta',**packet)
        self.assertEqual(result['budget_exhausted'],1)
        self.assertTrue(result['terminal_budget'])
        self.assertEqual(self.rpc(c,0,'peer_candidate_delta',**packet),result)
        packet['sequence']=2
        self.assertEqual(self.rpc(c,0,'peer_candidate_delta',**packet)['budget_exhausted'],0)


from test_remote_pull import RemotePullTests as _PullTests

class CandidatePullTests(_PullTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['candidate_sync']=dict(enabled=True,interval_expansions=10,max_records=64,skip_exhausted_targets=True)

    def check_result(self,q,budget):
        super().check_result(q,budget)
        self.assertEqual(len(q['branch_reports']),len({r['node_id']for r in q['branch_reports']}))

if __name__=='__main__':unittest.main()

