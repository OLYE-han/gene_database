"""Real spawned receivers: affinity, terminal ownership, failure and cleanup."""
import copy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
import threading
import unittest
import yaml

from dhdgs.config import validate
from dhdgs.head_receivers import ProcessHeadHub
from dhdgs.peer import HeadSession, execution_signature, exchange


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.c=yaml.safe_load((Path(__file__).parents[1]/'configs/dhdgs.yaml').read_text())
        self.c['peer'].update(head_receiver_processes=2,head_port=0,head_bind_host='127.0.0.1',
            head_advertise_host='127.0.0.1',control_timeout_seconds=3.)
        self.c['peer']['communication']['mode']='optimized'
        self.c['peer']['head_receiver_ipc']=getattr(self,'ipc_mode','shared')
        self.c['network']['timeout_seconds']=5.
        self.c['search']['max_expansions']=10
        self.c['peer']['max_branches']=3
        self.hub=ProcessHeadHub(self.c,'test')
        self.addCleanup(self.hub.close)

    def session(self,qid):return HeadSession(self.c,qid,'secret','root',0,self.hub)
    def report(self,bid='root',child=False):
        r=dict(branch_id=bid,parent_branch_id=None,node_id=0,work_grant=10,
               branch_slots=2,visited_nodes=[0],work_spent=10,work_unused=0,children=[])
        if child:
            r.update(work_spent=4,children=[dict(branch_id='child',parent_branch_id='root',
                       node_id=1,work_grant=6,branch_slots=1)])
        if bid=='child':r.update(parent_branch_id='root',node_id=1,work_grant=6,branch_slots=1,
                                 visited_nodes=[0,1],work_spent=6)
        return r
    def send(self,s,reports=None,**kwargs):
        return exchange(s.endpoint,dict(query_request_id=s.registry.query_id,token='secret',
            node_id=reports[0]['node_id'] if reports else 0,involved_nodes=[0,1],reports=reports or [],**kwargs),
            self.c['network']['max_message_bytes'],3.)[0]

    def test_out_of_order_children_dedup_terminal_and_affinity(self):
        a=self.session('a');b=self.session('b');self.assertNotEqual(a.endpoint,b.endpoint)
        self.send(a,[self.report('child')]);self.assertFalse(a.registry.complete())
        self.send(a,[self.report(child=True)])
        reports=a.wait(time.monotonic()+3);self.assertEqual(len(reports),2)
        self.assertEqual(a.control['rpc_count'],2)
        self.assertGreaterEqual(a.receiver_metrics['terminal_parent_received_ns'],a.receiver_metrics['terminal_publish_ns'])
        a.close(True)
        self.assertTrue(self.send(a,[self.report(child=True)])['cancelled'])
        bad=self.report(child=True);bad['work_spent']=5
        with self.assertRaisesRegex(RuntimeError,'Conflicting'):self.send(a,[bad])
        self.send(b,[self.report()]);b.wait(time.monotonic()+3);b.close(True)

    def test_concurrent_queries_no_cross_delivery(self):
        def query(i):
            s=self.session(str(i));self.send(s,[self.report()])
            result=s.wait(time.monotonic()+3);s.close(True)
            return result[0]['work_spent']
        with ThreadPoolExecutor(max_workers=16) as pool:self.assertEqual(list(pool.map(query,range(64))),[10]*64)
        self.assertFalse(self.hub.sessions)

    def test_receiver_death_wakes_query_and_registration(self):
        s=self.session('death');w=self.hub.workers[0]
        w['process'].terminate();w['process'].join(3)
        with self.assertRaisesRegex(RuntimeError,'unavailable'):s.wait(time.monotonic()+3)
        self.assertEqual(s.registry.nodes,set(range(len(self.c['cluster']['nodes']))))
        self.hub.unregister(s,False)

    def test_error_invalid_token_and_cancel_tombstone(self):
        s=self.session('error')
        with self.assertRaisesRegex(RuntimeError,'token'):
            exchange(s.endpoint,dict(query_request_id='error',token='bad',node_id=0,
                involved_nodes=[0],reports=[]),self.c['network']['max_message_bytes'],3.)
        self.send(s,error='branch failed')
        with self.assertRaisesRegex(RuntimeError,'branch failed'):s.wait(time.monotonic()+3)
        self.hub.unregister(s,False)
        t=self.session('cancel');self.hub.unregister(t,False)
        deadline=time.monotonic()+3
        while not self.send(t)['cancelled']:
            self.assertLess(time.monotonic(),deadline);time.sleep(.01)

    def test_shutdown_joins_all_processes(self):
        processes=[w['process'] for w in self.hub.workers]
        self.hub.close()
        self.assertTrue(all(not p.is_alive() for p in processes))

    def test_config_and_execution_signature(self):
        old=copy.deepcopy(self.c);old['peer'].pop('head_receiver_processes')
        self.assertEqual(execution_signature(old),execution_signature(self.c))
        self.c['peer']['head_receiver_processes']=-1
        with self.assertRaises(ValueError):validate(self.c)
        self.c['peer']['head_receiver_processes']=33
        with self.assertRaises(ValueError):validate(self.c)

    def test_session_budget_changes_without_restarting_receiver(self):
        changed=copy.deepcopy(self.c)
        changed['search']['max_expansions']=24;changed['peer']['max_branches']=2
        s=HeadSession(changed,'hot-budget','secret','root',0,self.hub)
        report=self.report();report.update(work_grant=24,work_spent=24,branch_slots=1)
        self.send(s,[report]);self.assertEqual(s.wait(time.monotonic()+3)[0]['work_spent'],24)
        s.close(True)

    def test_bad_credit_fails_explicitly(self):
        s=self.session('credit');report=self.report();report['work_spent']=11
        self.send(s,[report])
        with self.assertRaisesRegex(RuntimeError,'conservation'):s.wait(time.monotonic()+3)
        self.hub.unregister(s,False)

    def test_oversized_terminal_bundle_fails_without_hanging(self):
        c=copy.deepcopy(self.c);c['network']['max_message_bytes']=2500
        hub=ProcessHeadHub(c,'small');self.addCleanup(hub.close)
        s=HeadSession(c,'large','secret','root',0,hub)
        child=self.report('child');child['padding']='x'*1400
        root=self.report(child=True);root['padding']='y'*1400
        self.send(s,[child]);self.send(s,[root])
        with self.assertRaisesRegex(RuntimeError,'IPC message limit'):s.wait(time.monotonic()+3)
        hub.unregister(s,False)


class SplitReceiverTests(ReceiverTests):
    ipc_mode='split'

    def test_blocked_terminal_pipe_does_not_block_registration(self):
        gate=threading.Event()
        class PausedTerminalHub(ProcessHeadHub):
            def _read(self,w,conn=None):
                if conn is not None:gate.wait(5)
                return super()._read(w,conn)
        c=copy.deepcopy(self.c);c['peer']['head_receiver_processes']=1
        hub=PausedTerminalHub(c,'blocked-pipe')
        try:
            first=HeadSession(c,'big','secret','root',0,hub)
            report=self.report();report['padding']='x'*1048576
            self.send(first,[report])
            # Final payload cannot fit an unread pipe; its output lock must
            # never prevent a small registration acknowledgement from passing.
            second=HeadSession(c,'next','secret','root',0,hub)
            hub.unregister(second,False)
            gate.set();first.wait(time.monotonic()+3);first.close(True)
        finally:gate.set();hub.close()


if __name__=='__main__':unittest.main()
