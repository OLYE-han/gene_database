import copy
from pathlib import Path
import socketserver
import threading
import time
import unittest
import yaml
from dhdgs.head_receivers import ProcessHeadHub
from dhdgs.peer import HeadSession,exchange
from dhdgs.transport import send,receive

class AsyncReceiverTests(unittest.TestCase):
    def setUp(self):
        c=yaml.safe_load((Path(__file__).parents[1]/'configs/dhdgs.yaml').read_text())
        c['peer'].update(head_receiver_processes=1,head_receiver_ipc='split',head_query_workers=2,
            head_entry_workers=2,head_port=0,head_bind_host='127.0.0.1',head_advertise_host='127.0.0.1')
        c['peer']['communication']['mode']='optimized'
        c['search']['max_expansions']=10;c['peer']['max_branches']=3
        self.c=c;self.hub=ProcessHeadHub(c,'test-async');self.addCleanup(self.hub.close)
        self.gate=threading.Event();self.accepted=threading.Event();self.early_report=False
        owner=self
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                try:
                    message,_=receive(self.request,c['network']['max_message_bytes'])
                    state=message['state'];owner.accepted.set()
                    if owner.early_report:
                        report=dict(branch_id='root',parent_branch_id=None,node_id=0,work_grant=10,
                            branch_slots=2,visited_nodes=[0],work_spent=10,work_unused=0,children=[])
                        exchange(state['head_endpoint'],dict(query_request_id=state['query_request_id'],token='secret',
                            node_id=0,involved_nodes=[0],reports=[report]),c['network']['max_message_bytes'],3.)
                        owner.gate.wait(3)
                    send(self.request,{'ok':True,'accepted':True},c['network']['max_message_bytes'])
                except OSError:pass
        class Server(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=True
        self.server=Server(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.01},daemon=True)
        self.thread.start();self.addCleanup(self.stop)
    def stop(self):self.gate.set();self.server.shutdown();self.server.server_close();self.thread.join(2)
    def start(self,qid,timeout):
        s=HeadSession(self.c,qid,'secret','root',0,self.hub,deferred=True)
        f=self.hub.submit_query(s,{'query_request_id':qid},list(self.server.server_address),time.monotonic()+timeout)
        return s,f
    def test_terminal_before_entry_ack_cannot_complete_query(self):
        self.early_report=True;s,f=self.start('early',4)
        self.assertTrue(s.done.wait(2));self.assertFalse(f.done())
        self.gate.set();response=f.result(2)
        self.assertEqual(response['counts']['rpc_count'],1)
        self.assertTrue(s.registry.complete());s.close(True)
    def test_no_report_expires_without_occupying_stage_worker(self):
        s,f=self.start('expiry',.2)
        with self.assertRaisesRegex(TimeoutError,'deadline'):f.result(2)
        self.hub.unregister(s,False)
    def test_receiver_exit_fails_outstanding_continuation(self):
        s,f=self.start('death',4);self.assertTrue(self.accepted.wait(2))
        w=self.hub.workers[0];w['process'].terminate();w['process'].join(2)
        with self.assertRaisesRegex(RuntimeError,'unavailable'):f.result(2)
        self.hub.unregister(s,False)
