import copy
from concurrent.futures import Future
from pathlib import Path
import socketserver
import threading
import time
import unittest
import yaml
from dhdgs.head_direct import DirectHub,DirectFlow
from dhdgs.peer import HeadSession,exchange
from dhdgs.transport import receive,send
from dhdgs.config import validate

def config():
    c=yaml.safe_load((Path(__file__).parents[1]/'configs/dhdgs_pilot.yaml').read_text())
    c['peer'].update(head_port=0,head_bind_host='127.0.0.1',head_advertise_host='127.0.0.1')
    c['peer']['head_direct'].update(enabled=True,result_processes=2,distribution_threads=1,save_results=False,ack_timeout_ms=100)
    c['peer']['communication']['mode']='optimized';c['search']['execution']='peer';c['network']['transport']='tcp'
    c['search']['max_expansions']=10;c['peer']['max_branches']=3
    return c

class DirectTests(unittest.TestCase):
    def setUp(self):
        self.c=config();self.h=DirectHub(self.c,'test');self.addCleanup(self.h.close)
    def server(self,handle):
        class Handler(socketserver.BaseRequestHandler):
            def handle(inner):handle(inner.request)
        class Server(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=True
        server=Server(('127.0.0.1',0),Handler)
        t=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01});t.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),t.join()))
        return server.server_address
    def session(self,name):return HeadSession(self.c,name,'secret','root',0,self.h)
    def report(self):
        return dict(branch_id='root',parent_branch_id=None,node_id=0,work_grant=10,branch_slots=2,
            visited_nodes=[0],work_spent=10,work_unused=0,children=[])
    def callback(self,s):
        return exchange(s.endpoint,dict(query_request_id=s.registry.query_id,token='secret',node_id=0,
            involved_nodes=[0],reports=[self.report()]),8388608,2)
    def test_returns_before_ack_and_reports_cannot_finish_without_ack(self):
        gate=threading.Event();received=threading.Event()
        def handle(sock):
            receive(sock,8388608);received.set();gate.wait(2);send(sock,{'ok':True},8388608)
        endpoint=self.server(handle);s=self.session('early')
        try:
            # Gate release is explicit; returning here proves submit does not wait for ACK.
            f=self.h.submit_query(s,{},endpoint,time.monotonic()+2)
            self.assertTrue(received.wait(1));self.assertFalse(gate.is_set());self.assertFalse(f.done())
            self.callback(s);self.assertFalse(f.done());gate.set();self.assertTrue(f.result(2)['reply']['ok'])
            s.close(True)
        finally:gate.set()
    def test_lost_ack_retransmits_same_identity_once(self):
        messages=[]
        def handle(sock):
            message,_=receive(sock,8388608);messages.append(message)
            if len(messages)>1:send(sock,{'ok':True},8388608)
        endpoint=self.server(handle);s=self.session('retry');state={'query_request_id':'retry','branch_id':'root'}
        f=self.h.submit_query(s,state,endpoint,time.monotonic()+2);self.callback(s)
        entry=f.result(2);self.assertEqual(messages,[messages[0],messages[0]])
        self.assertEqual(entry['counts']['rpc_count'],2);s.close(True)
    def test_rejected_ack_fails_without_retransmit(self):
        messages=[]
        def handle(sock):messages.append(receive(sock,8388608)[0]);send(sock,{'ok':False,'error':'bad'},8388608)
        endpoint=self.server(handle);s=self.session('reject')
        with self.assertRaisesRegex(ValueError,'rejected'):
            self.h.submit_query(s,{},endpoint,time.monotonic()+2).result(2)
        self.assertEqual(len(messages),1);self.h.unregister(s,False)
    def test_partial_callback_does_not_block_other_slot(self):
        a=self.session('partial');b=self.session('other')
        import socket
        sock=socket.create_connection(a.endpoint);self.addCleanup(sock.close);sock.sendall(b'\0')
        self.assertTrue(self.callback(b)[0]['ok'])
        self.h.unregister(a,False);self.h.unregister(b,False)
    def test_config_rejects_both_paths(self):
        validate(self.c);self.c['peer']['head_pipeline']['enabled']=True
        with self.assertRaises(ValueError):validate(self.c)

    def test_close_does_not_wait_for_persistent_callback_timeout(self):
        import socket
        s=self.session('persistent');sock=socket.create_connection(s.endpoint);self.addCleanup(sock.close)
        send(sock,dict(query_request_id='persistent',token='secret',node_id=0,involved_nodes=[0],reports=[]),8388608)
        self.assertTrue(receive(sock,8388608)[0]['ok'])
        t=threading.Thread(target=self.h.close,daemon=True);t.start();t.join(1)
        self.assertFalse(t.is_alive(),'Close waited for peer socket timeout')

class FlowTests(unittest.TestCase):
    def test_one_distributor_can_reuse_before_remote_and_result_ready(self):
        flow=DirectFlow(config());self.addCleanup(flow.close)
        remote=[Future() for _ in range(32)];seen=[];ready=threading.Event()
        def task(i):
            seen.append(threading.get_ident())
            if len(seen)==32:ready.set()
            yield remote[i];return i
        out=[flow.submit(task(i),{}) for i in range(32)]
        self.assertTrue(ready.wait(3));self.assertEqual(len(set(seen)),1)
        self.assertFalse(any(f.done() for f in out))
        for f in remote:f.set_result(None)
        self.assertEqual([f.result(3) for f in out],list(range(32)))
    def test_cancel_frees_capacity(self):
        c=config();c['peer']['max_active_queries']=1;flow=DirectFlow(c);self.addCleanup(flow.close)
        remote=Future();entered=threading.Event();cleaned=threading.Event()
        def task():
            try:entered.set();yield remote
            finally:cleaned.set()
        f=flow.submit(task(),{});self.assertTrue(entered.wait(3));f.cancel()
        self.assertTrue(cleaned.wait(3));self.assertFalse(flow.records)
