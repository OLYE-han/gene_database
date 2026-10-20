import copy
from concurrent.futures import Future
from pathlib import Path
import socket
import socketserver
import threading
import time
import unittest
import yaml
from dhdgs.transport import send,receive

from dhdgs.head_pipeline import AdaptiveIngress,PipelineFlow
from dhdgs.pipeline_hub import PipelineHub
from dhdgs.peer import HeadSession,exchange
from dhdgs.config import validate


def config():
    c=yaml.safe_load((Path(__file__).parents[1]/'configs/dhdgs.yaml').read_text())
    c['peer'].update(head_port=0,head_bind_host='127.0.0.1',head_advertise_host='127.0.0.1')
    c['peer']['head_pipeline']['enabled']=True;c['peer']['communication']['mode']='optimized'
    c['network']['transport']='tcp';c['search']['execution']='peer'
    return c


class FlowTests(unittest.TestCase):
    def test_route_worker_reused_while_all_remote_results_pending(self):
        c=config();c['peer']['head_pipeline'].update(route_workers=1,merge_workers=1)
        flow=PipelineFlow(c);remote=[Future() for _ in range(128)];routed=[]
        complete=threading.Event()
        def task(i):
            routed.append(threading.get_ident())
            if len(routed)==128:complete.set()
            yield remote[i]
            return i
        queries=[flow.submit(task(i)) for i in range(128)]
        try:
            self.assertTrue(complete.wait(5))
            self.assertEqual(len(set(routed)),1)
            self.assertEqual(flow.route.submit(lambda: 'free').result(1),'free')
            self.assertFalse(any(f.done() for f in queries))
            for f in remote:f.set_result(None)
            self.assertEqual([f.result(5) for f in queries],list(range(128)))
        finally:flow.close()

    def test_adaptive_receive_grows_and_shrinks_without_exceeding_four(self):
        p=config()['peer']['head_pipeline'];p.update(receive_backlog=1,receive_idle_ms=20)
        pool=AdaptiveIngress(p,32);gate=threading.Event()
        try:
            futures=[pool.submit(gate.wait,2) for _ in range(12)]
            self.assertEqual(pool.status()['workers'],4)
            gate.set()
            for f in futures:f.result(2)
            until=time.monotonic()+2
            while pool.status()['workers']>1 and time.monotonic()<until:time.sleep(.01)
            self.assertEqual(pool.status()['workers'],1)
        finally:gate.set();pool.close()

    def test_merge_not_blocked_by_occupied_route_worker(self):
        c=config();c['peer']['head_pipeline'].update(route_workers=1,merge_workers=1)
        flow=PipelineFlow(c);ready=threading.Event();block=threading.Event();entered=threading.Event();remote=Future()
        def first():ready.set();yield remote;return 7
        def second():entered.set();block.wait(2);yield Future()
        a=flow.submit(first());self.assertTrue(ready.wait(1));b=flow.submit(second())
        try:
            self.assertTrue(entered.wait(1));remote.set_result(None)
            self.assertEqual(a.result(1),7)
        finally:block.set();flow.close()
        self.assertTrue(b.cancelled())

    def test_capacity_and_cancel_release_states(self):
        c=config();c['peer']['max_active_queries']=1;flow=PipelineFlow(c)
        gate=Future();started=threading.Event();cleaned=threading.Event()
        def task():
            try:started.set();yield gate
            finally:cleaned.set()
        a=flow.submit(task());self.assertTrue(started.wait(1))
        with self.assertRaisesRegex(RuntimeError,'capacity'):flow.submit(task()).result(1)
        flow.close();self.assertTrue(a.cancelled());self.assertTrue(cleaned.is_set())

    def test_config_rejects_fifth_receive_thread_and_extra_process(self):
        c=config();validate(c);c['peer']['head_pipeline']['receive_max']=5
        with self.assertRaises(ValueError):validate(c)

    def test_completion_callback_can_reuse_capacity_immediately(self):
        c=config();c['peer']['max_active_queries']=1;flow=PipelineFlow(c)
        self.addCleanup(flow.close);gate=Future();next_query=Future()
        def first():yield gate;return 1
        def second():
            if False:yield
            return 2
        one=flow.submit(first())
        one.add_done_callback(lambda _:next_query.set_result(flow.submit(second())))
        gate.set_result(None)
        self.assertEqual(next_query.result(1).result(1),2)
        c=config();c['peer']['head_receiver_processes']=1
        with self.assertRaises(ValueError):validate(c)


class HubTests(unittest.TestCase):
    def setUp(self):
        self.c=config();self.c['search']['max_expansions']=10;self.c['peer']['max_branches']=3
        self.h=PipelineHub(self.c,'test');self.addCleanup(self.h.close)
    def session(self,qid):return HeadSession(self.c,qid,'secret','root',0,self.h)
    def report(self):
        return dict(branch_id='root',parent_branch_id=None,node_id=0,work_grant=10,branch_slots=2,
                    visited_nodes=[0],work_spent=10,work_unused=0,children=[])
    def send(self,s,report):
        return exchange(s.endpoint,dict(query_request_id=s.registry.query_id,token='secret',
            node_id=report['node_id'],involved_nodes=[0,1],reports=[report]),self.c['network']['max_message_bytes'],2.)
    def test_reports_before_ack_wait_and_retry_dedup(self):
        s=self.session('early');item=self.h.pending['early'];r=self.report()
        self.send(s,r);self.assertFalse(item['future'].done())
        with s.delivery_lock:item['entry']={'reply':{'ok':True}};self.h._finish(s,item)
        self.assertTrue(item['future'].done());s.close(True)
        self.assertTrue(self.send(s,r)[0]['cancelled'])
        bad=copy.deepcopy(r);bad['work_spent']=9
        with self.assertRaisesRegex(RuntimeError,'Conflicting'):self.send(s,bad)
    def test_child_manifest_not_prematurely_ready(self):
        s=self.session('branch');item=self.h.pending['branch'];item['entry']={}
        r=self.report();r.update(work_spent=4,children=[dict(branch_id='child',parent_branch_id='root',node_id=1,work_grant=6,branch_slots=1)])
        self.send(s,r);self.assertFalse(item['future'].done())
        child=self.report();child.update(branch_id='child',parent_branch_id='root',node_id=1,work_grant=6,branch_slots=1,visited_nodes=[0,1],work_spent=6)
        self.send(s,child);item['future'].result(1);s.close(True)
    def test_partial_socket_cannot_block_other_results(self):
        sock=socket.create_connection(self.h.endpoint);self.addCleanup(sock.close);sock.sendall(b'\x00')
        s=self.session('normal');self.h.pending['normal']['entry']={}
        self.send(s,self.report());self.h.pending['normal']['future'].result(1);s.close(True)
    def test_deadline_without_report(self):
        s=self.session('expiry');item=self.h.pending['expiry']
        with self.h.cv:item['deadline']=time.monotonic()+.03;self.h.cv.notify()
        with self.assertRaisesRegex(TimeoutError,'deadline'):item['future'].result(1)
        self.h.unregister(s,False)
    def test_active_lease_targets_only_unfinished_known_nodes(self):
        calls=[];sent=threading.Event()
        class Handler(socketserver.BaseRequestHandler):
            def handle(inner):
                message,_=receive(inner.request,8388608);calls.append(message)
                send(inner.request,{'ok':True},8388608);sent.set()
        server=socketserver.TCPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01});thread.start()
        def stop():server.shutdown();server.server_close();thread.join()
        self.addCleanup(stop)
        self.c['cluster']['nodes'][1].update(host='127.0.0.1',port=server.server_address[1])
        s=self.session('lease');item=self.h.pending['lease'];item['entry']={}
        r=self.report();r.update(work_spent=4,children=[dict(branch_id='child',parent_branch_id='root',node_id=1,work_grant=6,branch_slots=1)])
        self.send(s,r)
        with self.h.cv:item['next_lease']=time.monotonic();self.h.cv.notify()
        self.assertTrue(sent.wait(1))
        until=time.monotonic()+1
        while not s.control_by_kind['lease']['rpc_count'] and time.monotonic()<until:time.sleep(.001)
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0]['op'],'peer_lease');self.assertEqual(s.control_by_kind['lease']['rpc_count'],1)
        self.h.unregister(s,False)
