"""Protocol tests for worker publication with independent, controllable TCP ACKs."""
import multiprocessing as mp
import socket
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from dhdgs.index import native
from dhdgs.transport import receive, send


def server(pipe, stop, gate, mode):
    listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen();listener.settimeout(.05)
    pipe.send(listener.getsockname()[1]);lock=threading.Lock();dropped=False
    def serve(sock):
        nonlocal dropped
        with sock:
            sock.settimeout(.05)
            while not stop.is_set():
                try:message,_=receive(sock,1 << 20)
                except socket.timeout:continue
                except (OSError,ValueError):break
                with lock:
                    pipe.send(message)
                    drop=mode=='drop_report' and bool(message['reports']) and not dropped
                    if drop:dropped=True
                if drop:break
                if mode=='hold_heartbeat' and not message['reports'] and not message['error']:gate.wait(1)
                if mode=='hold_results':gate.wait(1)
                try:send(sock,dict(ok=True,cancelled=False,lease_ms=5000,involved_nodes=[0]),1 << 20)
                except OSError:break
    try:
        while not stop.is_set():
            try:sock,_=listener.accept()
            except socket.timeout:continue
            threading.Thread(target=serve,args=(sock,),daemon=True).start()
    finally:listener.close()


class SearchThreadReportTests(unittest.TestCase):
    def setUp(self):self.controller=None;self.proc=None
    def tearDown(self):
        if self.controller:self.controller.close()
        if self.proc:
            self.gate.set();self.stop.set();self.proc.join(2)
            if self.proc.is_alive():self.proc.terminate();self.proc.join(2)
            self.pipe.close()
    def make(self,mode='',capacity=8,credits=128,adaptive=False,heartbeat=10,retry_ms=None):
        ctx=mp.get_context('fork');self.pipe,child=ctx.Pipe();self.stop=ctx.Event();self.gate=ctx.Event()
        self.proc=ctx.Process(target=server,args=(child,self.stop,self.gate,mode));self.proc.start();child.close()
        self.assertTrue(self.pipe.poll(2));self.port=self.pipe.recv()
        cfg={'peer':{'heartbeat_interval_ms':heartbeat,'retention_seconds':.3,'lease_timeout_ms':5000,
             'max_active_queries':credits,'control_timeout_seconds':1.,
             'control':{'initial_heartbeat_delay_ms':0 if mode=='hold_heartbeat' else 1000},
             'communication':{'max_pending_control':capacity,'connections_per_endpoint':4,'max_connection_endpoints':8}},
             'network':{'max_message_bytes':1 << 20},
             'native_runtime':{'data_enabled':True,'early_parent_report':True,'queued_control_registration':True,'search_thread_reports':True}}
        if retry_ms is not None:cfg['native_runtime']['report_capacity_retry_ms']=retry_ms
        if adaptive:
            cfg['native_runtime']['adaptive_control']=dict(enabled=True,min_lanes=1,max_lanes=2,
                command_queue_high=1,network_pending_high=1,high_samples=1,
                sample_interval_ms=2,cooldown_ms=2,idle_ms=10)
        self.controller=native().NativeController(cfg)
        return self.controller
    def branch(self,name='q'):
        now=time.monotonic();control=native().SearchControl(now+5,now+5)
        ticket=self.controller.register_branch(dict(query_request_id=name,branch_id=name+'-root',token='test',
            head_endpoint=['127.0.0.1',self.port],entry_node=0,target_node=0,visited_nodes=[0],
            created_ns=time.monotonic_ns(),accepted_ns=time.monotonic_ns(),lease=now+5,deadline=now+5,trace=True),control)
        ticket.start(.1,.2,.3)
        return ticket
    @staticmethod
    def report(name='q'):
        return dict(branch_id=name+'-root',ids=[7],squared_distances=[.25],stats={},
                    native_timestamps={'native_loop_done_ns':time.monotonic_ns()})
    def row(self,name='q'):
        return next((r for r in self.controller.snapshot(True)['branches'] if r['query_request_id']==name),None)
    def wait(self,predicate):
        end=time.monotonic()+3
        while time.monotonic()<end:
            self.controller.poll_reports()
            result=predicate()
            if result:return result
            time.sleep(.002)
        self.fail('condition did not complete')
    def test_worker_owns_publication_and_child_manifest(self):
        self.make();t=self.branch();t.child(dict(branch_id='child',node_id=1))
        owner=threading.get_native_id();t.complete(self.report())
        self.assertTrue(self.pipe.poll(1));message=self.pipe.recv()
        self.assertEqual(message['children'],[dict(branch_id='child',node_id=1)])
        self.assertEqual(message['involved_nodes'],[0,1])
        self.assertEqual(message['reports'][0]['pending_transfers_at_publication'],1)
        self.assertEqual(message['reports'][0]['ids'],[7])
        self.wait(lambda:self.row() and self.row()['acked'])
        trace=self.row()['execution_timestamps']
        self.assertEqual(trace['completion_publish_path'],'search_thread')
        self.assertEqual(trace['report_prepare_thread_id'],owner)
        self.assertEqual(trace['report_submit_thread_id'],owner)
        self.assertNotIn('local_done_enqueued_ns',trace)
        t.transfer_done({'peer_rpc_count':1})
        self.assertEqual(self.row()['report']['pending_transfers_at_publication'],1)
    def test_result_does_not_wait_for_heartbeat_ack(self):
        self.make('hold_heartbeat');t=self.branch()
        self.assertTrue(self.pipe.poll(1));self.assertFalse(self.pipe.recv()['reports'])
        t.complete(self.report())
        self.assertTrue(self.pipe.poll(.5));self.assertTrue(self.pipe.recv()['reports'])
        self.wait(lambda:self.row()['acked'])
        self.assertFalse(self.gate.is_set())
        self.gate.set()
    def test_saturated_sender_defers_without_losing_report(self):
        self.make('hold_results',capacity=1)
        a=self.branch();b=self.branch('b');a.complete(self.report())
        self.assertTrue(self.pipe.poll(1));self.pipe.recv()
        before=time.monotonic();b.complete(self.report('b'))
        self.assertLess(time.monotonic()-before,.25)
        self.assertFalse(self.pipe.poll(.03));self.gate.set()
        self.wait(lambda:self.row('b') and self.row('b')['acked'])
        self.assertGreater(self.row('b')['execution_timestamps']['report_deferred_count'],0)
        self.assertEqual(self.controller.snapshot()['report_credits_in_use'],0)
    def test_capacity_retry_does_not_wait_for_heartbeat_interval(self):
        self.make('hold_results',capacity=1,heartbeat=1000,retry_ms=5)
        a=self.branch();b=self.branch('b');a.complete(self.report())
        self.assertTrue(self.pipe.poll(1));self.pipe.recv()
        b.complete(self.report('b'))
        self.assertGreater(self.row('b')['execution_timestamps']['report_deferred_count'],0)
        self.gate.set();begin=time.monotonic()
        self.wait(lambda:self.row('b') and self.row('b')['acked'])
        self.assertLess(time.monotonic()-begin,.5)

    def test_lost_ack_retries_identical_report(self):
        self.make('drop_report');t=self.branch();t.complete(self.report())
        self.assertTrue(self.pipe.poll(1));first=self.pipe.recv()
        self.wait(lambda:self.row() and self.row()['acked'])
        self.assertTrue(self.pipe.poll(1));self.assertEqual(self.pipe.recv(),first)
    def test_duplicate_completion_is_idempotent(self):
        self.make();t=self.branch()
        with ThreadPoolExecutor(8) as pool:list(pool.map(lambda _:t.complete(self.report()),range(24)))
        self.wait(lambda:self.row() and self.row()['acked'])
        messages=[]
        while self.pipe.poll(.03):messages.append(self.pipe.recv())
        self.assertEqual(len([m for m in messages if m['reports']]),1)
    def test_late_transfer_error_is_separate(self):
        self.make();t=self.branch();t.child(dict(branch_id='child',node_id=1));t.complete(self.report())
        self.wait(lambda:self.row() and self.row()['acked'])
        t.transfer_done({'peer_rpc_count':1},'remote rejected')
        self.wait(lambda:self.row()['late_error_acked'])
        messages=[]
        while self.pipe.poll(.02):messages.append(self.pipe.recv())
        late=[m for m in messages if m['transfer_failure']]
        self.assertEqual(len(late),1);self.assertFalse(late[0]['reports'])
        self.assertEqual(self.row()['report']['ids'],[7])
    def test_cancel_and_invalid_report_publish_errors(self):
        self.make();t=self.branch();t.cancel();t.complete(self.report())
        self.wait(lambda:self.row() and self.row()['acked'])
        self.assertIsNone(self.row()['report'])
        bad=self.branch('bad');bad.complete({'missing':'stats'})
        self.wait(lambda:self.row('bad') and self.row('bad')['acked'])
        self.assertEqual(self.row('bad')['status'],'failed')
    def test_credits_bound_pending_reports_and_release_after_ack(self):
        self.make('hold_results',credits=1);t=self.branch()
        with self.assertRaises(RuntimeError):self.branch('overflow')
        t.complete(self.report());self.gate.set()
        self.wait(lambda:self.row() and self.row()['acked'])
        self.branch('next').complete(self.report('next'))
        self.wait(lambda:self.row('next') and self.row('next')['acked'])

    def test_adaptive_retirement_preserves_direct_report_counters(self):
        self.make('hold_results',adaptive=True)
        self.branch().complete(self.report())
        self.wait(lambda:self.controller.snapshot()['adaptive_control']['lanes']==2)
        self.branch('second').complete(self.report('second'))
        snapshot=self.controller.snapshot()
        self.assertTrue(snapshot['search_thread_reports'])
        self.assertEqual(snapshot['report_credits_in_use'],2)
        self.assertTrue(all(lane['report_credits_in_use']==1
            for lane in snapshot['adaptive_control']['lane_stats']))
        self.gate.set()
        self.wait(lambda:self.row() and self.row('second') and self.row()['acked'] and self.row('second')['acked'])
        self.assertEqual(self.controller.snapshot()['report_credits_in_use'],0)
        before=self.controller.snapshot()['control_totals']['sent']
        self.assertEqual(before,2)
        self.wait(lambda:self.controller.snapshot()['adaptive_control']['lanes']==1)
        self.assertEqual(self.controller.snapshot()['control_totals']['sent'],before)


if __name__=='__main__':unittest.main()
