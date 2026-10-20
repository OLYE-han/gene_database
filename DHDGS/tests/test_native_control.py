"""Native completion/control protocol checks against an independent TCP head."""
import ctypes
import multiprocessing as mp
import socket
import time
import unittest

from dhdgs.index import native
from dhdgs.transport import receive, send


def head_process(pipe, stop, mode):
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0));listener.listen();listener.settimeout(.05)
    pipe.send(listener.getsockname()[1])
    dropped = False
    try:
        while not stop.is_set():
            try:sock, _ = listener.accept()
            except socket.timeout:continue
            with sock:
                sock.settimeout(.05)
                while not stop.is_set():
                    try:message, _ = receive(sock, 1 << 20)
                    except socket.timeout:continue
                    except (OSError, ValueError):break
                    pipe.send(message)
                    if mode == 'drop_first' and not dropped:
                        dropped = True;break
                    if mode == 'hold_heartbeat' and not message['reports'] and not message['error']:
                        time.sleep(.03)
                    try:send(sock, {'ok':True,'cancelled':mode=='cancel','lease_ms':1000,'involved_nodes':[0]}, 1 << 20)
                    except OSError:break
    finally:listener.close();pipe.close()


def config(delay=1000):
    return {'peer':{'heartbeat_interval_ms':10,'retention_seconds':.2,'lease_timeout_ms':1000,
        'control_timeout_seconds':.2,'control':{'initial_heartbeat_delay_ms':delay},
        'communication':{'max_pending_control':4,'connections_per_endpoint':1,'max_connection_endpoints':8}},
        'network':{'max_message_bytes':1 << 20}}


class NativeControllerTests(unittest.TestCase):
    def setUp(self):
        self.controllers=[];self.heads=[]

    def tearDown(self):
        for c in self.controllers:c.close()
        for process,stop,pipe in self.heads:
            stop.set();process.join(2)
            if process.is_alive():process.terminate();process.join(2)
            pipe.close()

    def make(self,mode='',delay=1000,capacity=4):
        context=mp.get_context('fork');parent,child=context.Pipe();stop=context.Event()
        process=context.Process(target=head_process,args=(child,stop,mode));process.start();child.close()
        self.assertTrue(parent.poll(2));port=parent.recv();self.heads.append((process,stop,parent))
        cfg=config(delay);cfg['peer']['communication']['max_pending_control']=capacity
        c=native().NativeController(cfg);self.controllers.append(c)
        return c,port,parent

    def branch(self,c,port,name='q',lease=None):
        now=time.monotonic();lease=now+1 if lease is None else lease
        control=native().SearchControl(lease,now+2)
        ticket=c.register_branch({'query_request_id':name,'branch_id':name+'-root','token':'secret',
            'parent_branch_id':None,'head_endpoint':['127.0.0.1',port],'entry_node':0,'target_node':0,
            'visited_nodes':[0],'ancestry':[],'work_grant':10,'branch_slots':1,
            'accepted_ns':time.monotonic_ns(),'created_ns':time.monotonic_ns(),
            'lease':lease,'deadline':now+2,'trace':True},control)
        ticket.start(.1,.2,.3)
        return ticket,control

    def wait(self,predicate):
        end=time.monotonic()+2
        while time.monotonic()<end:
            result=predicate()
            if result:return result
            time.sleep(.002)
        self.fail('native controller condition did not complete')

    def row(self,c):
        rows=[r for r in c.snapshot(True)['branches'] if r['query_request_id']=='q']
        return rows[0] if rows else None

    def report(self,name='q'):
        return {'branch_id':name+'-root','stats':{},'ids':[1],'squared_distances':[.2],
                'native_timestamps':{'native_loop_done_ns':time.monotonic_ns()}}

    def test_completion_does_not_need_python_gil(self):
        c,port,pipe=self.make();ticket,_=self.branch(c,port)
        ticket.complete(self.report())
        # PyDLL deliberately keeps this interpreter's GIL while native threads
        # send to the independent process and receive its ACK.
        libc=ctypes.PyDLL(None);libc.usleep.argtypes=[ctypes.c_uint]
        libc.usleep(100_000);after=time.monotonic_ns()
        row=self.row(c)
        self.assertTrue(row['acked']);self.assertLess(row['control_events'][0]['sender_rpc_return_ns'],after)
        self.assertEqual(row['report']['stats']['query_ready_queue_ms'],.3)
        self.assertEqual(c.take_updates()[0]['kind'],'acked')

    def test_completion_waits_for_transfer_without_blocking_other_branch(self):
        c,port,pipe=self.make();ticket,_=self.branch(c,port)
        ticket.child({'branch_id':'child','node_id':1});ticket.complete(self.report())
        self.wait(lambda:self.row(c) and self.row(c)['status']=='waiting_transfer')
        other,_=self.branch(c,port,'other');other.complete(self.report('other'))
        self.wait(lambda:any(r['acked'] and r['query_request_id']=='other' for r in c.snapshot()['branches']))
        self.assertIsNone(self.row(c)['report'])
        ticket.transfer_done({'peer_rpc_count':1,'peer_request_bytes':123})
        self.wait(lambda:self.row(c)['acked'])
        self.assertEqual(self.row(c)['report']['peer_transfer']['peer_request_bytes'],123)

    def test_transfer_before_completion_and_transfer_error(self):
        c,port,pipe=self.make();ticket,_=self.branch(c,port)
        ticket.child({'branch_id':'child','node_id':1});ticket.transfer_done({'peer_rpc_count':1})
        ticket.complete(self.report());self.wait(lambda:self.row(c)['acked'])
        other,_=self.branch(c,port,'bad');other.child({'branch_id':'child2','node_id':1})
        other.transfer_done({},'transfer failed');other.complete(self.report('bad'))
        rows=self.wait(lambda:[r for r in c.snapshot(True)['branches'] if r['query_request_id']=='bad' and r['acked']])
        self.assertEqual(rows[0]['error'],'transfer failed');self.assertIsNone(rows[0]['report'])

    def test_cancel_and_expired_lease_never_publish_success(self):
        c,port,pipe=self.make();ticket,_=self.branch(c,port)
        ticket.child({'branch_id':'child','node_id':1});ticket.complete(self.report());ticket.cancel()
        ticket.transfer_done({});self.wait(lambda:self.row(c)['acked'])
        self.assertIsNone(self.row(c)['report']);self.assertIn('cancel',self.row(c)['error'])
        other,_=self.branch(c,port,'expired',lease=time.monotonic()-.01);other.complete(self.report('expired'))
        rows=self.wait(lambda:[r for r in c.snapshot(True)['branches'] if r['query_request_id']=='expired' and r['acked']])
        self.assertIsNone(rows[0]['report']);self.assertIn('expired',rows[0]['error'])

    def test_lost_ack_retries_immutable_report(self):
        c,port,pipe=self.make('drop_first');ticket,_=self.branch(c,port);ticket.complete(self.report())
        self.wait(lambda:self.row(c) and self.row(c)['acked'])
        self.assertTrue(pipe.poll(1));first=pipe.recv();self.assertTrue(pipe.poll(1));second=pipe.recv()
        self.assertEqual(first['reports'],second['reports'])
        self.assertEqual(len(c.take_updates()),1)
        self.assertGreaterEqual(c.snapshot()['control_totals']['errors'],1)

    def test_inflight_heartbeat_followed_by_result(self):
        c,port,pipe=self.make('hold_heartbeat',delay=0);ticket,_=self.branch(c,port)
        self.assertTrue(pipe.poll(1));heartbeat=pipe.recv();self.assertEqual(heartbeat['reports'],[])
        ticket.complete(self.report());self.wait(lambda:self.row(c)['acked'])
        self.assertTrue(pipe.poll(1));result=pipe.recv();self.assertTrue(result['reports'])
        self.assertEqual(len(c.take_updates()),1)

    def test_bad_completion_becomes_explicit_error(self):
        c,port,pipe=self.make();ticket,_=self.branch(c,port);ticket.complete({'missing':'stats'})
        self.wait(lambda:self.row(c) and self.row(c)['acked'])
        self.assertIn('Native completion error',self.row(c)['error'])

    def test_close_drains_pending_ownership_and_is_idempotent(self):
        c,port,pipe=self.make();ticket,_=self.branch(c,port)
        ticket.child({'branch_id':'child','node_id':1});ticket.complete(self.report())
        c.close();c.close()
        row=self.row(c);self.assertEqual(row['status'],'failed');self.assertTrue(row['cancelled'])
        self.assertEqual(c.snapshot()['control_active'],0)
        ticket.complete(self.report());ticket.transfer_done({})
        self.assertEqual(self.row(c)['status'],'failed')

    def test_duplicate_registration_is_synchronous_error(self):
        c,port,pipe=self.make();self.branch(c,port)
        with self.assertRaisesRegex(ValueError,'Duplicate'):self.branch(c,port)

    def test_oversize_unsent_report_returns_explicit_failure(self):
        c,port,pipe=self.make();ticket,_=self.branch(c,port)
        report=self.report();report['oversize']=b'x'*(1 << 20);ticket.complete(report)
        self.wait(lambda:self.row(c)['acked'])
        self.assertIn('encode error',self.row(c)['error']);self.assertIsNone(self.row(c)['report'])

    def test_result_upgrades_already_queued_heartbeat_priority(self):
        c,port,pipe=self.make('hold_heartbeat',delay=0,capacity=1)
        self.branch(c,port);self.assertTrue(pipe.poll(1));pipe.recv()
        self.branch(c,port,'a');ticket,_=self.branch(c,port,'b');ticket.complete(self.report('b'))
        self.assertTrue(pipe.poll(1));message=pipe.recv()
        self.assertEqual(message['branch_id'],'b-root');self.assertTrue(message['reports'])

    def test_head_cancel_fails_pending_transfer_without_waiting_for_it(self):
        c,port,pipe=self.make('cancel');ticket,control=self.branch(c,port)
        ticket.child({'branch_id':'child','node_id':1});ticket.complete(self.report())
        self.wait(lambda:self.row(c)['acked'])
        self.assertTrue(control.cancelled());self.assertIsNone(self.row(c)['report'])

    def test_acked_retention_expires_once(self):
        c,port,pipe=self.make();ticket,_=self.branch(c,port);ticket.complete(self.report())
        self.wait(lambda:self.row(c)['acked']);self.assertEqual(c.take_updates()[0]['kind'],'acked')
        self.wait(lambda:not c.snapshot()['branches'])
        self.assertEqual([u['kind'] for u in c.take_updates()],['expired'])
