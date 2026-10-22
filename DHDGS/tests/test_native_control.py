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
                    if not dropped and (mode == 'drop_first' or mode == 'drop_first_report' and message['reports']):
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

    def make(self,mode='',delay=1000,capacity=4,early=False):
        context=mp.get_context('fork');parent,child=context.Pipe();stop=context.Event()
        process=context.Process(target=head_process,args=(child,stop,mode));process.start();child.close()
        self.assertTrue(parent.poll(2));port=parent.recv();self.heads.append((process,stop,parent))
        cfg=config(delay);cfg['native_runtime']={'early_parent_report':early,
            'queued_control_registration':getattr(self,'queued_registration',False)};cfg['peer']['communication']['max_pending_control']=capacity
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

    def test_early_report_is_acked_before_transfer_and_stays_immutable(self):
        c,port,pipe=self.make(early=True);ticket,_=self.branch(c,port)
        ticket.child({'branch_id':'child','node_id':1});ticket.complete(self.report())
        self.wait(lambda:self.row(c)['acked'])
        first=self.row(c)['report']
        self.assertFalse(first['transfer_metrics_complete'])
        self.assertEqual(self.row(c)['pending_transfers'],1)
        time.sleep(.25) # Retention must not erase an outstanding transfer.
        self.assertIsNotNone(self.row(c))
        ticket.transfer_done({'peer_rpc_count':1,'peer_request_bytes':123})
        self.wait(lambda:self.row(c)['pending_transfers']==0)
        self.assertEqual(self.row(c)['report'],first)
        self.assertEqual(self.row(c)['peer_request_bytes'],123)

    def test_early_report_late_failure_uses_separate_notification(self):
        c,port,pipe=self.make(early=True);ticket,_=self.branch(c,port)
        ticket.child({'branch_id':'child','node_id':1});ticket.complete(self.report())
        self.wait(lambda:self.row(c)['acked']);first=self.row(c)['report']
        ticket.transfer_done({'peer_rpc_count':1},'child ACK lost')
        self.wait(lambda:self.row(c)['late_error_acked'])
        self.assertEqual(self.row(c)['report'],first)
        messages=[]
        while pipe.poll(.02):messages.append(pipe.recv())
        notice=next(m for m in messages if m.get('transfer_failure'))
        self.assertEqual(notice['reports'],[])
        self.assertEqual(notice['error'],'child ACK lost')
        self.assertEqual([u['kind'] for u in c.take_updates()],['acked'])

    def test_early_report_lost_result_ack_retries_same_payload(self):
        c,port,pipe=self.make('drop_first_report',early=True);ticket,_=self.branch(c,port)
        ticket.child({'branch_id':'child','node_id':1});ticket.complete(self.report())
        self.wait(lambda:self.row(c)['acked'])
        messages=[]
        while pipe.poll(.02):messages.append(pipe.recv())
        reports=[m['reports'] for m in messages if m['reports']]
        self.assertTrue(reports)
        self.assertTrue(all(r==reports[0] for r in reports))
        ticket.transfer_done({})

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


class QueuedRegistrationTests(NativeControllerTests):
    queued_registration=True

    def branch(self,c,port,name='q',lease=None):
        ticket,control=super().branch(c,port,name,lease)
        self.wait(lambda:any(r['query_request_id']==name for r in c.snapshot()['branches']))
        return ticket,control

    def test_immediate_commands_follow_queued_registration(self):
        c,port,_=self.make(early=True)
        # Do not use the snapshot barrier in branch(): these operations must
        # remain ordered even if registration has not been consumed yet.
        ticket,_=NativeControllerTests.branch(self,c,port)
        ticket.child({'branch_id':'child','node_id':1})
        ticket.transfer_done({'peer_rpc_count':1})
        ticket.complete(self.report())
        self.wait(lambda:self.row(c) and self.row(c)['acked'])
        row=self.row(c)
        self.assertEqual(row['report']['peer_transfer']['peer_rpc_count'],1)
        trace=row['execution_timestamps']
        self.assertLessEqual(trace['native_register_enqueued_ns'],trace['native_register_consumed_ns'])
        self.assertLessEqual(trace['native_register_consumed_ns'],trace['local_done_dequeued_ns'])

    def test_concurrent_identity_reservation_accepts_exactly_once(self):
        from concurrent.futures import ThreadPoolExecutor
        c,port,_=self.make()
        def attempt(_):
            try:return NativeControllerTests.branch(self,c,port)[0]
            except ValueError:return None
        with ThreadPoolExecutor(8) as pool:
            tickets=[t for t in pool.map(attempt,range(32)) if t is not None]
        self.assertEqual(len(tickets),1)
        tickets[0].complete(self.report())
        self.wait(lambda:self.row(c) and self.row(c)['acked'])
        self.wait(lambda:not c.snapshot()['branches'])
        # Expiry releases the identity reservation even if the old ticket lives.
        again,_=NativeControllerTests.branch(self,c,port)
        again.cancel();c.close()
        with self.assertRaises(RuntimeError):NativeControllerTests.branch(self,c,port,'after-close')


def parallel_head(pipe, stop):
    import threading
    listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen();listener.settimeout(.05)
    pipe.send(listener.getsockname()[1])
    def serve(sock):
        with sock:
            sock.settimeout(.05)
            while not stop.is_set():
                try:message,_=receive(sock,1 << 20)
                except socket.timeout:continue
                except (OSError,ValueError):break
                time.sleep(.02) # Deliberately occupy the outbound lane.
                try:send(sock,{'ok':True,'cancelled':False,'lease_ms':1000,'involved_nodes':[0]},1 << 20)
                except OSError:break
    try:
        while not stop.is_set():
            try:sock,_=listener.accept()
            except socket.timeout:continue
            threading.Thread(target=serve,args=(sock,),daemon=True).start()
    finally:listener.close();pipe.close()


class AdaptiveControllerTests(unittest.TestCase):
    branch=NativeControllerTests.branch
    report=NativeControllerTests.report
    wait=NativeControllerTests.wait
    def setUp(self):
        ctx=mp.get_context('fork');self.pipe,child=ctx.Pipe();self.stop=ctx.Event()
        self.process=ctx.Process(target=parallel_head,args=(child,self.stop));self.process.start();child.close()
        self.assertTrue(self.pipe.poll(2));self.port=self.pipe.recv()
        cfg=config(0);cfg['peer']['retention_seconds']=.15
        cfg['native_runtime']={'adaptive_control':dict(enabled=True,min_lanes=1,max_lanes=3,
            command_queue_high=1,network_pending_high=1,high_samples=1,
            sample_interval_ms=2,cooldown_ms=2,idle_ms=10)}
        self.c=native().NativeController(cfg)
    def tearDown(self):
        self.c.close();self.stop.set();self.process.join(2)
        if self.process.is_alive():self.process.terminate();self.process.join(2)
        self.pipe.close()
    def test_scale_preserves_identity_completion_and_reclaims_threads(self):
        tickets=[]
        for i in range(12):
            t,_=self.branch(self.c,self.port,f'q{i}');tickets.append(t)
            time.sleep(.003)
        self.wait(lambda:self.c.snapshot()['adaptive_control']['scale_ups']>0)
        with self.assertRaises(ValueError):self.branch(self.c,self.port,'q0')
        for i,t in enumerate(tickets):t.complete(self.report(f'q{i}'))
        seen=set();expired=set()
        def drained():
            for u in self.c.take_updates():
                if u['kind']=='acked':
                    self.assertNotIn(u['branch_id'],seen);seen.add(u['branch_id'])
                if u['kind']=='expired':expired.add(u['branch_id'])
            return len(seen)==12 and len(expired)==12
        self.wait(drained)
        self.wait(lambda:self.c.snapshot()['adaptive_control']['lanes']==1)
        snap=self.c.snapshot();self.assertGreater(snap['adaptive_control']['scale_downs'],0)
        self.assertFalse(snap['branches']);self.assertGreaterEqual(snap['control_totals']['sent'],12)
        self.c.close();self.c.close()
        with self.assertRaises(RuntimeError):self.branch(self.c,self.port,'late')
    def test_transfer_ack_still_gates_parent_report(self):
        t,_=self.branch(self.c,self.port);t.child({'branch_id':'child','node_id':1});t.complete(self.report())
        self.wait(lambda:self.c.snapshot()['branches'][0]['status']=='waiting_transfer')
        self.assertFalse(self.c.snapshot()['branches'][0]['acked'])
        t.transfer_done({'peer_rpc_count':1})
        self.wait(lambda:any(r['acked'] for r in self.c.snapshot()['branches']))
