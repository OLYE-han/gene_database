"""Ownership, fairness, expiry, shutdown and real RPC accounting."""
import socket
import threading
import time
import unittest
from unittest.mock import patch
from dhdgs.peer_rpc import FairConnectionPool,ConnectionPool,rpc_exchange
from dhdgs.transport import send,receive
import test_pool_reservation as reservation_tests


class FairReservationTests(reservation_tests.PoolReservationTests):
    """Apply existing failure/health-probe tests to the FIFO implementation."""
    def setUp(self):
        super().setUp()
        self.replace=patch('test_pool_reservation.ConnectionPool',
            side_effect=lambda a,b,mode='reserved':FairConnectionPool(a,b))
        self.replace.start()
        self.addCleanup(self.replace.stop)

    def test_default_remains_locked_and_invalid_mode_rejected(self):
        # This inherited check concerns the unchanged legacy constructor.
        self.assertEqual(ConnectionPool(1,1).probe_mode,'locked')
        with self.assertRaises(ValueError):ConnectionPool(1,1,'invalid')

    def test_probe_reservation_prevents_endpoint_eviction(self):
        # Legacy and fair paths use different timeout wording.
        pool=FairConnectionPool(1,1)
        _,entry=pool.borrow(('a',1),time.monotonic()+1)
        with self.assertRaises(TimeoutError):pool.borrow(('b',2),time.monotonic())
        self.assertIs(pool.entries[('a',1)],entry)
        pool.release(entry,None,False);pool.close()


class FairQueueTests(unittest.TestCase):
    def queued(self,pool,endpoint,size):
        deadline=time.monotonic()+2
        while time.monotonic()<deadline:
            with pool.cv:
                if len(pool.pending.get(endpoint,()))==size:return
            time.sleep(.001)
        self.fail('waiter did not enqueue')

    def test_fifo_no_barging_and_other_target_progress(self):
        p=FairConnectionPool(1,2);self.addCleanup(p.close)
        _,entry=p.borrow(('a',1),time.monotonic()+2)
        order=[];errors=[];threads=[]
        def worker(i):
            try:
                sock,e=p.borrow(('a',1),time.monotonic()+5)
                order.append(i);p.release(e,sock,True)
            except Exception as exc:errors.append(exc)
        for i in range(12):
            t=threading.Thread(target=worker,args=(i,));threads.append(t);t.start()
            self.queued(p,('a',1),i+1)
        other,e=p.borrow(('b',2),time.monotonic()+1);p.release(e,other,True)
        p.release(entry,None,True)
        newcomer=threading.Thread(target=worker,args=(12,));newcomer.start();threads.append(newcomer)
        for t in threads:t.join(6);self.assertFalse(t.is_alive())
        self.assertFalse(errors);self.assertEqual(order,list(range(13)))
        self.assertFalse(p.pending);self.assertTrue(all(e['borrowed']==0 for e in p.entries.values()))

    def test_timeout_unlinks_waiter_and_next_can_acquire(self):
        p=FairConnectionPool(1,1);self.addCleanup(p.close)
        _,entry=p.borrow(('a',1),time.monotonic()+1)
        with self.assertRaises(TimeoutError):p.borrow(('a',1),time.monotonic()+.01)
        self.assertFalse(p.pending)
        p.release(entry,None,False)
        _,again=p.borrow(('a',1),time.monotonic()+1);p.release(again,None,False)
        self.assertEqual(again['borrowed'],0)

    def test_endpoint_capacity_waiter_gets_released_capacity(self):
        p=FairConnectionPool(1,1);self.addCleanup(p.close)
        _,entry=p.borrow(('a',1),time.monotonic()+1);found=[]
        def worker():
            sock,e=p.borrow(('b',2),time.monotonic()+2);found.append(e['endpoint']);p.release(e,sock,True)
        t=threading.Thread(target=worker);t.start();self.queued(p,('b',2),1)
        p.release(entry,None,False);t.join(3)
        self.assertFalse(t.is_alive());self.assertEqual(found,[('b',2)])

    def test_close_wakes_all_pending_without_stealing_borrowed(self):
        p=FairConnectionPool(1,1);_,entry=p.borrow(('a',1),time.monotonic()+1)
        errors=[];threads=[]
        def worker(endpoint):
            try:p.borrow(endpoint,time.monotonic()+5)
            except Exception as exc:errors.append(str(exc))
        for endpoint in [('a',1),('b',2)]:
            t=threading.Thread(target=worker,args=(endpoint,));t.start();threads.append(t)
            self.queued(p,endpoint,1)
        p.close()
        for t in threads:t.join(1);self.assertFalse(t.is_alive())
        self.assertEqual(errors,['RPC pool closed']*2);self.assertEqual(entry['borrowed'],1)
        p.release(entry,None,False);self.assertEqual(entry['borrowed'],0)

    def test_close_or_expiry_after_grant_reclaims_slot(self):
        # Force the exact grant/wakeup boundary without nondeterministic races.
        for reason in ('close','expiry'):
            with self.subTest(reason=reason):
                p=FairConnectionPool(1,1)
                def wait(_event,_timeout):
                    if reason=='close':p.close()
                    else:time.sleep(.02)
                    return True
                with patch.object(threading.Event,'wait',wait):
                    with self.assertRaises((RuntimeError,TimeoutError)):
                        p.borrow(('a',1),time.monotonic()+.01)
                self.assertEqual(p.entries[('a',1)]['borrowed'],0)
                self.assertFalse(p.pending);p.close()

    def test_rpc_includes_release_timestamps_without_changing_wire(self):
        a,b=socket.socketpair();p=FairConnectionPool(1,1);self.addCleanup(p.close)
        self.addCleanup(a.close);self.addCleanup(b.close)
        _,entry=p.borrow(('local',1),time.monotonic()+1);p.release(entry,a,True)
        def server():
            message,_=receive(b,4096);send(b,dict(ok=True,echo=message['value']),4096)
        t=threading.Thread(target=server);t.start()
        reply,m=rpc_exchange(('local',1),dict(value=7),4096,2,p);t.join(2)
        self.assertEqual(reply['echo'],7)
        self.assertLessEqual(m['response_received_ns'],m['rpc_finally_enter_ns'])
        self.assertLessEqual(m['rpc_finally_enter_ns'],m['rpc_release_done_ns'])
        self.assertGreaterEqual(m['wall_including_release_ms'],m['wall_ms'])
        self.assertGreaterEqual(m['pool_release_ms'],m['pool_release_lock_wait_ms'])

    def test_config_optional_legacy_and_explicit_fifo(self):
        from pathlib import Path
        from dhdgs.config import load_config,validate
        c=load_config(Path(__file__).resolve().parents[1]/'configs/dhdgs_pilot.yaml')
        self.assertEqual(c['peer']['connection_pool']['head_scheduling'],'fifo')
        # load_config adds metadata; validate only the public configuration.
        c={k:v for k,v in c.items() if not k.startswith('_')}
        for mode in ('fifo','broadcast'):
            c['peer']['connection_pool']['head_scheduling']=mode;validate(c)
        c['peer']['connection_pool']['head_scheduling']='unknown'
        with self.assertRaises(ValueError):validate(c)
        del c['peer']['connection_pool']['head_scheduling'];validate(c)

    def test_head_policy_does_not_change_data_execution_signature(self):
        from pathlib import Path
        from dhdgs.config import load_config
        from dhdgs.peer import execution_signature
        c=load_config(Path(__file__).resolve().parents[1]/'configs/dhdgs_pilot.yaml')
        c['peer']['connection_pool'].pop('head_scheduling',None)
        signature=execution_signature(c)
        for mode in ('fifo','broadcast'):
            c['peer']['connection_pool']['head_scheduling']=mode
            self.assertEqual(execution_signature(c),signature)
            self.assertEqual(c['peer']['connection_pool']['head_scheduling'],mode)
        c['peer']['worker_threads']+=1
        self.assertNotEqual(execution_signature(c),signature)


if __name__=='__main__':unittest.main()
