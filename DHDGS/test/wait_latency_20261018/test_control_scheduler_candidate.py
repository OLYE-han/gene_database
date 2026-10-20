"""Deterministic scheduler tests; candidate module is not production-installed."""
import importlib.machinery
import importlib.util
from pathlib import Path
from concurrent.futures import Future
import threading
import unittest
from types import SimpleNamespace

p = Path(__file__).with_name('control_scheduler_candidate.py.txt')
loader = importlib.machinery.SourceFileLoader('control_candidate', str(p))
spec = importlib.util.spec_from_loader(loader.name, loader)
module = importlib.util.module_from_spec(spec)
loader.exec_module(module)


class Clock:
    now = 1.
    def monotonic(self):return self.now
    def thread_time(self):return self.now


class Sender:
    def __init__(self):self.calls=[];self.fail=False
    def submit(self, fn, key, queued):
        if self.fail:raise RuntimeError('sender full')
        self.calls.append((key,queued))


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();module.time=self.clock
        self.r=SimpleNamespace(c={'peer':{'heartbeat_interval_ms':1000,
            'communication':{'max_pending_control':2},'retention_seconds':10.,'max_tombstones':2}},
            lock=threading.Lock(),stop=threading.Event(),control_wakeup=threading.Event(),
            records={},tombstones={},query_visits={},control_active=0,control_workers=Sender())
        self.r._send_control=lambda *args:None
        self.r._finalize_ready=self.finalize
        self.s=module.ControlEventScheduler(self.r)

    def add(self,key=('q','root'),**values):
        rec=dict(children=[],report=None,error=None,status='running',acked=False,
            control_inflight=False,control_next_at=5.,control_signature=((),False,None),
            touched=0.,deadline=100.)
        rec.update(values);self.r.records[key]=rec;self.r.query_visits[key[0]]=key
        return rec

    def finish(self,key=('q','root'),acked=False):
        rec=self.r.records[key]
        rec['control_inflight']=False;rec['acked']=acked
        rec['control_next_at']=self.clock.now+1
        self.r.control_active-=1
        self.s.dirty(key)

    def finalize(self,key):
        with self.r.lock:
            rec=self.r.records[key]
            if not all(f.done() for f in rec['transfer_futures']):return
            report=rec.pop('pending_report');futures=rec.pop('transfer_futures')
            try:
                for future in futures:future.result()
                if self.r.stop.is_set():raise RuntimeError('stopped')
                rec['report']=report;rec['status']='complete'
            except Exception as exc:
                rec['error']=str(exc);rec['status']='failed'
            rec['touched']=self.clock.now

    def test_initial_timer_and_due_heartbeat(self):
        self.add();self.s.dirty(('q','root'))
        self.assertEqual(self.s.step(),4.)
        self.assertFalse(self.r.control_workers.calls)
        self.clock.now=5.;self.s.step()
        self.assertEqual(len(self.r.control_workers.calls),1)
        self.assertEqual(self.r.control_active,1)

    def test_topology_and_result_bypass_timer(self):
        rec=self.add();rec['children']=[{'branch_id':'child'}]
        self.s.dirty(('q','root'));self.s.step()
        self.assertEqual(len(self.r.control_workers.calls),1)
        rec['report']={'stats':{}};rec['status']='complete'
        self.s.dirty(('q','root'));self.s.step()
        self.assertEqual(len(self.r.control_workers.calls),1)
        self.finish();self.s.step()
        self.assertEqual(len(self.r.control_workers.calls),2)

    def test_inflight_heartbeat_upgrade_acked_result_is_not_resent(self):
        rec=self.add(control_next_at=0.)
        self.s.dirty(('q','root'));self.s.step()
        rec['report']={'stats':{}};rec['status']='complete'
        self.s.dirty(('q','root'));self.s.step()
        self.finish(acked=True);self.s.step()
        self.clock.now=6.;self.s.step()
        self.assertEqual(len(self.r.control_workers.calls),1)
        self.assertEqual(self.r.control_active,0)

    def test_failed_send_retries_on_timer_not_hot_loop(self):
        self.add(report={'stats':{}},status='complete')
        self.s.dirty(('q','root'));self.s.step();self.finish()
        self.assertEqual(self.s.step(),1.)
        self.assertEqual(len(self.r.control_workers.calls),1)
        self.clock.now=2.;self.s.step()
        self.assertEqual(len(self.r.control_workers.calls),2)

    def test_rejected_submission_restores_capacity(self):
        self.add(control_next_at=0.)
        self.r.control_workers.fail=True
        self.s.dirty(('q','root'));self.s.step()
        self.assertFalse(self.r.records[('q','root')]['control_inflight'])
        self.assertEqual(self.r.control_active,0)
        self.assertEqual(self.s.stats['submit_errors'],1)
        self.s.step();self.r.control_workers.fail=False
        self.clock.now=2.;self.s.step()
        self.assertEqual(len(self.r.control_workers.calls),1)

    def test_retention_and_tombstone_expire_without_table_scan(self):
        self.add(report={'stats':{}},status='complete',acked=True)
        self.s.dirty(('q','root'));self.s.step()
        self.clock.now=11.;self.s.step()
        self.assertFalse(self.r.records)
        self.assertIn(('q','root'),self.r.tombstones)
        self.assertIn('q',self.r.query_visits)
        self.clock.now=22.;self.s.step()
        self.assertFalse(self.r.tombstones)
        self.assertFalse(self.r.query_visits)

    def test_inflight_terminal_record_cannot_expire(self):
        rec=self.add(report={'stats':{}},status='complete',control_inflight=True)
        self.r.control_active=1
        self.s.dirty(('q','root'));self.s.step()
        self.clock.now=200.;self.s.step()
        self.assertIn(('q','root'),self.r.records)
        self.finish();self.s.step()
        self.assertNotIn(('q','root'),self.r.records)

    def test_local_done_never_takes_runtime_lock_and_waits_without_compute(self):
        rec=self.add();future=Future()
        with self.r.lock:
            self.s.local_done(('q','root'),{'stats':{}},[future],0.5)
        self.assertNotIn('pending_report',rec)
        self.s.step()
        self.assertEqual(rec['status'],'waiting_transfer')
        self.assertFalse(self.r.control_workers.calls)
        future.set_result(None)
        self.s.step()
        self.assertEqual(rec['status'],'complete')
        self.assertEqual(len(self.r.control_workers.calls),1)
        self.assertNotIn('pending_report',rec)

    def test_immediately_completed_future_and_failure_callback(self):
        rec=self.add();future=Future();future.set_exception(RuntimeError('transfer failed'))
        self.s.local_done(('q','root'),{'stats':{}},[future],0.5)
        self.s.step()
        self.assertEqual(rec['status'],'failed')
        self.assertEqual(rec['error'],'transfer failed')
        self.assertEqual(len(self.r.control_workers.calls),1)

    def test_close_drains_late_local_done_without_sending(self):
        rec=self.add();future=Future();future.set_result(None)
        self.r.stop.set()
        self.s.local_done(('q','root'),{'stats':{}},[future],0.5)
        self.s.finish_stopped()
        self.assertEqual(rec['status'],'failed')
        self.assertNotIn('pending_report',rec)
        self.assertFalse(self.r.control_workers.calls)

    def test_dirty_is_coalesced_and_terminal_priority_respects_capacity(self):
        self.r.c['peer']['communication']['max_pending_control']=1;self.s.capacity=1
        self.add(('a','a'),control_next_at=0.)
        self.add(('b','b'),report={'stats':{}},status='complete')
        for _ in range(100):self.s.dirty(('a','a'))
        self.s.dirty(('b','b'));self.s.step()
        self.assertEqual(self.s.stats['dirty_records'],2)
        self.assertEqual(self.r.control_workers.calls[0][0],('b','b'))
        self.finish(('b','b'),acked=True);self.s.step()
        self.assertEqual(self.r.control_workers.calls[1][0],('a','a'))

    def test_stale_timers_are_bounded(self):
        rec=self.add()
        for i in range(250):
            rec['control_next_at']=50.+i
            self.s.dirty(('q','root'));self.s.step()
        self.assertLessEqual(len(self.s._heap),2*len(self.s._timers)+64)
        self.clock.now=298.;self.s.step()
        self.assertFalse(self.r.control_workers.calls)
        self.clock.now=299.;self.s.step()
        self.assertEqual(len(self.r.control_workers.calls),1)


if __name__=='__main__':unittest.main()
