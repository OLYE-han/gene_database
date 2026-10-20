"""Real TCP acceptance, bounded channel backpressure and completion isolation."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from unittest.mock import patch
from test_head_direct import DirectTests,config
from dhdgs.head_direct import DirectHub
from dhdgs.peer import exchange
from dhdgs.pipeline_hub import PipelineHub


class ReportChannelsTests(DirectTests):
    def setUp(self):
        self.c=config();self.c['peer']['head_direct'].update(queued_reports=True,result_channels=2)
        self.h=DirectHub(self.c,'queued');self.addCleanup(self.h.close)

    def test_completion_overflow_fails_explicitly_instead_of_losing_notification(self):
        import queue
        s=self.session('overflow');item=self.h.pending['overflow']
        with patch.object(self.h.completions,'put_nowait',side_effect=queue.Full):
            with s.delivery_lock:self.h._finish(s,item)
        with self.assertRaisesRegex(RuntimeError,'capacity exceeded'):item['future'].result(1)

    def test_completion_does_not_hold_report_ack_or_confirmation_loop(self):
        gate=threading.Event();entered=threading.Event()
        original=PipelineHub._finish
        def blocked(h,s,item):
            entered.set();gate.wait(3);return original(h,s,item)
        a=self.session('blocked');b=self.session('independent')
        try:
            with patch.object(PipelineHub,'_finish',blocked):
                self.assertTrue(self.callback(a)[0]['ok'])
                self.assertTrue(entered.wait(1))
                # Completion worker has a's lock; another channel still ACKs b.
                self.assertTrue(self.callback(b)[0]['ok'])
                self.assertFalse(gate.is_set())
        finally:gate.set()

    def test_token_and_conflicting_terminal_duplicate(self):
        s=self.session('identity')
        message=dict(query_request_id='identity',token='wrong',node_id=0,involved_nodes=[0],reports=[self.report()])
        with self.assertRaises(RuntimeError):exchange(s.endpoint,message,8388608,2)
        self.assertFalse(s.registry.reports)
        self.assertTrue(self.callback(s)[0]['ok']);self.assertTrue(self.callback(s)[0]['ok'])
        s.close(True)
        self.assertTrue(self.callback(s)[0]['cancelled'])
        message['token']='secret';message['reports'][0]['work_spent']=9
        with self.assertRaises(RuntimeError):exchange(s.endpoint,message,8388608,2)

    def test_bounded_inbox_backpressure_preserves_reports(self):
        self.h.close();self.c['peer']['head_direct'].update(result_channels=1,report_queue_capacity=1)
        gate=threading.Event();original=DirectHub._consume_reports
        async def blocked(h,slot):
            while not gate.is_set():await asyncio.sleep(.001)
            return await original(h,slot)
        with patch.object(DirectHub,'_consume_reports',blocked):
            h=DirectHub(self.c,'bounded');self.h=h;self.addCleanup(h.close)
            sessions=[self.session('bounded'+str(i)) for i in range(4)]
            with ThreadPoolExecutor(4) as pool:
                futures=[pool.submit(self.callback,s) for s in sessions]
                try:
                    deadline=time.monotonic()+1
                    while h.report_status()[0]['backpressure']<2 and time.monotonic()<deadline:time.sleep(.001)
                    status=h.report_status()[0]
                    self.assertEqual(status['queue_peak'],1);self.assertGreaterEqual(status['backpressure'],2)
                    self.assertFalse(any(f.done() for f in futures))
                finally:gate.set()
                self.assertTrue(all(f.result(2)[0]['ok'] for f in futures))
            self.assertTrue(all(len(s.registry.reports)==1 for s in sessions))


class ProcessChannelsTests(DirectTests):
    def setUp(self):
        self.c=config();self.c['peer']['head_direct'].update(queued_reports=True,result_channels=2,report_backend='process')
        self.h=DirectHub(self.c,'process');self.addCleanup(self.h.close)

    def test_receiver_death_fails_query_and_releases_completion(self):
        s=self.session('dead');qid=s.registry.query_id
        with self.h.report_hub.lock:w=self.h.report_hub.sessions[qid][1]
        future=self.h.pending[qid]['future'];w['process'].terminate();w['process'].join(2)
        with self.assertRaisesRegex(RuntimeError,'unavailable'):future.result(3)

    def test_channels_are_distinct_processes_and_terminal_pipe_is_separate(self):
        import os
        rows=self.h.status();self.assertEqual(len(rows),2)
        self.assertEqual(len({r['pid'] for r in rows}),2)
        self.assertNotIn(os.getpid(),[r['pid'] for r in rows])
        self.assertTrue(all(w['terminal_conn'] is not None for w in self.h.report_hub.workers))


class SharedChannelsTests(ProcessChannelsTests):
    def setUp(self):
        self.c=config();self.c['peer']['head_direct'].update(queued_reports=True,result_channels=2,
            report_backend='process',report_registration='shared')
        self.h=DirectHub(self.c,'shared');self.addCleanup(self.h.close)

    def test_registration_has_no_command_round_trip(self):
        with patch.object(self.h.report_hub,'_enqueue',side_effect=AssertionError('Unexpected registration IPC')):
            s=self.session('shared-fast');self.assertTrue(self.callback(s)[0]['ok'])
        self.assertGreater(s.receiver_metrics['shared_identity_published_ns'],0)

    def test_cancel_before_first_report_revokes_shared_identity(self):
        s=self.session('cancel-before-report');s.close(False)
        with self.assertRaises(RuntimeError):self.callback(s)


class SharedRegistryTests(__import__('unittest').TestCase):
    def test_full_delete_collision_reuse(self):
        import multiprocessing as mp
        from dhdgs.report_registry import ReportRegistry
        r=ReportRegistry(mp.get_context('spawn'),2)
        with patch('dhdgs.report_registry.zlib.crc32',return_value=0):
            r.put(dict(qid='a',token='1'));r.put(dict(qid='b',token='2'))
            with self.assertRaises(RuntimeError):r.put(dict(qid='c',token='3'))
            r.remove('a');self.assertEqual(r.get('b')['token'],'2');self.assertIsNone(r.get('a'))
            r.put(dict(qid='c',token='3'));self.assertEqual(r.get('c')['token'],'3')
