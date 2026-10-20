"""Result continuation must never block independent data-report ACKs."""
from concurrent.futures import Future
import threading
import time
import unittest
from unittest.mock import patch

from test_head_direct import config
from dhdgs.config import validate
from dhdgs.head_direct import DirectFlow,DirectHub
from dhdgs.peer import HeadSession,exchange


def settings():
    c=config()
    c['peer']['head_direct'].update(queued_reports=True,report_backend='process',
        report_registration='shared',result_channels=1,completion_dispatch='inline',result_backend='inline')
    return c


class InlineResultTests(unittest.TestCase):
    def test_config_requires_independent_report_ack(self):
        c=settings();validate(c)
        c['peer']['head_direct']['report_backend']='thread'
        with self.assertRaisesRegex(ValueError,'independent'):validate(c)

    def test_result_error_releases_capacity_and_reaches_caller(self):
        c=settings();c['peer']['head_direct']['result_queue_capacity']=1
        f=DirectFlow(c);self.addCleanup(f.close)
        self.assertEqual(f.status()['result_processes'],[])
        with patch('dhdgs.head_direct.process_result',side_effect=ValueError('bad report')):
            with self.assertRaisesRegex(ValueError,'bad report'):f.merge({}).result()
        with patch('dhdgs.head_direct.process_result',return_value={'ids':[7]}):
            self.assertEqual(f.merge({}).result(),{'ids':[7]})

    def test_inline_result_does_not_occupy_distributor_or_report_ack(self):
        c=settings();h=DirectHub(c,'inline-isolation');self.addCleanup(h.close)
        f=DirectFlow(c);self.addCleanup(f.close)
        entered=threading.Event();release=threading.Event();submitted=threading.Event()
        sessions=[]
        def task():
            s=HeadSession(c,'first','token','root',0,h);sessions.append(s)
            item=h.pending['first']
            item['entry']={'ok':True}
            submitted.set()
            yield item['future']
            s.close(True)
            result=yield f.merge({})
            return result
        def merge(_):
            entered.set()
            if not release.wait(5):raise TimeoutError('test gate')
            return {'ids':[42]}
        def message(q):
            return dict(query_request_id=q,token='token',node_id=0,involved_nodes=[0],reports=[dict(
                branch_id='root',parent_branch_id=None,node_id=0,work_grant=10,branch_slots=2,
                visited_nodes=[0],work_spent=10,work_unused=0,children=[])])
        with patch('dhdgs.head_direct.process_result',merge):
            out=f.submit(task(),{})
            try:
                self.assertTrue(submitted.wait(3))
                self.assertTrue(exchange(sessions[0].endpoint,message('first'),8388608,2)[0]['ok'])
                self.assertTrue(entered.wait(3))
                self.assertFalse(out.done())
                # Same receiver process and report channel: ACK is independent
                # of the blocked parent-side merge/terminal consumer.
                other=HeadSession(c,'second','token','root',0,h)
                self.assertTrue(exchange(other.endpoint,message('second'),8388608,2)[0]['ok'])
                self.assertEqual(f.distributors.submit(lambda:42).result(2),42)
            finally:release.set()
            self.assertEqual(out.result(3),{'ids':[42]})
            self.assertTrue(other.done.wait(3))
            other.close(True)
