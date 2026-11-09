"""Native batched reads: isolated admission, shared bytes and bounded resumes."""
import copy
import time
import unittest
import yaml
from pathlib import Path
import test_async_pull as base
import test_hop_return as protocol


def enable(c, wait=0):
    options=yaml.safe_load((Path(__file__).resolve().parents[1]/'configs/dhdgs.yaml').read_text())['peer']['replica_pull']['async']
    c['peer']['replica_pull']['async']=copy.deepcopy(options)
    c['peer']['replica_pull']['async'].update(enabled=True,tail_threads=2,source_read_threads=2)
    c['peer']['replica_pull']['async']['coalesce'].update(enabled=True,wait_us=wait)


class CoalescedRuntimeTests(base.AsyncPullTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c);enable(c,1000)

    def test_concurrent_coalescing_and_bounded_release(self):
        source,target=self.install_entry()
        futures=[self.engine.search_async(self.x[0],0)for _ in range(32)]
        for f in futures:self.check_result(f.result(30),200)
        end=time.monotonic()+3
        while time.monotonic()<end:
            m=self.servers[target].control.peer.runtime.metrics()['pull_scheduler']
            if not m['contexts'] and not m['batch_reserved_bytes']:break
            time.sleep(.01)
        self.assertGreater(m['batch_multi'],0)
        self.assertGreater(m['batch_queries'],m['batch_rpcs'])
        self.assertEqual(m['batch_reserved_bytes'],0)
        s=self.servers[source].control.peer.runtime.metrics()['pull_scheduler']
        self.assertGreater(s['batch_logical_records'],s['batch_unique_records'])
        self.assertGreater(s['batch_logical_bytes'],s['batch_unique_bytes'])
        self.assertGreater(s['batch_source_cpu_ns'],0)

    def test_close_with_shared_replies_and_pending_aggregation(self):
        base.AsyncShutdownTests.test_close_cancels_tail_work_and_releases_contexts(self)
        for server in self.servers:
            self.assertEqual(server.control.peer.runtime.metrics()['pull_scheduler']['batch_reserved_bytes'],0)

    def test_invalid_configuration_bounds(self):
        from dhdgs.config import validate
        for fields in [dict(max_queries=0),dict(max_queries=33),dict(max_bytes=1),dict(wait_us=-1)]:
            c=copy.deepcopy({k:v for k,v in self.c.items()if not k.startswith('_')})
            c['peer']['replica_pull']['async']['coalesce'].update(fields)
            with self.assertRaisesRegex(ValueError,'coalesced pull'):validate(c)


class CoalescedProtocolTests(protocol.PullProtocolTests):
    def config(self,*args):
        # Existing tiny fixture releases port probes individually. Retry only
        # its explicit duplicate-endpoint validation, never a runtime failure.
        for attempt in range(5):
            try:return protocol.PullProtocolTests.config(self,*args)
            except ValueError as exc:
                if str(exc)!='Duplicate node endpoint' or attempt==4:raise

    def setup_batch(self):
        from test_region_tasks import mode
        c=mode(self.config(True,True));protocol.PullRuntimeTests.configure_replica_test(self,c)
        enable(c);self.start(c)
        state=self.state(c)
        fields={k:state[k]for k in ['query_request_id','token','epoch','signature']}
        fields.update(owner='replica:alpha',directory=dict(opened=[],reserved=[]),ids=[state['seeds'][0][0]])
        return c,fields

    def batch(self,c,fields,items):
        return self.rpc(c,0,'peer_replica_read_batch',epoch=fields['epoch'],signature=fields['signature'],items=items)

    def test_union_read_and_independent_invalid_token(self):
        c,f=self.setup_batch()
        other=dict(f,query_request_id='other')
        value=self.batch(c,f,[f,other,dict(f,token='wrong')])
        rows=value['items']
        self.assertTrue(rows[0]['ok']);self.assertTrue(rows[1]['ok']);self.assertFalse(rows[2]['ok'])
        self.assertEqual(value['unique_records'],1);self.assertEqual(value['logical_records'],2)
        self.assertEqual(rows[0]['result']['records_rows'],rows[1]['result']['records_rows'])
        normal=self.rpc(c,0,'peer_replica_read',**f)
        self.assertEqual(value['records'],normal['records']);self.assertEqual(value['codes'],normal['codes'])

    def test_bad_record_does_not_fail_other_query(self):
        c,f=self.setup_batch()
        value=self.batch(c,f,[f,dict(f,query_request_id='bad_record',ids=[2**60])])
        self.assertTrue(value['union_failed'])
        self.assertTrue(value['items'][0]['ok']);self.assertFalse(value['items'][1]['ok'])
        self.assertTrue(value['items'][0]['result']['accepted'])

    def test_source_ownership_is_independent(self):
        c,f=self.setup_batch()
        self.rpc(c,0,'peer_replica_read',**f)
        value=self.batch(c,f,[dict(f,owner='replica:wrong'),dict(f,query_request_id='new_query')])
        self.assertFalse(value['items'][0]['result']['accepted'])
        self.assertTrue(value['items'][1]['result']['accepted'])

    def test_group_limit_and_epoch_rejection(self):
        c,f=self.setup_batch()
        with self.assertRaises(AssertionError):self.batch(c,f,[f]*5)
        with self.assertRaises(AssertionError):self.batch(c,dict(f,epoch='wrong'),[f])
