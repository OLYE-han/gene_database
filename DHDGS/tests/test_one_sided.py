"""Real RNIC reads through the search continuation; TCP is control-only in fixtures."""
import copy
from pathlib import Path
import unittest
import yaml
import test_async_pull as base
import test_hop_return as protocol
from dhdgs.index import native


def enable(c):
    config=yaml.safe_load((Path(__file__).resolve().parents[1]/'configs/dhdgs.yaml').read_text())
    c['peer']['replica_pull']['one_sided']=copy.deepcopy(config['peer']['replica_pull']['one_sided'])
    c['peer']['replica_pull']['one_sided'].update(enabled=True,fallback_to_rpc=False,max_registered_bytes=64<<20,max_directory_bytes=4<<20)


def active_ib():
    return native().one_sided_available() and any('ACTIVE' in p.read_text() and (p.parent/'link_layer').read_text().strip()=='InfiniBand' for p in Path('/sys/class/infiniband').glob('*/ports/*/state'))


@unittest.skipUnless(active_ib(), 'Requires an active InfiniBand verbs device')
class OneSidedTests(base.AsyncPullTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        enable(c)

    def test_pull_computes_on_replica_preserves_credit_and_dependencies(self):
        a,b=self.install_entry()
        result=self.engine.search_async(self.x[0],0).result(20)
        self.check_result(result,200)
        reports=[x for x in result['branch_reports']if x.get('replica_version')=='pull_test']
        self.assertGreater(sum(x['stats'].get('one_sided_read_ops',0)for x in reports),0)
        self.assertGreater(result['stats']['pull_expanded'],0)
        self.assertEqual(result['stats']['pull_source_cpu_ms'],0)
        m=self.servers[b].control.peer.runtime.metrics()['pull_scheduler']['one_sided_client']
        self.assertGreater(m['read_operations'],0);self.assertGreater(m['completed_batches'],0)
        self.assertEqual(m['errors'],0)
        self.assertGreater(self.servers[a].control.peer.runtime.metrics()['pull_scheduler']['one_sided_source']['registered_bytes'],0)

    def test_concurrent_hardware_reads(self):
        a,b=self.install_entry()
        futures=[self.engine.search_async(self.x[i%4],i).result(20)for i in range(4)]
        futures=[self.engine.search_async(self.x[0],i+10)for i in range(24)]
        for f in futures:self.check_result(f.result(30),200)
        m=self.servers[b].control.peer.runtime.metrics()['pull_scheduler']['one_sided_client']
        self.assertGreater(m['completed_batches'],0);self.assertEqual(m['errors'],0)

    def test_shutdown_drains_hardware_and_continuations(self):
        base.AsyncShutdownTests.test_close_cancels_tail_work_and_releases_contexts(self)

    def test_invalid_resource_and_mapping_configuration(self):
        from dhdgs.config import validate
        for fields in [dict(port=0),dict(lanes_per_source=17),dict(max_connections=65),
                       dict(record_mapping='unknown'),dict(max_directory_bytes=1<<40)]:
            c=copy.deepcopy({k:v for k,v in self.c.items()if not k.startswith('_')})
            c['peer']['replica_pull']['one_sided'].update(fields)
            with self.assertRaisesRegex(ValueError,'one-sided'):validate(c)

    def test_hardware_bytes_equal_rpc_reader_and_invalid_id_fails(self):
        a,_=self.install_entry()
        shard=self.servers[a].control.worker
        cfg=self.c['peer']['replica_pull']['one_sided']
        check=native().one_sided_probe(shard.native_search,cfg,[int(x)for x in shard.ids[:4]],524288)
        self.assertTrue(check['records_equal']);self.assertTrue(check['codes_equal'])
        self.assertGreater(check['client']['read_operations'],0)
        with self.assertRaisesRegex(RuntimeError,'outside directory'):
            native().one_sided_probe(shard.native_search,cfg,[2**60],524288)


class DoorbellTests(OneSidedTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['one_sided']['doorbell_batch']=True

    def test_chain_reduces_doorbells_and_completions(self):
        _,b=self.install_entry()
        self.check_result(self.engine.search_async(self.x[0],0).result(20),200)
        m=self.servers[b].control.peer.runtime.metrics()['pull_scheduler']['one_sided_client']
        self.assertLess(m['post_calls'],m['read_operations'])
        self.assertEqual(m['completion_events'],m['post_calls'])
        self.assertEqual(m['errors'],0)


class MissingCodeTests(OneSidedTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['one_sided']['missing_codes_only']=True

    def test_sparse_and_all_present_dependencies_keep_exact_bytes(self):
        a,_=self.install_entry();shard=self.servers[a].control.worker
        ids=[int(x)for x in shard.ids[:4]];cfg=self.c['peer']['replica_pull']['one_sided']
        for bank in [shard.code_ids[::2],shard.code_ids]:
            known={int(x):1+i%3 for i,x in enumerate(bank)}
            result=native().one_sided_probe(shard.native_search,cfg,ids,524288,known)
            self.assertTrue(result['records_equal']);self.assertTrue(result['codes_equal'])
            m=result['client'];self.assertEqual(m['code_unique'],m['code_missing']+m['code_local']+m['code_overlay']+m['code_previous'])
            self.assertEqual(m['code_fetched'],m['code_missing']);self.assertLess(result['returned_code_rows'],result['reference_code_rows'])
        self.assertEqual(result['returned_code_rows'],0)

    def test_search_uses_existing_codes_without_remote_refetch(self):
        _,b=self.install_entry();r=self.engine.search_async(self.x[0],0).result(20)
        self.check_result(r,200)
        m=self.servers[b].control.peer.runtime.metrics()['pull_scheduler']['one_sided_client']
        self.assertGreater(m['code_unique'],m['code_fetched'])
        self.assertEqual(m['code_fetched'],m['code_missing'])
        self.assertEqual(m['errors'],0)


@unittest.skipUnless(active_ib(), 'Requires an active InfiniBand verbs device')
class RegistrationFallbackTests(base.AsyncPullTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c);enable(c)
        c['peer']['replica_pull']['one_sided'].update(max_registered_bytes=16,max_directory_bytes=16,fallback_to_rpc=True)

    def test_registration_failure_is_explicit_and_uses_rpc(self):
        a,b=self.install_entry();r=self.engine.search_async(self.x[0],0).result(20)
        self.check_result(r,200);self.assertGreater(r['stats']['pull_source_cpu_ms'],0)
        s=self.servers[a].control.peer.runtime.metrics()['pull_scheduler']
        self.assertTrue(s['one_sided_source_error']);self.assertIsNone(s['one_sided_source'])
        self.assertEqual(self.servers[b].control.peer.runtime.metrics()['pull_scheduler']['one_sided_client']['completed_batches'],0)


@unittest.skipUnless(active_ib(), 'Requires an active InfiniBand verbs device')
class OneSidedProtocolTests(protocol.PullProtocolTests):
    def test_one_sided_authorization_checks_owner_token_and_epoch(self):
        c=self.config(True,True)
        protocol.PullRuntimeTests.configure_replica_test(self,c)
        c['peer']['replica_pull']['async']=copy.deepcopy(yaml.safe_load((Path(__file__).resolve().parents[1]/'configs/dhdgs.yaml').read_text())['peer']['replica_pull']['async'])
        c['peer']['replica_pull']['async'].update(enabled=True,source_read_threads=2)
        enable(c);self.start(c)
        state=self.state(c)
        f={k:state[k]for k in ['query_request_id','token','epoch','signature']}
        f.update(owner='replica:alpha',directory=dict(opened=[],reserved=[]),ids=[state['seeds'][0][0]],one_sided=True)
        value=self.rpc(c,0,'peer_replica_read',**f)
        self.assertTrue(value['one_sided_ready']);self.assertNotIn('records',value)
        self.assertFalse(self.rpc(c,0,'peer_replica_read',**dict(f,owner='replica:beta'))['accepted'])
        for patch in [dict(token='wrong'),dict(epoch='wrong')]:
            with self.assertRaises(AssertionError):self.rpc(c,0,'peer_replica_read',**dict(f,**patch))


if __name__=='__main__':unittest.main()
