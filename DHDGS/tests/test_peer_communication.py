import copy
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch
import yaml
from threadpoolctl import threadpool_limits
from test_system import fixture
from dhdgs.config import load_config
from dhdgs.index import build,Shard
from dhdgs.peer import HeadSession,PeerRuntime,execution_signature
from dhdgs.peer_rpc import ConnectionPool,rpc_exchange
from dhdgs.transport import create_server


def eventually(predicate,seconds=3):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        if predicate():return
        time.sleep(.005)
    raise AssertionError('Condition did not become true')


class CommunicationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.c,self.x=fixture(self.root)
        self.c['search'].update(execution='peer',max_inflight=3)
        self.c['scheduler']['mode']='adaptive'
        self.c['peer'].update(worker_threads=1,heartbeat_interval_ms=10,lease_timeout_ms=3000)
        self.c['peer']['communication'].update(mode='optimized',reuse_connections=False,control_workers=2,max_pending_control=2)
        with threadpool_limits(limits=1):build(self.c)
        self.shard=Shard(Path(self.c['paths']['output'])/'index/shard_0',0)
    def tearDown(self):self.shard.close();self.temp.cleanup()
    def state(self,qid):
        gid=int(self.shard.ids[0])
        return {'query_request_id':qid,'branch_id':qid+'-root','parent_branch_id':None,'token':'secret',
            'visited_nodes':[0],'entry_node':0,'target_node':0,'epoch':self.shard.meta['epoch'],
            'signature':execution_signature(self.c),'query_f32':self.x[0].tobytes(),'work_grant':12,'branch_slots':1,
            'seeds':[[gid,0.,0.,[0],None,0,0,gid,0.]],'seen':[gid],'scale':1.,'decision_seed':42,
            'query_id':None,'head_endpoint':['127.0.0.1',1]}

    def test_transfer_wait_releases_only_compute_worker(self):
        entered=threading.Event();release=threading.Event();second=threading.Event()
        def transfer(*args):entered.set();release.wait(3)
        def search(state,shard,directory,c,spawn,cancelled):
            if state['query_request_id']=='first':spawn({'target_node':1},{'branch_id':'child','node_id':1})
            else:second.set()
            return {'stats':{}}
        with patch.object(PeerRuntime,'_control'),patch.object(PeerRuntime,'_transfer',side_effect=transfer),patch('dhdgs.peer.run_branch',side_effect=search):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state('first'));self.assertTrue(entered.wait(2))
                eventually(lambda:runtime.records[('first','first-root')]['status']=='waiting_transfer')
                self.assertIsNone(runtime.records[('first','first-root')]['report'])
                runtime.accept(self.state('second'));self.assertTrue(second.wait(2))
                release.set();eventually(lambda:runtime.records[('first','first-root')]['status']=='complete')
                self.assertNotIn('pending_report',runtime.records[('first','first-root')])
            finally:release.set();runtime.close()

    def test_prestarted_and_fixed_compute_run_native_with_complete_events(self):
        self.c['peer']['execution_model']='native'
        self.c['metrics']['detailed_trace']=True
        self.c['storage']['io']['backend']='sync_pread'
        self.c['scheduler'].update(mode='reference',max_wait_ms=1e9)
        expected=None
        for mode in ('prestarted','fixed'):
            with self.subTest(mode=mode),patch.object(PeerRuntime,'_control'):
                self.c['peer']['compute'].update(mode=mode,queue_capacity=2)
                runtime=PeerRuntime(self.c,self.shard)
                try:
                    state=self.state(mode);state['branch_slots']=0
                    runtime.accept(state)
                    key=(mode,mode+'-root')
                    eventually(lambda:runtime.records[key]['status'] in ('complete','failed'))
                    rec=runtime.records[key]
                    self.assertEqual(rec['status'],'complete',rec['error'])
                    if expected is None:expected=rec['report']['ids']
                    self.assertEqual(rec['report']['ids'],expected)
                    eventually(lambda:'submit_return_ns' in rec['execution_timestamps'])
                    trace=rec['execution_timestamps']
                    self.assertLessEqual(trace['worker_enter_ns'],trace['native_search_start_ns'])
                    if mode=='fixed':
                        self.assertLessEqual(trace['executor_enqueue_ns'],trace['executor_dequeue_ns'])
                        self.assertLessEqual(trace['executor_dequeue_ns'],trace['worker_enter_ns'])
                        self.assertEqual(runtime.workers.metrics()['capacity_rejections'],0)
                finally:runtime.close()

    def test_compute_config_migration_and_fixed_requires_native(self):
        from dhdgs.config import validate
        old=copy.deepcopy(self.c)
        old={k:v for k,v in old.items() if not k.startswith('_')}
        del old['peer']['compute']
        path=self.root/'old_compute.yaml';path.write_text(yaml.safe_dump(old))
        loaded=load_config(path)
        self.assertEqual(loaded['peer']['compute']['mode'],'executor')
        loaded['peer']['compute']['mode']='fixed'
        with self.assertRaisesRegex(ValueError,'requires native'):
            validate({k:v for k,v in loaded.items() if not k.startswith('_')})

    def test_transfer_failure_is_not_successful_publication(self):
        def search(state,shard,directory,c,spawn,cancelled):
            spawn({'target_node':1},{'branch_id':'child','node_id':1});return {'stats':{}}
        with patch.object(PeerRuntime,'_control'),patch.object(PeerRuntime,'_transfer',side_effect=TimeoutError('lost acceptance')),patch('dhdgs.peer.run_branch',side_effect=search):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state('q'));eventually(lambda:runtime.records[('q','q-root')]['status']=='failed')
                rec=runtime.records[('q','q-root')];self.assertIn('lost acceptance',rec['error']);self.assertIsNone(rec['report'])
                runtime._finalize_ready(('q','q-root'));self.assertEqual(rec['status'],'failed')
            finally:runtime.close()

    def test_compute_queues_all_outgoing_tcp_without_executor_submit(self):
        entered=threading.Event();release=threading.Event();calls=[]
        self.c['peer']['sender']['mode']='queued'
        self.c['metrics']['detailed_trace']=True
        def exchange(endpoint,message,*args):
            calls.append((message.get('op','control'),threading.current_thread().name))
            return {'lease_ms':3000,'cancelled':False},{'connections_created':1,'request_bytes':1,'response_bytes':1}
        def search(state,shard,directory,c,spawn,cancelled):
            entered.set();release.wait(3)
            spawn({'target_node':1},{'branch_id':'child','node_id':1})
            return {'stats':{}}
        with patch('dhdgs.peer.exchange',side_effect=exchange),patch('dhdgs.peer.run_branch',side_effect=search):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state('q'));self.assertTrue(entered.wait(1))
                with patch('concurrent.futures.ThreadPoolExecutor.submit',side_effect=AssertionError('submit on compute path')):
                    release.set()
                    eventually(lambda:runtime.records[('q','q-root')]['acked'])
                self.assertEqual(runtime.records[('q','q-root')]['status'],'complete')
                self.assertTrue(any(op=='peer_submit' for op,_ in calls))
                self.assertTrue(any(op=='control' for op,_ in calls))
                for op,name in calls:
                    self.assertTrue(name.startswith('dhdgs-transfer' if op=='peer_submit' else 'dhdgs-control'),name)
            finally:release.set();runtime.close()

    def test_sender_rejection_is_explicit_failure_not_lost_child(self):
        self.c['peer']['sender']['mode']='queued'
        def search(state,shard,directory,c,spawn,cancelled):
            spawn({'target_node':1},{'branch_id':'child','node_id':1});return {'stats':{}}
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',side_effect=search):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                with patch.object(runtime.transfers,'submit',side_effect=RuntimeError('sender capacity exceeded')):
                    runtime.accept(self.state('q'))
                    eventually(lambda:runtime.records[('q','q-root')]['status']=='failed')
                self.assertIn('sender capacity',runtime.records[('q','q-root')]['error'])
                self.assertIsNone(runtime.records[('q','q-root')]['report'])
            finally:runtime.close()

    def test_archived_config_migrates_to_executor_sender(self):
        c={k:v for k,v in copy.deepcopy(self.c).items() if not k.startswith('_')}
        del c['peer']['sender'];path=self.root/'old_sender.yaml';path.write_text(yaml.safe_dump(c))
        loaded=load_config(path)
        self.assertEqual(loaded['peer']['sender']['mode'],'executor')
        self.assertTrue(any('sender' in x for x in loaded['_config_migration']))

    def test_resumable_sender_rejection_releases_pending_transfer(self):
        self.c['peer']['execution_model']='resumable'
        self.c['peer']['sender']['mode']='queued'
        def steps(state,shard,directory,c,spawn,cancelled):
            spawn({'target_node':1,'dispatch_decision_id':'approved'},
                  {'branch_id':'child','node_id':1,'decision_id':'approved'})
            yield None
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.resumable.branch_steps',side_effect=steps):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                with patch.object(runtime.transfers,'submit',side_effect=RuntimeError('sender capacity exceeded')):
                    runtime.accept(self.state('q'))
                    eventually(lambda:runtime.records[('q','q-root')]['status']=='failed')
                task=runtime.records[('q','q-root')]['task']
                self.assertTrue(task.done.is_set())
                self.assertEqual(task.pending_transfers,0)
            finally:runtime.close()

    def test_slow_control_does_not_block_other_query(self):
        entered=threading.Event();release=threading.Event();fast=threading.Event();compute_release=threading.Event()
        def exchange(endpoint,message,*args):
            if message['query_request_id']=='slow':entered.set();release.wait(3)
            else:fast.set()
            return {'lease_ms':3000,'cancelled':False},{'connections_created':1}
        def search(*args):compute_release.wait(3);return {'stats':{}}
        with patch('dhdgs.peer.exchange',side_effect=exchange),patch('dhdgs.peer.run_branch',side_effect=search):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state('slow'));self.assertTrue(entered.wait(2))
                runtime.accept(self.state('fast'));self.assertTrue(fast.wait(2))
                self.assertLessEqual(runtime.control_active,2)
            finally:release.set();compute_release.set();runtime.close()

    def test_shared_head_isolation_terminal_retry_and_connection_reuse(self):
        self.c['peer']['communication']['reuse_connections']=True
        a=HeadSession(self.c,'a','ta','ra',0);b=HeadSession(self.c,'b','tb','rb',0)
        pool=ConnectionPool(1,1)
        def msg(qid,token,bid):
            return {'query_request_id':qid,'token':token,'node_id':0,'branch_id':bid,'involved_nodes':[0],
                    'reports':[{'node_id':0,'branch_id':bid,'parent_branch_id':None,'work_grant':96,
                        'branch_slots':15,'visited_nodes':[0],'work_spent':1,'work_unused':95,'children':[]}], 'error':None}
        try:
            self.assertIs(a.hub,b.hub);self.assertEqual(a.endpoint,b.endpoint)
            first=msg('a','ta','ra');second=msg('b','tb','rb')
            _,one=rpc_exchange(a.endpoint,first,1000000,1,pool)
            self.assertTrue(a.registry.complete());self.assertFalse(b.registry.complete())
            a.close(success=True)
            reply,two=rpc_exchange(b.endpoint,first,1000000,1,pool)
            self.assertTrue(reply['cancelled']);self.assertEqual(one['connections_created'],1);self.assertEqual(two['connections_reused'],1)
            wrong={**second,'token':'wrong'}
            with self.assertRaisesRegex(RuntimeError,'token'):rpc_exchange(b.endpoint,wrong,1000000,1,pool)
            self.assertFalse(b.registry.complete())
            conflict=copy.deepcopy(first);conflict['reports'][0]['work_spent']=2
            with self.assertRaisesRegex(RuntimeError,'Conflicting'):rpc_exchange(b.endpoint,conflict,1000000,1,pool)
            rpc_exchange(b.endpoint,second,1000000,1,pool)
            b.wait(time.monotonic()+1);b.close(success=True)
            self.assertEqual(a.control_by_kind['cancel']['rpc_count'],0)
            self.assertEqual(b.control_by_kind['lease']['rpc_count'],0)
        finally:
            pool.close()
            if not a.closed:a.close()
            if not b.closed:b.close()

    def test_archived_config_migrates_to_legacy(self):
        c={k:v for k,v in copy.deepcopy(self.c).items() if not k.startswith('_')}
        del c['peer']['communication'];path=self.root/'old.yaml';path.write_text(yaml.safe_dump(c))
        loaded=load_config(path)
        self.assertEqual(loaded['peer']['communication']['mode'],'legacy')
        self.assertTrue(any('communication' in x for x in loaded['_config_migration']))

    def test_cancel_while_waiting_transfer_prevents_success(self):
        release=threading.Event()
        def search(state,shard,directory,c,spawn,cancelled):
            spawn({'target_node':1},{'branch_id':'child','node_id':1});return {'stats':{}}
        with patch.object(PeerRuntime,'_control'),patch.object(PeerRuntime,'_transfer',side_effect=lambda *a:release.wait(3)),patch('dhdgs.peer.run_branch',side_effect=search):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state('q'))
                eventually(lambda:runtime.records[('q','q-root')]['status']=='waiting_transfer')
                runtime.lease('q','secret',cancel=True);release.set()
                eventually(lambda:runtime.records[('q','q-root')]['status']=='failed')
                self.assertIsNone(runtime.records[('q','q-root')]['report'])
            finally:release.set();runtime.close()

    def test_pool_capacity_failure_and_close_release_waiters(self):
        pool=ConnectionPool(1,1)
        _,entry=pool.borrow(('localhost',1),time.monotonic()+1)
        try:
            with self.assertRaisesRegex(TimeoutError,'connection pool'):
                pool.borrow(('localhost',1),time.monotonic()+.01)
            with self.assertRaisesRegex(TimeoutError,'endpoint capacity'):
                pool.borrow(('localhost',2),time.monotonic()+.01)
            # Failed connect owns a reservation too: release must make it reusable.
            pool.release(entry,None,False)
            _,other=pool.borrow(('localhost',2),time.monotonic()+1)
            pool.close();pool.release(other,None,False)
            with self.assertRaisesRegex(RuntimeError,'closed'):
                pool.borrow(('localhost',2),time.monotonic()+1)
        finally:pool.close()

    def test_pool_discards_idle_connection_closed_by_peer(self):
        pool=ConnectionPool(1,1)
        local,remote=socket.socketpair()
        try:
            _,entry=pool.borrow(('localhost',1),time.monotonic()+1)
            pool.release(entry,local,True)
            remote.close()
            candidate,reservation=pool.borrow(('localhost',1),time.monotonic()+1)
            self.assertIsNone(candidate)
            self.assertEqual(local.fileno(),-1)
            pool.release(reservation,None,False)
            self.assertEqual(reservation['borrowed'],0)
        finally:remote.close();local.close();pool.close()

    def test_buffered_accept_does_not_wait_for_executor_submission(self):
        entered=threading.Event();release=threading.Event();accepted=threading.Event()
        self.c['peer']['admission'].update(mode='buffered',queue_capacity=2)
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
            runtime=PeerRuntime(self.c,self.shard);submit=runtime.workers.submit
            def slow_submit(*args,**kwargs):
                entered.set();release.wait(3);return submit(*args,**kwargs)
            try:
                with patch.object(runtime.workers,'submit',side_effect=slow_submit):
                    caller=threading.Thread(target=lambda:(runtime.accept(self.state('q')),accepted.set()))
                    caller.start()
                    self.assertTrue(entered.wait(1));self.assertTrue(accepted.wait(.5))
                    self.assertEqual(runtime.accept(self.state('q')),{'accepted':True,'duplicate':True})
                    conflict=self.state('q');conflict['work_grant']=11
                    with self.assertRaisesRegex(ValueError,'Conflicting'):runtime.accept(conflict)
                    self.assertEqual(runtime.admission_pending,1)
                    release.set();caller.join()
                    eventually(lambda:runtime.records[('q','q-root')]['status']=='complete')
                    self.assertEqual(runtime.admission_pending,0)
            finally:release.set();runtime.close()

    def test_execution_timestamps_allow_worker_before_submit_returns(self):
        release=threading.Event()
        self.c['metrics']['detailed_trace']=True
        self.c['peer']['admission'].update(mode='buffered',queue_capacity=2)
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
            runtime=PeerRuntime(self.c,self.shard);submit=runtime.workers.submit
            def delayed_return(*args,**kwargs):
                future=submit(*args,**kwargs)
                release.wait(3)
                return future
            try:
                with patch.object(runtime.workers,'submit',side_effect=delayed_return):
                    runtime.accept(self.state('q'))
                    eventually(lambda:runtime.records[('q','q-root')]['status']=='complete')
                    before=runtime.metrics()['execution_timestamps'][0]
                    self.assertNotIn('submit_return_ns',before)
                    frozen=copy.deepcopy(runtime.records[('q','q-root')]['report'])
                    release.set()
                    eventually(lambda:'submit_return_ns' in runtime.metrics()['execution_timestamps'][0])
                    t=runtime.metrics()['execution_timestamps'][0]
                    ordered=[t[k] for k in ('accepted_ns','enqueued_ns','dequeued_ns',
                        'submit_state_lock_enter_ns','submit_state_lock_acquired_ns',
                        'submit_enter_ns','worker_enter_ns','submit_return_ns')]
                    self.assertEqual(ordered,sorted(ordered))
                    self.assertGreaterEqual(t['submit_cpu_return_ns'],t['submit_cpu_enter_ns'])
                    self.assertLessEqual(t['submit_enter_ns'],t['executor_adjust_enter_ns'])
                    self.assertLessEqual(t['executor_adjust_enter_ns'],t['executor_adjust_return_ns'])
                    self.assertLessEqual(t['executor_adjust_return_ns'],t['submit_return_ns'])
                    self.assertGreaterEqual(t['executor_workers_after'],t['executor_workers_before'])
                    self.assertEqual(frozen,runtime.records[('q','q-root')]['report'])
            finally:release.set();runtime.close()

    def test_admission_capacity_includes_executor_queue_and_reject_is_atomic(self):
        running=threading.Event();release=threading.Event()
        self.c['peer']['admission'].update(mode='buffered',queue_capacity=1)
        def search(state,*args):
            if state['query_request_id']=='first':running.set();release.wait(3)
            return {'stats':{}}
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',side_effect=search):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state('first'));self.assertTrue(running.wait(1))
                runtime.accept(self.state('second'))
                eventually(lambda:not runtime.admission_queue)
                self.assertEqual(runtime.admission_pending,1)
                with self.assertRaisesRegex(RuntimeError,'admission queue'):runtime.accept(self.state('third'))
                self.assertNotIn(('third','third-root'),runtime.records)
                self.assertNotIn('third',runtime.query_visits)
                release.set();eventually(lambda:runtime.records[('second','second-root')]['status']=='complete')
                runtime.accept(self.state('third'))
                eventually(lambda:runtime.records[('third','third-root')]['status']=='complete')
                self.assertEqual(runtime.admission_pending,0)
                self.assertEqual(runtime.admission_rejected,1)
            finally:release.set();runtime.close()

    def test_queued_cancel_and_expiry_do_not_execute_search(self):
        entered=threading.Event();release=threading.Event();executed=[]
        self.c['peer']['admission'].update(mode='buffered',queue_capacity=3)
        def search(state,*args):executed.append(state['query_request_id']);return {'stats':{}}
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',side_effect=search):
            runtime=PeerRuntime(self.c,self.shard);submit=runtime.workers.submit
            def slow_submit(*args,**kwargs):entered.set();release.wait(3);return submit(*args,**kwargs)
            try:
                with patch.object(runtime.workers,'submit',side_effect=slow_submit):
                    runtime.accept(self.state('first'));self.assertTrue(entered.wait(1))
                    runtime.accept(self.state('cancel'));runtime.accept(self.state('expired'))
                    runtime.lease('cancel','secret',cancel=True)
                    with runtime.lock:runtime.records[('expired','expired-root')]['deadline']=time.monotonic()-1
                    release.set()
                    eventually(lambda:all(runtime.records[(q,q+'-root')]['status']=='failed' for q in ('cancel','expired')))
                    self.assertNotIn('cancel',executed);self.assertNotIn('expired',executed)
                    eventually(lambda:runtime.admission_pending==0)
            finally:release.set();runtime.close()

    def test_background_submit_failure_is_explicit_and_dispatcher_survives(self):
        self.c['peer']['admission'].update(mode='buffered',queue_capacity=2)
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                with patch.object(runtime.workers,'submit',side_effect=RuntimeError('executor unavailable')):
                    self.assertTrue(runtime.accept(self.state('bad'))['accepted'])
                    eventually(lambda:runtime.records[('bad','bad-root')]['status']=='failed')
                    self.assertIn('executor unavailable',runtime.records[('bad','bad-root')]['error'])
                    self.assertIsNone(runtime.records[('bad','bad-root')]['report'])
                    self.assertEqual(runtime.admission_pending,0)
                runtime.accept(self.state('good'))
                eventually(lambda:runtime.records[('good','good-root')]['status']=='complete')
            finally:runtime.close()

    def test_archived_config_migrates_to_direct_admission(self):
        c={k:v for k,v in copy.deepcopy(self.c).items() if not k.startswith('_')}
        del c['peer']['admission'];path=self.root/'old_admission.yaml';path.write_text(yaml.safe_dump(c))
        loaded=load_config(path)
        self.assertEqual(loaded['peer']['admission']['mode'],'direct')
        self.assertTrue(any('admission' in x for x in loaded['_config_migration']))

    def test_tcp_acceptance_reply_arrives_while_executor_submission_is_blocked(self):
        entered=threading.Event();release=threading.Event()
        self.c['peer']['admission']['mode']='buffered'
        self.c['cluster']['nodes'][0]['port']=0
        original=PeerRuntime._submit_branch
        def blocked(runtime,key):entered.set();release.wait(3);return original(runtime,key)
        with patch.object(PeerRuntime,'_control'),patch.object(PeerRuntime,'_submit_branch',blocked),patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
            server=create_server(self.c,0)
            listener=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01});listener.start()
            try:
                response,_=rpc_exchange(server.server_address,{'version':1,'op':'peer_submit','state':self.state('tcp')},1000000,1)
                self.assertTrue(entered.wait(1));self.assertFalse(release.is_set())
                self.assertEqual(response['result'],{'accepted':True,'duplicate':False})
            finally:release.set();server.shutdown();server.server_close();listener.join()

    def test_shutdown_drains_admission_queue_without_silent_loss(self):
        entered=threading.Event();release=threading.Event()
        self.c['peer']['admission'].update(mode='buffered',queue_capacity=2)
        original=PeerRuntime._submit_branch
        def blocked(runtime,key):entered.set();release.wait(3);return original(runtime,key)
        with patch.object(PeerRuntime,'_control'),patch.object(PeerRuntime,'_submit_branch',blocked),patch('dhdgs.peer.run_branch') as search:
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state('one'));self.assertTrue(entered.wait(1))
                runtime.accept(self.state('two'))
                closer=threading.Thread(target=runtime.close);closer.start()
                self.assertTrue(runtime.stop.wait(1));release.set();closer.join(2)
                self.assertFalse(closer.is_alive());self.assertEqual(runtime.admission_pending,0)
                self.assertFalse(runtime.admission_queue);search.assert_not_called()
                self.assertTrue(all(r['status']=='failed' for r in runtime.records.values()))
            finally:release.set();runtime.close()

    def test_resumable_initial_submit_failure_signals_terminal_completion(self):
        self.c['peer']['admission']['mode']='buffered'
        self.c['peer']['execution_model']='resumable'
        with patch.object(PeerRuntime,'_control'):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                with patch.object(runtime.workers,'submit',side_effect=RuntimeError('enqueue failed')):
                    runtime.accept(self.state('q'))
                    eventually(lambda:runtime.records[('q','q-root')]['status']=='failed')
                    task=runtime.records[('q','q-root')]['task']
                    self.assertTrue(task.done.is_set())
                    self.assertEqual(runtime.admission_pending,0)
            finally:
                # Prevent a broken implementation from hanging the regression runner.
                for rec in runtime.records.values():
                    if 'task' in rec:rec['task'].done.set()
                runtime.close()
