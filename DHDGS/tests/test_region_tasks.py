"""Query-region ownership, fanout, live candidate merging and source guards."""
import copy
import json
import socket
import time
import unittest
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from dhdgs.config import validate
from dhdgs.index import native
from dhdgs.peer import execution_signature
from dhdgs.search import Engine
from dhdgs.transport import send,receive
from test_cpp_runtime import NativeRuntimeTests
from test_replicas import ReplicaTests

def mode(c):
    c['peer'].update(region_tasks=True,concurrent_region_admission=True,region_visit_broadcast=True,allow_node_revisit=True,
        logical_region_visits=True,max_excluded_region_ids=1000,deduplicate_node_expansions=True,
        min_expansions_after_cross=1,cross_on_local_exhaustion=True,max_branches=16,max_active_queries=4096,
        control_timeout_seconds=5.)
    c['search'].update(max_expansions=200)
    c['scheduler'].update(remote_weight=2.)
    return c

class RegionRuntimeTests(unittest.TestCase):
    setUpClass=classmethod(NativeRuntimeTests.setUpClass.__func__)
    tearDownClass=classmethod(NativeRuntimeTests.tearDownClass.__func__)
    config=NativeRuntimeTests.config
    start=NativeRuntimeTests.start

    def check_result(self,q,budget):
        self.assertEqual(q['status'],'ok',q.get('error'))
        rows=q['branch_reports'];root=next(b for b in rows if b['parent_branch_id'] is None)
        self.assertTrue(native().validate_manifests(rows,root['branch_id'],root['node_id'],budget,root['branch_slots'],3,True,True))
        self.assertLessEqual(q['stats']['expanded'],budget)
        actors=defaultdict(list);ids=defaultdict(list)
        for b in rows:
            ids[b['node_id']].extend(b['expanded_ids'])
            self.assertFalse(b.get('excluded_regions'))
            if b.get('region_admission')=='source_reserved_not_started':continue
            actors[(b['node_id'],b['logical_region'])].append(b)
        for expanded in ids.values():self.assertEqual(len(expanded),len(set(expanded)))
        for group in actors.values():
            self.assertEqual(len({b['region_executor_id'] for b in group}),1)
            intervals=sorted((b['native_timestamps']['native_search_start_ns'],b['native_timestamps']['native_loop_done_ns']) for b in group)
            for a,b in zip(intervals,intervals[1:]):self.assertLessEqual(a[1],b[0], 'Concurrent execution of one query/region')

    def test_fanout_merge_budget_and_no_parallel_duplicate_actor(self):
        c=mode(self.config(True,True));validate({k:v for k,v in c.items() if not k.startswith('_')})
        self.start(c);engine=Engine(c);self.addCleanup(engine.close)
        futures=[engine.search_async(x,i) for i,x in enumerate(self.x)]
        rows=[f.result(15) for f in futures]
        for q in rows:self.check_result(q,200)
        self.assertTrue(any(len(b['children'])>1 for q in rows for b in q['branch_reports']))
        self.assertTrue(any(len({child['node_id']for child in b['children']})>1 for q in rows for b in q['branch_reports']))
        self.assertTrue(any(b.get('region_merged_admission') for q in rows for b in q['branch_reports']))
        self.assertTrue(any(q['stats']['region_candidates_merged'] for q in rows))
        self.assertTrue(all(q['stats']['remote_candidates_dropped_after_forward']==0 for q in rows))
        for i,q in enumerate(rows):
            distances=np.sum((self.x.astype(float)-self.x[i])**2,axis=1);distances[i]=np.inf
            self.assertEqual(set(q['ids']),set(map(int,np.argsort(distances)[:c['search']['top_k']])))
        self.assertEqual(engine.runtime.metrics()['active'],0)

    def rpc(self,c,node,op,**fields):
        n=c['cluster']['nodes'][node]
        with socket.create_connection((n['host'],n['port']),3) as sock:
            sock.settimeout(5);send(sock,dict(version=1,op=op,**fields),c['network']['max_message_bytes'])
            reply=receive(sock,c['network']['max_message_bytes'])[0]
        self.assertTrue(reply['ok'],reply);return reply['result']

    def test_saturated_advisory_queue_does_not_reject_search_transfers(self):
        c=mode(self.config(True,True));c['peer']['region_visit_queue_capacity']=1
        self.start(c);e=Engine(c);self.addCleanup(e.close)
        futures=[e.search_async(x,i)for i,x in enumerate(self.x)]
        for f in futures:self.check_result(f.result(15),200)
        snapshots=[self.rpc(c,n,'metrics')['peer_lifecycle']for n in range(3)]
        self.assertGreater(sum(s['region_tasks']['visit_errors']for s in snapshots),0)
        self.assertEqual(sum(s['network_send']['capacity_rejections']for s in snapshots),0)

    def test_admission_timestamps_separate_receive_and_identity_lock(self):
        c=mode(self.config(True,True));self.start(c)
        e=Engine(c);self.addCleanup(e.close)
        for i,x in enumerate(self.x[:8]):self.check_result(e.search_async(x,i).result(15),200)
        traces=[t for n in range(3)for t in self.rpc(c,n,'metrics')['peer_lifecycle']['execution_timestamps']]
        self.assertTrue(traces)
        for t in traces:
            keys=['receiver_message_ready_ns','receiver_worker_enter_ns','receiver_decode_done_ns',
                  'receiver_handler_enter_ns','managed_gate_enter_ns','managed_gate_acquired_ns',
                  'accept_enter_ns','accept_validated_ns','accepted_ns','accept_state_lock_enter_ns',
                  'accept_state_lock_acquired_ns','accept_region_ready_ns','accept_register_done_ns',
                  'accept_inbox_enter_ns','accept_inbox_done_ns','accept_state_lock_released_ns']
            values=[t[k]for k in keys]
            self.assertEqual(values,sorted(values),t)

    def test_context_expiry_preserves_later_query_admission(self):
        self.check_context_expiry(False)

    def test_async_expiry_small_queue_preserves_admission_and_eventual_reclaim(self):
        self.check_context_expiry(True)

    def check_context_expiry(self, asynchronous):
        from dhdgs.cpp_runtime import NativePeer, NativeServer
        from dhdgs.index import Shard, shard_path
        c=mode(self.config(True,True))
        c['peer'].update(query_timeout_seconds=1.,retention_seconds=.05)
        c['native_runtime'].update(lifecycle_async_reclaim=asynchronous,
            lifecycle_cleanup_max_records=1,lifecycle_cleanup_max_contexts=1,
            lifecycle_reclaim_threads=1,lifecycle_reclaim_batches=1,lifecycle_timer_batch=1)
        peers=[]
        for node in c['cluster']['nodes']:
            worker=Shard(shard_path(c,node['id']),0)
            peer=NativePeer(c,worker);peers.append(peer)
            def close(p=peer,w=worker):p.close();w.close()
            server=NativeServer(c,node['id'],peer,lambda request:dict(ok=False),close)
            server.configure(peer,True);self.addCleanup(server.server_close)
        engine=Engine(c);self.addCleanup(engine.close)
        for wave in range(2):
            rows=[engine.search_async(x,i)for i,x in enumerate(self.x[:8])]
            for row in rows:self.check_result(row.result(5),200)
            if wave==0:
                deadline=time.monotonic()+5
                while time.monotonic()<deadline:
                    if sum(p.runtime.network_metrics()['lifecycle_cleanup']['contexts_retired']
                           for p in peers)>0:break
                    time.sleep(.01)
                else:self.fail('Completed query contexts were not reclaimed')
                for peer in peers:
                    m=peer.runtime.network_metrics()['lifecycle_cleanup']
                    self.assertEqual(m['async_reclaim'],asynchronous)
                    self.assertLessEqual(m['pending_reclaim_batches'],1)
                self.assertGreater(sum(p.runtime.network_metrics()['lifecycle_cleanup']['records_retired']
                                       for p in peers),0)

    def state(self,c,qid='ownership',bid='grant',origin='base:1'):
        ids=np.load(self.root/'out/index/shard_0/ids.npy');gid=int(ids[0]);member=np.load(self.root/'out/index/membership.npy')
        meta=json.loads((self.root/'out/index/manifest.json').read_text())
        with socket.socket()as s:s.bind(('127.0.0.1',0));port=s.getsockname()[1]
        return dict(query_request_id=qid,branch_id=bid,parent_branch_id='parent-'+bid,token='test-token',
            head_endpoint=['127.0.0.1',port],entry_node=1,target_node=0,epoch=meta['epoch'],signature=execution_signature(c),
            query_f32=self.x[0].tobytes(),query_id=0,work_grant=8,branch_slots=0,
            seeds=[[gid,0.,0.,list(map(int,np.flatnonzero(member[gid]))),None,0,0,gid,0.]],seen=[gid],
            fallback_entries=[],visited_nodes=[1,0],remote_frontier=[],fallback_remaining=0,scale=1.,
            ancestry=[dict(branch_id='parent-'+bid,node_id=1,logical_region=origin)],decision_seed=42,
            logical_region='base:0',visited_regions=[origin,'base:0'])

    def reserve(self,c,s,owner='replica:alpha'):
        return self.rpc(c,0,'peer_region_reserve',**{k:s[k] for k in ['query_request_id','token','epoch','signature']},
            owner=owner,directory=dict(opened=[],reserved=[]))

    def test_source_reservation_then_authorized_start_and_other_merge(self):
        c=mode(self.config(True,True));c['peer']['region_visit_broadcast']=False;self.start(c)
        blocked=self.state(c);self.reserve(c,blocked)
        a=self.rpc(c,0,'peer_submit',state=blocked);self.assertEqual(a['region_admission'],'reserved')
        authorized=self.state(c,bid='alpha-grant',origin='replica:alpha')
        b=self.rpc(c,0,'peer_submit',state=authorized);self.assertEqual(b['region_admission'],'created')
        # A quiescent task must accept another conserved grant without a new actor.
        time.sleep(.02)
        late=self.state(c,bid='later-grant')
        d=self.rpc(c,0,'peer_submit',state=late);self.assertEqual(d['region_admission'],'merged')
        self.assertEqual(b['region_executor_id'],d['region_executor_id'])
        retry=self.rpc(c,0,'peer_submit',state=late);self.assertTrue(retry['duplicate'])
        bad=copy.deepcopy(late);bad['work_grant']=7
        with self.assertRaises(AssertionError):self.rpc(c,0,'peer_submit',state=bad)
        self.rpc(c,0,'peer_cancel',query_request_id=late['query_request_id'],token=late['token'])

    def test_already_started_source_is_not_revoked_by_reservation(self):
        c=mode(self.config(True,True));c['peer']['region_visit_broadcast']=False;self.start(c)
        first=self.state(c);a=self.rpc(c,0,'peer_submit',state=first)
        self.reserve(c,first)
        later=self.state(c,bid='later');b=self.rpc(c,0,'peer_submit',state=later)
        self.assertEqual(b['region_admission'],'merged');self.assertEqual(a['region_executor_id'],b['region_executor_id'])
        self.rpc(c,0,'peer_cancel',query_request_id=first['query_request_id'],token=first['token'])

    def test_concurrent_first_claims_have_one_owner_without_broadcast(self):
        c=mode(self.config(True,True));c['peer']['region_visit_broadcast']=False;self.start(c)
        states=[self.state(c,bid='grant-'+str(i)) for i in range(16)]
        with ThreadPoolExecutor(16)as pool:answers=list(pool.map(lambda s:self.rpc(c,0,'peer_submit',state=s),states))
        self.assertEqual(sum(a['region_admission']=='created' for a in answers),1)
        self.assertEqual(len({a['region_executor_id']for a in answers}),1)
        self.rpc(c,0,'peer_cancel',query_request_id=states[0]['query_request_id'],token=states[0]['token'])

    def test_concurrent_duplicate_retry_does_not_consume_admission_credit(self):
        c=mode(self.config(True,True));c['peer'].update(region_visit_broadcast=False,max_active_queries=1);self.start(c)
        state=self.state(c)
        with ThreadPoolExecutor(16)as pool:
            answers=list(pool.map(lambda _:self.rpc(c,0,'peer_submit',state=state),range(16)))
        self.assertEqual(sum(not x['duplicate']for x in answers),1)
        with self.assertRaises(AssertionError):self.rpc(c,0,'peer_submit',state=self.state(c,qid='different-query'))
        self.rpc(c,0,'peer_cancel',query_request_id=state['query_request_id'],token=state['token'])

    def test_fanout_manifest_rejects_overallocated_credit(self):
        child=lambda bid,n:dict(branch_id=bid,parent_branch_id='r',node_id=n,work_grant=10,branch_slots=0)
        def report(bid,parent,n,path,grant,slots,spent,children):return dict(branch_id=bid,parent_branch_id=parent,node_id=n,visited_nodes=path,work_grant=grant,branch_slots=slots,work_spent=spent,work_unused=0,children=children)
        rows=[report('r',None,0,[0],30,2,10,[child('b',1),child('c',2)]),report('b','r',1,[0,1],10,0,10,[]),report('c','r',2,[0,2],10,0,10,[])]
        self.assertTrue(native().validate_manifests(rows,'r',0,30,2,3,True,True))
        with self.assertRaises(ValueError):native().validate_manifests(rows,'r',0,30,2,3,True,False)
        bad=copy.deepcopy(rows);bad[0]['children'][1]['work_grant']=11
        with self.assertRaises(ValueError):native().validate_manifests(bad,'r',0,30,2,3,True,True)

    def test_failed_query_acknowledges_late_announcements_and_reports(self):
        self.check_failed_query_reports(False)

    def test_transfer_failure_grace_returns_error_without_incomplete_success(self):
        self.check_failed_query_reports(True)

    def check_failed_query_reports(self, bounded):
        import queue,socketserver,threading
        c=mode(self.config(True,True));accepted=queue.Queue()
        if bounded:c['peer'].update(transfer_failure_grace_ms=150,query_timeout_seconds=3.)
        class Server(socketserver.ThreadingTCPServer):
            daemon_threads=True
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(3)
                try:
                    while True:
                        request,_=receive(self.request,1<<20)
                        if request['op']=='peer_submit':accepted.put(request['state'])
                        send(self.request,dict(ok=True,result=dict(accepted=True,found=True)),1<<20)
                except (OSError,ValueError,EOFError):pass
        for node in c['cluster']['nodes']:
            s=Server(('127.0.0.1',0),Handler);node['port']=s.server_address[1]
            t=threading.Thread(target=s.serve_forever,kwargs={'poll_interval':.01},daemon=True);t.start()
            self.addCleanup(t.join,3);self.addCleanup(s.server_close);self.addCleanup(s.shutdown)
        e=Engine(c);self.addCleanup(e.close);f=e.search_async(self.x[0],0);state=accepted.get(timeout=3)
        message={k:state[k]for k in ['query_request_id','token','branch_id']}
        message.update(node_id=state['target_node'],involved_nodes=[state['target_node']],reports=[],error='injected query failure')
        if bounded:message['transfer_failure']=True
        def report(m):
            with socket.create_connection(tuple(state['head_endpoint']),3)as sock:
                sock.settimeout(3);send(sock,m,1<<20);return receive(sock,1<<20)[0]
        started=time.monotonic();reply=report(message);self.assertTrue(reply['ok'])
        if bounded:
            self.assertFalse(reply['cancelled'])
            self.assertFalse(f.done())
        with self.assertRaisesRegex(RuntimeError,'recovery grace exceeded' if bounded else 'injected query failure'):f.result(2)
        metrics=e.runtime.metrics()
        self.assertEqual(metrics['successful_queries'],0)
        self.assertEqual(metrics['failed_queries'],1)
        self.assertEqual(len(metrics['recent_failures']),1)
        self.assertEqual(metrics['recent_failures'][0]['request_id'],state['query_request_id'])
        if bounded:self.assertLess(time.monotonic()-started,1.5)
        message.update(error=None,children=[dict(branch_id='late-child',parent_branch_id=state['branch_id'],node_id=(state['target_node']+1)%3)],
                       reports=[dict(branch_id='late-report',node_id=state['target_node'])])
        reply=report(message);self.assertTrue(reply['ok']);self.assertTrue(reply['cancelled'])
        message['token']='wrong-token';self.assertFalse(report(message)['ok'])

class RegionReplicaTests(unittest.TestCase):
    setUpClass=classmethod(ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(ReplicaTests.tearDownClass.__func__)
    setUp=ReplicaTests.setUp
    rpc=ReplicaTests.rpc
    info=ReplicaTests.info
    check_result=RegionRuntimeTests.check_result
    def configure_replica_test(self,c):mode(c)

    def test_replica_source_privilege_and_no_whole_region_exclusion(self):
        info=self.info();a=info['source'];b=(a+1)%3
        entries=sorted({self.info(i)['entry_id'] for i in range(len(self.x)) if self.info(i)['source']==a})
        self.rpc(a,'source_start',id='region_test',plan=dict(ids=entries,entries=entries,target=b))
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            s=self.rpc(a,'status',id='region_test')
            if s['phase']=='FAILED':self.fail(s['error'])
            if s['phase']=='SENT':
                t=self.rpc(b,'status',id='region_test')
                if t['phase']=='FAILED':self.fail(t['error'])
                if t['phase']=='READY':break
            time.sleep(.01)
        else:self.fail('Replica not ready')
        self.engine.runtime.replica_routes([dict(source=a,target=b,version='region_test',entries=entries,fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=10000)])
        rows=[self.engine.search_async(x,i).result(10)for i,x in enumerate(self.x)]
        hits=[q for q in rows if q['stats'].get('replica_version')=='region_test'];self.assertTrue(hits)
        merged=False;source_opened=False
        for q in hits:
            self.check_result(q,200);by_id={r['branch_id']:r for r in q['branch_reports']}
            for r in q['branch_reports']:
                self.assertFalse(r.get('excluded_regions'))
                if r['node_id']==a and r.get('region_admission')!='source_reserved_not_started':
                    if not r['region_merged_admission']:
                        source_opened=True;self.assertEqual(by_id[r['parent_branch_id']]['logical_region'],'replica:region_test')
                    else:merged=True
        self.assertTrue(source_opened)
        recalls=[]
        for i,q in enumerate(rows):
            distances=np.sum((self.x.astype(float)-self.x[i])**2,axis=1);distances[i]=np.inf
            truth=set(map(int,np.argsort(distances)[:self.c['search']['top_k']]))
            recalls.append(len(truth&set(q['ids']))/len(truth))
        self.assertGreaterEqual(float(np.mean(recalls)),.95)
        self.engine.runtime.replica_routes([]);self.rpc(b,'retire',id='region_test')
        self.assertTrue(self.rpc(b,'reap',id='region_test')['reclaimed'])

del NativeRuntimeTests,ReplicaTests
