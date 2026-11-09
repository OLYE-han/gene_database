"""Nonblocking start, canonical budget refunds and independent delivery closure."""
import copy
from test_candidate_sync import CandidateSyncTests, CandidatePullTests
from dhdgs.search import Engine

class NonblockingCandidateTests(CandidateSyncTests):
    def configuration(self):
        c=super().configuration()
        c['peer']['candidate_sync'].update(nonblocking_start=True,early_result=True,completion_threads=4,completion_capacity=8192)
        return c

    def test_no_claim_pause_and_budget_accounting(self):
        c=self.configuration();self.start(c);e=Engine(c);self.addCleanup(e.close)
        rows=[f.result(15)for f in [e.search_async(x,i)for i,x in enumerate(self.x[:64])]]
        for q in rows:
            self.assertEqual(q['status'],'ok',q.get('error'))
            self.assertLessEqual(q['stats']['expanded'],200)
            self.assertEqual(len(q['branch_reports']),len({b['node_id']for b in q['branch_reports']}))
            for b in q['branch_reports']:
                self.assertFalse(any(x['kind']=='claim'for x in b.get('candidate_pause_events',[])))
                self.assertEqual(b['work_grant'],b['work_spent']+b['work_unused']+sum(x['work_grant']for x in b['children']))
                if b.get('candidate_delivery_required'):
                    self.assertIn('delivery_done_ns:'+b['branch_id'],q['stats']['native_head_timestamps'])
        self.assertEqual(e.runtime.metrics()['active'],0)

    def test_candidate_may_arrive_before_atomic_start(self):
        c=self.configuration();self.start(c);s=self.state(c,qid='out-of-order-start')
        fields={k:s[k]for k in ('query_request_id','token','epoch','signature')}
        packet=dict(sender_branch='sender',sequence=1,rows=s['seeds'],admission_bound=None,bound_valid=False,expansions=10,**fields)
        self.assertTrue(self.rpc(c,0,'peer_candidate_delta',**packet)['accepted'])
        self.assertTrue(self.rpc(c,0,'peer_candidate_delta',**packet)['accepted'])

class NonblockingPullTests(CandidatePullTests):
    def configure_replica_test(self,c):
        super().configure_replica_test(c)
        c['peer']['candidate_sync'].update(nonblocking_start=True,early_result=True,completion_threads=4,completion_capacity=8192)

class DelayedCandidateTests(NonblockingCandidateTests):
    # Run only deliberate ordering tests on this fixture; the parent suite is
    # selected separately by the round runner.
    def withheld(self,operation,reject=False,move_reports=None):
        import socket,socketserver,threading,time,resource
        limits=resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE,(min(65536,limits[1]),limits[1]))
        self.addCleanup(resource.setrlimit,resource.RLIMIT_NOFILE,limits)
        from dhdgs.cpp_runtime import NativePeer, NativeServer
        from dhdgs.index import Shard, shard_path
        from dhdgs.transport import receive,send
        c=self.configuration();c['metrics']['detailed_trace']=True
        if move_reports is not None:c['native_runtime']['move_head_reports']=move_reports
        c['search'].update(max_expansions=64,local_beam=1)
        c['peer']['query_timeout_seconds']=10.
        real=copy.deepcopy(c);release=threading.Event();seen=threading.Event()
        peers=[];servers=[];proxies=[];threads=[];shards=[];engine=None
        class Proxy(socketserver.ThreadingTCPServer):
            daemon_threads=True
            request_queue_size=1024
        def handler(endpoint):
            class Handler(socketserver.BaseRequestHandler):
                def handle(self):
                    self.request.settimeout(8)
                    try:
                        with socket.create_connection(endpoint,5)as up:
                            while True:
                                req,_=receive(self.request,1<<24);send(up,req,1<<24);reply,_=receive(up,1<<24)
                                if req.get('op')==operation:
                                    seen.set()
                                    if not release.wait(6):return
                                    if reject:reply=dict(ok=False,error='injected candidate rejection')
                                send(self.request,reply,1<<24)
                    except (OSError,EOFError,ValueError):pass
            return Handler
        try:
            for node in real['cluster']['nodes']:
                proxy=Proxy(('127.0.0.1',0),handler((node['host'],node['port'])));proxies.append(proxy)
                c['cluster']['nodes'][node['id']]['port']=proxy.server_address[1]
                t=threading.Thread(target=proxy.serve_forever,kwargs={'poll_interval':.01},daemon=True);t.start();threads.append(t)
            for node in real['cluster']['nodes']:
                shard=Shard(shard_path(c,node['id']),0);shards.append(shard)
                peer=NativePeer(c,shard);peers.append(peer)
                server=NativeServer(real,node['id'],peer,lambda r:{},lambda:None);servers.append(server);server.configure(peer,True)
            engine=Engine(c);futures=[engine.search_async(x,i)for i,x in enumerate(self.x)]
            self.assertTrue(seen.wait(3),'Fixture must cross requested protocol path')
            time.sleep(.08)
            self.assertTrue(any(not f.done()for f in futures),'Unconfirmed work must prevent final success')
            released=time.monotonic_ns();release.set()
            if reject:
                failures=0
                for f in futures:
                    try:f.result(5)
                    except RuntimeError as exc:
                        self.assertIn('Candidate',str(exc));failures+=1
                self.assertGreater(failures,0,'Rejected delivery must never produce partial success')
                return
            results=[f.result(5)for f in futures]
            for q in results:
                self.assertEqual(q['status'],'ok')
                self.assertLessEqual(q['stats']['expanded'],64)
                self.assertEqual(len(q['branch_reports']),len({b['node_id']for b in q['branch_reports']}))
            if operation=='peer_candidate_delta':
                early=[(q,b)for q in results for b in q['branch_reports']if b.get('candidate_delivery_required')]
                self.assertTrue(early,'Need a report published before candidate ACK')
                self.assertTrue(any(b['native_timestamps']['native_report_ready_ns']<released for q,b in early))
                self.assertTrue(all(q['stats']['native_head_timestamps']['delivery_done_ns:'+b['branch_id']]>=released for q,b in early))
                # Final reports must include traffic whose ACK arrived after
                # search/report completion, including coalesced follow-up packets.
                sent=sum(p.metrics()['candidate_sync']['sent'] for p in peers)
                received=sum(p.metrics()['candidate_sync']['received'] for p in peers)
                reported=sum(q['stats'].get('candidate_sync_rpc_count',0) for q in results)
                conflicts=sum(q['stats'].get('candidate_start_conflict',0) for q in results)
                self.assertGreater(sent,0)
                self.assertEqual(received-conflicts,sent)
                self.assertEqual(reported,sent,'Early report lost final candidate delivery counters')
                self.assertEqual(sum(b['peer_transfer'].get('peer_candidate_sync_rpc_count',0)
                                     for q in results for b in q['branch_reports']),sent)
            else:
                children=[b for q in results for b in q['branch_reports']if b['parent_branch_id']is not None]
                self.assertTrue(any(b['native_timestamps']['native_search_start_ns']<released for b in children),'Child should start before first-start ACK')
                self.assertFalse(any(e['kind']=='claim'for q in results for b in q['branch_reports']for e in b.get('candidate_pause_events',[])))
            self.assertEqual(engine.runtime.metrics()['active'],0)
        finally:
            release.set()
            if engine:engine.close()
            for server in servers:server.server_close()
            for proxy in proxies:proxy.shutdown();proxy.server_close()
            for t in threads:t.join(3)
            for peer in peers:peer.close()
            for shard in shards:shard.close()

    def test_result_precedes_ack_but_query_waits_for_delivery(self):self.withheld('peer_candidate_delta')
    def test_child_computes_before_first_start_ack(self):self.withheld('peer_candidate_start')

    def test_failed_delivery_after_report_is_query_failure(self):self.withheld('peer_candidate_delta',reject=True)


class CoalescedCandidateTests(NonblockingCandidateTests):
    def configuration(self):
        c=super().configuration()
        c['peer']['candidate_sync'].update(coalesce_pending=True,max_buffered_records=4096,completion_threads=0)
        return c

class CoalescedDelayedTests(DelayedCandidateTests):
    def configuration(self):
        c=super().configuration()
        c['peer']['candidate_sync'].update(coalesce_pending=True,max_buffered_records=4096,completion_threads=0)
        return c

    def test_final_delivery_metrics_copy(self):
        self.withheld('peer_candidate_delta',move_reports=False)

    def test_final_delivery_metrics_move(self):
        self.withheld('peer_candidate_delta',move_reports=True)


class SeparateAdmissionTests(CoalescedCandidateTests):
    def configuration(self):
        c=super().configuration()
        c['peer']['candidate_sync'].update(start_threads=4,start_capacity=512)
        return c

class SeparateAdmissionDelayedTests(CoalescedDelayedTests):
    def configuration(self):
        c=super().configuration()
        c['peer']['candidate_sync'].update(start_threads=4,start_capacity=512)
        return c


class ReservedRegistryTests(CoalescedCandidateTests):
    def configuration(self):
        c=super().configuration()
        c['native_runtime']['registry_reserve_entries']=4096
        return c

    def test_zero_reservation_is_valid_and_negative_is_rejected(self):
        from dhdgs.config import validate
        c={k:v for k,v in self.configuration().items()if not k.startswith('_')}
        c['native_runtime']['registry_reserve_entries']=0
        validate(c)
        c['native_runtime']['registry_reserve_entries']=-1
        with self.assertRaises(ValueError):validate(c)

    def test_reservation_avoids_runtime_growth_and_keeps_fixed_budget(self):
        c=self.configuration();self.start(c);e=Engine(c);self.addCleanup(e.close)
        tasks=[e.search_async(self.x[i%len(self.x)],i)for i in range(128)]
        for f in tasks:
            q=f.result(15)
            self.assertEqual(q['status'],'ok',q.get('error'))
            self.assertLessEqual(q['stats']['expanded'],200)
            self.assertEqual(len(q['branch_reports']),len({b['node_id']for b in q['branch_reports']}))
        h=e.runtime.metrics()
        self.assertEqual(h['active'],0)
        self.assertGreaterEqual(h['registry_buckets'],4096)
        self.assertEqual(h['registry_rehashes'],0)
        for node in range(3):
            a=self.rpc(c,node,'metrics')['peer_lifecycle']['admissions']
            self.assertGreaterEqual(a['registry_context_buckets'],4096)
            self.assertEqual(a['registry_rehashes'],0)
