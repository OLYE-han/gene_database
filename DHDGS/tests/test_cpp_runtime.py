"""Native protocol/search integration, including progress with the GIL held."""
import copy
import ctypes
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest

import numpy as np
from threadpoolctl import threadpool_limits

from dhdgs.index import build, native
from dhdgs.config import validate
from dhdgs.search import Engine
from dhdgs.transport import create_server
from test_system import fixture


class NativeRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.base, cls.x = fixture(cls.root)
        with threadpool_limits(limits=1):
            build(cls.base)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def config(self, native_data=True, native_head=False):
        c = copy.deepcopy(self.base)
        c['native_runtime'].update(data_enabled=native_data, head_enabled=native_head,
                                  receive_threads=2, route_threads=2, merge_threads=2,
                                  queued_control_registration=True,move_head_reports=True)
        c['peer'].update(execution_model='native', worker_threads=2, head_port=0)
        c['peer']['communication']['mode'] = 'optimized'
        c['peer']['control']['scheduler'] = 'native'
        c['peer']['head_direct']['enabled'] = False
        c['peer']['head_pipeline']['enabled'] = False
        c['peer']['head_lanes']['enabled'] = False
        c['peer']['head_receiver_processes'] = 0
        c['peer']['head_query_workers'] = 0
        c['storage']['io']['backend'] = 'sync_pread'
        c['network']['transport'] = 'tcp'
        c['search'].update(execution='peer', max_expansions=64)
        c['metrics']['lifecycle_trace'] = True
        for node in c['cluster']['nodes']:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                node['port'] = sock.getsockname()[1]
        validate({k: v for k, v in c.items() if not k.startswith('_')})
        return c

    def start(self, c):
        servers = []
        for node in c['cluster']['nodes']:
            server = create_server(c, node['id'])
            servers.append(server)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(thread.join, 3)
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
        return servers

    def test_pcg64_matches_numpy(self):
        for seed in [0, 1, 42, 2**32-1, 2**32, 2**63, 2**64-1]:
            for count in [0, 1, 17, 200]:
                rng = np.random.default_rng(seed)
                draws, child = native().pcg64_compat(seed, count)
                np.testing.assert_array_equal(draws, rng.random(count))
                self.assertEqual(child, int(rng.integers(0, 2**32)))

    def test_opaque_capture_preserves_full_native_result(self):
        import msgpack
        c = self.config(native_head=True)
        self.start(c)
        engine = Engine(c)
        self.addCleanup(engine.close)
        record, offered, delivered = engine.search_capture_async(self.x[0], 0).result(10)
        packet = record.pack(c['network']['max_message_bytes'])
        self.assertGreaterEqual(record.retained_bytes, len(packet))
        result = msgpack.unpackb(packet, raw=False, strict_map_key=False)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['ids'], engine.search(self.x[0], 0)['ids'])
        self.assertTrue(result['branch_reports'])
        self.assertLessEqual(offered, result['stats']['native_head_timestamps']['native_ready_ns'])
        self.assertLessEqual(result['stats']['native_head_timestamps']['native_ready_ns'], delivered)
        self.assertNotIn('api_result_delivery_ms', result['stats'])

    def test_archived_configuration_keeps_legacy_runtime(self):
        c = {k: v for k, v in self.base.items() if not k.startswith('_')}
        c.pop('native_runtime')
        validate(c)

    def test_early_parent_report_completes_query_while_child_ack_is_held(self):
        self._withheld_child('ack')

    def test_early_parent_report_waits_for_missing_child_result(self):
        self._withheld_child('request')

    def test_late_child_rejection_cannot_produce_partial_success(self):
        self._withheld_child('reject')

    def _withheld_child(self,hold):
        import socketserver
        from dhdgs.cpp_runtime import NativePeer, NativeServer
        from dhdgs.index import Shard, shard_path
        from dhdgs.transport import receive, send
        c=self.config(True,True)
        c['native_runtime']['early_parent_report']=True
        if hold=='reject':c['peer']['query_timeout_seconds']=.5
        c['scheduler']['mode']='adaptive'
        c['search']['local_beam']=1
        c['scheduler']['remote_weight']=1000000.0
        c['scheduler']['max_wait_ms']=.001
        real=copy.deepcopy(c)
        release=threading.Event();withheld=threading.Event()
        proxies=[];servers=[];threads=[];peers=[];shards=[];engine=None
        class Proxy(socketserver.ThreadingTCPServer):
            daemon_threads=True
        def handler_for(endpoint):
            class Handler(socketserver.BaseRequestHandler):
                def handle(self):
                    self.request.settimeout(4)
                    try:
                        with socket.create_connection(endpoint,4) as upstream:
                            while True:
                                request,_=receive(self.request,1<<20)
                                child=request.get('op')=='peer_submit' and request['state']['parent_branch_id'] is not None
                                if child and hold!='ack':
                                    withheld.set()
                                    if not release.wait(4):return
                                    if hold=='reject':
                                        send(self.request,dict(ok=False,error='injected child rejection',request_id=request.get('request_id')),1<<20)
                                        continue
                                send(upstream,request,1<<20);reply,_=receive(upstream,1<<20)
                                if child and hold=='ack':
                                    withheld.set()
                                    if not release.wait(4):return
                                send(self.request,reply,1<<20)
                    except (OSError,ValueError):pass
            return Handler
        try:
            for node in real['cluster']['nodes']:
                proxy=Proxy(('127.0.0.1',0),handler_for((node['host'],node['port'])))
                proxies.append(proxy);c['cluster']['nodes'][node['id']]['port']=proxy.server_address[1]
                thread=threading.Thread(target=proxy.serve_forever,kwargs={'poll_interval':.01});thread.start();threads.append(thread)
            for node in real['cluster']['nodes']:
                shard=Shard(shard_path(c,node['id']),0);shards.append(shard)
                peer=NativePeer(c,shard);peers.append(peer)
                server=NativeServer(real,node['id'],peer,lambda request:{},lambda:None)
                servers.append(server);server.configure(peer,True)
            engine=Engine(c);future=engine.search_async(self.x[35],35)
            if not withheld.wait(2):
                self.fail('fixture must perform cross-node search: '+str(future.result(.1)))
            if hold!='ack':
                time.sleep(.05)
                self.assertFalse(future.done(),'parent report alone must not complete the query')
                release.set()
            if hold=='reject':
                with self.assertRaisesRegex(RuntimeError,'child report missing after transfer failure'):
                    future.result(2)
                return
            result=future.result(2)
            self.assertEqual(result['status'],'ok')
            self.assertGreater(len(result['branch_reports']),1)
            root=next(r for r in result['branch_reports'] if r['parent_branch_id'] is None)
            self.assertGreater(root['pending_transfers_at_publication'],0)
            if hold=='ack':
                self.assertFalse(result['stats']['transfer_metrics_complete'])
                self.assertFalse(release.is_set())
        finally:
            release.set()
            if engine:engine.close()
            for server in servers:server.server_close()
            for proxy in proxies:proxy.shutdown();proxy.server_close()
            for thread in threads:thread.join(3)
            for peer in peers:peer.close()
            for shard in shards:shard.close()

    def test_native_data_legacy_head_concurrent(self):
        c = self.config()
        self.start(c)
        engine = Engine(c)
        self.addCleanup(engine.close)
        with ThreadPoolExecutor(8) as pool:
            results = list(pool.map(lambda i: engine.search(self.x[i], i), range(24)))
        for result in results:
            self.assertEqual(result['status'], 'ok')
            self.assertLessEqual(result['stats']['expanded'], 64)
            self.assertEqual(result['stats']['node_revisit_count'], 0)
            self.assertTrue(result['branch_reports'])
            for report in result['branch_reports']:
                self.assertEqual(report['compute_thread_migrations'], 0)

    def test_native_head_data_equivalence_and_gil_independence(self):
        c = self.config()
        self.start(c)
        legacy = Engine(c)
        self.addCleanup(legacy.close)
        expected = [legacy.search(self.x[i], i) for i in [0, 35, 70]]
        c['native_runtime']['head_enabled'] = True
        engine = Engine(c)
        self.addCleanup(engine.close)
        futures = [engine.search_async(self.x[i], i) for i in [0, 35, 70]]
        libc = ctypes.PyDLL(None)
        libc.usleep.argtypes = [ctypes.c_uint]
        libc.usleep(150_000)
        released = time.monotonic_ns()
        actual = [future.result(3) for future in futures]
        for a, b in zip(expected, actual, strict=True):
            self.assertEqual(a['ids'], b['ids'])
            self.assertEqual(a['squared_distances'], b['squared_distances'])
            self.assertEqual(a['stats']['visited_nodes'], b['stats']['visited_nodes'])
            self.assertEqual(a['stats']['expanded'], b['stats']['expanded'])
            self.assertLess(b['stats']['native_head_timestamps']['native_ready_ns'], released)
            self.assertGreater(b['stats']['api_result_delivery_ms'], 50)
            self.assertAlmostEqual(sum(b['stats']['latency_breakdown_ms'].values()), b['stats']['latency_ms'], places=5)
            self.assertEqual(b['stats']['metrics_complete'], b['stats']['metrics_complete_excluding_pending_transfers'] and b['stats']['transfer_metrics_complete'])

    def test_native_aggregation_matches_python(self):
        from dhdgs.head_merge import aggregate_reports
        c = self.config(True, True)
        self.start(c)
        engine = Engine(c)
        self.addCleanup(engine.close)
        result = engine.search(self.x[35], 35)
        reports = result['branch_reports']
        transfer = result['stats']['entry_rpc']
        ranked, expected = aggregate_reports(c, reports, transfer, result['stats']['keepalive_nodes'])
        actual = native().aggregate_native(c, reports, transfer, result['stats']['keepalive_nodes'])
        self.assertEqual(actual['ids'], [row[0] for row in ranked])
        # MessagePack native maps have string keys; JSON archives do too.
        import json
        self.assertEqual(json.loads(json.dumps(expected)), json.loads(json.dumps(actual['stats'])))

    def test_native_head_legacy_data(self):
        c = self.config(False, True)
        self.start(c)
        engine = Engine(c)
        self.addCleanup(engine.close)
        result = engine.search(self.x[0], 0)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['stats']['metrics_complete'], result['stats']['metrics_complete_excluding_pending_transfers'] and result['stats']['transfer_metrics_complete'])

    def test_capacity_and_close_with_unresponsive_data_nodes(self):
        c = self.config(True, True)
        c['peer']['max_active_queries'] = 1
        listeners = []
        for node in c['cluster']['nodes']:
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            sock.listen()
            listeners.append(sock)
            self.addCleanup(sock.close)
            node['port'] = sock.getsockname()[1]
        engine = Engine(c)
        self.addCleanup(engine.close)
        future = engine.search_async(self.x[0], 0)
        with self.assertRaisesRegex(ValueError, 'capacity'):
            engine.search_async(self.x[1], 1)
        started = time.monotonic()
        engine.close()
        self.assertLess(time.monotonic()-started, 2)
        with self.assertRaises(RuntimeError):
            future.result(1)

    def test_refused_entry_returns_error(self):
        c = self.config(False, True)
        engine = Engine(c)
        self.addCleanup(engine.close)
        future = engine.search_async(self.x[0], 0)
        with self.assertRaises(RuntimeError):
            future.result(3)

    def test_http_boundary_without_python_query_flow(self):
        from http.client import HTTPConnection
        from dhdgs.head_http import PipelineHTTPServer, DeferredHandler
        c = self.config(True, True)
        self.start(c)
        engine = Engine(c)
        self.addCleanup(engine.close)
        vector = self.x[0]
        class Handler(DeferredHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                self.defer_query(engine.search_async(vector, 0))
            def reply(self, status, value):
                body = b'ok'
                self.send_response(status)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        server = PipelineHTTPServer(('127.0.0.1', 0), Handler, None, 32, ingress_workers=2)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
        thread.start()
        try:
            conn = HTTPConnection(*server.server_address, timeout=3)
            try:
                conn.request('GET', '/')
                response = conn.getresponse()
                self.assertEqual((response.status, response.read()), (200, b'ok'))
            finally:
                conn.close()
        finally:
            server.shutdown()
            thread.join()
            server.server_close()

    def test_manifest_faults_and_out_of_order(self):
        c = self.config(True, True)
        self.start(c)
        engine = Engine(c)
        self.addCleanup(engine.close)
        result = engine.search(self.x[35], 35)
        reports = result['branch_reports']
        root = next(r for r in reports if r['parent_branch_id'] is None)
        def valid(rs):
            return native().validate_manifests(rs, root['branch_id'], root['node_id'],
                                              root['work_grant'], root['branch_slots'], 3)
        self.assertTrue(valid(list(reversed(reports))))
        self.assertFalse(valid([]))
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            valid(reports + [reports[0]])
        broken = copy.deepcopy(reports)
        broken[0]['work_unused'] += 1
        with self.assertRaises(ValueError):
            valid(broken)
        broken = copy.deepcopy(reports)
        broken[0]['visited_nodes'] += [broken[0]['node_id']]
        with self.assertRaises(ValueError):
            valid(broken)

    def test_tcp_partial_frames_retries_conflicts_and_lease(self):
        import msgpack
        import struct
        from dhdgs.peer import HeadSession, execution_signature
        from dhdgs.transport import receive, send
        c = self.config()
        self.start(c)
        sid = 0
        ids = np.load(self.root/'out/index/shard_0/ids.npy')
        gid = int(ids[0])
        member = np.load(self.root/'out/index/membership.npy')
        qid = 'retry-query'
        session = HeadSession(c, qid, 'test-token', 'root', sid)
        self.addCleanup(session.close, True)
        state = dict(query_request_id=qid, branch_id='root', parent_branch_id=None, token='test-token',
            head_endpoint=session.endpoint, entry_node=sid, target_node=sid, epoch=session.c and
            __import__('json').loads((self.root/'out/index/manifest.json').read_text())['epoch'],
            signature=execution_signature(c), query_f32=self.x[0].tobytes(), query_id=0,
            work_grant=64, branch_slots=c['peer']['max_branches']-1, seeds=[[gid, 0., 0.,
                list(map(int, np.flatnonzero(member[gid]))), None, 0, 0, gid, 0.]],
            seen=[gid], fallback_entries=[], visited_nodes=[sid], remote_frontier=[],
            fallback_remaining=0, scale=1., ancestry=[], decision_seed=42)
        node = c['cluster']['nodes'][0]
        with socket.create_connection((node['host'], node['port']), 3) as sock:
            request = dict(version=1, op='peer_submit', request_id='r', state=state)
            payload = msgpack.packb(request, use_bin_type=True)
            frame = struct.pack('!I', len(payload)) + payload
            for i in range(0, len(frame), 7):
                sock.sendall(frame[i:i+7])
            first, _ = receive(sock, 1 << 20)
            self.assertTrue(first['ok'])
            self.assertFalse(first['result']['duplicate'])
            send(sock, request, 1 << 20)
            again, _ = receive(sock, 1 << 20)
            self.assertTrue(again['result']['duplicate'])
            changed = copy.deepcopy(request)
            changed['state']['decision_seed'] = 1
            send(sock, changed, 1 << 20)
            conflict, _ = receive(sock, 1 << 20)
            self.assertFalse(conflict['ok'])
            self.assertIn('Conflicting', conflict['error'])
            send(sock, dict(version=1, op='peer_lease', query_request_id=qid, token='wrong'), 1 << 20)
            lease, _ = receive(sock, 1 << 20)
            self.assertFalse(lease['ok'])
        self.assertTrue(session.wait(time.monotonic()+3))

    def test_live_configuration_and_result_collection(self):
        import json
        from dhdgs.managed_node import create_managed_server
        from dhdgs.managed_cluster import ClusterHead, admin_rpc
        c = self.config(True, True)
        token = 'native-management-test-'*3
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            servers = []
            for n in c['cluster']['nodes']:
                server = create_managed_server(c, n['id'], token, root/f"node_{n['id']}.json")
                servers.append(server)
            selection = root/'selection.json'
            selection.write_text(json.dumps({'validation': [0, 1, 2]}))
            settings = dict(results_directory=str(root/'results'), command_timeout_seconds=3,
                drain_timeout_seconds=5, max_queries=3, max_client_threads=4,
                benchmark=dict(queries=3, concurrency=2, warmup=1), query_selection=str(selection))
            qids = [0, 35, 70]
            def truth(row, k):
                ds = ((self.x-self.x[qids[row]])**2).sum(axis=1)
                ds[qids[row]] = np.inf
                return list(map(int, np.argsort(ds)[:k]))
            head = ClusterHead(c, settings, token, truth)
            try:
                head.configure({}, recover=True)
                before = head.status()
                self.assertEqual(head.query(self.x[0], 0)['status'], 'ok')
                head.quiescent()
                head.configure({'search.max_expansions': 32, 'peer.worker_threads': 3, 'routing.ef_search': 32})
                after = head.status()
                for a, b in zip(before['nodes'], after['nodes'], strict=True):
                    self.assertEqual((a['pid'], a['index_fd']), (b['pid'], b['index_fd']))
                    self.assertEqual(b['worker_threads'], 3)
                result = head.benchmark({})
                self.assertEqual(result['successful_queries'], 3)
                self.assertEqual(set(head.pull()), {0, 1, 2})
                head.quiescent()
                for n, server in zip(c['cluster']['nodes'], servers, strict=True):
                    self.assertFalse(admin_rpc(n, token, 'admin_shutdown')['accepting'])
                    server.transport.wait_shutdown()
            finally:
                head.close()
                for server in servers:
                    server.server_close()
