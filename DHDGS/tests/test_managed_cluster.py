"""Persistent cluster control: real TCP, native search, transaction recovery."""
import copy
import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch
import numpy as np
from threadpoolctl import threadpool_limits
from dhdgs.index import build
from dhdgs.managed_node import create_managed_server, revision
from dhdgs.managed_cluster import ClusterHead
from test_system import fixture

class ManagedClusterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory();cls.root=Path(cls.tmp.name)
        cls.base,cls.x=fixture(cls.root)
        with threadpool_limits(limits=1):build(cls.base)
    @classmethod
    def tearDownClass(cls):cls.tmp.cleanup()
    def setUp(self):
        self.work=tempfile.TemporaryDirectory();self.addCleanup(self.work.cleanup)
        root=Path(self.work.name);c=copy.deepcopy(self.base)
        c['network']['transport']='tcp';c['search'].update(execution='peer',max_expansions=32)
        c['peer'].update(execution_model='native',worker_threads=2,head_bind_host='127.0.0.1',head_advertise_host='127.0.0.1',head_port=0)
        c['peer']['control'].update(scheduler='native',completion_fast_path=False)
        c['peer']['communication'].update(mode='optimized',reuse_connections=True)
        c['storage']['io']['backend']='sync_pread'
        if getattr(self,'native_head',False):
            c['native_runtime'].update(head_enabled=True,data_enabled=True)
        if hasattr(self,'capture_limit'):
            c['benchmark_results']['memory_limit_bytes']=self.capture_limit
        self.servers=[];self.threads=[]
        held=[]
        for n in c['cluster']['nodes']:
            s=socket.socket();s.bind(('127.0.0.1',0));held.append(s);n.update(host='127.0.0.1',port=s.getsockname()[1])
        for s in held:s.close()
        self.token='test-token'*8
        self.addCleanup(self.stop)
        for n in c['cluster']['nodes']:
            server=create_managed_server(c,n['id'],self.token,root/f"node{n['id']}.json")
            t=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01},daemon=True)
            self.servers.append(server);self.threads.append(t);t.start()
        settings=dict(results_directory=str(root/'results'),command_timeout_seconds=3,drain_timeout_seconds=5,
            max_queries=3,max_client_threads=4,benchmark=dict(queries=3,concurrency=2,warmup=1),query_selection=str(root/'selection.json'))
        (root/'selection.json').write_text(json.dumps({'validation':[0,1,2]}))
        qids=[0,35,70]
        def truth(row,k):
            d=((self.x-self.x[qids[row]])**2).sum(axis=1);d[qids[row]]=np.inf
            return list(map(int,np.argsort(d)[:k]))
        self.head=ClusterHead(c,settings,self.token,truth)
    def stop(self):
        if hasattr(self,'head'):self.head.close()
        for server in self.servers:server.shutdown();server.server_close()
        for t in self.threads:t.join(2)
    def test_live_patch_query_benchmark_and_pull(self):
        self.head.configure({},recover=True);before=self.head.status()
        r=self.head.query(self.x[0],0);self.assertEqual(r['status'],'ok')
        self.head.quiescent()
        self.head.configure({'search.max_expansions':24,'peer.worker_threads':3,'scheduler.remote_weight':.5,'routing.ef_search':32})
        after=self.head.status()
        for a,b in zip(before['nodes'],after['nodes']):
            self.assertEqual((a['pid'],a['index_fd']),(b['pid'],b['index_fd']))
            self.assertEqual(b['worker_threads'],3);self.assertNotEqual(a['revision'],b['revision'])
        run=self.head.benchmark({});self.assertEqual(run['successful_queries'],3)
        self.assertTrue((Path(run['directory'])/'queries.csv').exists())
        self.assertEqual(set(self.head.pull()),{0,1,2})
        self.assertTrue(Path(self.head.collect()['directory']).is_dir())
    def test_auth_and_immutable_and_stale(self):
        owner=self.servers[0].control
        with self.assertRaises(ValueError):owner.command({'op':'admin_status','token':'bad'})
        self.head.configure({},recover=True)
        with self.assertRaises(ValueError):self.head.configure({'storage.page_bytes':8192})
        self.assertTrue(self.head.ready)
        with self.assertRaisesRegex(ValueError,'revision'):
            with owner.data({'op':'peer_submit','state':{'cluster_revision':'stale'}}):pass
        with self.assertRaises(ValueError):owner.command(dict(op='admin_chunk',token=self.token,snapshot_id=None,offset=0,count=5))
    def test_partial_commit_recovery(self):
        self.head.configure({},recover=True);old=revision(self.head.c);original=self.head.rpc
        def fault(n,op,**kw):
            if n['id']==1 and op=='admin_commit':raise OSError('injected lost node')
            return original(n,op,**kw)
        with patch.object(self.head,'rpc',side_effect=fault),self.assertRaises(OSError):
            self.head.configure({'search.max_expansions':24})
        self.assertFalse(self.head.ready)
        with self.assertRaises(RuntimeError):self.head.query(self.x[0],0)
        self.head.configure({},recover=True)
        self.assertTrue(self.head.ready)
        self.assertTrue(all(n['revision']==old for n in self.head.status()['nodes']))
        self.assertEqual(self.head.query(self.x[0],0)['status'],'ok');self.head.quiescent()
    def test_transaction_idempotency_and_busy(self):
        self.head.configure({},recover=True);owner=self.servers[0].control
        def cmd(op,**kw):return owner.command(dict(op=op,token=self.token,**kw))
        with owner.data({'op':'metrics'}),self.assertRaises(RuntimeError):
            cmd('admin_prepare',transaction='busy',expected_revision=owner.rev,patch={})
        cmd('admin_prepare',transaction='t',expected_revision=owner.rev,patch={'search.max_expansions':24})
        cmd('admin_prepare',transaction='t',expected_revision=owner.rev,patch={'search.max_expansions':24})
        cmd('admin_commit',transaction='t');cmd('admin_commit',transaction='t')
        cmd('admin_abort',transaction='t');cmd('admin_abort',transaction='t')
        with self.assertRaises(RuntimeError):cmd('admin_commit',transaction='t')
        with self.assertRaises(RuntimeError):cmd('admin_finalize',transaction='t')
        self.head.configure({},recover=True)

    def test_node_restart_restores_committed_settings_but_stays_paused(self):
        self.head.configure({},recover=True)
        self.head.configure({'search.max_expansions':24,'peer.worker_threads':3})
        self.head.quiescent();server=self.servers[0];state=server.control.state_file
        server.shutdown();server.server_close();self.threads[0].join(2)
        # Start from the original YAML, not the updated in-memory config.
        original=copy.deepcopy(self.head.c);original.pop('_cluster_revision',None)
        original['search']['max_expansions']=32;original['peer']['worker_threads']=2
        server=create_managed_server(original,0,self.token,state)
        self.servers[0]=server
        t=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01},daemon=True)
        self.threads[0]=t;t.start()
        status=server.control.status()
        self.assertFalse(status['accepting']);self.assertEqual(status['settings']['search.max_expansions'],24)
        self.assertEqual(status['worker_threads'],3)
        self.head.configure({},recover=True)
        self.assertTrue(server.control.status()['accepting'])
        self.assertEqual(self.head.query(self.x[0],0)['status'],'ok');self.head.quiescent()
