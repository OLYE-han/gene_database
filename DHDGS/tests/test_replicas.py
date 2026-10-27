"""Replica correctness: authenticated transfer, immutable graphs, native routing."""
import copy
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest

import numpy as np
import yaml
from threadpoolctl import threadpool_limits

from dhdgs.config import validate
from dhdgs.cpp_head import NativeEngine
from dhdgs.index import build, Shard, native
from dhdgs.managed_node import create_managed_server, revision
from dhdgs.managed_cluster import admin_rpc
from dhdgs.replicas import package, digest, maintenance_config
from test_cpp_runtime import NativeRuntimeTests
from test_system import fixture

ROOT = Path(__file__).resolve().parents[1]


class ReplicaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.base, cls.x = fixture(cls.root)
        with threadpool_limits(limits=1): build(cls.base)

    @classmethod
    def tearDownClass(cls): cls.temp.cleanup()

    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.c = NativeRuntimeTests.config(self, native_data=True, native_head=True)
        self.c['search']['max_expansions'] = 256
        b = copy.deepcopy(yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())['load_balance'])
        b.update(enabled=True, mode='static', selection_policy='entry_worksets', max_target_shard_fraction=10., sample_every=1, max_region_rows=1000, max_core_rows=1000,
                 reserve_disk_bytes=1048576, operation_timeout_s=20., min_validation_queries=2)
        self.c['load_balance'] = b
        # Reserve query and maintenance ports as pairs before starting servers.
        held = []
        for n in self.c['cluster']['nodes']:
            while True:
                a = socket.socket();a.bind(('127.0.0.1',0));port=a.getsockname()[1]
                z = socket.socket()
                try:
                    z.bind(('127.0.0.1', port+b['port_offset']))
                    held.extend([a,z]);n['port']=port;break
                except (OSError, OverflowError): a.close();z.close()
        for s in held:s.close()
        if hasattr(self,'configure_replica_test'):self.configure_replica_test(self.c)
        validate({k:v for k,v in self.c.items() if not k.startswith('_')})
        self.token = 'replica-test-token-' + 'x'*40
        self.servers=[]
        for n in self.c['cluster']['nodes']:
            server=create_managed_server(self.c,n['id'],self.token,Path(self.work.name)/str(n['id'])/'state.json')
            self.servers.append(server)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            self.addCleanup(thread.join,3);self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
            admin_rpc(n,self.token,'admin_resume',revision=revision(self.c))
        self.c['_cluster_revision']=revision(self.c)
        self.engine=NativeEngine(self.c);self.addCleanup(self.engine.close)
        self.client=native().MaintenanceClient(maintenance_config(self.c));self.addCleanup(self.client.close)

    def rpc(self, node, op, **kw):
        n=self.c['cluster']['nodes'][node]
        result=self.client.request(n['host'],n['port']+self.c['load_balance']['port_offset'],dict(token=self.token,op=op,**kw))['response']
        if not result['ok']:raise RuntimeError(result['error'])
        return result['result']

    def info(self, i=0):return self.engine.runtime.route_info(self.x[i].astype(float),i)

    def copy_source(self, version='test_replica'):
        info=self.info();source=info['source'];target=(source+1)%3
        directory=Path(self.c['paths']['output'])/'index'/f'shard_{source}'
        ids=np.load(directory/'ids.npy').tolist()
        self.rpc(source,'source_start',id=version,plan=dict(ids=ids,entries=[info['entry_id']],target=target))
        deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            s=self.rpc(source,'status',id=version)
            if s['phase']=='FAILED':self.fail(s['error'])
            if s['phase']=='SENT':
                target_status=self.rpc(target,'status',id=version)
                if target_status['phase']=='FAILED':self.fail(target_status['error'])
                if target_status['phase']=='READY':return info,source,target
            time.sleep(.02)
        self.fail('Replica did not become ready')

    def test_transfer_route_budget_and_retire(self):
        original=[digest(Path(self.c['paths']['output'])/'index'/f'shard_{i}'/'nodes.bin')for i in range(3)]
        info,a,b=self.copy_source()
        route=dict(source=a,target=b,version='test_replica',entries=[info['entry_id']],fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=10000)
        self.engine.runtime.replica_routes([route])
        result=self.engine.search_async(self.x[0],0).result(10)
        self.assertEqual(result['stats']['replica_version'],'test_replica')
        reports=result['branch_reports']
        self.assertEqual(next(r for r in reports if r['parent_branch_id'] is None)['node_id'],b)
        self.assertEqual(len({r['node_id']for r in reports}),len(reports))
        self.assertLessEqual(result['stats']['expanded'],256)
        exact=np.argsort(np.sum((self.x.astype(float)-self.x[0])**2,axis=1))[1:6]
        self.assertEqual(set(result['ids']),set(map(int,exact)))
        self.engine.runtime.replica_routes([])
        self.rpc(b,'retire',id='test_replica')
        self.assertTrue(self.rpc(b,'reap',id='test_replica')['reclaimed'])
        again=self.engine.search_async(self.x[0],0).result(10)
        self.assertNotIn('replica_version',again['stats'])
        for i,want in enumerate(original):self.assertEqual(digest(Path(self.c['paths']['output'])/'index'/f'shard_{i}'/'nodes.bin'),want)

    def test_validation_private_completion_and_expiry(self):
        info,a,b=self.copy_source('private')
        descriptor=dict(entry_id=info['entry_id'],source=a,target=b,version='private')
        result=self.engine.runtime.validate_replica(self.x[0].astype(float),0,descriptor)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(self.engine.runtime.metrics()['completed_pending'],0)
        self.engine.runtime.replica_routes([dict(source=a,target=b,version='private',entries=[info['entry_id']],fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=1)])
        time.sleep(.02)
        result=self.engine.search_async(self.x[0],0).result(10)
        self.assertNotIn('replica_version',result['stats'])

    def test_transfer_rejects_incomplete_and_corrupt_chunks(self):
        info=self.info();a=info['source'];b=(a+1)%3
        shard=Shard(Path(self.c['paths']['output'])/'index'/f'shard_{a}',0)
        try:
            path=Path(self.work.name)/'package'
            manifest=package(shard,[info['entry_id']],[info['entry_id']],path,self.c['load_balance'])
        finally:shard.close()
        self.rpc(b,'prepare',id='incomplete',manifest=manifest)
        with self.assertRaisesRegex(RuntimeError,'incomplete'):self.rpc(b,'commit',id='incomplete')
        raw=(path/'nodes.bin').read_bytes()
        with self.assertRaisesRegex(RuntimeError,'checksum'):
            self.rpc(b,'chunk',id='incomplete',file='nodes.bin',offset=0,data=raw,sha256='wrong')
        result=self.rpc(b,'chunk',id='incomplete',file='nodes.bin',offset=0,data=raw,sha256=hashlib.sha256(raw).hexdigest())
        self.assertFalse(result['duplicate'])
        result=self.rpc(b,'chunk',id='incomplete',file='nodes.bin',offset=0,data=raw,sha256=hashlib.sha256(raw).hexdigest())
        self.assertTrue(result['duplicate'])
        with self.assertRaisesRegex(RuntimeError,'Invalid replica'):
            self.rpc(b,'chunk',id='incomplete',file='../escape',offset=0,data=raw,sha256=hashlib.sha256(raw).hexdigest())

    def test_independent_calibration_gate_and_no_truth_fail_closed(self):
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'controller')
        info,a,b=self.copy_source('gate')
        result=controller.validate(dict(source=a,target=b,entries=[info['entry_id']],training_query_ids=[],min_score=-1e300,max_score=1e300),'gate')
        self.assertFalse(result['passed'])
        self.assertIn('ground-truth',result['reason'])

    def test_conflicting_source_retry_and_halo(self):
        info,a,b=self.copy_source('conflict')
        with self.assertRaisesRegex(RuntimeError,'Conflicting source'):
            self.rpc(a,'source_start',id='conflict',plan=dict(ids=[],entries=[],target=b))
        shard=Shard(Path(self.c['paths']['output'])/'index'/f'shard_{a}',0)
        try:
            settings=dict(self.c['load_balance'],halo_hops=1,max_region_rows=20)
            manifest=package(shard,[info['entry_id']],[info['entry_id']],Path(self.work.name)/'halo',settings)
            self.assertEqual(manifest['core_rows'],1)
            self.assertGreater(manifest['halo_rows'],0)
            self.assertLessEqual(manifest['rows'],20)
        finally:shard.close()

    def test_retired_overlay_cannot_be_republished_by_late_warm(self):
        info,a,b=self.copy_source('cancel')
        self.rpc(b,'retire',id='cancel')
        self.assertTrue(self.rpc(b,'reap',id='cancel')['reclaimed'])
        self.assertTrue(self.rpc(b,'reap',id='cancel')['reclaimed'])
        with self.assertRaisesRegex(RuntimeError,'not verified'):
            self.rpc(b,'warm',id='cancel')

    def test_nonadmitted_replica_request_falls_back_to_source(self):
        info=self.info();a=info['source'];b=(a+1)%3
        self.engine.runtime.replica_routes([dict(source=a,target=b,version='never_installed',
            entries=[info['entry_id']],fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=10000)])
        q=self.engine.search_async(self.x[0],0).result(10)
        self.assertEqual(q['status'],'ok')
        self.assertTrue(q['stats']['replica_unavailable_fallback'])
        self.assertNotIn('replica_version',q['stats'])
        self.assertLessEqual(q['stats']['expanded'],256)
        self.assertEqual(q['stats']['node_revisit_count'],0)



class ControllerPolicyTests(unittest.TestCase):
    def controller(self):
        from collections import defaultdict
        from dhdgs.load_balance import LoadBalancer
        x=LoadBalancer.__new__(LoadBalancer)
        x.b=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())['load_balance']
        x.lock=threading.RLock();x.active=x.in_progress=x.last_pair=None
        x.streak=0;x.cooldown_until=0;x.loads={};x.rates=defaultdict(float);x.logged=[]
        x.log=lambda event,**kw:x.logged.append((event,kw))
        x.propose=lambda a,b:dict(source=a,target=b)
        def create(plan):x.in_progress='test';x.created=plan
        x.create=create
        return x

    def test_stale_or_intermittent_pressure_does_not_trigger(self):
        x=self.controller()
        for i in range(5):
            now=10+i
            x.loads={0:dict(util=.95,queue_ms=8,updated=now,usable=True),
                     1:dict(util=.20,queue_ms=.1,updated=now,usable=True)}
            if i==3:x.loads[1]['updated']=now-1
            x.control(now)
        self.assertIsNone(x.in_progress)
        for i in range(4):
            now=15+i
            for s in x.loads.values():s['updated']=now
            x.control(now)
        self.assertEqual(x.created,dict(source=0,target=1))
        self.assertEqual(sum(e=='IMBALANCED' for e,_ in x.logged),1)

    def test_idle_target_is_not_throttled_by_historical_wait(self):
        x=self.controller();now=100
        x.loads={0:dict(util=.95,queue_ms=50,backlog_estimate_ms=40,updated=now,usable=True,workers=64),
                 1:dict(util=.40,queue_ms=32.768,backlog_estimate_ms=0,updated=now,usable=True,workers=64)}
        x.active=dict(id='r',source=0,target=1,entries=[1],fraction=.2,last_use=now,published=now,
                      validation=dict(source_saved_s=.003,target_added_s=.003))
        x.rates[1]=1000;x.adjust(now);self.assertGreater(x.active['fraction'],.2)
        x.loads[1]['backlog_estimate_ms']=100
        before=x.active['fraction'];x.adjust(now);self.assertEqual(x.active['fraction'],before*.5)

    def test_capacity_backoff_and_idle_retirement(self):
        x=self.controller();now=100
        x.loads={0:dict(util=.95,queue_ms=8,updated=now,usable=True,workers=64),
                 1:dict(util=.90,queue_ms=6,updated=now,usable=True,workers=64)}
        x.active=dict(id='r',source=0,target=1,entries=[1],fraction=.4,last_use=now,published=now,
                      validation=dict(source_saved_s=.003,target_added_s=.003))
        x.rates[1]=1000;x.adjust(now);self.assertEqual(x.active['fraction'],.2)
        x.active['last_use']=0;x.active['published']=0;x.rates[1]=1e-20
        x.retired=False
        def retire():x.retired=True
        x.retire=retire;x.adjust(now)
        self.assertTrue(x.retired)

class ControllerPipelineTests(ReplicaTests):
    # This class is invoked by its single method, not by discovery, to reuse
    # the real three-node fixture for a controlled trigger-to-drain exercise.
    def test_trigger_to_ready_and_drain(self):
        info=self.info();a=info['source'];b=(a+1)%3
        ids=np.load(Path(self.c['paths']['output'])/'index'/f'shard_{a}'/'ids.npy').tolist()
        qids=np.arange(len(self.x),dtype='uint64')
        distances=((self.x[:,None,:].astype(float)-self.x[None,:,:])**2).sum(axis=2)
        np.fill_diagonal(distances,np.inf)
        calibration=Path(self.work.name)/'truth.npz'
        np.savez(calibration,queries=self.x,ids=qids,truth=np.argsort(distances,axis=1)[:,:self.c['search']['top_k']])
        self.c['load_balance']['calibration_file']=str(calibration)
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'pipeline')
        # Synthetic load windows isolate the control trigger. Copy, transport,
        # quality queries, route publication and reclamation are real.
        controller.stop.set();controller.thread.join();controller.stop.clear()
        plan=dict(source=a,target=b,ids=ids,entries=ids,training_query_ids=[],min_score=-1e300,max_score=1e300)
        controller.propose=lambda source,target:plan
        for i in range(controller.b['sustained_windows']):
            now=time.monotonic()
            controller.loads={a:dict(util=.95,queue_ms=10,updated=now,usable=True),b:dict(util=.1,queue_ms=.1,updated=now,usable=True)}
            controller.control(now)
        deadline=time.monotonic()+20
        while controller.in_progress and time.monotonic()<deadline:time.sleep(.02)
        self.assertIsNotNone(controller.active,controller.events[-1])
        self.assertTrue(controller.active['qualified'])
        controller.set_fraction(.5);controller.publish(time.monotonic())
        controller.retire();self.assertIsNone(controller.active)
        self.assertTrue(any(e['event']=='RETIRED' for e in controller.events))


class PeerReplicaTests(ReplicaTests):
    def test_peer_replica_selection_and_missing_version_fallback(self):
        info,a,b=self.copy_source('peer_copy')
        ids=np.load(Path(self.c['paths']['output'])/'index'/f'shard_{a}'/'ids.npy').tolist()
        eligible=[dict(source=a,target=b,version='peer_copy',fraction=1.,ttl_ms=10000,weighted_entries=[[v,1.] for v in ids])]
        for n in self.c['cluster']['nodes']:self.rpc(n['id'],'routes',routes=eligible)
        hits=[]
        for i,x in enumerate(self.x):
            q=self.engine.search_async(x,i).result(10)
            self.assertEqual(q['status'],'ok',q)
            hits.extend(r for r in q['branch_reports'] if r.get('replica_access_kind')=='peer')
        self.assertTrue(hits,'No cross-node replica accesses exercised')
        counts=self.engine.runtime.metrics()['replica_access_counts']['peer_copy']
        self.assertEqual(counts.get('entry_completed',0),0)
        self.assertEqual(counts['peer_completed'],len(hits))
        # Withdraw the overlay while a stale directory still exists. Explicit
        # non-admission must fallback, with the SAME child credit and identity.
        self.rpc(b,'retire',id='peer_copy')
        fallbacks=0
        for i,x in enumerate(self.x):
            q=self.engine.search_async(x,i).result(10)
            self.assertEqual(q['status'],'ok',q)
            fallbacks+=sum(r.get('replica_unavailable_fallback',False) for r in q['branch_reports'])
            self.assertLessEqual(q['stats']['expanded'],self.c['search']['max_expansions'])
        self.assertGreater(fallbacks,0)
        for n in self.c['cluster']['nodes']:self.rpc(n['id'],'routes',routes=[])

if __name__=='__main__':unittest.main()
