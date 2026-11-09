"""Admission streaming: real native records, maintenance sockets and rollback."""
import copy
import hashlib
import json
import os
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

import dhdgs.hop_expansion  # Load dependency before fixtures temporarily patch metadata.
from dhdgs.index import Shard, native, HEADER, MAGIC
from dhdgs.replicas import ReplicaService, ReplicaTransportError, package, holds_reservation
from dhdgs.replica_layout import PIPELINE_DEFAULTS
import test_replicas as fixtures
import test_partition_pipeline as planning_fixtures


class AdmissionPlannerTests(unittest.TestCase):
    def setUp(self):
        f=planning_fixtures.PartitionPipelineTests();f.setUp();self.addCleanup(f.doCleanups);self.f=f

    def test_monotonic_admission_equals_final_plan_without_changing_certificate(self):
        f=self.f;base,_=f.build();cfg=copy.deepcopy(f.layout)
        cfg['pipeline'].update(copy_during_expansion=True,growth_batch_rows=2)
        streams={}
        class Stream:
            def __init__(self,target):self.ids=[];self.closed=False;streams[target]=self
            def admit(self,ids):
                assert not self.closed and 0<len(ids)<=2
                assert not set(ids).intersection(self.ids)
                self.ids.extend(ids)
            def finish(self,plan):
                assert sorted(self.ids)==plan['ids'];self.closed=True
        plans,_=f.build(cfg,stream_factory=lambda target,cap:Stream(target))
        for a,b in zip(base,plans):
            for k in ('ids','entries','region_proof','reserved_package_bytes'):self.assertEqual(a[k],b[k])
            self.assertTrue(streams[b['target']].closed)

    def test_callback_failure_and_invalid_setup_fail_closed(self):
        f=self.f;cfg=copy.deepcopy(f.layout);cfg['pipeline']['copy_during_expansion']=True
        with self.assertRaisesRegex(ValueError,'online'):f.build(cfg)
        class Broken:
            def admit(self,ids):raise RuntimeError('copy queue rejected')
            def finish(self,plan):raise AssertionError('must not finalize')
        emitted=[]
        with self.assertRaisesRegex(RuntimeError,'copy queue rejected'):
            f.build(cfg,stream_factory=lambda *args:Broken(),on_plan=emitted.append)
        self.assertEqual(emitted,[])


class GrowingReplicaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixtures.ReplicaTests.setUpClass.__func__(cls)
        cls.addClassCleanup(fixtures.ReplicaTests.tearDownClass.__func__,cls)
    setUp=fixtures.ReplicaTests.setUp
    info=fixtures.ReplicaTests.info
    rpc=fixtures.ReplicaTests.rpc

    def configure_replica_test(self,c):
        b=c['load_balance']
        b.update(replica_transfer_mode='pipeline_v2',replica_loader='native',native_replica_pack=True,
            chunk_bytes=4096,producer_queue_bytes=8192,inflight_chunks=2,publication_mode='ready',warm_bytes=0,
            compute_region_quality=False,quality_weight_routing=False,peer_routes_enabled=False,
            max_region_bytes=4000000,max_total_replica_bytes=8000000,max_code_bytes=3000000,
            max_replicas=4,max_target_shard_fraction=10000.,community_max_plan_ms=10000,community_top_k=16)
        b['locality'].update(enabled=True,source_capacity_fraction=.3)
        b['partitioned_regions'].update(total_alpha=.3,hops=1,deadline_s=15.,max_edge_scans=100000)
        b['partitioned_regions']['pipeline'].update(copy_during_expansion=True,growth_batch_rows=4,growth_queue_batches=1)

    def service(self,node):return self.servers[node].control.replica_service

    def inputs(self):
        info=self.info();a=info['source'];others=[i for i in range(3) if i!=a]
        shard=self.service(a).owner.worker
        history=dict(hot_ids=list(map(int,shard.ids[:4])),weights=[4.,3.,2.,1.],training_query_ids=[])
        layout=copy.deepcopy(self.c['load_balance']['partitioned_regions'])
        layout['target_weights']={str(t):1. for t in others}
        return a,others,shard,history,layout

    def test_native_append_layout_original_records_and_dependency_quality(self):
        a,others,shard,history,layout=self.inputs();m=shard.meta
        path=Path(self.work.name)/'append';path.mkdir()
        header=HEADER.pack(MAGIC,1,m['dimension'],0,m['record_bytes'])+bytes(m['header_bytes']-HEADER.size)
        w=native().ReplicaGrowingWriter(shard.fd,shard.ids,m['dimension'],m['record_bytes'],m['header_bytes'],
            m['max_degree'],1000,4000000,str(path/'nodes.bin'),header,True)
        self.addCleanup(w.close)
        ordered=list(map(int,shard.ids))
        for ids in reversed([ordered[i:i+4] for i in range(0,len(ordered),4)]):w.append(ids)
        with self.assertRaisesRegex(ValueError,'duplicate'):w.append([ordered[0]])
        packed=w.finish(ordered)
        self.assertNotEqual(list(packed['ids']),ordered)
        b=dict(self.c['load_balance'],halo_hops=0,compute_region_quality=True)
        mf=package(shard,ordered,[ordered[0]],path,b,prepacked=packed)
        standard=package(shard,ordered,[ordered[0]],Path(self.work.name)/'standard',b)
        for k in ('code_ids.npy','codes.npy','extra.npy','factors.npy','center_ids.npy','code_membership.npy'):
            self.assertEqual(mf['files'][k],standard['files'][k])
        for key in ('mean_expected_walk_steps','internal_edge_fraction'):
            self.assertAlmostEqual(mf['quality'][key],standard['quality'][key],places=12)
        self.assertEqual(mf['quality']['entry_weights'],standard['quality']['entry_weights'])
        self.assertEqual(mf['quality']['peer_entries'],standard['quality']['peer_entries'])
        replica=Shard(path,0);self.addCleanup(replica.close)
        for gid in ordered:
            self.assertEqual(os.pread(shard.fd,m['record_bytes'],m['header_bytes']+shard.location[gid]*m['record_bytes']),
                os.pread(replica.fd,m['record_bytes'],m['header_bytes']+replica.location[gid]*m['record_bytes']))
        search=self.service(others[0]).owner.worker.native_search
        prepared=search.prepare_replica_directory('native_append',str(path),json.loads((path/'manifest.json').read_text()),
            mf['centers_sha256'],mf['entries'],0,0,native().ReplicaCancel(20))
        search.publish_replica(prepared);search.retire_replica('native_append')
        ids=np.load(path/'ids.npy');ids[0]=ids[1];np.save(path/'ids.npy',ids)
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            search.prepare_replica_directory('duplicate',str(path),json.loads((path/'manifest.json').read_text()),
                mf['centers_sha256'],mf['entries'],0,0,native().ReplicaCancel(20))

    def test_online_growth_copy_before_plan_close_lost_acks_and_search(self):
        a,others,shard,history,layout=self.inputs()
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'controller')
        original=ReplicaService.rpc;lost=set();ops={'pipeline_begin','pipeline_open','pipeline_chunk','pipeline_seal_nodes','pipeline_finish'}
        def lose(service,node,op,**fields):
            reply=original(service,node,op,**fields)
            if op in ops and op not in lost:
                lost.add(op);raise ReplicaTransportError('Injected lost growth ACK')
            return reply
        from dhdgs.replica_growth import GrowingReplica
        original_finish=GrowingReplica.finish
        def finish_after_copy(stream,plan):
            target=self.service(plan['target']);until=time.monotonic()+5
            while time.monotonic()<until:
                state=target.states.get(stream.version,{})
                if state.get('received_bytes',0)>0:break
                time.sleep(.001)
            self.assertGreater(state.get('received_bytes',0),0)
            self.assertTrue(target.owner.worker.native_search.replica_released(stream.version))
            original_finish(stream,plan)
        # Functional fixture uses an inflated accounting base; no speed/capacity conclusion.
        with patch('dhdgs.locality.source_index_bytes',return_value=10000000),patch.object(ReplicaService,'rpc',lose),patch.object(GrowingReplica,'finish',finish_after_copy):
            try:report,versions=controller.build_partitioned_online(a,history,layout)
            except BaseException:
                for st in self.service(a).states.values():
                    print('GROWTH_DIAGNOSTIC',st.get('error'),st.get('_producer_traceback'),flush=True)
                print('CANCEL_DIAGNOSTIC',[e for e in controller.events if e['event']=='PARTITION_CANCEL_PENDING'],flush=True)
                raise
        self.assertEqual(lost,ops);self.assertEqual(controller.partition_pending,{})
        self.assertEqual(len(versions),2)
        for version in versions:
            r=controller.regions[version];src=self.rpc(a,'status',id=version);dst=self.rpc(r['target'],'status',id=version)
            self.assertNotIn('_stream_version',r)
            self.assertEqual(src['admitted_rows'],len(r['ids']))
            self.assertLess(src['first_chunk_ns'],src['source_sealed_ns'])
            self.assertEqual(dst['phase'],'READY')
            self.assertEqual(dst['received_bytes'],dst['manifest']['bytes'])
            self.assertLessEqual(src['admission_queue_peak'],1)
        query=self.engine.search_async(self.x[0],0).result(10)
        self.assertEqual(query['status'],'ok')
        self.assertEqual(set(query['ids']),set(map(int,np.argsort(np.sum((self.x.astype(float)-self.x[0])**2,axis=1))[1:6])))
        controller.retire(wait=True)
        for version in versions:
            self.assertTrue(all(not(s.root/version).exists() for s in [self.service(t) for t in others]))

    def test_source_preflight_rejection_releases_head_reservations(self):
        a,others,shard,history,layout=self.inputs()
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'controller')
        with patch('dhdgs.locality.source_index_bytes',return_value=10000000),patch.object(
                self.service(a),'check_growth_request',side_effect=ValueError('Injected preflight rejection')):
            with self.assertRaisesRegex(RuntimeError,'Injected preflight rejection'):
                controller.build_partitioned_online(a,history,layout)
        self.assertEqual(controller.partition_pending,{})
        self.assertFalse(controller.regions)
        self.assertIsNone(self.service(a).partition_planner.state)
        self.assertFalse(any(holds_reservation(s) for n in others for s in self.service(n).states.values()))

    def test_automatic_pipeline_installs_real_graph_without_static_mode(self):
        from dhdgs.automatic_replica import run
        a,others,shard,history,layout=self.inputs()
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'controller_auto')
        controller.b['automatic_pipeline']=dict(enabled=True,max_targets=1,max_seed_entries=4,
            planning_seconds=15.,finish_reserve_seconds=3.,min_capacity_bytes=100000)
        controller.b['max_load_age_ms']=10000
        obs=[dict(source=a,entry=e,query_id=i,replica=None,branches=[dict(node=a,cpu_ms=w)])
             for i,(e,w)in enumerate(zip(history['hot_ids'],history['weights']))]
        controller.b['min_entry_queries']=1
        controller.b['min_group_queries']=len(history['hot_ids'])
        with patch('dhdgs.locality.source_index_bytes',return_value=10000000),patch.object(controller,'control'),patch.object(controller,'pair_eligible',return_value=True):
            controller.observe();controller.observe()
            controller.b['mode']='dynamic'
            try:report=run(controller,a,others[0],obs)
            finally:controller.b['mode']='static'
        self.assertEqual(len(report['publications']),1)
        self.assertFalse(controller.partition_pending)
        self.assertTrue(any(e['event']=='AUTO_PIPELINE_COMPLETED'for e in controller.events))
        for r in controller.regions.values():self.assertEqual(self.rpc(r['target'],'status',id=r['id'])['phase'],'READY')
        controller.retire(wait=True)

    def test_rejected_ready_publication_reclaims_graph_and_reservations(self):
        a,others,shard,history,layout=self.inputs()
        layout['target_weights']={str(others[0]):1.}
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'controller_gate')
        checks=[]
        def gate(plan):
            checks.append(plan['target'])
            if len(checks)==2:
                # A target may lose its headroom while copying. No head route
                # has published, even though its immutable graph is READY.
                self.assertFalse(controller.regions)
                self.assertEqual(self.rpc(plan['target'],'status',id=plan['_stream_version'])['phase'],'READY')
                raise ValueError('Target pressure changed after READY')
        with patch('dhdgs.locality.source_index_bytes',return_value=10000000):
            with self.assertRaisesRegex(ValueError,'pressure changed after READY'):
                controller.build_partitioned_online(a,history,layout,_publication_gate=gate)
        self.assertEqual(len(checks),2)
        self.assertFalse(controller.regions)
        self.assertFalse(controller.partition_pending)
        self.assertIsNone(controller.in_progress)
        self.assertFalse(any(holds_reservation(s) for s in self.service(others[0]).states.values()))
        self.assertFalse(list(self.service(a).root.glob('*_source')))

    def test_admission_failure_reclaims_staging_and_capacity(self):
        a,others,shard,history,layout=self.inputs()
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'controller')
        from dhdgs.replica_growth import GrowingReplica
        admit=GrowingReplica.admit
        def bad(stream,ids):
            admit(stream,ids)
            raise ValueError('Injected expansion failure')
        with patch('dhdgs.locality.source_index_bytes',return_value=10000000),patch.object(GrowingReplica,'admit',bad):
            with self.assertRaisesRegex(RuntimeError,'Injected expansion failure'):
                controller.build_partitioned_online(a,history,layout)
        self.assertFalse(controller.regions);self.assertFalse(controller.partition_pending)
        for node in others:
            self.assertFalse(any(holds_reservation(s) for s in self.service(node).states.values()))
        self.assertFalse(list(self.service(a).root.glob('*_source')))

    def test_growing_protocol_rejects_holes_oversized_chunks_and_conflicting_seal(self):
        a,others,shard,history,layout=self.inputs();target=others[0];m=shard.meta
        from dhdgs.replicas import CONTRACT,digest
        self.rpc(target,'pipeline_begin',id='manual_growth',contract={k:m[k] for k in CONTRACT},
            centers_sha256=digest(shard.directory/'centers.npy'),reserve_bytes=100000,record_order='admission_v1')
        self.rpc(target,'pipeline_open',id='manual_growth',file='nodes.bin',bytes=0)
        def send(off,data):return self.rpc(target,'pipeline_chunk',id='manual_growth',file='nodes.bin',offset=off,
            data=data,sha256=hashlib.sha256(data).hexdigest())
        with self.assertRaisesRegex(RuntimeError,'range'):send(0,b'x'*4097)
        with self.assertRaisesRegex(RuntimeError,'reservation'):send(102400,b'x'*4096)
        raw=os.pread(shard.fd,m['header_bytes']+m['record_bytes'],0)
        # Small fixture records leave an intentionally incomplete first chunk.
        header=HEADER.pack(MAGIC,1,m['dimension'],1,m['record_bytes'])+bytes(m['header_bytes']-HEADER.size)
        with self.assertRaisesRegex(RuntimeError,'incomplete'):
            self.rpc(target,'pipeline_seal_nodes',id='manual_growth',bytes=len(raw),header=header)
        for off in range(0,len(raw),4096):send(off,raw[off:off+4096])
        self.rpc(target,'pipeline_seal_nodes',id='manual_growth',bytes=len(raw),header=header)
        self.rpc(target,'pipeline_seal_nodes',id='manual_growth',bytes=len(raw),header=header)
        with self.assertRaisesRegex(RuntimeError,'Conflicting'):
            self.rpc(target,'pipeline_seal_nodes',id='manual_growth',bytes=len(raw)+1,header=header)
        self.rpc(target,'pipeline_abort',id='manual_growth')
        self.assertTrue(self.rpc(target,'reap',id='manual_growth')['reclaimed'])
