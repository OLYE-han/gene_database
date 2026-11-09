import copy,json,hashlib,os,struct,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import numpy as np
import yaml
from dhdgs.index import native
from dhdgs.geometry import exact_minimum,small_group_distances,build_directory,Directory,select,verify

ROOT=Path(__file__).resolve().parents[1]
class GeometryTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);p=Path(tmp.name)
        self.ids=np.array([10,20,30,40,50],np.uint64);self.points=np.array([[0,0],[1,0],[2,0],[4,0],[8,0]],np.float32)
        self.m=dict(epoch='fixture',shard_id=0,dimension=2,metric='l2',rows=5,record_bytes=64,header_bytes=64,max_degree=3,
                    community_code_bytes=16,community_fixed_bytes=1024)
        self.graph={10:[20],20:[30],30:[40],40:[50],50:[10,30]}
        self.p=p;self.write_graph();fd=os.open(p/'nodes.bin',os.O_RDONLY);self.addCleanup(os.close,fd)
        self.shard=SimpleNamespace(directory=p,ids=self.ids,meta=self.m,fd=fd)
        self.b=copy.deepcopy(yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())['load_balance'])
        self.b.update(max_region_rows=10,max_region_bytes=1000000,max_code_bytes=1000000,selection_policy='entry_geometry')
        self.b['geometry'].update(cache_directory=str(p/'directory'),cache_threads=1,cache_block_rows=2,group_size=1,seed_starts=1,buffer_factors=[0.,.15],max_plan_ms=5000)
        self.portal_ids=np.array([10,50,90],np.uint64);self.portals=np.array([[0,0],[8,0],[100,0]],np.float64)
        self.addCleanup(patch.stopall);patch('dhdgs.geometry.metadata',return_value=self.m).start()

    def write_graph(self):
        with(self.p/'nodes.bin').open('wb')as f:
            f.write(bytes(64))
            for i,vid in enumerate(self.ids):
                out=self.graph[int(vid)];raw=self.points[i].tobytes()+struct.pack('<I',len(out))+np.asarray(out,dtype='<u8').tobytes()
                f.write(raw+bytes(64-len(raw)))

    def directory(self):
        build_directory(self.shard,self.portal_ids,self.portals,self.p/'directory',self.b)
        return Directory(self.shard,self.b)

    def test_group_membership_matches_direct_full_competitor_oracle(self):
        d=self.directory()
        for factor in [0.,.15,.5]:
            got,tau=d.core([10],factor,float('inf'))
            all_d=np.linalg.norm(self.points[:,None,:]-self.portals[None,:,:],axis=2)
            expected=np.flatnonzero(all_d[:,0]-all_d.min(axis=1)<=tau)
            np.testing.assert_array_equal(got,expected)

    def test_navigation_restores_external_bridge_and_strong_paths(self):
        d=self.directory();r=d.region([10],0.,[10,50],float('inf'))
        self.assertEqual(r['ids'],list(self.ids));self.assertGreater(r['stats']['navigation_added_rows'],0)
        for root in r['entries']:
            seen={root};pending=[root]
            while pending:
                for v in self.graph[pending.pop()]:
                    if v in r['ids']and v not in seen:seen.add(v);pending.append(v)
            self.assertEqual(seen,set(r['ids']))

    def test_complete_navigation_over_cap_is_rejected_not_truncated(self):
        d=self.directory();self.b['max_region_rows']=4
        with self.assertRaisesRegex(ValueError,'budget'):d.region([10],0.,[10],float('inf'))

    def test_reused_directory_obeys_current_request_capacity(self):
        d=self.directory();strict=copy.deepcopy(self.b);strict['max_region_rows']=4
        with self.assertRaisesRegex(ValueError,'No complete geometric region'):
            select(self.shard,[10],[1.],strict,d)
        strict=copy.deepcopy(self.b);strict['max_region_bytes']=1
        with self.assertRaisesRegex(ValueError,'No complete geometric region'):
            select(self.shard,[10],[1.],strict,d)

    def test_missing_return_path_is_not_a_publishable_region(self):
        self.graph[50]=[];self.write_graph();d=self.directory()
        with self.assertRaisesRegex(ValueError,'strongly connected'):d.region([10],0.,[10],float('inf'))

    def test_epoch_and_corrupted_cache_rejected(self):
        self.directory();self.m['epoch']='new'
        with self.assertRaisesRegex(ValueError,'epoch'):Directory(self.shard,self.b)
        self.m['epoch']='fixture'
        with(self.p/'directory/distance.npy').open('r+b')as f:f.seek(-1,2);f.write(b'x')
        with self.assertRaisesRegex(ValueError,'checksum'):Directory(self.shard,self.b)

    def test_forged_digest_cannot_hide_missing_member(self):
        d=self.directory();r=select(self.shard,[10],[1.],self.b,d);verify(self.shard,r,self.b,d)
        r['ids'].remove(30);r['region_proof']['members_sha256']=hashlib.sha256(np.asarray(r['ids'],dtype='<u8').tobytes()).hexdigest()
        with self.assertRaisesRegex(ValueError,'membership/navigation'):verify(self.shard,r,self.b,d)

    def test_tree_cycle_or_nonedge_rejected(self):
        d=self.directory();forward=np.array(d.forward);forward[2]=2
        with self.assertRaises(ValueError):native().geometry_close(self.shard.fd,self.ids,self.m,np.array([2],np.uint32),np.array([0],np.uint32),forward,d.reverse,0,10,2.)

    def test_fp64_cancellation_ties_and_duplicate_sites(self):
        p=np.array([[1e10,0],[1e10+2,0],[1e10+2,0]])
        d,pos=exact_minimum(np.array([[1e10+1,0],[1e10+2,0]]),p,np.sum(p*p,axis=1))
        np.testing.assert_array_equal(pos,[0,1]);np.testing.assert_allclose(d,[1,0])

    def test_vectorized_group_guard_matches_direct_oracle(self):
        rng=np.random.default_rng(47)
        for offset in [0.,1e10]:
            x=rng.normal(size=(25,13))+offset;p=rng.normal(size=(5,13))+offset;p[4]=p[0]
            expected=np.min(np.linalg.norm(x[:,None,:]-p[None,:,:],axis=2),axis=1)
            np.testing.assert_allclose(small_group_distances(x,p,np.sum(p*p,axis=1)),expected,rtol=1e-14,atol=1e-14)

    def test_config_requires_nonnegative_finite_buffers_and_async(self):
        from dhdgs.config import validate
        c=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text());c['load_balance']['geometry']['buffer_factors']=[float('nan')]
        with self.assertRaises(ValueError):validate(c)
        c['load_balance']['geometry']['buffer_factors']=[0.15];c['load_balance']['selection_policy']='entry_geometry'
        c['load_balance']['planner_mode']='inline'
        with self.assertRaises(ValueError):validate(c)

from test_replicas import ReplicaTests
class GeometryRuntimeTests(unittest.TestCase):
    setUpClass=classmethod(ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(ReplicaTests.tearDownClass.__func__)
    setUp=ReplicaTests.setUp
    info=ReplicaTests.info
    rpc=ReplicaTests.rpc

    def configure_replica_test(self,c):
        from dhdgs.index import Shard
        from dhdgs.geometry import vectors
        b=c['load_balance']
        b.update(selection_policy='entry_geometry',planner_mode='native_async',halo_hops=0,
            portal_budget_fraction=0.,publication_mode='ready',quality_weight_routing=False,
            compute_region_quality=False,peer_routes_enabled=False,native_replica_pack=True,
            replica_transfer_mode='pipeline_v2',replica_loader='native',warm_bytes=0,
            initial_fraction=1.,max_fraction=1.)
        b['geometry'].update(cache_directory='geometry_directory',cache_threads=1,blas_threads=1,
            group_size=1,seed_starts=1,buffer_factors=[0.,.15],max_plan_ms=10000)
        root=Path(c['paths']['output'])/'index'
        ids=np.load(root/'route_ids.npy');points=self.x[ids]
        for sid in range(3):
            shard=Shard(root/f'shard_{sid}',0)
            try:
                directory=shard.directory/'geometry_directory'
                if not directory.exists():build_directory(shard,ids,points,directory,b)
            finally:shard.close()

    def test_online_plan_verify_transfer_publish_and_search(self):
        import time
        info=self.info();source=info['source'];target=(source+1)%3
        self.rpc(source,'community_plan_start',job='geometryintegration1',hot_ids=[info['entry_id']],weights=[1.])
        deadline=time.monotonic()+15
        while True:
            response=self.rpc(source,'community_plan_poll',job='geometryintegration1')
            if response['done']:region=response['region'];break
            self.assertLess(time.monotonic(),deadline);time.sleep(.01)
        self.assertEqual(region['region_proof']['kind'],'complete_entry_geometry_v1')
        plan=dict(region,source=source,target=target,training_query_ids=[],min_score=-1e300,max_score=1e300)
        controller=self.engine.start_load_balancer(self.token,Path(self.work.name)/'geometry')
        controller.stop.set();controller.thread.join();controller.stop.clear()
        controller.loads={n:dict(updated=time.monotonic(),usable=True)for n in (source,target)}
        version=controller.create(plan).result(20)
        self.assertEqual(self.rpc(target,'status',id=version)['phase'],'READY')
        result=self.engine.search_async(self.x[0],0).result(10)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(result['stats'].get('replica_version'),version)
        expected=np.argsort(np.sum((self.x.astype(float)-self.x[0])**2,axis=1))[1:6]
        self.assertEqual(set(result['ids']),set(map(int,expected)))
        controller.retire()

if __name__=='__main__':unittest.main()
