import copy,json,os,struct,tempfile,time,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import numpy as np
from dhdgs.boundary_portals import BoundaryGraph,verify,validate_config,augment
from dhdgs.community import _verify as verify_plan
from dhdgs.replica_budget import package_limit,region_limit

class BoundaryPortalTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);p=Path(self.tmp.name)
  ids=np.array([10,20,30,40,50],dtype=np.uint64);codes=np.array([10,20,30,40,50,60],dtype=np.uint64)
  adj={10:[20],20:[30],30:[60],40:[10],50:[60]};m=dict(shard_id=0,epoch='fixed',dimension=2,record_bytes=64,header_bytes=32,max_degree=2)
  with (p/'nodes.bin').open('wb')as f:
   f.write(bytes(32))
   for id in ids:
    edges=adj[int(id)];raw=struct.pack('<ffI',float(id),0.,len(edges))+np.array(edges,dtype='<u8').tobytes();f.write(raw+bytes(64-len(raw)))
  membership=np.array([[1,0,0]]*5+[[0,1,1]],dtype=bool);np.save(p/'code_membership.npy',membership);np.save(p/'centers.npy',np.zeros((3,2),dtype=np.float32));(p/'rotation.bin').write_bytes(b'rotation')
  fd=os.open(p/'nodes.bin',os.O_RDONLY);self.addCleanup(os.close,fd)
  self.shard=SimpleNamespace(directory=p,ids=ids,code_ids=codes,meta=m,fd=fd,location={int(v):i for i,v in enumerate(ids)},codes=np.zeros((6,1),np.uint8),extra=np.zeros((6,0),np.uint8),factors=np.zeros((6,3),np.float32),center_ids=np.zeros(6,np.uint32))
  self.cfg=dict(enabled=True,mode='representative',per_direction=1,projection_bits=2,seed=7,max_connector_hops=3,max_extra_bytes=1000000,max_plan_seconds=10.,scan_block_rows=2)
  self.b=dict(boundary_augmentation=self.cfg,max_region_bytes=4000000,max_region_rows=10,locality={'source_capacity_fraction':.3},capacity_policy=dict(mode='per_region',small_alpha=.3,large_alpha=.8),max_code_bytes=4000000)
  self.base=dict(ids=[10],entries=[10],region_proof={'kind':'fixture'},reserved_package_bytes=100)
  # Base certificate algorithms are covered by the existing community suite.
  self.patch=patch('dhdgs.community.verify',return_value=dict(verified=True));self.patch.start();self.addCleanup(self.patch.stop)
  self.capacity_patch=patch('dhdgs.locality.source_index_bytes',return_value=1000000);self.capacity_patch.start();self.addCleanup(self.capacity_patch.stop)
 def test_directed_connector_portal_and_unreachable_no_false_shortcut(self):
  before=(self.shard.directory/'nodes.bin').read_bytes();g=BoundaryGraph(self.shard,self.cfg);out=g.augment(self.base,self.b)
  self.assertEqual(out['ids'],[10,20,30]);self.assertEqual(out['entries'],[10]);self.assertEqual(out['region_proof']['portals'],[30]);self.assertEqual(out['stats']['missing_boundary_unreachable'],1)
  self.assertEqual(out['region_proof']['connectors'],[[20,10],[30,20]]);self.assertEqual((self.shard.directory/'nodes.bin').read_bytes(),before)
  self.assertTrue(verify(self.shard,out,self.b)['verified'])
 def test_forged_directed_path_wrong_epoch_and_members_rejected(self):
  out=BoundaryGraph(self.shard,self.cfg).augment(self.base,self.b)
  bad=copy.deepcopy(out);bad['region_proof']['connectors'][1]=[30,10]
  with self.assertRaisesRegex(ValueError,'original directed'):verify(self.shard,bad,self.b)
  bad=copy.deepcopy(out);bad['region_proof']['epoch']='wrong'
  with self.assertRaisesRegex(ValueError,'epoch'):verify(self.shard,bad,self.b)
  bad=copy.deepcopy(out);bad['ids'].append(50)
  with self.assertRaisesRegex(ValueError,'members'):verify(self.shard,bad,self.b)
 def test_alpha_separate_but_total_and_rows_remain_hard(self):
  out=BoundaryGraph(self.shard,self.cfg).augment(self.base,self.b)
  self.assertGreater(out['reserved_package_bytes'],region_limit(self.b,100000))
  self.assertEqual(package_limit(self.b,100000,out),1030000)
  b=copy.deepcopy(self.b);b['max_region_rows']=2
  with self.assertRaisesRegex(ValueError,'row hard'):BoundaryGraph(self.shard,self.cfg).augment(self.base,b)
  b=copy.deepcopy(self.b);b['max_region_bytes']=100
  with self.assertRaisesRegex(ValueError,'budget|core exceeds alpha'):BoundaryGraph(self.shard,self.cfg).augment(self.base,b)
 def test_all_mode_still_requires_paths_and_config_validation(self):
  c=dict(self.cfg,mode='all');out=BoundaryGraph(self.shard,c).augment(self.base,dict(self.b,boundary_augmentation=c));self.assertEqual(out['ids'],[10,20,30])
  for bad in [dict(c,projection_bits=0),dict(c,max_connector_hops=0),dict(c,mode='unlimited'),dict(c,max_extra_bytes=1)]:
   with self.assertRaises(ValueError):validate_config(bad)
 def test_certified_and_active_entries_are_bound_separately(self):
  base=dict(self.base,ids=[10,20],entries=[10],certified_entries=[10,20])
  out=BoundaryGraph(self.shard,self.cfg).augment(base,self.b)
  self.assertEqual(out['entries'],[10]);self.assertEqual(out['region_proof']['base']['certified_entries'],[10,20])
  self.assertTrue(verify_plan(self.shard,out,self.b)['verified'])
  bad=copy.deepcopy(out);bad['certified_entries']=[10]
  with self.assertRaisesRegex(ValueError,'entries mismatch'):verify(self.shard,bad,self.b)
 def test_enclosing_planning_deadline_is_respected(self):
  b=dict(self.b,_planning_deadline=time.monotonic()-1)
  with self.assertRaises(TimeoutError):augment(self.shard,self.base,b)
 def test_actual_core_cannot_bypass_alpha_with_underdeclared_reservation(self):
  b=copy.deepcopy(self.b);b['capacity_policy']['small_alpha']=.00001
  with self.assertRaisesRegex(ValueError,'Actual original core'):BoundaryGraph(self.shard,self.cfg).augment(dict(self.base,reserved_package_bytes=1),b)
 def test_original_tier_cannot_be_reclassified_to_bypass_core_alpha(self):
  out=BoundaryGraph(self.shard,self.cfg).augment(self.base,self.b);out['replica_tier']='large'
  with self.assertRaisesRegex(ValueError,'tier changed'):verify(self.shard,out,self.b)
 def test_non_augmented_legacy_package_keeps_its_original_limit(self):
  for plan in [{},{'region_proof':None}]:
   self.assertEqual(package_limit(self.b,100000,plan),region_limit(self.b,100000))
