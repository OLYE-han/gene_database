"""Relief policy regression: no duplicate entry ownership, shared capacity."""
import copy,threading,unittest
from collections import defaultdict
from test_replicas import ControllerPolicyTests,ReplicaTests
from dhdgs.load_balance import candidate

class ReliefPolicyTests(ControllerPolicyTests):
    def test_multiple_regions_share_target_headroom(self):
        x=self.controller();now=100
        x.loads={0:dict(util=.99,queue_ms=80,updated=now,usable=True,workers=64),
                 1:dict(util=.80,queue_ms=0,updated=now,usable=True,workers=64)}
        x.regions={str(i):dict(id=str(i),source=0,target=1,entries=[i],fraction=.1,last_use=now,published=now,
                   validation=dict(source_saved_s=.01,target_added_s=.01)) for i in range(2)}
        for i in range(2):x.rates[i]=10000
        old=sum(r['fraction']*100 for r in x.regions.values());x.adjust(now)
        delta=sum(r['fraction']*100 for r in x.regions.values())-old
        self.assertLessEqual(delta,(x.b['target_util_limit']-.80)*64+1e-9)

    def test_second_region_can_trigger_but_capacity_is_enforced(self):
        x=self.controller();x.b['max_replicas']=2;x.b['sustained_windows']=1
        x.active=dict(id='first',source=0,target=1,entries=[10])
        x.loads={0:dict(util=.99,queue_ms=90,updated=10,usable=True),1:dict(util=.1,queue_ms=0,updated=10,usable=True)}
        x.control(10);self.assertEqual(x.created,dict(source=0,target=1))
        x.in_progress=None;x.regions['second']=dict(id='second');del x.created;x.control(10)
        self.assertFalse(hasattr(x,'created'))

    def test_cpu_policy_prioritizes_saved_work_over_vertex_frequency(self):
        rows=[dict(source=0,replica=None,query_id=i,entry=i,score=1.,branches=[dict(node=0,expanded_ids=[i],cpu_ms=cost)]) for i,cost in [(1,1.),(2,9.)]]
        b=dict(min_entry_queries=1,min_coverage=.5,max_core_rows=1,min_group_queries=1,selection_policy='cpu_worksets')
        p=candidate(rows,0,1,b,{1:1000,2:1});self.assertEqual(p['entries'],[2])

class ReliefNativeTests(ReplicaTests):
    def test_observation_baseline_and_route_funnel(self):
        result=self.engine.search_async(self.x[0],0).result(10)
        self.assertEqual(result['status'],'ok')
        observations=self.engine.runtime.replica_observations()
        self.assertTrue(observations);self.assertEqual(observations[-1]['result_ids'],result['ids'])
        f=self.engine.runtime.metrics()['replica_routing_funnel']
        self.assertGreater(f['queries'],0);self.assertEqual(f['selected'],0)

class NativePackingTests(ReplicaTests):
    def test_native_pack_preserves_records_halo_and_dependencies(self):
        from dhdgs.replicas import package
        from dhdgs.index import Shard,native
        from pathlib import Path
        import numpy as np
        shard=Shard(Path(self.c['paths']['output'])/'index/shard_0',0)
        self.addCleanup(shard.close)
        settings=copy.deepcopy(self.c['load_balance']);settings.update(max_region_rows=40,max_core_rows=20,halo_hops=1)
        ids=list(map(int,shard.ids[:10]));entries=ids[:3]
        a=Path(self.work.name)/'python_pack';b=Path(self.work.name)/'native_pack'
        left=package(shard,ids,entries,a,settings);settings['native_replica_pack']=True
        right=package(shard,ids,entries,b,settings)
        self.assertEqual(left['files'],right['files']);self.assertEqual(left['quality'],right['quality'])
        replica=Shard(b,0);self.addCleanup(replica.close)
        n=native().replica_validate_records(replica.fd,replica.ids,replica.code_ids,replica.meta['dimension'],replica.meta['record_bytes'],replica.meta['header_bytes'],replica.meta['max_degree'])
        self.assertEqual(n,len(replica.ids)*replica.meta['record_bytes'])
        with self.assertRaises(ValueError):native().replica_validate_records(replica.fd,replica.ids,np.array([],dtype='uint64'),replica.meta['dimension'],replica.meta['record_bytes'],replica.meta['header_bytes'],replica.meta['max_degree'])

from test_hotness_logical import LogicalRegionTests
class ReplicaBudgetTests(LogicalRegionTests):
    def test_replica_budget_remains_conserved_with_larger_local_share(self):
        self.c['peer']['replica_child_budget_fraction']=.2
        root,children=self.run_branch(self.root_state(self.a_seed),self.b)
        self.assertTrue(children)
        self.assertEqual(root['work_spent']+root['work_unused']+sum(c[0]['work_grant'] for c in children),64)
        self.assertLessEqual(children[0][0]['work_grant'],13)

class ProgressivePolicyTests(ControllerPolicyTests):
    def test_cached_independent_validation_publishes_partial_batches(self):
        import numpy as np
        from types import SimpleNamespace
        from collections import deque
        x=self.controller();x.b.update(reuse_observed_baseline=True,validation_publish_batch=2,max_validation_queries=10,
                                     min_validation_queries=2,min_entry_validation_queries=1,validation_workers=2,shadow_overlap_min=.95)
        x.c={'search':{'top_k':2}}
        x.history=deque(dict(query_id=i,entry=i,source=0,replica=None,query_f32=np.zeros(64,dtype='<f4').tobytes(),result_ids=[i,i+10],branches=[dict(node=0,cpu_ms=10.)]) for i in range(6))
        def validate(q,qid,d):
            self.assertEqual(d['target'],1) # No duplicate baseline search.
            return dict(status='ok',ids=[qid,qid+10],branch_reports=[dict(node_id=1,stats={'server_thread_cpu_ms':5.},parent_branch_id=None,work_spent=100,children=[])])
        x.engine=SimpleNamespace(runtime=SimpleNamespace(validate_replica=validate));updates=[]
        v=x.validate_shadow(dict(source=0,target=1,entries=list(range(6)),training_query_ids=[0,1]),'r',lambda p:updates.append(p))
        self.assertEqual(v['count'],4);self.assertEqual([u['count'] for u in updates],[2,4])
        self.assertEqual(set(v['qualified_entries']),{2,3,4,5})
