"""Decay, bounded asynchronous aggregation and region-exclusion semantics."""
import copy
from collections import Counter
import json
import struct
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import numpy as np
import yaml
from dhdgs.index import native, Shard
from dhdgs.native_search import prepare_shard, run_native
from dhdgs.replicas import package
from dhdgs.load_balance import candidate
from test_native_search import NativeSearchTests
from test_cpp_runtime import NativeRuntimeTests
from test_replicas import ReplicaTests
from dhdgs.search import Engine

ROOT=Path(__file__).resolve().parents[1]
def heat_config(**kw):
    c=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())['hotness']
    return dict(c,max_candidates=256,top_k=16,merge_interval_ms=5.,**kw)
def await_merged(h,n):
    deadline=time.monotonic()+3
    while time.monotonic()<deadline:
        s=h.snapshot()
        if s.get('events_merged',0)>=n:return s
        time.sleep(.005)
    raise AssertionError(h.snapshot())

class HotnessTests(unittest.TestCase):
    def test_space_saving_bounds_contain_true_counts_after_eviction(self):
        c=heat_config(half_life_ms=1e12)
        c.update(max_candidates=8,top_k=8)
        h=native().DecayedHotness(c);self.addCleanup(h.close)
        stream=[v for v in range(100) for _ in range(1+v%7)]+[999]*100
        exact=Counter(stream);h.record(stream);h.flush();s=await_merged(h,len(stream))
        self.assertGreater(s['evictions'],0);self.assertEqual(s['events_dropped'],0)
        # The enormous half-life makes decay below this tolerance; checking
        # against the independent event stream also covers weighted batches.
        for row in s['top']:
            self.assertLessEqual(row['lower_bound'],exact[row['id']]+1e-5)
            self.assertGreaterEqual(row['score']+1e-5,exact[row['id']])

    def test_exact_decay_without_eviction_and_cold_hot_transition(self):
        h=native().DecayedHotness(heat_config(half_life_ms=100.));self.addCleanup(h.close)
        h.record([7]*20);h.flush();a=await_merged(h,20)
        time.sleep(.12);b=h.snapshot()
        va=next(v['score'] for v in a['top'] if v['id']==7);vb=next(v['score'] for v in b['top'] if v['id']==7)
        expected=va*2**(-(b['time_ns']-a['time_ns'])/1e8)
        self.assertAlmostEqual(vb,expected,places=7)
        h.record([8]*20);h.flush();s=await_merged(h,40)
        self.assertEqual(s['top'][0]['id'],8)
        self.assertEqual(s['events_dropped'],0)
        self.assertTrue(all(v['error']==0 for v in s['top']))

    def test_concurrent_thread_local_aggregation(self):
        h=native().DecayedHotness(heat_config(half_life_ms=10000.));self.addCleanup(h.close)
        def work(i):
            for _ in range(10):h.record([i]*30+[99]*10)
            h.flush()
        with ThreadPoolExecutor(8) as pool:list(pool.map(work,range(8)))
        s=await_merged(h,3200)
        self.assertEqual(s['events_recorded'],3200);self.assertEqual(s['events_submitted'],3200)
        self.assertEqual(s['events_dropped'],0);self.assertEqual(s['top'][0]['id'],99)
        self.assertLessEqual(s['lanes'],8);self.assertLess(s['merge_updates'],s['events_recorded'])

    def test_bounded_candidates_and_overflow_is_visible(self):
        c=heat_config(half_life_ms=10000.);c.update(max_candidates=8,top_k=4)
        h=native().DecayedHotness(c);self.addCleanup(h.close)
        h.record(list(range(100)));h.flush();s=await_merged(h,100)
        self.assertEqual(s['candidates'],8);self.assertEqual(len(s['top']),4)
        self.assertEqual(s['evictions'],92)
        self.assertTrue(all(0<=v['lower_bound']<=v['score'] for v in s['top']))
        c.update(local_capacity=1,queue_batches=1,merge_interval_ms=1000.)
        slow=native().DecayedHotness(c);self.addCleanup(slow.close)
        slow.record(list(range(100)));slow.flush();slow.close();s=slow.snapshot()
        self.assertGreater(s['events_dropped'],0)
        self.assertEqual(s['events_merged']+s['events_dropped'],100)

    def test_hotness_ranks_worksets_without_changing_required_dependencies(self):
        observations=[dict(source=0,replica=None,query_id=i,entry=e,score=1.,branches=[dict(node=0,expanded_ids=[e,e+1],cpu_ms=cpu)]) for i,e,cpu in [(1,10,100),(2,20,1)]]
        cfg=dict(min_entry_queries=1,min_coverage=.5,max_core_rows=2,min_group_queries=1)
        self.assertEqual(candidate(observations,0,1,cfg)['entries'],[10])
        hot=candidate(observations,0,1,cfg,{20:50,21:50,10:1})
        self.assertEqual(hot['entries'],[20]);self.assertEqual(hot['ids'],[20,21])

class LogicalRegionTests(unittest.TestCase):
    setUpClass=classmethod(NativeSearchTests.setUpClass.__func__)
    tearDownClass=classmethod(NativeSearchTests.tearDownClass.__func__)
    state=NativeSearchTests.state
    base_setup=NativeSearchTests.setUp
    def setUp(self):
        self.base_setup()
        self.c['peer'].update(allow_node_revisit=True,logical_region_visits=True,max_excluded_region_ids=1000,
                             min_expansions_after_cross=1,cross_on_local_exhaustion=True)
        self.c['search'].update(local_beam=1,max_expansions=64)
        self.b=Shard(self.root/'out/index/shard_1',0);self.addCleanup(self.b.close)
        prepare_shard(self.b,self.directory)
        own=[int(v) for v in self.shard.ids if 1 not in self.directory[int(v)]]
        self.alpha=own[:3];self.a_seed=own[3]
        self.b_seed=next(int(v) for v in self.b.ids if int(v) not in self.alpha)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        settings=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())['load_balance']
        settings.update(halo_hops=0,max_region_rows=100,max_region_bytes=1<<24,max_code_bytes=1<<24,reserve_disk_bytes=1<<20)
        path=Path(self.temp.name)/'alpha';package(self.shard,self.alpha,[self.alpha[0]],path,settings)
        self.replica=Shard(path,0);self.addCleanup(self.replica.close)
        self.b.native_search.install_replica('alpha',self.replica,np.load(path/'code_membership.npy'))
    def row(self,gid,d=0.):return [gid,d,0.,self.directory[gid],None,0,0,gid,0.]
    def root_state(self,target):
        s=self.state();s.update(target_node=1,entry_node=1,visited_nodes=[1],work_grant=64,branch_slots=2,
             replica_version='alpha',replica_access_kind='entry',seeds=[self.row(self.alpha[0])],seen=[self.alpha[0]],
             remote_frontier=[self.row(target,-1e20)])
        s['seeds'][0][3]=[0,1]
        return s
    def run_branch(self,s,shard):
        children=[];r=run_native(s,shard,self.directory,self.c,lambda c,d:children.append((c,d)),lambda:False)
        return r,children
    def test_alpha_to_source_to_host_base_and_all_alpha_ids_excluded(self):
        root,children=self.run_branch(self.root_state(self.a_seed),self.b)
        self.assertEqual(len(children),1);a=children[0][0]
        self.assertEqual(a['target_node'],0);self.assertEqual(a['visited_regions'],['replica:alpha','base:0'])
        self.assertNotIn('replica_version',a)
        self.assertEqual(list(struct.unpack('<'+'Q'*(len(a['excluded_region_ids'])//8),a['excluded_region_ids'])),sorted(self.alpha))
        self.assertGreater(root['stats']['region_unexpanded_at_handoff'],0)
        self.assertTrue(set(root['expanded_ids'])<=set(self.alpha))
        a['remote_frontier']=[self.row(self.b_seed,-1e20)]
        ar,children=self.run_branch(a,self.shard)
        self.assertFalse(set(ar['expanded_ids'])&set(self.alpha));self.assertEqual(children[0][0]['target_node'],1)
        b=children[0][0];br,rest=self.run_branch(b,self.b)
        self.assertFalse(rest);self.assertNotIn('replica_version',b)
        self.assertEqual(b['visited_regions'],['replica:alpha','base:0','base:1'])
        self.assertFalse(set(br['expanded_ids'])&set(self.alpha))
        self.assertTrue(native().validate_manifests([root,ar,br],root['branch_id'],1,64,2,3,True))
    def test_same_host_region_transition_is_distinct_and_validated(self):
        s=self.root_state(self.b_seed);s['branch_slots']=1
        r,children=self.run_branch(s,self.b);b=children[0][0]
        self.assertEqual(b['visited_nodes'],[1,1]);self.assertEqual(b['visited_regions'],['replica:alpha','base:1'])
        br,_=self.run_branch(b,self.b)
        self.assertTrue(native().validate_manifests([r,br],r['branch_id'],1,64,1,3,True))
        bad=copy.deepcopy(br);bad['visited_regions'][-1]='replica:alpha'
        with self.assertRaises(ValueError):native().validate_manifests([r,bad],r['branch_id'],1,64,1,3,True)
    def test_malformed_exclusions_are_not_silently_truncated(self):
        s=self.root_state(self.a_seed);s['excluded_region_ids']=b'bad'
        with self.assertRaises(ValueError):self.run_branch(s,self.b)

class HotnessRuntimeTests(unittest.TestCase):
    setUpClass=classmethod(NativeRuntimeTests.setUpClass.__func__)
    tearDownClass=classmethod(NativeRuntimeTests.tearDownClass.__func__)
    config=NativeRuntimeTests.config
    start=NativeRuntimeTests.start
    def test_successful_expansion_counts_and_private_validation_exclusion(self):
        c=self.config(True,True);c['hotness']=heat_config(half_life_ms=10000.)
        c['peer'].update(deduplicate_node_expansions=True,allow_node_revisit=True,logical_region_visits=True,
                         max_excluded_region_ids=1000,min_expansions_after_cross=4,cross_on_local_exhaustion=True)
        self.start(c);engine=Engine(c);self.addCleanup(engine.close)
        rows=[engine.search(x,i)for i,x in enumerate(self.x[:12])]
        self.assertTrue(all(q['status']=='ok' for q in rows))
        for q in rows:
            self.assertEqual(q['stats']['hotness_expansions_recorded'],q['stats']['expanded'])
            for r in q['branch_reports']:self.assertEqual(r['logical_region'],f"base:{r['node_id']}")
        info=engine.runtime.route_info(self.x[0].astype(float),0)
        private=engine.runtime.validate_replica(self.x[0].astype(float),0,dict(entry_id=info['entry_id'],source=info['source'],target=info['source'],version=''))
        self.assertEqual(private['status'],'ok');self.assertEqual(private['stats']['hotness_expansions_recorded'],0)

class LogicalReplicaRuntimeTests(unittest.TestCase):
    setUpClass=classmethod(ReplicaTests.setUpClass.__func__)
    tearDownClass=classmethod(ReplicaTests.tearDownClass.__func__)
    setUp=ReplicaTests.setUp
    rpc=ReplicaTests.rpc
    info=ReplicaTests.info
    def configure_replica_test(self,c):
        c['peer'].update(deduplicate_node_expansions=True,allow_node_revisit=True,logical_region_visits=True,
                         max_excluded_region_ids=1000,min_expansions_after_cross=1,cross_on_local_exhaustion=True)
        c['hotness']=heat_config(half_life_ms=10000.)
    def test_real_runtime_region_isolation_and_physical_revisit(self):
        info=self.info();a=info['source'];b=(a+1)%3
        entries=sorted({self.info(i)['entry_id'] for i in range(len(self.x)) if self.info(i)['source']==a})
        self.rpc(a,'source_start',id='isolated',plan=dict(ids=entries,entries=entries,target=b))
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            s=self.rpc(a,'status',id='isolated')
            if s['phase']=='FAILED':self.fail(s.get('error',str(s)))
            if s['phase']=='SENT':
                target_status=self.rpc(b,'status',id='isolated')
                if target_status['phase']=='FAILED':self.fail(target_status.get('error',str(target_status)))
                if target_status['phase']=='READY':break
            time.sleep(.01)
        else:self.fail('Replica did not become ready')
        self.engine.runtime.replica_routes([dict(source=a,target=b,version='isolated',entries=entries,fraction=1.,min_score=-1e300,max_score=1e300,ttl_ms=10000)])
        rows=[self.engine.search_async(x,i).result(5)for i,x in enumerate(self.x)]
        hits=[q for q in rows if q['stats'].get('replica_version')=='isolated'];self.assertTrue(hits)
        for q in hits:
            self.assertEqual(q['status'],'ok');self.assertLessEqual(q['stats']['expanded'],256)
            root=next(r for r in q['branch_reports'] if r['parent_branch_id'] is None)
            self.assertEqual(root['logical_region'],'replica:isolated');self.assertTrue(set(root['expanded_ids'])<=set(entries))
            for r in q['branch_reports']:
                if r is not root:self.assertFalse(set(r['expanded_ids'])&set(entries))
        self.assertTrue(any(q['stats']['node_revisit_count'] for q in hits))
        self.assertTrue(any(q['stats']['same_host_region_handoffs'] for q in hits))
        for q in rows:
            reports=q['branch_reports'];nodes={r['branch_id']:r['node_id'] for r in reports}
            crossings=sum(r['parent_branch_id'] is not None and nodes[r['parent_branch_id']]!=r['node_id'] for r in reports)
            self.assertEqual(q['stats']['server_to_server_branches'],crossings)
            self.assertEqual(q['stats']['same_host_branches'],len(reports)-1-crossings)
        expected={n:sum(r['work_spent']for q in rows for r in q['branch_reports'] if r['node_id']==n)for n in range(3)}
        deadline=time.monotonic()+3
        while time.monotonic()<deadline:
            counts={n:self.rpc(n,'hotness')for n in range(3)}
            if all(counts[n].get('events_merged')==expected[n]for n in range(3)):break
            time.sleep(.01)
        for n in range(3):
            self.assertEqual(counts[n]['events_merged'],expected[n]);self.assertEqual(counts[n]['events_dropped'],0)
        self.engine.runtime.replica_routes([]);self.rpc(b,'retire',id='isolated')
        self.assertTrue(self.rpc(b,'reap',id='isolated')['reclaimed'])

del NativeSearchTests,NativeRuntimeTests,ReplicaTests
