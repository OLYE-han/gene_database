import unittest
from dhdgs.replica_branch_metrics import ReplicaBranchMetrics,distribution


def branch(bid,node,parent,work=0,**kw):
    return dict(branch_id=bid,node_id=node,parent_branch_id=parent,work_spent=work,
                stats=dict(server_thread_cpu_ms=work*.1),fork_events=[],**kw)


def event(child,target,remaining=40,grant=20):
    return dict(decision_id='to-'+child,child_branch_id=child,target=target,
                remaining_before=remaining,work_grant=grant,remaining_after=remaining-grant,
                local_expansions_before=10,search_elapsed_ms=2.)


def query(job=0):
    root=branch('r',1,None,30,replica_version='v')
    root['fork_events']=[event('s',0,60,30),event('t',2,20,10),event('h',1,10,5)]
    return dict(job=job,kind='sample',status='ok',stats=dict(original_entry_node=0),
                branches=[root,branch('s',0,'r',20),branch('t',2,'r',8),branch('h',1,'r',0)])


class ReplicaBranchMetricTests(unittest.TestCase):
    def group(self,m,scope='direct',role='third_node'):
        return next(g for g in m.summary()['groups']if g['scope']==scope and g['target_role']==role)

    def test_direct_roles_frequency_and_distinct_budgets(self):
        m=ReplicaBranchMetrics();rows=m.add(query());q=query(1)
        q['branches']=q['branches'][:1];q['branches'][0]['fork_events']=[];m.add(q)
        third=self.group(m)
        self.assertEqual((third['queries'],third['events'],third['query_fraction_per_hit']),(1,1,.5))
        self.assertEqual(third['remaining_before']['mean'],20)
        self.assertEqual(third['child_grant']['mean'],10)
        self.assertEqual(third['child_work'],8)
        self.assertEqual(third['events_per_hit_distribution'],{0:1,1:1})
        self.assertFalse(next(r for r in rows if r['target_role']=='host_base')['cross_server'])

    def test_downstream_not_counted_as_direct_and_query_frequency_not_event_count(self):
        q=query();b=q['branches'][1];b['fork_events']=[event('z',2)]
        q['branches'].append(branch('z',2,'s',4));m=ReplicaBranchMetrics();m.add(q)
        self.assertEqual(self.group(m)['events'],1)
        self.assertEqual(self.group(m,'downstream')['events'],1)
        self.assertEqual(self.group(m,'any')['events'],2)
        self.assertEqual(self.group(m,'any')['query_fraction_per_hit'],1)

    def test_windows_are_not_full_query_observations(self):
        m=ReplicaBranchMetrics();m.add(dict(kind='window',count=200000));m.add(query())
        s=m.summary();self.assertEqual(s['counts']['queries'],1)
        self.assertEqual(s['counts']['window_queries'],200000);self.assertTrue(s['sampled'])

    def test_peer_replica_requires_own_source(self):
        q=query();q['branches'][0]['parent_branch_id']='ordinary'
        q['branches'].append(branch('ordinary',2,None))
        m=ReplicaBranchMetrics();m.add(q)
        self.assertEqual(m.summary()['counts']['queries_with_unknown_replica_source'],1)
        self.assertEqual(self.group(m)['events'],0)
        m=ReplicaBranchMetrics({'v':2});m.add(q)
        self.assertEqual(self.group(m)['events'],1)  # target 0 is third, not original-entry source.
        self.assertEqual(self.group(m,role='source')['child_work'],8)

    def test_pair_source_overrides_original_entry(self):
        q=query();q['branches'][0]['replica_pair_source']=2
        m=ReplicaBranchMetrics();m.add(q)
        self.assertEqual(self.group(m,role='source')['child_work'],8)

    def test_duplicate_event_and_request_rejected(self):
        q=query();m=ReplicaBranchMetrics();m.add(q)
        with self.assertRaisesRegex(ValueError,'Duplicate request'):m.add(q)
        q=query();q['branches'][0]['fork_events']*=2
        with self.assertRaisesRegex(ValueError,'Duplicate fork'):ReplicaBranchMetrics().add(q)

    def test_missing_child_is_not_executed_and_wrapper_supported(self):
        q=query();q['branches']=q['branches'][:1]
        m=ReplicaBranchMetrics();m.add(dict(job=0,transport_ok=True,result=q))
        third=self.group(m)
        self.assertEqual(third['events'],1);self.assertEqual(third['executed_queries'],0)
        self.assertEqual(third['child_reports'],0)

    def test_no_fork_is_not_confused_with_no_replica_hit(self):
        q=query();q['branches']=q['branches'][:1];del q['branches'][0]['fork_events']
        m=ReplicaBranchMetrics();m.add(q)
        self.assertEqual(m.summary()['counts']['replica_hit_queries'],1)
        self.assertEqual(m.summary()['counts']['replica_reports_without_fork_events'],1)

    def test_integer_histogram_quantiles(self):
        s=distribution({0:1,10:8,100:1})
        self.assertEqual((s['mean'],s['p50'],s['p95'],s['max']),(18.,10,100,100))
        self.assertIsNone(distribution({})['p95'])


if __name__=='__main__':unittest.main()
