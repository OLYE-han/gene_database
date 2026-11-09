import copy
import unittest
from pathlib import Path
import yaml
from dhdgs.layout_policy import plan_layout


class LayoutPolicyTests(unittest.TestCase):
    def setUp(self):
        self.c=yaml.safe_load((Path(__file__).resolve().parents[1]/'configs/dhdgs.yaml').read_text())['load_balance']['layout_policy']
        self.o=dict(observed_ms=100000,healthy=True,resources={'cpu:0':1.,'cpu:1':1.,'cpu:2':1.},
            source_resource='cpu:0',entries=[dict(id='hot',fraction=1.,costs={'cpu:0':.001,'cpu:1':.00001,'cpu:2':.00001})],
            source_bytes=1000,node_free_bytes={'1':1000,'2':1000},benefit_horizon_s=1000,offered_qps=10000,
            source_runnable_p95_ms=10.,target_runnable_p95_ms={'1':0.,'2':0.},sustained_windows=1,recurring_windows=0)
        self.a=dict(id='single',kind='single',tier='small',physical_bytes_by_node={'1':100},peak_bytes_by_node={'1':100},
            qualified=True,cost_observed_ms=100000,build_seconds=1.,build_source_work=.1,build_lost_queries=0,
            routes=[dict(entry='hot',target=1,max_share=1.,recall=.96,costs={'cpu:0':.0001,'cpu:1':.001,'cpu:2':.00001})])

    def plan(self,plans=None):return plan_layout(self.o,plans or [self.a],self.c,100000)

    def test_identical_copies_help_only_when_target_is_limited(self):
        b=copy.deepcopy(self.a);b.update(id='twins',kind='identical_copies',physical_bytes_by_node={'1':100,'2':100},peak_bytes_by_node={'1':100,'2':100})
        b['routes'].append(dict(entry='hot',target=2,max_share=1.,recall=.96,costs={'cpu:0':.0001,'cpu:1':.00001,'cpu:2':.001}))
        out=self.plan([self.a,b]);self.assertEqual(out['selected'],'twins')
        shares=out['candidates'][1]['shares'];self.assertLessEqual(sum(x['fraction']for x in shares),1.0000001)
        # Both targets now have abundant capacity. Identical content cannot
        # remove more source work than serving the same cohort once.
        self.o['resources'].update({'cpu:1':100,'cpu:2':100})
        rows=self.plan([self.a,b])['candidates']
        self.assertAlmostEqual(rows[0]['predicted']['qps_capacity'],rows[1]['predicted']['qps_capacity'])

    def test_aggregate_and_transition_bytes_count_repeated_copies(self):
        for physical,peak in [({'1':200,'2':200},{'1':200,'2':200}),({'1':200},{'1':400})]:
            a=copy.deepcopy(self.a);a.update(physical_bytes_by_node=physical,peak_bytes_by_node=peak)
            out=self.plan([a]);self.assertIsNone(out['selected'])
            self.assertIn('aggregate_transition_capacity',out['candidates'][0]['reasons'])

    def test_large_requires_persistence_or_recurrence(self):
        self.a.update(tier='large',kind='partitioned_large')
        self.assertIn('hotspot_not_persistent',self.plan()['candidates'][0]['reasons'])
        self.o['recurring_windows']=self.c['large_recurring_windows']
        self.assertEqual(self.plan()['selected'],'single')

    def test_recall_and_target_queue_are_hard_gates(self):
        self.a['routes'][0]['recall']=.94;self.o['target_runnable_p95_ms']['1']=100
        out=self.plan();self.assertIsNone(out['selected'])
        self.assertIn('route_recall',out['candidates'][0]['reasons']);self.assertIn('target_pressure:1',out['candidates'][0]['reasons'])

    def test_short_hotspot_does_not_amortize_copy(self):
        self.o['benefit_horizon_s']=.5
        self.assertIn('not_amortized',self.plan()['candidates'][0]['reasons'])

    def test_stale_or_unhealthy_observations_hold(self):
        self.o['observed_ms']=0;self.assertEqual(self.plan()['reason'],'stale_observation')
        self.o['observed_ms']=100000;self.o['healthy']=False
        self.assertEqual(self.plan()['reason'],'unhealthy_observation')

    def test_missing_cohort_or_cost_is_not_zero_filled(self):
        self.o['entries'][0]['fraction']=.5
        with self.assertRaisesRegex(ValueError,'cohort'):self.plan()
        self.o['entries'][0]['fraction']=1.;del self.a['routes'][0]['costs']['cpu:2']
        with self.assertRaisesRegex(ValueError,'Incomplete'):self.plan()

    def test_current_portfolio_not_recounted_as_new_benefit(self):
        out=self.plan();self.o['current']=dict(routes=self.a['routes'],shares=[x['fraction']for x in out['candidates'][0]['shares']])
        self.assertIsNone(self.plan()['selected'])
        self.assertIn('insufficient_capacity_gain',self.plan()['candidates'][0]['reasons'])

    def test_invalid_policy_rejected_without_service_configuration(self):
        for key,value in [('share_step',0),('small_alpha',.5),('coordinate_passes',1.5)]:
            cfg=copy.deepcopy(self.c);cfg[key]=value
            with self.assertRaises(ValueError):plan_layout(self.o,[self.a],cfg,100000)
