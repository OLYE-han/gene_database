import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
from threadpoolctl import threadpool_limits

from dhdgs.benchmark import run_benchmark
from dhdgs.index import build
from dhdgs.metrics import (CLOCK_DOMAIN, wait_breakdown, ResourceSampler, export_query_csv,
                          summarize_query_timings, summarize_per_query_timings, export_resource_csvs)
from dhdgs.search import Engine
from test_system import fixture


class MeasurementTests(unittest.TestCase):
    def test_stage_means_repetitions_and_missing_measurements(self):
        rows=[{'status':'ok','query_row':0,'stats':{'latency_ms':10.,
                  'latency_breakdown_ms':{'route_ms':2.,'search_ms':8.},'server_thread_cpu_ms':20.}},
              {'status':'ok','query_row':0,'stats':{'latency_ms':30.,
                  'latency_breakdown_ms':{'route_ms':12.,'search_ms':18.},'server_thread_cpu_ms':40.}},
              {'status':'error','query_row':1,'stats':{'latency_ms':5.,'server_thread_cpu_ms':999.}},
              {'status':'ok','query_row':2,'stats':{'latency_ms':8.,'pread_ms':None}}]
        result=summarize_query_timings(rows)
        self.assertEqual(result['request_count'],4)
        self.assertEqual(result['successful_requests'],3)
        self.assertEqual(result['wall_unavailable_success_count'],1)
        self.assertTrue(result['wall_means_additive'])
        self.assertEqual(result['mean_latency_ms_for_wall_samples'],20.)
        route=result['wall_stages']['route_ms']
        self.assertEqual(route['mean_ms'],7.)
        self.assertAlmostEqual(route['mean_query_fraction'],.3)
        self.assertAlmostEqual(route['fraction_of_total_latency'],.35)
        cpu=result['cumulative_work']['server_thread_cpu_ms']
        self.assertEqual(cpu['sample_count'],2)
        self.assertEqual(cpu['mean_ms'],30.)
        self.assertIsNone(cpu['mean_query_fraction'])
        self.assertIsNone(result['cumulative_work']['pread_ms']['mean_ms'])
        per_query=summarize_per_query_timings(rows)
        self.assertEqual(len(per_query),3)
        self.assertEqual(per_query[0]['request_count'],2)
        self.assertEqual(per_query[0]['wall_stages']['route_ms']['mean_ms'],7.)
        self.assertIsNone(per_query[1]['mean_latency_ms_for_wall_samples'])

    def test_inconsistent_or_invalid_wall_breakdown_is_not_added(self):
        rows=[{'status':'ok','stats':{'latency_ms':5.,'latency_breakdown_ms':{'a':3.,'b':3.}}},
              {'status':'ok','stats':{'latency_ms':5.,'latency_breakdown_ms':{'a':5.},
                                    'metrics_complete':False,'pread_ms':10.}}]
        summary=summarize_query_timings(rows)
        self.assertEqual(summary['wall_sample_count'],1)
        self.assertIsNone(summary['cumulative_work']['pread_ms']['mean_ms'])
        rows.append({'status':'ok','stats':{'latency_ms':5.,'latency_breakdown_ms':{'different_stage':5.}}})
        self.assertFalse(summarize_query_timings(rows)['wall_means_additive'])

    def test_blocked_time_partition_and_foreign_clock(self):
        action = {'client_start_ns':0,'client_end_ns':9_000_000,
                  'service_trace':{'clock_domain':CLOCK_DOMAIN,'start_ns':2_000_000,'end_ns':8_000_000,
                      'spans':[['pread',3_000_000,5_000_000],['rabitq',5_000_000,7_000_000]]}}
        values = wait_breakdown(1_000_000,10_000_000,[action])
        self.assertAlmostEqual(sum(values.values()),9.)
        if CLOCK_DOMAIN:
            self.assertEqual(values['storage_call_wait_ms'],2.)
            self.assertEqual(values['data_compute_wait_ms'],2.)
            self.assertEqual(values['data_service_other_wait_ms'],2.)
            self.assertEqual(values['transport_and_runtime_wait_ms'],2.)
        self.assertEqual(values['coordinator_wakeup_wait_ms'],1.)
        action['service_trace']['clock_domain']='different-machine'
        values = wait_breakdown(1_000_000,10_000_000,[action])
        self.assertEqual(values['rpc_wait_unattributed_ms'],8.)
        self.assertEqual(values['storage_call_wait_ms'],0.)

    def test_parallel_service_is_not_double_counted(self):
        actions = [{'client_start_ns':0,'client_end_ns':end,'service_trace':None} for end in (5_000_000,8_000_000)]
        values = wait_breakdown(1_000_000,6_000_000,actions)
        self.assertAlmostEqual(sum(values.values()),5.)
        self.assertEqual(values['rpc_wait_unattributed_ms'],4.)

    def test_sampler_is_bounded(self):
        sampler = ResourceSampler(1,2).start()
        rows = sampler.finish()
        self.assertLessEqual(len(rows),2)
        self.assertIsNotNone(rows[-1]['cpu_seconds'])


class SearchMetricTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.c,self.x=fixture(self.root)
        with threadpool_limits(limits=1):
            self.manifest=build(self.c)

    def tearDown(self):
        self.temp.cleanup()

    def test_counters_paths_and_latency_conservation(self):
        engine=Engine(self.c)
        try:
            result=engine.search(self.x[0],0)
            s=result['stats']
            self.assertEqual(s['exact_distance_evaluations'],s['expanded'])
            self.assertEqual(s['pread_calls'],s['expanded'])
            self.assertEqual(sum(v['exact_distance_evaluations'] for v in s['per_shard'].values()),s['expanded'])
            self.assertAlmostEqual(sum(s['latency_breakdown_ms'].values()),s['latency_ms'],places=7)
            self.assertAlmostEqual(sum(s['latency_breakdown_fraction'].values()),1.,places=7)
            self.assertTrue(all(v>=0 for v in s['latency_breakdown_ms'].values()))
            tree={v['id']:v for v in result['discovery_tree']}
            for gid,p in tree.items():
                if p['parent'] is None:
                    self.assertEqual(p['depth'],0)
                else:
                    self.assertEqual(p['depth'],tree[p['parent']]['depth']+1)
                    self.assertEqual(p['root'],tree[p['parent']]['root'])
            self.assertGreater(s['scanned_edges'],s['expanded'])
            self.assertTrue(self.manifest['build_profile']['phases'])
            self.assertGreater(s['server_thread_cpu_ms'],0)
        finally: engine.close()

    def test_concurrent_benchmark_and_exports(self):
        self.c['experiment'].update(benchmark_concurrency=2,benchmark_repetitions=2,warmup_queries=1)
        report=run_benchmark(self.c)
        self.assertEqual(report['query_count'],6)
        self.assertEqual(report['successful_queries'],6)
        self.assertEqual(report['recall_at_k'],1.)
        self.assertAlmostEqual(report['qps']*report['measured_wall_seconds'],6.)
        self.assertEqual(len({r['request_id'] for r in report['queries']}),6)
        export_query_csv(self.root/'queries.csv',report['queries'])
        self.assertEqual(len((self.root/'queries.csv').read_text().splitlines()),7)
        export_resource_csvs(self.root,report)
        self.assertTrue((self.root/'query_stage_means.csv').exists())
        self.assertTrue((self.root/'per_query_stage_means.csv').exists())
        summary=report['query_timing_summary']
        self.assertEqual(summary['wall_sample_count'],6)
        self.assertAlmostEqual(sum(s['mean_ms'] for s in summary['wall_stages'].values()),report['latency_ms']['mean'])
        self.assertEqual(len(report['per_query_timing_means']),3)
        self.assertTrue(all(s['request_count']==2 for s in report['per_query_timing_means']))

    def test_peer_stage_means(self):
        self.c['search']['execution']='peer'
        self.c['scheduler']['mode']='adaptive'
        report=run_benchmark(self.c)
        stages=report['query_timing_summary']['wall_stages']
        self.assertEqual(set(stages),{'routing_wall_ms','peer_execution_and_control_wait_ms','head_merge_and_lifecycle_ms'})
        self.assertAlmostEqual(sum(s['mean_ms'] for s in stages.values()),report['latency_ms']['mean'])

    def test_failures_stay_in_denominator(self):
        with patch.object(Engine,'search',side_effect=TimeoutError('injected timeout')):
            report=run_benchmark(self.c)
        self.assertEqual(report['failed_queries'],3)
        self.assertEqual(report['qps'],0.)
        self.assertEqual(report['recall_at_k'],0.)
        self.assertIsNone(report['latency_ms']['p99'])
        self.assertEqual(report['failure_latency_ms']['count'],3)
        self.assertTrue(all(r['status']=='error' for r in report['queries']))
        self.assertEqual(report['query_timing_summary']['wall_sample_count'],0)
        self.assertIsNone(report['query_timing_summary']['mean_latency_ms_for_wall_samples'])

    def test_trace_can_be_omitted(self):
        self.c['metrics']['detailed_trace']=False
        engine=Engine(self.c)
        try:
            result=engine.search(self.x[0],0)
            self.assertNotIn('batch_trace',result)
            self.assertNotIn('discovery_tree',result)
            self.assertEqual(result['stats']['expanded'],96)
        finally: engine.close()
