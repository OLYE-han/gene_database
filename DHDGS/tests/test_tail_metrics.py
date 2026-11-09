"""Observed pull gates use pre-split credit; ready wait excludes preparation."""
from test_hop_return import PullRuntimeTests

class TailMetricTests(PullRuntimeTests):
 def configure_replica_test(self,c):
  super().configure_replica_test(c)
  c['peer']['replica_pull'].update(max_rounds=1,max_remaining=200)
  c['metrics'].update(lifecycle_trace=True,detailed_trace=True)
 def test_gate_credits_and_ready_wait(self):
  self.install_entry();q=self.engine.search_async(self.x[0],0).result(15)
  self.check_result(q,200)
  reports=[b for b in q['branch_reports'] if b.get('region_admission')!='source_reserved_not_started']
  gates=[g for b in reports for g in b.get('pull_gate_events',[])]
  self.assertTrue(gates);self.assertEqual(gates[0]['reason'],'attempt')
  self.assertTrue(any(g['reason']=='rounds' for g in gates))
  for b in reports:
   for f in b.get('fork_events',[]):
    self.assertEqual(f['remaining_before']-f['remaining_after'],f['work_grant'])
   self.assertGreaterEqual(b['stats']['query_ready_queue_ms'],0)
   self.assertGreaterEqual(b['stats']['query_admission_queue_ms'],0)
   self.assertGreaterEqual(b['stats']['query_actor_serial_wait_ms'],0)
   self.assertAlmostEqual(b['stats']['query_ready_queue_ms'],
       b['stats']['query_actor_serial_wait_ms']+b['stats']['query_runnable_queue_ms'],places=5)
  self.assertEqual(q['stats']['pull_gate_attempt'],sum(g['reason']=='attempt' for g in gates))
  for node in self.c['cluster']['nodes']:
   rt=self.rpc(node['id'],'load')['runtime']
   self.assertEqual(rt['ready_clock_fallbacks'],0)
   self.assertEqual(sum(rt['compute_ready_wait_hist_us_log2']),sum(rt['compute_executor_wait_hist_us_log2']))
   self.assertEqual(sum(rt['compute_ready_wait_hist_us_log2']),sum(rt['compute_actor_wait_hist_us_log2']))
