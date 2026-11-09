"""Offline counter reconciliation must not change delivery times or raw reports."""
import copy
import unittest
from dhdgs.benchmark_process import ReconciledRecords

class TransferMetricsTests(unittest.TestCase):
    def test_pull_reads_are_rpc_traffic_not_failed_search_retries(self):
        q=self.record();q['branch_reports'][0]['region_task_mode']=True
        row=dict(query_request_id='q',branch_id='parent',pending_transfers=0,peer_visit_pending=0,
                 peer_rpc_count=4,peer_pull_rpc_count=3,peer_pull_wall_ms=2.,
                 peer_request_bytes=200,peer_response_bytes=120000)
        after={0:dict(peer_lifecycle=dict(transfer_records=[row]))}
        result=next(iter(ReconciledRecords([q],after)))
        self.assertEqual(result['stats']['rpc_retries'],0)
        self.assertEqual(result['stats']['peer_pull_rpc_count'],3)
        self.assertEqual(result['stats']['all_protocol_bytes'],120200)
        self.assertTrue(result['stats']['metrics_complete'])
    def record(self):
        stats={k:0 for k in ['peer_branch_rpc_count','rpc_count','all_protocol_rpc_count','peer_request_bytes','request_bytes','peer_response_bytes','response_bytes','wire_bytes','all_protocol_bytes','rpc_retries']}
        stats.update(metrics_complete=False,metrics_complete_excluding_pending_transfers=True)
        return dict(status='ok',request_id='q',client_wall_ms=3.5,stats=stats,
                    branch_reports=[dict(branch_id='parent',children=[{}],transfer_metrics_complete=False,peer_transfer={'peer_rpc_count':0})])
    def test_late_counters_join_without_changing_latency_or_original_report(self):
        raw=self.record();source=copy.deepcopy(raw)
        after={0:dict(peer_lifecycle=dict(transfer_records=[dict(query_request_id='q',branch_id='parent',pending_transfers=0,peer_rpc_count=1,peer_request_bytes=100,peer_response_bytes=30,peer_response_wait_ms=99)]))}
        q=next(iter(ReconciledRecords([source],after)))
        self.assertEqual(q['client_wall_ms'],3.5)
        self.assertTrue(q['stats']['metrics_complete'])
        self.assertFalse(q['stats']['metrics_complete_at_delivery'])
        self.assertEqual(q['stats']['all_protocol_bytes'],130)
        self.assertEqual(q['stats']['all_protocol_rpc_count'],1)
        self.assertEqual(q['stats']['peer_response_wait_ms'],99)
        self.assertEqual(q['branch_reports'][0]['peer_transfer_at_publication'],raw['branch_reports'][0]['peer_transfer'])
        self.assertEqual(raw['stats']['all_protocol_bytes'],0)
    def test_missing_or_unfinished_snapshot_stays_incomplete(self):
        for after in [{},{0:dict(peer_lifecycle=dict(transfer_records=[dict(query_request_id='q',branch_id='parent',pending_transfers=1)]))}]:
            q=next(iter(ReconciledRecords([self.record()],after)))
            self.assertFalse(q['stats']['metrics_complete'])
            self.assertFalse(q['branch_reports'][0]['transfer_metrics_complete'])
    def test_region_notifications_are_counted_without_becoming_search_retries(self):
        q=self.record();b=q['branch_reports'][0];b.update(region_task_mode=True,transfer_metrics_complete=True)
        row=dict(query_request_id='q',branch_id='parent',pending_transfers=0,peer_visit_pending=0,
                 peer_rpc_count=5,peer_visit_rpc_count=3,peer_region_reserve_rpc_count=1,
                 peer_request_bytes=500,peer_response_bytes=50)
        after={0:dict(peer_lifecycle=dict(transfer_records=[row]))}
        result=next(iter(ReconciledRecords([q],after)))
        self.assertEqual(result['stats']['rpc_retries'],0)
        self.assertEqual(result['stats']['all_protocol_rpc_count'],5)
        self.assertEqual(result['stats']['all_protocol_bytes'],550)
        self.assertTrue(result['stats']['metrics_complete'])
        pending=self.record();pending['branch_reports'][0].update(region_task_mode=True,transfer_metrics_complete=True)
        row['peer_visit_pending']=1
        self.assertFalse(next(iter(ReconciledRecords([pending],after)))['stats']['metrics_complete'])
