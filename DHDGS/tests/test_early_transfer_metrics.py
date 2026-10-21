"""Offline counter reconciliation must not change delivery times or raw reports."""
import copy
import unittest
from dhdgs.benchmark_process import ReconciledRecords

class TransferMetricsTests(unittest.TestCase):
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
