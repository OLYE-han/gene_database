"""Pure result aggregation shared by in-process and process-pool heads."""
import numpy as np
from .peer_search import COUNTERS

REPLICA_COUNTERS=('tail_policy_expanded','tail_policy_stops','tail_policy_gain_stops','tail_policy_unused_credit','lazy_navigation_vertices','lazy_refined_vectors','lazy_unrefined_vertices','lazy_refine_batches','lazy_refine_cpu_ms','lazy_refine_wait_ms','pull_approved_actions','pull_limit_rejections','pull_batches','pull_records','pull_expanded','pull_fallbacks','pull_wait_ms','pull_wire_bytes','pull_record_bytes','pull_code_bytes','pull_source_cpu_ms','pull_source_read_ms','pull_source_service_ms','region_exclusion_ids_inherited','region_excluded_candidates','region_exclusion_ids_added','region_unexpanded_at_handoff','region_handoffs','same_host_region_handoffs','hotness_record_ms','hotness_expansions_recorded','duplicate_local_expansions_avoided','physical_node_revisits','cross_dwell_blocked_decisions','cross_dwell_exhaustion_escapes','peer_replica_selected','replica_record_reads','replica_local_expansions')

# Keep offline Python aggregation aligned with native branch accounting.
REPLICA_COUNTERS += (
    'demand_wait_ms', 'demand_resume_batches', 'demand_fallbacks',
    'demand_fast_rejections', 'demand_source_already_started',
    'demand_cache_expansions', 'demand_durable_expansions',
    'remote_first_actions','remote_first_pushes','remote_pull_streak_pushes','remote_pull_disabled_pushes','one_sided_read_ops','one_sided_data_bytes','one_sided_record_ms','one_sided_code_ms','one_sided_filter_ms','one_sided_filter_cpu_ms','one_sided_lane_queue_ms','one_sided_result_cpu_ms','one_sided_result_ms','one_sided_auth_ms','pull_install_ms','pull_install_cpu_ms',
    'pull_policy_pause_ms', 'pull_main_resume_count', 'pull_tail_resume_count',
    'pull_gate_attempt', 'pull_gate_disabled', 'pull_gate_prior_failure',
    'pull_gate_remaining', 'pull_gate_rounds', 'pull_gate_bytes',
    'pull_dispatch_queue_ms', 'pull_network_ms', 'pull_resume_queue_ms',
    'pull_source_queue_ms', 'pull_resume_count',
    'query_actor_serial_wait_ms', 'query_runnable_queue_ms',
)

def aggregate_reports(c, reports, transfer, involved):
    cfg=c['search']
    merged={}
    for report in reports:
        for gid,d in zip(report['ids'],report['squared_distances']):
            if not np.isfinite(d) or d<0: raise ValueError('Invalid branch result distance')
            if gid in merged and not np.isclose(merged[gid],d,rtol=1e-6,atol=1e-8):
                raise ValueError('Replica exact distance mismatch')
            merged[gid]=min(d,merged.get(gid,float('inf')))
    ranked=sorted(merged.items(),key=lambda p:(p[1],p[0]))[:cfg['top_k']]
    stats={key:sum(r['stats'].get(key,0) for r in reports) for key in (*REPLICA_COUNTERS,*COUNTERS)}
    for key in ('expanded','batches','fallback_entries','frontier_dropped','improved_topk_batches','server_thread_cpu_ms',
                'visited_node_candidates_dropped','remote_candidates_dropped_after_forward',
                'region_candidates_merged','region_frontier_saved','source_reserved_candidate_checks','source_reserved_credit_unused',
                'replica_overlay_expansions','replica_host_base_expansions',
                'candidate_sync_candidates_sent','candidate_sync_batches','candidate_sync_resumes',
                'candidate_sync_resume_queue_ms','candidate_sync_park_ms','candidate_budget_terminal',
                'candidate_receive_to_consume_ms','candidate_timed_consumed','candidate_sync_known_exhausted_skipped',
                'candidate_start_accepted','candidate_start_conflicts','candidate_start_refunded_budget','candidate_early_results'):
        stats[key]=sum(r['stats'].get(key,0) for r in reports)
    ids=[gid for r in reports for gid in r['expanded_ids']]
    per_shard={sid:{} for sid in range(len(c['cluster']['nodes']))}
    for r in reports:
        for key in (*REPLICA_COUNTERS,*COUNTERS,'expanded','batches','server_thread_cpu_ms'):
            per_shard[r['node_id']][key]=per_shard[r['node_id']].get(key,0)+r['stats'].get(key,0)
    peer_rpc=sum(r.get('peer_transfer',{}).get('peer_rpc_count',0) for r in reports)
    peer_tx=sum(r.get('peer_transfer',{}).get('peer_request_bytes',0) for r in reports)
    peer_rx=sum(r.get('peer_transfer',{}).get('peer_response_bytes',0) for r in reports)
    placements={r['branch_id']:r['node_id'] for r in reports}
    local_branches=sum(r['parent_branch_id'] is not None and placements[r['parent_branch_id']]==r['node_id'] for r in reports)
    stats.update(peer_execution_model=c['peer']['execution_model'],
        io_backends=sorted({r.get('execution',{}).get('io_backend','sync_pread') for r in reports}),
        per_shard=per_shard,unique_expanded=len(set(ids)),duplicate_expansions=len(ids)-len(set(ids)),
        branch_count=len(reports),branch_unused_credit=sum(r['work_unused'] for r in reports),
        visited_nodes=max((r['visited_nodes'] for r in reports),key=len),
        node_revisit_count=len(reports)-len({r['node_id'] for r in reports}),
        router_submit_rpc_count=transfer['rpc_count'],peer_branch_rpc_count=peer_rpc,
        router_expand_rpc_count=0,
        rpc_count=transfer['rpc_count']+peer_rpc,
        request_bytes=transfer['request_bytes']+peer_tx,response_bytes=transfer['response_bytes']+peer_rx,
        wire_bytes=transfer['request_bytes']+peer_tx+transfer['response_bytes']+peer_rx,
        peer_request_bytes=peer_tx,peer_response_bytes=peer_rx,
        rpc_retries=sum(max(0,r.get('peer_transfer',{}).get('peer_rpc_count',0)
            -r.get('peer_transfer',{}).get('peer_visit_rpc_count',0)
            -r.get('peer_transfer',{}).get('peer_region_reserve_rpc_count',0)-r.get('peer_transfer',{}).get('peer_pull_rpc_count',0)-len(r['children'])) for r in reports),
        shards_visited=sorted({r['node_id'] for r in reports}),keepalive_nodes=involved,
        expansions_by_shard={sid:v.get('expanded',0) for sid,v in per_shard.items()},
        max_expanded_depth=max(r['stats']['max_expanded_depth'] for r in reports),
        max_discovery_depth=None,max_cross_shard_depth=max(r['stats']['max_cross_shard_depth'] for r in reports),
        graph_rounds=None,server_to_server_migrations=0,server_to_server_branches=len(reports)-1-local_branches,
        same_host_branches=local_branches,
        exact_reranked=stats['exact_distance_evaluations'],transport=c['network']['transport'],
        execution='peer_bounded_revisit_parallel_local' if c['peer'].get('allow_node_revisit',False) else 'peer_single_visit_parallel_local',stop_reason='all_branch_manifests_complete',
        branch_stop_reasons=[r['stats']['stop_reason'] for r in reports],
        pending_at_stop=sum(r['stats']['pending_at_stop'] for r in reports),
        pure_network_latency_fraction=None,physical_ssd_latency_fraction=None,
        result_paths=[p for r in reports for p in r['result_paths'] if p['id'] in dict(ranked)])
    for key in ('peer_transfer_queue_ms','peer_pool_wait_ms','peer_connect_ms','peer_send_ms',
                'peer_response_wait_ms','peer_transfer_wall_ms','peer_connections_created','peer_connections_reused',
                'peer_visit_rpc_count','peer_visit_wall_ms','peer_region_reserve_rpc_count','peer_region_reserve_wall_ms','peer_pull_rpc_count','peer_pull_wall_ms'):
        stats[key]=sum(r.get('peer_transfer',{}).get(key,0) for r in reports)
    stats['communication_mode']=c['peer']['communication']['mode']
    return ranked,stats
