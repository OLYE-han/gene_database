"""Pure result aggregation shared by in-process and process-pool heads."""
import numpy as np
from .peer_search import COUNTERS

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
    stats={key:sum(r['stats'][key] for r in reports) for key in COUNTERS}
    for key in ('expanded','batches','fallback_entries','frontier_dropped','improved_topk_batches','server_thread_cpu_ms',
                'visited_node_candidates_dropped','remote_candidates_dropped_after_forward'):
        stats[key]=sum(r['stats'][key] for r in reports)
    ids=[gid for r in reports for gid in r['expanded_ids']]
    per_shard={sid:{} for sid in range(len(c['cluster']['nodes']))}
    for r in reports:
        for key in (*COUNTERS,'expanded','batches','server_thread_cpu_ms'):
            per_shard[r['node_id']][key]=per_shard[r['node_id']].get(key,0)+r['stats'][key]
    peer_rpc=sum(r.get('peer_transfer',{}).get('peer_rpc_count',0) for r in reports)
    peer_tx=sum(r.get('peer_transfer',{}).get('peer_request_bytes',0) for r in reports)
    peer_rx=sum(r.get('peer_transfer',{}).get('peer_response_bytes',0) for r in reports)
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
        rpc_retries=sum(max(0,r.get('peer_transfer',{}).get('peer_rpc_count',0)-len(r['children'])) for r in reports),
        shards_visited=sorted({r['node_id'] for r in reports}),keepalive_nodes=involved,
        expansions_by_shard={sid:v.get('expanded',0) for sid,v in per_shard.items()},
        max_expanded_depth=max(r['stats']['max_expanded_depth'] for r in reports),
        max_discovery_depth=None,max_cross_shard_depth=max(r['stats']['max_cross_shard_depth'] for r in reports),
        graph_rounds=None,server_to_server_migrations=0,server_to_server_branches=len(reports)-1,
        exact_reranked=stats['exact_distance_evaluations'],transport=c['network']['transport'],
        execution='peer_single_visit_parallel_local',stop_reason='all_branch_manifests_complete',
        branch_stop_reasons=[r['stats']['stop_reason'] for r in reports],
        pending_at_stop=sum(r['stats']['pending_at_stop'] for r in reports),
        pure_network_latency_fraction=None,physical_ssd_latency_fraction=None,
        result_paths=[p for r in reports for p in r['result_paths'] if p['id'] in dict(ranked)])
    for key in ('peer_transfer_queue_ms','peer_pool_wait_ms','peer_connect_ms','peer_send_ms',
                'peer_response_wait_ms','peer_transfer_wall_ms','peer_connections_created','peer_connections_reused'):
        stats[key]=sum(r.get('peer_transfer',{}).get(key,0) for r in reports)
    stats['communication_mode']=c['peer']['communication']['mode']
    return ranked,stats
