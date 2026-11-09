"""One explicit YAML configuration; reject missing/unknown/duplicate keys."""
from __future__ import annotations

import copy
import hashlib
import math
from pathlib import Path

import yaml


class StrictLoader(yaml.SafeLoader):
    pass


def _mapping(loader, node, deep=False):
    output = {}
    for key, value in node.value:
        name = loader.construct_object(key, deep=deep)
        if name in output:
            raise ValueError(f"Duplicate YAML key: {name}")
        output[name] = loader.construct_object(value, deep=deep)
    return output


StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)

# Types are the schema, not a second set of parameter defaults.
SCHEMA = {
    "hotness": {"enabled":bool,"half_life_ms":float,"merge_interval_ms":float,"local_capacity":int,"queue_batches":int,"max_candidates":int,"top_k":int,"max_lanes":int,"min_score":float},
    "load_balance": {"boundary_augmentation":{"enabled":bool,"mode":str,"per_direction":int,"projection_bits":int,"seed":int,"max_connector_hops":int,"max_extra_bytes":int,"max_plan_seconds":float,"scan_block_rows":int},"partitioned_regions":{"pipeline":{"enabled":bool,"adjacency_cache":bool,"io_threads":int,"prefetch_rows":int,"queue_reads":int,"cache_bytes":int,"memoize_verification":bool,"verification_cache_entries":int,"pending_plans":int,"copy_during_expansion":bool,"growth_batch_rows":int,"growth_queue_batches":int,"growth_copy_workers":int},"total_alpha":float,"hops":int,"target_weights":dict,"balance_weight":float,"deadline_s":float,"max_edge_scans":int},"layout_policy":{"small_alpha":float,"large_alpha":float,"capacity_headroom":float,"observation_max_age_ms":float,"cost_max_age_ms":float,"large_sustained_windows":int,"large_recurring_windows":int,"cooldown_ms":float,"source_queue_trigger_ms":float,"target_queue_limit_ms":float,"recall_min":float,"coordinate_passes":int,"share_step":float,"min_capacity_gain":float,"build_amortization_factor":float},"hop_expansion":{"fast":{"enabled":bool,"cut_budget_ms":int,"cut_rounds":int,"cut_candidates":int},"promote_covered_entries":bool,"enabled":bool,"core_fraction":float,"rounds":int,"candidates":int,"cut_steps":int,"history_weight":float},"locality":{"metadata_reserve_bytes":int,"enabled":bool,"source_capacity_fraction":float,"max_regions":int,"per_region_fraction":float,"generalization_weight":float},"adaptive_expansion": {
        "enabled":bool,"max_copies_per_family":int,"source_goal":float,"target_goal":float,
        "target_queue_ms":float,"sustained_windows":int,"scale_down_windows":int,"cooldown_ms":float,
        "fraction_step":float,"min_fraction":float,"min_saved_s":float,"min_validation_queries":int,
        "require_qualified":bool,"cost_max_age_ms":float,"new_target_cost_margin":float,
        "decision_horizon_ms":float,"amortization_factor":float,"scale_down_margin":float},"adaptive_decay": {
        "enabled":bool,"fast_half_life_ms":float,"slow_half_life_ms":float,
        "peak_half_life_ms":float,"max_sample_gap_ms":float,"min_keep_qps":float,
        "reheat_qps":float,"cold_ratio":float,"reheat_ratio":float,"base_hold_ms":float,
        "hold_min_ms":float,"hold_max_ms":float,"min_residency_ms":float,
        "build_cost_weight":float,"rebound_weight":float,"rebound_confidence_events":float,
        "max_rebound_score":float,"rebound_window_ms":float,"pressure_discount":float,"history_ewma_weight":float,
        "history_entry_overlap":float,"history_limit":int,"history_ttl_ms":float,
        "reclaim_workers":int,"reclaim_retry_ms":float},"community_poll_ms":int,"community_hops":int,"community_top_k":int,"community_seed_starts":int,"community_hot_closure":bool,"community_ppr_rounds":int,"community_ppr_candidates":int,"community_ppr_iterations":int,"community_ppr_vertices":int,"community_ppr_restart":float,"community_ppr_tolerance":float,"community_byte_price":float,"community_max_edge_scans":int,"community_adjacency_bytes":int,"community_max_plan_ms":int,"replica_transfer_mode":str,"producer_queue_bytes":int,"transfer_buffer_budget_bytes":int,"replica_write_workers":int,"replica_loader":str,"planner_mode":str,"planner_max_age_ms":int,"publication_mode":str,"compute_region_quality":bool,"replica_id_lookup_bytes":int,"progressive_validation":bool,"validation_publish_batch":int,"portal_budget_fraction":float,"native_replica_pack":bool,"copy_pressure_source_queue":bool,"quality_weight_routing":bool,"reuse_observed_baseline":bool,"lifecycle_poll_ms":int,"max_total_replica_bytes":int,"selection_policy":str,"target_queue_limit_ms":float,"min_entry_validation_queries":int,"validation_mode":str,"max_validation_queries":int,"shadow_overlap_min":float,
        "min_replica_expansions":int,"max_early_exit_fraction":float,"quality_walk_steps":int,
        "entry_weight_floor":float,"entry_weight_power":float,"max_peer_entries":int,
        "peer_routes_enabled":bool,"max_target_shard_fraction":float,
        "enabled":bool,"mode":str,"sample_every":int,"max_observations":int,
        "max_history":int,"min_group_queries":int,"min_entry_queries":int,"load_interval_ms":int,
        "control_interval_ms":int,"sustained_windows":int,"source_util_high":float,"target_util_low":float,
        "target_util_limit":float,"util_gap":float,"queue_high_ms":float,"queue_low_ms":float,
        "max_load_age_ms":int,"max_core_rows":int,"halo_hops":int,"max_region_rows":int,"max_region_bytes":int,"max_code_bytes":int,
        "min_coverage":float,"port_offset":int,"chunk_bytes":int,"inflight_chunks":int,
        "copy_mib_per_s":float,"warm_bytes":int,"reserve_disk_bytes":int,"operation_timeout_s":float,
        "initial_fraction":float,"fraction_step":float,"max_fraction":float,"route_adjust_ms":int,
        "cooldown_ms":int,"min_residency_ms":int,"idle_retire_ms":int,"max_replicas":int,
        "calibration_file":str,"min_validation_queries":int,"recall_min":float,"recall_margin":float,
        "validation_workers":int,"maintenance_min_reactors":int,"maintenance_max_reactors":int,
        "maintenance_connections":int,"copy_pressure_queue_tasks":int,"copy_pressure_check_ms":int,
        "copy_pressure_rate_factor":float},

    "version": int, "seed": int,
    "benchmark_results": {"memory_limit_bytes":int,"json_shard_bytes":int,"processor_timeout_seconds":int},
    "runtime": {"blas_threads": int,"python_switch_interval_ms":float},
    "native_runtime": {"compute_threads":int,"tail_cpu_ids":list,"compute_cpu_ids":list,"data_enabled":bool,"head_enabled":bool,
                       "receive_threads":int,"receive_queue_capacity":int,
                       "max_connections":int,"compute_queue_capacity":int,"registry_reserve_entries":int,"lifecycle_cleanup_max_contexts":int,"lifecycle_cleanup_max_records":int,"lifecycle_async_reclaim":bool,"lifecycle_reclaim_threads":int,"lifecycle_reclaim_batches":int,"lifecycle_timer_batch":int,
                       "network_queue_capacity":int,"connections_per_endpoint":int,
                       "route_threads":int,"merge_threads":int,"early_parent_report":bool,"queued_control_registration":bool,"move_head_reports":bool,"search_thread_reports":bool,
                       "head_maintenance_mode":str,"head_pin_threads":bool,
                       "report_capacity_retry_ms":int,
                       "report_network":{"enabled":bool,"connections_per_endpoint_per_reactor":int},
                       "result_sink":{"enabled":bool,"writers":int,"batch_records":int,"batch_bytes":int,"completion":str,"max_retained_bytes":int},
                       "head_entry_network":{"enabled":bool,"send_min_reactors":int,"send_max_reactors":int,
                                             "connections_per_endpoint_per_reactor":int},
                       "network_module":{"send_warmup_grace_ms":int,"idle_probe_ms":int,"scale_up_busy_fraction":float,"async_connect_progress":bool,"async_connect":bool,"cpu_ids":list,"transport":str,"rdma_poll_interval_us":int,"min_connections_per_endpoint_per_reactor":int,"max_connecting_per_reactor":int,"send_pressure_mode":str,"rebalance_connections":bool,"rebalance_interval_ms":int,"rebalance_batch":int,"enabled":bool,"send_min_reactors":int,"send_max_reactors":int,
                           "receive_min_reactors":int,"receive_max_reactors":int,"handlers_per_receive_reactor":int,
                           "connections_per_endpoint_per_reactor":int,"send_queue_capacity":int,
                           "receive_queue_capacity_per_reactor":int,"max_receive_connections":int,
                           "send_pending_high":int,"receive_queue_high":int,"high_samples":int,
                           "sample_interval_ms":int,"cooldown_ms":int,"idle_ms":int},
                       "adaptive_control":{"enabled":bool,"min_lanes":int,"max_lanes":int,
                           "command_queue_high":int,"network_pending_high":int,"high_samples":int,
                           "sample_interval_ms":int,"cooldown_ms":int,"idle_ms":int}},
    "metrics": {"snapshot_chunk_bytes":int,"expansion_counter_stripes":int,"detailed_trace": bool,"trace_query_ids":list,"lifecycle_trace":bool,"gc_trace":bool, "resource_sample_interval_ms": int, "resource_max_samples": int},
    "paths": {"dataset": str, "output": str},
    "data": {"metric": str, "dimension": int, "row_limit": int, "block_rows": int, "inspection_rows": int},
    "partition": {"method": str, "overlap_fraction": float, "max_copies": int, "train_rows": int,
                  "calibration_rows": int, "kmeans_iterations": int, "kmeans_tolerance": float},
    "build": {"memory_limit_bytes": int, "degree": int, "candidate_list_size": int, "prune_alphas": list},
    "quantization": {"bits": int},
    "storage": {"native_id_lookup_bytes":int,"page_bytes": int, "cache_bytes_per_shard": int,
                "io": {"backend": str, "fallback_backend": str, "worker_threads": int,
                       "max_submit_requests": int, "max_inflight_per_query": int,
                       "max_inflight_per_device": int, "max_inflight_bytes_per_device": int,
                       "buffer_pool_bytes_per_device": int, "max_pending_batches": int,
                       "max_completion_events_per_poll": int, "completion_poll_ms": int}},
    "routing": {"sample_ratio": float, "min_samples": int, "max_samples": int, "hnsw_m": int,
                "ef_construction": int, "ef_search": int, "entry_candidates": int},
    "search": {"skip_seen_estimates": bool,"execution": str, "top_k": int, "frontier_size": int, "max_expansions": int, "local_beam": int,
               "max_inflight": int, "exclude_self": bool, "fallback_enabled": bool,
               "max_fallback_entries": int, "stagnation_batches": int},
    "peer": {"demand_growth":{"indexed_directory":bool,"persistence_pipeline":bool,"persistence_queue_batches":int,"persistence_queue_bytes":int,"maintenance_interval_ms":int,"lock_sample_every":int,"ssd_directories":dict,"enabled":bool,"cache_records":int,"promotion_hot_vertices":int,"growth_fraction":float,"promotion_trigger":str,"persist":bool,"cache_bytes":int,"max_path_records":int,"max_waiters":int,"max_inflight_batches":int,"read_batch_records":int,"read_batch_bytes":int,"maintenance_bytes":int,"maintenance_timeout_ms":int,"training_seconds":float,"max_events":int},"candidate_sync":{"scope":str,"start_threads":int,"start_capacity":int,"coalesce_pending":bool,"max_buffered_records":int,"nonblocking_start":bool,"early_result":bool,"completion_threads":int,"completion_capacity":int,"skip_exhausted_targets":bool,"early_budget_report":bool,"enabled":bool,"interval_expansions":int,"max_records":int},"remote_pull":{"enabled":bool,"pull_enabled":bool,"switch_after_batches":int},"replica_pull":{"tail_policy":{"enabled":bool,"max_expansions":int,"min_expansions":int,"stable_batches":int,"uncertainty_weight":float,"improvement_margin":float},"lazy_rerank":{"enabled":bool,"max_vectors":int,"uncertainty_weight":float},"one_sided":{"io_threads":int,"enabled":bool,"device":str,"record_mapping":str,"doorbell_batch":bool,"missing_codes_only":bool,"measure_code_availability":bool,"port":int,"max_registered_bytes":int,"max_directory_bytes":int,"lanes_per_source":int,"max_connections":int,"poll_interval_us":int,"timeout_ms":int,"fallback_to_rpc":bool},"retry_pruned_candidates":bool,"enabled":bool,"require_rdma":bool,"max_remaining":int,"max_rounds":int,"batch_records":int,"batch_bytes":int,"max_bytes":int,"async":{"dedicated_dispatcher":bool,"shared_compute":{"enabled":bool,"tail_fraction":float,"credit_limit_ms":float},"coalesce":{"enabled":bool,"max_queries":int,"max_bytes":int,"wait_us":int},"exclude_pause_from_age":bool,"main_help_max_tasks":int,"main_help_reserve_workers":int,"main_help_queue_low":int,"target_nodes":list,"source_nodes":list,"enabled":bool,"tail_threads":int,"pin_tail_threads":bool,"max_contexts":int,"max_inflight_per_source":int,"response_bytes":int,"source_read_threads":int,"source_read_queue":int,"send_reactors":int,"connections_per_reactor":int}},"concurrent_region_admission":bool,"region_tasks":bool,"region_visit_broadcast":bool,"region_visit_queue_capacity":int,"fanout_child_slot_fraction":float,"replica_child_budget_fraction":float,"logical_region_visits":bool,"max_excluded_region_ids":int,"deduplicate_node_expansions":bool,"allow_node_revisit":bool,"min_expansions_after_cross":int,"cross_on_local_exhaustion":bool,"execution_model": str,"max_branches": int,"child_budget_fraction": float,"worker_threads": int,
             "max_active_queries": int,"heartbeat_interval_ms": int,"lease_timeout_ms": int,
             "transfer_failure_grace_ms":int,"query_timeout_seconds": float,"control_timeout_seconds": float,"handoff_timeout_seconds": float,
             "retention_seconds": float,"max_tombstones": int,"head_bind_host": str,
             "head_advertise_host": str,"head_port": int,"head_receiver_processes":int,"head_receiver_ipc":str,
             "head_query_workers":int,"head_entry_workers":int,
             "head_lanes":{"enabled":bool,"processes":int,"route_threads":int,"dispatch_threads":int,
                           "callback_connections":int,"startup_timeout_seconds":float,"save_results":bool,"results_directory":str},
             "head_direct":{"enabled":bool,"distribution_threads":int,"confirmation_threads":int,
                 "result_processes":int,"ack_timeout_ms":int,"submit_retries":int,
                 "result_queue_capacity":int,"startup_timeout_seconds":float,"save_results":bool,
                 "results_directory":str,"queued_reports":bool,"result_channels":int,
                 "report_queue_capacity":int,"report_connections_per_channel":int,"completion_threads":int,"report_backend":str,"report_registration":str,"completion_dispatch":str,"result_backend":str},
             "head_pipeline":{"enabled":bool,"receive_min":int,"receive_max":int,
                 "receive_backlog":int,"receive_idle_ms":int,"route_workers":int,"merge_workers":int,
                 "dispatch_workers":int,"callback_connections":int,"ack_inline":bool},
             "adaptive_services": {"enabled":bool,"admission":bool,"head_dispatch":bool,"transfer":bool,"mirror_outside_lock":bool,"submit_without_state_lock":bool,"start_without_state_lock":bool,"connections":bool,"head_connections":bool,"connections_min":int,"connections_max":int,
                 "admission_min":int,"admission_max":int,"head_dispatch_min":int,"head_dispatch_max":int,
                 "transfer_min":int,"transfer_max":int,"sample_interval_ms":int,"target_wait_ms":int,
                 "cooldown_ms":int,"downscale_idle_ms":int,"growth_step":int,"target_utilization":float,
                 "ewma_weight":float,"history_size":int},
             "locking": {"split_native_registration":bool,"native_register_release_gil":bool},
             "admission": {"mode":str,"queue_capacity":int,"queue_lock":str,"dispatch_threads":int},
             "sender": {"mode":str,"transfer_queue_capacity":int},
             "compute": {"mode":str,"queue_capacity":int},
             "connection_pool": {"probe_mode":str,"head_scheduling":str,"data_scheduling":str},
             "control": {"initial_heartbeat_delay_ms":int,"scheduler":str,"max_events_per_turn":int,"completion_fast_path":bool},
             "communication": {"mode":str,"transfer_workers":int,"control_workers":int,
                 "max_pending_control":int,"reuse_connections":bool,"connections_per_endpoint":int,
                 "max_connection_endpoints":int}},
    "scheduler": {"mode": str, "kappa": float, "exploration": float, "max_wait_ms": float,
                  "local_batch_ms": float, "remote_rtt_ms": float, "bandwidth_bytes_per_ms": float,
                  "ewma_weight": float,"remote_selection": str,"remote_weight": float,
                  "probability_temperature": float},
    "network": {"rpc_tcp_nodelay":bool,"transport": str, "batch_size": int, "timeout_seconds": float,
                "max_message_bytes": int, "max_retries": int, "listen_backlog": int},
    "cluster": {"nodes": list},
    "query": {"vectors": str, "ids": (str, type(None)), "row": int},
    "assignment": {"model": str, "threshold": str},
    "experiment": {"metrics": list, "overlap_fractions": list, "seeds": list, "train_rows": int,
                   "calibration_rows": int, "evaluation_rows": int, "benchmark_queries": int,
                   "exact_block_rows": int, "target_recall": float,
                   "benchmark_concurrency": int, "benchmark_repetitions": int, "warmup_queries": int},
}


SCHEMA['load_balance']['entry_routing']={'mode':str,'telemetry':bool,'poll_ms':int,'ttl_ms':int,'temperature_ms':float,'prior_ms':float,'cpu_threshold':float,'cpu_penalty_ms':float}
SCHEMA['load_balance']['fast_core']={'enabled':bool,'seed_count':int,'hops':int}
SCHEMA['load_balance']['automatic_pipeline']={'enabled':bool,'max_targets':int,'max_seed_entries':int,
    'planning_seconds':float,'finish_reserve_seconds':float,'min_capacity_bytes':int,'max_entry_candidates':int}
SCHEMA['load_balance']['entry_admission']={'enabled':bool,'interval_seconds':float,
    'min_returns':int,'max_mean_remaining_fraction':float,'counter_stripes':int,
    'score_mode':str,'max_expected_remaining_budget':float}
SCHEMA['load_balance']['capacity_policy']={'mode':str,'small_alpha':float,'large_alpha':float,
    'max_source_total_bytes':int,'max_target_replica_bytes':int}
SCHEMA['load_balance']['online_policy']={'enabled':bool,'decision_interval_ms':float,
    'large_sustained_seconds':float,'min_episode_seconds':float,'episode_cool_seconds':float,
    'recurrence_count':int,'recurrence_window_seconds':float,'entry_stability_min':float,
    'max_observation_gap_seconds':float,'large_cooldown_seconds':float,'large_planning_seconds':float,
    'large_seed_entries':int,'large_max_targets':int,'benefit_horizon_seconds':float,
    'build_amortization_factor':float,'calibration_interval_seconds':float,'holdout_modulus':int,
    'transition_slots':int,'calibration_failure_limit':int,'rejected_entry_cooldown_seconds':float,
    'target_backoff_seconds':float,'publish_busy_target_paused':bool}
SCHEMA['load_balance']['geometry']={
    'cache_directory':str,'cache_threads':int,'cache_block_rows':int,'cache_max_seconds':float,
    'cache_max_bytes':int,'hub_row':int,'tie_tolerance':float,'blas_threads':int,
    'distance_block_rows':int,'group_size':int,'seed_starts':int,'buffer_factors':list,
    'max_plan_ms':int,'byte_price':float}


def _check(values, schema, path=""):
    optional={'listen_backlog','rpc_tcp_nodelay'} if path=='network' else set()
    if path=='':optional={'native_runtime','benchmark_results','load_balance','hotness'}  # Archived plans remain usable.
    if path=='load_balance':optional={'hop_expansion','locality','adaptive_expansion','adaptive_decay','community_poll_ms','community_hops','community_top_k','community_seed_starts','community_hot_closure','community_ppr_rounds','community_ppr_candidates','community_ppr_iterations','community_ppr_vertices','community_ppr_restart','community_ppr_tolerance','community_byte_price','community_max_edge_scans','community_adjacency_bytes','community_max_plan_ms','replica_transfer_mode','producer_queue_bytes','transfer_buffer_budget_bytes','replica_write_workers','replica_loader','planner_mode','planner_max_age_ms','publication_mode','compute_region_quality','replica_id_lookup_bytes','progressive_validation','validation_publish_batch','portal_budget_fraction','native_replica_pack','copy_pressure_source_queue','quality_weight_routing','reuse_observed_baseline','lifecycle_poll_ms','max_total_replica_bytes','selection_policy','target_queue_limit_ms','min_entry_validation_queries','validation_mode','max_validation_queries','shadow_overlap_min','min_replica_expansions','max_early_exit_fraction','quality_walk_steps','entry_weight_floor','entry_weight_power','max_peer_entries','peer_routes_enabled','max_target_shard_fraction'}
    if path=='load_balance':optional.update({'entry_routing','boundary_augmentation','geometry','fast_core','layout_policy','partitioned_regions','automatic_pipeline','capacity_policy','online_policy','entry_admission'})
    if path=='load_balance.automatic_pipeline':optional.add('max_entry_candidates')
    if path=='load_balance.entry_admission':optional.update({'score_mode','max_mean_remaining_fraction','max_expected_remaining_budget'})
    if path=='load_balance.partitioned_regions':optional.add('pipeline')
    if path=='load_balance.partitioned_regions.pipeline':optional.update({'copy_during_expansion','growth_batch_rows','growth_queue_batches','growth_copy_workers'})
    if path=='search':optional={'skip_seen_estimates'}
    if path=='metrics':optional={'snapshot_chunk_bytes','expansion_counter_stripes'}
    if path=='storage':optional={'native_id_lookup_bytes'}
    if path=='native_runtime':optional={'registry_reserve_entries','lifecycle_cleanup_max_records','lifecycle_async_reclaim','lifecycle_reclaim_threads','lifecycle_reclaim_batches','lifecycle_timer_batch','lifecycle_cleanup_max_contexts','compute_threads','tail_cpu_ids','compute_cpu_ids','report_network','result_sink','report_capacity_retry_ms','adaptive_control','network_module','early_parent_report','queued_control_registration','move_head_reports','search_thread_reports','head_maintenance_mode','head_pin_threads','head_entry_network'}
    if path=='native_runtime.network_module':optional={'send_warmup_grace_ms','idle_probe_ms','scale_up_busy_fraction','async_connect_progress','async_connect','cpu_ids','transport','rdma_poll_interval_us','min_connections_per_endpoint_per_reactor','max_connecting_per_reactor','send_pressure_mode','rebalance_connections','rebalance_interval_ms','rebalance_batch'}
    if path=='native_runtime.result_sink':optional={'max_retained_bytes'}
    if path=='peer.replica_pull':optional={'async','retry_pruned_candidates','one_sided','tail_policy','lazy_rerank'}
    if path=='peer.replica_pull.one_sided':optional={'io_threads','record_mapping','doorbell_batch','missing_codes_only','measure_code_availability'}
    if path=='peer.replica_pull.async':optional={'dedicated_dispatcher','shared_compute','coalesce','pin_tail_threads','target_nodes','source_nodes','exclude_pause_from_age','main_help_max_tasks','main_help_reserve_workers','main_help_queue_low'}
    if path=='peer.candidate_sync':optional={'scope','start_threads','start_capacity','coalesce_pending','max_buffered_records','early_budget_report','skip_exhausted_targets','nonblocking_start','early_result','completion_threads','completion_capacity'}
    if path=='peer.connection_pool':optional={'head_scheduling','data_scheduling'}
    if path=='peer.head_pipeline':optional={'ack_inline'}
    if path=='peer.head_direct':optional={'queued_reports','result_channels','report_queue_capacity','report_connections_per_channel','completion_threads','report_backend','report_registration','completion_dispatch','result_backend'}
    if path=='load_balance.hop_expansion':optional={'promote_covered_entries','fast'}
    if path=='peer.demand_growth':optional={'ssd_directories','indexed_directory','persistence_pipeline','persistence_queue_batches','persistence_queue_bytes','maintenance_interval_ms','lock_sample_every'}
    if path=='peer':optional={'demand_growth','candidate_sync','remote_pull','transfer_failure_grace_ms','replica_pull','concurrent_region_admission','region_tasks','region_visit_broadcast','region_visit_queue_capacity','fanout_child_slot_fraction','replica_child_budget_fraction','logical_region_visits','max_excluded_region_ids','deduplicate_node_expansions','allow_node_revisit','min_expansions_after_cross','cross_on_local_exhaustion','locking','adaptive_services','head_receiver_processes','head_receiver_ipc','head_query_workers','head_entry_workers','head_pipeline','head_direct','head_lanes'}
    if not isinstance(values, dict) or (set(values)-set(schema)) or (set(schema)-set(values)-optional):
        missing = set(schema) - set(values) if isinstance(values, dict) else set(schema)
        extra = set(values) - set(schema) if isinstance(values, dict) else set()
        raise ValueError(f"Configuration keys at {path or 'root'}: missing={sorted(missing)}, unknown={sorted(extra)}")
    for key, expected in schema.items():
        if key in optional and key not in values:continue
        value = values[key]
        label = f"{path}.{key}".lstrip(".")
        if isinstance(expected, dict):
            _check(value, expected, label)
        elif expected is float:
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f"{label} must be a finite number")
        elif isinstance(expected, tuple):
            if type(value) not in expected:
                raise ValueError(f"Invalid type for {label}")
        elif type(value) is not expected:
            raise ValueError(f"{label} must be {expected.__name__}")


def validate(c):
    if 'replica_source_min_expansions' in c.get('peer', {}):
        raise ValueError('peer.replica_source_min_expansions was removed; delete this key. '
                         'Replica source actions use the regular expansion policy.')
    _check(c, SCHEMA)
    if not 1<=c['metrics'].get('expansion_counter_stripes',1)<=4096:
        raise ValueError('expansion_counter_stripes must be in [1, 4096]')
    if any(v <= 0 for v in c.get('benchmark_results',{}).values()):
        raise ValueError('benchmark_results limits must be positive')
    h=c.get('hotness')
    if h:
        for key in ('half_life_ms','merge_interval_ms','local_capacity','queue_batches','max_candidates','top_k','max_lanes'):
            if h[key]<=0:raise ValueError('Invalid hotness parameter: '+key)
        if h['top_k']>h['max_candidates'] or h['min_score']<0:raise ValueError('Invalid hotness capacity/score')
        if h['enabled'] and (not c.get('native_runtime',{}).get('data_enabled') or not c['peer'].get('deduplicate_node_expansions',False)):
            raise ValueError('Hotness requires native data runtime and physical-node query deduplication')
    if not 0<=c['peer'].get('fanout_child_slot_fraction',.5)<1:
        raise ValueError('Invalid fanout child slot fraction')
    if c['peer'].get('region_visit_queue_capacity',512)<1:
        raise ValueError('Invalid region visit queue capacity')
    if c['peer'].get('region_tasks',False):
        if not (c['peer'].get('logical_region_visits') and c['peer'].get('deduplicate_node_expansions')):
            raise ValueError('Region tasks require logical regions and physical-node expansion deduplication')
    if c['peer'].get('logical_region_visits',False):
        if not c['peer'].get('allow_node_revisit',False) or c['peer'].get('max_excluded_region_ids',0)<=0:raise ValueError('Logical regions require physical revisits and bounded exclusion capacity')
        if not c.get('native_runtime',{}).get('data_enabled') or not c.get('native_runtime',{}).get('head_enabled'):raise ValueError('Logical regions require native data/head runtime')
        if c['peer']['max_excluded_region_ids']*8+c['data']['dimension']*4+65536>c['network']['max_message_bytes']:raise ValueError('Logical exclusion payload exceeds network frame budget')
    chunk=c['metrics'].get('snapshot_chunk_bytes',262144)
    if not 0<chunk<=min(16777216,c['network']['max_message_bytes']//2):raise ValueError('Invalid metrics snapshot chunk limit')
    lb=c.get('load_balance')
    if lb and lb.get('entry_routing'):
        e=lb['entry_routing']
        if (e['mode'] not in ('fixed','least_queue','cost') or
            (e['mode']!='fixed' and not e['telemetry']) or
            not 1<=e['poll_ms']<=e['ttl_ms']<=60000 or e['temperature_ms']<=0 or e['prior_ms']<=0 or
            e['cpu_threshold']<0 or e['cpu_penalty_ms']<0):
            raise ValueError('Invalid entry_routing')
    if lb is not None:
        admission=lb.get('entry_admission')
        if admission:
            mode=admission.get('score_mode','conditional_remaining_fraction')
            key='max_expected_remaining_budget' if mode=='expected_remaining_per_query' else 'max_mean_remaining_fraction'
            threshold=admission.get(key)
            if (not math.isfinite(admission['interval_seconds']) or admission['interval_seconds']<=0
                    or admission['min_returns']<=0 or not 1<=admission['counter_stripes']<=64
                    or mode not in ('conditional_remaining_fraction','expected_remaining_per_query')
                    or threshold is None or not math.isfinite(threshold) or threshold<0
                    or (mode=='conditional_remaining_fraction' and threshold>1)):
                raise ValueError('Invalid covered entry admission settings')
            if admission['enabled'] and (lb.get('peer_routes_enabled',False)
                    or lb.get('quality_weight_routing',False) or lb.get('publication_mode')!='ready'):
                raise ValueError('Covered entry admission requires ready, unweighted root routing')
        capacity=lb.get('capacity_policy')
        if capacity and (capacity['mode'] not in ('legacy_aggregate','per_region')
                or not 0<capacity['small_alpha']<=.4 or not capacity['small_alpha']<=capacity['large_alpha']<=.8
                or capacity['max_source_total_bytes']<0 or capacity['max_target_replica_bytes']<=0):
            raise ValueError('Invalid independent replica capacity policy')
        online=lb.get('online_policy')
        if online:
            if any(type(v) in (int,float) and (not math.isfinite(v) or v<=0) for v in online.values()):
                raise ValueError('Invalid online replica policy positive limit')
            if (not 0<online['entry_stability_min']<=1 or online['holdout_modulus']<2
                    or not online['min_episode_seconds']<=online['large_sustained_seconds']<=online['recurrence_window_seconds']
                    or online['recurrence_count']<2 or online['large_seed_entries']>4096
                    or online['build_amortization_factor']<1):
                raise ValueError('Invalid online replica persistence/calibration limits')
            if online['enabled'] and (not capacity or capacity['mode']!='per_region'
                    or not lb.get('automatic_pipeline',{}).get('enabled')
                    or not lb.get('adaptive_expansion',{}).get('enabled')
                    or not lb.get('adaptive_decay',{}).get('enabled')
                    or online['large_max_targets']>=len(c['cluster']['nodes'])
                    or online['large_seed_entries']<lb['automatic_pipeline']['max_seed_entries']
                    or not online['large_max_targets']<=online['transition_slots']<lb['max_replicas']
                    or online['large_planning_seconds']>lb['operation_timeout_s']
                    or online['calibration_interval_seconds']*1000>=lb['adaptive_expansion']['cost_max_age_ms']):
                raise ValueError('Online replica policy requires capacity, automatic growth, expansion and decay with fresh calibration')
        expand=lb.get('adaptive_expansion')
        if expand:
            for key,value in expand.items():
                if type(value) in (int,float) and (not math.isfinite(value) or value<=0):
                    raise ValueError('Invalid adaptive_expansion positive parameter: '+key)
            if not 0<expand['source_goal']<=1 or not 0<expand['target_goal']<=1 or (expand['enabled'] and expand['target_goal']>lb['target_util_limit']):
                raise ValueError('Invalid adaptive_expansion utilization goals')
            if not 0<expand['min_fraction']<=expand['fraction_step']<=1:
                raise ValueError('Invalid adaptive_expansion fraction')
            if expand['new_target_cost_margin']<1 or expand['amortization_factor']<1:
                raise ValueError('Invalid adaptive_expansion cost margin')
            if expand['enabled'] and (lb.get('peer_routes_enabled',False) or not c['peer'].get('region_tasks',False)):
                raise ValueError('Multiple copies require root-only routing and region task protocol')
        decay=lb.get('adaptive_decay')
        if decay:
            for key,value in decay.items():
                if type(value) in (int,float) and (not math.isfinite(value) or value<=0):
                    raise ValueError('Invalid adaptive_decay positive parameter: '+key)
            if not decay['fast_half_life_ms']<decay['slow_half_life_ms']<decay['peak_half_life_ms']:
                raise ValueError('Invalid adaptive_decay half-life ordering')
            if not 0<decay['cold_ratio']<decay['reheat_ratio']<=1 or decay['min_keep_qps']>=decay['reheat_qps']:
                raise ValueError('Invalid adaptive_decay hysteresis')
            if not decay['hold_min_ms']<=decay['base_hold_ms']<=decay['hold_max_ms']:
                raise ValueError('Invalid adaptive_decay retention bounds')
            if any(decay[k]>1 for k in ('pressure_discount','history_ewma_weight','history_entry_overlap')):
                raise ValueError('Invalid adaptive_decay fraction')
            if decay['enabled'] and decay['history_limit']<lb['max_replicas']:
                raise ValueError('Adaptive history smaller than replica limit')
        if lb.get('replica_transfer_mode','staged_v1') not in ('staged_v1','pipeline_v2'):raise ValueError('Invalid replica transfer mode')
        if lb.get('replica_loader','python') not in ('python','native'):raise ValueError('Invalid replica loader')
        if lb.get('planner_mode','inline') not in ('inline','native_async'):raise ValueError('Invalid replica planner mode')
        if lb.get('replica_transfer_mode')=='pipeline_v2':
            if not lb.get('native_replica_pack',False):raise ValueError('Pipeline transfer requires native replica extraction')
            if lb.get('producer_queue_bytes',67108864)<lb['chunk_bytes']:raise ValueError('Replica producer queue smaller than one chunk')
            if lb.get('transfer_buffer_budget_bytes',134217728)<(4*lb['inflight_chunks']+2)*lb['chunk_bytes']:raise ValueError('Replica transfer buffer budget too small')
        if lb.get('publication_mode','validated') not in ('validated','ready'):raise ValueError('Invalid replica publication_mode')
        if not lb.get('compute_region_quality',True) and (lb.get('quality_weight_routing',True) or lb.get('peer_routes_enabled',False)):
            raise ValueError('Region scores required by enabled weighted/peer routing')
        if 'locality' in lb:
            loc=lb['locality']
            if not 0<loc['source_capacity_fraction']<1:raise ValueError('Source capacity fraction must lie in (0,1)')
            if not 0<loc['per_region_fraction']<=1 or not 0<=loc['generalization_weight']<=1 or loc['max_regions']<1 or loc['metadata_reserve_bytes']<65536:
                raise ValueError('Invalid locality portfolio configuration')
        if lb.get('selection_policy','entry_worksets') not in ('entry_worksets','hotness_worksets','cpu_worksets','hot_entry_community','entry_geometry'):raise ValueError('Invalid replica selection policy')
        if 'boundary_augmentation' in lb:
            from .boundary_portals import validate_config
            validate_config(lb['boundary_augmentation'])
        if 'geometry' in lb:
            g=lb['geometry']
            if any(g[k]<=0 for k in ('cache_threads','cache_block_rows','cache_max_seconds','cache_max_bytes','tie_tolerance','blas_threads','distance_block_rows','group_size','seed_starts','max_plan_ms'))or g['hub_row']<0 or g['byte_price']<0:
                raise ValueError('Invalid geometry limits')
            if not g['buffer_factors']or any(type(v)not in (int,float)or not math.isfinite(v)or v<0 for v in g['buffer_factors']):raise ValueError('Invalid geometry buffer factors')
        if lb.get('selection_policy')=='entry_geometry':
            if 'geometry'not in lb or not lb['geometry']['cache_directory']:raise ValueError('Geometry directory configuration required')
            if lb.get('planner_mode')!='native_async'or lb['halo_hops']!=0 or lb.get('portal_budget_fraction',0)!=0:raise ValueError('Geometry requires async planning and immutable members')
        if lb.get('selection_policy')=='hot_entry_community':
            required=['community_hops', 'community_top_k', 'community_seed_starts', 'community_hot_closure', 'community_ppr_rounds', 'community_ppr_candidates', 'community_ppr_iterations', 'community_ppr_vertices', 'community_ppr_restart', 'community_ppr_tolerance', 'community_byte_price', 'community_max_edge_scans', 'community_adjacency_bytes', 'community_max_plan_ms']
            if any(k not in lb for k in required):raise ValueError('Missing community planner settings')
            if lb['halo_hops']!=0 or lb.get('portal_budget_fraction',0)!=0:raise ValueError('Community members cannot be changed by halo or portals')
            if lb.get('planner_mode')!='native_async':raise ValueError('Community planning requires native_async')
            if not 1<=lb['community_hops']<=8:raise ValueError('Invalid community hops')
            if not 0<lb['community_ppr_restart']<1 or not 0<lb['community_ppr_tolerance']<1 or lb['community_byte_price']<0:raise ValueError('Invalid community score parameters')
        if lb['enabled'] and lb.get('selection_policy','entry_worksets')=='hotness_worksets' and not c.get('hotness',{}).get('enabled'):raise ValueError('Hotness selection requires enabled hotness statistics')
        if lb['enabled'] and c['peer'].get('logical_region_visits',False) and lb['max_region_rows']>c['peer']['max_excluded_region_ids']:raise ValueError('Replica row cap exceeds full-region exclusion capacity')
        if lb.get('validation_mode','ground_truth') not in ('ground_truth','shadow'):raise ValueError('Invalid replica validation mode')
        for key in ('shadow_overlap_min','max_early_exit_fraction','entry_weight_floor'):
            if key in lb and not 0<lb[key]<=1:raise ValueError('Invalid load_balance fraction: '+key)
        if lb.get('max_target_shard_fraction',1.)<=0:raise ValueError('Invalid replica shard fraction')
        if lb.get('target_queue_limit_ms',1.)<=0:raise ValueError('Invalid target queue limit')
        if lb.get('entry_weight_power',1.)<=0:raise ValueError('Invalid entry weight exponent')
        if lb.get('max_validation_queries',4096)<lb['min_validation_queries']:raise ValueError('Replica validation cap below minimum')
        if lb['mode'] not in ('observe','static','dynamic'):raise ValueError('Invalid load_balance.mode')
        fractions=('source_util_high','target_util_low','target_util_limit','util_gap','min_coverage','initial_fraction','fraction_step','max_fraction','recall_min','recall_margin')
        for key in fractions:
            if not 0<=lb[key]<=1:raise ValueError('Invalid load_balance fraction: '+key)
        for key,value in lb.items():
            if type(value) is int and (value<0 if key in ('halo_hops','replica_id_lookup_bytes','warm_bytes','community_ppr_rounds') else value<=0):raise ValueError('Invalid load_balance integer: '+key)
        if not 0<=lb.get('portal_budget_fraction',0)<1:raise ValueError('Invalid portal budget fraction')
        if lb['max_core_rows']>lb['max_region_rows']:raise ValueError('Replica core exceeds region limit')
        if not 0<lb['copy_pressure_rate_factor']<=1:raise ValueError('Invalid replica pressure rate factor')
        if lb['initial_fraction']>lb['max_fraction']:raise ValueError('Initial replica fraction exceeds maximum')
        if lb['chunk_bytes']+4096>c['network']['max_message_bytes']:raise ValueError('Replica chunk exceeds wire frame')
        if lb['maintenance_min_reactors']>lb['maintenance_max_reactors']:raise ValueError('Invalid maintenance reactor range')
        if lb['copy_mib_per_s']<=0 or lb['operation_timeout_s']<=0:raise ValueError('Invalid maintenance limit')
        if (lb.get('replica_loader')=='native' or lb.get('replica_transfer_mode')=='pipeline_v2') and not 0<lb['operation_timeout_s']<=86400:
            raise ValueError('Native replica deadline must be at most one day')
        if any(n['port']+lb['port_offset']>65535 for n in c['cluster']['nodes']):raise ValueError('Invalid replica service port')
        if lb['enabled'] and (not c.get('native_runtime',{}).get('data_enabled') or not c.get('native_runtime',{}).get('head_enabled') or c['storage']['io']['backend']!='sync_pread'):
            raise ValueError('Load balancing requires native head/data and sync_pread')
    if not 0<c['peer'].get('replica_child_budget_fraction',.5)<1:raise ValueError('Invalid replica child budget fraction')
    if c['peer'].get('min_expansions_after_cross',0)<0:raise ValueError('Minimum local expansions cannot be negative')
    if c['peer'].get('allow_node_revisit',False) and not (c.get('native_runtime',{}).get('head_enabled') and c.get('native_runtime',{}).get('data_enabled')):
        raise ValueError('Bounded revisits require native head/data runtime')
    nr = c.get('native_runtime', {})
    if not 0<=nr.get('registry_reserve_entries',0)<=100000000:
        raise ValueError('native_runtime.registry_reserve_entries must be in [0, 100000000]')
    if not 1 <= nr.get('lifecycle_cleanup_max_contexts',256) <= 65536:
        raise ValueError('native_runtime.lifecycle_cleanup_max_contexts must be in [1, 65536]')
    for key,default,maximum in [('lifecycle_cleanup_max_records',256,65536),('lifecycle_reclaim_threads',2,16),('lifecycle_reclaim_batches',8,1024),('lifecycle_timer_batch',256,65536)]:
        if not 1<=nr.get(key,default)<=maximum:raise ValueError('Invalid native_runtime.'+key)
    if nr.get('head_maintenance_mode','single') not in ('single','per_node'):
        raise ValueError('head_maintenance_mode must be single or per_node')
    sink=nr.get('result_sink')
    if sink is not None:
        if not 1<=sink['writers']<=64 or not 1<=sink['batch_records']<=4096 or sink['batch_bytes']<=0 or sink.get('max_retained_bytes',8<<30)<=0 or sink['completion'] not in ('write','fdatasync'):
            raise ValueError('Invalid native result sink settings')
    if nr.get('report_capacity_retry_ms',1)<=0:
        raise ValueError('report_capacity_retry_ms must be positive')
    report_net=nr.get('report_network')
    if report_net is not None:
        if report_net['connections_per_endpoint_per_reactor']<0:
            raise ValueError('Invalid report_network connections')
        if report_net['enabled'] and not nr.get('network_module',{}).get('enabled',False):
            raise ValueError('report_network requires network_module')
    entry=nr.get('head_entry_network')
    if entry is not None:
        if entry['send_min_reactors']<1 or entry['send_max_reactors']<0 or (entry['send_max_reactors'] and entry['send_min_reactors']>entry['send_max_reactors']) or entry['connections_per_endpoint_per_reactor']<0:
            raise ValueError('Invalid head_entry_network resource bounds')
        if entry['enabled'] and not nr.get('network_module',{}).get('enabled',False):
            raise ValueError('head_entry_network requires network_module')
    if nr.get('data_enabled',False) and nr.get('search_thread_reports',False) and not nr.get('early_parent_report',False):
        raise ValueError('search_thread_reports requires early_parent_report')
    nm=nr.get('network_module')
    if nm is not None:
        if not 1<=nm.get('rdma_poll_interval_us',50)<=100000:
            raise ValueError('rdma_poll_interval_us must be in [1,100000]')
        if nm.get('transport','tcp') not in ('tcp','rdma'):
            raise ValueError('network_module.transport must be tcp or rdma')
        if nm.get('transport','tcp')=='rdma' and not (nm['enabled'] and nr.get('data_enabled') and nr.get('head_enabled')):
            raise ValueError('RDMA requires native data/head and network_module enabled')
        unlimited_keys={'send_max_reactors','receive_max_reactors','connections_per_endpoint_per_reactor','max_receive_connections'}
        if nm.get('send_warmup_grace_ms',0)<0:
            raise ValueError('Invalid send_warmup_grace_ms')
        if any(nm[k]<0 for k in unlimited_keys) or any(v<=0 for k,v in nm.items() if k not in unlimited_keys | {'send_warmup_grace_ms','async_connect_progress','async_connect','cpu_ids','enabled','transport','send_pressure_mode','rebalance_connections','min_connections_per_endpoint_per_reactor'}) or any(nm[x+'_max_reactors'] and nm[x+'_min_reactors']>nm[x+'_max_reactors'] for x in ('send','receive')):
            raise ValueError('Invalid network_module resource bounds')
        if nm.get('min_connections_per_endpoint_per_reactor',0)<0 or (nm['connections_per_endpoint_per_reactor'] and nm.get('min_connections_per_endpoint_per_reactor',0)>nm['connections_per_endpoint_per_reactor']):
            raise ValueError('Invalid warm connection count')
        if not 0<nm.get('scale_up_busy_fraction',0.5)<=1:raise ValueError('Invalid network scale_up_busy_fraction')
        if nm.get('send_pressure_mode','pending') not in ('pending','queued'):
            raise ValueError('Invalid send pressure mode')
        if nm['enabled'] and nr.get('adaptive_control',{}).get('enabled',False):
            raise ValueError('Enable network_module or adaptive_control separately')
    growth=c['peer'].get('demand_growth',{})
    if growth:
        for node,path in growth.get('ssd_directories',{}).items():
            if str(node) not in {str(x['id']) for x in c['cluster']['nodes']} or not isinstance(path,str) or not Path(path).is_absolute():
                raise ValueError('Demand SSD paths must be absolute and identify configured nodes')
        if (not 1<=growth['promotion_hot_vertices']<=growth['cache_records']<=100000 or
            not 0<growth['growth_fraction']<=1 or growth['promotion_trigger'] not in ('full_no_singleton','full_hot_batch') or
            any(growth[k]<=0 for k in ('cache_bytes','max_path_records','max_waiters','max_inflight_batches','read_batch_records','read_batch_bytes','maintenance_bytes','maintenance_timeout_ms','max_events')) or
            growth['training_seconds']<0 or not math.isfinite(growth['training_seconds'])):
            raise ValueError('Invalid demand growth resource/trigger bounds')
        if (growth.get('persistence_queue_batches',2)<=0 or
            growth.get('persistence_queue_bytes',growth['maintenance_bytes']*2)<growth['maintenance_bytes'] or
            growth.get('maintenance_interval_ms',0)<0 or growth.get('lock_sample_every',0)<0):
            raise ValueError('Invalid demand persistence pipeline bounds')
        if growth['enabled']:
            p=c['peer'].get('replica_pull',{})
            if not(p.get('enabled') and p.get('async',{}).get('enabled') and p.get('one_sided',{}).get('enabled')):
                raise ValueError('Demand growth requires asynchronous one-sided READ')
            if c['peer'].get('candidate_sync',{}).get('enabled') or c['peer'].get('remote_pull',{}).get('enabled'):
                raise ValueError('Demand growth does not enable candidate sync or general pull')
            if growth['read_batch_records']>p['batch_records'] or growth['read_batch_bytes']>p['batch_bytes']:
                raise ValueError('Demand READ exceeds native transport bounds')
            if c['load_balance'].get('replica_transfer_mode')!='staged_v1':
                raise ValueError('Demand growth currently requires staged replica publication')
    pull=c['peer'].get('replica_pull')
    sync=c['peer'].get('candidate_sync',{})
    if sync:
        if sync.get('scope','all') not in ('all','replica_source'):
            raise ValueError('Invalid candidate_sync scope')
        if sync['enabled'] and sync.get('scope','all')=='replica_source' and not sync.get('nonblocking_start',False):
            raise ValueError('Replica source scope requires nonblocking candidate start')
        if not 0<=sync.get('start_threads',0)<=128 or not 1<=sync.get('start_capacity',8192)<=1048576:
            raise ValueError('Invalid candidate first-task admission pool')
        if not sync['max_records']<=sync.get('max_buffered_records',4096)<=1048576:
            raise ValueError('Invalid candidate buffered record capacity')
        if not 0<=sync.get('completion_threads',0)<=64 or not 1<=sync.get('completion_capacity',8192)<=1048576:
            raise ValueError('Invalid candidate completion pool')
        if (sync.get('nonblocking_start',False) or sync.get('early_result',False) or sync.get('coalesce_pending',False) or sync.get('start_threads',0)>0) and not sync['enabled']:
            raise ValueError('Nonblocking candidate paths require candidate_sync')
        if not 1<=sync['interval_expansions']<=1000 or not 1<=sync['max_records']<=1024:
            raise ValueError('Invalid candidate_sync packet/expansion bound')
        if sync['enabled'] and (not nr.get('data_enabled') or not nr.get('head_enabled') or not c['peer'].get('concurrent_region_admission') or not c['peer'].get('region_tasks')):
            raise ValueError('candidate_sync requires native head/data and concurrent regions')
        if sync['enabled'] and c.get('load_balance',{}).get('enabled',False) and (not c['peer'].get('logical_region_visits') or c['load_balance'].get('peer_routes_enabled',False)):
            raise ValueError('candidate_sync with LB requires logical regions and root-only replica routing')
    remote=c['peer'].get('remote_pull',{})
    if remote:
        if not 1<=remote['switch_after_batches']<=1024:
            raise ValueError('Invalid remote_pull consecutive batch threshold')
        if remote['enabled']:
            if not nr.get('data_enabled') or not c['peer'].get('region_tasks') or not c['peer'].get('concurrent_region_admission') or not c['peer'].get('allow_node_revisit'):
                raise ValueError('remote_pull requires native concurrent revisitable regions')
            if c.get('load_balance',{}).get('enabled') and not sync.get('enabled',False):
                raise ValueError('remote_pull with LB requires candidate_sync')
            if remote['pull_enabled'] and (not pull or not pull['enabled'] or not pull.get('async',{}).get('enabled') or not pull.get('one_sided',{}).get('enabled')):
                raise ValueError('remote_pull requires asynchronous one-sided RDMA')
            if pull and (pull.get('lazy_rerank',{}).get('enabled') or pull.get('tail_policy',{}).get('enabled') or pull.get('async',{}).get('coalesce',{}).get('enabled')):
                raise ValueError('remote_pull does not combine with replica tail policies')
            if remote['pull_enabled'] and (pull['async'].get('target_nodes') or pull['async'].get('source_nodes')):
                raise ValueError('remote_pull requires read support on every data node')
    if pull is not None:
        if any(pull[k]<=0 for k in ('max_remaining','max_rounds','batch_records','batch_bytes','max_bytes')) or pull['batch_records']>1024 or pull['max_bytes']<pull['batch_bytes'] or pull['batch_bytes']+65536>c['network']['max_message_bytes']:
            raise ValueError('Invalid replica_pull resource bounds')
        if pull['enabled'] and (not nr.get('data_enabled') or not c['peer'].get('region_tasks') or not c['peer'].get('concurrent_region_admission')):
            raise ValueError('Replica pull requires native concurrent region runtime')
        if pull['enabled'] and pull['require_rdma'] and (nm or {}).get('transport')!='rdma':
            raise ValueError('Replica pull requires RDMA transport')
    if pull:
        t=pull.get('tail_policy',{})
        if t and (not 0<=t['min_expansions']<=t['max_expansions']<=1024 or t['max_expansions']<1 or t['stable_batches']<0
                  or any(not math.isfinite(t[k]) or t[k]<0 for k in ('uncertainty_weight','improvement_margin'))):
            raise ValueError('Invalid tail policy bounds')
        l=pull.get('lazy_rerank',{})
        if l and (not 1<=l['max_vectors']<=1024 or not math.isfinite(l['uncertainty_weight']) or l['uncertainty_weight']<0
                  or (l['enabled'] and (not pull.get('one_sided',{}).get('enabled') or not pull.get('async',{}).get('enabled')))):
            raise ValueError('Lazy rerank requires bounded asynchronous one-sided reads')
        shared=pull.get('async',{}).get('shared_compute',{})
        if shared and (not 0<shared['tail_fraction']<1 or not 0<shared['credit_limit_ms']<=100
                       or (shared['enabled'] and not pull['async']['enabled'])):
            raise ValueError('Invalid shared compute bounds')
    if pull and 'one_sided' in pull:
        o=pull['one_sided'];a=pull.get('async',{})
        threads=o.get('io_threads',1)
        if (not 1<=threads<=16 or threads>o['lanes_per_source'] or o['lanes_per_source']%threads
                or o['max_connections']%threads or a.get('max_contexts',1)<threads):
            raise ValueError('Invalid one-sided I/O shards')
        if (o.get('record_mapping','private_cow') not in ('private_cow','read_only') or not 1<=o['port']<=255 or not 1<=o['lanes_per_source']<=o['max_connections']<=64
                or not 1<=o['poll_interval_us']<=10000 or not 1<=o['timeout_ms']<=120000
                or not 16<=o['max_directory_bytes']<=o['max_registered_bytes']
                or (o['enabled'] and (not a.get('enabled') or a.get('coalesce',{}).get('enabled')))):
            raise ValueError('Invalid one-sided pull bounds or transport')
    if pull and 'async' in pull:
        a=pull['async']
        if (a['tail_threads']<0 or (a['enabled'] and a['tail_threads']==0 and not a.get('shared_compute',{}).get('enabled')) or a['source_read_threads']<0
                or any(a[k]<=0 for k in ('max_contexts','max_inflight_per_source','response_bytes','source_read_queue','send_reactors','connections_per_reactor'))
                or a['max_inflight_per_source']>a['max_contexts']
                or a['response_bytes']//pull['batch_bytes']<a['max_contexts']):
            raise ValueError('Invalid asynchronous pull capacity')
        if (any(a.get(k,0)<0 for k in ('main_help_max_tasks','main_help_reserve_workers','main_help_queue_low'))
                or a.get('main_help_max_tasks',0)>c['peer']['worker_threads']
                or (a.get('main_help_max_tasks',0)>0 and a.get('main_help_reserve_workers',0)>=c['peer']['worker_threads'])):
            raise ValueError('Invalid asynchronous pull main help bounds')
        b=a.get('coalesce',{})
        if b:
            if (not 1<=b['max_queries']<=32 or not 0<=b['wait_us']<=10000
                    or b['max_bytes']<pull['batch_bytes'] or b['max_bytes']>a['response_bytes']
                    or b['max_bytes']+262144>c['network']['max_message_bytes']
                    or b['max_queries']*pull['batch_records']>1024
                    or (b['enabled'] and (not a['enabled'] or not a['source_read_threads']))):
                raise ValueError('Invalid coalesced pull bounds')
        for key in ('target_nodes','source_nodes'):
            ids=a.get(key,[])
            if any(type(i) is not int or i<0 or i>=len(c['cluster']['nodes']) for i in ids) or len(ids)!=len(set(ids)):
                raise ValueError('Invalid asynchronous pull node list: '+key)
    partition=c.get('load_balance',{}).get('partitioned_regions')
    auto=c.get('load_balance',{}).get('automatic_pipeline')
    if auto:
        lb=c['load_balance'];pipe=(partition or {}).get('pipeline',{})
        entry_candidates=auto.get('max_entry_candidates',0)
        if not lb.get('entry_admission',{}).get('enabled') and entry_candidates and (not auto['max_seed_entries']<=entry_candidates<=4096 or auto['max_targets']!=1):
            raise ValueError('Automatic entry promotion requires one target and bounded historical candidates')
        if (not 1<=auto['max_targets']<=64 or not 1<=auto['max_seed_entries']<=4096
                or not math.isfinite(auto['planning_seconds']) or not math.isfinite(auto['finish_reserve_seconds'])
                or not 0<auto['finish_reserve_seconds']<auto['planning_seconds']<=86400
                or auto['min_capacity_bytes']<=65536):
            raise ValueError('Invalid automatic pipeline limits')
        if auto['enabled'] and (auto['max_targets']>=len(c['cluster']['nodes']) or auto['max_seed_entries']>lb.get('community_top_k',64)
                or auto['planning_seconds']>lb['operation_timeout_s'] or lb.get('planner_mode')!='native_async' or not lb.get('locality',{}).get('enabled')
                or not pipe.get('enabled') or not pipe.get('adjacency_cache') or not pipe.get('copy_during_expansion')
                or lb.get('replica_transfer_mode')!='pipeline_v2' or not lb.get('native_replica_pack')):
            raise ValueError('Automatic pipeline requires bounded cached growth maintenance')
    if partition and (not 0<partition['total_alpha']<=.8 or not 1<=partition['hops']<=8
                      or not math.isfinite(partition['balance_weight']) or partition['balance_weight']<0
                      or not math.isfinite(partition['deadline_s']) or partition['deadline_s']<=0
                      or partition['max_edge_scans']<=0 or not partition['target_weights']
                      or any(not isinstance(k,str) or not k.isdecimal() or int(k)<0 or int(k)>=len(c['cluster']['nodes']) or not isinstance(v,(int,float)) or not math.isfinite(v) or v<=0 for k,v in partition['target_weights'].items())):
        raise ValueError('Invalid partitioned region configuration')
    if partition:
        from .replica_layout import pipeline_settings
        pipeline_settings(partition)
    layout=c.get('load_balance',{}).get('layout_policy')
    if layout and (not 0<layout['small_alpha']<=.4 or not layout['small_alpha']<=layout['large_alpha']<=.8
                   or not 0<layout['capacity_headroom']<=1 or not 0<layout['share_step']<=1
                   or not 0<layout['recall_min']<=1 or not 1<=layout['coordinate_passes']<=100
                   or any(not math.isfinite(v) or v<0 for v in layout.values())
                   or layout['large_sustained_windows']<1 or layout['large_recurring_windows']<1):
        raise ValueError('Invalid replica layout policy')
    fast_core=c.get('load_balance',{}).get('fast_core',{})
    if fast_core and (fast_core['seed_count']<=0 or not 1<=fast_core['hops']<=8 or (fast_core['enabled'] and fast_core['hops']>c['load_balance'].get('community_hops',3))):
        raise ValueError('Invalid complete fast core limits')
    hop=c.get('load_balance',{}).get('hop_expansion')
    if hop is not None and (not 0<hop['core_fraction']<=1 or not 0<=hop['history_weight']<=1 or any(hop[k]<=0 for k in ('rounds','candidates','cut_steps'))):
        raise ValueError('Invalid hop expansion parameters')
    if not 0<=nr.get('compute_threads',0)<=1024:raise ValueError('Invalid native compute thread count')
    for ids in (nr.get('compute_cpu_ids',[]),nr.get('tail_cpu_ids',[]),(nm or {}).get('cpu_ids',[])):
        if len(ids)!=len(set(ids)) or any(type(i) is not int or i<0 or i>=1024 for i in ids):raise ValueError('Invalid native CPU list')
    ac=nr.get('adaptive_control')
    if ac is not None and (any(v<=0 for k,v in ac.items() if k!='enabled') or ac['min_lanes']>ac['max_lanes'] or ac['max_lanes']>64):
        raise ValueError('Invalid adaptive_control lane bounds or thresholds')
    if nr.get('data_enabled', False) or nr.get('head_enabled', False):
        if (c['peer']['execution_model'] != 'native' or c['search']['execution'] != 'peer'
                or c['network']['transport'] != 'tcp' or c['storage']['io']['backend'] != 'sync_pread'
                or c['peer']['control']['scheduler'] != 'native'
                or c['peer']['communication']['mode'] != 'optimized'):
            raise ValueError('C++ runtime requires native peer/search/control, optimized TCP and sync_pread')
    if nr.get('head_enabled',False) and (any(c['peer'].get(key,{}).get('enabled',False) for key in ('head_lanes','head_direct','head_pipeline'))
                               or c['peer'].get('head_receiver_processes',0) or c['peer'].get('head_query_workers',0)):
        raise ValueError('C++ head replaces the Python head pools; disable their configuration switches')
    if c['runtime']['python_switch_interval_ms']<=0:
        raise ValueError('runtime.python_switch_interval_ms must be positive')
    if any(type(qid) is not int or not 0<=qid<2**64 for qid in c['metrics']['trace_query_ids']):
        raise ValueError('metrics.trace_query_ids must contain uint64 integers')
    if c["version"] != 1 or not 0 <= c["seed"] < 2**32:
        raise ValueError("Unsupported config version or seed")
    for section, key, choices in [
        ("data", "metric", ("l2", "cosine")),
        ("partition", "method", ("margin", "boundary", "relative_gap")),
        ("scheduler", "mode", ("adaptive", "reference", "local_first")),
        ("network", "transport", ("local", "tcp")),
        ("search", "execution", ("peer", "reference")),
        ("peer", "execution_model", ("blocking", "resumable", "native")),
        ("scheduler", "remote_selection", ("probabilistic", "threshold")),
    ]:
        if c[section][key] not in choices:
            raise ValueError(f"Invalid {section}.{key}")
    zero_allowed = {("data", "row_limit"), ("storage", "cache_bytes_per_shard"), ("storage", "native_id_lookup_bytes"),
                    ("search", "max_fallback_entries"), ("network", "max_retries"), ("query", "row"),
                    ("experiment", "warmup_queries")}
    zero_allowed.add(("peer", "min_expansions_after_cross"))
    zero_allowed.add(("peer", "head_port"))
    zero_allowed.add(("peer", "head_receiver_processes"))
    zero_allowed.add(("peer", "head_query_workers"))
    receivers=c['peer'].get('head_receiver_processes',0)
    pipeline=c['peer'].get('head_pipeline',{})
    direct=c['peer'].get('head_direct',{})
    lanes=c['peer'].get('head_lanes',{})
    if lanes:
        if any(type(v)is int and v<1 for v in lanes.values()) or lanes['startup_timeout_seconds']<=0:
            raise ValueError('Invalid head lane capacity')
        if lanes['processes']>32 or c['peer']['head_port']+lanes['processes']-1>65535:
            raise ValueError('Invalid head lane endpoint range')
        if lanes['enabled'] and (receivers or c['peer'].get('head_query_workers',0) or pipeline.get('enabled') or direct.get('enabled') or
                c['peer']['communication']['mode']!='optimized' or c['network']['transport']!='tcp' or c['search']['execution']!='peer'):
            raise ValueError('Head lanes require exclusive optimized TCP peer mode')
    if direct:
        if direct.get('result_backend','process') not in ('process','inline'):raise ValueError('Invalid result backend')
        if direct.get('result_backend','process')=='inline' and not (direct.get('queued_reports') and direct.get('report_backend')=='process'):
            raise ValueError('Inline results require independent process report receivers')
        if direct.get('report_backend','thread') not in ('thread','process'):raise ValueError('Invalid report backend')
        if direct.get('report_registration','ipc') not in ('ipc','shared'):raise ValueError('Invalid report registration')
        if direct.get('completion_dispatch','queued') not in ('queued','inline'):raise ValueError('Invalid completion dispatch')
        if any(type(v)is int and (v<0 if k=='submit_retries' else v<1) for k,v in direct.items()):
            raise ValueError('Invalid head_direct capacity or retries')
        if direct['startup_timeout_seconds']<=0:raise ValueError('Invalid head_direct startup timeout')
        if direct['enabled'] and (receivers or c['peer'].get('head_query_workers',0) or pipeline.get('enabled') or
                c['peer']['communication']['mode']!='optimized' or c['network']['transport']!='tcp' or c['search']['execution']!='peer'):
            raise ValueError('head_direct requires exclusive optimized TCP peer mode')
        if direct['confirmation_threads']>32 or c['peer']['head_port']+direct['confirmation_threads']-1>65535:
            raise ValueError('Invalid head_direct confirmation endpoint range')
        if direct.get('result_channels',8)>64 or direct.get('completion_threads',2)>32:
            raise ValueError('Invalid report channel or completion worker count')
        if direct.get('queued_reports',False) and c['peer']['head_port']+direct['confirmation_threads']+direct.get('result_channels',8)-1>65535:
            raise ValueError('Invalid head_direct report endpoint range')
    if pipeline:
        if any(type(v) is int and v<1 for v in pipeline.values()):raise ValueError('Invalid head_pipeline capacity')
        if not 1<=pipeline['receive_min']<=pipeline['receive_max']<=4:raise ValueError('head_pipeline receive threads must be in [1,4]')
        if pipeline['enabled'] and (receivers or c['peer'].get('head_query_workers',0) or
                c['peer']['communication']['mode']!='optimized' or c['network']['transport']!='tcp' or c['search']['execution']!='peer'):
            raise ValueError('head_pipeline requires single-process optimized TCP peer mode')
    if c['peer'].get('head_receiver_ipc','shared') not in ('shared','split'):
        raise ValueError('Invalid peer.head_receiver_ipc')
    if c['peer'].get('head_query_workers',0) and (not receivers or c['peer'].get('head_receiver_ipc','shared')!='split'):
        raise ValueError('head_query_workers requires independent receivers and split IPC')
    if receivers>32 or (receivers and c['peer']['communication']['mode']!='optimized'):
        raise ValueError('head_receiver_processes requires optimized mode and must be in [0,32]')
    if c['peer']['head_port']+max(0,receivers-1)>65535:
        raise ValueError('Head receiver port range exceeds 65535')
    zero_allowed.update({("native_runtime", "registry_reserve_entries"),("native_runtime", "compute_threads"),("load_balance", "warm_bytes"),("load_balance", "replica_id_lookup_bytes"),("load_balance", "halo_hops"), ("scheduler", "remote_weight"), ("scheduler", "kappa"),
                         ("scheduler", "remote_rtt_ms"), ("peer", "transfer_failure_grace_ms")})
    for section, fields in c.items():
        if not isinstance(fields, dict):
            continue
        for key, value in fields.items():
            if type(value) is int and value < (0 if (section, key) in zero_allowed else 1):
                raise ValueError(f"Invalid nonpositive {section}.{key}")
    p, r, s, net, sched = (c[k] for k in ("partition", "routing", "search", "network", "scheduler"))
    nodes = c["cluster"]["nodes"]
    if not nodes:
        raise ValueError("At least one data node required")
    for i, node in enumerate(nodes):
        _check(node, {"id": int, "host": str, "port": int, "index_directory": (str, type(None))}, "cluster.nodes")
        if node["id"] != i or not node["host"] or not 1 <= node["port"] <= 65535:
            raise ValueError("Node IDs must be consecutive from zero; host and port required")
    if len({(n["host"], n["port"]) for n in nodes}) != len(nodes):
        raise ValueError("Duplicate node endpoint")
    if not 0 <= p["overlap_fraction"] <= 1 or p["kmeans_tolerance"] < 0:
        raise ValueError("Invalid overlap fraction or k-means tolerance")
    if not 1 <= p["max_copies"] <= len(nodes):
        raise ValueError("max_copies must not exceed number of data nodes")
    if not 64 <= c["data"]["dimension"] <= 65536 or not 1 <= c["quantization"]["bits"] <= 9:
        raise ValueError("Unsupported RaBitQ dimension or bits")
    alphas = c["build"]["prune_alphas"]
    if (not alphas or any(type(a) not in (int, float) or not math.isfinite(a) or a < 1 for a in alphas)
            or c["build"]["candidate_list_size"] < c["build"]["degree"]):
        raise ValueError("Invalid Vamana pruning parameters")
    if not 0 < r["sample_ratio"] <= 1 or r["min_samples"] > r["max_samples"] or r["hnsw_m"] < 2:
        raise ValueError("Invalid routing sample settings")
    if r["ef_construction"] < r["hnsw_m"] or r["ef_search"] < r["entry_candidates"]:
        raise ValueError("HNSW ef must cover M / entry candidates")
    if s["frontier_size"] < s["top_k"] or s["max_expansions"] < s["top_k"]:
        raise ValueError("Search frontier and budget must cover top_k")
    if sched["mode"] == "reference" and s["max_inflight"] != 1:
        raise ValueError("Reference order requires max_inflight=1")
    if (sched["remote_weight"] < 0 or sched["probability_temperature"]<=0
        or sched["kappa"] < 0 or any(sched[k] <= 0 for k in
        ("exploration", "max_wait_ms", "local_batch_ms", "bandwidth_bytes_per_ms"))
        or sched["remote_rtt_ms"] < 0 or not 0 < sched["ewma_weight"] <= 1):
        raise ValueError("Invalid scheduler costs/weights")
    if net["timeout_seconds"] <= 0 or net["max_message_bytes"] < c["data"]["dimension"]*4+1024:
        raise ValueError("Network timeout/message bound too small")
    page = c["storage"]["page_bytes"]
    if page < 512 or page & (page-1):
        raise ValueError("page_bytes must be a power of two >= 512")
    io = c['storage']['io']
    if io['backend'] not in ('io_uring','thread_pool','sync_pread') or io['fallback_backend'] not in ('error','thread_pool'):
        raise ValueError('Invalid storage.io backend')
    if any(type(v) is int and v <= 0 for v in io.values()):
        raise ValueError('storage.io capacities must be positive')
    if c['peer']['execution_model']=='resumable' and (s['execution']!='peer' or net['transport']!='tcp'):
        raise ValueError('resumable execution requires peer search over TCP data services')
    if c['peer']['execution_model']=='resumable' and io['backend']=='sync_pread':
        raise ValueError('Resumable execution requires an asynchronous I/O backend')
    if c['peer']['execution_model']=='native':
        if s['execution']!='peer' or c['storage']['cache_bytes_per_shard']!=0:
            raise ValueError('Native execution requires peer search and disabled application cache')
        if net['transport']=='local' and io['backend']!='sync_pread':
            raise ValueError('Native local simulation supports sync_pread; use TCP for shared asynchronous I/O')
    exp = c["experiment"]
    peer = c["peer"]
    if peer['admission']['mode'] not in ('direct','buffered') or peer['admission']['queue_capacity']<=0:
        raise ValueError('Invalid peer.admission settings')
    if peer['admission']['queue_lock'] not in ('shared','independent'):
        raise ValueError('Invalid peer.admission.queue_lock')
    if peer['connection_pool'].get('data_scheduling','broadcast') not in ('fifo','broadcast'):
        raise ValueError('Invalid data connection scheduling')
    locking=peer.get('locking',{})
    if locking.get('split_native_registration',False) and (peer['control']['scheduler']!='native' or
            peer['execution_model']!='native' or peer['admission']['mode']!='buffered'):
        raise ValueError('Split registration requires native control/search and buffered admission')
    adaptive=peer.get('adaptive_services')
    if adaptive:
        for name in ('admission','head_dispatch','transfer','connections'):
            if not 1<=adaptive[name+'_min']<=adaptive[name+'_max']:
                raise ValueError('Invalid adaptive service worker bounds')
        if any(adaptive[k]<=0 for k in ('sample_interval_ms','target_wait_ms','cooldown_ms',
                                       'downscale_idle_ms','growth_step','history_size')):
            raise ValueError('Adaptive timing/capacity must be positive')
        if not 0<adaptive['ewma_weight']<=1 or not 0<adaptive['target_utilization']<=1:
            raise ValueError('Invalid adaptive EWMA/utilization')
        if adaptive['enabled'] and adaptive['admission'] and (peer['admission']['mode']!='buffered' or
                peer['compute']['mode']!='fixed' or peer['admission']['queue_lock']!='independent'):
            raise ValueError('Adaptive admission requires buffered/independent admission and fixed compute')
        if adaptive['enabled'] and adaptive['connections'] and (peer['communication']['mode']!='optimized' or not peer['communication']['reuse_connections']):
            raise ValueError('Adaptive connections require reusable optimized communication')
        if (adaptive['submit_without_state_lock'] or adaptive['start_without_state_lock']) and (peer['compute']['mode']!='fixed' or peer['execution_model']!='native'):
            raise ValueError('Lock-free submission requires native fixed compute')
        if adaptive['enabled'] and adaptive['transfer'] and peer['sender']['mode']!='queued':
            raise ValueError('Adaptive transfer requires queued sender')
    if peer['admission']['dispatch_threads']<=0:
        raise ValueError('peer.admission.dispatch_threads must be positive')
    comm=peer['communication']
    if (peer['sender']['mode'] not in ('executor','queued')
            or peer['sender']['transfer_queue_capacity']<=0):
        raise ValueError('Invalid peer.sender settings')
    if peer['compute']['mode'] not in ('executor','prestarted','fixed') or peer['compute']['queue_capacity']<=0:
        raise ValueError('Invalid peer.compute settings')
    if peer['compute']['mode']=='fixed' and peer['execution_model']!='native':
        raise ValueError('Fixed compute queue currently requires native execution')
    if peer['connection_pool']['probe_mode'] not in ('locked','reserved'):
        raise ValueError('Invalid peer.connection_pool.probe_mode')
    if peer['connection_pool'].get('head_scheduling','broadcast') not in ('broadcast','fifo'):
        raise ValueError('Invalid peer.connection_pool.head_scheduling')
    if not 0<=peer['control']['initial_heartbeat_delay_ms']<=peer['heartbeat_interval_ms']:
        raise ValueError('Initial heartbeat delay must be between zero and heartbeat_interval_ms')
    if peer['control']['scheduler'] not in ('scan','event','native'):
        raise ValueError('Invalid peer.control.scheduler')
    if peer['control']['max_events_per_turn']<0:
        raise ValueError('peer.control.max_events_per_turn must be nonnegative')
    if peer['control']['scheduler'] in ('event','native') and comm['mode']!='optimized':
        raise ValueError('Event control scheduler requires optimized communication')
    if peer['control']['scheduler']=='native':
        if (peer['execution_model']!='native' or c['network']['transport']!='tcp'
                or c['storage']['io']['backend']!='sync_pread'):
            raise ValueError('Native control requires native search, TCP and sync_pread')
        if peer['control']['completion_fast_path']:
            raise ValueError('Native control does not use Python completion_fast_path')
        if not comm['reuse_connections']:
            raise ValueError('Native control requires reuse_connections')
    if (comm['mode'] not in ('legacy','optimized') or any(type(v) is int and v<=0 for v in comm.values())
            or comm['max_pending_control']<comm['control_workers']):
        raise ValueError('Invalid peer.communication settings')
    if (peer["lease_timeout_ms"] <= 3*peer["heartbeat_interval_ms"]
            or not 0 < peer["child_budget_fraction"] < 1
            or any(peer[k] <= 0 for k in ("query_timeout_seconds","control_timeout_seconds",
                                         "handoff_timeout_seconds","retention_seconds"))
            or not peer["head_bind_host"] or not peer["head_advertise_host"]
            or peer["head_advertise_host"] in ("0.0.0.0", "::")
            or not 0 <= peer["head_port"] <= 65535):
        raise ValueError("Invalid peer lifecycle settings")
    if not 0 <= peer.get('transfer_failure_grace_ms',0) <= peer['query_timeout_seconds']*1000:
        raise ValueError('Invalid peer.transfer_failure_grace_ms')
    if peer["head_port"] and exp["benchmark_concurrency"] > 1 and comm['mode']=='legacy':
        raise ValueError("Use head_port=0 for multiple benchmark Engine instances")
    if (not exp["metrics"] or any(m not in ("l2", "cosine") for m in exp["metrics"])
            or len(set(exp["metrics"])) != len(exp["metrics"])):
        raise ValueError("Invalid experiment metrics")
    for key, low, high, expected in (("overlap_fractions", 0, 1, (int,float)), ("seeds", 0, 2**32-1, (int,))):
        if not exp[key] or any(type(v) not in expected or not low <= v <= high for v in exp[key]) or len(set(exp[key])) != len(exp[key]):
            raise ValueError(f"Invalid experiment.{key}")
    if not 0 < exp["target_recall"] <= 1:
        raise ValueError("target_recall must be in (0,1]")


def load_config(path):
    path = Path(path).resolve()
    raw = yaml.load(path.read_text(), Loader=StrictLoader)
    migration=[]
    if isinstance(raw,dict) and isinstance(raw.get('metrics'),dict) and 'lifecycle_trace' not in raw['metrics']:
        raw['metrics']['lifecycle_trace']=False
        migration.append('metrics.lifecycle_trace=False (archived trace selection)')
    if isinstance(raw,dict) and isinstance(raw.get('metrics'),dict) and 'gc_trace' not in raw['metrics']:
        raw['metrics']['gc_trace']=False
        migration.append('metrics.gc_trace=False (archived GC diagnostics disabled)')
    if isinstance(raw,dict) and isinstance(raw.get('runtime'),dict) and 'python_switch_interval_ms' not in raw['runtime']:
        raw['runtime']['python_switch_interval_ms']=5.0
        migration.append('runtime.python_switch_interval_ms=5.0 (archived Python switch interval)')
    if isinstance(raw,dict) and isinstance(raw.get('metrics'),dict) and 'trace_query_ids' not in raw['metrics']:
        raw['metrics']['trace_query_ids']=[]
        migration.append('metrics.trace_query_ids=[] (archived global trace selection)')
    if isinstance(raw,dict) and isinstance(raw.get('network'),dict) and 'listen_backlog' not in raw['network']:
        raw['network']['listen_backlog']=5
        migration.append('network.listen_backlog=5 (legacy Python TCP default)')
    # Old v1 archives predate both async fields. Keep their blocking semantics;
    # hydrate inactive I/O settings from the single canonical YAML, and archive
    # the resulting values. Partly specified new configs still fail strictly.
    if (isinstance(raw,dict) and raw.get('version')==1 and isinstance(raw.get('peer'),dict)
            and isinstance(raw.get('storage'),dict) and 'execution_model' not in raw['peer']
            and 'io' not in raw['storage']):
        defaults=yaml.load((Path(__file__).resolve().parents[2]/'configs/dhdgs_pilot.yaml').read_text(),Loader=StrictLoader)
        raw['peer']['execution_model']='blocking'
        raw['storage']['io']=copy.deepcopy(defaults['storage']['io'])
        migration+=['peer.execution_model=blocking','storage.io (inactive canonical defaults)']
    if isinstance(raw,dict) and isinstance(raw.get('peer'),dict) and 'communication' not in raw['peer']:
        defaults=yaml.load((Path(__file__).resolve().parents[2]/'configs/dhdgs_pilot.yaml').read_text(),Loader=StrictLoader)
        raw['peer']['communication']=copy.deepcopy(defaults['peer']['communication'])
        raw['peer']['communication']['mode']='legacy'
        migration.append('peer.communication.mode=legacy (archived protocol behavior); inactive capacities from canonical YAML')
    if isinstance(raw,dict) and isinstance(raw.get('peer'),dict) and 'admission' not in raw['peer']:
        defaults=yaml.load((Path(__file__).resolve().parents[2]/'configs/dhdgs_pilot.yaml').read_text(),Loader=StrictLoader)
        raw['peer']['admission']=copy.deepcopy(defaults['peer']['admission'])
        raw['peer']['admission']['mode']='direct'
        migration.append('peer.admission.mode=direct (archived synchronous executor submission)')
    if isinstance(raw,dict) and isinstance(raw.get('peer'),dict) and 'sender' not in raw['peer']:
        defaults=yaml.load((Path(__file__).resolve().parents[2]/'configs/dhdgs_pilot.yaml').read_text(),Loader=StrictLoader)
        raw['peer']['sender']=copy.deepcopy(defaults['peer']['sender'])
        raw['peer']['sender']['mode']='executor'
        migration.append('peer.sender.mode=executor (archived communication submission)')
    if isinstance(raw,dict) and isinstance(raw.get('peer'),dict) and 'compute' not in raw['peer']:
        defaults=yaml.load((Path(__file__).resolve().parents[2]/'configs/dhdgs_pilot.yaml').read_text(),Loader=StrictLoader)
        raw['peer']['compute']=copy.deepcopy(defaults['peer']['compute'])
        raw['peer']['compute']['mode']='executor'
        migration.append('peer.compute.mode=executor (archived on-demand compute workers)')
    if isinstance(raw,dict) and isinstance(raw.get('peer'),dict) and 'connection_pool' not in raw['peer']:
        raw['peer']['connection_pool']={'probe_mode':'locked'}
        migration.append('peer.connection_pool.probe_mode=locked (archived connection health checks under pool lock)')
    if isinstance(raw,dict) and isinstance(raw.get('peer'),dict) and 'control' not in raw['peer']:
        raw['peer']['control']={'initial_heartbeat_delay_ms':0,'scheduler':'scan'}
        migration.append('peer.control.initial_heartbeat_delay_ms=0 (archived immediate initial heartbeat)')
        migration.append('peer.control.scheduler=scan (archived control table scans)')
    elif (isinstance(raw,dict) and isinstance(raw.get('peer'),dict)
          and isinstance(raw['peer'].get('control'),dict)
          and 'initial_heartbeat_delay_ms' in raw['peer']['control']
          and 'scheduler' not in raw['peer']['control']):
        raw['peer']['control']['scheduler']='scan'
        migration.append('peer.control.scheduler=scan (archived control table scans)')
    if (isinstance(raw,dict) and isinstance(raw.get('peer'),dict)
          and isinstance(raw['peer'].get('control'),dict)
          and 'max_events_per_turn' not in raw['peer']['control']):
        raw['peer']['control']['max_events_per_turn']=0
        migration.append('peer.control.max_events_per_turn=0 (archived unbounded event consumption)')
    if (isinstance(raw,dict) and isinstance(raw.get('peer'),dict)
          and isinstance(raw['peer'].get('admission'),dict)
          and 'queue_lock' not in raw['peer']['admission']):
        raw['peer']['admission']['queue_lock']='shared'
        migration.append('peer.admission.queue_lock=shared (archived queue shares branch-state lock)')
    if (isinstance(raw,dict) and isinstance(raw.get('peer'),dict)
          and isinstance(raw['peer'].get('admission'),dict)
          and 'dispatch_threads' not in raw['peer']['admission']):
        raw['peer']['admission']['dispatch_threads']=1
        migration.append('peer.admission.dispatch_threads=1 (archived single admission consumer)')
    if (isinstance(raw,dict) and isinstance(raw.get('peer'),dict)
          and isinstance(raw['peer'].get('control'),dict)
          and 'completion_fast_path' not in raw['peer']['control']):
        raw['peer']['control']['completion_fast_path']=False
        migration.append('peer.control.completion_fast_path=False (archived queued local completion)')
    if isinstance(raw,dict) and 'native_runtime' not in raw:
        defaults=yaml.load((Path(__file__).resolve().parents[2]/'configs/dhdgs_pilot.yaml').read_text(),Loader=StrictLoader)
        raw['native_runtime']=copy.deepcopy(defaults['native_runtime'])
        raw['native_runtime'].update(data_enabled=False,head_enabled=False)
        raw['native_runtime']['network_module'].update(enabled=False,transport='tcp',cpu_ids=[])
        raw['native_runtime']['head_entry_network']['enabled']=False
        raw['native_runtime']['report_network']['enabled']=False
        raw['native_runtime']['compute_cpu_ids']=[]
        migration.append('native_runtime disabled (archived Python runtime)')
    validate(raw)
    c = copy.deepcopy(raw)
    for section, key in (("paths","dataset"), ("paths","output"), ("query","vectors"), ("query","ids"),
                         ("assignment", "model"), ("assignment", "threshold")):
        if c[section][key] is not None:
            c[section][key] = str((path.parent / c[section][key]).resolve())
    for node in c["cluster"]["nodes"]:
        if node["index_directory"] is not None:
            node["index_directory"] = str((path.parent / node["index_directory"]).resolve())
    output, source = Path(c["paths"]["output"]), Path(c["paths"]["dataset"])
    if output == source or output.is_relative_to(source):
        raise ValueError("Output must be outside source dataset")
    c["_config_migration"] = migration
    c["_config_path"] = str(path)
    c["_config_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return c


def snapshot(c, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "resolved.yaml"
    with path.open("x") as handle:
        yaml.safe_dump({k: v for k, v in c.items() if not k.startswith("_")}, handle,
                       allow_unicode=True, sort_keys=False)
    from .data import write_json
    write_json(directory / "config_provenance.json", {
        "source": c["_config_path"], "source_sha256": c["_config_sha256"],
        "migration":c.get("_config_migration",[]),
        "resolved_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})


def index_settings(c):
    """Settings whose modification requires a new immutable index."""
    return {"seed": c["seed"], "data": {k: c["data"][k] for k in ("metric", "dimension", "row_limit")},
            "partition": c["partition"],
            "build": {k: c["build"][k] for k in ("degree", "candidate_list_size", "prune_alphas")},
            "quantization": c["quantization"], "page_bytes": c["storage"]["page_bytes"],
            "routing": {k: c["routing"][k] for k in
                        ("sample_ratio", "min_samples", "max_samples", "hnsw_m", "ef_construction")},
            "shards": len(c["cluster"]["nodes"])}
