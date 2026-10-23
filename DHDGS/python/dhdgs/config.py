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
    "version": int, "seed": int,
    "benchmark_results": {"memory_limit_bytes":int,"json_shard_bytes":int,"processor_timeout_seconds":int},
    "runtime": {"blas_threads": int,"python_switch_interval_ms":float},
    "native_runtime": {"compute_cpu_ids":list,"data_enabled":bool,"head_enabled":bool,
                       "receive_threads":int,"receive_queue_capacity":int,
                       "max_connections":int,"compute_queue_capacity":int,
                       "network_queue_capacity":int,"connections_per_endpoint":int,
                       "route_threads":int,"merge_threads":int,"early_parent_report":bool,"queued_control_registration":bool,"move_head_reports":bool,"search_thread_reports":bool,
                       "head_maintenance_mode":str,
                       "report_capacity_retry_ms":int,
                       "report_network":{"enabled":bool,"connections_per_endpoint_per_reactor":int},
                       "result_sink":{"enabled":bool,"writers":int,"batch_records":int,"batch_bytes":int,"completion":str,"max_retained_bytes":int},
                       "head_entry_network":{"enabled":bool,"send_min_reactors":int,"send_max_reactors":int,
                                             "connections_per_endpoint_per_reactor":int},
                       "network_module":{"scale_up_busy_fraction":float,"async_connect_progress":bool,"async_connect":bool,"cpu_ids":list,"transport":str,"rdma_poll_interval_us":int,"min_connections_per_endpoint_per_reactor":int,"max_connecting_per_reactor":int,"send_pressure_mode":str,"rebalance_connections":bool,"rebalance_interval_ms":int,"rebalance_batch":int,"enabled":bool,"send_min_reactors":int,"send_max_reactors":int,
                           "receive_min_reactors":int,"receive_max_reactors":int,"handlers_per_receive_reactor":int,
                           "connections_per_endpoint_per_reactor":int,"send_queue_capacity":int,
                           "receive_queue_capacity_per_reactor":int,"max_receive_connections":int,
                           "send_pending_high":int,"receive_queue_high":int,"high_samples":int,
                           "sample_interval_ms":int,"cooldown_ms":int,"idle_ms":int},
                       "adaptive_control":{"enabled":bool,"min_lanes":int,"max_lanes":int,
                           "command_queue_high":int,"network_pending_high":int,"high_samples":int,
                           "sample_interval_ms":int,"cooldown_ms":int,"idle_ms":int}},
    "metrics": {"detailed_trace": bool,"trace_query_ids":list,"lifecycle_trace":bool,"gc_trace":bool, "resource_sample_interval_ms": int, "resource_max_samples": int},
    "paths": {"dataset": str, "output": str},
    "data": {"metric": str, "dimension": int, "row_limit": int, "block_rows": int, "inspection_rows": int},
    "partition": {"method": str, "overlap_fraction": float, "max_copies": int, "train_rows": int,
                  "calibration_rows": int, "kmeans_iterations": int, "kmeans_tolerance": float},
    "build": {"memory_limit_bytes": int, "degree": int, "candidate_list_size": int, "prune_alphas": list},
    "quantization": {"bits": int},
    "storage": {"page_bytes": int, "cache_bytes_per_shard": int,
                "io": {"backend": str, "fallback_backend": str, "worker_threads": int,
                       "max_submit_requests": int, "max_inflight_per_query": int,
                       "max_inflight_per_device": int, "max_inflight_bytes_per_device": int,
                       "buffer_pool_bytes_per_device": int, "max_pending_batches": int,
                       "max_completion_events_per_poll": int, "completion_poll_ms": int}},
    "routing": {"sample_ratio": float, "min_samples": int, "max_samples": int, "hnsw_m": int,
                "ef_construction": int, "ef_search": int, "entry_candidates": int},
    "search": {"execution": str, "top_k": int, "frontier_size": int, "max_expansions": int, "local_beam": int,
               "max_inflight": int, "exclude_self": bool, "fallback_enabled": bool,
               "max_fallback_entries": int, "stagnation_batches": int},
    "peer": {"execution_model": str,"max_branches": int,"child_budget_fraction": float,"worker_threads": int,
             "max_active_queries": int,"heartbeat_interval_ms": int,"lease_timeout_ms": int,
             "query_timeout_seconds": float,"control_timeout_seconds": float,"handoff_timeout_seconds": float,
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


def _check(values, schema, path=""):
    optional={'listen_backlog','rpc_tcp_nodelay'} if path=='network' else set()
    if path=='':optional={'native_runtime','benchmark_results'}  # Archived plans remain usable.
    if path=='native_runtime':optional={'compute_cpu_ids','report_network','result_sink','report_capacity_retry_ms','adaptive_control','network_module','early_parent_report','queued_control_registration','move_head_reports','search_thread_reports','head_maintenance_mode','head_entry_network'}
    if path=='native_runtime.network_module':optional={'scale_up_busy_fraction','async_connect_progress','async_connect','cpu_ids','transport','rdma_poll_interval_us','min_connections_per_endpoint_per_reactor','max_connecting_per_reactor','send_pressure_mode','rebalance_connections','rebalance_interval_ms','rebalance_batch'}
    if path=='native_runtime.result_sink':optional={'max_retained_bytes'}
    if path=='peer.connection_pool':optional={'head_scheduling','data_scheduling'}
    if path=='peer.head_pipeline':optional={'ack_inline'}
    if path=='peer.head_direct':optional={'queued_reports','result_channels','report_queue_capacity','report_connections_per_channel','completion_threads','report_backend','report_registration','completion_dispatch','result_backend'}
    if path=='peer':optional={'locking','adaptive_services','head_receiver_processes','head_receiver_ipc','head_query_workers','head_entry_workers','head_pipeline','head_direct','head_lanes'}
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
    _check(c, SCHEMA)
    if any(v <= 0 for v in c.get('benchmark_results',{}).values()):
        raise ValueError('benchmark_results limits must be positive')
    nr = c.get('native_runtime', {})
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
        if any(nm[k]<0 for k in unlimited_keys) or any(v<=0 for k,v in nm.items() if k not in unlimited_keys | {'async_connect_progress','async_connect','cpu_ids','enabled','transport','send_pressure_mode','rebalance_connections','min_connections_per_endpoint_per_reactor'}) or any(nm[x+'_max_reactors'] and nm[x+'_min_reactors']>nm[x+'_max_reactors'] for x in ('send','receive')):
            raise ValueError('Invalid network_module resource bounds')
        if nm.get('min_connections_per_endpoint_per_reactor',0)<0 or (nm['connections_per_endpoint_per_reactor'] and nm.get('min_connections_per_endpoint_per_reactor',0)>nm['connections_per_endpoint_per_reactor']):
            raise ValueError('Invalid warm connection count')
        if not 0<nm.get('scale_up_busy_fraction',0.5)<=1:raise ValueError('Invalid network scale_up_busy_fraction')
        if nm.get('send_pressure_mode','pending') not in ('pending','queued'):
            raise ValueError('Invalid send pressure mode')
        if nm['enabled'] and nr.get('adaptive_control',{}).get('enabled',False):
            raise ValueError('Enable network_module or adaptive_control separately')
    for ids in (nr.get('compute_cpu_ids',[]),(nm or {}).get('cpu_ids',[])):
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
    zero_allowed = {("data", "row_limit"), ("storage", "cache_bytes_per_shard"),
                    ("search", "max_fallback_entries"), ("network", "max_retries"), ("query", "row"),
                    ("experiment", "warmup_queries")}
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
    zero_allowed.update({("scheduler", "remote_weight"), ("scheduler", "kappa"),
                         ("scheduler", "remote_rtt_ms")})
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
