"""Measurement primitives. Durations use monotonic clocks; missing data is null."""
from __future__ import annotations

from contextlib import contextmanager
from collections import deque, defaultdict
import csv
import math
import os
from pathlib import Path
import socket
import threading
import time

import numpy as np


def clock_domain():
    try:
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        # Time namespaces can give different monotonic offsets on the same kernel.
        namespace = os.readlink('/proc/self/ns/time')
        return f'{boot}:{namespace}'
    except OSError:
        return None


CLOCK_DOMAIN = clock_domain()


class Recorder:
    def __init__(self):
        self.started_ns = time.perf_counter_ns()
        self.cpu_started_ns = time.thread_time_ns()
        self.spans = []

    @contextmanager
    def span(self, category):
        start = time.perf_counter_ns()
        try:
            yield
        finally:
            self.spans.append([category, start, time.perf_counter_ns()])

    def finish(self):
        end = time.perf_counter_ns()
        return {'clock_domain': CLOCK_DOMAIN, 'start_ns': self.started_ns, 'end_ns': end,
                'spans': self.spans, 'thread_cpu_ms': (time.thread_time_ns()-self.cpu_started_ns)/1e6}


def interval_overlap(a, b, c, d):
    return max(0, min(b,d)-max(a,c))


def wait_breakdown(start, end, actions):
    """Attribute ONLY blocked coordinator time, never sum parallel service times.

    FIRST_COMPLETED waits depend on the first completed action. Remote clocks
    that cannot be compared remain unattributed. Outside the server service
    span, transport also includes serialization, queuing and runtime overhead.
    """
    parts = {'storage_call_wait_ms': 0., 'data_compute_wait_ms': 0., 'data_service_other_wait_ms': 0.,
             'transport_and_runtime_wait_ms': 0., 'rpc_wait_unattributed_ms': 0.,
             'coordinator_wakeup_wait_ms': 0.}
    if not actions:
        parts['rpc_wait_unattributed_ms'] = (end-start)/1e6
        return parts
    action = min(actions, key=lambda a: a['client_end_ns'])
    ready = max(start, min(end, action['client_end_ns']))
    parts['coordinator_wakeup_wait_ms'] = (end-ready)/1e6
    trace = action.get('service_trace')
    if not trace or CLOCK_DOMAIN is None or trace['clock_domain'] != CLOCK_DOMAIN:
        parts['rpc_wait_unattributed_ms'] = (ready-start)/1e6
        return parts
    if not action['client_start_ns'] <= trace['start_ns'] <= trace['end_ns'] <= action['client_end_ns']:
        parts['rpc_wait_unattributed_ms'] = (ready-start)/1e6
        return parts
    service = interval_overlap(start,ready,trace['start_ns'],trace['end_ns'])
    storage = sum(interval_overlap(start,ready,a,b) for cat,a,b in trace['spans'] if cat == 'pread')
    compute = sum(interval_overlap(start,ready,a,b) for cat,a,b in trace['spans']
                  if cat in ('exact_distance', 'rabitq'))
    storage = min(service,storage)
    parts['storage_call_wait_ms'] = storage/1e6
    compute = min(service-storage,compute)
    parts['data_compute_wait_ms'] = compute/1e6
    parts['data_service_other_wait_ms'] = (service-storage-compute)/1e6
    parts['transport_and_runtime_wait_ms'] = (ready-start-service)/1e6
    return parts


def distribution(values):
    if not values:
        return {'count':0,'mean':None,'p50':None,'p90':None,'p95':None,'p99':None,'max':None}
    x = np.asarray(values,dtype=float)
    return {'count':len(x), 'mean':float(x.mean()),
            **{f'p{q}':float(np.percentile(x,q)) for q in (50,90,95,99)}, 'max':float(x.max())}


def imbalance(values):
    x = np.asarray(values,dtype=float)
    mean = float(x.mean()) if len(x) else 0.
    return {'max_to_mean':float(x.max()/mean) if mean else None,
            'coefficient_of_variation':float(x.std()/mean) if mean else None}


WORK_TIMINGS = ('pread_ms','rabitq_ms','exact_distance_ms','service_ms',
                'server_thread_cpu_ms','coordinator_thread_cpu_ms',
                'io_batch_wall_ms','io_capacity_wait_ms','io_queue_wait_ms','io_read_residence_ms',
                'query_ready_queue_ms','query_compute_slice_wall_ms','query_io_wait_ms',
                'query_admission_queue_ms','query_executor_start_wait_ms',
                'query_transfer_finalize_wait_ms','query_branch_wall_ms','query_state_other_ms',
                'peer_transfer_queue_ms','peer_pool_wait_ms','peer_connect_ms','peer_send_ms',
                'peer_response_wait_ms','peer_transfer_wall_ms')


def summarize_query_timings(queries):
    """Request-weighted means; cumulative worker time is not a latency fraction."""
    successful=[q for q in queries if q.get('status')=='ok']
    def measured(value):
        return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) and value>=0
    def mean(values):
        return math.fsum(values)/len(values) if values else None
    wall=[]
    for q in successful:
        stats=q.get('stats',{});parts=stats.get('latency_breakdown_ms')
        latency=stats.get('latency_ms')
        if (isinstance(parts,dict) and parts and measured(latency)
                and all(measured(v) for v in parts.values())
                and math.isclose(math.fsum(parts.values()),latency,rel_tol=1e-6,abs_tol=1e-6)):
            wall.append((parts,latency))
    wall_stages={}
    for key in sorted({key for parts,_ in wall for key in parts}):
        samples=[(parts[key],latency) for parts,latency in wall if key in parts]
        durations=[v for v,_ in samples];total_latency=math.fsum(t for _,t in samples)
        wall_stages[key]={'sample_count':len(samples),'mean_ms':mean(durations),'total_ms':math.fsum(durations),
            'mean_query_fraction':mean([v/t for v,t in samples if t>0]),
            'fraction_of_total_latency':math.fsum(durations)/total_latency if total_latency else None}
    cumulative={}
    complete=[q.get('stats',{}) for q in successful if q.get('stats',{}).get('metrics_complete') is not False]
    for key in WORK_TIMINGS:
        values=[s[key] for s in complete if measured(s.get(key))
                and (not key.startswith(('io_','query_')) or s.get('peer_execution_model') in ('resumable','native'))]
        cumulative[key]={'sample_count':len(values),'mean_ms':mean(values),
                         'total_ms':math.fsum(values) if values else None,
                         'mean_query_fraction':None,'fraction_of_total_latency':None}
    return {'scope':'successful_measured_requests; warmup excluded; repetitions count as separate requests',
            'request_count':len(queries),'successful_requests':len(successful),
            'failed_requests':len(queries)-len(successful),'wall_sample_count':len(wall),
            'wall_unavailable_success_count':len(successful)-len(wall),
            'mean_latency_ms_for_wall_samples':mean([t for _,t in wall]),
            'wall_means_additive':bool(wall) and all(set(p)==set(wall[0][0]) for p,_ in wall),
            'wall_stages':wall_stages,'cumulative_work':cumulative}


def summarize_per_query_timings(queries):
    """For repeated benchmarks, average the observations of each query row."""
    groups=defaultdict(list)
    for q in queries:
        key=('row',q['query_row']) if q.get('query_row') is not None else ('request',q.get('request_id'))
        groups[key].append(q)
    return [{'query_row':rows[0].get('query_row'),'query_id':rows[0].get('query_id'),
             **summarize_query_timings(rows)} for rows in groups.values()]


def export_timing_csvs(directory, summary, per_query):
    directory=Path(directory)
    fields=['query_row','query_id','request_count','successful_requests','failed_requests',
            'category','stage','sample_count','mean_ms','total_ms','mean_query_fraction','fraction_of_total_latency']
    for name,summaries in (('query_stage_means.csv',[summary]),('per_query_stage_means.csv',per_query)):
        with (directory/name).open('x',newline='') as handle:
            writer=csv.DictWriter(handle,fieldnames=fields);writer.writeheader()
            for item in summaries:
                context={k:item.get(k) for k in fields[:5]}
                for category in ('wall_stages','cumulative_work'):
                    for stage,values in item[category].items():
                        writer.writerow({**context,'category':category,'stage':stage,**values})


def process_snapshot(pid=None):
    pid = os.getpid() if pid is None else pid
    result = {'pid':pid,'host':socket.gethostname(),'monotonic_ns':time.perf_counter_ns(),
              'cpu_seconds':None,'rss_bytes':None,'peak_rss_bytes':None,
              'storage_read_bytes':None,'storage_write_bytes':None,'threads':None}
    try:
        root = Path(f'/proc/{pid}')
        stat = (root/'stat').read_text().rsplit(')',1)[1].split()
        result['cpu_seconds'] = (int(stat[11])+int(stat[12]))/os.sysconf('SC_CLK_TCK')
        for line in (root/'status').read_text().splitlines():
            name, _, value = line.partition(':')
            if name in ('VmRSS','VmHWM'):
                result['rss_bytes' if name=='VmRSS' else 'peak_rss_bytes'] = int(value.split()[0])*1024
            elif name=='Threads': result['threads'] = int(value)
        for line in (root/'io').read_text().splitlines():
            name, value = line.split(':')
            if name in ('read_bytes','write_bytes'): result[f'storage_{name}'] = int(value)
    except (OSError,ValueError,IndexError):
        pass
    return result


class ResourceSampler:
    def __init__(self, interval_ms, max_samples):
        self.interval = interval_ms/1000
        self.samples = deque(maxlen=max_samples)
        self.stop_event = threading.Event()
        self.thread = None

    def start(self):
        self.samples.append(process_snapshot())
        def run():
            while not self.stop_event.wait(self.interval):
                self.samples.append(process_snapshot())
        self.thread = threading.Thread(target=run,daemon=True)
        self.thread.start()
        return self

    def finish(self):
        self.stop_event.set()
        if self.thread: self.thread.join()
        self.samples.append(process_snapshot())
        return list(self.samples)


def export_query_csv(path, queries):
    fields = ['request_id','query_row','query_id','status','recall','latency_ms','queue_wait_ms',
              'rpc_count','wire_bytes','request_bytes','response_bytes','rpc_retries','file_read_bytes',
              'pread_calls','pread_ms','cache_hits','expanded','scanned_edges','rabitq_evaluations',
              'exact_distance_evaluations','graph_rounds','max_discovery_depth','max_expanded_depth','max_cross_shard_depth',
              'coordinator_thread_cpu_ms','server_thread_cpu_ms','fallback_entries','frontier_dropped',
              'shards_visited','error']
    fields += ['execution','router_submit_rpc_count','router_expand_rpc_count','peer_branch_rpc_count',
               'control_rpc_count','control_request_bytes','control_response_bytes','control_wire_bytes',
               'all_protocol_bytes','all_protocol_rpc_count','branch_count',
               'unique_expanded','duplicate_expansions','branch_unused_credit','server_to_server_branches',
               'visited_nodes','node_revisit_count','visited_node_candidates_dropped',
               'remote_candidates_dropped_after_forward']
    fields += ['peer_execution_model','io_backends','async_read_requests','query_resume_count'] + [k for k in WORK_TIMINGS if k not in fields]
    fields += ['head_receive_queue_ms','head_route_queue_ms','head_dispatch_queue_ms','head_merge_queue_ms',
               'head_stage_queue_ms','head_stage_resume_queue_ms',
               'head_distribution_queue_ms','head_result_queue_ms','result_return_ipc_ms','result_write_ms',
               'result_process_pid','entry_submit_retries']
    breakdown_keys = list(dict.fromkeys(key for q in queries for key in q.get('stats',{}).get('latency_breakdown_ms',{})))
    fields += [key for key in breakdown_keys if key not in fields] + [key.replace('_ms','_fraction') for key in breakdown_keys]
    with Path(path).open('x',newline='') as handle:
        writer = csv.DictWriter(handle,fieldnames=fields)
        writer.writeheader()
        for q in queries:
            values = {**q.get('stats',{}), **{k:v for k,v in q.items() if k!='stats'}}
            values.update(q.get('stats',{}).get('latency_breakdown_ms',{}))
            values.update({key.replace('_ms','_fraction'):value for key,value in
                           q.get('stats',{}).get('latency_breakdown_fraction',{}).items()})
            writer.writerow({key:values.get(key) for key in fields})


def export_resource_csvs(directory, report):
    directory = Path(directory)
    if 'query_timing_summary' in report:
        export_timing_csvs(directory,report['query_timing_summary'],report['per_query_timing_means'])
    def write(name, rows):
        if not rows:
            return
        fields = list(dict.fromkeys(key for row in rows for key in row))
        with (directory/name).open('x',newline='') as handle:
            writer = csv.DictWriter(handle,fieldnames=fields)
            writer.writeheader(); writer.writerows(rows)
    write('nodes.csv',[{'shard_id':sid,**values} for sid,values in report['per_node_query_totals'].items()])
    write('process_load.csv',[{'shard_id':row['shard_id'],'worker':row['worker'],
        'mean_cpu_cores':row['mean_cpu_cores'],'window_seconds':row['process_window_seconds'],
        **row['process'],**{f'delta_{k}':v for k,v in row['process_deltas'].items()}}
        for row in report['node_process_load']])
    profile = report.get('build_profile')
    if profile: write('build_stages.csv',profile['phases'])
