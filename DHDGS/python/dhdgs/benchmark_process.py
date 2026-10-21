"""Separate program: decode RAM capture batches, write JSON shards, then report."""
import argparse
import gzip
import json
import os
from pathlib import Path
import time

import msgpack

from .benchmark_archive import HEADER, STAMP
from .managed_node import save_json
from .metrics import (distribution, export_query_csv, export_resource_csvs,
                      summarize_query_timings, summarize_per_query_timings)


def read_exact(source, size):
    data = bytearray()
    while len(data) < size:
        block = source.read(size-len(data))
        if not block:
            raise ValueError('Truncated capture frame')
        data.extend(block)
    return data


def normalize(payload, meta, stored_ns, context):
    q = msgpack.unpackb(payload, raw=False, strict_map_key=False)
    q.setdefault('ids', [])
    job = meta['job']
    expected = context['truth'][job]
    start, end = meta['start_ns'], meta['service_done_ns']
    stats = q.setdefault('stats', {})
    if 'result_encoding' in meta:
        q['result_encoding_timestamps'] = meta['result_encoding']
        stats['result_encoding_ms'] = (meta['result_encoding']['end_ns']-meta['result_encoding']['start_ns'])/1e6
        q['result_encoding_phase'] = 'deferred_archive' if meta.get('result_encoding_deferred') else 'capture'
    if meta['native_capture'] and q['status'] == 'ok':
        offered, delivered = meta['api_offered_ns'], meta['api_delivered_ns']
        native_ready = stats['native_head_timestamps']['native_ready_ns']
        stats.update(api_result_delivery_ms=(delivered-native_ready)/1e6,
                     api_submit_ms=(stats['native_head_timestamps']['search_enter_ns']-offered)/1e6,
                     latency_ms=(delivered-offered)/1e6)
        parts = stats['latency_breakdown_ms']
        parts.update(api_result_delivery_ms=stats['api_result_delivery_ms'],api_submit_ms=stats['api_submit_ms'])
        stats['latency_breakdown_fraction'] = {k:v/stats['latency_ms'] for k,v in parts.items()}
        stats['head_timestamps']['search_return_ns'] = delivered
    observed = meta['observed_ns']
    q.update(job=job, query_id=context['query_ids'][job], query_row=context['selection'][job],
        client_start_ns=start, client_end_ns=end, api_future_done_ns=end,
        client_wall_ms=(end-start)/1e6, offered_ns=context['offered_ns'],
        client_backlog_ms=(start-context['offered_ns'])/1e6,
        offered_to_complete_ms=(end-context['offered_ns'])/1e6,
        benchmark_consumer_wait_ms=(observed-end)/1e6,
        benchmark_observed_end_ns=observed, benchmark_observed_wall_ms=(observed-start)/1e6,
        capture_stored_ns=meta.get('native_retained_ns',stored_ns), encoded_stored_ns=stored_ns,
        capture_store_wall_ms=(meta.get('native_retained_ns',stored_ns)-observed)/1e6,
        truth_ids=expected, recall=len(set(q['ids']) & set(expected))/len(expected),
        config_revision=context['config_revision'], capture_format=(
            'native_value_deferred' if meta.get('result_encoding_deferred') else
            'native_msgpack' if meta['native_capture'] else 'python_msgpack'))
    return q


def capture(directory, fd, batch):
    context = json.loads((directory/'capture_context.json').read_text())
    target = context['settings']['json_shard_bytes']
    shards = []
    handle = None
    used = rows = 0
    def finish():
        nonlocal handle
        if handle is not None:
            handle.write(b'\n]\n');handle.close();handle = None
            partial.replace(final)
            shards.append(dict(file=final.name, records=rows, bytes=final.stat().st_size,
                               processor_pid=os.getpid()))
    try:
        with os.fdopen(os.dup(fd), 'rb') as source:
            while True:
                prefix = source.read(HEADER.size)
                if not prefix:
                    break
                if len(prefix) != HEADER.size:
                    prefix += read_exact(source, HEADER.size-len(prefix))
                meta_size, payload_size = HEADER.unpack(prefix)
                if meta_size > 65536 or payload_size > context['max_message_bytes']:
                    raise ValueError('Capture frame exceeds configured size limit')
                meta = msgpack.unpackb(read_exact(source, meta_size), raw=False)
                payload = read_exact(source, payload_size)
                stored, = STAMP.unpack(read_exact(source, STAMP.size))
                q = normalize(payload, meta, stored, context)
                raw = json.dumps(q, allow_nan=False, separators=(',', ':')).encode()
                if handle is not None and rows and used+len(raw)+5 > target:
                    finish()
                if handle is None:
                    final = directory/f'queries.part-{batch:05d}-{len(shards):05d}.json'
                    partial = final.with_suffix('.json.partial')
                    if final.exists():
                        raise FileExistsError(final)
                    handle = partial.open('xb');handle.write(b'[\n');used=2;rows=0
                prefix = b',\n' if rows else b''
                handle.write(prefix);handle.write(raw)
                used += len(prefix)+len(raw);rows += 1
        finish()
    finally:
        if handle is not None:
            handle.close()
    return dict(shards=shards)


class JsonRecords:
    """Our shards are JSON arrays with exactly one result per physical line."""
    def __init__(self, directory):
        self.directory = directory
        self.manifest = json.loads((directory/'capture_manifest.json').read_text())
    def __iter__(self):
        for shard in self.manifest['shards']:
            count = 0
            with (self.directory/shard['file']).open() as source:
                for line in source:
                    line = line.strip()
                    if not line or line in ('[', ']', ','):
                        continue
                    yield json.loads(line.rstrip(','))
                    count += 1
            if count != shard['records']:
                raise ValueError('JSON shard count mismatch')
    def __len__(self):
        return self.manifest['records']


class ReconciledRecords:
    """Join late transfer counters offline; do not delay result delivery for ACKs."""
    def __init__(self, records, after):
        self.records=records
        self.transfers={(r['query_request_id'],r['branch_id']):r
            for node in after.values()
            for r in node.get('peer_lifecycle',{}).get('transfer_records',[])}
    def __len__(self):return len(self.records)
    def __iter__(self):
        for q in self.records:
            stats=q.get('stats',{})
            if q.get('status')!='ok' or 'metrics_complete_excluding_pending_transfers' not in stats:
                yield q;continue
            complete=True
            stats['metrics_complete_at_delivery']=stats['metrics_complete']
            for branch in q.get('branch_reports',[]):
                if branch.get('transfer_metrics_complete',True):continue
                record=self.transfers.get((q['request_id'],branch['branch_id']))
                if record is None or record['pending_transfers']:
                    complete=False;continue
                old=branch.get('peer_transfer',{});new={k:v for k,v in record.items() if k.startswith('peer_')}
                branch['peer_transfer_at_publication']=old
                branch['peer_transfer']=new;branch['transfer_metrics_complete']=True
                branch['transfer_metrics_source']='post_query_node_snapshot'
                def delta(key):return new.get(key,0)-old.get(key,0)
                for key in ('peer_transfer_queue_ms','peer_pool_wait_ms','peer_connect_ms','peer_send_ms',
                            'peer_response_wait_ms','peer_transfer_wall_ms','peer_connections_created','peer_connections_reused'):
                    stats[key]=stats.get(key,0)+delta(key)
                rpc,tx,rx=delta('peer_rpc_count'),delta('peer_request_bytes'),delta('peer_response_bytes')
                for key in ('peer_branch_rpc_count','rpc_count','all_protocol_rpc_count'):stats[key]+=rpc
                for key in ('peer_request_bytes','request_bytes'):stats[key]+=tx
                for key in ('peer_response_bytes','response_bytes'):stats[key]+=rx
                for key in ('wire_bytes','all_protocol_bytes'):stats[key]+=tx+rx
                children=len(branch.get('children',[]))
                stats['rpc_retries']+=max(0,new.get('peer_rpc_count',0)-children)-max(0,old.get('peer_rpc_count',0)-children)
            stats['transfer_metrics_complete']=complete
            stats['metrics_complete']=bool(stats['metrics_complete_excluding_pending_transfers'] and complete and stats['rpc_retries']==0)
            stats['transfer_metrics_reconciled_offline']=True
            yield q


def finalize(path, output=None):
    report_started = time.monotonic_ns()
    context = json.loads((path/'report_context.json').read_text())
    count, concurrency, offered = context['count'], context['concurrency'], context['offered_ns']
    before = {int(k):v for k,v in context['before'].items()}
    after = {int(k):v for k,v in context['after'].items()}
    head_before, head_after = context['head_before'], context['head_after']
    receiver_roles = {int(k):v for k,v in context['receiver_roles'].items()}
    report_receiver_pids = set(context['report_receiver_pids'])
    records = ReconciledRecords(JsonRecords(path), after)
    if output is not None:
        output.mkdir(parents=True, exist_ok=False)
        path = output
    # Keep only scalar summaries for exact percentiles; never retain full report
    # trees, timestamps or per-shard tables for the entire run in the processor.
    results = []
    totals = {sid:{} for sid in after}
    jobs = set()
    for q in records:
        if q['job'] in jobs or not 0 <= q['job'] < count:
            raise ValueError('Duplicate or invalid job in capture')
        jobs.add(q['job'])
        for sid,values in q['stats'].get('per_shard',{}).items():
            for key,value in values.items():
                totals[int(sid)][key] = totals[int(sid)].get(key,0)+value
        compact = {k:v for k,v in q.items() if not isinstance(v,(dict,list))}
        compact['stats'] = {k:v for k,v in q['stats'].items() if isinstance(v,(int,float,str,bool))
            or k in ('latency_breakdown_ms','latency_breakdown_fraction')}
        results.append(compact)
    if len(results)!=count:raise RuntimeError('Missing benchmark results')
    end=max(q['client_end_ns'] for q in results);ok=[q for q in results if q['status']=='ok']
    events=sorted([(q['client_start_ns'],1) for q in results]+[(q['client_end_ns'],-1) for q in results]);active=peak=0
    for _,delta in events:active+=delta;peak=max(peak,active)
    summary=dict(query_count=count,successful_queries=len(ok),failed_queries=count-len(ok),
        qps=len(ok)/((end-offered)/1e9),measured_wall_seconds=(end-offered)/1e9,
        actual_peak_inflight=peak,configured_concurrency=concurrency,
        latency_ms=distribution([q['client_wall_ms'] for q in ok]),
        client_backlog_ms=distribution([q['client_backlog_ms'] for q in results]),
        offered_to_complete_ms=distribution([q['offered_to_complete_ms'] for q in results]),
        recall_at_k_all_requests=sum(q['recall'] for q in results)/count,
        config_revision=context['config_revision'],clock_domain='per-host monotonic; never subtract across hosts',
        node_pids={str(k):v['process']['pid'] for k,v in after.items()})
    # Host-local counter differences only. Snapshot windows include
    # collection overhead, and are distinct from the QPS interval.
    loads=[]
    for sid,current in after.items():
        proc,old=current['process'],before[sid]['process']
        if proc['pid']!=old['pid']:raise RuntimeError('Data process restarted during benchmark')
        span=(proc['monotonic_ns']-old['monotonic_ns'])/1e9
        deltas={key:None if proc[key] is None or old[key] is None else proc[key]-old[key]
            for key in ('cpu_seconds','storage_read_bytes','storage_write_bytes')}
        loads.append(dict(shard_id=sid,worker=sid,process=proc,process_deltas=deltas,
            process_window_seconds=span,mean_cpu_cores=None if deltas['cpu_seconds'] is None else deltas['cpu_seconds']/span))
    summary['node_process_load']=loads
    summary['head_process_load']=[]
    for i,(old,proc) in enumerate(zip(head_before,head_after)):
        span=(proc['monotonic_ns']-old['monotonic_ns'])/1e9
        cpu=None if proc['cpu_seconds'] is None or old['cpu_seconds'] is None else proc['cpu_seconds']-old['cpu_seconds']
        summary['head_process_load'].append(dict(role='hnsw_head' if i==0 else
            (receiver_roles[proc['pid']] if proc['pid'] in report_receiver_pids else 'result_process'),
            process=proc,process_window_seconds=span,cpu_seconds=cpu,
            mean_cpu_cores=None if cpu is None else cpu/span))
    summary['query_metric_distributions']={key:distribution([q['stats'][key] for q in ok if isinstance(q['stats'].get(key),(int,float))])
        for key in ('query_ready_queue_ms','pread_ms','exact_distance_ms','rabitq_ms','expanded',
            'max_expanded_depth','max_cross_shard_depth','peer_branch_rpc_count','control_rpc_count',
            'all_protocol_rpc_count','all_protocol_bytes','file_read_bytes','server_thread_cpu_ms')}
    summary['audit']=dict(metrics_complete=all(q['stats'].get('metrics_complete',False) for q in ok),
        no_node_revisit=all(q['stats'].get('node_revisit_count',0)==0 for q in ok),
        budget_respected=all(q['stats']['expanded']<=context['max_expansions'] for q in ok),
        no_router_expansion=all(q['stats'].get('router_expand_rpc_count',0)==0 for q in ok))
    for name,value in [('nodes_before',before),('nodes_after',after)]:
        with gzip.open(path/(name+'.json.gz'),'wt') as f:json.dump(value,f,allow_nan=False)
    with gzip.open(path/'queries.jsonl.gz','wt') as f:
        for q in records:f.write(json.dumps(q,allow_nan=False)+'\n')
    export_query_csv(path/'queries.csv',records)
    export_resource_csvs(path,dict(per_node_query_totals=totals,node_process_load=loads,
        query_timing_summary=summarize_query_timings(results),per_query_timing_means=summarize_per_query_timings(results),
        build_profile=context.get('build_profile')))
    summary.update(latency_definition='submit_to_api_future_done; archive consumption excluded',
        benchmark_observed_latency_ms=distribution([q['benchmark_observed_wall_ms'] for q in results]),
        capture_queue_ms=distribution([q['benchmark_consumer_wait_ms'] for q in results]),
        result_capture=context['capture'], report_processor_pid=os.getpid(),
        report_started_ns=report_started, report_finished_ns=time.monotonic_ns())
    save_json(path/'summary.json',summary)
    return dict(directory=str(path), **summary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--capture-fd', type=int)
    parser.add_argument('--batch', type=int, default=0)
    parser.add_argument('--output-directory', type=Path,
                        help='Write a fresh report to a new directory; raw shards are unchanged')
    args = parser.parse_args()
    result = capture(args.directory, args.capture_fd, args.batch) if args.capture_fd is not None else finalize(args.directory, args.output_directory)
    print(json.dumps(result, allow_nan=False))


if __name__ == '__main__':
    main()
