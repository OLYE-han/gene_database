"""Manage only the loopback data services created by this test invocation."""
from __future__ import annotations

import json
from pathlib import Path
import selectors
import subprocess
import sys
import time

from .benchmark import run_benchmark
from .data import write_json
from .index import build, shard_path


def run_local_cluster(c, directory, truth_provider=None, query_rows=None):
    if c['network']['transport'] != 'tcp' or any(n['host'] not in ('127.0.0.1','::1','localhost')
                                                for n in c['cluster']['nodes']):
        raise ValueError('local-cluster-test requires TCP and loopback-only node hosts')
    if any(n['host'] == '::1' for n in c['cluster']['nodes']):
        raise ValueError('Current server supports IPv4; use 127.0.0.1')
    root = Path(directory)
    reused = (Path(c['paths']['output'])/'index/manifest.json').exists()
    if not reused:
        build(c)
    processes, logs, lifecycle = [], [], []
    started = time.perf_counter()
    report = None
    try:
        for node in c['cluster']['nodes']:
            sid = node['id']
            error_log = (root/f'server_{sid}.stderr.log').open('x')
            logs.append(error_log)
            proc = subprocess.Popen([sys.executable,'-m','dhdgs','serve','--config',str(root/'resolved.yaml'),
                                     '--node-id',str(sid)],stdout=subprocess.PIPE,stderr=error_log,text=True)
            processes.append(proc)
            record = {'shard_id':sid,'pid':proc.pid,'host':node['host'],'port':node['port']}
            lifecycle.append(record)
            with selectors.DefaultSelector() as selector:
                selector.register(proc.stdout,selectors.EVENT_READ)
                if not selector.select(c['network']['timeout_seconds']):
                    raise TimeoutError(f'Shard {sid} readiness timeout; see server log')
                line = proc.stdout.readline()
            (root/f'server_{sid}.stdout.log').write_text(line)
            if not line or json.loads(line).get('status') != 'listening':
                raise RuntimeError(f'Shard {sid} failed to start; see {root}/server_{sid}.stderr.log')
            record['ready'] = True
        startup_seconds = time.perf_counter()-started
        report = run_benchmark(c,truth_provider=truth_provider,query_rows=query_rows)
        if truth_provider is not None:
            report['truth_preparation_seconds']=report.pop('exact_scan_seconds')
            report['ground_truth_source']=truth_provider.provenance
        quotas={}
        for node in c['cluster']['nodes']:
            path=shard_path(c,node['id'])/'nodes.bin'
            device=str(path.stat().st_dev)
            entry=quotas.setdefault(device,{'filesystem_device_id':device,'node_ids':[],
                                           'max_inflight_requests_sum':0,'max_inflight_bytes_sum':0,
                                           'buffer_pool_bytes_sum':0})
            entry['node_ids'].append(node['id'])
            if c['peer']['execution_model']=='resumable' or (c['peer']['execution_model']=='native' and c['storage']['io']['backend']!='sync_pread'):
                io=c['storage']['io']
                entry['max_inflight_requests_sum']+=io['max_inflight_per_device']
                entry['max_inflight_bytes_sum']+=io['max_inflight_bytes_per_device']
                entry['buffer_pool_bytes_sum']+=io['buffer_pool_bytes_per_device']
        report['local_io_quota_allocation']={'filesystem_groups':list(quotas.values()),
            'note':'Quotas are per process, not a host-wide semaphore. Check mount/partition to physical SSD mapping; allocate YAML quotas before launch.'}
        report['local_cluster'] = {'reused_index':reused,'startup_seconds':startup_seconds,
                                   'services':lifecycle,'scope':'independent processes, TCP loopback on one physical host'}
        return report
    finally:
        for proc in processes:
            if proc.poll() is None: proc.terminate()
        for record,proc in zip(lifecycle,processes):
            try:
                proc.wait(timeout=c['network']['timeout_seconds'])
            except subprocess.TimeoutExpired:
                proc.kill(); proc.wait()
            record['exit_code'] = proc.returncode
            record['stopped'] = proc.poll() is not None
            if proc.stdout: proc.stdout.close()
        for log in logs: log.close()
        write_json(root/'services.json',{'services':lifecycle,'test_completed':report is not None})
