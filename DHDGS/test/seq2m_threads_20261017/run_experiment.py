"""Physical three-host worker sweep; reuse immutable deployment and truth.

Only experiment files/configurations are written. Search implementation is unchanged.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import shutil
import statistics
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parent.parent
sys.path.insert(0, str(PROJECT / 'tools'))
from deploy_cluster import Deployment, digest, save
from full_truth import FullTruth, select_queries
from dhdgs.config import validate
import yaml


class Sweep(Deployment):
    def __init__(self, path):
        super().__init__(path)
        self.opts = self.plan['thread_experiment']
        for option in self.opts['ssh_options']:
            self.ssh += ['-o', option]
        self.truth_provider = FullTruth(self.opts['baseline_plan'])
        self.query_rows = select_queries(self.opts['baseline_plan'])['validation']
        if len(self.query_rows) != self.base['experiment']['benchmark_queries']:
            raise ValueError('Use the complete frozen validation query set')
        if sorted(m['worker_threads'] for m in self.modes) != self.opts['worker_threads']:
            raise ValueError('Modes differ from requested thread sweep')

    def settings(self, mode, host=None, concurrency=None):
        c = super().settings(mode, host, concurrency)
        c['peer']['worker_threads'] = mode['worker_threads']
        validate(c)
        return c

    def prepare_reuse(self):
        baseline = Path(self.opts['baseline_plan']).parent
        marker = json.loads((baseline / 'prepared.json').read_text())
        self.progress('Verifying frozen local source and full index')
        for p in self.source_files():
            if marker['source_sha256'].get(str(p.relative_to(PROJECT.parent))) != digest(p):
                raise ValueError(f'Source changed since baseline: {p}')
        for name, expected in marker['index_sha256'].items():
            if digest(self.index / name) != expected:
                raise ValueError(f'Index changed: {name}')
        self.root.joinpath('configs').mkdir(exist_ok=True)
        for mode in self.modes:
            folder = self.root / mode['name']; folder.mkdir(exist_ok=True)
            if not (folder / 'index').exists():
                (folder / 'index').symlink_to(self.index, target_is_directory=True)
            self.config(mode, self.hosts[0]).write_text(yaml.safe_dump(self.settings(mode), sort_keys=False))
        save(self.root / 'query_selection.json', {'validation': self.query_rows,
             'source': str(baseline / 'query_selection.json'),
             'sha256': digest(baseline / 'query_selection.json')})

        def remote(host):
            sid = host['id']; root = Path(host['root'])
            hashes = {str(root / 'gene_database' / name): sha
                      for name, sha in marker['source_sha256'].items()}
            hashes.update({str(root / 'index' / name): sha
                           for name, sha in marker['index_sha256'].items()
                           if name.startswith(f'shard_{sid}/') or name in ('ids.npy', 'membership.npy')})
            configs = {str(self.config(m, host)): yaml.safe_dump(self.settings(m, host), sort_keys=False)
                       for m in self.modes}
            payload = {'hashes': hashes, 'configs': configs}
            # JSON travels through stdin, never through shell interpolation.
            script = '''import hashlib,json,pathlib,sys,socket
p=json.load(sys.stdin)
for name,expected in p['hashes'].items():
    h=hashlib.sha256()
    with open(name,'rb') as f:
        for block in iter(lambda:f.read(8*1024*1024),b''):h.update(block)
    if h.hexdigest()!=expected:raise ValueError('Hash mismatch: '+name)
for name,body in p['configs'].items():
    path=pathlib.Path(name);path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists() and path.read_text()!=body:raise ValueError('Existing configuration differs: '+name)
    path.write_text(body)
print(json.dumps({'hostname':socket.gethostname(),'verified_files':len(p['hashes']),'configs':len(p['configs'])}))
'''
            with (self.root / f'node_{sid}_prepare_input.json').open('w+') as incoming:
                json.dump(payload, incoming); incoming.flush(); incoming.seek(0)
                self.run(host, [self.python(host), '-c', script], self.root / f'prepare_node_{sid}.log', incoming)
            self.run(host, [self.python(host), self.helper(host), 'environment', '--file',
                           str(root / f'index/shard_{sid}/nodes.bin'), '--timeout',
                           str(self.d['network_probe_timeout_seconds'])], self.root / f'environment_node_{sid}.log')
            self.progress(f'Node {sid}: existing source/index verified; experiment configurations installed')

        with ThreadPoolExecutor(max_workers=2) as pool:
            for result in pool.map(remote, self.hosts[1:]): pass
        self.run(self.hosts[0], [self.python(self.hosts[0]), self.helper(self.hosts[0]), 'environment',
                 '--file', str(self.index / 'shard_0/nodes.bin'), '--timeout',
                 str(self.d['network_probe_timeout_seconds'])], self.root / 'environment_node_0.log')
        save(self.root / 'prepared.json', {**marker, 'plan_sha256': digest(self.plan_path),
             'runner_sha256': digest(Path(__file__)), 'baseline_prepared': str(baseline / 'prepared.json')})

    def summarize_point(self, mode, concurrency, audit):
        folder = self.root / mode['name'] / f'c{concurrency}' / 'run'
        report = json.loads((folder / 'report.json').read_text())
        queries = report['queries']
        successful = [q for q in queries if q['status'] == 'ok']
        complete = [q for q in successful if q['stats'].get('metrics_complete')]
        assert report['indexed_rows'] == 2000000
        assert len(queries) == len(self.query_rows)
        assert sorted(q['query_row'] for q in queries) == self.query_rows
        counts = {'duplicate_result_queries': 0, 'over_budget_queries': 0,
                  'wrong_io_backend_queries': 0, 'nonzero_control_error_queries': 0,
                  'node_revisit_queries': 0, 'router_expansion_queries': 0}
        for q in successful:
            s = q['stats']
            counts['duplicate_result_queries'] += int(q['duplicate_result_ids'] != 0)
            counts['over_budget_queries'] += int(s['expanded'] > self.base['search']['max_expansions'])
            counts['wrong_io_backend_queries'] += int(s['io_backends'] != ['sync_pread'])
            counts['nonzero_control_error_queries'] += int(s['control_errors'] != 0)
            counts['node_revisit_queries'] += int(s['node_revisit_count'] != 0)
            counts['router_expansion_queries'] += int(s['router_expand_rpc_count'] != 0)
        scalar_keys = sorted({k for q in complete for k, v in q['stats'].items()
                              if isinstance(v, (int, float)) and not isinstance(v, bool)})
        means = {}
        for k in scalar_keys:
            values = [q['stats'][k] for q in complete if isinstance(q['stats'].get(k), (int, float))]
            means[k] = statistics.mean(values) if values else None
        depths = [min(p['depth'] for p in q['stats']['result_paths'] if p['id'] == q['ids'][0])
                  for q in complete if q['ids']]
        means['top1_min_discovery_depth'] = statistics.mean(depths) if depths else None
        nodes = report['node_process_load']
        reads = [n['process_deltas']['storage_read_bytes'] for n in nodes]
        valid = not any(audit['audit'].values()) and not any(counts.values())
        point = {**audit, 'worker_threads': mode['worker_threads'], 'audit_details': counts,
                 'valid_measurement': valid, 'target_recall_met': report['recall_at_k'] >= self.base['experiment']['target_recall'],
                 'complete_metric_queries': len(complete), 'mean_metrics_complete_queries': means,
                 'physical_read_bytes': sum(reads) if all(v is not None for v in reads) else None,
                 'cpu_cores_sum': sum(n['mean_cpu_cores'] for n in nodes),
                 'nodes': nodes, 'query_timing_summary': report['query_timing_summary'],
                 'stage_means': list(csv.DictReader((folder / 'query_stage_means.csv').open())),
                 'recall_by_query': [{'query_row': q['query_row'], 'recall': q['recall']} for q in queries]}
        save(folder.parent / 'summary.json', point)
        return point

    def compress_point(self, mode, concurrency):
        folder = self.root / mode['name'] / f'c{concurrency}' / 'run'
        archive = []
        for name in self.opts['compress_files']:
            path = folder / name
            raw_sha = digest(path); compressed = path.with_name(path.name + '.gz')
            with path.open('rb') as incoming, gzip.open(compressed, 'wb', compresslevel=self.opts['gzip_level']) as out:
                shutil.copyfileobj(incoming, out, length=8*1024*1024)
            h = hashlib.sha256()
            with gzip.open(compressed, 'rb') as f:
                for block in iter(lambda: f.read(8*1024*1024), b''): h.update(block)
            if h.hexdigest() != raw_sha: raise ValueError('Compression verification failed')
            archive.append({'file': name, 'uncompressed_sha256': raw_sha,
                            'bytes': path.stat().st_size, 'gzip_bytes': compressed.stat().st_size})
            path.unlink()  # Own newly generated file, losslessly archived and verified above.
        save(folder / 'archive.json', archive)

    def execute(self):
        results = []
        save(self.root / 'status.json', {'phase': 'preparing', 'started_at': time.time()})
        self.prepare_reuse()
        for mode in self.modes:
            started = []
            try:
                with ThreadPoolExecutor(max_workers=3) as pool:
                    futures = [pool.submit(self.start, mode, h) for h in self.hosts]
                    errors = []
                    for future in futures:
                        try: started.append(future.result())
                        except Exception as exc: errors.append(exc)
                    if errors: raise errors[0]
                self.probes(mode, started)
                for concurrency in mode['client_order']:
                    if shutil.disk_usage(self.root).free < self.opts['reserve_disk_bytes']:
                        raise RuntimeError('Insufficient free disk for next detailed measurement')
                    save(self.root / 'status.json', {'phase': 'benchmark', 'worker_threads': mode['worker_threads'],
                         'client_concurrency': concurrency, 'finished_points': len(results), 'updated_at': time.time()})
                    result = self.benchmark(mode, concurrency)
                    results.append(self.summarize_point(mode, concurrency, result))
                    save(self.root / 'comparison.json', results)
                    self.compress_point(mode, concurrency)
                    write_summary(self, results)
            finally:
                with ThreadPoolExecutor(max_workers=3) as pool:
                    for result in pool.map(lambda r: self.stop(mode, r), started): pass
        stops = []
        for mode in self.modes:
            hostnames = set()
            for sid in range(3):
                folder = self.root / mode['name'] / 'services'
                ready = json.loads((folder / f'node_{sid}_ready.json').read_text())
                stopped = json.loads((folder / f'node_{sid}_stopped.json').read_text())
                assert stopped['supervisor_exit_code'] == stopped['event']['exit_code'] == 0
                assert stopped['event']['stopped'] and ready['pid'] == stopped['event']['pid']
                hostnames.add(ready['hostname']); stops.append(stopped['event']['pid'])
            assert len(hostnames) == 3
        save(self.root / 'completed.json', {'tests': len(results), 'requests': sum(p['queries'] for p in results),
             'all_services_stopped': True, 'valid_measurements': sum(p['valid_measurement'] for p in results),
             'all_recall_targets_met': all(p['target_recall_met'] for p in results)})
        save(self.root / 'status.json', {'phase': 'complete', 'finished_points': len(results), 'updated_at': time.time()})
        write_summary(self, results)


def write_summary(sweep, results):
    rows = []
    for p in sorted(results, key=lambda p: (p['concurrency'], p['worker_threads'])):
        means = p['mean_metrics_complete_queries']
        row = {'worker_threads_per_node': p['worker_threads'], 'client_concurrency': p['concurrency'],
               'requests': p['queries'], 'qps': p['qps'], 'recall_at_10': p['recall'],
               **{f'latency_{k}_ms': v for k, v in p['latency_ms'].items()},
               'valid_measurement': p['valid_measurement'], 'complete_metric_queries': p['complete_metric_queries'],
               'cpu_cores_sum': p['cpu_cores_sum'], 'physical_read_bytes': p['physical_read_bytes'],
               **{f'mean_{k}': v for k, v in means.items()},
               **{f'node{n["shard_id"]}_cpu_cores': n['mean_cpu_cores'] for n in p['nodes']},
               **{f'node{n["shard_id"]}_rss_bytes': n['process']['rss_bytes'] for n in p['nodes']}}
        rows.append(row)
    keys = list(dict.fromkeys(k for row in rows for k in row))
    with (sweep.root / 'comparison.csv').open('w') as f:
        writer = csv.DictWriter(f, keys); writer.writeheader(); writer.writerows(rows)
    lines = ['# seq_2M 三台物理服务器：计算线程数扫描', '',
             '固定完整 200 万条索引、相同 1,024 条验证查询、搜索预算 1024 / frontier 256、BLAS=1。',
             '扫描的是每台数据服务器的 peer.worker_threads，不是建图线程或 SSD I/O 线程。',
             '客户端闭环并发为 8 / 32，各配置一次测量；详细指标开启，同步 pread，应用缓存关闭。',
             '复用操作系统页缓存，不主动清空共享机器的缓存。纯网络、纯物理 SSD 等待占比不能从现有计时精确分离。',
             '累计 CPU / 距离 / I/O 时间存在不同计时范围，不可相加作为端到端耗时占比。', '',
             f'已完成 {len(results)} / {len(sweep.modes)*len(sweep.d["client_concurrencies"])} 组。', '',
             '| 每节点线程 | 客户端并发 | QPS | 平均 ms | p95 ms | p99 ms | Recall@10 | CPU 核数合计 | 指标完整/请求数 | 审计 |',
             '|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|']
    for p in sorted(results, key=lambda p: (p['concurrency'], p['worker_threads'])):
        l = p['latency_ms']
        lines.append(f'| {p["worker_threads"]} | {p["concurrency"]} | {p["qps"]:.2f} | {l["mean"]:.2f} | {l["p95"]:.2f} | {l["p99"]:.2f} | {p["recall"]:.6f} | {p["cpu_cores_sum"]:.3f} | {p["complete_metric_queries"]}/{p["queries"]} | {"通过" if p["valid_measurement"] else "未通过"} |')
    lines += ['', '在各自相同客户端并发下，筛选无失败、指标完整、平均 Recall@10≥0.99 的配置，再按 p99、QPS 排序。',
              '单轮结果仅用于观察变化，p99 小差异需要重复测量；不能据此证明全局最优线程数。', '']
    for concurrency in sweep.d['client_concurrencies']:
        candidates = [p for p in results if p['concurrency'] == concurrency and p['valid_measurement'] and p['target_recall_met']]
        if candidates:
            best = min(candidates, key=lambda p: (p['latency_ms']['p99'], -p['qps']))
            lines.append(f'- 并发 {concurrency}：已测配置中最低 p99 为 {best["latency_ms"]["p99"]:.2f} ms，线程数 {best["worker_threads"]}，QPS {best["qps"]:.2f}。')
    lines += ['', '文件：', '', '- `experiment.yaml`：统一实验设置与冻结基础配置。',
              '- `comparison.csv`：线程、并发、所有可聚合逐查询标量指标与节点负载。',
              '- `comparison.json`：详细汇总、各阶段均值、每节点采样、逐查询召回。',
              '- 每组 `run/queries.csv`、`per_query_stage_means.csv`：逐查询指标、阶段耗时。',
              '- 每组 `run/queries.jsonl.gz`、`report.json.gz`：完整原始记录，压缩前后哈希验证。',
              '- 每组 `run/query_stage_means.csv`、`nodes.csv`、`process_load.csv`：阶段与节点负载。',
              '- `completed.json`：全部组执行完且服务退出后才生成。', '',
              '运行：`../../.venv/bin/python run_experiment.py --plan experiment.yaml`（在本实验目录中）。',
              '已有同名运行产物时不覆盖；复验应使用新的实验目录和唯一的 mode 名称。', '']
    (sweep.root / 'SUMMARY.md').write_text('\n'.join(lines))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plan', default=str(ROOT / 'experiment.yaml'))
    parser.add_argument('--validate-only', action='store_true')
    args = parser.parse_args()
    sweep = Sweep(args.plan)
    for mode in sweep.modes:
        for host in sweep.hosts: sweep.settings(mode, host)
    if args.validate_only:
        print(json.dumps({'valid': True, 'threads': sweep.opts['worker_threads'],
                          'queries_per_point': len(sweep.query_rows), 'points': len(sweep.modes)*len(sweep.d['client_concurrencies'])}))
        return
    try: sweep.execute()
    except BaseException as exc:
        save(sweep.root / 'failure.json', {'type': type(exc).__name__, 'error': str(exc), 'traceback': traceback.format_exc()})
        raise


if __name__ == '__main__': main()
