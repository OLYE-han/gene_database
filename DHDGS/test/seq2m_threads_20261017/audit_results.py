"""Audit archived measurements without rerunning searches or changing results."""
import gzip
import hashlib
import json
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from run_experiment import Sweep, digest, save
import yaml


def main():
    sweep = Sweep(ROOT / 'experiment.yaml')
    prepared = json.loads((ROOT / 'prepared.json').read_text())
    completed = json.loads((ROOT / 'completed.json').read_text())
    assert prepared['plan_sha256'] == digest(sweep.plan_path)
    assert prepared['runner_sha256'] == digest(ROOT / 'run_experiment.py')
    # Source paths in the deployment marker are relative to gene_database.
    project = ROOT.parent.parent
    for path in sweep.source_files():
        assert digest(path) == prepared['source_sha256'][str(path.relative_to(project.parent))]
    for name, expected in prepared['index_sha256'].items():
        assert digest(sweep.index / name) == expected, name
    baseline = Path(sweep.opts['baseline_plan']).parent
    def environment(path):
        records = []
        for line in path.read_text().splitlines():
            if not line.startswith('{'): continue
            try: record = json.loads(line)
            except json.JSONDecodeError: continue
            if 'native_module_sha256' in record: records.append(record)
        assert records, path
        return records[-1]
    for sid in range(3):
        previous = environment(baseline / f'prepare_node_{sid}.log')
        current = environment(ROOT / f'environment_node_{sid}.log')
        for key in ('hostname', 'python', 'numpy', 'yaml', 'msgpack', 'threadpoolctl',
                    'native_module_sha256', 'native_runtime_libraries', 'cpu_model'):
            assert previous[key] == current[key], (sid, key)
    manifest = json.loads((sweep.index / 'manifest.json').read_text())
    points = []
    for mode in sweep.modes:
        hosts = set()
        for sid in range(3):
            service = ROOT / mode['name'] / 'services'
            ready = json.loads((service / f'node_{sid}_ready.json').read_text())
            stopped = json.loads((service / f'node_{sid}_stopped.json').read_text())
            assert stopped['supervisor_exit_code'] == stopped['event']['exit_code'] == 0
            assert stopped['event']['stopped'] and ready['pid'] == stopped['event']['pid']
            hosts.add(ready['hostname'])
        assert len(hosts) == 3
        for concurrency in sweep.d['client_concurrencies']:
            folder = ROOT / mode['name'] / f'c{concurrency}'
            config = yaml.safe_load((folder / 'run/resolved.yaml').read_text())
            assert config == sweep.settings(mode, concurrency=concurrency)
            archived = json.loads((folder / 'run/archive.json').read_text())
            for item in archived:
                h = hashlib.sha256()
                with gzip.open(folder / 'run' / (item['file'] + '.gz'), 'rb') as f:
                    for block in iter(lambda: f.read(8*1024*1024), b''): h.update(block)
                assert h.hexdigest() == item['uncompressed_sha256']
            with gzip.open(folder / 'run/report.json.gz', 'rt') as f: report = json.load(f)
            queries = report['queries']
            assert len(queries) == len(sweep.query_rows) == 1024
            assert sorted(q['query_row'] for q in queries) == sweep.query_rows
            assert report['index_epoch'] == manifest['epoch'] and report['indexed_rows'] == 2000000
            assert report['concurrency'] == concurrency
            failures = incomplete = invariant_errors = control_errors = 0
            with gzip.open(folder / 'run/queries.jsonl.gz', 'rt') as stream:
                for q in queries:
                    assert json.loads(next(stream)) == q
                    expected = sweep.truth_provider.ids[q['query_row'], :10].tolist()
                    assert q['truth_ids'] == expected
                    assert q['recall'] == len(set(q['ids']) & set(expected)) / 10
                    if q['status'] != 'ok': failures += 1; continue
                    s = q['stats']
                    incomplete += int(not s['metrics_complete'])
                    control_errors += s['control_errors']
                    invariant_errors += int(s['expanded'] > 1024 or s['node_revisit_count'] != 0 or
                                            s['router_expand_rpc_count'] != 0 or q['duplicate_result_ids'] != 0)
                    invariant_errors += int(s['io_backends'] != ['sync_pread'])
                    nodes = [b['node_id'] for b in q['branch_reports']]
                    invariant_errors += int(len(nodes) != len(set(nodes)) or len(nodes) > 3)
                    for b in q['branch_reports']:
                        invariant_errors += int(b['work_spent'] + b['work_unused'] + sum(ch['work_grant'] for ch in b['children']) != b['work_grant'])
                        for ch in b['children']:
                            approved = sum(d['decision_id'] == ch['decision_id'] and d['remote'] and d['selected_node'] == ch['node_id'] for d in b['decisions'])
                            invariant_errors += int(approved != 1)
                assert next(stream, None) is None
            point = {'worker_threads': mode['worker_threads'], 'concurrency': concurrency,
                     'requests': len(queries), 'failures': failures, 'incomplete_metrics': incomplete,
                     'control_errors': control_errors, 'invariant_errors': invariant_errors,
                     'recall': statistics.mean(q['recall'] for q in queries)}
            assert failures == report['failed_queries']
            assert abs(point['recall'] - report['recall_at_k']) < 1e-12
            points.append(point)
            print(json.dumps(point), flush=True)
    assert len(points) == completed['tests'] == 12
    assert sum(p['requests'] for p in points) == completed['requests'] == 12288
    save(ROOT / 'verification.json', {'provenance_and_archives_verified': True,
         'all_services_stopped': True, 'results': points,
         'all_measurements_valid': all(not any(p[k] for k in ('failures', 'incomplete_metrics', 'control_errors', 'invariant_errors')) for p in points)})


if __name__ == '__main__': main()
