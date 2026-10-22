"""Sequential thread-layout search, with interleaved baselines and confirmation."""
import copy
import gzip
import hashlib
import json
import random
import subprocess
import time
import shutil
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
PYTHON = str(PROJECT/'.venv/bin/python')
OPTIONS = yaml.safe_load((ROOT/'sweep.yaml').read_text())
BASELINE = OPTIONS['baseline']
ROWS = []


def save(name, value):
    (ROOT/name).write_text(json.dumps(value, indent=2))


def compress(case):
    manifest = []
    for source in sorted((case/'runs').glob('*/*')):
        if not (source.name.startswith('queries.part-') and source.suffix == '.json'
                or source.name == 'report_context.json' or source.suffix == '.csv'):
            continue
        target = source.with_suffix(source.suffix+'.gz')
        temporary = target.with_suffix(target.suffix+'.tmp')
        digest = hashlib.sha256()
        with source.open('rb') as stream, gzip.open(temporary, 'wb', compresslevel=6) as out:
            while chunk := stream.read(1024*1024):
                digest.update(chunk); out.write(chunk)
        verified = hashlib.sha256()
        with gzip.open(temporary, 'rb') as stream:
            while chunk := stream.read(1024*1024):
                verified.update(chunk)
        assert verified.digest() == digest.digest()
        temporary.replace(target)
        manifest.append(dict(source=str(source.relative_to(case)),
                             target=str(target.relative_to(case)),
                             sha256=digest.hexdigest(), bytes=source.stat().st_size,
                             compressed_bytes=target.stat().st_size))
        source.unlink()
    if manifest:
        (case/'archive_manifest.json').write_text(json.dumps(manifest, indent=2))


def run(name, layout, count=10000, stage='screen'):
    case = ROOT/'cases'/name
    layout_file = ROOT/'layouts'/f'{name}.yaml'
    layout_file.parent.mkdir(exist_ok=True)
    layout_file.write_text(yaml.safe_dump(layout, sort_keys=False))
    if not (case/'result.json').exists():
        assert shutil.disk_usage(PROJECT).free >= 1024**3, 'Less than 1GiB free; pause before starting next case'
        assert not case.exists(), f'Unfinished case needs investigation: {case}'
        argv = [PYTHON, str(ROOT/'experiment.py'), 'cases/'+name, 'cpp',
                '--topology', 'three', '--count', str(count), '--concurrency', '256',
                '--head-connections', '128', '--network-mode', 'fixed',
                '--connections', '128', '--control-capacity', '1024', '--early',
                '--layout', str(layout_file)]
        with (ROOT/(name+'.log')).open('w') as log:
            completed = subprocess.run(argv, cwd=PROJECT, stdout=log, stderr=subprocess.STDOUT)
        if completed.returncode:
            raise RuntimeError(f'{name} failed: inspect {ROOT/(name+".log")}')
    summary = json.loads((case/'result.json').read_text())
    spec = json.loads((case/'spec.json').read_text())
    assert spec['layout'] == layout and spec['count'] == count
    assert summary['failed_queries'] == 0 and summary['recall_at_k_all_requests'] >= .95
    assert all(value for key,value in summary['audit'].items() if key!='metrics_complete')
    assert summary['result_capture']['overflow_batches'] == 0
    row = dict(name=name, stage=stage, layout=layout, queries=count,
               qps=summary['qps'], p99=summary['latency_ms']['p99'],
               mean=summary['latency_ms']['mean'], recall=summary['recall_at_k_all_requests'],
               wall_seconds=summary['measured_wall_seconds'],metrics_complete=summary['audit']['metrics_complete'])
    ROWS.append(row); save('progress.json', ROWS)
    print(json.dumps(row), flush=True)
    compress(case)
    return row


def best(rows):
    return max(rows, key=lambda r:r['qps'])




nominees=[dict(label=f'rx{n}_accept{n}',layout={**BASELINE,'data_rx':n}) for n in OPTIONS['receive_values']]
save('confirmation_candidates.json',nominees)
for repeat, order in enumerate(OPTIONS['order']):
    for n in order:
        run(f'confirm_r{repeat}_rx{n}_accept{n}',{**BASELINE,'data_rx':n},count=OPTIONS['queries_per_case'],stage='confirm')
save('completed.json',dict(timestamp=time.time(),cases=len(ROWS),queries=sum(r['queries'] for r in ROWS)))
print('SWEEP COMPLETE',flush=True)
