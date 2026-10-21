"""Sequential thread-layout search, with interleaved baselines and confirmation."""
import copy
import gzip
import hashlib
import json
import random
import subprocess
import time
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
PYTHON = str(PROJECT/'.venv/bin/python')
OPTIONS = yaml.safe_load((ROOT/'sweep.yaml').read_text())
BASELINE = OPTIONS['prior_best']
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
        with source.open('rb') as stream, gzip.open(temporary, 'wb', compresslevel=1) as out:
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



run('prior_best_screen', BASELINE, count=OPTIONS['screen_queries'], stage='baseline')
layouts=[(s,n) for s in OPTIONS['search_values'] for n in OPTIONS['network_values']]
random.Random(20261021).shuffle(layouts)
for search, network in layouts:
    layout=dict(search=search,route=OPTIONS['fixed']['route'],merge=OPTIONS['fixed']['merge'],
                data_tx=network,data_rx=network,data_handlers_per_rx=4,
                head_tx=network,head_rx=network,head_handlers_per_rx=4)
    run(f'screen_s{search}_net{network}',layout,count=OPTIONS['screen_queries'])
expanded=best([r for r in ROWS if r['stage']=='screen' and r['layout']['search']>32 and r['layout']['data_rx']>4])
matched={**expanded['layout'], 'data_tx':4,'data_rx':4,'head_tx':4,'head_rx':4}
nominees=[dict(label='prior_best',layout=BASELINE),dict(label='expanded',layout=expanded['layout']),dict(label='matched_net4',layout=matched)]
save('confirmation_candidates.json',nominees)
for repeat in range(OPTIONS['confirm_repeats']):
    order=list(nominees)
    if repeat%2:order.reverse()
    for nominee in order:
        run(f'confirm_r{repeat}_{nominee["label"]}',nominee['layout'],count=OPTIONS['confirm_queries'],stage='confirm')
save('completed.json',dict(timestamp=time.time(),cases=len(ROWS),queries=sum(r['queries'] for r in ROWS)))
print('SWEEP COMPLETE',flush=True)
