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


screen = OPTIONS['screening']
rest = screen[1:]
random.Random(20261021).shuffle(rest)
order = [screen[0], *rest]
for i, candidate in enumerate(order):
    run(f'screen_{i:02d}_{candidate["name"]}', candidate['layout'])
    if i in (3, 7):
        run(f'baseline_screen_{i:02d}', BASELINE, stage='baseline')

winner = copy.deepcopy(best([r for r in ROWS if r['stage']=='screen'])['layout'])
save('screen_winner.json', best([r for r in ROWS if r['stage']=='screen']))

# Coordinate refinement tests every adjustable native role around a candidate.
# These short runs nominate candidates; the longer alternated runs decide.
dimensions = ['search', 'data_tx', 'data_rx', 'data_handlers_per_rx',
              'head_tx', 'head_rx', 'head_handlers_per_rx', 'route', 'merge']
for dim_index, key in enumerate(dimensions):
    options = OPTIONS['refinement'][key]
    candidates = []
    for value in options:
        layout = {**winner, key:value}
        matches = [r for r in ROWS if r['layout']==layout]
        if matches:
            candidates.append(best(matches)); continue
        candidates.append(run(f'refine_{dim_index:02d}_{key}_{value}', layout, stage='refine'))
    current = best([r for r in candidates if r['layout']==winner])
    challenger = best(candidates)
    # Prevent a noisy <3% short-window gain from needlessly adding resources.
    if challenger['qps'] > current['qps']*1.03:
        winner = copy.deepcopy(challenger['layout'])
    save('refinement_checkpoint.json', dict(dimension=key, selected_layout=winner))
    if dim_index in (2, 5, 8):
        run(f'baseline_refine_{dim_index:02d}', BASELINE, stage='baseline')

nominees = [winner]
for row in sorted(ROWS, key=lambda r:r['qps'], reverse=True):
    if row['layout'] != BASELINE and row['layout'] not in nominees:
        nominees.append(row['layout']); break
if BASELINE not in nominees:
    nominees.insert(0, BASELINE)
save('confirmation_candidates.json', nominees)
for repeat in range(3):
    indices = list(range(len(nominees)))
    if repeat % 2: indices.reverse()
    for index in indices:
        run(f'confirm_r{repeat}_c{index}', nominees[index],
            count=OPTIONS['confirmation']['queries'], stage='confirm')
save('completed.json', dict(timestamp=time.time(), cases=len(ROWS),
                            queries=sum(row['queries'] for row in ROWS)))
print('SWEEP COMPLETE', flush=True)
