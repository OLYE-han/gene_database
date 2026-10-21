"""Remove explicitly requested pre-Oct20 test records; retain runtime assets."""
import hashlib
import json
import re
import shutil
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
inventory = json.loads((ROOT/'cleanup_inventory.json').read_text())
assert inventory['cutoff_exclusive'] == '2026-10-20'
assert not (ROOT/'cleanup_completed.json').exists()
moved = []


def move(source, destination):
    src, dst = PROJECT/source, PROJECT/destination
    assert src.exists() and not dst.exists(), (src, dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    src.rename(dst)
    moved.append(dict(source=source, destination=destination))


before = shutil.disk_usage(PROJECT).free
move('test/seq2m_full_20261017/build', 'artifacts/seq2m_runtime/build')
for name in ['deployment.selected.yaml', 'query_selection.json', 'truth_validation.json']:
    move('test/seq2m_full_20261017/backlog128/'+name,
         'artifacts/seq2m_runtime/backlog128/'+name)
validation = PROJECT/'artifacts/seq2m_runtime/backlog128/truth_validation.json'
original = validation.read_bytes()
(ROOT/'truth_validation_before_relocation.json').write_bytes(original)
v = json.loads(original)
v['provenance']['verification'] = str(validation)
validation.write_text(json.dumps(v, indent=2))
move('test/concurrency_20261018/c32/query_selection.json',
     'artifacts/runtime_assets/concurrency/c32/query_selection.json')
legacy = PROJECT/'test/managed_continuation_three_20261019'
for path in sorted(legacy.iterdir()):
    if path.is_file() and (path.suffix in ('.yaml', '.token', '.log') or 'state' in path.name):
        move(str(path.relative_to(PROJECT)), 'artifacts/runtime_assets/legacy_service/'+path.name)

deleted = []
for item in inventory['candidates']:
    path = PROJECT/item['path']
    assert path.parent == PROJECT/'test' and not path.is_symlink()
    date = re.search(r'2026\d{4}', path.name)
    assert date and date[0] < '20261020'
    shutil.rmtree(path)
    deleted.append(item['path'])

# Existing 220xx processes retain these names in their in-memory configs.
# Links resolve only to retained runtime assets, never to old test records.
links = {
    'test/seq2m_full_20261017': 'artifacts/seq2m_runtime',
    'test/concurrency_20261018': 'artifacts/runtime_assets/concurrency',
    'test/managed_continuation_three_20261019': 'artifacts/runtime_assets/legacy_service',
}
for src, dst in links.items():
    (PROJECT/src).symlink_to(PROJECT/dst, target_is_directory=True)
after = shutil.disk_usage(PROJECT).free
out = dict(completed_at=time.time(), source_commit=inventory['source_commit'],
           deleted_test_directories=deleted, retained_runtime_assets=moved,
           compatibility_links=links, free_bytes_before=before, free_bytes_after=after,
           freed_bytes=after-before,
           original_truth_validation_sha256=hashlib.sha256(original).hexdigest(),
           truth_provenance_change='verification pathname only; original retained alongside this manifest')
(ROOT/'cleanup_completed.json').write_text(json.dumps(out, indent=2))
inventory['status'] = 'completed'
(ROOT/'cleanup_inventory.json').write_text(json.dumps(inventory, indent=2))
print(json.dumps(dict(deleted=len(deleted), freed_GiB=(after-before)/2**30,
                      free_GiB=after/2**30, compatibility_links=links), indent=2))
