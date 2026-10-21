"""Archive only planned, untracked inactive raw logs; retain lossless evidence."""
from pathlib import Path
import gzip
import hashlib
import json
import os
import shutil
import time

ROOT = Path(__file__).resolve().parent
TEST = ROOT.parent


def open_paths():
    opened = set()
    for proc in Path('/proc').iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            if proc.stat().st_uid != os.getuid():
                continue
            for fd in (proc/'fd').iterdir():
                try:
                    target = fd.resolve(strict=True)
                    if target.is_relative_to(TEST):
                        opened.add(target)
                except (OSError, RuntimeError):
                    pass
        except (PermissionError, FileNotFoundError, ProcessLookupError):
            pass
    return opened


def main():
    plan = json.loads((ROOT/'plan.json').read_text())
    before = shutil.disk_usage(TEST).free
    opened = open_paths()
    rows = []
    journal = ROOT/'actions.jsonl'
    with journal.open('x') as log:
        for item in sorted(plan['files'], key=lambda x: x['bytes'], reverse=True):
            p = Path(item['path'])
            assert p.is_relative_to(TEST) and not p.is_symlink()
            target = p.with_name(p.name+'.gz')
            tmp = target.with_name(target.name+'.cleanup-tmp')
            st = p.stat()
            row = dict(path=item['relative'], raw_bytes=st.st_size)
            if p in opened or st.st_ino != item['inode'] or st.st_mtime_ns != item['mtime_ns']:
                row.update(status='skipped_changed_or_open')
            elif target.exists() or tmp.exists():
                row.update(status='skipped_archive_exists')
            else:
                original = hashlib.sha256()
                with p.open('rb') as source, tmp.open('xb') as raw:
                    with gzip.GzipFile(fileobj=raw, mode='wb', compresslevel=3) as archive:
                        while block := source.read(1024*1024):
                            original.update(block); archive.write(block)
                    raw.flush(); os.fsync(raw.fileno())
                checked = hashlib.sha256()
                with gzip.open(tmp,'rb') as source:
                    while block := source.read(1024*1024):
                        checked.update(block)
                after = p.stat()
                if (original.digest() != checked.digest() or
                    (after.st_ino,after.st_size,after.st_mtime_ns) !=
                    (st.st_ino,st.st_size,st.st_mtime_ns)):
                    raise RuntimeError(f'Raw file changed or verification failed: {p}')
                tmp.rename(target)
                row.update(status='verified_gzip', gzip_bytes=target.stat().st_size,
                           sha256=original.hexdigest(), archive=str(target.relative_to(TEST)))
                # Persist successful verification before removing a redundant raw file.
                log.write(json.dumps(row)+'\n');log.flush();os.fsync(log.fileno())
                p.unlink()
                rows.append(row)
                if len(rows)%50==0:
                    print('Archived',len(rows),'released GiB',
                          sum(r['raw_bytes']-r.get('gzip_bytes',r['raw_bytes']) for r in rows)/2**30,flush=True)
                continue
            rows.append(row);log.write(json.dumps(row)+'\n');log.flush()
    summary = dict(files=len(rows),archived=sum(r['status']=='verified_gzip' for r in rows),
                   raw_bytes=sum(r['raw_bytes'] for r in rows if r['status']=='verified_gzip'),
                   gzip_bytes=sum(r.get('gzip_bytes',0) for r in rows),
                   free_before_bytes=before,free_after_bytes=shutil.disk_usage(TEST).free,
                   completed_unix_seconds=time.time())
    (ROOT/'summary.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps(summary),flush=True)


if __name__=='__main__':main()
