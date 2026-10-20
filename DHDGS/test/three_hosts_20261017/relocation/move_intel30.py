"""One-time relocation requested by the owner; run on intel-30 via stdin."""
from pathlib import Path
import hashlib
import json
import os
import selectors
import signal
import subprocess
import sys

old = Path('/sharenvme/xulihan_dhdgs/three_hosts_20261017')
new = Path('/sharenvme/xulihan/three_hosts_20261017')
assert old.is_dir() and not new.exists(), 'Refuse missing source or occupied destination'
for proc in Path('/proc').glob('[0-9]*/cmdline'):
    try:
        args = proc.read_bytes().split(b'\0')
    except (OSError, PermissionError):
        continue
    assert not (b'serve' in args and any(str(old).encode() in a for a in args)), 'Service still running'

def hashes(folder):
    return {str(p.relative_to(folder)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in folder.rglob('*') if p.is_file()}

before = hashes(old / 'index')
new.parent.mkdir(exist_ok=True)
assert old.stat().st_dev == new.parent.stat().st_dev
old.rename(new)
project = new / 'gene_database/DHDGS'
record = new / 'relocation'
record.mkdir()
changes = []

def rewrite(p):
    if not p.is_file() or p.is_symlink():
        return
    raw = p.read_bytes()
    if b'\0' in raw or str(old).encode() not in raw:
        return
    backup = record / 'original_files' / p.relative_to(new)
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_bytes(raw)
    p.write_bytes(raw.replace(str(old).encode(), str(new).encode()))
    changes.append(str(p.relative_to(new)))

for p in (new / 'configs').glob('*.yaml'):
    rewrite(p)
rewrite(new / 'bootstrap.sh')
rewrite(project / '.venv/pyvenv.cfg')
for p in (project / '.venv/bin').iterdir():
    rewrite(p)
for pattern in ('*.pth', '__editable__*.py', 'direct_url.json'):
    for p in (project / '.venv/lib').rglob(pattern):
        rewrite(p)
links = []
for p in [new / 'output/index', *list((project / '.venv').rglob('*'))]:
    if p.is_symlink() and str(old) in os.readlink(p):
        target = os.readlink(p).replace(str(old), str(new))
        p.unlink()
        p.symlink_to(target)
        links.append(str(p.relative_to(new)))

python = str(project / '.venv/bin/python')
def run(args):
    print('RUN', args, flush=True)
    subprocess.run(args, cwd=project, check=True, timeout=300)

# Reconfigure in a fresh directory; preserve old CMake caches as historical files.
run(['cmake', '-S', '.', '-B', 'build-relocated', '-DCMAKE_BUILD_TYPE=Release',
     '-DCMAKE_CXX_COMPILER=/usr/bin/g++', '-DPython_EXECUTABLE=' + python])
run(['cmake', '--build', 'build-relocated', '--parallel', '4'])
run([str(project / '.venv/bin/dhdgs'), '--help'])
run([python, 'tools/deploy_node.py', 'environment', '--file',
     str(new / 'index/shard_2/nodes.bin'), '--timeout', '10'])
assert before == hashes(new / 'index'), 'Index checksum changed'

services = []
for mode in ('blocking', 'thread_pool', 'io_uring'):
    logfile = record / (mode + '.stderr.log')
    with logfile.open('x') as stderr:
        child = subprocess.Popen([python, '-m', 'dhdgs', 'serve', '--config',
                                  str(new / 'configs' / (mode + '.yaml')), '--node-id', '2'],
                                 cwd=project, stdout=subprocess.PIPE, stderr=stderr,
                                 text=True, start_new_session=True)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                assert selector.select(30), 'Service readiness timeout'
                ready = json.loads(child.stdout.readline())
                assert ready['status'] == 'listening', ready
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGINT)
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGTERM)
                child.wait(timeout=15)
        assert child.returncode == 0, logfile.read_text()
        services.append({'mode': mode, 'ready': ready, 'exit_code': child.returncode})

result = {'old_root': str(old), 'new_root': str(new), 'index_hashes': before,
          'index_unchanged': True, 'rewritten_files': changes, 'rewritten_links': links,
          'services': services, 'native_read_verified': True}
(record / 'verification.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2), flush=True)
