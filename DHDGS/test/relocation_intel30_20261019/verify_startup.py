"""Start only the relocated node, confirm readiness, and stop our own child."""
from pathlib import Path
import json
import os
import selectors
import signal
import subprocess

root = Path('/sharenvme/xulihan/relocation_20261019')
launcher = '/sharenvme/xulihan_dhdgs/managed_cluster_20261019/run_data.sh'
with (root/'startup.stderr.log').open('x') as stderr:
    child = subprocess.Popen([launcher], stdout=subprocess.PIPE, stderr=stderr,
                             text=True, start_new_session=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            assert selector.select(60), 'Node readiness timeout'
            ready = json.loads(child.stdout.readline())
            assert ready['role'] == 'data' and ready['node_id'] == 2, ready
            assert ready['pid'] == child.pid
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
        try:
            child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=10)
    assert child.returncode == 0, (root/'startup.stderr.log').read_text()
result = {'ready': ready, 'exit_code': child.returncode, 'service_left_running': False}
(root/'startup.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
