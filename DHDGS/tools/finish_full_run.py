"""Continue an already-started full build through validation and physical tests."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import yaml
from deploy_cluster import save

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--plan',required=True)
    path=Path(parser.parse_args().plan).resolve();root=path.parent
    plan=yaml.safe_load(path.read_text());opts=plan['full_run'];tools=Path(__file__).resolve().parent
    deadline=time.monotonic()+opts['pipeline_timeout_seconds']
    def status(phase):
        record={'phase':phase,'time':time.time()};save(root/'pipeline_status.json',record);print(json.dumps(record),flush=True)
    def run(name,script,selected=False):
        status(name);cfg=root/'deployment.selected.yaml' if selected else path
        with (root/f'{name}.log').open('a',buffering=1) as log:
            subprocess.run([sys.executable,str(tools/script),'--plan',str(cfg)],stdout=log,stderr=subprocess.STDOUT,
                           check=True,timeout=max(1,deadline-time.monotonic()))
    try:
        status('waiting_for_build_and_truth')
        required=[Path(plan['deployment']['index_source'])/'manifest.json',root/'truth_validation.json']
        while not all(p.exists() for p in required):
            if time.monotonic()>deadline:raise TimeoutError('Waiting for full build/truth exceeded pipeline timeout')
            time.sleep(opts['pipeline_poll_seconds'])
        if not json.loads(required[1].read_text())['passed']:raise ValueError('Truth validation did not pass')
        run('index_validation','verify_full_index.py')
        run('calibration','calibrate_full.py')
        run('deployment','run_full_deployment.py',selected=True)
        run('audit','audit_full_run.py',selected=True)
        status('complete');save(root/'pipeline_completed.json',{'complete':True,'time':time.time()})
    except BaseException as exc:
        save(root/'pipeline_failed.json',{'type':type(exc).__name__,'error':str(exc),'time':time.time()})
        raise

if __name__=='__main__':main()
