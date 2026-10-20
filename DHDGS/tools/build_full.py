"""Central full-corpus construction driven by the deployment master YAML."""
import argparse
import json
from pathlib import Path
import sys
import time
import yaml
from threadpoolctl import threadpool_limits

PROJECT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT/'python'))
from dhdgs.config import load_config
from dhdgs.index import build

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--plan',type=Path,required=True)
    parser.add_argument('--phase',choices=['preflight','full'],required=True)
    args=parser.parse_args();root=args.plan.resolve().parent
    plan=yaml.safe_load(args.plan.read_text());c=plan['dhdgs'];options=plan['full_run']
    folder=root/(options.get('preflight_directory','preflight') if args.phase=='preflight' else 'build')
    folder.mkdir(exist_ok=True)
    c['paths']['output']=str(folder)
    if args.phase=='preflight':
        c['data']['row_limit']=options['preflight_rows']
        c['partition']['train_rows']=options['preflight_train_rows']
        c['partition']['calibration_rows']=options['preflight_calibration_rows']
    cfg=folder/'build.yaml';text=yaml.safe_dump(c,sort_keys=False)
    if cfg.exists() and cfg.read_text()!=text:raise ValueError('Existing build config differs')
    cfg.write_text(text);config=load_config(cfg)
    with (folder/'progress.jsonl').open('a',buffering=1) as log:
        def progress(record):
            record={'time':time.time(),**record};line=json.dumps(record)
            log.write(line+'\n');print(line,flush=True)
        with threadpool_limits(limits=c['runtime']['blas_threads']):
            manifest=build(config,shard_workers=options['shard_workers'],graph_threads=options['graph_threads'],
                checkpoint_directory=options['checkpoint_directory'],
                reserve_disk_bytes=options['reserve_disk_bytes'],progress=progress)
        progress({'phase':'complete','rows':manifest['rows'],'build_seconds':manifest['build_seconds']})

if __name__=='__main__':main()
