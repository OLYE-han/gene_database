"""Run one saved local TCP configuration using verified full-corpus truth."""
import argparse
from datetime import datetime
import gzip
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parent;PROJECT=ROOT.parent.parent
sys.path[:0]=[str(PROJECT/'python'),str(PROJECT/'tools')]
import yaml
from threadpoolctl import threadpool_limits
from deploy_cluster import save,digest
from full_truth import FullTruth,select_queries
from dhdgs.config import load_config,snapshot
from dhdgs.cluster_test import run_local_cluster
from dhdgs.metrics import export_query_csv,export_resource_csvs


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',required=True);args=parser.parse_args()
    c=load_config(args.config)
    baseline=yaml.safe_load((ROOT/'experiment.yaml').read_text())['baseline_plan']
    truth=FullTruth(baseline);rows=select_queries(baseline)['validation']
    count=c['experiment']['benchmark_queries']
    if count>len(rows):raise ValueError('This experiment has 1024 frozen validation rows; provide a separate selection for larger studies')
    stamp=datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%dT%H%M%S_%f')
    run=Path(c['paths']['output'])/'runs'/stamp;snapshot(c,run)
    with threadpool_limits(limits=c['runtime']['blas_threads']):
        report=run_local_cluster(c,run,truth_provider=truth,query_rows=rows[:count])
    report['ground_truth_source']=truth.provenance
    with gzip.open(run/'report.json.gz','wt',compresslevel=1) as f:json.dump(report,f,allow_nan=False)
    with gzip.open(run/'queries.jsonl.gz','wt',compresslevel=1) as f:
        for q in report['queries']:f.write(json.dumps(q,allow_nan=False)+'\n')
    export_query_csv(run/'queries.csv',report['queries']);export_resource_csvs(run,report)
    save(run/'native_provenance.json',{'native_sha256':digest(next((PROJECT/'python/dhdgs').glob('_native*.so'))),
                                     'query_selection_sha256':digest(Path(baseline).parent/'query_selection.json')})
    summary={k:report[k] for k in ('qps','recall_at_k','latency_ms','failed_queries','query_count')}
    summary['output']=str(run);save(run/'summary.json',summary);print(json.dumps(summary))
    if report['failed_queries'] or any(not q['stats'].get('metrics_complete') for q in report['queries']):
        raise RuntimeError('Run has failures or incomplete metrics; inspect preserved records')


if __name__=='__main__':main()
