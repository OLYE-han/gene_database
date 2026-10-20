"""Calibrate budgets on separate queries with the complete index, then publish a deployment plan."""
import argparse
import copy
import json
from pathlib import Path
import socket
import sys
import yaml
from threadpoolctl import threadpool_limits
from deploy_cluster import save,digest
from full_truth import FullTruth,select_queries

PROJECT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT/'python'))
from dhdgs.config import load_config,snapshot
from dhdgs.cluster_test import run_local_cluster
from dhdgs.metrics import export_query_csv,export_resource_csvs

def choose_candidate(rows,target):
    eligible=[r for r in rows if r['failed_queries']==0 and r['incomplete_metrics']==0 and r['recall']>=target]
    if not eligible:raise ValueError('No calibration candidate meets the requested recall')
    return min(eligible,key=lambda r:(r['p99_ms'],-r['qps']))

def select_for_deployment(path):
    """The scan stopping margin is separate from the user's deployment target."""
    path=Path(path).resolve();root=path.parent;plan=yaml.safe_load(path.read_text())
    rows=json.loads((root/'calibration/comparison.json').read_text())
    chosen=choose_candidate(rows,plan['dhdgs']['experiment']['target_recall'])
    if plan['dhdgs']['search']['max_expansions']!=chosen['candidate']['max_expansions'] or plan['dhdgs']['search']['frontier_size']!=chosen['candidate']['frontier_size']:
        if (root/'prepared.json').exists():raise ValueError('Cannot change a prepared deployment')
        archive=root/'deployment.calibration_stop.yaml'
        if not archive.exists():archive.write_text(path.read_text())
        plan['dhdgs']['search'].update(chosen['candidate']);path.write_text(yaml.safe_dump(plan,sort_keys=False))
    save(root/'calibration/deployment_selection.json',{'selected':chosen,
        'target_recall':plan['dhdgs']['experiment']['target_recall'],
        'policy':'Lowest measured calibration p99 among successful complete-metric candidates meeting requested recall; then highest QPS',
        'held_out_validation_still_required':True})

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--plan',required=True)
    args=parser.parse_args();path=Path(args.plan).resolve();root=path.parent
    plan=yaml.safe_load(path.read_text());opts=plan['full_run'];truth=FullTruth(path)
    selection=select_queries(path)
    index=Path(plan['deployment']['index_source']);rows=[];chosen=None
    calibration=root/'calibration';calibration.mkdir(exist_ok=True)
    for candidate in opts['calibration_candidates']:
        name=f"budget{candidate['max_expansions']}_frontier{candidate['frontier_size']}"
        out=calibration/name
        if (out/'summary.json').exists():
            summary=json.loads((out/'summary.json').read_text())
            if summary['index_manifest_sha256']!=digest(index/'manifest.json'):raise ValueError('Calibration index changed')
        else:
            out.mkdir();(out/'index').symlink_to(index,target_is_directory=True)
            c=copy.deepcopy(plan['dhdgs']);c['paths']['output']=str(out)
            c['search'].update(candidate);c['query']['row']=opts['calibration_query_row']
            c['experiment'].update(benchmark_queries=opts['calibration_queries'],benchmark_concurrency=1,benchmark_repetitions=1)
            c['peer']['head_advertise_host']='127.0.0.1'
            sockets=[]
            try:
                for node in c['cluster']['nodes']:
                    sock=socket.socket();sock.bind(('127.0.0.1',0));sockets.append(sock)
                    node.update(host='127.0.0.1',port=sock.getsockname()[1],index_directory=str(index/f"shard_{node['id']}"))
                config=out/'experiment.yaml';config.write_text(yaml.safe_dump(c,sort_keys=False));c=load_config(config)
                run=out/'run';snapshot(c,run)
            finally:
                for sock in sockets:sock.close()
            with threadpool_limits(limits=c['runtime']['blas_threads']):
                report=run_local_cluster(c,run,truth_provider=truth,query_rows=selection['calibration'])
            save(run/'report.json',report);export_query_csv(run/'queries.csv',report['queries']);export_resource_csvs(run,report)
            with (run/'queries.jsonl').open('x') as f:
                for q in report['queries']:f.write(json.dumps(q,allow_nan=False)+'\n')
            summary={'candidate':candidate,'recall':report['recall_at_k'],'p99_ms':report['latency_ms'].get('p99'),
                'qps':report['qps'],'failed_queries':report['failed_queries'],
                'incomplete_metrics':sum(not q['stats'].get('metrics_complete',False) for q in report['queries']),
                'index_manifest_sha256':digest(index/'manifest.json'),'report':str(run/'report.json')}
            save(out/'summary.json',summary)
        rows.append(summary);print(json.dumps(summary),flush=True);save(calibration/'comparison.json',rows)
        if summary['failed_queries']==0 and summary['incomplete_metrics']==0 and summary['recall']>=opts['calibration_target_recall']:
            chosen=summary;break
    if chosen is None:
        save(calibration/'selection.json',{'target_met':False,'results':rows})
        raise RuntimeError('Calibration target not reached; inspect recall before deploying')
    plan['dhdgs']['search'].update(chosen['candidate'])
    selected=root/'deployment.selected.yaml';body=yaml.safe_dump(plan,sort_keys=False)
    if selected.exists() and selected.read_text()!=body:raise ValueError('Selected plan already differs')
    selected.write_text(body)
    save(calibration/'selection.json',{'target_met':True,'selected':chosen,'plan':str(selected),
        'policy':'First increasing-budget candidate reaching calibration target; physical-node held-out validation still required'})

if __name__=='__main__':main()
