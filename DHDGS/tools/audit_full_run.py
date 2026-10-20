"""Independent audit of full-index physical-node results and immutable inputs."""
import argparse
import json
from pathlib import Path
import statistics
import numpy as np
from deploy_cluster import Deployment,digest,save,PROJECT
from full_truth import select_queries

def read(path):return json.loads(Path(path).read_text())

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--plan',required=True)
    args=parser.parse_args();d=Deployment(args.plan);root=d.root
    marker=read(root/'prepared.json');manifest=read(d.index/'manifest.json')
    assert manifest['rows']==manifest['dataset']['rows']==2000000
    assert marker['plan_sha256']==digest(d.plan_path)
    for p in d.source_files():assert marker['source_sha256'][str(p.relative_to(PROJECT.parent))]==digest(p)
    for name,sha in marker['index_sha256'].items():assert digest(d.index/name)==sha
    selection=select_queries(args.plan);assert not set(selection['calibration'])&set(selection['validation'])
    expected=set(selection['validation']);results=[];hosts=set();total=0
    for mode in d.modes:
        folder=root/mode['name'];mode_hosts=set()
        for sid in range(3):
            ready=read(folder/f'services/node_{sid}_ready.json');stopped=read(folder/f'services/node_{sid}_stopped.json')
            assert stopped['supervisor_exit_code']==0 and stopped['event']['exit_code']==0 and stopped['event']['stopped']
            assert ready['pid']==stopped['event']['pid'];mode_hosts.add(ready['hostname'])
            network=read(folder/f'network_node_{sid}.json')
            assert network['status']=='probe_complete' and len(network['connections'])==4
            assert all(n['source_ip']==d.base['cluster']['nodes'][sid]['host'] for n in network['connections'])
        assert len(mode_hosts)==3;hosts.update(mode_hosts)
        for concurrency in d.d['client_concurrencies']:
            report=read(folder/f'c{concurrency}/run/report.json');queries=report['queries']
            assert report['indexed_rows']==2000000 and report['index_epoch']==manifest['epoch']
            assert report['failed_queries']==0 and set(q['query_row'] for q in queries)==expected
            for q in queries:
                s=q['stats'];assert s['metrics_complete'] and s['node_revisit_count']==s['router_expand_rpc_count']==0
                expected_backend='sync_pread' if mode['execution_model']=='blocking' else mode['backend']
                assert s['io_backends']==[expected_backend]
                assert s['expanded']<=d.base['search']['max_expansions']
                assert q['duplicate_result_ids']==0 and q['returned_count']==d.base['search']['top_k']
                assert len(q['branch_reports'])<=3
                for branch in q['branch_reports']:
                    assert branch['work_spent']+branch['work_unused']+sum(ch['work_grant'] for ch in branch['children'])==branch['work_grant']
                    for child in branch['children']:
                        assert sum(x['decision_id']==child['decision_id'] and x['remote'] and x['selected_node']==child['node_id'] for x in branch['decisions'])==1
            # Resample unique query identities; repeated requests are not new quality samples.
            recalls=np.array([statistics.mean(q['recall'] for q in queries if q['query_row']==row) for row in sorted(expected)])
            boot=np.random.default_rng(42).choice(recalls,(2000,len(recalls)),replace=True).mean(axis=1)
            physical=[n['process_deltas']['storage_read_bytes'] for n in report['node_process_load']]
            result={'mode':mode['name'],'concurrency':concurrency,'requests':len(queries),'qps':report['qps'],
                'recall':report['recall_at_k'],'recall_bootstrap_95_percent_interval':np.quantile(boot,[.025,.975]).tolist(),
                'target_met':report['recall_at_k']>=d.base['experiment']['target_recall'],
                'latency_ms':report['latency_ms'],'mean_branches':statistics.mean(q['stats']['branch_count'] for q in queries),
                'mean_file_bytes':statistics.mean(q['stats']['file_read_bytes'] for q in queries),
                'mean_protocol_bytes':statistics.mean(q['stats']['all_protocol_bytes'] for q in queries),
                'mean_protocol_rpcs':statistics.mean(q['stats']['all_protocol_rpc_count'] for q in queries),
                'physical_read_bytes':sum(physical) if all(x is not None for x in physical) else None,
                'nodes':report['node_process_load'],'timing':report['query_timing_summary']}
            results.append(result);total+=len(queries)
    save(root/'verification.json',{'passed':True,'hosts':sorted(hosts),'requests':total,'indexed_rows':manifest['rows'],
        'all_services_stopped':True,'all_dispatches_approved':True,'all_budgets_conserved':True,
        'source_and_index_hashes_verified':True,'validation_is_disjoint':True,
        'all_recall_targets_met':all(r['target_met'] for r in results),'results':results})
    print(json.dumps({'requests':total,'passed':True,'all_recall_targets_met':all(r['target_met'] for r in results)}))

if __name__=='__main__':main()
