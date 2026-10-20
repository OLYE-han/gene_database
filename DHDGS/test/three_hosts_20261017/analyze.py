"""Audit archived physical-host results without modifying product sources."""
from pathlib import Path
import hashlib,json,statistics,sys
root=Path(__file__).resolve().parent
project=root.parent.parent
sys.path.insert(0,str(project/'tools'))
from deploy_cluster import Deployment

def read(p):return json.loads(p.read_text())

d=Deployment(root/'deployment.yaml');marker=read(root/'prepared.json')
assert marker['plan_sha256']==hashlib.sha256((root/'deployment.yaml').read_bytes()).hexdigest()
for p in d.source_files():assert marker['source_sha256'][str(p.relative_to(project.parent))]==hashlib.sha256(p.read_bytes()).hexdigest()
assert all(hashlib.sha256((d.index/k).read_bytes()).hexdigest()==v for k,v in marker['index_sha256'].items())
rows=[];total=0;incomplete=0;all_hosts=set()
for mode in d.modes:
    folder=root/mode['name'];hosts=set()
    for sid in range(3):
        ready=read(folder/f'services/node_{sid}_ready.json');hosts.add(ready['hostname'])
        stopped=read(folder/f'services/node_{sid}_stopped.json')
        assert stopped['supervisor_exit_code']==0 and stopped['event']['stopped']
        assert stopped['event']['pid']==ready['pid'] and stopped['event']['exit_code']==0
        network=read(folder/f'network_node_{sid}.json')
        assert network['status']=='probe_complete' and network['hostname']==ready['hostname']
        assert len(network['connections'])==4
        assert all(n['source_ip']==d.base['cluster']['nodes'][sid]['host'] for n in network['connections'])
    assert len(hosts)==3;all_hosts.update(hosts)
    for concurrency in d.d['client_concurrencies']:
        report=read(folder/f'c{concurrency}/run/report.json')
        assert report['failed_queries']==0
        for q in report['queries']:
            s=q['stats'];assert s['node_revisit_count']==s['router_expand_rpc_count']==0
            assert s['expanded']<=d.base['search']['max_expansions']
            incomplete+=int(not s['metrics_complete'])
            expected='sync_pread' if mode['execution_model']=='blocking' else mode['backend']
            assert s['io_backends']==[expected]
            assert len(q['branch_reports'])<=3
            for branch in q['branch_reports']:
                assert len(branch['children'])<=1
                assert branch['work_spent']+branch['work_unused']+sum(ch['work_grant'] for ch in branch['children'])==branch['work_grant']
                for ch in branch['children']:
                    assert sum(x['decision_id']==ch['decision_id'] and x['remote'] and x['selected_node']==ch['node_id'] for x in branch['decisions'])==1
        total+=len(report['queries'])
        r={'mode':mode['name'],'concurrency':concurrency,'qps':report['qps'],
           'recall':report['recall_at_k'],'p99_ms':report['latency_ms']['p99'],'mean_ms':report['latency_ms']['mean'],
           'physical_read_bytes':sum(n['process_deltas']['storage_read_bytes'] or 0 for n in report['node_process_load']),
           'complete_metrics':sum(q['stats']['metrics_complete'] for q in report['queries']),
           'mean_branches':statistics.mean(q['stats']['branch_count'] for q in report['queries']),
           'mean_file_bytes':statistics.mean(q['stats']['file_read_bytes'] for q in report['queries'])}
        rows.append(r)
result={'hosts':sorted(all_hosts),'tests':len(rows),'requests':total,'incomplete_metrics':incomplete,
        'source_and_index_hashes_verified':True,'all_dispatches_approved':True,'all_budgets_conserved':True,
        'all_services_stopped_gracefully':True,'all_data_and_callback_paths_verified':True,'results':rows}
(root/'verification.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
