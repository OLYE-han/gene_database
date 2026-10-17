from pathlib import Path
import hashlib,json,os
root=Path(__file__).resolve().parent;project=root.parent.parent
provenance=json.loads((root/'provenance.json').read_text())
assert all(hashlib.sha256((project/path).read_bytes()).hexdigest()==value
           for path,value in provenance['source_sha256'].items())
result={'source_matches_benchmarked_code':True,'modes':{}}
for mode in ('blocking','thread_pool','io_uring'):
    r=json.loads((root/mode/'run/report.json').read_text())
    assert r['failed_queries']==0
    assert all(s['stopped'] for s in r['local_cluster']['services'])
    live=[]
    for service in r['local_cluster']['services']:
        try:os.kill(service['pid'],0);live.append(service['pid'])
        except ProcessLookupError:pass
    assert not live
    for row in r['queries']:
        st=row['stats'];assert st['node_revisit_count']==st['router_expand_rpc_count']==0
        assert st['expanded']<=64
        for branch in row['branch_reports']:
            assert len(branch['children'])<=1
            assert branch['work_spent']+branch['work_unused']+sum(c['work_grant'] for c in branch['children'])==branch['work_grant']
            for child in branch['children']:
                approvals=[d for d in branch['decisions'] if d['decision_id']==child['decision_id']
                           and d['remote'] and d['selected_node']==child['node_id']]
                assert len(approvals)==1
            if mode!='blocking':
                s=branch['stats'];parts=sum(s[k] for k in ('query_ready_queue_ms','query_compute_slice_wall_ms',
                    'query_io_wait_ms','query_transfer_finalize_wait_ms','query_state_other_ms'))
                assert abs(parts-s['query_branch_wall_ms'])<1e-5
                assert s['async_read_requests' if mode=='io_uring' else 'pread_calls']==branch['work_spent']
                for trace in branch['local_batch_trace']:
                    io=trace['io_trace'];assert io is not None
                    assert len(io['reads'])==len(set(io['ids']))
                    assert all(x['completion_ns']>=x['submit_ns'] for x in io['reads'])
    for node in r['node_process_load']:
        io=node['peer_lifecycle_after']['io']
        if mode!='blocking':
            assert io['inflight_requests']==io['active_batches']==io['reserved_bytes']==0
            assert io['counters']['peak_inflight_requests']<=8
            assert io['counters']['peak_reserved_bytes']<=4194304
            assert io['actual_backend']==mode and io['fallback_reason'] is None
    result['modes'][mode]={'queries':len(r['queries']),'all_services_stopped':True,
        'budget_and_dispatch_audit_passed':True,'round_trace_and_state_time_audit_passed':True}
(root/'verification.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
