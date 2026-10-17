"""Read-only checks of completed runs and generated candidate configurations."""
from pathlib import Path
import hashlib,json
from dhdgs.config import load_config
r=Path(__file__).resolve().parent
prov=json.loads((r/'provenance.json').read_text())
for name,digest in prov['source_sha256'].items():
    assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest,name
expected={Path(name).name:digest for name,digest in prov['source_sha256'].items() if Path(name).suffix=='.py' and Path(name).parent.name=='dhdgs'}
reports=[];count=failed=revisit=unknown_io=incomplete=control_errors=0
for stage,n in [('calibration',72),('validation',24),('confirmation',24)]:
    records=json.loads((r/f'{stage}_results.json').read_text());assert len(records)==n
    for item in records:
        p=Path(item['report']);report=json.loads(p.read_text());reports.append(p)
        assert report['source_sha256']==expected
        assert all(s['stopped'] for s in report['local_cluster']['services'])
        assert all(n['active_after']==0 for n in report['node_process_load'])
        unknown_io+=sum(n['process_deltas']['storage_read_bytes'] is None for n in report['node_process_load'])
        for q in report['queries']:
            s=q['stats'];count+=1;failed+=q['status']!='ok';revisit+=s.get('node_revisit_count',0)
            incomplete+=s.get('metrics_complete') is not True;control_errors+=s.get('control_errors',0)
            assert q['duplicate_result_ids']==0 and s['expanded']<=64
            assert abs(sum(s['latency_breakdown_ms'].values())-s['latency_ms'])<1e-5
for p in (r/'recommended').glob('*/config.yaml'):
    c=load_config(p);assert Path(c['paths']['output'],'index/manifest.json').exists()
remaining=[]
for d in Path('/proc').iterdir():
    if not d.name.isdigit():continue
    try:args=(d/'cmdline').read_bytes().decode(errors='replace').split('\0')
    except (OSError,ProcessLookupError):continue
    is_server=any(args[i:i+3]==['-m','dhdgs','serve'] for i in range(len(args)))
    if is_server and any(str(r) in a for a in args):remaining.append(d.name)
assert not remaining,remaining
assert count==39936 and failed==0 and revisit==0 and unknown_io==0
result={'reports':len(reports),'requests':count,'failed_queries':failed,'node_revisits':revisit,
        'successful_queries_with_incomplete_resource_metrics':incomplete,'control_error_events':control_errors,
        'all_services_stopped':True,'no_running_expands_after_measurement':True,'product_source_unchanged':True,
        'recommended_configs_valid':True,'wall_breakdowns_conserved':True,'unknown_process_io_counters':unknown_io}
(r/'verification.json').write_text(json.dumps(result,indent=2))
files=[p for p in r.iterdir() if p.is_file() and p.name!='final_artifacts_sha256.json']
files+=reports+[p.parent/'resolved.yaml' for p in reports]+list((r/'recommended').glob('*/config.yaml'))
(r/'final_artifacts_sha256.json').write_text(json.dumps({str(p.relative_to(r)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
print(json.dumps(result,indent=2))
