"""Convert the independent network client's framed archive to JSON offline."""
import argparse,gzip,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'python'))
from dhdgs.network_api import records
p=argparse.ArgumentParser();p.add_argument('--directory',required=True);p.add_argument('--output',required=True);a=p.parse_args()
path=Path(a.directory);manifest=json.loads((path/'manifest.json').read_text());jobs=set();failed=0
with gzip.open(a.output,'xt') as out:
    for q in records(path/'results'):
        if q['job'] in jobs:raise ValueError('Duplicate result job')
        jobs.add(q['job']);failed+=not q['transport_ok'] or q['result'].get('status')!='ok'
        out.write(json.dumps(q,separators=(',',':'))+'\n')
if len(jobs)!=manifest['received']:raise ValueError('Missing results')
print(json.dumps({'records':len(jobs),'failed':failed,'output':a.output}))
