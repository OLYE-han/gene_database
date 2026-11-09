"""Offline direct/descendant replica dispatch statistics; run after load stops."""
from pathlib import Path
import argparse
import csv
import gzip
import hashlib
import json
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'python'))
from dhdgs.network_api import records
from dhdgs.replica_branch_metrics import ReplicaBranchMetrics


def stream(path):
    if path.is_dir():yield from records(path);return
    opener=gzip.open if path.suffix=='.gz'else open
    with opener(path,'rt')as f:
        for line in f:
            if line.strip():yield json.loads(line)


def run(paths,output,replica_sources=None):
    output.mkdir(parents=True,exist_ok=False)
    metric=ReplicaBranchMetrics(replica_sources);inputs=[]
    with (output/'events.csv').open('w')as out:
        writer=None
        for path in paths:
            digest=hashlib.sha256();file_count=0
            files=sorted(path.glob('lane-*.frames*'))if path.is_dir()else[path]
            for f in files:
                digest.update(f.name.encode());file_count+=1
                with f.open('rb')as data:
                    while block:=data.read(1<<20):digest.update(block)
            inputs.append(dict(path=str(path.resolve()),files=file_count,sha256=digest.hexdigest()))
            for record in stream(path):
                for row in metric.add(record,str(path.resolve())):
                    if writer is None:writer=csv.DictWriter(out,fieldnames=list(row));writer.writeheader()
                    writer.writerow(row)
    result=metric.summary();result['inputs']=inputs
    (output/'summary.json').write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n')
    with (output/'groups.csv').open('w')as out:
        rows=[]
        for g in result['groups']:
            row={k:v for k,v in g.items()if not isinstance(v,dict)}
            for key in ['remaining_before','child_grant','local_expansions_before']:
                row.update({key+'_'+k:v for k,v in g[key].items()})
            rows.append(row)
        writer=csv.DictWriter(out,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    print(json.dumps(dict(counts=result['counts'],sampled=result['sampled'],output=str(output)),ensure_ascii=False))
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',required=True,type=Path,nargs='+',help='Raw results directory or sampled JSONL[.gz]; do not feed both copies of a run')
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--replica-sources',type=Path,help='Optional JSON version -> source node for peer-routed replicas')
    args=parser.parse_args()
    sources=json.loads(args.replica_sources.read_text())if args.replica_sources else None
    run(args.input,args.output,sources)
