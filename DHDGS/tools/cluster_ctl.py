"""Send configuration and test commands only to the persistent HNSW head."""
import argparse
import json
from pathlib import Path
import sys
import urllib.error
import urllib.request
import yaml
from cluster_service import load_plan

def request(plan,op,body=None):
    control=plan['control'];token=Path(control['token_file']).read_text().strip()
    data=None if op=='status' else json.dumps(body or {},allow_nan=False).encode()
    req=urllib.request.Request(f"http://{control['advertise_host']}:{control['port']}/{op}",data=data,
        headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'},method='GET' if op=='status' else 'POST')
    # Internal management addresses should not go through an HTTP proxy.
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req,timeout=control['benchmark_timeout_seconds']) as response:value=json.load(response)
    except urllib.error.HTTPError as exc:raise RuntimeError(exc.read().decode()) from exc
    if not value.get('ok'):raise RuntimeError(str(value))
    return value['result']

def main():
    p=argparse.ArgumentParser();p.add_argument('--plan',type=Path,required=True)
    p.add_argument('command',choices=['status','recover','configure','query','benchmark','collect','stop_nodes','shutdown'])
    p.add_argument('--request',type=Path,help='YAML body: dotted config keys, benchmark settings, or query row')
    args=p.parse_args();plan=load_plan(args.plan)
    body=yaml.safe_load(args.request.read_text()) if args.request else {}
    if not isinstance(body,dict):raise ValueError('Request YAML must be a mapping')
    print(json.dumps(request(plan,args.command,body),indent=2,allow_nan=False))

if __name__=='__main__':main()
