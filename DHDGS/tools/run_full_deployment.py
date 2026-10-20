"""Use validated supplied truth during the ordinary physical-host workflow."""
import argparse
from deploy_cluster import Deployment
from full_truth import FullTruth,select_queries
from calibrate_full import select_for_deployment

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--plan',required=True)
    parser.add_argument('--phase',choices=['prepare','run','all'],default='all')
    args=parser.parse_args();select_for_deployment(args.plan);deployment=Deployment(args.plan)
    deployment.truth_provider=FullTruth(args.plan)
    deployment.query_rows=select_queries(args.plan)['validation']
    if args.phase in ('prepare','all'):deployment.prepare()
    if args.phase in ('run','all'):deployment.run_all()

if __name__=='__main__':main()
