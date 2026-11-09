"""Offline review command; never modifies graph or live routing."""
import argparse,json
from pathlib import Path
import yaml
from dhdgs.config import StrictLoader
from dhdgs.layout_policy import plan_layout

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--input',type=Path,required=True,help='observations and candidate measured portfolio costs')
    parser.add_argument('--now-ms',type=float,required=True,help='Same clock domain as input observations')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();data=json.loads(args.input.read_text())
    result=plan_layout(data['observation'],data['candidates'],yaml.load(args.config.read_text(),Loader=StrictLoader)['load_balance']['layout_policy'],args.now_ms)
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
if __name__=='__main__':main()
