"""Build a reusable source-local directory from the frozen global router universe."""
from pathlib import Path
import argparse,json,sys,time,resource
P=Path(__file__).resolve().parents[1];sys.path.insert(0,str(P/'python'))
import numpy as np
from dhdgs.config import load_config
from dhdgs.data import Dataset
from dhdgs.index import Shard
from dhdgs.geometry import build_directory

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--index',required=True,type=Path);p.add_argument('--config',required=True,type=Path)
    p.add_argument('--node',type=int,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--dataset',required=True)
    p.add_argument('--summary',type=Path,required=True);a=p.parse_args()
    c=load_config(a.config);shard=Shard(a.index/f'shard_{a.node}',0);started=time.monotonic();cpu=time.process_time();status='failed'
    def io():return {line.split(':')[0]:int(line.split(':')[1])for line in Path('/proc/self/io').read_text().splitlines()}
    before=io();stats={}
    try:
        dataset=Dataset(a.dataset);ids=np.load(a.index/'route_ids.npy');points=dataset.take(ids)
        stats=build_directory(shard,ids,points,a.output,c['load_balance'],progress=lambda event:print(json.dumps(event),flush=True))
        dataset.assert_unchanged();status='complete'
    except BaseException as e:stats['error']=repr(e);raise
    finally:
        shard.close();a.summary.write_text(json.dumps(dict(status=status,wall_seconds=time.monotonic()-started,cpu_seconds=time.process_time()-cpu,
            peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,io_before=before,io_after=io(),result=stats),indent=2))
if __name__=='__main__':main()
