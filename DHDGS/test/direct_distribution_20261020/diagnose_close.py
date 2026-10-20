import os
for k in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[k]='1'
from pathlib import Path
import faulthandler,sys,time
import numpy as np
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from dhdgs.managed_cluster import ClusterHead

def main():
    plan=load_plan(R/'cases/01_direct/plan.yaml');c=plan['dhdgs'];c['peer']['head_port']=22700
    c['peer']['head_direct'].update(result_processes=2,save_results=False)
    plan['control']['results_directory']=str(R/'close_diagnostic')
    head=ClusterHead(c,plan['control'],Path(plan['control']['token_file']).read_text().strip())
    x=np.load(c['query']['vectors'],mmap_mode='r');ids=np.load(c['query']['ids'],mmap_mode='r')
    print('query',head.engine.search(x[0],int(ids[0]))['status'],flush=True)
    for obj,label in ((head.engine.head_flow,'flow'),(head.engine.head_hub,'hub'),(head.engine.transport,'transport')):
        original=obj.close
        def close(fn=original,name=label):
            print('closing',name,time.monotonic(),flush=True);fn();print('closed',name,time.monotonic(),flush=True)
        obj.close=close
    faulthandler.dump_traceback_later(5,repeat=True)
    head.close();faulthandler.cancel_dump_traceback_later()
if __name__=='__main__':main()
