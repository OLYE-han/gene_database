"""Diagnostic only: instrument ready stages, exclude from performance comparisons."""
import os
for k in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[k]='1'
from pathlib import Path
import cProfile,pstats,threading,json,traceback,faulthandler
import run_scan
R=Path(__file__).resolve().parent

def main():
    from dhdgs.head_flow import QueryFlow
    diagnostic=(R/'profile_single_thread'/'tracebacks.log').open('w')
    faulthandler.dump_traceback_later(15,repeat=True,file=diagnostic)
    original=QueryFlow._step;profiles=[cProfile.Profile()];lock=threading.Lock();chosen=[]
    def step(self,rec,operation):
        with lock:
            if not chosen:chosen.append(threading.get_ident())
            selected=chosen[0]==threading.get_ident()
        if not selected:return original(self,rec,operation)
        try:return profiles[0].runcall(original,self,rec,operation)
        except BaseException as exc:
            traceback.print_exc()
            if not rec['output'].done():rec['output'].set_exception(exc)
            with self.cv:self.records.pop(id(rec),None);self.cv.notify_all()
            raise
    QueryFlow._step=step
    case=R/'profile_single_thread';(case/'case.json').write_text(json.dumps(dict(mode=2,queries=2000,concurrency=128,warmup=32)))
    run_scan.child(case)
    faulthandler.cancel_dump_traceback_later();diagnostic.close()
    stats=pstats.Stats(profiles[0])
    for p in profiles[1:]:stats.add(p)
    stats.dump_stats(str(case/'stages.pstats'))
    with (case/'stages.txt').open('w') as f:
        stats.stream=f;stats.sort_stats('cumtime').print_stats(60);stats.sort_stats('tottime').print_stats(60)
if __name__=='__main__':main()
