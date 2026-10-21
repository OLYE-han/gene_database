import subprocess,json,gzip,hashlib,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parents[1]
def compress_case(case):
 entries=[]
 for source in sorted((case/'runs').glob('*/*')):
  if not (source.name.startswith('queries.part-') and source.suffix=='.json' or source.name=='report_context.json'):continue
  dest=source.with_suffix(source.suffix+'.gz');temp=dest.with_suffix(dest.suffix+'.tmp');h=hashlib.sha256()
  with source.open('rb') as f,gzip.open(temp,'wb',compresslevel=1) as out:
   while chunk:=f.read(1024*1024):h.update(chunk);out.write(chunk)
  check=hashlib.sha256()
  with gzip.open(temp,'rb') as f:
   while chunk:=f.read(1024*1024):check.update(chunk)
  assert check.digest()==h.digest()
  temp.replace(dest);entries.append(dict(original=str(source.relative_to(case)),compressed=str(dest.relative_to(case)),sha256=h.hexdigest(),bytes=source.stat().st_size,compressed_bytes=dest.stat().st_size));source.unlink()
 (case/'archive_manifest.json').write_text(json.dumps(entries,indent=2))
for i,early in enumerate([False,True,True,False]):
 name=f'{i:02d}_'+('early' if early else 'wait_ack')
 argv=[str(PROJECT/'.venv/bin/python'),str(ROOT/'experiment.py'),'three/'+name,'cpp','--topology','three','--count','10000','--concurrency','256','--threads','64','--route-threads','16','--head-connections','128','--network-mode','fixed','--connections','128','--control-capacity','1024']
 if early:argv.append('--early')
 with (ROOT/(name+'.log')).open('w') as f:p=subprocess.run(argv,cwd=PROJECT,stdout=f,stderr=subprocess.STDOUT)
 print(name,p.returncode,flush=True)
 if p.returncode:raise SystemExit(p.returncode)
 compress_case(ROOT/'three'/name)
 subprocess.run([str(PROJECT/'.venv/bin/python'),str(ROOT/'plot_p99.py'),'--case',str(ROOT/'three'/name),'--output',str(ROOT/'p99'/name)],cwd=PROJECT,stdout=subprocess.DEVNULL,check=True)
