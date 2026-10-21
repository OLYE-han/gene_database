import concurrent.futures,hashlib,json,shlex,shutil,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent;PROJECT=ROOT.parents[1]
hosts=json.loads((ROOT/'deployment.json').read_text());manifest=json.loads((ROOT/'source_hashes.json').read_text())
changes={}
for rel in manifest:
 content=(PROJECT/rel).read_bytes();digest=hashlib.sha256(content).hexdigest()
 if digest!=manifest[rel]:
  if rel.startswith('cpp/') or rel=='CMakeLists.txt':raise RuntimeError('Native code changed; rebuild required')
  changes[rel]=content.decode();shutil.copy2(PROJECT/rel,ROOT/'source'/rel);manifest[rel]=digest
code='''import hashlib,json,sys
from pathlib import Path
p=json.load(sys.stdin);root=Path(p['project'])
for rel,text in p['changes'].items():(root/rel).write_text(text)
hashes={rel:hashlib.sha256((root/rel).read_bytes()).hexdigest() for rel in p['manifest']}
sys.path.insert(0,str(root/'python'))
from dhdgs import _native
assert hasattr(_native,'NativeHead') and hasattr(_native,'NativeDataRuntime')
print(json.dumps({'mismatches':[rel for rel,h in hashes.items() if h!=p['manifest'][rel]],'native_path':_native.__file__,'native_sha256':hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest(),'python':sys.version}))
'''
def check(item):
 sid,h=item;argv=[h['python'],'-c',code]
 if h['ssh']:argv=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8',h['ssh'],shlex.join(argv)]
 result=subprocess.run(argv,input=json.dumps(dict(project=h['project'],changes=changes,manifest=manifest)),text=True,capture_output=True,timeout=180,check=True)
 return sid,json.loads(result.stdout)
with concurrent.futures.ThreadPoolExecutor(3) as pool:results=dict(pool.map(check,hosts.items()))
(ROOT/'source_hashes.json').write_text(json.dumps(manifest,indent=2));(ROOT/'deployed_provenance.json').write_text(json.dumps(results,indent=2))
assert all(not r['mismatches'] for r in results.values())
print(json.dumps(results,indent=2))
