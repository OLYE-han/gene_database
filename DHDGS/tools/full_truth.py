"""Validate and reuse supplied full-corpus L2 truth, never subset truth."""
from pathlib import Path
import argparse
import hashlib
import json
import sys
import time
import numpy as np
import yaml
from threadpoolctl import threadpool_limits

PROJECT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT/'python'))
from dhdgs.data import Dataset,load_complete_npy,fingerprint
from dhdgs.benchmark import exact_topk
from deploy_cluster import digest,save

def select_queries(plan_path):
    path=Path(plan_path).resolve();plan=yaml.safe_load(path.read_text());opts=plan['full_run']
    total=len(load_complete_npy(Path(plan['dhdgs']['query']['vectors'])))
    ncal=opts['calibration_queries'];nval=opts['validation_queries']
    chosen=np.random.default_rng(opts['query_selection_seed']).choice(total,ncal+nval,replace=False)
    result={'seed':opts['query_selection_seed'],'population':total,
            'calibration':sorted(chosen[:ncal].tolist()),'validation':sorted(chosen[ncal:].tolist())}
    target=path.parent/'query_selection.json'
    if target.exists() and json.loads(target.read_text())!=result:raise ValueError('Query selection changed')
    if not target.exists():save(target,result)
    return result

class FullTruth:
    def __init__(self,plan_path,require_verified=True):
        self.plan_path=Path(plan_path).resolve();self.plan=yaml.safe_load(self.plan_path.read_text())
        self.selection=select_queries(self.plan_path)
        c=self.plan['dhdgs'];self.dataset=Dataset(c['paths']['dataset']);self.identity=self.dataset.identity()
        self.path=Path(self.plan['full_run']['ground_truth_path'])
        self.qpath=Path(c['query']['vectors']);self.idpath=Path(c['query']['ids'])
        self.queries=load_complete_npy(self.qpath);self.qids=load_complete_npy(self.idpath)
        with np.load(self.path,allow_pickle=False) as f:
            if str(f['metric'])!='euclidean' or int(f['query_start'])!=0 or int(f['query_end'])!=len(self.queries):
                raise ValueError('Unexpected supplied ground truth metric/range')
            if not np.array_equal(f['query_ids'],self.qids):raise ValueError('Ground truth query IDs differ')
            self.ids=f['ids'];self.distances=f['distances']
        if self.ids.shape!=self.distances.shape or self.ids.shape[0]!=len(self.qids) or not np.isfinite(self.distances).all():
            raise ValueError('Invalid truth dimensions/distances')
        if np.any(self.ids>=self.dataset.rows) or np.any(self.distances<0):raise ValueError('Invalid truth IDs/distances')
        if len(np.unique(self.qids))!=len(self.qids):raise ValueError('Duplicate query identities')
        self.lookup={int(v):i for i,v in enumerate(self.qids)};self._indexed=None
        self.provenance={'kind':'supplied_full_corpus_euclidean_truth','path':str(self.path),'sha256':digest(self.path),
            'dataset':self.identity,'query_vectors_fingerprint':list(fingerprint(self.qpath)),
            'query_ids_sha256':digest(self.idpath),'indexed_rows':self.dataset.rows,
            'query_selection_sha256':digest(self.plan_path.parent/'query_selection.json'),
            'self_excluded':True,'verification':str(self.plan_path.parent/'truth_validation.json')}
        if require_verified:
            validation=json.loads((self.plan_path.parent/'truth_validation.json').read_text())
            if validation['provenance']!=self.provenance or not validation['passed']:
                raise ValueError('Ground truth has not passed validation for these inputs')
            self.provenance={**self.provenance,'validation_queries':len(validation['queries'])}

    def __call__(self,dataset,indexed,query,k,metric,excluded_id,block_rows):
        if dataset.identity()!=self.identity or metric!='l2' or excluded_id is None or k>self.ids.shape[1]:
            raise ValueError('Full truth incompatible with benchmark inputs')
        if self._indexed is not indexed:
            if len(indexed)!=dataset.rows or not np.array_equal(indexed,np.arange(dataset.rows,dtype=np.uint64)):
                raise ValueError('Full truth cannot evaluate a subset index')
            self._indexed=indexed
        row=self.lookup[int(excluded_id)]
        if not np.array_equal(query,self.queries[row]):raise ValueError('Query vector differs from truth source')
        ids=self.ids[row,:k]
        if excluded_id in ids or len(np.unique(ids))!=k:raise ValueError('Self/duplicate ID in supplied truth')
        return ids.tolist(),np.square(self.distances[row,:k]).tolist()

def validate_truth(path):
    truth=FullTruth(path,require_verified=False);p=truth.plan;f=p['full_run'];c=p['dhdgs']
    # Include both calibration and held-out evaluation ranges.
    count=f['ground_truth_validation_queries'];selection=select_queries(path)
    rows=sorted(set(group[int(offset)] for group in (selection['calibration'],selection['validation'])
        for offset in np.linspace(0,len(group)-1,max(1,count//2),dtype=int)))
    indexed=np.arange(truth.dataset.rows,dtype=np.uint64);records=[];started=time.perf_counter()
    with threadpool_limits(limits=f['ground_truth_threads']):
        for row in rows:
            qid=int(truth.qids[row]);q=truth.queries[row]
            if not np.array_equal(truth.dataset.take(np.array([qid],dtype='u8'))[0],q):
                raise ValueError('Query source ID does not match base vector')
            ids,distances=exact_topk(truth.dataset,indexed,q,c['search']['top_k'],'l2',qid,c['experiment']['exact_block_rows'])
            supplied,sd=truth(truth.dataset,indexed,q,c['search']['top_k'],'l2',qid,0)
            if set(ids)!=set(supplied) or not np.allclose(distances,sd,rtol=1e-8,atol=1e-8):
                raise ValueError(f'Supplied full truth mismatch at query {row}')
            records.append({'row':row,'id':qid,'matches':True});print(json.dumps(records[-1]),flush=True)
    truth.dataset.assert_unchanged()
    result={'passed':True,'provenance':truth.provenance,'queries':records,'seconds':time.perf_counter()-started,
            'scope':'Exact direct-difference fp64 full 2M scan on selected queries; other supplied rows not independently rescanned'}
    save(Path(path).resolve().parent/'truth_validation.json',result)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--plan',required=True)
    validate_truth(parser.parse_args().plan)
