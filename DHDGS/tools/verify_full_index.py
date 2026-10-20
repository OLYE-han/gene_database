"""Check full coverage, code ownership, stored samples and every shared-node edge union."""
import argparse
import json
from pathlib import Path
import sys
import time
import numpy as np
import yaml
from deploy_cluster import save,digest
PROJECT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT/'python'))
from dhdgs.data import Dataset,load_complete_npy
from dhdgs.index import native

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--plan',required=True)
    path=Path(parser.parse_args().plan).resolve();plan=yaml.safe_load(path.read_text())
    root=Path(plan['deployment']['index_source']);m=json.loads((root/'manifest.json').read_text())
    dataset=Dataset(plan['dhdgs']['paths']['dataset']);opts=plan['full_run'];start=time.perf_counter()
    assert dataset.identity()==m['dataset'] and dataset.rows==m['rows']==2000000
    ids=load_complete_npy(root/'ids.npy');membership=load_complete_npy(root/'membership.npy')
    assert np.array_equal(ids,np.arange(dataset.rows,dtype='u8')) and np.all(membership.sum(axis=1)>=1)
    files=[];checks=[];rng=np.random.default_rng(plan['dhdgs']['seed'])
    for sid,meta in enumerate(m['shards']):
        directory=root/f'shard_{sid}';local=load_complete_npy(directory/'ids.npy')
        assert np.array_equal(local,ids[membership[:,sid]]) and len(local)==meta['rows']
        assert (directory/'nodes.bin').stat().st_size==meta['header_bytes']+len(local)*meta['record_bytes']
        dtype=np.dtype({'names':['vector','degree','neighbors'],
            'formats':[('<f4',(dataset.dimension,)),'<u4',('<u8',(meta['max_degree'],))],
            'offsets':[0,4*dataset.dimension,4*dataset.dimension+4],'itemsize':meta['record_bytes']})
        records=np.memmap(directory/'nodes.bin',mode='r',dtype=dtype,offset=meta['header_bytes'],shape=(len(local),))
        positions=rng.choice(len(local),min(opts['index_validation_samples'],len(local)),replace=False)
        assert np.array_equal(records[positions]['vector'],dataset.take(local[positions]))
        assert np.all(records[positions]['degree']<=meta['max_degree'])
        for row in records[positions]:
            edges=row['neighbors'][:int(row['degree'])]
            assert np.all(edges<dataset.rows) and len(np.unique(edges))==len(edges)
        code_ids=load_complete_npy(directory/'code_ids.npy');owners=load_complete_npy(directory/'code_membership.npy')
        assert np.array_equal(owners,membership[code_ids])
        bank_positions=rng.choice(len(code_ids),min(opts['index_validation_samples'],len(code_ids)),replace=False)
        centers=load_complete_npy(directory/'centers.npy');center_ids=load_complete_npy(directory/'center_ids.npy')
        quantizer=native().Quantizer(centers,m['bits'],m['rotation_seed'],(directory/'rotation.bin').read_bytes())
        encoded=quantizer.encode(dataset.take(code_ids[bank_positions]),center_ids[bank_positions])
        for name,values in zip(('codes','extra','factors'),encoded):
            actual=load_complete_npy(directory/f'{name}.npy')[bank_positions]
            assert np.array_equal(actual,values),f'RaBitQ stored {name} differs'
        files.append((local,records));checks.append({'node':sid,'rows':len(local),'stored_samples_checked':len(positions),'code_samples_checked':len(bank_positions)})
    overlaps=[]
    for a in range(len(files)):
        for b in range(a+1,len(files)):
            shared=ids[membership[:,a]&membership[:,b]]
            for begin in range(0,len(shared),plan['dhdgs']['data']['block_rows']):
                block=shared[begin:begin+plan['dhdgs']['data']['block_rows']]
                x=files[a][1][np.searchsorted(files[a][0],block)]
                y=files[b][1][np.searchsorted(files[b][0],block)]
                assert np.array_equal(x['vector'],y['vector']) and np.array_equal(x['degree'],y['degree'])
                for left,right in zip(x,y):
                    degree=int(left['degree'])
                    assert np.array_equal(left['neighbors'][:degree],right['neighbors'][:degree])
            overlaps.append({'nodes':[a,b],'all_shared_records_verified':len(shared)})
    dataset.assert_unchanged()
    result={'passed':True,'indexed_rows':m['rows'],'index_manifest_sha256':digest(root/'manifest.json'),
        'seconds':time.perf_counter()-start,'nodes':checks,'shared_copies':overlaps,
        'scope':'All IDs/ownership and shared-copy vectors/edge unions; sampled original vectors and RaBitQ re-encoding'}
    save(path.parent/'index_validation.json',result);print(json.dumps(result),flush=True)

if __name__=='__main__':main()
