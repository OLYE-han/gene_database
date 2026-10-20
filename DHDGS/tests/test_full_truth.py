import json
from pathlib import Path
import sys
import tempfile
import unittest
import numpy as np
import yaml
from test_system import fixture
from dhdgs.data import Dataset
from dhdgs.benchmark import exact_topk,query_input,run_benchmark
from dhdgs.index import build
from threadpoolctl import threadpool_limits

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from full_truth import FullTruth,validate_truth

class FullTruthTests(unittest.TestCase):
    def test_explicit_query_rows_keep_actual_ids_and_report_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            c,x=fixture(Path(temp));selection=[2,0,1]
            queries,ids,_=query_input(c,3,rows=selection)
            self.assertEqual(ids.tolist(),[70,0,35]);np.testing.assert_array_equal(queries,x[[70,0,35]])
            with self.assertRaisesRegex(ValueError,'explicit query'):
                query_input(c,3,rows=[0,0,1])
            with threadpool_limits(limits=1):
                build(c);report=run_benchmark(c,query_rows=selection)
            self.assertEqual(report['recall_at_k'],1.)
            for q in report['queries']:
                self.assertEqual(q['query_row'],selection[q['job']%3])
                self.assertEqual(q['query_id'],[70,0,35][q['job']%3])

    def test_full_truth_validates_and_rejects_subset_or_changed_query(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);c,x=fixture(root);dataset=Dataset(root/'data')
            qids=np.array([0,35,70],dtype='u8');all_ids=np.arange(96,dtype='u8')
            rows=[exact_topk(dataset,all_ids,x[int(q)],5,'l2',int(q),17) for q in qids]
            truth_file=root/'truth.npz'
            np.savez(truth_file,query_start=0,query_end=3,metric='euclidean',query_ids=qids,
                     ids=np.array([r[0] for r in rows]),distances=np.sqrt([r[1] for r in rows]))
            c['query']['row']=1;c['experiment']['benchmark_queries']=2
            plan={'dhdgs':{k:v for k,v in c.items() if not k.startswith('_')},'full_run':{
                'ground_truth_path':str(truth_file),'ground_truth_threads':1,'ground_truth_validation_queries':4,
                'calibration_query_row':0,'calibration_queries':1,'validation_queries':2,'query_selection_seed':17}}
            path=root/'plan.yaml';path.write_text(yaml.safe_dump(plan));validate_truth(path)
            provider=FullTruth(path)
            self.assertEqual(provider(dataset,all_ids,x[35],5,'l2',35,17)[0],rows[1][0])
            with self.assertRaisesRegex(ValueError,'subset'):
                provider(dataset,all_ids[:-1],x[35],5,'l2',35,17)
            with self.assertRaisesRegex(ValueError,'Query vector differs'):
                provider(dataset,all_ids,x[35]+1,5,'l2',35,17)
            with self.assertRaisesRegex(ValueError,'incompatible'):
                provider(dataset,all_ids,x[35],5,'cosine',35,17)
