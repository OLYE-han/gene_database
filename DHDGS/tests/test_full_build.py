import tempfile
from pathlib import Path
import unittest
import numpy as np
from threadpoolctl import threadpool_limits
from test_system import fixture
from dhdgs.index import build,native
from dhdgs.config import load_config
import yaml

class FullBuildTests(unittest.TestCase):
    def test_parallel_graph_checkpoints_and_full_connectivity(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);c,x=fixture(root);events=[]
            with threadpool_limits(limits=1):
                first=build(c,shard_workers=3,graph_threads=4,checkpoint_directory=root/'checkpoints',progress=events.append)
                old=Path(c['paths']['output'])/'index'
                raw=yaml.safe_load(Path(c['_config_path']).read_text()) if '_config_path' in c else {k:v for k,v in c.items() if not k.startswith('_')}
                raw['paths']['output']=str(root/'second')
                cfg=root/'second.yaml';cfg.write_text(yaml.safe_dump(raw));c2=load_config(cfg)
                second=build(c2,shard_workers=3,graph_threads=4,checkpoint_directory=root/'checkpoints',progress=events.append)
            self.assertTrue(first['strong_connectivity_verified'])
            self.assertEqual(first['partition'],second['partition'])
            self.assertEqual(sum(e['phase']=='graph_checkpoint_loaded' for e in events),3)
            for sid in range(3):
                self.assertEqual((old/f'shard_{sid}/nodes.bin').read_bytes(),(root/f'second/index/shard_{sid}/nodes.bin').read_bytes())

    def test_parallel_native_callback_error_propagates(self):
        x=np.random.default_rng(3).normal(size=(128,64)).astype('f4')
        def fail(*args):raise RuntimeError('progress failed')
        with self.assertRaisesRegex(RuntimeError,'progress failed'):
            native().build_vamana(x,8,16,[1.,1.2],42,fail,4)

    def test_disk_reserve_rejects_before_graph(self):
        with tempfile.TemporaryDirectory() as temp:
            c,_=fixture(Path(temp))
            with self.assertRaisesRegex(ValueError,'space for complete index'):
                build(c,reserve_disk_bytes=2**62)
