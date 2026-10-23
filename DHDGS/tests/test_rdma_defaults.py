"""Deployment defaults must remain usable after deleting experiment scratch."""
import copy,sys,tempfile,unittest
from pathlib import Path
import yaml
from dhdgs.config import load_config
P=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(P/'tools'))

class DefaultProfileTests(unittest.TestCase):
    def test_default_is_accepted_rdma_and_paths_are_durable(self):
        s=yaml.safe_load((P/'configs/rdma_deployment.yaml').read_text())
        c=load_config(P/'configs'/s['runtime_config'])
        self.assertEqual(c['native_runtime']['network_module']['transport'],'rdma')
        self.assertTrue(c['native_runtime']['network_module']['async_connect_progress'])
        self.assertTrue(c['native_runtime']['network_module']['rebalance_connections'])
        self.assertEqual((c['peer']['worker_threads'],c['search']['max_expansions']),(64,200))
        self.assertEqual(s['profile']['concurrency'],1024)
        self.assertEqual(c['experiment']['benchmark_concurrency'],1024)
        for h in s['hosts']:
            for key in ('project','root','rdma_library_directory','token_file'):
                self.assertNotIn('/tmp/',h[key]);self.assertNotIn('/test/',h[key])
        self.assertEqual(len(set(h['root'] for h in s['hosts'])),3)
    def test_portable_pilot_and_archived_config_stay_portable(self):
        c=yaml.safe_load((P/'configs/dhdgs_pilot.yaml').read_text())
        self.assertFalse(c['native_runtime']['head_enabled'])
        self.assertEqual(c['native_runtime']['network_module']['transport'],'tcp')
        c.pop('native_runtime')
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'old.yaml';path.write_text(yaml.safe_dump(c))
            old=load_config(path)
            self.assertFalse(old['native_runtime']['head_enabled'])
            self.assertFalse(old['native_runtime']['network_module']['enabled'])
            self.assertEqual(old['native_runtime']['network_module']['transport'],'tcp')
