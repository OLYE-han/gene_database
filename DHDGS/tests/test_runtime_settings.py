import copy
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml
from dhdgs.config import load_config, validate
from dhdgs.runtime import apply_process_runtime
from dhdgs.search import Engine
from dhdgs.transport import create_server


class RuntimeSettingsTests(unittest.TestCase):
    def setUp(self):
        path=Path(__file__).resolve().parents[1]/'configs/dhdgs_pilot.yaml'
        self.raw=yaml.safe_load(path.read_text())

    def test_switch_interval_is_applied_in_seconds_and_can_be_shared(self):
        original=sys.getswitchinterval()
        try:
            self.raw['runtime']['python_switch_interval_ms']=.25
            actual=apply_process_runtime(self.raw)
            self.assertAlmostEqual(sys.getswitchinterval(),.00025)
            self.assertAlmostEqual(actual['python_switch_interval_ms'],.25)
            apply_process_runtime(copy.deepcopy(self.raw))
            self.assertAlmostEqual(sys.getswitchinterval(),.00025)
        finally:
            sys.setswitchinterval(original)

    def test_helper_rejects_invalid_values_without_changing_process(self):
        original=sys.getswitchinterval()
        for value in (0.,-1.,float('inf'),float('nan'),True,'1'):
            with self.subTest(value=value):
                self.raw['runtime']['python_switch_interval_ms']=value
                with self.assertRaises(ValueError):apply_process_runtime(self.raw)
                self.assertEqual(sys.getswitchinterval(),original)

    def test_engine_applies_settings_before_index_initialization(self):
        self.raw['search']['execution']='reference'
        events=[]
        with patch('dhdgs.runtime.apply_process_runtime',side_effect=lambda c:events.append('runtime') or {}),\
             patch('dhdgs.search.ReferenceEngine.__init__',side_effect=lambda c:events.append('index')):
            Engine(self.raw)
        self.assertEqual(events,['runtime','index'])

    def test_server_applies_settings_before_shard_and_threads(self):
        events=[]
        def shard(*args):
            events.append('shard')
            raise RuntimeError('stop before server creation')
        with patch('dhdgs.runtime.apply_process_runtime',side_effect=lambda c:events.append('runtime') or {}),\
             patch('dhdgs.transport.Shard',side_effect=shard):
            with self.assertRaisesRegex(RuntimeError,'stop before'):
                create_server(self.raw,0)
        self.assertEqual(events,['runtime','shard'])

    def test_archived_settings_migrate_and_selected_ids_round_trip(self):
        del self.raw['runtime']['python_switch_interval_ms']
        del self.raw['metrics']['trace_query_ids']
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'old.yaml'
            path.write_text(yaml.safe_dump(self.raw))
            c=load_config(path)
            self.assertEqual(c['runtime']['python_switch_interval_ms'],5.0)
            self.assertEqual(c['metrics']['trace_query_ids'],[])
            self.assertTrue(any('python_switch_interval_ms' in m for m in c['_config_migration']))
            self.assertTrue(any('trace_query_ids' in m for m in c['_config_migration']))
            self.raw['runtime']['python_switch_interval_ms']=.1
            self.raw['metrics']['trace_query_ids']=[0,734744,2**64-1]
            path.write_text(yaml.safe_dump(self.raw))
            self.assertEqual(load_config(path)['metrics']['trace_query_ids'],[0,734744,2**64-1])

    def test_schema_rejects_invalid_interval_and_trace_ids(self):
        for value in (0.,-1.,float('inf'),float('nan'),True,'1'):
            raw=copy.deepcopy(self.raw)
            raw['runtime']['python_switch_interval_ms']=value
            with self.assertRaises(ValueError):validate(raw)
        for ids in ([True],[-1],[2**64],[1.0],['1']):
            raw=copy.deepcopy(self.raw)
            raw['metrics']['trace_query_ids']=ids
            with self.assertRaisesRegex(ValueError,'trace_query_ids'):validate(raw)


if __name__=='__main__':unittest.main()
