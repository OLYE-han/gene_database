import copy
from pathlib import Path
import unittest
import yaml
from dhdgs.config import validate


class TransferGraceConfigTests(unittest.TestCase):
    def setUp(self):
        self.config = yaml.safe_load((Path(__file__).resolve().parents[1] /
                                     'configs/dhdgs.yaml').read_text())

    def test_disabled_enabled_and_legacy_settings(self):
        for value in (None, 0, 1, 5000):
            with self.subTest(grace_ms=value):
                c = copy.deepcopy(self.config)
                if value is None:
                    c['peer'].pop('transfer_failure_grace_ms', None)
                else:
                    c['peer']['transfer_failure_grace_ms'] = value
                validate(c)

    def test_recovery_window_cannot_be_negative_or_exceed_query_deadline(self):
        for value in (-1, int(self.config['peer']['query_timeout_seconds']*1000)+1):
            with self.subTest(grace_ms=value):
                c = copy.deepcopy(self.config)
                c['peer']['transfer_failure_grace_ms'] = value
                with self.assertRaisesRegex(ValueError, 'transfer_failure_grace_ms'):
                    validate(c)
