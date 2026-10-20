from pathlib import Path
import sys
import unittest
root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(root / 'python'))
import dhdgs
import _native_wire_test
sys.modules['dhdgs._native'] = _native_wire_test
dhdgs._native = _native_wire_test
suite = unittest.defaultTestLoader.discover(str(root / 'tests'), pattern='test_native_wire.py')
with open(Path(__file__).with_name('regression.log'), 'w') as log:
    result = unittest.TextTestRunner(stream=log, verbosity=2).run(suite)
print(Path(__file__).with_name('regression.log').read_text())
sys.exit(not result.wasSuccessful())
