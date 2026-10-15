from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "third_party" / "VecBench-paper"))

from benchmarks.vecbench_adapter import PyramidVecBench


class VecBenchAdapterTest(unittest.TestCase):
    def config(self):
        return {"pyramid": {"endpoint": "http://127.0.0.1:1234", "dimension": 3}}

    @patch("benchmarks.vecbench_adapter.http.client.HTTPConnection")
    def test_query_and_recall(self, connection_class):
        connection = MagicMock()
        health = MagicMock(status=200)
        health.read.return_value = json.dumps({"dimension": 3}).encode()
        result = MagicMock(status=200)
        result.read.return_value = json.dumps({
            "hits": [{"id": 10}, {"id": 20}],
        }).encode()
        connection.getresponse.side_effect = [health, result]
        connection_class.return_value = connection

        db = PyramidVecBench(self.config())
        db.connect()
        ids, elapsed = db.query({
            "query_vector": [1.0, 2.0, 3.0],
            "scalar_filters": [],
            "limit": 2,
        }, "ignored")
        self.assertEqual(ids, [10, 20])
        self.assertGreaterEqual(elapsed, 0)
        self.assertEqual(db.cal_recall(ids, [10, 30]), 0.5)
        body = connection.request.call_args_list[1].kwargs["body"]
        np.testing.assert_array_equal(np.frombuffer(body, dtype="<f4"), [1, 2, 3])

if __name__ == "__main__":
    unittest.main()
