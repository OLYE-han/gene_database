import unittest

from pydantic import ValidationError

from pyramid_rabitq.api import SearchRequest, create_app


class ApiTests(unittest.TestCase):
    def test_routes_and_request_validation(self):
        app = create_app("unused", 1024, admin_token="secret", top_k_limit=10)
        paths = {route.path for route in app.routes}
        self.assertTrue(
            {"/health", "/index", "/search", "/search/binary", "/admin/reload"}.issubset(paths)
        )
        self.assertTrue(paths.isdisjoint({"/snapshots", "/mutations/upsert", "/mutations/delete", "/admin/compact-lsm"}))
        self.assertNotIn("snapshot_id", SearchRequest.model_fields)
        self.assertEqual(SearchRequest(vector=[1.0, 0.0], top_k=2).top_k, 2)
        with self.assertRaises(ValidationError):
            SearchRequest(vector=[1.0], top_k=0)


if __name__ == "__main__":
    unittest.main()
