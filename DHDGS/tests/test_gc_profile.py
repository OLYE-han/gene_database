import gc
import unittest
from dhdgs.gc_profile import GCProfile


class GCProfileTests(unittest.TestCase):
    def test_gc_events_are_bounded_and_do_not_disable_collection(self):
        before = gc.isenabled()
        profile = GCProfile(2)
        try:
            for _ in range(3):
                cycle = []
                cycle.append(cycle)
                del cycle
                gc.collect(2)
            metrics = profile.snapshot()
            self.assertGreaterEqual(metrics['count'], 3)
            self.assertEqual(len(metrics['events']), 2)
            for e in metrics['events']:
                self.assertLessEqual(e['start_ns'], e['end_ns'])
                self.assertGreaterEqual(e['wall_ms'], 0)
            self.assertEqual(gc.isenabled(), before)
        finally:
            profile.close()
        self.assertNotIn(profile.callback, gc.callbacks)
