import copy
import unittest
from dhdgs.load_pressure import search_pressure


class LoadPressureTests(unittest.TestCase):
    def snapshots(self):
        before = dict(compute_completed=10, compute_wall_ns=100000000,
                      compute_ready_wait_hist_us_log2=[0]*20,
                      compute_wait_hist_us_log2=[0]*20,
                      compute_queue=dict(workers=4, queued=0, active=0))
        after = copy.deepcopy(before)
        after['compute_completed'] += 4
        after['compute_wall_ns'] += 40000000
        after['compute_ready_wait_hist_us_log2'][3] = 4
        after['compute_wait_hist_us_log2'][16] = 4
        return before, after

    def test_ready_excludes_admission_and_reports_bounds(self):
        b, a = self.snapshots()
        r = search_pressure(a, b)
        self.assertEqual(r['queue_p95_ms_bounds'], (.008, .016))
        self.assertEqual(r['queue_time_basis'], 'ready_to_worker')
        self.assertEqual(r['service_ms'], 10.)

    def test_pending_without_starts_is_pressure_not_idle(self):
        b, a = self.snapshots();a = copy.deepcopy(b)
        a['region_ready_grants'] = 40
        a['compute_queue'].update(queued=40, queued_region_executors=8)
        r = search_pressure(a, b, 10.)
        self.assertEqual(r['queue_ms'], 100.)
        self.assertEqual(r['queued_actors'], 8)
        self.assertIsNone(r['queue_p95_ms'])

    def test_legacy_and_reset_are_explicit(self):
        b, a = self.snapshots()
        del a['compute_ready_wait_hist_us_log2']
        self.assertEqual(search_pressure(a, b)['queue_time_basis'], 'accepted_to_worker_legacy')
        a['compute_completed'] = 0
        with self.assertRaises(ValueError): search_pressure(a, b)

    def test_serial_actor_wait_is_not_pool_saturation(self):
        b, a = self.snapshots()
        b['compute_executor_wait_hist_us_log2'] = [0]*20
        b['compute_actor_wait_hist_us_log2'] = [0]*20
        a['compute_executor_wait_hist_us_log2'] = [0]*20
        a['compute_actor_wait_hist_us_log2'] = [0]*20
        a['compute_executor_wait_hist_us_log2'][3] = 4
        a['compute_actor_wait_hist_us_log2'][16] = 4
        a['compute_queue'].update(queued=8, queued_region_executors=0)
        r = search_pressure(a, b)
        self.assertEqual(r['worker_pressure_ms'], .016)
        self.assertGreater(r['queue_ms'], r['worker_pressure_ms'])
