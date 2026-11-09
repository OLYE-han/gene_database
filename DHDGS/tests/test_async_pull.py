"""Real native continuation, credit conservation and bounded tail overload."""
import copy
import threading
import time
import unittest
from pathlib import Path
import yaml
import test_hop_return as base
from dhdgs.managed_node import idle


class AsyncIdleTests(unittest.TestCase):
    def test_waiting_pull_and_source_reads_block_maintenance(self):
        for pull in ({'contexts': 1}, {'pending': 1}, {'inflight': [1, 0]},
                     {'source_read': {'active': 1}}, {'compute': {'queued': 1}}):
            self.assertFalse(idle({'pull_scheduler': pull}))
        self.assertTrue(idle({'pull_scheduler': {'contexts': 0, 'inflight': [0, 0]}}))


class AsyncPullTests(base.PullRuntimeTests):
    def configure_replica_test(self, c):
        super().configure_replica_test(c)
        options = yaml.safe_load((Path(__file__).resolve().parents[1] /
                                  'configs/dhdgs.yaml').read_text())['peer']['replica_pull']['async']
        c['peer']['replica_pull']['async'] = copy.deepcopy(options)
        c['peer']['replica_pull']['async'].update(enabled=True, tail_threads=2,
                                                 source_read_threads=2)

    def test_continuation_preserves_budget_and_changes_worker(self):
        _, target = self.install_entry()
        q = self.engine.search_async(self.x[0], 0).result(15)
        self.check_result(q, 200)
        reports = [r for r in q['branch_reports'] if r['stats'].get('pull_resume_count', 0)]
        self.assertTrue(reports)
        self.assertGreater(q['stats']['pull_resume_count'], 0)
        for r in reports:
            self.assertEqual(r['node_id'], target)
            self.assertGreaterEqual(r['compute_thread_migrations'], 1)
            self.assertEqual(r['execution']['query_parallelism'], 'one_serial_compute_thread_per_resume')
            self.assertGreaterEqual(r['stats']['pull_network_ms'], 0)
            self.assertGreaterEqual(r['stats']['pull_resume_queue_ms'], 0)
            self.assertGreaterEqual(r['stats']['pull_source_queue_ms'], 0)
            self.assertEqual(r['work_spent'] + r['work_unused'] +
                             sum(c['work_grant'] for c in r['children']), r['work_grant'])


class AsyncCapacityTests(AsyncPullTests):
    def configure_replica_test(self, c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['async'].update(max_contexts=1, max_inflight_per_source=1,
                                                 tail_threads=1, source_read_threads=1)

    def test_concurrent_requests_preserve_results_under_tail_limit(self):
        self.install_entry()
        tasks = [self.engine.search_async(self.x[0], 0) for _ in range(32)]
        for task in tasks:
            q = task.result(30)
            self.check_result(q, 200)
            # check_result audits actual expanded IDs; avoided duplicates are valid.
            self.assertTrue(q['ids'])


class AsyncPolicyHelpTests(AsyncPullTests):
    def configure_replica_test(self, c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['async'].update(exclude_pause_from_age=True,
            main_help_max_tasks=2, main_help_reserve_workers=0, main_help_queue_low=0)

    def test_continuation_preserves_budget_and_changes_worker(self):
        # A helper may be the same physical worker that ran the first slice;
        # the contract is serial continuation, not mandatory thread migration.
        self.install_entry()
        result = self.engine.search_async(self.x[0], 0).result(15)
        self.check_result(result, 200)
        self.assertGreater(result['stats']['pull_resume_count'], 0)
        self.assertEqual(result['stats'].get('pull_main_resume_count', 0) +
                         result['stats'].get('pull_tail_resume_count', 0),
                         result['stats']['pull_resume_count'])

    def test_real_help_and_pause_accounting(self):
        _, target = self.install_entry()
        runtime = self.servers[target].control.peer.runtime
        before = runtime.metrics()['pull_scheduler']
        # Serial arrivals guarantee spare main slots; each query still has
        # dependent asynchronous record reads and real native continuations.
        results = [self.engine.search_async(self.x[0], 0).result(15) for _ in range(4)]
        for result in results:
            self.check_result(result, 200)
            for b in result['branch_reports']:
                s = b['stats']
                if not s.get('pull_resume_count', 0):
                    continue
                self.assertGreater(s['pull_policy_pause_ms'], 0)
                self.assertEqual(s.get('pull_main_resume_count', 0) + s.get('pull_tail_resume_count', 0),
                                 s['pull_resume_count'])
                # Excluded time spans handoff, network, and resume queue only;
                # it must not include the continuation's later search work.
                self.assertLessEqual(s['pull_policy_pause_ms'], s['pull_wait_ms'] + .05)
        after = runtime.metrics()['pull_scheduler']
        self.assertGreater(after['main_help_resumes'], before['main_help_resumes'])
        self.assertLessEqual(after['main_help_peak_reserved'], 2)
        self.assertGreater(after['main_help_cpu_ns'], before['main_help_cpu_ns'])

    def test_helper_concurrency(self):
        AsyncCapacityTests.test_concurrent_requests_preserve_results_under_tail_limit(self)

    def test_helper_shutdown(self):
        # Reuses the real in-flight shutdown assertion rather than mocked queues.
        AsyncShutdownTests.test_close_cancels_tail_work_and_releases_contexts(self)

    def test_invalid_helper_bounds(self):
        from dhdgs.config import validate
        for values in [dict(main_help_max_tasks=-1),
                       dict(main_help_max_tasks=self.c['peer']['worker_threads'] + 1),
                       dict(main_help_reserve_workers=self.c['peer']['worker_threads']),
                       dict(main_help_queue_low=-1)]:
            c = copy.deepcopy({k:v for k,v in self.c.items() if not k.startswith('_')})
            c['peer']['replica_pull']['async'].update(values)
            with self.assertRaisesRegex(ValueError, 'main help bounds'):
                validate(c)


class AsyncRetryPrunedTests(AsyncPullTests):
    def configure_replica_test(self, c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['retry_pruned_candidates'] = True

    def test_discovery_reset_is_once_and_preserves_expansion_claims(self):
        self.install_entry()
        result = self.engine.search_async(self.x[0], 0).result(15)
        self.check_result(result, 200)
        reports = [b for b in result['branch_reports'] if b['stats'].get('pull_batches', 0)]
        self.assertTrue(reports)
        self.assertTrue(all(b['stats']['pull_seen_resets'] == 1 for b in reports))


class AsyncFailureTests(AsyncPullTests):
    def configure_replica_test(self, c):
        super().configure_replica_test(c)
        c['peer']['replica_pull']['batch_bytes'] = 1

    # Override successful-pull inherited checks for this failure-only fixture.
    def test_pull_computes_on_replica_preserves_credit_and_dependencies(self):
        base.PullFailureTests.test_rejected_read_does_not_spend_credit_or_drop_candidates(self)

    def test_continuation_preserves_budget_and_changes_worker(self):
        self.install_entry()
        q = self.engine.search_async(self.x[0], 0).result(15)
        self.check_result(q, 200)
        self.assertGreater(q['stats']['pull_resume_count'], 0)
        self.assertEqual(q['stats']['pull_expanded'], 0)
        self.assertGreater(q['stats']['pull_fallbacks'], 0)


class AsyncShutdownTests(AsyncPullTests):
    def test_close_cancels_tail_work_and_releases_contexts(self):
        _, target = self.install_entry()
        tasks = [self.engine.search_async(self.x[0], 0) for _ in range(128)]
        runtime = self.servers[target].control.peer.runtime
        deadline = time.monotonic() + 5
        observed = None
        while time.monotonic() < deadline:
            observed = runtime.metrics()['pull_scheduler']
            if observed['contexts']:
                break
            time.sleep(.001)
        self.assertGreater(observed['contexts'], 0, 'Fixture must close during a tail execution')
        closer = threading.Thread(target=runtime.close, daemon=True)
        closer.start()
        closer.join(10)
        self.assertFalse(closer.is_alive(), 'Close stranded a paused continuation')
        scheduler = runtime.metrics()['pull_scheduler']
        self.assertEqual(scheduler['contexts'], 0)
        self.assertEqual(scheduler['pending'], 0)
        self.assertEqual(sum(scheduler['inflight']), 0)
        self.assertEqual(scheduler['compute']['active'], 0)
        self.assertEqual(scheduler['compute']['queued'], 0)
        # Cancellations may fail individual queries; they must all terminate.
        self.engine.close()
        self.assertTrue(all(task.done() for task in tasks))


class AsyncRoleTests(unittest.TestCase):
    setUpClass = classmethod(base.PullRuntimeTests.setUpClass.__func__)
    tearDownClass = classmethod(base.PullRuntimeTests.tearDownClass.__func__)
    setUp = base.PullRuntimeTests.setUp
    rpc = base.PullRuntimeTests.rpc

    def configure_replica_test(self, c):
        import os
        base.PullRuntimeTests.configure_replica_test(self, c)
        c['peer']['replica_pull']['async'] = copy.deepcopy(yaml.safe_load((Path(__file__).resolve().parents[1] / 'configs/dhdgs.yaml').read_text())['peer']['replica_pull']['async'])
        c['peer']['replica_pull']['async'].update(enabled=True, tail_threads=2, source_read_threads=2, target_nodes=[1], source_nodes=[0])
        c['native_runtime']['tail_cpu_ids'] = sorted(os.sched_getaffinity(0))[-2:]

    def test_roles_affinity_and_real_tail_search(self):
        for i, server in enumerate(self.servers):
            p = server.control.peer.runtime.metrics()['pull_scheduler']
            self.assertEqual(p['main_workers'], self.c['peer']['worker_threads'])
            self.assertEqual(p['tail_workers'], 2 if i == 1 else 0)
            self.assertEqual(p['source_read_workers'], 2 if i == 0 else 0)
            self.assertEqual(p['enabled'], i == 1)
            self.assertEqual(p['tail_cpu_ids'], self.c['native_runtime']['tail_cpu_ids'] if i == 1 else [])
        i, info = next((i, r) for i, x in enumerate(self.x)
                      if (r := self.engine.runtime.route_info(x.astype(float), i))['source'] == 0)
        self.rpc(0, 'source_start', id='role_test', plan=dict(ids=[info['entry_id']],
                      entries=[info['entry_id']], target=1))
        until = time.monotonic() + 15
        while time.monotonic() < until:
            if self.rpc(0, 'status', id='role_test')['phase'] == 'SENT' and self.rpc(1, 'status', id='role_test')['phase'] == 'READY':
                break
            time.sleep(.01)
        else:
            self.fail('Role fixture not ready')
        self.engine.runtime.replica_routes([dict(source=0, target=1, version='role_test',
            entries=[info['entry_id']], fraction=1., min_score=-1e300, max_score=1e300, ttl_ms=10000)])
        q = self.engine.search_async(self.x[i], i).result(15)
        base.PullRuntimeTests.check_result(self, q, 200)
        self.assertGreater(q['stats']['pull_resume_count'], 0)
        self.assertGreater(q['stats']['pull_expanded'], 0)

    def test_invalid_roles_cpus_and_node_local_signature(self):
        from dhdgs.config import validate
        from dhdgs.peer import execution_signature
        c = {k: v for k, v in self.c.items() if not k.startswith('_')}
        for key, values in [('target_nodes', [3]), ('source_nodes', [-1]),
                            ('target_nodes', [1, 1]), ('source_nodes', [True])]:
            invalid = copy.deepcopy(c)
            invalid['peer']['replica_pull']['async'][key] = values
            with self.assertRaisesRegex(ValueError, 'pull node list'):
                validate(invalid)
        for values in [[-1], [1024], [1, 1], [True]]:
            invalid = copy.deepcopy(c)
            invalid['native_runtime']['tail_cpu_ids'] = values
            with self.assertRaisesRegex(ValueError, 'CPU list'):
                validate(invalid)
        changed = copy.deepcopy(c)
        changed['native_runtime']['tail_cpu_ids'] = []
        self.assertEqual(execution_signature(c), execution_signature(changed))
