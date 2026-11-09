"""Offline journal grouping: child evidence must not replace a round summary."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor


spec = importlib.util.spec_from_file_location(
    'experiment_log', Path(__file__).resolve().parents[1] / 'tools/experiment_log.py')
journal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(journal)


class ExperimentLogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.log = self.root / 'TestLog.md'
        self.metadata = dict(
            id='study', title='Study', purpose='One problem', hypothesis='Hypothesis',
            expected='Expected', configuration='Fixed', variables=['threads: 32, 64'],
            code_changes='Logger only', results='Pending', artifacts=['report'],
            next_steps='Analyze', status='运行中', revision='fixture')

    def test_children_cannot_publish_or_overwrite_round(self):
        parent = journal.record(self.metadata, self.log)
        original = self.log.read_bytes()
        for name in ('a', 'b', 'a_retry'):
            child = journal.record(dict(self.metadata, id='study_' + name),
                                   self.log, round_id='study')
            self.assertEqual(child['round_id'], 'study')
            self.assertEqual(child['id'], 'study_' + name)
        self.assertEqual(self.log.read_bytes(), original)
        parent.update(status='已完成', results='Results of all configurations')
        journal.record(parent, self.log)
        journal.record(parent, self.log)
        content = self.log.read_text()
        self.assertEqual(content.count('<!-- experiment:study:start -->'), 1)
        self.assertNotIn('threads: 32, 64', content)
        detail = self.root / 'test/log_archive/rounds/study.md'
        self.assertIn('threads: 32, 64', detail.read_text())
        self.assertIn('Results of all configurations', content)

    def test_success_failure_and_reanalysis_keep_local_evidence(self):
        for name, fail in (('good', False), ('bad', True)):
            evidence = self.root / name / 'experiment_log.json'
            child = dict(self.metadata, id=name, round_id='study')
            try:
                with journal.experiment(child, evidence, self.log) as state:
                    if fail:
                        raise RuntimeError('controlled failure')
                    state['results'] = 'Measured'
            except RuntimeError:
                self.assertTrue(fail)
            saved = json.loads(evidence.read_text())
            self.assertEqual(saved['status'], '失败' if fail else '测量结束，待分析')
            self.assertNotEqual(saved['ended_at'], '尚未结束')
            self.assertIn('+08:00', saved['started_at'])
            if fail:
                self.assertIn('controlled failure', saved['results'])
            updated = journal.record(saved, self.log)
            self.assertEqual(updated['started_at'], saved['started_at'])
            self.assertEqual(updated['ended_at'], saved['ended_at'])
        self.assertFalse(self.log.exists())

    def test_round_lifecycle_and_unrelated_round_are_preserved(self):
        other = journal.record(dict(self.metadata, id='another_problem'), self.log)
        with journal.experiment(self.metadata, self.root / 'round.json', self.log):
            pass
        saved = json.loads((self.root / 'round.json').read_text())
        self.assertEqual(saved['round_id'], 'study')
        self.assertEqual(saved['status'], '测量结束，待分析')
        content = self.log.read_text()
        self.assertEqual(content.count('<!-- experiment:'), 4)
        detail = self.root / 'test/log_archive/rounds/another_problem.json'
        self.assertEqual(json.loads(detail.read_text())['updated_at'], other['updated_at'])

    def test_group_order_report_links_and_updates(self):
        for ident, group, date in [('b', 'Network', '2026-11-02T10:00:00+08:00'),
                                   ('a', 'Network', '2026-10-23T10:00:00+08:00'),
                                   ('c', 'Search', '2026-10-24T10:00:00+08:00')]:
            journal.record(dict(self.metadata, id=ident, log_group=group,
                                started_at=date, summary='Measured; limitation retained',
                                report=f'test/{ident}/REPORT.md'), self.log)
        content = self.log.read_text()
        self.assertLess(content.index('experiment:a:start'), content.index('experiment:b:start'))
        self.assertLess(content.index('experiment:b:start'), content.index('experiment:c:start'))
        self.assertIn('[报告](test/a/REPORT.md)', content)
        data = json.loads((self.root / 'test/log_archive/rounds/a.json').read_text())
        data.update(status='已完成', summary='Final result')
        journal.record(data, self.log)
        self.assertEqual(self.log.read_text().count('experiment:a:start'), 1)
        self.assertIn('Final result', self.log.read_text())
        self.assertIn('id="study', self.log.read_text())  # Original title anchor retained.

    def test_concurrent_round_updates_preserve_all_records(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda i: journal.record(dict(self.metadata, id=f'round_{i}'),
                                                  self.log), range(24)))
        entries = journal.compact.parse(self.log.read_text())
        self.assertEqual(len(entries), 24)
        self.assertEqual(len(list((self.root / 'test/log_archive/rounds').glob('*.json'))), 24)

    def test_relative_evidence_links_remain_valid_in_detail(self):
        journal.record(dict(self.metadata, artifacts=['[报告](test/study/REPORT.md)']), self.log)
        detail = (self.root / 'test/log_archive/rounds/study.md').read_text()
        self.assertIn('[报告](../../study/REPORT.md)', detail)
        self.assertIn('[报告](test/study/REPORT.md)', self.log.read_text())

    def test_writer_imported_before_migration_keeps_new_results(self):
        journal.record(dict(self.metadata, log_group='Network'), self.log)
        original = self.log.read_text()
        detail = self.root / 'test/log_archive/rounds/study.md'
        legacy = detail.read_text().replace('**结果与限制**：Pending',
                                          '**结果与限制**：Latest legacy result')
        # Emulate an old, already running process replacing only its own block.
        block = journal.compact.BLOCK.search(original)
        self.log.write_text(original[:block.start()] + legacy + original[block.end():])
        journal.record(dict(self.metadata, id='other', log_group='Search'), self.log)
        entries = journal.compact.parse(self.log.read_text())
        old = next(e for e in entries if e['id'] == 'study')
        self.assertEqual(old['group'], 'Network')
        self.assertIn('Latest legacy result', old['summary'])
        self.assertIn('Latest legacy result', detail.read_text())
        self.assertEqual(len(entries), 2)

    def test_damaged_index_is_not_silently_truncated(self):
        journal.record(self.metadata, self.log)
        damaged = self.log.read_text().replace('<!-- experiment:study:end -->', '')
        self.log.write_text(damaged)
        with self.assertRaises(ValueError):
            journal.record(dict(self.metadata, id='other'), self.log)
        self.assertEqual(self.log.read_text(), damaged)

    def test_invalid_parent_is_rejected(self):
        with self.assertRaises(ValueError):
            journal.record(self.metadata, self.log, round_id='../invalid')
        self.assertFalse(self.log.exists())


if __name__ == '__main__':
    unittest.main()
