"""Offline journal grouping: child evidence must not replace a round summary."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


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
        self.assertIn('threads: 32, 64', content)
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
        self.assertIn(other['updated_at'], content)

    def test_invalid_parent_is_rejected(self):
        with self.assertRaises(ValueError):
            journal.record(self.metadata, self.log, round_id='../invalid')
        self.assertFalse(self.log.exists())


if __name__ == '__main__':
    unittest.main()
