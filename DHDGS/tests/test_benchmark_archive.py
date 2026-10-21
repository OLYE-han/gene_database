"""Bounded RAM capture, overflow JSON and timestamp preservation."""
import json
import os
from pathlib import Path
import tempfile
import time
import unittest

import msgpack

from dhdgs.benchmark_archive import MemoryArchive, HEADER
from dhdgs.benchmark_process import JsonRecords


class ArchiveTests(unittest.TestCase):
    def setup_archive(self, limit=65536, shard=1024):
        temp = tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        path = Path(temp.name)
        settings = dict(memory_limit_bytes=limit,json_shard_bytes=shard,processor_timeout_seconds=30)
        now = time.monotonic_ns()
        context = dict(settings=settings,selection=[0,1,2],query_ids=[100,101,102],
            truth=[[1,2],[1,2],[1,2]],config_revision='test',offered_ns=now,max_message_bytes=1<<20)
        archive = MemoryArchive(path,settings,context);self.addCleanup(archive.close)
        meta = dict(job=0,start_ns=now+1,service_done_ns=now+100,observed_ns=now+1100,
            native_capture=False,api_offered_ns=now+1,api_delivered_ns=now+90)
        result = dict(status='ok',ids=[1,3],request_id='r0',stats=dict(expanded=200,metrics_complete=True,
            head_timestamps=dict(native_ready_ns=now+80)),branch_reports=[dict(timestamp=now+65)])
        return path,archive,meta,result

    def test_ram_only_until_flush_and_separate_processor(self):
        path,a,meta,result = self.setup_archive()
        a.append(msgpack.packb(result),meta)
        self.assertFalse(list(path.glob('queries*.json')))
        self.assertFalse((path/'capture_context.json').exists())
        self.assertGreater(a.used,0)
        self.assertLessEqual(a.stats['peak_buffer_bytes'],a.limit)
        a.flush()
        rows = list(JsonRecords(path));self.assertEqual(len(rows),1)
        q = rows[0]
        self.assertEqual(q['branch_reports'],result['branch_reports'])
        self.assertEqual(q['stats'],result['stats'])
        self.assertEqual(q['recall'],.5)
        self.assertEqual(q['client_end_ns'],meta['service_done_ns'])
        self.assertEqual(q['benchmark_observed_end_ns'],meta['observed_ns'])
        self.assertAlmostEqual(q['benchmark_consumer_wait_ms'],.001)
        self.assertNotEqual(a.shards[0]['processor_pid'],os.getpid())
        self.assertEqual(a.stats['overflow_batches'],0)
        self.assertEqual(a.used,0)

    def test_overflow_is_batched_and_lossless(self):
        path,a,meta,result = self.setup_archive(limit=700,shard=400)
        for i in range(3):
            a.append(msgpack.packb({**result,'request_id':str(i)}),{**meta,'job':i})
        a.flush()
        self.assertEqual([q['job'] for q in JsonRecords(path)],[0,1,2])
        self.assertGreater(a.stats['overflow_batches'],0)
        self.assertLessEqual(a.stats['peak_buffer_bytes'],700)
        self.assertTrue(all(json.loads((path/s['file']).read_text()) for s in a.shards))

    def test_oversized_record_streams_without_growing_ram_buffer(self):
        path,a,meta,result = self.setup_archive(limit=32)
        a.append(msgpack.packb(result),meta)
        self.assertEqual(a.used,0)
        self.assertEqual(a.stats['peak_buffer_bytes'],0)
        self.assertEqual(a.stats['oversized_records'],1)
        self.assertEqual(len(list(JsonRecords(path))),1)

    def test_failed_processor_preserves_raw_batch(self):
        path,a,meta,result = self.setup_archive()
        a.append(msgpack.packb(result),meta)
        a.context['max_message_bytes']=1
        with self.assertRaises(Exception):a.flush()
        a.close()
        self.assertGreater((path/'capture-unprocessed.bin').stat().st_size,HEADER.size)
        self.assertFalse((path/'capture_manifest.json').exists())
