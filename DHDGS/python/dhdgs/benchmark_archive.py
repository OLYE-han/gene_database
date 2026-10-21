"""Bounded, RAM-only capture; JSON and analysis run in an exec'd process."""
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import threading
import time

import msgpack


HEADER = struct.Struct('!QQ')
STAMP = struct.Struct('!Q')


def write_all(fd, data):
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        if n <= 0:
            raise OSError('Short archive write')
        view = view[n:]


def processor_command(directory, *args):
    return [sys.executable, '-m', 'dhdgs.benchmark_process', '--directory', str(directory), *args]


def processor_env():
    return {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1]),
            'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1'}


class MemoryArchive:
    """The limit bounds retained encoded frames, not total process RSS.

    At most one buffer exists. Overflow is drained by another program before
    reuse, providing explicit backpressure instead of an unbounded writer queue.
    A single oversized frame is streamed to that program, never put in memfd.
    """
    def __init__(self, directory, settings, context):
        self.path = Path(directory)
        self.limit = settings['memory_limit_bytes']
        self.timeout = settings['processor_timeout_seconds']
        self.context = context
        self.fd = os.memfd_create('dhdgs-query-results', os.MFD_CLOEXEC)
        self.used = 0
        self.count = 0
        self.batch = 0
        self.shards = []
        self.stats = dict(memory_limit_bytes=self.limit, peak_buffer_bytes=0, encoded_bytes=0,
                          oversized_records=0, append_wall_ms=0., overflow_wall_ms=0.,
                          overflow_batches=0, drain_events=[], storage='linux_memfd')

    def _run(self, input_fd, writer=None):
        context_path = self.path/'capture_context.json'
        if not context_path.exists():
            context_path.write_text(json.dumps(self.context))
        args = processor_command(self.path, '--capture-fd', str(input_fd), '--batch', str(self.batch))
        log = self.path/'processor.log'
        with log.open('ab') as errors:
            if writer is None:
                run = subprocess.run(args, pass_fds=(input_fd,), stdout=subprocess.PIPE, stderr=errors,
                                     env=processor_env(), timeout=self.timeout, check=True)
            else:
                # Only oversized single records use a bounded OS pipe.
                child = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                         stderr=errors, env=processor_env())
                # Bound writes as well as the final wait: a stalled reader must
                # not block the capture thread forever on a full pipe.
                timer = threading.Timer(self.timeout, child.kill)
                timer.daemon = True
                timer.start()
                try:
                    writer(child.stdin.fileno())
                    child.stdin.close()
                    child.stdin = None
                    stdout, _ = child.communicate(timeout=self.timeout)
                    if child.returncode:
                        raise subprocess.CalledProcessError(child.returncode, args)
                    run = subprocess.CompletedProcess(args, child.returncode, stdout)
                except BaseException:
                    child.kill(); child.wait()
                    raise
                finally:
                    timer.cancel()
        report = json.loads(run.stdout)
        self.shards.extend(report['shards'])
        self.batch += 1
        (self.path/'capture_manifest.json').write_text(json.dumps(dict(
            version=1, shards=self.shards, records=sum(s['records'] for s in self.shards)), indent=2))

    def flush(self, during_queries=False):
        if not self.used:
            return
        start = time.monotonic_ns()
        os.lseek(self.fd, 0, os.SEEK_SET)
        self._run(self.fd)
        os.ftruncate(self.fd, 0)
        os.lseek(self.fd, 0, os.SEEK_SET)
        end = time.monotonic_ns()
        self.stats['drain_events'].append(dict(start_ns=start, end_ns=end, bytes=self.used,
                                              during_queries=during_queries))
        if during_queries:
            self.stats['overflow_wall_ms'] += (end-start)/1e6
            self.stats['overflow_batches'] += 1
        self.used = 0

    def append(self, payload, metadata):
        start = time.monotonic_ns()
        meta = msgpack.packb(metadata, use_bin_type=True)
        frame_bytes = HEADER.size + len(meta) + len(payload) + STAMP.size
        if self.used and self.used + frame_bytes > self.limit:
            self.flush(during_queries=True)
        def write_record(fd):
            write_all(fd, HEADER.pack(len(meta), len(payload)))
            write_all(fd, meta)
            write_all(fd, payload)
            write_all(fd, STAMP.pack(time.monotonic_ns()))
        if frame_bytes > self.limit:
            self.stats['oversized_records'] += 1
            drain_start = time.monotonic_ns()
            try:
                self._run(0, write_record)
            except BaseException:
                with (self.path/f'capture-oversized-{self.batch:05d}-unprocessed.bin').open('xb') as output:
                    write_record(output.fileno())
                raise
            drain_end = time.monotonic_ns()
            self.stats['overflow_wall_ms'] += (drain_end-drain_start)/1e6
            self.stats['overflow_batches'] += 1
            self.stats['drain_events'].append(dict(start_ns=drain_start, end_ns=drain_end,
                bytes=frame_bytes, during_queries=True, oversized=True))
        else:
            write_record(self.fd)
            self.used += frame_bytes
            self.stats['peak_buffer_bytes'] = max(self.used, self.stats['peak_buffer_bytes'])
        self.count += 1
        self.stats['encoded_bytes'] += frame_bytes
        self.stats['append_wall_ms'] += (time.monotonic_ns()-start)/1e6

    def close(self):
        if self.fd < 0:
            return
        # A failed processor must not silently discard the only remaining batch.
        try:
            if self.used:
                context_path = self.path/'capture_context.json'
                if not context_path.exists():
                    context_path.write_text(json.dumps(self.context))
                os.lseek(self.fd, 0, os.SEEK_SET)
                with (self.path/'capture-unprocessed.bin').open('xb') as output:
                    while True:
                        block = os.read(self.fd, 1 << 20)
                        if not block:
                            break
                        output.write(block)
        finally:
            os.close(self.fd)
            self.fd = -1


class CaptureArchive:
    """Keep native values intact; encode only when a batch must be drained.

    Native estimates deliberately overcharge STL nodes/capacities. The 4096-byte
    per-record reserve covers Python handles, metadata and list entries. Service
    in-flight state and encoding scratch space are not this retained-data budget.
    """
    def __init__(self, directory, settings, context):
        self.raw = MemoryArchive(directory, settings, context)
        self.limit = settings['memory_limit_bytes']
        self.native = []
        self.native_bytes = 0
        self.count = 0
        self.peak = 0
        self.native_peak = 0
        self.oversized_native_records = 0
        self.capture_wall_ms = 0.
        self.overflow_wall_ms = 0.
        self.drain_events = []

    def _peak(self):
        self.peak = max(self.peak, self.native_bytes+self.raw.used)
        self.native_peak = max(self.native_peak, self.native_bytes)

    def append_native(self, record, metadata):
        start = time.monotonic_ns()
        charge = record.retained_bytes + 4096
        if self.native_bytes+self.raw.used+charge > self.limit:
            self.flush(during_queries=True)
        metadata['native_retained_ns'] = time.monotonic_ns()
        if charge > self.limit:
            # Oversized single results remain transient, never retained in the
            # bounded archive. Encode and stream/drain this record immediately.
            self.oversized_native_records += 1
            drain_start = time.monotonic_ns()
            self._encode(record, metadata)
            self.raw.flush(during_queries=True)
            drain_end = time.monotonic_ns()
            self.overflow_wall_ms += (drain_end-drain_start)/1e6
            self.drain_events.append(dict(start_ns=drain_start,end_ns=drain_end,
                                         during_queries=True,oversized=True))
        else:
            self.native.append((record, metadata, charge))
            self.native_bytes += charge
            self._peak()
        self.count += 1
        self.capture_wall_ms += (time.monotonic_ns()-start)/1e6

    def _encode(self, record, metadata):
        from .index import native
        payload, error, begin, end = native().pack_completed_records(
            [record], self.raw.context['max_message_bytes'])[0]
        if error:
            raise RuntimeError(error)
        metadata['result_encoding'] = dict(start_ns=begin,end_ns=end)
        self.raw.append(payload, metadata)

    def append(self, payload, metadata):
        # Compatibility path: older engines already produced Python objects.
        if self.native:
            self.flush(during_queries=True)
        self.raw.append(payload, metadata)
        self.count += 1
        self._peak()

    def flush(self, during_queries=False):
        if not self.native and not self.raw.used:
            return
        begin = time.monotonic_ns()
        for i in range(len(self.native)):
            entry = self.native[i]
            if entry is None:
                continue
            record, metadata, charge = entry
            # Account transformation without retaining an unbounded second copy.
            self._encode(record, metadata)
            self.native[i] = None
            self.native_bytes -= charge
            del entry, record
            self._peak()
        self.native.clear()
        self.raw.flush(during_queries=during_queries)
        end = time.monotonic_ns()
        self.drain_events.append(dict(start_ns=begin,end_ns=end,during_queries=during_queries))
        if during_queries:
            self.overflow_wall_ms += (end-begin)/1e6

    @property
    def stats(self):
        raw = dict(self.raw.stats)
        raw.update(storage='native_values_then_linux_memfd',
            peak_encoded_buffer_bytes=raw['peak_buffer_bytes'],
            peak_buffer_bytes=max(self.peak,raw['peak_buffer_bytes']),
            peak_native_accounted_bytes=self.native_peak,
            oversized_native_records=self.oversized_native_records,
            native_capture_wall_ms=self.capture_wall_ms,
            native_drain_events=self.drain_events,
            overflow_wall_ms=max(raw['overflow_wall_ms'],self.overflow_wall_ms))
        return raw

    def close(self):
        try:
            # Also try to preserve pending native records on a failed benchmark.
            if self.native:
                self.flush()
        finally:
            self.raw.close()
