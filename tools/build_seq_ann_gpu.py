#!/usr/bin/env python3
"""Out-of-core exhaustive L2/cosine kNN on one GPU; see build_seq_ann_gpu.md."""
import argparse
import json
import os
from pathlib import Path
import time
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed


def atomic_json(path, value):
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def validate_array(path, rows, dimension):
    import numpy as np
    x = np.load(path, mmap_mode='r')
    if x.shape != (rows, dimension) or x.dtype != np.float32 or not x.flags.c_contiguous:
        raise ValueError(f'Invalid array shape/dtype/layout: {path}')
    if path.stat().st_size != x.offset + rows * dimension * 4:
        raise ValueError(f'Invalid array file length: {path}')
    return x


def build_part(task):
    """One process exclusively owns one output file and its checkpoint."""
    import h5py
    import numpy as np
    part, shards, key, output, dimension, copy_rows = task
    output = Path(output)
    target = output / part['file']
    partial = target.with_suffix('.partial.npy')
    checkpoint = target.with_suffix('.progress.json')
    count = part['end'] - part['start']
    if target.exists():
        validate_array(target, count, dimension)
        atomic_json(checkpoint, dict(rows_done=count, complete=True))
        return part['id']
    saved = json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    done = saved.get('rows_done', 0)
    if not 0 <= done <= count or (done and not partial.exists()):
        raise ValueError(f'Invalid part checkpoint: {checkpoint}')
    if partial.exists():
        array = validate_array(partial, count, dimension)
    else:
        array = np.lib.format.open_memmap(partial, mode='w+', dtype=np.float32,
                                         shape=(count, dimension))
        array.flush()
    data_offset = array.offset
    del array
    began = time.monotonic()
    initial = done
    fd = os.open(partial, os.O_WRONLY)
    try:
        buffer = np.empty((min(copy_rows, count), dimension), dtype=np.float32)
        for shard in shards:
            lo = max(part['start'] + done, shard['start'])
            hi = min(part['end'], shard['start'] + shard['rows'])
            if lo >= hi:
                continue
            st = Path(shard['path']).stat()
            if (st.st_size, st.st_mtime_ns) != (shard['size'], shard['mtime_ns']):
                raise ValueError(f'Source changed: {shard["path"]}')
            with h5py.File(shard['path'], 'r') as f:
                source = f[key]
                for start in range(lo, hi, copy_rows):
                    end = min(start + copy_rows, hi)
                    values = buffer[:end - start]
                    source.read_direct(values, source_sel=np.s_[start-shard['start']:end-shard['start'], :])
                    if not np.isfinite(values).all() or np.any(np.max(np.abs(values), axis=1) == 0):
                        raise ValueError(f'Nonfinite/zero vector in {shard["path"]}, global rows {start}:{end}')
                    data = memoryview(values).cast('B')
                    position = data_offset + (start - part['start']) * dimension * 4
                    while data:
                        written = os.pwrite(fd, data, position)
                        if written <= 0:
                            raise OSError('Short write')
                        position += written
                        data = data[written:]
            st = Path(shard['path']).stat()
            if (st.st_size, st.st_mtime_ns) != (shard['size'], shard['mtime_ns']):
                raise ValueError(f'Source changed during read: {shard["path"]}')
            # Only sync this worker's file, once per source-file intersection.
            os.fsync(fd)
            done = hi - part['start']
            atomic_json(checkpoint, dict(rows_done=done, complete=False))
            rate = (done - initial) * dimension * 4 / max(time.monotonic()-began, 1e-6) / 1e6
            print(f'part {part["id"]:02d}: {done:,}/{count:,} rows, {rate:.1f} MB/s', flush=True)
        if done != count:
            raise ValueError(f'Incomplete source coverage for part {part["id"]}')
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(partial, target)
    atomic_json(checkpoint, dict(rows_done=count, complete=True))
    return part['id']


def prepare(args):
    # Each worker does I/O and elementwise validation, not multithreaded BLAS.
    os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
    os.environ.setdefault('OMP_NUM_THREADS', '1')
    import h5py
    source = args.source.resolve()
    paths = sorted(source.glob('shard_*.h5'), key=lambda p: int(p.stem.split('_')[-1]))
    shards, total, dim = [], 0, None
    for path in paths:
        with h5py.File(path, 'r') as f:
            x = f[args.key]
            if x.ndim != 2 or x.dtype.kind != 'f' or x.dtype.itemsize > 4:
                raise ValueError(f'{path}: expected float16/float32 matrix, got {x.shape}, {x.dtype}')
            if dim is not None and dim != x.shape[1]:
                raise ValueError(f'{path}: inconsistent dimension')
            dim = x.shape[1]
            count = min(len(x), args.rows - total)
            st = path.stat()
            shards.append(dict(path=str(path), rows=count, source_rows=len(x),
                               start=total, size=st.st_size, mtime_ns=st.st_mtime_ns))
            total += count
        if total == args.rows:
            break
    if total != args.rows:
        raise ValueError(f'Only {total} rows available, requested {args.rows}')
    if args.parts > total:
        raise ValueError('--parts must not exceed --rows')
    parts = [dict(id=i, start=total*i//args.parts, end=total*(i+1)//args.parts,
                  file=f'base/part_{i:02d}.npy') for i in range(args.parts)]
    manifest = dict(version=2, rows=total, dimension=dim, key=args.key, shards=shards,
                    metric='cosine', self_excluded=True, order='numeric shard ID, then row order',
                    base_dtype='float32', parts=parts)
    mp = args.output / 'manifest.json'
    if mp.exists() and json.loads(mp.read_text()) != manifest:
        raise ValueError('Source/layout changed (including legacy base.npy); use a new output directory')
    atomic_json(mp, manifest)
    (args.output / 'base').mkdir(exist_ok=True)
    completed = set()
    for part in parts:
        path = args.output / part['file']
        if path.exists():
            validate_array(path, part['end']-part['start'], dim)
            completed.add(part['id'])

    def checkpoint():
        rows = sum(p['end']-p['start'] for p in parts if p['id'] in completed)
        atomic_json(args.output / 'prepare_progress.json',
                    dict(rows_completed=rows, completed_parts=sorted(completed),
                         complete=len(completed) == len(parts)))

    checkpoint()
    tasks = [(p, shards, args.key, str(args.output), dim, args.copy_rows)
             for p in parts if p['id'] not in completed]
    if not tasks:
        print('All base parts already complete', flush=True)
        return
    if args.workers == 1:
        for task in tasks:
            completed.add(build_part(task))
            checkpoint()
        return
    with ProcessPoolExecutor(max_workers=min(args.workers, len(tasks)),
                             mp_context=multiprocessing.get_context('spawn')) as pool:
        futures = [pool.submit(build_part, task) for task in tasks]
        try:
            for future in as_completed(futures):
                completed.add(future.result())
                checkpoint()
        finally:
            for future in futures:
                future.cancel()


class ShardedBase:
    """Read-only logical matrix; global IDs never depend on file-local row numbers."""
    def __init__(self, output, manifest, metadata_only=False):
        self.metadata_only = metadata_only
        self.shape = (manifest['rows'], manifest['dimension'])
        # Legacy datasets remain searchable, but prepare never overwrites them.
        self.parts = manifest.get('parts', [dict(id=0, start=0, end=self.shape[0], file='base.npy')])
        self.arrays = []
        end = 0
        for part in self.parts:
            if part['start'] != end or part['end'] <= end:
                raise ValueError('Noncontiguous base parts')
            if not metadata_only:
                self.arrays.append(validate_array(output / part['file'], part['end']-part['start'], self.shape[1]))
            end = part['end']
        if end != self.shape[0]:
            raise ValueError('Incomplete base parts')

    def __len__(self):
        return self.shape[0]

    def __getitem__(self, key):
        import numpy as np
        if self.metadata_only:
            raise RuntimeError('Metadata-only base cannot read vectors')
        if not isinstance(key, slice) or key.step not in (None, 1):
            raise ValueError('Only contiguous row slices supported')
        start, stop, _ = key.indices(len(self))
        pieces = [a[max(start,p['start'])-p['start']:min(stop,p['end'])-p['start']]
                  for p,a in zip(self.parts,self.arrays) if start < p['end'] and stop > p['start']]
        if not pieces:
            return np.empty((0,self.shape[1]), dtype=np.float32)
        return pieces[0] if len(pieces) == 1 else np.concatenate(pieces)

    def block_ranges(self, rows):
        for part in self.parts:
            for start in range(part['start'], part['end'], rows):
                yield start, min(start + rows, part['end'])

    def gather(self, ids):
        import numpy as np
        if self.metadata_only:
            raise RuntimeError('Metadata-only base cannot read vectors')
        ids = np.asarray(ids, dtype=np.int64)
        if ids.ndim != 1 or np.any(ids < 0) or np.any(ids >= len(self)):
            raise ValueError('Invalid global query IDs')
        result = np.empty((len(ids), self.shape[1]), dtype=np.float32)
        for part, array in zip(self.parts, self.arrays):
            positions = np.flatnonzero((ids >= part['start']) & (ids < part['end']))
            if len(positions):
                result[positions] = array[ids[positions] - part['start']]
        return result


def database_blocks(base, rows, prefetch=True):
    """Bounded read-ahead: at most one upcoming host block, never whole-file loading."""
    import numpy as np
    ranges = (base.block_ranges(rows) if isinstance(base, ShardedBase) else
              ((s, min(s+rows,len(base))) for s in range(0,len(base),rows)))
    def read(bounds):
        start, end = bounds
        return start, np.array(base[start:end], dtype=np.float32, copy=True)
    if not prefetch:
        for bounds in ranges:
            yield read(bounds)
        return
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = next(ranges, None)
        future = pool.submit(read, first) if first is not None else None
        while future is not None:
            block = future.result()
            bounds = next(ranges, None)
            future = pool.submit(read, bounds) if bounds is not None else None
            yield block


def configure_torch():
    import torch
    torch.set_grad_enabled(False)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.set_float32_matmul_precision('highest')
    return torch


def normalized(values, device, dtype):
    import numpy as np
    import torch
    # FP64 normalization avoids overflow and underflow for finite float32 inputs.
    x = torch.from_numpy(np.array(values, copy=True)).to(device=device, dtype=torch.float64)
    x /= torch.linalg.vector_norm(x, dim=1, keepdim=True)
    return x.to(dtype=dtype)


def scan(base, qs, qe, k, db_rows, tile_rows, device, dtype, log=False, prefetch=True,
         query_ids=None, query_values=None, metric='cosine'):
    import numpy as np
    import torch
    if metric not in ('cosine', 'euclidean'):
        raise ValueError(f'Unsupported metric: {metric}')
    if query_ids is not None:
        query_ids = np.asarray(query_ids, dtype=np.int64)
        if query_ids.shape != (qe-qs,):
            raise ValueError('Query ID count does not match query batch')
        values = query_values
        if values is None:
            values = base.gather(query_ids) if isinstance(base, ShardedBase) else base[query_ids]
    else:
        values = base[qs:qe]
    if values.shape != (qe-qs, base.shape[1]):
        raise ValueError('Query matrix shape mismatch')
    if log:
        print(f'Loading/uploading {qe-qs:,} query vectors for {metric} on {device}', flush=True)
    def upload(values):
        if metric == 'cosine':
            return normalized(values, device, dtype)
        return torch.from_numpy(np.array(values, copy=True)).to(device=device, dtype=dtype)
    q = upload(values)
    qnorm = (q*q).sum(dim=1) if metric == 'euclidean' else None
    if log:
        if str(device).startswith('cuda'):
            torch.cuda.synchronize()
        print(f'All {len(q):,} queries in this batch are GPU/device-resident; '
              f'each database block is compared with ALL of them in tiles of {tile_rows:,}. '
              f'Database block size: {db_rows:,}.', flush=True)
    del values
    best = torch.full((qe - qs, k), -torch.inf, device=device, dtype=dtype)
    ids = torch.full((qe - qs, k), -1, device=device, dtype=torch.int64)
    scan_started = time.monotonic()
    for block_index, (bs, host_block) in enumerate(database_blocks(base, db_rows, prefetch)):
        be = bs + len(host_block)
        b = upload(host_block)
        bnorm = (b*b).sum(dim=1) if metric == 'euclidean' else None
        del host_block
        for offset in range(0, len(q), tile_rows):
            stop = min(offset + tile_rows, len(q))
            scores = q[offset:stop] @ b.T
            if metric == 'euclidean':
                # Rank negative squared L2; sqrt only the final selected distances.
                scores.mul_(2).sub_(qnorm[offset:stop,None]).sub_(bnorm[None,:])
                scores.clamp_(max=0)
            if query_ids is None:
                lo, hi = max(qs + offset, bs), min(qs + stop, be)
                if lo < hi:
                    diag = torch.arange(lo, hi, device=device)
                    scores[diag - qs - offset, diag - bs] = -torch.inf
            else:
                current = query_ids[offset:stop]
                positions = np.flatnonzero((current >= bs) & (current < be))
                if len(positions):
                    row = torch.as_tensor(positions, device=device)
                    col = torch.as_tensor(current[positions]-bs, device=device)
                    scores[row, col] = -torch.inf
            values, indices = torch.topk(scores, min(k, be - bs), dim=1)
            del scores
            candidates = torch.cat((best[offset:stop], values), dim=1)
            candidate_ids = torch.cat((ids[offset:stop], indices + bs), dim=1)
            values, positions = torch.topk(candidates, k, dim=1)
            best[offset:stop] = values
            ids[offset:stop] = candidate_ids.gather(1, positions)
        del b
        if log and block_index % 100 == 0:
            if str(device).startswith('cuda'):
                torch.cuda.synchronize()
            elapsed = time.monotonic()-scan_started
            eta = elapsed*(len(base)-be)/be
            print(f'  ALL {len(q):,} queries [positions {qs:,}:{qe:,}) compared against '
                  f'{be:,}/{len(base):,} database vectors ({100*be/len(base):.2f}%) | '
                  f'elapsed {elapsed/60:.1f} min, ETA {eta/60:.1f} min', flush=True)
    distances = (-best).clamp_min(0).sqrt() if metric == 'euclidean' else 1-best.clamp(-1,1)
    return ids.cpu().numpy(), distances.cpu().numpy()


def search(args, benchmark=False, device='cuda:0'):
    import numpy as np
    torch = configure_torch()
    if str(device).startswith('cuda') and not torch.cuda.is_available():
        raise RuntimeError('CUDA is unavailable; run on the H100 with a CUDA-enabled PyTorch')
    manifest = json.loads((args.output / 'manifest.json').read_text())
    progress = json.loads((args.output / 'prepare_progress.json').read_text())
    if not progress['complete']:
        raise ValueError('Run prepare to completion first')
    base = ShardedBase(args.output, manifest)
    n, d = base.shape
    if (n, d) != (manifest['rows'], manifest['dimension']) or not 0 < args.k < n:
        raise ValueError('Invalid base shape or k')
    dtype = getattr(torch, args.precision)
    if benchmark:
        # An actual full-database scan of one query block includes real storage I/O.
        qcount = min(args.query_rows, n)
        start = time.monotonic()
        scan(base, 0, qcount, args.k, args.db_rows, args.tile_rows, device, dtype, True, not args.no_prefetch)
        seconds = time.monotonic() - start
        report = dict(query_rows=qcount, database_rows=n, seconds=seconds,
                      estimated_full_days=seconds * n / qcount / 86400,
                      note='One full database pass; caching and throttling can change total runtime')
        atomic_json(args.output / 'benchmark.json', report)
        print(json.dumps(report, indent=2))
        return
    config = dict(k=args.k, precision=args.precision, query_rows=args.query_rows,
                  db_rows=args.db_rows, tile_rows=args.tile_rows, rows=n,
                  layout_version=2, parts=base.parts,
                  metric='cosine', exclude_self=True, exhaustive=True, tf32=False,
                  ties='Any k among equal similarity values; tie IDs are not stable',
                  distances_dtype=args.precision)
    cp = args.output / 'search_config.json'
    if cp.exists() and json.loads(cp.read_text()) != config:
        raise ValueError('Search configuration changed; use the original configuration')
    atomic_json(cp, config)
    result = args.output / 'knn'
    result.mkdir(exist_ok=True)
    query_blocks = [(p['id'], s, min(s+args.query_rows,p['end']))
                    for p in base.parts for s in range(p['start'],p['end'],args.query_rows)]
    count = len(query_blocks)
    for block, (part_id, qs, qe) in enumerate(query_blocks):
        part_result = result / f'part_{part_id:02d}'
        part_result.mkdir(exist_ok=True)
        target = part_result / f'{qs:012d}_{qe:012d}.npz'
        if target.exists():
            with np.load(target) as saved:
                if (int(saved['query_start']) != qs or int(saved['query_end']) != qe or
                        saved['ids'].shape != (qe - qs, args.k) or
                        saved['distances'].shape != (qe - qs, args.k)):
                    raise ValueError(f'Invalid checkpoint {target}')
            continue
        begin = time.monotonic()
        ids, distances = scan(base, qs, qe, args.k, args.db_rows, args.tile_rows,
                              device, dtype, True, not args.no_prefetch)
        if np.any(ids < 0) or not np.isfinite(distances).all():
            raise ValueError('Incomplete/nonfinite search results')
        # N=200M fits in uint32. Preserve int64 automatically for larger corpora.
        ids = ids.astype(np.uint32 if n <= 2**32 else np.int64)
        tmp = target.with_suffix('.tmp')
        with tmp.open('wb') as f:
            np.savez(f, query_start=qs, query_end=qe, ids=ids, distances=distances)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, target)
        seconds = time.monotonic() - begin
        atomic_json(args.output / 'search_progress.json',
                    dict(rows_done=qe, total_rows=n, complete=qe == n,
                         last_block_seconds=seconds))
        print(f'block {block + 1}/{count}, queries {qe:,}/{n:,}, {seconds:.1f}s', flush=True)
    atomic_json(args.output / 'search_progress.json',
                dict(rows_done=n, total_rows=n, complete=True))


def self_test():
    import numpy as np
    torch = configure_torch()
    device = 'cuda:0' if torch.cuda.is_available() else 'cpu'
    rng = np.random.default_rng(20261012)
    x = rng.normal(size=(137, 31)).astype(np.float32)
    x[8] = x[3]  # Distinct rows with identical vectors must remain eligible.
    norm = x.astype(np.float64)
    norm /= np.linalg.norm(norm, axis=1, keepdims=True)
    similarities = norm @ norm.T
    np.fill_diagonal(similarities, -np.inf)
    k = 7
    expected = np.sort(similarities, axis=1)[:, -k:][:, ::-1]
    import tempfile
    with tempfile.TemporaryDirectory() as directory:
        output = Path(directory)
        parts = []
        for i in range(16):
            start, end = len(x)*i//16, len(x)*(i+1)//16
            name = f'part_{i:02d}.npy'
            np.save(output / name, x[start:end])
            parts.append(dict(id=i, start=start, end=end, file=name))
        base = ShardedBase(output, dict(rows=len(x), dimension=x.shape[1], parts=parts))
        for precision, tolerance in [(torch.float64, 1e-12), (torch.float32, 3e-6)]:
            for source, prefetch in [(x, False), (base, False), (base, True)]:
                for qs, qe in [(0, 19), (19, 137)]:
                    ids, distances = scan(source, qs, qe, k, 5, 11, device, precision,
                                          prefetch=prefetch)
                    assert np.all(ids != np.arange(qs, qe)[:, None])
                    assert all(len(set(row)) == k for row in ids.tolist())
                    selected = similarities[np.arange(qs, qe)[:, None], ids]
                    np.testing.assert_allclose(selected, expected[qs:qe], atol=tolerance, rtol=0)
                    np.testing.assert_allclose(distances, 1-selected, atol=tolerance, rtol=0)
    print(f'PASS: 16-part/global exhaustive reference, prefetch on/off, both precisions, '
          f'self exclusion, duplicates, partial tiles, db block < k on {device}')
    sampled = rng.choice(len(x), size=23, replace=False)
    for precision, tolerance in [(torch.float64, 1e-12), (torch.float32, 3e-6)]:
        ids, distances = scan(x, 0, len(sampled), k, 5, 11, device, precision,
                              query_ids=sampled)
        assert np.all(ids != sampled[:, None])
        selected = similarities[sampled[:, None], ids]
        np.testing.assert_allclose(selected, expected[sampled], atol=tolerance, rtol=0)
        np.testing.assert_allclose(distances, 1-selected, atol=tolerance, rtol=0)
    print(f'PASS: sampled query global IDs and exact full-base neighbors on {device}')
    from symmetric_seq_ann import self_test_symmetric
    self_test_symmetric(x, device)


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'sample', 'recover-sample', 'search', 'benchmark', 'self-test'])
    parser.add_argument('--source', type=Path, default=root / 'Data/Embedded/emb_1B')
    parser.add_argument('--output', type=Path, default=root / 'Data/seq_200M_cosine_top50_16parts')
    parser.add_argument('--key', default='embeddings')
    parser.add_argument('--rows', type=int, default=200_000_000)
    parser.add_argument('--k', type=int, default=100)
    parser.add_argument('--query-count', type=int, default=200000,
                        help='sampled queries drawn without replacement from the full base')
    parser.add_argument('--seed', type=int, default=20261013)
    parser.add_argument('--copy-rows', type=int, default=16384)
    parser.add_argument('--sample-chunk-rows', type=int, default=1024,
                        help='sample rows extracted per worker checkpoint')
    parser.add_argument('--progress-interval', type=float, default=10.0,
                        help='sample aggregate progress interval in seconds (default: 10)')
    parser.add_argument('--parts', type=int, default=16, help='number of independent base files')
    parser.add_argument('--workers', type=int, default=16, help='prepare/sample CPU processes')
    parser.add_argument('--no-prefetch', action='store_true', help='disable search host read-ahead')
    parser.add_argument('--algorithm', choices=['sampled', 'symmetric', 'directed'], default='sampled',
                        help='search/benchmark algorithm (default: sampled 200K vs full base)')
    parser.add_argument('--query-rows', type=int, default=1048576)
    parser.add_argument('--db-rows', type=int, default=65536)
    parser.add_argument('--tile-rows', type=int, default=2048)
    parser.add_argument('--precision', choices=['float32', 'float64'], default='float32')
    parser.add_argument('--metric', choices=['euclidean', 'cosine'], default='euclidean',
                        help='sampled search distance (default: euclidean; no normalization)')
    args = parser.parse_args()
    if args.command in ('search', 'benchmark') and args.algorithm != 'sampled' and args.metric != 'cosine':
        parser.error('Euclidean search currently requires --algorithm sampled')
    if any(getattr(args, name) <= 0 for name in
           ['rows', 'k', 'query_count', 'copy_rows', 'sample_chunk_rows', 'query_rows', 'db_rows', 'tile_rows', 'workers', 'parts']):
        parser.error('All sizes must be positive')
    if args.seed < 0:
        parser.error('--seed must be nonnegative')
    import math
    if not math.isfinite(args.progress_interval) or args.progress_interval <= 0:
        parser.error('--progress-interval must be finite and positive')
    if args.command == 'self-test':
        self_test()
    else:
        args.output.mkdir(parents=True, exist_ok=True)
        if args.command == 'prepare':
            prepare(args)
        elif args.command == 'sample':
            from sampled_seq_ann import sample_queries
            sample_queries(args)
        elif args.command == 'recover-sample':
            from sampled_seq_ann import recover_sample
            recover_sample(args)
        elif args.algorithm == 'sampled':
            from sampled_seq_ann import search_sampled
            search_sampled(args, benchmark=args.command == 'benchmark')
        elif args.algorithm == 'symmetric':
            from symmetric_seq_ann import search_symmetric
            search_symmetric(args, benchmark=args.command == 'benchmark')
        else:
            search(args, benchmark=args.command == 'benchmark')


if __name__ == '__main__':
    main()
