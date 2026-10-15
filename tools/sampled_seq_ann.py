"""Reproducible sampled-query exhaustive kNN against the complete base."""
import hashlib
import json
import os
import time
import multiprocessing
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from build_seq_ann_gpu import ShardedBase, atomic_json, configure_torch, scan, validate_array
from symmetric_seq_ann import save_npz


def sampling_context(args, create=False, metadata_only=False):
    import numpy as np
    if create:
        print(f'[sample:init] Reading {args.output / "manifest.json"}',flush=True)
    manifest = json.loads((args.output / 'manifest.json').read_text())
    if create:
        print('[sample:init] Checking prepare completion (metadata only)',flush=True)
    # Progress is a resumability aid, not the authoritative dataset description.
    # Published base files are still validated below (or by sample workers), and
    # search separately validates the completed query cache and source identities.
    prepare_progress = args.output / 'prepare_progress.json'
    try:
        prepare_state = json.loads(prepare_progress.read_text())
    except FileNotFoundError:
        print('[base:validate] prepare_progress.json absent; validating published base files and cache instead',flush=True)
    else:
        if not prepare_state['complete']:
            raise ValueError('Run prepare to completion first')
    base = ShardedBase(args.output, manifest, metadata_only=create or metadata_only)
    n = len(base)
    if not 0 < args.query_count <= n or not 0 < args.k < n:
        raise ValueError('Require 0 < query-count <= base rows and 0 < k < base rows')
    work = args.output / f'sampled_search_q{args.query_count}_k{args.k}_seed{args.seed}'
    if create:
        work.mkdir(exist_ok=True)
    elif not work.exists():
        raise ValueError(f'Run the CPU sample stage first: missing directory {work}')
    sampling = dict(version=1, manifest=manifest, query_count=args.query_count, k=args.k,
                    seed=args.seed, generator='numpy.PCG64', replace=False,
                    query_order='ascending global ID', database='entire prepared base')
    cp = work / 'sampling_config.json'
    if cp.exists() and json.loads(cp.read_text()) != sampling:
        raise ValueError('Sampling configuration or source manifest changed')
    if create:
        atomic_json(cp, sampling)
    elif not cp.exists():
        raise ValueError(f'Missing {cp}; if queries.npy exists, run recover-sample with the original query-count/k/seed; otherwise run the CPU sample stage first')
    query_path = work / 'query_ids.npy'
    if create:
        print(f'[sample:init] Loading or generating query IDs: {query_path}',flush=True)
    if query_path.exists():
        queries = np.load(query_path)
    elif not create:
        raise ValueError('Run the CPU sample stage first: query_ids.npy is missing')
    else:
        # Sort after sampling for efficient shard access; the subset is still uniform without replacement.
        queries = np.sort(np.random.Generator(np.random.PCG64(args.seed)).choice(
            n, size=args.query_count, replace=False)).astype(np.int64)
        tmp = query_path.with_suffix('.tmp')
        with tmp.open('wb') as f:
            np.save(f, queries)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, query_path)
    if (queries.dtype != np.int64 or queries.shape != (args.query_count,) or
            np.any(queries < 0) or np.any(queries >= n) or np.any(queries[1:] <= queries[:-1])):
        raise ValueError('Invalid saved query IDs')
    digest = hashlib.sha256(queries.tobytes()).hexdigest()
    base_files = []
    for index,part in enumerate(base.parts):
        path = args.output/part['file']
        if create:
            print(f'[sample:init] stat {index+1}/{len(base.parts)}: {path} (no vector read)',flush=True)
        st = path.stat()
        base_files.append(dict(file=part['file'],size=st.st_size,mtime_ns=st.st_mtime_ns))
    cache_config = dict(version=1, query_ids_sha256=digest, dimension=base.shape[1],
                        query_count=len(queries), dtype='float32',
                        base_files=base_files)
    return base, queries, work, cache_config


def extract_query_part(task):
    # CPU-only worker. Each worker owns its cache file and checkpoint.
    import numpy as np
    source, part, query_path, target, start, end, dimension, chunk_rows = task
    target = Path(target)
    partial = target.with_suffix('.partial.npy')
    progress = target.with_suffix('.progress.json')
    count = end-start
    print(f'[sample:worker {part["id"]:02d}] Checking query cache: {target}',flush=True)
    if target.exists():
        validate_array(target,count,dimension)
        return start,end,str(target)
    state = json.loads(progress.read_text()) if progress.exists() else {}
    done = state.get('rows_done',0)
    if not 0 <= done <= count or (done and not partial.exists()):
        raise ValueError(f'Invalid query-part checkpoint: {progress}')
    if partial.exists():
        validate_array(partial,count,dimension)
        result = np.load(partial,mmap_mode='r+')
    else:
        result = np.lib.format.open_memmap(partial,mode='w+',dtype=np.float32,shape=(count,dimension))
    print(f'[sample:worker {part["id"]:02d}] Loading query IDs: {query_path}',flush=True)
    queries = np.load(query_path,mmap_mode='r')[start:end]
    print(f'[sample:worker {part["id"]:02d}] Opening base header/mmap: {source}',flush=True)
    base = validate_array(Path(source),part['end']-part['start'],dimension)
    began = time.monotonic()
    print(f'sample part {part["id"]:02d}: extracting {count:,} queries, resume {done:,}',flush=True)
    for offset in range(done,count,chunk_rows):
        stop = min(offset+chunk_rows,count)
        values = np.asarray(base[queries[offset:stop]-part['start']],dtype=np.float32)
        if not np.isfinite(values).all() or np.any(np.max(np.abs(values),axis=1)==0):
            raise ValueError(f'Invalid sampled vectors in {source}')
        result[offset:stop] = values
        result.flush()
        with partial.open('rb') as f:
            os.fsync(f.fileno())
        atomic_json(progress,dict(rows_done=stop,complete=False))
        print(f'sample part {part["id"]:02d}: {stop:,}/{count:,}, {time.monotonic()-began:.1f}s',flush=True)
    del result
    os.replace(partial,target)
    atomic_json(progress,dict(rows_done=count,complete=True))
    return start,end,str(target)



def duration_text(seconds):
    if seconds is None:
        return '--'
    seconds = max(0, int(seconds))
    return f'{seconds//3600:02d}:{seconds//60%60:02d}:{seconds%60:02d}'


class SampleProgress:
    """One reporter publishes periodic aggregate progress, even during blocked reads."""
    def __init__(self, work, tasks, total, dimension, interval):
        self.work, self.tasks = work, tasks
        self.total, self.dimension, self.interval = total, dimension, interval
        self.started = self.phase_started = time.monotonic()
        self.phase, self.merged = 'extract', 0
        self.stop = threading.Event()
        self.lock = threading.RLock()  # Serializes only logger snapshots, never vector I/O.
        self.baseline = self.extracted()[0]
        self.previous = self.baseline
        self.last_change = self.started
        self.thread = threading.Thread(target=self.run, daemon=True)

    def extracted(self):
        rows, complete = 0, 0
        for task in self.tasks:
            target, count = Path(task[3]), task[5]-task[4]
            if target.exists():
                rows += count
                complete += 1
            else:
                path = target.with_suffix('.progress.json')
                if path.exists():
                    done = json.loads(path.read_text()).get('rows_done', 0)
                    rows += min(count, max(0, done))
        return rows, complete

    def report(self):
        with self.lock:
            now = time.monotonic()
            extracted, complete = self.extracted()
            done = extracted if self.phase == 'extract' else self.merged
            if done != self.previous:
                self.last_change, self.previous = now, done
            elapsed = now-self.phase_started
            rate = max(0,done-self.baseline)/max(elapsed,1e-6)
            eta = (self.total-done)/rate if rate > 0 and self.phase != 'finalize' else None
            percent = 100*done/self.total
            status = dict(queries_done=extracted,total_queries=self.total,
                          completed_parts=complete,total_parts=len(self.tasks),
                          phase=self.phase,stage_rows_done=done,percent=percent,
                          rows_per_second=rate,eta_seconds=eta,
                          elapsed_seconds=now-self.started,
                          unchanged_seconds=now-self.last_change,complete=False)
            atomic_json(self.work/'sample_progress.json',status)
            print(f'[sample:{self.phase}] {done:,}/{self.total:,} ({percent:.2f}%) '
                  f'parts={complete}/{len(self.tasks)} | {rate:.1f} queries/s '
                  f'({rate*self.dimension*4/1e6:.2f} MB/s vectors) | '
                  f'elapsed={duration_text(now-self.started)} ETA={duration_text(eta)} | '
                  f'no progress={duration_text(now-self.last_change)}',flush=True)

    def set_phase(self, phase):
        with self.lock:
            self.phase = phase
            self.phase_started = self.last_change = time.monotonic()
            self.baseline = self.merged if phase == 'finalize' else 0
            self.previous = self.merged
            self.report()

    def merged_to(self, rows):
        with self.lock:
            self.merged = rows
            self.report()

    def run(self):
        while not self.stop.wait(self.interval):
            try:
                self.report()
            except Exception as exc:
                print(f'[sample:progress] Could not report progress: {exc}',flush=True)

    def __enter__(self):
        self.report()
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join()


def verify_cached_part(task):
    """Read-only comparison: no query vectors are regenerated or written."""
    import numpy as np
    source, part, query_path, cache_path, start, end, dimension, chunk_rows = task
    print(f'[recover:part {part["id"]:02d}] Opening {source}; checking {end-start:,} queries',flush=True)
    base = validate_array(Path(source),part['end']-part['start'],dimension)
    ids = np.load(query_path,mmap_mode='r')
    cache = validate_array(Path(cache_path),len(ids),dimension)
    for offset in range(start,end,chunk_rows):
        stop = min(offset+chunk_rows,end)
        expected = base[ids[offset:stop]-part['start']]
        if not np.array_equal(cache[offset:stop],expected):
            raise ValueError(f'Query cache does not match base rows at query positions [{offset},{stop}); check original seed, query IDs and base. Metadata recovery was not completed.')
        print(f'[recover:part {part["id"]:02d}] verified {stop-start:,}/{end-start:,}',flush=True)
    return end-start


def recover_sample(args):
    """Reconstruct deleted metadata only after verifying every cached query."""
    import numpy as np
    os.environ.setdefault('OMP_NUM_THREADS','1')
    os.environ.setdefault('OPENBLAS_NUM_THREADS','1')
    work = args.output / f'sampled_search_q{args.query_count}_k{args.k}_seed{args.seed}'
    target = work/'queries.npy'
    if not target.is_file():
        raise ValueError(f'Cannot recover absent query cache: {target}; run sample')
    # Never allow an interrupted/failed recovery to appear ready for search.
    atomic_json(work/'sample_progress.json',dict(complete=False,phase='recover',queries_done=0,total_queries=args.query_count))
    base,queries,work,config = sampling_context(args,create=True)
    validate_array(target,len(queries),base.shape[1])
    expected = np.sort(np.random.Generator(np.random.PCG64(args.seed)).choice(
        len(base),size=args.query_count,replace=False)).astype(np.int64)
    if not np.array_equal(queries,expected):
        raise ValueError('Saved query IDs do not match the specified seed; recovery refused')
    config_path = work/'query_cache_config.json'
    if config_path.exists() and json.loads(config_path.read_text()) != config:
        raise ValueError('Base files/query IDs differ from saved cache configuration; recovery refused')
    before = target.stat()
    tasks = []
    for part in base.parts:
        start,end = map(int,np.searchsorted(queries,[part['start'],part['end']]))
        if start < end:
            tasks.append((str(args.output/part['file']),part,str(work/'query_ids.npy'),
                          str(target),start,end,base.shape[1],getattr(args,'sample_chunk_rows',1024)))
    done = 0
    with ProcessPoolExecutor(max_workers=min(args.workers,len(tasks)),
                             mp_context=multiprocessing.get_context('spawn')) as pool:
        for future in as_completed([pool.submit(verify_cached_part,t) for t in tasks]):
            done += future.result()
            print(f'[recover] verified {done:,}/{len(queries):,} ({100*done/len(queries):.2f}%)',flush=True)
            atomic_json(work/'sample_progress.json',dict(complete=False,phase='recover',queries_done=done,total_queries=len(queries)))
    after = target.stat()
    if ((before.st_size,before.st_mtime_ns) != (after.st_size,after.st_mtime_ns) or
            sampling_context(args,metadata_only=True)[3] != config):
        raise ValueError('Source/cache changed during recovery; metadata not published')
    atomic_json(config_path,config)
    atomic_json(work/'sample_progress.json',dict(complete=True,phase='complete',percent=100.0,queries_done=done,total_queries=len(queries)))
    print('[recover:complete] All cached queries verified; metadata restored without rewriting queries.npy',flush=True)


def sample_queries(args):
    import numpy as np
    os.environ.setdefault('OMP_NUM_THREADS','1')
    os.environ.setdefault('OPENBLAS_NUM_THREADS','1')
    work = args.output / f'sampled_search_q{args.query_count}_k{args.k}_seed{args.seed}'
    if (work/'queries.npy').exists():
        required = ['query_ids.npy','sampling_config.json','query_cache_config.json','sample_progress.json']
        if any(not (work/name).exists() for name in required):
            raise ValueError('Existing queries.npy has missing metadata; run recover-sample with the original query-count/k/seed')
        if not json.loads((work/'sample_progress.json').read_text()).get('complete',False):
            raise ValueError('Existing queries.npy is not marked complete; run recover-sample')
    print('[sample:init] Checking base files and loading/generating query IDs...',flush=True)
    base,queries,work,config = sampling_context(args,create=True)
    cache_config = work/'query_cache_config.json'
    if cache_config.exists() and json.loads(cache_config.read_text()) != config:
        raise ValueError('Base files/query IDs changed since query cache creation')
    atomic_json(cache_config,config)
    target = work/'queries.npy'
    if target.exists():
        validate_array(target,len(queries),base.shape[1])
        atomic_json(work/'sample_progress.json',dict(queries_done=len(queries),total_queries=len(queries),phase='complete',percent=100.0,complete=True))
        print(f'[sample:complete] {len(queries):,}/{len(queries):,} (100.00%) — query cache already complete: {target}',flush=True)
        return
    directory = work/'query_parts'
    directory.mkdir(exist_ok=True)
    tasks = []
    for part in base.parts:
        start,end = map(int,np.searchsorted(queries,[part['start'],part['end']]))
        if start == end:
            continue
        tasks.append((str(args.output/part['file']),part,str(work/'query_ids.npy'),
                      str(directory/f'part_{part["id"]:02d}.npy'),start,end,base.shape[1],
                      getattr(args,'sample_chunk_rows',1024)))
    results = []
    def record(result):
        results.append(result)
    with SampleProgress(work,tasks,len(queries),base.shape[1],
                        getattr(args,'progress_interval',10.0)) as progress:
        workers = getattr(args,'workers',16)
        if workers == 1:
            for task in tasks:
                record(extract_query_part(task))
        else:
            with ProcessPoolExecutor(max_workers=min(workers,len(tasks)),
                                     mp_context=multiprocessing.get_context('spawn')) as pool:
                futures = [pool.submit(extract_query_part,task) for task in tasks]
                try:
                    for future in as_completed(futures):
                        record(future.result())
                finally:
                    for future in futures:
                        future.cancel()
        progress.set_phase('merge')
        print(f'Merging {len(results)} query parts sequentially into {target}',flush=True)
        partial = target.with_suffix('.partial.npy')
        merged = np.lib.format.open_memmap(partial,mode='w+',dtype=np.float32,
                                           shape=(len(queries),base.shape[1]))
        cursor = 0
        for start,end,path in sorted(results):
            if start != cursor:
                raise ValueError('Incomplete query-part coverage')
            merged[start:end] = validate_array(Path(path),end-start,base.shape[1])
            cursor=end
            progress.merged_to(cursor)
        if cursor != len(queries):
            raise ValueError('Incomplete query cache')
        progress.set_phase('finalize')
        merged.flush()
        del merged
        with partial.open('rb') as f:
            os.fsync(f.fileno())
        # Recheck source identities before publishing the cache.
        if sampling_context(args,metadata_only=True)[3] != config:
            raise ValueError('Base files changed while sampling')
    os.replace(partial,target)
    atomic_json(work/'sample_progress.json',dict(queries_done=len(queries),total_queries=len(queries),phase='complete',percent=100.0,complete=True))
    print(f'[sample:complete] 100.00% — CPU sample complete: {len(queries):,} queries, {len(queries)*base.shape[1]*4/1e9:.3f} GB',flush=True)


def search_sampled(args, benchmark=False, device='cuda:0'):
    import numpy as np
    base,queries,work,cache_config = sampling_context(args)
    cp = work/'query_cache_config.json'
    if not cp.exists() or json.loads(cp.read_text()) != cache_config:
        raise ValueError('Missing/stale query cache; run the CPU sample stage first')
    if not (work/'queries.npy').exists():
        raise ValueError('Run the CPU sample stage first: queries.npy is missing')
    cache = validate_array(work/'queries.npy',len(queries),base.shape[1])
    if not (work/'sample_progress.json').exists() or not json.loads((work/'sample_progress.json').read_text())['complete']:
        raise ValueError('Run sample to completion first')
    digest = cache_config['query_ids_sha256']
    metric = getattr(args, 'metric', 'euclidean')
    if metric not in ('cosine', 'euclidean'):
        raise ValueError(f'Unsupported metric: {metric}')
    # Query caches contain raw vectors and are shared across distance metrics.
    # Keep the historical cosine layout; never resume its results for L2.
    if metric == 'euclidean':
        work = work/'euclidean'
        work.mkdir(exist_ok=True)
    n = len(base)
    print('Query cache validated; loading PyTorch and initializing compute device',flush=True)
    torch = configure_torch()
    if str(device).startswith('cuda') and not torch.cuda.is_available():
        raise RuntimeError('CUDA is unavailable; run on the H100 with CUDA-enabled PyTorch')
    dtype = getattr(torch, args.precision)
    if benchmark:
        positions = np.linspace(0,len(queries)-1,min(1024,args.query_rows,len(queries)),dtype=np.int64)
        sample = queries[positions]
        begin = time.monotonic()
        scan(base,0,len(sample),args.k,args.db_rows,args.tile_rows,device,dtype,
             True,not args.no_prefetch,query_ids=sample, query_values=cache[positions],metric=metric)
        report = dict(metric=metric,seconds=time.monotonic()-begin,query_count=len(sample),base_rows=n,
                      note='At most 1024 sampled queries, one complete base scan; I/O overhead prevents simple linear extrapolation')
        atomic_json(work / 'benchmark.json',report)
        print(json.dumps(report,indent=2),flush=True)
        return
    config = dict(algorithm='sampled_exact',precision=args.precision,k=args.k,
                  query_rows=args.query_rows,db_rows=args.db_rows,tile_rows=args.tile_rows,
                  query_ids_sha256=digest,exclude_self=True,tf32=False)
    if metric == 'euclidean':
        config.update(metric=metric,normalized=False,squared_distances=False)
    cp = work / 'search_config.json'
    if cp.exists() and json.loads(cp.read_text()) != config:
        raise ValueError('Search configuration/query IDs changed; restore original parameters')
    atomic_json(cp,config)
    result = work / 'knn'
    result.mkdir(exist_ok=True)
    progress = work / 'progress.json'
    atomic_json(progress,dict(queries_done=0,total_queries=len(queries),base_rows=n,complete=False))
    for start in range(0,len(queries),args.query_rows):
        end = min(start+args.query_rows,len(queries))
        qids = queries[start:end]
        target = result / f'{start:09d}_{end:09d}.npz'
        if target.exists():
            with np.load(target) as saved:
                if (int(saved['query_start']) != start or int(saved['query_end']) != end or
                        not np.array_equal(saved['query_ids'],qids) or
                        saved['ids'].shape != (end-start,args.k) or
                        saved['distances'].shape != (end-start,args.k)):
                    raise ValueError(f'Invalid checkpoint {target}')
        else:
            begin = time.monotonic()
            ids,distances = scan(base,start,end,args.k,args.db_rows,args.tile_rows,device,dtype,
                                 True,not args.no_prefetch,query_ids=qids, query_values=cache[start:end],metric=metric)
            if (np.any(ids < 0) or np.any(ids >= n) or np.any(ids == qids[:,None]) or
                    not np.isfinite(distances).all()):
                raise ValueError('Invalid sampled search results')
            save_npz(target,query_start=start,query_end=end,query_ids=qids,metric=metric,
                     ids=ids.astype(np.uint32 if n <= 2**32 else np.int64),distances=distances)
            print(f'sampled queries {end:,}/{len(queries):,}, {time.monotonic()-begin:.1f}s',flush=True)
        atomic_json(progress,dict(queries_done=end,total_queries=len(queries),base_rows=n,
                                  complete=end==len(queries)))
