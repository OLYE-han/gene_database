"""Upper-triangular all-pairs kNN with transactional, two-sided top-k checkpoints."""
import json
import os
import re
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor

from build_seq_ann_gpu import ShardedBase, atomic_json, configure_torch, normalized


def merge_topk(best, ids, scores, candidate_start):
    import torch
    k = best.shape[1]
    values, positions = torch.topk(scores, min(k, scores.shape[1]), dim=1)
    candidates = torch.cat((best, values), dim=1)
    candidate_ids = torch.cat((ids, positions + candidate_start), dim=1)
    values, positions = torch.topk(candidates, k, dim=1)
    best.copy_(values)
    ids.copy_(candidate_ids.gather(1, positions))


def symmetric_pair(left, right, ls, li, rs, ri, left_start, right_start,
                   tile_rows, db_rows, diagonal=False):
    """One GEMM supplies both directions; only diagonal microtiles are full squares."""
    import torch
    pairs = 0
    # Matching microtile boundaries make triangular traversal well-defined on diagonal blocks.
    right_tile = tile_rows if diagonal else db_rows
    for b in range(0, len(right), right_tile):
        be = min(b + right_tile, len(right))
        for a in range(0, be if diagonal else len(left), tile_rows):
            ae = min(a + tile_rows, len(left))
            scores = left[a:ae] @ right[b:be].T
            pairs += (ae-a) * (be-b)
            if diagonal and a == b:
                scores.fill_diagonal_(-torch.inf)
            merge_topk(ls[a:ae], li[a:ae], scores, right_start+b)
            if not diagonal or a != b:
                merge_topk(rs[b:be], ri[b:be], scores.T, left_start+a)
    return pairs


def empty_state(rows, k, device, dtype):
    import torch
    return (torch.full((rows,k), -torch.inf, device=device, dtype=dtype),
            torch.full((rows,k), -1, device=device, dtype=torch.int64))


def save_npz(path, **arrays):
    import numpy as np
    tmp = path.with_suffix('.tmp')
    with tmp.open('wb') as f:
        np.savez(f, **arrays)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_state(work, versions, index, blocks, k, device, dtype):
    import numpy as np
    import torch
    _, start, end = blocks[index]
    if versions[index] is None:
        return empty_state(end-start, k, device, dtype)
    path = work / 'state' / f'{index:06d}_{versions[index]:09d}.npz'
    with np.load(path) as saved:
        scores, ids = saved['scores'], saved['ids']
        if (int(saved['start']) != start or int(saved['end']) != end or
                scores.shape != (end-start,k) or ids.shape != scores.shape):
            raise ValueError(f'Invalid state {path}')
        return (torch.from_numpy(scores).to(device=device, dtype=dtype),
                torch.from_numpy(ids).to(device=device, dtype=torch.int64))


def save_state(work, index, version, bounds, scores, ids):
    _, start, end = bounds
    path = work / 'state' / f'{index:06d}_{version:09d}.npz'
    save_npz(path, start=start, end=end, scores=scores.cpu().numpy(), ids=ids.cpu().numpy())


def publish_result(work, index, version, blocks, n, k):
    import numpy as np
    part, start, end = blocks[index]
    directory = work / 'knn' / f'part_{part:02d}'
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f'{start:012d}_{end:012d}.npz'
    if target.exists():
        with np.load(target) as saved:
            if (int(saved['query_start']) != start or int(saved['query_end']) != end or
                    saved['ids'].shape != (end-start,k) or saved['distances'].shape != (end-start,k)):
                raise ValueError(f'Invalid result {target}')
        return
    with np.load(work / 'state' / f'{index:06d}_{version:09d}.npz') as state:
        ids, scores = state['ids'], state['scores']
        if np.any(ids < 0) or np.any(ids >= n) or not np.isfinite(scores).all():
            raise ValueError('Cannot finalize an incomplete top-k block')
        if np.any(ids == np.arange(start,end)[:,None]):
            raise ValueError('Self neighbor in state')
        save_npz(target, query_start=start, query_end=end,
                 ids=ids.astype(np.uint32 if n <= 2**32 else np.int64),
                 distances=1 - np.clip(scores,-1,1))


def search_symmetric(args, benchmark=False, device='cuda:0', stop_after_pairs=None,
                     fault_hook=None):
    """Commit both block states and the cursor together via an atomic metadata pointer.

    stop_after_pairs/fault_hook are internal test seams, not production CLI options.
    """
    import numpy as np
    torch = configure_torch()
    if str(device).startswith('cuda') and not torch.cuda.is_available():
        raise RuntimeError('CUDA is unavailable; run on the H100 with CUDA-enabled PyTorch')
    manifest = json.loads((args.output / 'manifest.json').read_text())
    if not json.loads((args.output / 'prepare_progress.json').read_text())['complete']:
        raise ValueError('Run prepare to completion first')
    base = ShardedBase(args.output, manifest)
    n, d = base.shape
    if not 0 < args.k < n:
        raise ValueError('k must be between 1 and N-1')
    dtype = getattr(torch, args.precision)
    blocks = [(p['id'], s, min(s+args.query_rows,p['end']))
              for p in base.parts for s in range(p['start'],p['end'],args.query_rows)]
    count = len(blocks)
    work = args.output / 'symmetric_search'
    work.mkdir(exist_ok=True)
    # Include a copy of the source manifest so a changed dataset cannot reuse old states.
    config = dict(version=1, algorithm='upper_triangular', manifest=manifest,
                  k=args.k, precision=args.precision, query_rows=args.query_rows,
                  db_rows=args.db_rows, tile_rows=args.tile_rows,
                  tf32=False, exclude_self=True, ties='arbitrary equal-score neighbors')
    cp = work / 'config.json'
    if cp.exists() and json.loads(cp.read_text()) != config:
        raise ValueError('Symmetric configuration changed; restore original search parameters')
    atomic_json(cp, config)
    if benchmark:
        i, j = 0, min(1,count-1)
        _, a, ae = blocks[i]
        _, b, be = blocks[j]
        begin = time.monotonic()
        left = normalized(base[a:ae], device, dtype)
        right = left if i == j else normalized(base[b:be], device, dtype)
        ls, li = empty_state(ae-a,args.k,device,dtype)
        rs, ri = (ls,li) if i == j else empty_state(be-b,args.k,device,dtype)
        evaluated = symmetric_pair(left,right,ls,li,rs,ri,a,b,args.tile_rows,args.db_rows,i==j)
        if str(device).startswith('cuda'):
            torch.cuda.synchronize()
        seconds = time.monotonic()-begin
        diagonal_extra = sum(min(args.tile_rows,e-s-t)**2
                             for _,s,e in blocks for t in range(0,e-s,args.tile_rows))
        total_evaluated = (n*n + diagonal_extra)//2
        report = dict(seconds=seconds, evaluated_pairs=evaluated,
                      total_evaluated_pairs=total_evaluated,
                      estimated_compute_days=seconds*total_evaluated/evaluated/86400,
                      note='One block-pair sample includes vector loading; excludes checkpoint I/O; not an end-to-end guarantee')
        atomic_json(work / 'benchmark.json', report)
        print(json.dumps(report,indent=2), flush=True)
        return
    (work / 'state').mkdir(exist_ok=True)
    progress_path = work / 'progress.json'
    state = (json.loads(progress_path.read_text()) if progress_path.exists() else
             dict(next_i=0,next_j=0,versions=[None]*count,completed_pairs=0,
                  evaluated_pairs=0,rows_done=0,complete=False))
    if (len(state['versions']) != count or not 0 <= state['next_i'] <= count or
            (state['next_i'] < count and not state['next_i'] <= state['next_j'] < count)):
        raise ValueError('Invalid symmetric cursor')
    # Only this application's unreferenced generations are removed. Referenced states are immutable.
    referenced = {f'{i:06d}_{v:09d}.npz' for i,v in enumerate(state['versions']) if v is not None}
    for path in (work / 'state').glob('*.npz'):
        if re.fullmatch(r'\d{6}_\d{9}\.npz', path.name) and path.name not in referenced:
            path.unlink()
    # A crash after commit but before final result publication is recovered here.
    for i in range(state['next_i']):
        publish_result(work,i,state['versions'][i],blocks,n,args.k)
    completed_this_run = 0
    for i in range(state['next_i'],count):
        _, a, ae = blocks[i]
        left = normalized(base[a:ae],device,dtype)
        ls, li = load_state(work,state['versions'],i,blocks,args.k,device,dtype)
        first_j = state['next_j'] if i == state['next_i'] else i
        begin = time.monotonic()
        def read_block(j):
            _, start, end = blocks[j]
            return np.array(base[start:end],copy=True)
        with ThreadPoolExecutor(max_workers=1) as pool:
            upcoming = None
            if not args.no_prefetch and first_j > i:
                upcoming = pool.submit(read_block,first_j)
            for j in range(first_j,count):
                _, b, be = blocks[j]
                if i == j:
                    right, rs, ri = left, ls, li
                else:
                    host = upcoming.result() if upcoming is not None else read_block(j)
                    right = normalized(host,device,dtype)
                    del host
                    rs, ri = load_state(work,state['versions'],j,blocks,args.k,device,dtype)
                upcoming = (pool.submit(read_block,j+1)
                            if not args.no_prefetch and j+1 < count else None)
                evaluated = symmetric_pair(left,right,ls,li,rs,ri,a,b,
                                           args.tile_rows,args.db_rows,i==j)
                version = state['completed_pairs']+1
                old_versions = state['versions'].copy()
                # Write new generations first; until metadata commits, neither update is visible.
                save_state(work,i,version,blocks[i],ls,li)
                if fault_hook:
                    fault_hook('after_left_write',i,j)
                if i != j:
                    save_state(work,j,version,blocks[j],rs,ri)
                if fault_hook:
                    fault_hook('before_commit',i,j)
                versions = old_versions.copy()
                versions[i] = versions[j] = version
                ni, nj = (i+1,i+1) if j == count-1 else (i,j+1)
                state = dict(next_i=ni,next_j=nj,versions=versions,completed_pairs=version,
                             evaluated_pairs=state['evaluated_pairs']+evaluated,
                             rows_done=ae if ni > i else a,complete=False)
                atomic_json(progress_path,state)
                if fault_hook:
                    fault_hook('after_commit',i,j)
                for index in {i,j}:
                    old = old_versions[index]
                    if old is not None:
                        (work / 'state' / f'{index:06d}_{old:09d}.npz').unlink(missing_ok=True)
                del right, rs, ri
                completed_this_run += 1
                print(f'symmetric pair ({i},{j}), {version}/{count*(count+1)//2}, '
                      f'row elapsed {time.monotonic()-begin:.1f}s',flush=True)
                if stop_after_pairs is not None and completed_this_run >= stop_after_pairs:
                    return
        publish_result(work,i,state['versions'][i],blocks,n,args.k)
        del left, ls, li
    state['complete'] = True
    state['rows_done'] = n
    atomic_json(progress_path,state)


def self_test_symmetric(x, device):
    import numpy as np
    torch=configure_torch()
    k=7
    reference=x.astype(np.float64)
    reference/=np.linalg.norm(reference,axis=1,keepdims=True)
    scores=reference@reference.T
    np.fill_diagonal(scores,-np.inf)
    expected=np.sort(scores,axis=1)[:,-k:][:,::-1]
    edges=[len(x)*i//5 for i in range(6)]
    for dtype,tolerance in [(torch.float32,3e-6),(torch.float64,1e-12)]:
        vectors=[normalized(x[a:b],device,dtype) for a,b in zip(edges,edges[1:])]
        states=[empty_state(len(v),k,device,dtype) for v in vectors]
        for i in range(5):
            for j in range(i,5):
                symmetric_pair(vectors[i],vectors[j],*states[i],*states[j],
                               edges[i],edges[j],7,11,i==j)
        ids=np.concatenate([state[1].cpu().numpy() for state in states])
        got=np.concatenate([state[0].cpu().numpy() for state in states])
        assert np.all(ids != np.arange(len(x))[:,None])
        assert all(len(set(row))==k for row in ids.tolist())
        selected=scores[np.arange(len(x))[:,None],ids]
        np.testing.assert_allclose(selected,expected,atol=tolerance,rtol=0)
        np.testing.assert_allclose(got,selected,atol=tolerance,rtol=0)
    print(f'PASS: symmetric upper-triangle, both-sided top-k and both precisions on {device}')
