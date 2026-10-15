#!/usr/bin/env python3
"""
emb_deepspeed.py
DeepSpeed-Inference powered ESM embedding pipeline (multi-GPU model-parallel).

Key behavior:
 - Distributed: one process per GPU (torch.distributed).
 - Model loaded with deepspeed.init_inference(mp_size=world_size) to shard weights.
 - Rank 0 splits FASTA into shard_{i}.fasta in tmp_dir (reused on resume).
 - Shards assigned round-robin: shard_i processed by rank (i % world_size).
 - Streaming reader -> buffer -> adaptive batches (sorted by length) -> forward -> mean-pool -> move to CPU -> append to shard HDF5.
 - Per-shard .ckpt written atomically AFTER data flush (resume safety).
 - Supports bf16/autocast for H100. Uses immediate free (del + empty_cache + gc).

Run examples:
 - deepspeed --num_gpus=8 emb_deepspeed.py -i /path/to.fa --model ESM2_3B --shard-size 100000 --batch-size 64 --buffer-size 4096 --tmp-dir /tmp/embds --output merged.h5 --merge
 - torchrun --nproc_per_node=8 emb_deepspeed.py ...

"""

import argparse, os, time, pathlib, sys, gc, shutil
from typing import List, Tuple
import numpy as np
import h5py
from tqdm import tqdm
import torch
import random

# Ensure local ESM path if present (optional)
sys.path.insert(0, str(pathlib.Path(__file__).parent / 'ESM'))
from ESM import esm

# Deepspeed import
try:
    import deepspeed
except Exception as e:
    deepspeed = None

import torch.distributed as dist


def init_distributed():
    if not dist.is_initialized():
        dist.init_process_group(backend='nccl')
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    return rank, world_size


def load_model_deepspeed(model_name, device, mp_size=1, dtype=torch.bfloat16, replace_method='auto'):
    # load model using ESM APIs then wrap with deepspeed.init_inference
    if model_name == 'ESM2_3B':
        rep_layer = 36
        model, alphabet = esm.pretrained.esm2_t36_3B_UR50D()
        dsize = 2560
    elif model_name == 'ESM1b':
        rep_layer = 33
        model, alphabet = esm.pretrained.esm1b_t33_650M_UR50S()
        dsize = 1280
    elif model_name == 'ESM2_650M':
        rep_layer = 33
        model, alphabet = esm.pretrained.esm2_t33_650M_UR50D()
        dsize = 1280
    else:
        raise ValueError('Unknown model')
    model.eval()
    # move to device before init_inference (deepspeed will handle sharding)
    model.to(device)
    if deepspeed is not None and mp_size > 1:
        # init_inference will shard parameters across mp_size processes
        model = deepspeed.init_inference(model,
                                         mp_size=mp_size,
                                         dtype=dtype,
                                         replace_method=replace_method,
                                         replace_with_kernel_inject=True)
    return model, alphabet, rep_layer, dsize


def stream_fasta_reader(path):
    with open(path, 'r') as f:
        name = None
        lines = []
        for l in f:
            l = l.rstrip('\n')
            if not l:
                continue
            if l[0] == '>':
                if name is not None:
                    yield name, ''.join(lines)
                name = l[1:].split()[0]
                lines = []
            else:
                lines.append(l)
        if name is not None:
            yield name, ''.join(lines)


def split_fasta(input_fasta, tmp_dir, shard_size):
    tmp_dir = pathlib.Path(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    shards = []
    it = stream_fasta_reader(str(input_fasta))
    idx = 0
    while True:
        buf = []
        try:
            for _ in range(shard_size):
                buf.append(next(it))
        except StopIteration:
            pass
        if not buf:
            break
        p = tmp_dir / f'shard_{idx}.fasta'
        with p.open('w') as fh:
            for nm, seq in buf:
                fh.write(f'>{nm}\n{seq}\n')
        shards.append(str(p))
        idx += 1
    return shards


def atomic_write_ckpt(ckpt_path, count):
    tmp = str(ckpt_path) + '.tmp'
    with open(tmp, 'w') as f:
        f.write(str(count))
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, str(ckpt_path))


def flush_write(h5path, new_embs: List[np.ndarray], new_names: List[str], new_lens: List[int], dsize:int):
    if len(new_embs) == 0:
        return
    with h5py.File(str(h5path), 'a') as f:
        str_dt = h5py.string_dtype(encoding='utf-8')
        embs_arr = np.array(new_embs, dtype='float32')
        names_arr = np.array(new_names, dtype=object)
        if 'embeddings' not in f:
            f.create_dataset('embeddings', data=embs_arr, compression='gzip', chunks=(max(1, len(embs_arr)), dsize), maxshape=(None, dsize))
            f.create_dataset('Seq_Name', data=names_arr, compression='gzip', chunks=(max(1, len(names_arr)),), maxshape=(None,), dtype=str_dt)
            f.create_dataset('seq_len', data=np.array(new_lens, dtype='i4'), compression='gzip', chunks=(max(1, len(new_lens)),), maxshape=(None,))
        else:
            cur = f['embeddings'].shape[0]
            f['embeddings'].resize((cur + embs_arr.shape[0]), axis=0)
            f['embeddings'][cur:cur+embs_arr.shape[0]] = embs_arr
            f['Seq_Name'].resize((f['Seq_Name'].shape[0] + names_arr.shape[0]), axis=0)
            try:
                f['Seq_Name'][-names_arr.shape[0]:] = names_arr
            except Exception:
                f['Seq_Name'][-names_arr.shape[0]:] = [str(n) for n in new_names]
            f['seq_len'].resize((f['seq_len'].shape[0] + len(new_lens)), axis=0)
            f['seq_len'][-len(new_lens):] = np.array(new_lens, dtype='i4')
        f.flush()
        try:
            os.fsync(f.id)  # may fail depending on h5py/driver
        except Exception:
            pass


def adaptive_process_buffer(items: List[Tuple[str,str]], model, batch_converter, rep_layer, device,
                            batch_size:int, max_batch:int, min_batch:int, use_bf16:bool, dsize:int):
    # sort by length to reduce padding
    items_sorted = sorted(items, key=lambda x: len(x[1]))
    out_embs, out_names, out_lens = [], [], []
    i = 0
    bs = batch_size
    N = len(items_sorted)
    while i < N:
        cur = []
        while len(cur) < bs and i < N:
            nm, seq = items_sorted[i]
            cur.append((nm, seq.upper().replace(' ', '')[:1022]))
            i += 1
        if not cur:
            break
        try:
            data = [(nm, s) for nm,s in cur]
            _, _, tokens = batch_converter(data)
            tokens = tokens.to(device)
            # bf16 autocast for H100
            if use_bf16 and device.type.startswith('cuda'):
                ctx = torch.autocast(device_type='cuda', dtype=torch.bfloat16)
            else:
                class _C:
                    def __enter__(self): return None
                    def __exit__(self, *a): return False
                ctx = _C()
            with torch.inference_mode():
                with ctx:
                    results = model(tokens, repr_layers=[rep_layer], return_contacts=False)
            reps = results['representations'][rep_layer]
            for bi in range(reps.size(0)):
                L = min(len(cur[bi][1]), 1022)
                vec = reps[bi, 1:L+1, :].mean(dim=0).cpu().numpy().astype('float32')
                out_embs.append(vec)
            out_names.extend([nm for nm,_ in cur])
            out_lens.extend([min(len(s),1022) for _,s in cur])
            # free GPU memory aggressively
            try:
                del reps, results, tokens
            except Exception:
                pass
            if device.type.startswith('cuda'):
                torch.cuda.empty_cache()
            gc.collect()
            # try increasing bs mildly
            if bs*2 <= max_batch:
                bs = min(bs*2, max_batch)
        except RuntimeError as e:
            msg = str(e).lower()
            if 'out of memory' in msg or 'oom' in msg:
                # back off
                if device.type.startswith('cuda'):
                    torch.cuda.empty_cache()
                gc.collect()
                bs = max(min_batch, bs//2)
                # roll back i to retry cur
                i -= len(cur)
                time.sleep(0.5)
            else:
                raise
    return out_embs, out_names, out_lens


def process_assigned_shards(rank:int, world_size:int, shards:List[str], model_name:str, args):
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    mp_size = world_size if (deepspeed is not None) else 1
    dtype = torch.bfloat16 if args.bf16 else torch.float16 if args.fp16 else torch.float32
    model, batch_converter, rep_layer, dsize = load_model_deepspeed(model_name, device, mp_size=mp_size, dtype=dtype)
    # which shards belong to this rank
    assigned = [s for i,s in enumerate(shards) if (i % world_size) == rank]
    if not assigned:
        print(f'Rank {rank}: no shards assigned, exiting.')
        return
    for path in assigned:
        shard_path = pathlib.Path(path)
        out_h5 = pathlib.Path(args.tmp_dir) / f'{shard_path.stem}.h5'
        ckpt = out_h5.with_suffix('.ckpt')
        processed = 0
        if ckpt.exists():
            try:
                with open(ckpt, 'r') as f:
                    processed = int(f.read().strip())
            except Exception:
                processed = 0
        elif out_h5.exists():
            try:
                with h5py.File(str(out_h5),'r') as f:
                    processed = f['embeddings'].shape[0]
            except Exception:
                processed = 0
        # prepare iterator that skips processed items
        it = stream_fasta_reader(str(shard_path))
        for _ in range(processed):
            try: next(it)
            except StopIteration: break
        pbar = tqdm(desc=f'Rank{rank}-{shard_path.name}', unit='seq', initial=processed)
        local_count = processed
        buffer=[]
        forward_time=0.0
        start=time.time()
        for nm, seq in it:
            buffer.append((nm, seq))
            if len(buffer) >= args.buffer_size:
                t0=time.time()
                embs, names, lens = adaptive_process_buffer(buffer, model, batch_converter, rep_layer, device,
                                                            args.batch_size, args.max_batch_size, args.min_batch_size,
                                                            args.bf16, dsize)
                forward_time += time.time()-t0
                flush_write(out_h5, embs, names, lens, dsize)
                local_count += len(names)
                pbar.update(len(names))
                atomic_write_ckpt(ckpt, local_count)
                buffer.clear()
                if args.test_limit and local_count >= args.test_limit:
                    break
        # flush remaining
        if buffer:
            t0=time.time()
            embs, names, lens = adaptive_process_buffer(buffer, model, batch_converter, rep_layer, device,
                                                        args.batch_size, args.max_batch_size, args.min_batch_size,
                                                        args.bf16, dsize)
            forward_time += time.time()-t0
            flush_write(out_h5, embs, names, lens, dsize)
            local_count += len(names)
            pbar.update(len(names))
            atomic_write_ckpt(ckpt, local_count)
            buffer.clear()
        pbar.close()
        elapsed=time.time()-start
        overall = local_count / elapsed if elapsed>0 else 0.0
        forward_only = local_count / forward_time if forward_time>0 else 0.0
        print(f'Rank{rank} Completed shard {shard_path.name}: processed {local_count}, overall {overall:.2f} seq/s, forward-only {forward_only:.2f} seq/s')


def merge_shards_if_needed(tmp_dir, output, dsize):
    shard_paths = sorted(pathlib.Path(tmp_dir).glob('shard_*.h5'))
    if not shard_paths:
        print('No shard outputs to merge')
        return
    with h5py.File(output,'w') as fout:
        fout.create_dataset('embeddings', shape=(0,dsize), maxshape=(None,dsize), dtype='float32', compression='gzip', chunks=True)
        fout.create_dataset('Seq_Name', shape=(0,), maxshape=(None,), dtype=h5py.string_dtype(encoding='utf-8'), compression='gzip', chunks=True)
        fout.create_dataset('seq_len', shape=(0,), maxshape=(None,), dtype='i4', compression='gzip', chunks=True)
    for sp in shard_paths:
        with h5py.File(str(sp),'r') as fs, h5py.File(output,'a') as fout:
            e = fs['embeddings'][:]
            names = fs['Seq_Name'][:]
            lens = fs['seq_len'][:]
            cur = fout['embeddings'].shape[0]
            fout['embeddings'].resize((cur + e.shape[0]), axis=0)
            fout['embeddings'][cur:cur+e.shape[0]] = e
            fout['Seq_Name'].resize((fout['Seq_Name'].shape[0] + names.shape[0]), axis=0)
            try:
                fout['Seq_Name'][-names.shape[0]:] = names
            except Exception:
                fout['Seq_Name'][-names.shape[0]:] = [n.decode('utf-8') if isinstance(n, bytes) else str(n) for n in names]
            fout['seq_len'].resize((fout['seq_len'].shape[0] + lens.shape[0]), axis=0)
            fout['seq_len'][-lens.shape[0]:] = lens
    print('Merge done:', output)


def count_sequences(path):
    return sum(1 for _ in stream_fasta_reader(path))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    parser.add_argument('-o','--output', default='emb_deepspeed.h5')
    parser.add_argument('--model', default='ESM2_3B')
    parser.add_argument('--tmp-dir', default='../../tmp_emb_deepspeed')
    parser.add_argument('--shard-size', type=int, default=100000)
    parser.add_argument('--batch-size', type=int, default=128)
    parser.add_argument('--max-batch-size', type=int, default=512)
    parser.add_argument('--min-batch-size', type=int, default=1)
    parser.add_argument('--buffer-size', type=int, default=1000)
    parser.add_argument('--merge', action='store_true')
    parser.add_argument('--test-limit', type=int, default=0)
    parser.add_argument('--bf16', action='store_true', help='use bfloat16/autocast (recommended on H100)')
    parser.add_argument('--fp16', action='store_true', help='use fp16 if preferred')
    args = parser.parse_args()

    # init distributed
    if not dist.is_available():
        raise RuntimeError('torch.distributed not available')
    # Expect user to launch via deepspeed or torchrun
    rank = int(os.environ.get('RANK', '0'))
    world_size = int(os.environ.get('WORLD_SIZE', '1'))
    if not dist.is_initialized():
        dist.init_process_group(backend='nccl')
        rank = dist.get_rank(); world_size = dist.get_world_size()

    # rank0 manages splitting
    tmp_dir = pathlib.Path(args.tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    if rank == 0:
        # reuse existing shards if present
        existing = sorted(list(tmp_dir.glob('shard_*.fasta')))
        if existing:
            shards = [str(p) for p in existing]
        else:
            shards = split_fasta(args.input, tmp_dir, args.shard_size)
        # Broadcast the shards list by writing a shards.txt for all ranks to read
        sh_list_file = tmp_dir / 'shards.list'
        with open(sh_list_file, 'w') as fh:
            for s in shards:
                fh.write(s + '\n')
    # barrier then all ranks read shards.list
    dist.barrier()
    sh_list_file = tmp_dir / 'shards.list'
    if not sh_list_file.exists():
        if rank == 0:
            raise RuntimeError('Shard list missing')
        else:
            # wait briefly for file
            time.sleep(1.0)
    shards = [line.strip() for line in open(sh_list_file).read().splitlines() if line.strip()]
    process_assigned_shards(rank, world_size, shards, args.model, args)

    dist.barrier()
    # merging done only by rank 0
    if args.merge and rank == 0:
        # need model dsize to set dataset shape
        # load a cpu model for dimension
        model_tmp, _, _, dsize = load_model_deepspeed(args.model, torch.device('cpu'), mp_size=1, dtype=torch.float32)
        merge_shards_if_needed(args.tmp_dir, args.output, dsize)

if __name__ == '__main__':
    main()
