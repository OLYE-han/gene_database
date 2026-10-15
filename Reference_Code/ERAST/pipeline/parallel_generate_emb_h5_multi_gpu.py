#!/usr/bin/env python3
"""
Multi-GPU checkpointed embedding generator (worker-pool) tuned for ESM2_3B.
- Uses one worker process per GPU (or custom worker count).
- Workers pull shard paths from a shared queue and process them sequentially (model loaded once per GPU).
- Each shard produces shard_{i}.h5 and shard_{i}.ckpt under tmp_dir so runs can be resumed.

Test (SMAGs.fasta):
  python parallel_generate_emb_h5_multi_gpu.py -i ../../SMAGs.fasta --gpus 0,1 --shard-size 2000 --batch-size 4 --model ESM2_3B --merge

Notes:
- Default model is ESM2_3B as requested. Adjust --batch-size for best throughput.
- For huge input, prefer not to merge into a single file; use sharded outputs and process downstream in parallel.
"""

import argparse
import pathlib
import time
import os
from multiprocessing import Process, Manager
from Bio import SeqIO
from tqdm import tqdm
import h5py
import numpy as np
import torch
import random
import sys, pathlib as _pl
# Ensure local ESM package (pipeline/ESM) is importable when running from this directory
sys.path.insert(0, str(_pl.Path(__file__).parent / 'ESM'))
from ESM import esm


def load_model(model_name, device):
    dsize = 1280
    if model_name == 'ESM1b':
        rep_layer = 33
        model, alphabet = esm.pretrained.esm1b_t33_650M_UR50S()
    elif model_name == 'ESM2_3B':
        rep_layer = 36
        model, alphabet = esm.pretrained.esm2_t36_3B_UR50D()
        dsize = 2560
    elif model_name == 'ESM2_650M':
        rep_layer = 33
        model, alphabet = esm.pretrained.esm2_t33_650M_UR50D()
    else:
        raise ValueError('Unknown model')
    model.eval()
    model.to(device)
    batch_converter = alphabet.get_batch_converter()
    return model, batch_converter, rep_layer, dsize


def split_fasta(input_fasta, tmp_dir, shard_size):
    tmp_dir = pathlib.Path(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    shards = []
    it = SeqIO.parse(str(input_fasta), 'fasta')
    shard_idx = 0
    while True:
        records = []
        try:
            for _ in range(shard_size):
                rec = next(it)
                records.append(rec)
        except StopIteration:
            pass
        if len(records) == 0:
            break
        shard_path = tmp_dir / f'shard_{shard_idx}.fasta'
        with shard_path.open('w') as out:
            SeqIO.write(records, out, 'fasta')
        shards.append(str(shard_path))
        shard_idx += 1
    return shards


def process_shard(shard_path, out_h5_path, model, batch_converter, rep_layer, dsize, batch_size, ckpt_interval, test_limit, use_bf16=False):
    device = torch.device('cuda:0')

    shard_path = pathlib.Path(shard_path)
    out_h5 = pathlib.Path(out_h5_path)
    ckpt_path = out_h5.with_suffix('.ckpt')

    # Determine how many already processed
    processed = 0
    if ckpt_path.exists():
        try:
            with open(ckpt_path, 'r') as cf:
                processed = int(cf.read().strip())
        except Exception:
            processed = 0
    elif out_h5.exists():
        try:
            with h5py.File(str(out_h5), 'r') as f:
                processed = f['embeddings'].shape[0]
        except Exception:
            processed = 0

    # iterate sequences and skip processed
    try:
        total_in_shard = sum(1 for _ in SeqIO.parse(str(shard_path), 'fasta'))
    except Exception:
        total_in_shard = None
    seq_iter = SeqIO.parse(str(shard_path), 'fasta')
    for _ in range(processed):
        try:
            next(seq_iter)
        except StopIteration:
            break

    # setup progress bar for this shard
    if total_in_shard is not None:
        pbar = tqdm(total=total_in_shard, desc=f"Shard {shard_path.name}", unit='seq', initial=processed)
    else:
        pbar = tqdm(desc=f"Shard {shard_path.name}", unit='seq')

    local_count = processed
    forward_time = 0.0
    start_time = time.time()

    def flush_write(h5path, new_embs, new_names, new_lens):
        if len(new_embs) == 0:
            return
        with h5py.File(str(h5path), 'a') as f:
            str_dt = h5py.string_dtype(encoding='utf-8')
            names_arr = np.array([n.encode('utf-8') if isinstance(n, str) else n for n in new_names], dtype='S')
            if 'embeddings' not in f:
                maxshape = (None, new_embs.shape[1])
                f.create_dataset('embeddings', data=new_embs, compression='gzip', chunks=True, maxshape=maxshape)
                f.create_dataset('Seq_Name', data=names_arr, compression='gzip', chunks=True, maxshape=(None,), dtype=str_dt)
                f.create_dataset('seq_len', data=np.array(new_lens, dtype='i4'), compression='gzip', chunks=True, maxshape=(None,))
            else:
                f['embeddings'].resize((f['embeddings'].shape[0] + new_embs.shape[0]), axis=0)
                f['embeddings'][-new_embs.shape[0]:] = new_embs
                f['Seq_Name'].resize((f['Seq_Name'].shape[0] + len(new_names)), axis=0)
                try:
                    f['Seq_Name'][-len(new_names):] = names_arr
                except Exception:
                    f['Seq_Name'][-len(new_names):] = [n.decode('utf-8') if isinstance(n, (bytes, np.bytes_)) else str(n) for n in new_names]
                f['seq_len'].resize((f['seq_len'].shape[0] + len(new_lens)), axis=0)
                f['seq_len'][-len(new_lens):] = np.array(new_lens, dtype='i4')

    try:
        batch = []
        local_batch_size = batch_size
        for rec in seq_iter:
            seq = str(rec.seq).upper().replace(' ', '')
            plen = min(len(seq), 1022)
            seq = seq[:plen]
            batch.append((rec.id, seq))
            if len(batch) >= local_batch_size:
                # Attempt inference with OOM backoff
                success = False
                while not success:
                    data = [(nm, s) for nm, s in batch]
                    labels = [("p", s) for (_, s) in data]
                    names_batch = [nm for (nm, _) in data]
                    try:
                        batch_tokens = batch_converter(labels)[2].to(device)
                        if use_bf16:
                            with torch.inference_mode():
                                with torch.autocast(device_type='cuda', dtype=torch.bfloat16):
                                    t0 = time.time()
                                    results = model(batch_tokens, repr_layers=[rep_layer], return_contacts=False)
                                    forward_time += (time.time() - t0)
                        else:
                            with torch.inference_mode():
                                t0 = time.time()
                                results = model(batch_tokens, repr_layers=[rep_layer], return_contacts=False)
                                forward_time += (time.time() - t0)
                        success = True
                    except RuntimeError as e:
                        msg = str(e).lower()
                        if 'out of memory' in msg:
                            # OOM: reduce batch and retry
                            old = local_batch_size
                            local_batch_size = max(1, local_batch_size // 2)
                            print(f"OOM on shard {shard_path.name}: reducing batch {old} -> {local_batch_size} and retrying")
                            torch.cuda.empty_cache()
                            time.sleep(1)
                            if local_batch_size == 1 and old == 1:
                                raise
                            # Trim batch to new size and continue retry
                            if len(batch) > local_batch_size:
                                # keep only last local_batch_size sequences to retry
                                batch = batch[:local_batch_size]
                            continue
                        else:
                            raise

                token_reprs = results['representations'][rep_layer]
                arrs = []
                for bi in range(token_reprs.size(0)):
                    tr = token_reprs[bi].cpu().numpy()
                    tr = np.mean(tr[1:-1], axis=0).astype('float32')
                    arrs.append(tr)
                embs_batch = np.stack(arrs, axis=0)
                lens_batch = [min(len(s), 1022) for (_, s) in data]
                flush_write(out_h5, embs_batch, names_batch, lens_batch)
                local_count += len(names_batch)
                pbar.update(len(names_batch))
                with open(ckpt_path, 'w') as cf:
                    cf.write(str(local_count))
                batch = []
                if local_count % ckpt_interval == 0:
                    elapsed = time.time() - start_time
                    overall_throughput = local_count / elapsed if elapsed > 0 else 0.0
                    gpu_forward_throughput = local_count / forward_time if forward_time > 0 else 0.0
                    print(f"{out_h5.name}: processed {local_count} seqs, elapsed {elapsed:.1f}s, overall {overall_throughput:.2f} seq/s, forward-only {gpu_forward_throughput:.2f} seq/s")
                if test_limit and local_count >= test_limit:
                    break

        # flush remaining batch
        if len(batch) > 0:
            data = [(nm, s) for nm, s in batch]
            labels = [("p", s) for (_, s) in data]
            names_batch = [nm for (nm, _) in data]
            batch_tokens = batch_converter(labels)[2].to(device)
            if use_bf16:
                with torch.inference_mode():
                    with torch.autocast(device_type='cuda', dtype=torch.bfloat16):
                        t0 = time.time()
                        results = model(batch_tokens, repr_layers=[rep_layer], return_contacts=False)
                        forward_time += (time.time() - t0)
            else:
                with torch.inference_mode():
                    t0 = time.time()
                    results = model(batch_tokens, repr_layers=[rep_layer], return_contacts=False)
                    forward_time += (time.time() - t0)

            token_reprs = results['representations'][rep_layer]
            arrs = []
            for bi in range(token_reprs.size(0)):
                tr = token_reprs[bi].cpu().numpy()
                tr = np.mean(tr[1:-1], axis=0).astype('float32')
                arrs.append(tr)
            embs_batch = np.stack(arrs, axis=0)
            lens_batch = [min(len(s), 1022) for (_, s) in data]
            flush_write(out_h5, embs_batch, names_batch, lens_batch)
            local_count += len(names_batch)
            pbar.update(len(names_batch))
            with open(ckpt_path, 'w') as cf:
                cf.write(str(local_count))

    finally:
        pbar.close()

    elapsed_total = time.time() - start_time
    overall_throughput = local_count / elapsed_total if elapsed_total > 0 else 0.0
    gpu_forward_throughput = local_count / forward_time if forward_time > 0 else 0.0
    print(f"Completed shard {shard_path.name}: total processed={local_count}, elapsed {elapsed_total:.1f}s, overall {overall_throughput:.2f} seq/s, forward-only {gpu_forward_throughput:.2f} seq/s")


def worker_loop(gpu_id, shard_queue, tmp_dir, model_name, batch_size, ckpt_interval, test_limit, use_bf16=False, use_compile=False):
    # bind this process to a single visible GPU
    os.environ['CUDA_VISIBLE_DEVICES'] = str(gpu_id)
    # serialize model loading across workers on the same tmp_dir using a lock file
    lock_path = pathlib.Path(tmp_dir) / 'model_load.lock'
    acquired = False
    while not acquired:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode('utf-8'))
            os.close(fd)
            acquired = True
        except FileExistsError:
            # wait a bit and retry to avoid simultaneous large allocations
            time.sleep(random.uniform(0.5, 2.0))
    try:
        device = torch.device('cuda:0')
        model, batch_converter, rep_layer, dsize = load_model(model_name, device)
        if use_compile:
            try:
                model = torch.compile(model)
            except Exception as e:
                print(f"Warning: torch.compile failed: {e}")
        print(f"GPU {gpu_id}: model loaded")
    finally:
        try:
            lock_path.unlink()
        except Exception:
            pass

    while True:
        try:
            shard_idx, shard_path = shard_queue.get_nowait()
        except Exception:
            break
        out_h5 = pathlib.Path(tmp_dir) / f"shard_{shard_idx}.h5"
        print(f"GPU {gpu_id} processing {shard_path}")
        try:
            process_shard(shard_path, out_h5, model, batch_converter, rep_layer, dsize, batch_size, ckpt_interval, test_limit, use_bf16=use_bf16)
        except RuntimeError as e:
            print(f"RuntimeError processing {shard_path} on GPU {gpu_id}: {e}")
        except Exception as e:
            print(f"Error processing {shard_path} on GPU {gpu_id}: {e}")


def merge_shards(shard_h5_paths, final_h5, dsize):
    final_h5 = pathlib.Path(final_h5)
    with h5py.File(str(final_h5), 'w') as fout:
        fout.create_dataset('embeddings', shape=(0, dsize), maxshape=(None, dsize), dtype='float32', compression='gzip', chunks=True)
        str_dt = h5py.string_dtype(encoding='utf-8')
        fout.create_dataset('Seq_Name', shape=(0,), maxshape=(None,), dtype=str_dt)
        fout.create_dataset('seq_len', shape=(0,), maxshape=(None,), dtype='i4')
    for sp in shard_h5_paths:
        with h5py.File(str(sp), 'r') as fs, h5py.File(str(final_h5), 'a') as fout:
            e = fs['embeddings'][:]
            names = fs['Seq_Name'][:]
            lens = fs['seq_len'][:]
            fout['embeddings'].resize((fout['embeddings'].shape[0] + e.shape[0]), axis=0)
            fout['embeddings'][-e.shape[0]:] = e
            fout['Seq_Name'].resize((fout['Seq_Name'].shape[0] + names.shape[0]), axis=0)
            # ensure names are utf-8 bytes/strings
            try:
                fout['Seq_Name'][-names.shape[0]:] = names
            except Exception:
                fout['Seq_Name'][-names.shape[0]:] = [n.decode('utf-8') if isinstance(n, bytes) else str(n) for n in names]
            fout['seq_len'].resize((fout['seq_len'].shape[0] + lens.shape[0]), axis=0)
            fout['seq_len'][-lens.shape[0]:] = lens
    print(f"Merged {len(shard_h5_paths)} shards into {final_h5}")


def count_sequences(fasta_path):
    c = 0
    for _ in SeqIO.parse(str(fasta_path), 'fasta'):
        c += 1
    return c


def estimate_size(num_seqs, dsize):
    per_seq = dsize * 4
    total_bytes = per_seq * num_seqs
    overhead = total_bytes * 0.1
    return total_bytes + overhead


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True)
    parser.add_argument('-o', '--output', default='embeddings.h5')
    parser.add_argument('--gpus', default='0', help='comma separated GPU ids or number of GPUs e.g. 0,1,2 or 2')
    parser.add_argument('--shard-size', type=int, default=100000)
    parser.add_argument('--batch-size', type=int, default=128)
    parser.add_argument('--model', default='ESM2_3B')
    parser.add_argument('--tmp-dir', default='../../tmp_emb', help='temporary dir for shards and shard outputs')
    parser.add_argument('--ckpt-interval', type=int, default=1000)
    parser.add_argument('--merge', action='store_true')
    parser.add_argument('--test-limit', type=int, default=0)
    parser.add_argument('--workers', type=int, default=0, help='number of worker processes (default = number of GPUs)')
    parser.add_argument('--bf16', action='store_true', help='Use bfloat16 autocast for inference (recommended on H100)')
    parser.add_argument('--compile', action='store_true', help='Enable torch.compile optimization (PyTorch >=2.0)')
    args = parser.parse_args()

    # Validate torch.compile prerequisites early to avoid confusing errors in worker subprocesses
    if args.compile:
        can_compile = True
        # torch.compile availability
        if not hasattr(torch, 'compile'):
            print("Warning: torch.compile not available in this PyTorch build. Disabling --compile.")
            can_compile = False
        else:
            try:
                import sympy  # required by torch.compile internals
            except Exception:
                print("Warning: sympy not importable in this Python environment. --compile will be disabled.")
                print("Install sympy in your environment, e.g.: conda install -n esmfold -c conda-forge sympy")
                can_compile = False
        if not can_compile:
            args.compile = False

    input_fasta = pathlib.Path(args.input)
    tmp_dir = pathlib.Path(args.tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # prepare GPU list (interpret --gpus as a comma-separated list of GPU ids)
    gpu_list = [int(x) for x in args.gpus.split(',') if x.strip() != '']
    if len(gpu_list) == 0:
        print("No GPUs specified. Use --gpus to list GPU ids, e.g. --gpus 0 or --gpus 0,1")
        raise SystemExit(1)

    num_workers = args.workers if args.workers > 0 else len(gpu_list)
    print(f"Using {num_workers} workers on GPUs: {gpu_list}")

    print('Counting sequences in input (this may take some time)')
    total_seqs = count_sequences(input_fasta)
    print(f"Total sequences: {total_seqs}")

    print(f"Checking for existing shard files under {tmp_dir}")
    # If shard fasta files already exist in tmp_dir, reuse them to keep consistent indexing and allow resume
    existing_shard_paths = sorted(list(tmp_dir.glob('shard_*.fasta')))
    if len(existing_shard_paths) > 0:
        print(f"Found {len(existing_shard_paths)} existing shard fasta files; reusing them (won't re-split)")
        shards = [str(p) for p in existing_shard_paths]
    else:
        print(f"Splitting fasta into shards (shard_size={args.shard_size}) under {tmp_dir}")
        shards = split_fasta(input_fasta, tmp_dir, args.shard_size)
        print(f"Created {len(shards)} shards")

    # Determine which shards are already fully processed using per-shard fasta and .ckpt/.h5
    unfinished = []
    for idx, shard in enumerate(shards):
        shard_path = pathlib.Path(shard)
        out_h5 = pathlib.Path(tmp_dir) / f"shard_{idx}.h5"
        ckpt_path = out_h5.with_suffix('.ckpt')
        # count sequences in shard fasta
        try:
            total_in_shard = sum(1 for _ in SeqIO.parse(str(shard_path), 'fasta'))
        except Exception:
            total_in_shard = None
        processed = 0
        if ckpt_path.exists():
            try:
                with open(ckpt_path, 'r') as cf:
                    processed = int(cf.read().strip())
            except Exception:
                processed = 0
        elif out_h5.exists():
            try:
                with h5py.File(str(out_h5), 'r') as f:
                    processed = f['embeddings'].shape[0]
            except Exception:
                processed = 0
        # If we know total_in_shard and processed >= total, skip this shard
        if total_in_shard is not None and processed >= total_in_shard and total_in_shard > 0:
            print(f"Shard {idx} already complete ({processed}/{total_in_shard}), skipping")
            continue
        # Otherwise add to unfinished list
        unfinished.append((idx, str(shard_path)))

    if len(unfinished) == 0:
        print('All shards appear complete; nothing to do. Exiting.')
        # still print existing shard outputs for user's info
        shard_h5_paths = [str(pathlib.Path(tmp_dir) / f'shard_{i}.h5') for i in range(len(shards))]
        existing_shards = [p for p in shard_h5_paths if pathlib.Path(p).exists()]
        if args.merge and len(existing_shards) > 0:
            print('Merging shards...')
            model_tmp, _, _, dsize = load_model(args.model, torch.device('cpu'))
            merge_shards(existing_shards, args.output, dsize)
            print('Merge done')
        raise SystemExit(0)

    print(f"{len(unfinished)} shards unfinished; starting from first unfinished shard")

    manager = Manager()
    shard_queue = manager.Queue()
    for idx, shard in unfinished:
        shard_queue.put((idx, shard))

    # start worker processes, assign GPUs round-robin
    workers = []
    for w in range(num_workers):
        gpu_id = gpu_list[w % len(gpu_list)]
        p = Process(target=worker_loop, args=(gpu_id, shard_queue, tmp_dir, args.model, args.batch_size, args.ckpt_interval, args.test_limit, args.bf16, args.compile))
        p.start()
        workers.append(p)

    for p in workers:
        p.join()

    print('All workers finished')

    shard_h5_paths = [str(pathlib.Path(tmp_dir) / f'shard_{i}.h5') for i in range(len(shards))]
    existing_shards = [p for p in shard_h5_paths if pathlib.Path(p).exists()]

    model_tmp, _, _, dsize = load_model(args.model, torch.device('cpu'))

    if args.merge:
        if len(existing_shards) == 0:
            print('No shard outputs found; nothing to merge. Check that workers ran and checkpoints exist.')
        else:
            print('Merging shards...')
            merge_shards(existing_shards, args.output, dsize)
            print('Merge done')
    else:
        print('Shard outputs (not merged):')
        for s in existing_shards:
            print(s)

    est_bytes = estimate_size(total_seqs, dsize)
    print(f"Estimated final size (bytes, ~10% overhead): {int(est_bytes)} (~{est_bytes/1e12:.2f} TB)")
