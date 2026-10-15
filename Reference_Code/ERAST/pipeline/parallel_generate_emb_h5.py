#!/usr/bin/env python3
"""
Parallel, checkpointed embedding generator using multiple GPUs.
Usage (test on small file):
  python parallel_generate_emb_h5.py -i ../../SMAGs.fasta -o ../../SMAGs.emb.h5 --gpus 0,1 --shard-size 5000 --batch-size 4 --model ESM1b

Design:
- Split input fasta into shard fasta files under tmp_dir.
- Launch one worker process per GPU to process shards (can process multiple shards per GPU).
- Each worker writes a per-shard HDF5 and a checkpoint file (.ckpt) regularly so processing can resume.
- After all shards complete, optional merge into single HDF5.

Notes:
- Benchmark on SMAGs.fasta to measure seqs/sec; then estimate runtime for large file using formula printed by script.
- Estimated size: per-seq bytes = dsize * 4 (float32). Total bytes = per-seq * total_seqs. Add ~10% overhead for metadata/compression.
"""

import argparse
import os
import pathlib
import time
import multiprocessing as mp
from Bio import SeqIO
import h5py
import numpy as np
import torch
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


def worker(shard_path, out_h5, gpu_id, model_name, batch_size, ckpt_interval, test_limit):
    os.environ['CUDA_VISIBLE_DEVICES'] = str(gpu_id)
    device = torch.device('cuda:0')
    model, batch_converter, rep_layer, dsize = load_model(model_name, device)

    # Prepare output h5
    out_h5 = pathlib.Path(out_h5)
    ckpt_path = out_h5.with_suffix('.ckpt')
    processed = 0
    if out_h5.exists():
        # determine how many already written
        try:
            with h5py.File(str(out_h5), 'r') as f:
                processed = f['embeddings'].shape[0]
        except Exception:
            processed = 0
    # load checkpoint file if exists
    if ckpt_path.exists():
        try:
            with open(ckpt_path, 'r') as cf:
                processed = int(cf.read().strip())
        except Exception:
            processed = processed

    seq_iter = SeqIO.parse(str(shard_path), 'fasta')
    # skip already processed
    for _ in range(processed):
        try:
            next(seq_iter)
        except StopIteration:
            break

    names = []
    embs = []
    lens = []
    last_ckpt = processed
    start_time = time.time()
    batch = []
    batch_names = []

    def flush_write(h5path, new_embs, new_names, new_lens):
        if len(new_embs) == 0:
            return
        with h5py.File(str(h5path), 'a') as f:
            if 'embeddings' not in f:
                maxshape = (None, new_embs.shape[1])
                f.create_dataset('embeddings', data=new_embs, compression='gzip', chunks=True, maxshape=maxshape)
                f.create_dataset('Seq_Name', data=np.array(new_names, dtype='S'), compression='gzip', chunks=True, maxshape=(None,))
                f.create_dataset('seq_len', data=np.array(new_lens, dtype='i4'), compression='gzip', chunks=True, maxshape=(None,))
            else:
                f['embeddings'].resize((f['embeddings'].shape[0] + new_embs.shape[0]), axis=0)
                f['embeddings'][-new_embs.shape[0]:] = new_embs
                f['Seq_Name'].resize((f['Seq_Name'].shape[0] + len(new_names)), axis=0)
                f['Seq_Name'][-len(new_names):] = np.array(new_names, dtype='S')
                f['seq_len'].resize((f['seq_len'].shape[0] + len(new_lens)), axis=0)
                f['seq_len'][-len(new_lens):] = np.array(new_lens, dtype='i4')

    for rec in seq_iter:
        seq = str(rec.seq).upper().replace(' ', '')
        plen = min(len(seq), 1022)
        seq = seq[:plen]
        batch.append((rec.id, seq))
        if len(batch) >= batch_size:
            # prepare data for batch
            data = [(nm, s) for nm, s in batch]
            labels = [("p", s) for (_, s) in data]
            names_batch = [nm for (nm, _) in data]
            batch_tokens = batch_converter(labels)[2].to(device)
            with torch.no_grad():
                results = model(batch_tokens, repr_layers=[rep_layer], return_contacts=False)
            token_reprs = results['representations'][rep_layer]
            # token_reprs shape: (batch, seq_len, dsize+?) ; take mean over tokens excluding BOS/EOS
            arrs = []
            for bi in range(token_reprs.size(0)):
                tr = token_reprs[bi].cpu().numpy()
                tr = np.mean(tr[1:-1], axis=0).astype('float32')
                arrs.append(tr)
            embs_batch = np.stack(arrs, axis=0)
            lens_batch = [min(len(s), 1022) for (_, s) in data]
            # flush to disk
            flush_write(out_h5, embs_batch, names_batch, lens_batch)
            processed += len(names_batch)
            # update checkpoint
            with open(ckpt_path, 'w') as cf:
                cf.write(str(processed))
            batch = []
            # progress
            if processed % ckpt_interval == 0:
                elapsed = time.time() - start_time
                print(f"GPU {gpu_id} {out_h5.name}: processed {processed} sequences, elapsed {elapsed:.1f}s")
            if test_limit and processed >= test_limit:
                break

    # flush remaining
    if len(batch) > 0:
        data = [(nm, s) for nm, s in batch]
        labels = [("p", s) for (_, s) in data]
        names_batch = [nm for (nm, _) in data]
        batch_tokens = batch_converter(labels)[2].to(device)
        with torch.no_grad():
            results = model(batch_tokens, repr_layers=[rep_layer], return_contacts=False)
        token_reprs = results['representations'][rep_layer]
        arrs = []
        for bi in range(token_reprs.size(0)):
            tr = token_reprs[bi].cpu().numpy()
            tr = np.mean(tr[1:-1], axis=0).astype('float32')
            arrs.append(tr)
        embs_batch = np.stack(arrs, axis=0)
        lens_batch = [min(len(s), 1022) for (_, s) in data]
        flush_write(out_h5, embs_batch, names_batch, lens_batch)
        processed += len(names_batch)
        with open(ckpt_path, 'w') as cf:
            cf.write(str(processed))

    # done
    print(f"Worker finished {out_h5}, total processed={processed}")


def merge_shards(shard_h5_paths, final_h5, dsize):
    final_h5 = pathlib.Path(final_h5)
    # create final file with extendable dataset
    with h5py.File(str(final_h5), 'w') as fout:
        fout.create_dataset('embeddings', shape=(0, dsize), maxshape=(None, dsize), dtype='float32', compression='gzip', chunks=True)
        fout.create_dataset('Seq_Name', shape=(0,), maxshape=(None,), dtype='S')
        fout.create_dataset('seq_len', shape=(0,), maxshape=(None,), dtype='i4')
    # append each shard
    for sp in shard_h5_paths:
        with h5py.File(str(sp), 'r') as fs, h5py.File(str(final_h5), 'a') as fout:
            e = fs['embeddings'][:]
            names = fs['Seq_Name'][:]
            lens = fs['seq_len'][:]
            fout['embeddings'].resize((fout['embeddings'].shape[0] + e.shape[0]), axis=0)
            fout['embeddings'][-e.shape[0]:] = e
            fout['Seq_Name'].resize((fout['Seq_Name'].shape[0] + names.shape[0]), axis=0)
            fout['Seq_Name'][-names.shape[0]:] = names
            fout['seq_len'].resize((fout['seq_len'].shape[0] + lens.shape[0]), axis=0)
            fout['seq_len'][-lens.shape[0]:] = lens
    print(f"Merged {len(shard_h5_paths)} shards into {final_h5}")


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
    parser.add_argument('--shard-size', type=int, default=10000, help='number of sequences per shard')
    parser.add_argument('--batch-size', type=int, default=4, help='inference batch size per process')
    parser.add_argument('--model', default='ESM1b')
    parser.add_argument('--tmp-dir', default='../../tmp_emb', help='temporary dir for shards and shard outputs')
    parser.add_argument('--ckpt-interval', type=int, default=1000, help='how often to print progress and write checkpoint')
    parser.add_argument('--merge', action='store_true', help='merge shard h5 files into single output')
    parser.add_argument('--test-limit', type=int, default=0, help='process only first N sequences per shard (for testing)')
    args = parser.parse_args()

    input_fasta = pathlib.Path(args.input)
    tmp_dir = pathlib.Path(args.tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # parse gpus
    if ',' in args.gpus:
        gpu_list = [int(x) for x in args.gpus.split(',') if x.strip() != '']
    else:
        # single number means count
        try:
            n = int(args.gpus)
            gpu_list = list(range(n))
        except ValueError:
            gpu_list = [int(args.gpus)]

    print(f"Splitting fasta into shards (shard_size={args.shard_size}) under {tmp_dir}")
    shards = split_fasta(input_fasta, tmp_dir, args.shard_size)
    print(f"Created {len(shards)} shards")

    # assign shards round-robin to GPUs
    shard_outputs = []
    jobs = []
    for idx, shard in enumerate(shards):
        gpu = gpu_list[idx % len(gpu_list)]
        out_h5 = tmp_dir / f"shard_{idx}.h5"
        shard_outputs.append(str(out_h5))
        p = mp.Process(target=worker, args=(shard, out_h5, gpu, args.model, args.batch_size, args.ckpt_interval, args.test_limit))
        jobs.append(p)

    # start up to len(gpu_list) processes at a time to avoid overloading system
    running = []
    for p in jobs:
        while True:
            # count active processes per GPU
            if sum(1 for r in running if r.is_alive()) >= len(gpu_list):
                time.sleep(1)
            else:
                p.start()
                running.append(p)
                break

    # wait for all
    for p in running:
        p.join()

    print('All shard workers completed')

    # merge if requested
    model_tmp, _, _, dsize = load_model(args.model, torch.device('cpu'))
    if args.merge:
        print('Merging shards...')
        merge_shards(shard_outputs, args.output, dsize)
        print('Merge done')
    else:
        print('Shard outputs:')
        for s in shard_outputs:
            print(s)

    # Print size estimate example (user should provide total seq count for exact)
    print('To estimate final size: use estimate_size(num_seqs, dsize) function. Example per-seq bytes:', dsize * 4)
