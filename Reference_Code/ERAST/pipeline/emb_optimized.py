#!/usr/bin/env python3
"""
emb_optimized.py

Usage example:
  python emb_optimized.py -i ../../subcatalogs_GMGC10.100nr.faa --gpus 0,1,2 --shard-size 100000 --buffer-size 4096 --model ESM2_3B --merge
  
Note: 
  Using "--complie" might not be a good option, because the sequences' length is not fixed.
  TODO Available improvement: only use compile when the whole batch is the same length.

performance with different settings:
  With "--compile"                   very slow!
  With "--bf16"                      99-102 seqs/sec
  With "--compile" & "--bf16"        slow! 
  With nothing                       81-98 seqs/sec

This script:
 - Splits input FASTA into shard_{i}.fasta under tmp_dir (reuses if present for resume)
 - Uses one worker process per GPU; each worker loads model once (lock to avoid concurrent heavy allocations)
 - Processes shards streaming sequences, uses adaptive batch sizing per worker to increase throughput while avoiding OOM
 - Writes per-shard HDF5 (shard_{i}.h5) and shard_{i}.ckpt to allow resume on interruption
 - Optionally merges shard HDF5 files into a single output

Designed for very large inputs (billions of sequences). Test carefully on small data first.
"""

import argparse
import pathlib
import time
import os
from multiprocessing import Process, Manager
from tqdm import tqdm
import h5py
import numpy as np
import torch
import random
import sys
import pathlib as _pl
import gc
import threading
import queue
import psutil
import concurrent.futures
import logging
# setup logging to file ../../emb_log.log and console
log_path = _pl.Path(__file__).parent.joinpath('../../emb_log.log').resolve()
try:
    log_path.parent.mkdir(parents=True, exist_ok=True)
except Exception:
    pass
file_handler = logging.FileHandler(str(log_path), mode='a')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# avoid adding duplicate handlers on reimport
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# Lightweight PeakPerformanceMemoryManager (subset inspired by gen_emb_parrallel_ab_gc.py)
class PeakPerformanceMemoryManager:
    def __init__(self, device_id=0, target_utilization=0.85):
        self.device_id = device_id
        self.target_utilization = target_utilization
        self.last_cleanup_time = time.time()
        self.cleanup_interval = 30
        self.batch_history = []
        self.performance_history = []
        self.optimal_batch_size = None

    def get_memory_stats(self):
        if not torch.cuda.is_available():
            # provide CPU fallback stats
            vm = psutil.virtual_memory()
            return {'gpu_free_gb': None, 'gpu_allocated_gb': None, 'system_available_gb': vm.available/(1024**3)}
        try:
            free, total = torch.cuda.mem_get_info(self.device_id)
            allocated = torch.cuda.memory_allocated(self.device_id)
            return {'gpu_free_gb': free/(1024**3), 'gpu_allocated_gb': allocated/(1024**3)}
        except Exception:
            return None

    def optimize_for_speed(self, reason='periodic'):
        gc.collect()
        try:
            torch.cuda.empty_cache()
            torch.cuda.reset_peak_memory_stats()
        except Exception:
            pass

    def optimize_for_memory(self, reason='oom'):
        for _ in range(2):
            gc.collect()
            try:
                torch.cuda.empty_cache()
                torch.cuda.reset_peak_memory_stats()
                if torch.cuda.is_available():
                    torch.cuda.synchronize()
            except Exception:
                pass

    def adaptive_batch_size_adjustment(self, current_batch_size, batch_processing_time, sequences_processed):
        # simple heuristic: if processing time per seq increases, reduce; else grow slowly
        tps = batch_processing_time / max(1, sequences_processed)
        self.performance_history.append(tps)
        if len(self.performance_history) > 8:
            recent = self.performance_history[-4:]
            prev = self.performance_history[-8:-4]
            if np.mean(recent) > np.mean(prev) * 1.2:
                return max(1, int(current_batch_size * 0.8))
        stats = self.get_memory_stats()
        if stats and stats.get('gpu_free_gb') and stats['gpu_free_gb'] > 2.0:
            return min(current_batch_size * 2, current_batch_size + 64)
        return current_batch_size


# TensorMemoryPool - reuse tensors to avoid repeated allocations
class TensorMemoryPool:
    def __init__(self, device='cuda'):
        self.device = device
        self.pool = {}
        self.hits = 0
        self.misses = 0

    def get(self, shape, dtype=torch.int64):
        key = (tuple(shape), str(dtype))
        lst = self.pool.get(key)
        if lst and len(lst) > 0:
            self.hits += 1
            return lst.pop()
        self.misses += 1
        return torch.zeros(shape, dtype=dtype, device=self.device)

    def put(self, tensor):
        key = (tuple(tensor.shape), str(tensor.dtype))
        self.pool.setdefault(key, []).append(tensor)

    def clear(self):
        for k, lst in self.pool.items():
            for t in lst:
                try:
                    del t
                except Exception:
                    pass
        self.pool.clear()
        gc.collect()
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass


# multi-threaded reader for a shard to hide IO
def multi_threaded_fasta_reader(fasta_path: str, buffer_size: int = 8192):
    q = queue.Queue(maxsize=max(8, buffer_size//4))
    stop = threading.Event()

    def reader():
        try:
            with open(fasta_path, 'r') as f:
                name = None
                seq_lines = []
                for line in f:
                    if stop.is_set():
                        break
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith('>'):
                        if name is not None:
                            q.put((name, ''.join(seq_lines)))
                        name = line[1:].split()[0]
                        seq_lines = []
                    else:
                        seq_lines.append(line)
                if name is not None and seq_lines:
                    q.put((name, ''.join(seq_lines)))
        finally:
            q.put(None)

    t = threading.Thread(target=reader, name='ShardReader', daemon=True)
    t.start()
    while True:
        item = q.get()
        if item is None:
            break
        yield item


# ensure local ESM import path
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


def stream_fasta_reader(fasta_path: str, max_sequences: int = None):
    """Stream FASTA entries as (name, seq) pairs."""
    current_name = None
    current_seq = []
    count = 0
    with open(fasta_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith('>'):
                if current_name:
                    seq_str = ''.join(current_seq)
                    if seq_str:
                        yield current_name, seq_str
                        count += 1
                        if max_sequences and count >= max_sequences:
                            return
                current_name = line[1:].split()[0]
                current_seq = []
            else:
                current_seq.append(line)
        if current_name and current_seq:
            seq_str = ''.join(current_seq)
            if seq_str:
                yield current_name, seq_str


def split_fasta(input_fasta, tmp_dir, shard_size):
    tmp_dir = pathlib.Path(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    shards = []
    it = stream_fasta_reader(str(input_fasta))
    shard_idx = 0
    while True:
        records = []
        try:
            for _ in range(shard_size):
                nm, seq = next(it)
                records.append((nm, seq))
        except StopIteration:
            pass
        if len(records) == 0:
            break
        shard_path = tmp_dir / f'shard_{shard_idx}.fasta'
        with shard_path.open('w') as out:
            for nm, seq in records:
                out.write(f">{nm}\n")
                out.write(seq + "\n")
        shards.append(str(shard_path))
        shard_idx += 1
    return shards


def estimate_batch_size_by_length(max_seq_len: int, model_hidden_dim: int, target_gpu_memory_gb: float = 8.0) -> int:
    # conservative heuristic similar to gen_emb
    bytes_per_token = 4
    bytes_per_hidden_dim = 4
    seq_len_with_special = max_seq_len + 2
    input_memory = seq_len_with_special * bytes_per_token
    hidden_memory = seq_len_with_special * model_hidden_dim * bytes_per_hidden_dim
    intermediate_memory = input_memory * 12
    total_memory_per_seq = (input_memory + hidden_memory + intermediate_memory) / (1024**3)
    batch_size = int(target_gpu_memory_gb / total_memory_per_seq) if total_memory_per_seq > 0 else 1
    batch_size = max(1, min(batch_size, 1024))
    return batch_size


def adaptive_process_buffer(seq_buffer, model, batch_converter, rep_layer, device, max_length,
                            initial_batch_size=32, max_batch_size=512, min_batch_size=1,
                            memory_manager=None, use_bf16=False, writer_queue=None):
    """Process a list of (name, seq) using adaptive batching.
    If writer_queue is provided, enqueue pinned CPU tensors + completion event for async write and return number processed.
    Otherwise fallback to synchronous CPU numpy return (legacy path).
    """
    items = sorted(seq_buffer, key=lambda x: len(x[1]))
    cur_bs = initial_batch_size
    i = 0
    N = len(items)
    processed_count = 0
    # legacy accumulators when no writer_queue
    all_embs = []
    all_names = []
    all_lens = []

    while i < N:
        batch_items = []
        batch_max_len = 0
        while len(batch_items) < cur_bs and i < N:
            nm, seq = items[i]
            # normalize sequence: uppercase, remove spaces and '*' chars, then trim to max_length
            s = seq.upper().replace(' ', '').replace('*', '')[:max_length]
            batch_items.append((nm, s))
            if len(s) > batch_max_len:
                batch_max_len = len(s)
            i += 1
        if not batch_items:
            break
        try:
            data = [(nm, s) for nm, s in batch_items]
            _, _, batch_tokens = batch_converter(data)
            batch_tokens = batch_tokens.to(device)

            # choose precision/autocast
            if use_bf16 and device.type.startswith('cuda'):
                ctx = torch.autocast(device_type='cuda', dtype=torch.bfloat16)
            else:
                class _noop_ctx:
                    def __enter__(self):
                        return None
                    def __exit__(self, exc_type, exc, tb):
                        return False
                ctx = _noop_ctx()

            t0 = time.time()
            with torch.inference_mode():
                with ctx:
                    results = model(batch_tokens, repr_layers=[rep_layer], return_contacts=False)
            batch_time = time.time() - t0

            token_reprs = results['representations'][rep_layer]
            # process each sequence in batch
            for bi in range(token_reprs.size(0)):
                L = min(len(batch_items[bi][1]), max_length)
                seq_repr = token_reprs[bi, 1:L+1, :].mean(dim=0)
                if writer_queue is None:
                    # synchronous path: move to cpu and store numpy
                    cpu_vec = seq_repr.cpu().numpy().astype('float32')
                    all_embs.append(cpu_vec)
                    all_names.append(batch_items[bi][0])
                    all_lens.append(min(len(batch_items[bi][1]), max_length))
                else:
                    # async path: create pinned cpu tensor and enqueue with CUDA event
                    try:
                        pinned = torch.empty(seq_repr.shape, dtype=seq_repr.dtype, pin_memory=True)
                        # copy asynchronously to pinned memory
                        pinned.copy_(seq_repr, non_blocking=True)
                    except Exception:
                        # fallback to synchronous copy
                        pinned = seq_repr.cpu()
                    # record CUDA event so writer can wait without synchronizing whole device
                    try:
                        event = torch.cuda.Event()
                        torch.cuda.current_stream().record_event(event)
                    except Exception:
                        event = None
                    # enqueue (pinned_cpu_tensor, name, length, event)
                    # block if queue is full to provide backpressure
                    writer_queue.put((pinned, batch_items[bi][0], min(len(batch_items[bi][1]), max_length), event))
                    processed_count += 1

            # cleanup GPU allocations
            try:
                del token_reprs
                del results
                del batch_tokens
            except Exception:
                pass

            if writer_queue is None:
                processed_count += len(all_embs) - processed_count

            # free GPU cache lightly (avoid empty_cache per batch if writer_queue used)
            if device.type.startswith('cuda') and writer_queue is None:
                torch.cuda.empty_cache()
            gc.collect()

            # adapt batch size
            if memory_manager is not None:
                new_bs = memory_manager.adaptive_batch_size_adjustment(cur_bs, batch_time, len(batch_items))
                cur_bs = max(min_batch_size, min(new_bs, max_batch_size))
            else:
                if cur_bs * 2 <= max_batch_size:
                    cur_bs = min(cur_bs * 2, max_batch_size)
        except RuntimeError as e:
            msg = str(e).lower()
            if 'out of memory' in msg or 'oom' in msg:
                if device.type.startswith('cuda'):
                    torch.cuda.empty_cache()
                gc.collect()
                cur_bs = max(min_batch_size, cur_bs // 2)
                i -= len(batch_items)
                time.sleep(0.5)
            else:
                raise
    if writer_queue is None:
        return all_embs, all_names, all_lens
    return processed_count


def atomic_write_ckpt(ckpt_path, count):
    tmp = str(ckpt_path) + '.tmp'
    with open(tmp, 'w') as f:
        f.write(str(count))
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, str(ckpt_path))


def flush_write(h5path, new_embs, new_names, new_lens, dsize, h5_chunks=4096):
    if len(new_embs) == 0:
        return
    with h5py.File(str(h5path), 'a') as f:
        str_dt = h5py.string_dtype(encoding='utf-8')
        names_arr = np.array([n for n in new_names], dtype=object)
        new_embs_arr = np.array(new_embs, dtype='float32')
        if 'embeddings' not in f:
            maxshape = (None, dsize)
            # choose chunk rows aligned with typical buffer
            chunk_rows = min(h5_chunks, max(1, new_embs_arr.shape[0]))
            f.create_dataset('embeddings', data=new_embs_arr, compression='gzip', chunks=(chunk_rows, dsize), maxshape=maxshape)
            f.create_dataset('Seq_Name', data=names_arr, compression='gzip', chunks=(chunk_rows,), maxshape=(None,), dtype=str_dt)
            f.create_dataset('seq_len', data=np.array(new_lens, dtype='i4'), compression='gzip', chunks=(chunk_rows,), maxshape=(None,))
        else:
            f['embeddings'].resize((f['embeddings'].shape[0] + new_embs_arr.shape[0]), axis=0)
            f['embeddings'][-new_embs_arr.shape[0]:] = new_embs_arr
            f['Seq_Name'].resize((f['Seq_Name'].shape[0] + names_arr.shape[0]), axis=0)
            try:
                f['Seq_Name'][-names_arr.shape[0]:] = names_arr
            except Exception:
                f['Seq_Name'][-names_arr.shape[0]:] = [str(n) for n in new_names]
            f['seq_len'].resize((f['seq_len'].shape[0] + len(new_lens)), axis=0)
            f['seq_len'][-len(new_lens):] = np.array(new_lens, dtype='i4')
        f.flush()
        try:
            # best-effort fsync if available
            os.fsync(f.id.get_vfd_handle())
        except Exception:
            try:
                os.fsync(f.fileno())
            except Exception:
                pass


def writer_worker(h5path, q, dsize, ckpt_path, ckpt_interval, flush_threshold=512, h5_chunks=4096):
    """Writer thread: consumes (pinned_tensor, name, length, event) and writes to HDF5 in batches."""
    import math
    local_count = 0
    # initialize local_count from existing file
    if pathlib.Path(h5path).exists():
        try:
            with h5py.File(str(h5path), 'r') as f:
                local_count = f['embeddings'].shape[0]
        except Exception:
            local_count = 0
    emb_buf = []
    name_buf = []
    len_buf = []
    try:
        while True:
            item = q.get()
            if item is None:
                break
            pinned, name, slen, event = item
            # Wait for GPU ops that populated pinned to complete
            if event is not None:
                try:
                    event.synchronize()
                except Exception:
                    # fallback to global sync
                    torch.cuda.synchronize()
            # convert pinned tensor to numpy
            try:
                arr = pinned.numpy()
            except Exception:
                # if pinned is not tensor, coerce
                arr = np.array(pinned, dtype='float32')
            emb_buf.append(arr.astype('float32'))
            name_buf.append(name)
            len_buf.append(int(slen))

            # flush in batches
            if len(emb_buf) >= flush_threshold:
                flush_write(h5path, emb_buf, name_buf, len_buf, dsize, h5_chunks=h5_chunks)
                local_count += len(emb_buf)
                try:
                    atomic_write_ckpt(ckpt_path, local_count)
                except Exception:
                    with open(ckpt_path, 'w') as f:
                        f.write(str(local_count))
                emb_buf.clear(); name_buf.clear(); len_buf.clear()
    finally:
        # final flush
        if len(emb_buf) > 0:
            flush_write(h5path, emb_buf, name_buf, len_buf, dsize, h5_chunks=h5_chunks)
            local_count += len(emb_buf)
            try:
                atomic_write_ckpt(ckpt_path, local_count)
            except Exception:
                with open(ckpt_path, 'w') as f:
                    f.write(str(local_count))


def process_shard_stream(shard_path, out_h5_path, model, batch_converter, rep_layer, dsize,
                         buffer_size, initial_batch_size, max_batch_size, min_batch_size, ckpt_interval, test_limit, use_bf16=False):
    device = torch.device('cuda:0')
    # set cuda optimization 
    torch.backends.cudnn.benchmark = True
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True

    shard_path = pathlib.Path(shard_path)
    out_h5 = pathlib.Path(out_h5_path)
    ckpt_path = out_h5.with_suffix('.ckpt')

    # determine resume safety using both ckpt and existing h5 count
    h5_count = 0
    if out_h5.exists():
        try:
            with h5py.File(str(out_h5), 'r') as f:
                h5_count = f['embeddings'].shape[0]
        except Exception:
            h5_count = 0

    ckpt_count = 0
    if ckpt_path.exists():
        try:
            with open(ckpt_path, 'r') as cf:
                ckpt_count = int(cf.read().strip())
        except Exception:
            ckpt_count = 0

    # decide processed count (use HDF5 as source-of-truth if available)
    if h5_count > 0:
        processed = min(ckpt_count, h5_count) if ckpt_count > 0 else h5_count
    else:
        processed = ckpt_count

    # log resume decision
    if processed > 0:
        logger.info(f"Resuming shard {shard_path.name}: h5_count={h5_count}, ckpt_count={ckpt_count}, resuming_at={processed}")
    else:
        logger.debug(f"Starting shard {shard_path.name}: h5_count={h5_count}, ckpt_count={ckpt_count}")

    # use threaded reader to hide IO
    seq_iter = multi_threaded_fasta_reader(str(shard_path), buffer_size=buffer_size)

    # determine total sequences in shard for a meaningful per-shard tqdm (best-effort)
    total_in_shard = None
    try:
        # fast path: if h5 has count, trust that as upper bound
        if h5_count > 0:
            # try to get total by counting fasta only if counts disagree
            if ckpt_count != h5_count:
                total_in_shard = sum(1 for _ in stream_fasta_reader(str(shard_path)))
            else:
                total_in_shard = h5_count
        else:
            total_in_shard = sum(1 for _ in stream_fasta_reader(str(shard_path)))
    except Exception:
        total_in_shard = None

    # skip already processed entries from iterator (and log skipping)
    if processed > 0:
        logger.debug(f"Shard {shard_path.name}: skipping {processed} sequences from iterator")
    for _ in range(processed):
        try:
            next(seq_iter)
        except StopIteration:
            break

    enable_tqdm = os.environ.get('ENABLE_PER_SHARD_TQDM', '1') == '1'
    if enable_tqdm:
        if total_in_shard is not None and total_in_shard > 0:
            pbar = tqdm(total=total_in_shard, desc=f"Shard {shard_path.name}", unit='seq', initial=processed)
        else:
            pbar = tqdm(desc=f"Shard {shard_path.name}", unit='seq', initial=processed)
    else:
        pbar = None

    local_count = processed
    forward_time = 0.0
    start_time = time.time()

    # create memory manager and tensor pool
    memory_manager = PeakPerformanceMemoryManager(device_id=0)
    try:
        tensor_pool = TensorMemoryPool(device='cuda:0')
    except Exception:
        tensor_pool = TensorMemoryPool(device='cpu')

    # writer queue and thread: async copy -> writer pipeline to overlap GPU and IO
    writer_queue = queue.Queue(maxsize=max(8, buffer_size*4))
    writer_thread = threading.Thread(target=writer_worker, args=(str(out_h5), writer_queue, dsize, ckpt_path, ckpt_interval), daemon=True)
    writer_thread.start()

    # warmup model (small dummy forward) to reduce jitter
    try:
        if device.type.startswith('cuda'):
            warm_tokens = torch.zeros((2, 16), dtype=torch.long, device=device)
            with torch.inference_mode():
                _ = model(warm_tokens, repr_layers=[rep_layer])
            torch.cuda.synchronize()
    except Exception:
        pass

    seq_buffer = []
    try:
        for nm, seq in seq_iter:
            seq_buffer.append((nm, seq))
            if len(seq_buffer) >= buffer_size:
                # process buffer -> we will asynchronously copy results to CPU and let writer thread handle IO
                t0 = time.time()
                # adaptive_process_buffer will enqueue pinned CPU tensors to writer_queue
                processed = adaptive_process_buffer(seq_buffer, model, batch_converter, rep_layer, device,
                                                     max_length=1022,
                                                     initial_batch_size=initial_batch_size,
                                                     max_batch_size=max_batch_size,
                                                     min_batch_size=min_batch_size,
                                                     memory_manager=memory_manager,
                                                     use_bf16=use_bf16,
                                                     writer_queue=writer_queue)
                forward_time += (time.time() - t0)
                # writer thread will flush and update ckpt; we can poll queue size or rely on atomic ckpt written by writer
                local_count += processed
                if pbar is not None:
                    pbar.update(processed)
                seq_buffer.clear()
                if local_count % ckpt_interval == 0:
                    elapsed = time.time() - start_time
                    overall_throughput = local_count / elapsed if elapsed>0 else 0.0
                    gpu_forward_throughput = local_count / forward_time if forward_time>0 else 0.0
                    logger.info(f"{out_h5.name}: processed {local_count} seqs, elapsed {elapsed:.1f}s, overall {overall_throughput:.2f} seq/s, forward-only {gpu_forward_throughput:.2f} seq/s")
                if test_limit and local_count >= test_limit:
                    break
        # flush remaining
        if seq_buffer:
            t0 = time.time()
            processed = adaptive_process_buffer(seq_buffer, model, batch_converter, rep_layer, device,
                                                 max_length=1022,
                                                 initial_batch_size=initial_batch_size,
                                                 max_batch_size=max_batch_size,
                                                 min_batch_size=min_batch_size,
                                                 memory_manager=memory_manager,
                                                 use_bf16=use_bf16,
                                                 writer_queue=writer_queue)
            forward_time += (time.time() - t0)
            local_count += processed
            if pbar is not None:
                pbar.update(processed)
    finally:
        # signal writer thread to finish and join
        try:
            writer_queue.put(None)
        except Exception:
            pass
        try:
            writer_thread.join(timeout=60)
        except Exception:
            pass
        if pbar is not None:
            pbar.close()
    elapsed_total = time.time() - start_time
    overall_throughput = local_count / elapsed_total if elapsed_total>0 else 0.0
    gpu_forward_throughput = local_count / forward_time if forward_time>0 else 0.0
    logger.info(f"Completed shard {shard_path.name}: total processed={local_count}, elapsed {elapsed_total:.1f}s, overall {overall_throughput:.2f} seq/s, forward-only {gpu_forward_throughput:.2f} seq/s")


def worker_loop(gpu_id, shard_queue, tmp_dir, model_name, buffer_size, initial_batch_size, max_batch_size, min_batch_size, ckpt_interval, test_limit, use_bf16=False, use_compile=False):
    os.environ['CUDA_VISIBLE_DEVICES'] = str(gpu_id)
    lock_path = pathlib.Path(tmp_dir) / 'model_load.lock'
    acquired = False
    while not acquired:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode('utf-8'))
            os.close(fd)
            acquired = True
        except FileExistsError:
            time.sleep(random.uniform(0.5, 2.0))
    try:
        device = torch.device('cuda:0')
        model, batch_converter, rep_layer, dsize = load_model(model_name, device)
        if use_compile and hasattr(torch, 'compile'):
            try:
                model = torch.compile(model)
            except Exception as e:
                logger.warning(f"Warning: torch.compile failed: {e}")
        logger.info(f"GPU {gpu_id}: model loaded")
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
        logger.info(f"GPU {gpu_id} processing {shard_path}")
        try:
            process_shard_stream(shard_path, out_h5, model, batch_converter, rep_layer, dsize,
                                 buffer_size, initial_batch_size, max_batch_size, min_batch_size, ckpt_interval, test_limit, use_bf16=use_bf16)
        except RuntimeError as e:
            logger.exception(f"RuntimeError processing {shard_path} on GPU {gpu_id}: {e}")
        except Exception as e:
            logger.exception(f"Error processing {shard_path} on GPU {gpu_id}: {e}")


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
            try:
                fout['Seq_Name'][-names.shape[0]:] = names
            except Exception:
                fout['Seq_Name'][-names.shape[0]:] = [n.decode('utf-8') if isinstance(n, bytes) else str(n) for n in names]
            fout['seq_len'].resize((fout['seq_len'].shape[0] + lens.shape[0]), axis=0)
            fout['seq_len'][-lens.shape[0]:] = lens
    logger.info(f"Merged {len(shard_h5_paths)} shards into {final_h5}")


def monitor_progress(tmp_dir, total_seqs, workers, interval=1.0):
    """Efficiently monitor per-shard .ckpt files and show a single overall tqdm in the main process.
    Uses mtime-based caching to avoid re-reading unchanged files and displays only overall speed in the progress bar.
    """
    from tqdm import tqdm
    import time
    tmp = pathlib.Path(tmp_dir)
    pbar = tqdm(total=total_seqs, desc='Overall', unit='seq')
    seen = 0
    last_counts = {}  # path -> (count, mtime_ns)
    last_total = 0
    last_time = time.time()
    try:
        while any(p.is_alive() for p in workers):
            total_embedded = 0
            now = time.time()
            for ck in tmp.glob('shard_*.ckpt'):
                try:
                    st = ck.stat()
                    mtime = st.st_mtime_ns
                    key = str(ck)
                    if key in last_counts and last_counts[key][1] == mtime:
                        cnt = last_counts[key][0]
                    else:
                        try:
                            cnt = int(ck.read_text().strip())
                        except Exception:
                            # fallback: if ckpt unreadable, try corresponding h5
                            h5p = ck.with_suffix('.h5')
                            if h5p.exists():
                                try:
                                    with h5py.File(str(h5p), 'r') as f:
                                        cnt = f['embeddings'].shape[0]
                                except Exception:
                                    cnt = 0
                            else:
                                cnt = 0
                        last_counts[key] = (cnt, mtime)
                    total_embedded += cnt
                except Exception:
                    continue

            if total_embedded > seen:
                pbar.update(total_embedded - seen)
                seen = total_embedded

            # compute recent speed (seq/s) over this interval
            dt = now - last_time if now > last_time else 1.0
            speed = (total_embedded - last_total) / dt if dt > 0 else 0.0
            pbar.set_postfix({'speed': f'{speed:.2f} seq/s'})
            pbar.refresh()

            last_total = total_embedded
            last_time = now
            time.sleep(interval)

        # final collect
        total_embedded = 0
        for ck in tmp.glob('shard_*.ckpt'):
            try:
                key = str(ck)
                st = ck.stat()
                mtime = st.st_mtime_ns
                if key in last_counts and last_counts[key][1] == mtime:
                    cnt = last_counts[key][0]
                else:
                    try:
                        cnt = int(ck.read_text().strip())
                    except Exception:
                        h5p = ck.with_suffix('.h5')
                        if h5p.exists():
                            try:
                                with h5py.File(str(h5p), 'r') as f:
                                    cnt = f['embeddings'].shape[0]
                            except Exception:
                                cnt = 0
                        else:
                            cnt = 0
                    last_counts[key] = (cnt, mtime)
                total_embedded += cnt
            except Exception:
                continue

        if total_embedded > seen:
            pbar.update(total_embedded - seen)
            seen = total_embedded

    finally:
        pbar.close()



def count_sequences(fasta_path):
    c = 0
    for _ in stream_fasta_reader(fasta_path):
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
    parser.add_argument('--buffer-size', type=int, default=100000, help='number of sequences buffered per write (larger favors throughput)')
    parser.add_argument('--initial-batch-size', type=int, default=128)
    parser.add_argument('--max-batch-size', type=int, default=512)
    parser.add_argument('--min-batch-size', type=int, default=4)
    parser.add_argument('--model', default='ESM2_3B')
    parser.add_argument('--tmp-dir', default='../../tmp_emb_large', help='temporary dir for shards and shard outputs')
    parser.add_argument('--ckpt-interval', type=int, default=4096)
    parser.add_argument('--merge', action='store_true')
    parser.add_argument('--test-limit', type=int, default=0)
    parser.add_argument('--workers', type=int, default=0, help='number of worker processes (default = number of GPUs)')
    parser.add_argument('--bf16', action='store_true')
    parser.add_argument('--compile', action='store_true')
    parser.add_argument('--target-gpu-memory', type=float, default=70, help='GB target memory for batch estimation')
    args = parser.parse_args()

    input_fasta = pathlib.Path(args.input)
    tmp_dir = pathlib.Path(args.tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # prepare shards
    logger.info('prepare shards')
    existing_shard_paths = sorted(list(tmp_dir.glob('shard_*.fasta')))
    if len(existing_shard_paths) > 0:
        shards = [str(p) for p in existing_shard_paths]
    else:
        shards = split_fasta(input_fasta, tmp_dir, args.shard_size)

    # derive GPU list
    gpu_list = [int(x) for x in args.gpus.split(',') if x.strip()!='']
    if len(gpu_list) == 0:
        logger.error('No GPUs specified')
        raise SystemExit(1)
    num_workers = args.workers if args.workers>0 else len(gpu_list)
    logger.info(f'found gpus:{gpu_list}')

    # determine which shards are unfinished (multithreaded check with progress bar)
    logger.info('check shards status')
    unfinished = []

    def _check_shard(idx, shard):
        shard_path = pathlib.Path(shard)
        out_h5 = pathlib.Path(tmp_dir) / f'shard_{idx}.h5'
        ckpt_path = out_h5.with_suffix('.ckpt')
        # try to get total count in shard (best-effort)
        total_in_shard = None
        try:
            total_in_shard = sum(1 for _ in stream_fasta_reader(shard))
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
        skip = (total_in_shard is not None and processed >= total_in_shard and total_in_shard>0)
        return (idx, str(shard_path), processed, total_in_shard, skip)

    max_workers = min(len(shards), max(1, (os.cpu_count() or 4) * 2, 4, 8))
    try:
        pbar = tqdm(total=len(shards), desc='Checking shards', unit='shard')
    except Exception:
        pbar = None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_check_shard, idx, shard): idx for idx, shard in enumerate(shards)}
        for fut in concurrent.futures.as_completed(futures):
            try:
                idx, shard_path_str, processed, total_in_shard, skip = fut.result()
            except Exception as e:
                logger.exception(f"Error checking shard {futures.get(fut, '?')}: {e}")
                # fallback conservative behavior: mark shard as unfinished
                idx = futures.get(fut)
                shard_path_str = shards[idx] if idx is not None else None
                processed = 0
                total_in_shard = None
                skip = False
            if pbar is not None:
                pbar.update(1)
            if not skip:
                unfinished.append((idx, shard_path_str))
    if pbar is not None:
        pbar.close()

    if len(unfinished) == 0:
        logger.info('All shards complete')
        if args.merge:
            # load a model to get dsize
            model_tmp, _, _, dsize = load_model(args.model, torch.device('cpu'))
            shard_h5_paths = [str(pathlib.Path(tmp_dir)/f'shard_{i}.h5') for i in range(len(shards))]
            existing_shards = [p for p in shard_h5_paths if pathlib.Path(p).exists()]
            if existing_shards:
                merge_shards(existing_shards, args.output, dsize)
        raise SystemExit(0)
    else:
        logger.info(f'unfinished shards num: {len(unfinished)}')

    # compute total sequences for overall progress bar
    total_seqs = count_sequences(input_fasta)

    # prepare manager queue
    manager = Manager()
    shard_queue = manager.Queue()
    for idx, shard in unfinished:
        shard_queue.put((idx, shard))

    # disable per-shard tqdm in worker processes to avoid flickering
    os.environ['ENABLE_PER_SHARD_TQDM'] = '0'

    workers = []
    for w in range(num_workers):
        gpu_id = gpu_list[w % len(gpu_list)]
        p = Process(target=worker_loop, args=(gpu_id, shard_queue, str(tmp_dir), args.model, args.buffer_size, args.initial_batch_size, args.max_batch_size, args.min_batch_size, args.ckpt_interval, args.test_limit, args.bf16, args.compile))
        p.start()
        workers.append(p)

    # monitor overall progress in main process
    try:
        monitor_progress(str(tmp_dir), total_seqs, workers, interval=1.0)
    except KeyboardInterrupt:
        logger.warning('Interrupted monitoring')

    for p in workers:
        p.join()

    # merging
    shard_h5_paths = [str(pathlib.Path(tmp_dir)/f'shard_{i}.h5') for i in range(len(shards))]
    existing_shards = [p for p in shard_h5_paths if pathlib.Path(p).exists()]
    model_tmp, _, _, dsize = load_model(args.model, torch.device('cpu'))
    if args.merge:
        if len(existing_shards)==0:
            logger.warning('No shard outputs to merge')
        else:
            merge_shards(existing_shards, args.output, dsize)
    else:
        logger.info('Shard outputs:')
        for s in existing_shards:
            logger.info(s)

    # Final integrity check: compare input FASTA sequence count with sum of per-shard ckpt counts (or HDF5 counts)
    total_seqs = count_sequences(input_fasta)
    # try summing .ckpt files as they are written atomically during processing
    total_embedded = 0
    ckpt_files = sorted(pathlib.Path(tmp_dir).glob('shard_*.ckpt'))
    if ckpt_files:
        for ck in ckpt_files:
            try:
                with open(ck, 'r') as cf:
                    total_embedded += int(cf.read().strip())
            except Exception:
                # fallback: try reading HDF5 if available
                h5p = ck.with_suffix('.h5')
                if h5p.exists():
                    try:
                        with h5py.File(str(h5p), 'r') as f:
                            total_embedded += f['embeddings'].shape[0]
                    except Exception:
                        pass
    else:
        # no ckpt files; try summing HDF5 files
        for h5p in existing_shards:
            try:
                with h5py.File(str(h5p), 'r') as f:
                    total_embedded += f['embeddings'].shape[0]
            except Exception:
                pass

    est = estimate_size(total_seqs, dsize)
    logger.info(f'Estimated final size (bytes): {int(est)}')

    if total_embedded != 0:
        if total_embedded == total_seqs:
            logger.info(f'Integrity check OK: input sequences = {total_seqs}, embedded = {total_embedded}')
        else:
            logger.warning(f'WARNING: mismatch detected: input sequences = {total_seqs}, embedded = {total_embedded}')
            logger.warning('Possible causes: empty FASTA entries skipped, writer thread interrupted before final flush, or race conditions in final ckpt/h5 writes.')
            logger.warning(f'Inspect per-shard .ckpt and .h5 files in {tmp_dir}')
    else:
        logger.warning(f'Could not determine embedded count from shard .ckpt or .h5 files; please inspect outputs in {tmp_dir}')



'''
cd HDD_POOL/xlh/gene_database/ERAST/pipeline
screen -S emb_1B
conda activate esmfold
python emb_optimized.py -i ../../subcatalogs_GMGC10.100nr.faa -o emb_1B.h5 --merge --bf16 --gpus 0,1,2,3,4,5,6,7
'''