import argparse
import torch
import esm
import numpy as np
import h5py
from tqdm import tqdm
import sys
import gc
import os
import time
from typing import List, Tuple, Optional
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_gpu_memory():
    """获取GPU内存使用情况"""
    if torch.cuda.is_available():
        free_memory, total_memory = torch.cuda.mem_get_info()
        free_memory_gb = free_memory / (1024**3)
        total_memory_gb = total_memory / (1024**3)
        used_memory_gb = total_memory_gb - free_memory_gb
        return {
            'free_gb': free_memory_gb,
            'total_gb': total_memory_gb,
            'used_gb': used_memory_gb,
            'usage_percent': (used_memory_gb / total_memory_gb) * 100
        }
    return None

def estimate_batch_size_by_length(max_seq_len: int, model_hidden_dim: int, 
                                  target_gpu_memory_gb: float = 4.0) -> int:
    """
    根据序列长度估算合适的批次大小
    
    Args:
        max_seq_len: 批次中最长序列的长度
        model_hidden_dim: 模型隐藏层维度
        target_gpu_memory_gb: 目标GPU内存使用量(GB)
    
    Returns:
        估算的批次大小
    """
    # 估算一个token占用的内存(bytes)
    # float32: 4 bytes, 加上激活和梯度的大致估计
    bytes_per_token = 4  # 基础存储
    bytes_per_hidden_dim = 4  # 每个隐藏维度的存储
    
    # 内存占用估算公式
    # 输入token: batch_size * seq_len * bytes_per_token
    # 中间激活: 大致是输入的10-20倍
    # 输出嵌入: batch_size * seq_len * hidden_dim * bytes_per_hidden_dim
    
    seq_len_with_special = max_seq_len + 2  # 加上[CLS]和[EOS]
    
    # 总内存估算(字节)
    input_memory = seq_len_with_special * bytes_per_token
    hidden_memory = seq_len_with_special * model_hidden_dim * bytes_per_hidden_dim
    intermediate_memory = input_memory * 15  # 中间激活的估算倍数
    
    total_memory_per_seq = (input_memory + hidden_memory + intermediate_memory) / (1024**3)  # 转换为GB
    
    # 计算批次大小
    batch_size = int(target_gpu_memory_gb / total_memory_per_seq)
    
    # 确保批次大小合理
    batch_size = max(1, min(batch_size, 512))  # 限制在1-512之间
    
    logger.info(f"Estimated batch size: {batch_size} for max_seq_len={max_seq_len}, "
                f"memory_per_seq={total_memory_per_seq:.4f}GB")
    
    return batch_size

def adaptive_batch_processor(seq_items: List[Tuple[str, str]], 
                            model, 
                            alphabet, 
                            device='cuda',
                            max_length: int = 1022,
                            rep_layer: int = 33,
                            initial_batch_size: int = 32,
                            max_batch_size: int = 256,
                            min_batch_size: int = 4,
                            memory_safety_factor: float = 0.8) -> Tuple[List[np.ndarray], List[str], List[int]]:
    """
    自适应批次处理器，根据内存使用动态调整批次大小
    
    Args:
        seq_items: 序列列表，每个元素为(name, sequence)
        model: ESM模型
        alphabet: ESM字母表
        device: 计算设备
        max_length: 最大序列长度
        rep_layer: 要提取的表示层
        initial_batch_size: 初始批次大小
        max_batch_size: 最大批次大小
        min_batch_size: 最小批次大小
        memory_safety_factor: 内存安全因子(0-1)
    
    Returns:
        嵌入向量列表，序列名列表，序列长度列表
    """
    batch_converter = alphabet.get_batch_converter()
    
    all_embeddings = []
    all_names = []
    all_lengths = []
    
    # 按序列长度排序，提高填充效率
    seq_items_sorted = sorted(seq_items, key=lambda x: len(x[1]))
    
    current_batch_size = initial_batch_size
    successful_batches = 0
    failed_batches = 0
    
    i = 0
    total_items = len(seq_items_sorted)
    
    with tqdm(total=total_items, desc="Adaptive batch processing", unit="seq") as pbar:
        while i < total_items:
            batch_items = []
            batch_max_len = 0
            
            # 收集当前批次
            while len(batch_items) < current_batch_size and i < total_items:
                name, seq = seq_items_sorted[i]
                seq = seq.upper().replace(' ', '')
                seq_len = min(len(seq), max_length)
                
                if seq_len > batch_max_len:
                    batch_max_len = seq_len
                
                batch_items.append((name, seq[:seq_len]))
                i += 1
            
            if not batch_items:
                break
            
            try:
                # 准备批次数据
                batch_labels = [item[0] for item in batch_items]
                batch_seqs = [item[1] for item in batch_items]
                batch_lengths = [len(seq) for seq in batch_seqs]
                
                # 转换为模型输入
                data = list(zip(batch_labels, batch_seqs))
                _, _, batch_tokens = batch_converter(data)
                batch_tokens = batch_tokens.to(device)
                
                # 模型推理
                with torch.no_grad():
                    results = model(batch_tokens, repr_layers=[rep_layer])
                
                # 提取并池化
                token_reprs = results["representations"][rep_layer]
                
                for j in range(token_reprs.size(0)):
                    seq_repr = token_reprs[j, 1:batch_lengths[j]+1, :]
                    mean_repr = seq_repr.mean(dim=0).cpu().numpy()
                    all_embeddings.append(mean_repr)
                
                all_names.extend(batch_labels)
                all_lengths.extend(batch_lengths)
                
                # 成功，尝试增加批次大小
                successful_batches += 1
                if successful_batches >= 3 and current_batch_size < max_batch_size:
                    # 检查GPU内存
                    gpu_info = get_gpu_memory()
                    if gpu_info and gpu_info['free_gb'] > 2.0:  # 至少有2GB空闲内存
                        new_batch_size = min(current_batch_size * 2, max_batch_size)
                        if new_batch_size != current_batch_size:
                            logger.info(f"Increasing batch size from {current_batch_size} to {new_batch_size}")
                            current_batch_size = new_batch_size
                        successful_batches = 0
                
                pbar.update(len(batch_items))
                
            except torch.cuda.OutOfMemoryError:
                # 内存不足，减少批次大小
                logger.warning(f"GPU OOM with batch_size={current_batch_size}, reducing...")
                
                # 清理GPU缓存
                torch.cuda.empty_cache()
                gc.collect()
                
                # 重置索引，重新处理这批数据
                i -= len(batch_items)
                
                # 减少批次大小
                current_batch_size = max(min_batch_size, current_batch_size // 2)
                logger.info(f"Reduced batch size to {current_batch_size}")
                
                failed_batches += 1
                successful_batches = 0
                
                # 如果连续失败多次，暂停一下
                if failed_batches >= 3:
                    logger.warning("Multiple OOM errors, waiting for GPU memory to free up...")
                    time.sleep(5)
                    failed_batches = 0
    
    logger.info(f"Final batch size: {current_batch_size}")
    return all_embeddings, all_names, all_lengths

def stream_fasta_reader(fasta_path: str, max_sequences: int = None):
    """流式读取FASTA，避免一次性加载到内存"""
    current_name = ""
    current_seq = []
    count = 0
    
    with open(fasta_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            if line.startswith('>'):
                # 保存前一条序列
                if current_name:
                    seq_str = ''.join(current_seq)
                    if seq_str:  # 确保序列不为空
                        yield current_name, seq_str
                        count += 1
                        if max_sequences and count >= max_sequences:
                            return
                
                # 开始新序列
                current_name = line[1:].split()[0]  # 只取ID，去掉描述
                current_seq = []
            else:
                current_seq.append(line)
        
        # 最后一条序列
        if current_name and current_seq:
            seq_str = ''.join(current_seq)
            if seq_str:
                yield current_name, seq_str

def scan_fasta_for_stats(fasta_path: str, sample_size: int = 10000) -> dict:
    """
    扫描FASTA文件获取统计信息
    
    Args:
        fasta_path: FASTA文件路径
        sample_size: 采样大小
    
    Returns:
        包含统计信息的字典
    """
    logger.info(f"Scanning FASTA file for statistics (sampling {sample_size} sequences)...")
    
    lengths = []
    total_seqs = 0
    
    for i, (name, seq) in enumerate(stream_fasta_reader(fasta_path)):
        lengths.append(len(seq))
        total_seqs += 1
        
        if i >= sample_size - 1:
            break
    
    if not lengths:
        return {
            'total_sequences': 0,
            'avg_length': 0,
            'max_length': 0,
            'min_length': 0
        }
    
    stats = {
        'total_sequences': total_seqs,
        'avg_length': np.mean(lengths),
        'max_length': np.max(lengths),
        'min_length': np.min(lengths),
        'median_length': np.median(lengths),
        'std_length': np.std(lengths)
    }
    
    logger.info(f"Sample statistics: avg_len={stats['avg_length']:.1f}, "
                f"max_len={stats['max_length']}, min_len={stats['min_length']}")
    
    return stats

def main(args):
    """主函数"""
    
    # 设置GPU
    if args.gpuid and torch.cuda.is_available():
        torch.cuda.set_device(int(args.gpuid))
        device = torch.device(f"cuda:{args.gpuid}")
        gpu_info = get_gpu_memory()
        if gpu_info:
            logger.info(f"Using GPU {args.gpuid}: {gpu_info['total_gb']:.1f}GB total, "
                       f"{gpu_info['free_gb']:.1f}GB free")
    else:
        device = torch.device("cpu")
        logger.warning("Using CPU - this will be very slow!")
    
    # 扫描FASTA文件获取统计信息
    stats = scan_fasta_for_stats(args.input, args.sample_size)
    
    if stats['total_sequences'] == 0:
        logger.error("No sequences found in FASTA file")
        sys.exit(1)
    
    # 加载模型
    logger.info(f"Loading model: {args.model}")
    if args.model == 'ESM1b':
        rep_layer = 33
        model, alphabet = esm.pretrained.esm1b_t33_650M_UR50S()
        hidden_dim = 1280
    elif args.model == 'ESM2_3B':
        rep_layer = 36
        model, alphabet = esm.pretrained.esm2_t36_3B_UR50D()
        hidden_dim = 2560
    elif args.model == 'ESM2_650M':
        rep_layer = 33
        model, alphabet = esm.pretrained.esm2_t33_650M_UR50D()
        hidden_dim = 1280
    else:
        logger.error(f"Unknown model: {args.model}")
        sys.exit(1)
    
    model.to(device)
    model.eval()  # 设置为评估模式
    
    # 根据统计信息计算初始批次大小
    max_seq_len = min(stats['max_length'], args.max_length)
    target_gpu_memory_gb = args.target_gpu_memory
    
    if args.auto_batch_size:
        initial_batch_size = estimate_batch_size_by_length(
            max_seq_len, hidden_dim, target_gpu_memory_gb
        )
    else:
        initial_batch_size = args.batch_size
    
    logger.info(f"Initial batch size: {initial_batch_size}")
    logger.info(f"Max sequence length: {max_seq_len}")
    
    # 创建HDF5文件
    output_path = args.output if args.output else 'embeddings.h5'
    logger.info(f"Output will be saved to: {output_path}")
    
    with h5py.File(output_path, 'w') as h5_file:
        # 预创建可扩展的数据集
        max_shape = (None, hidden_dim)
        dset_emb = h5_file.create_dataset(
            'embeddings', 
            shape=(0, hidden_dim),
            maxshape=max_shape,
            dtype='float32',
            compression="gzip", 
            chunks=(1024, hidden_dim)
        )
        
        dset_names = h5_file.create_dataset(
            'Seq_Name', 
            shape=(0,), 
            maxshape=(None,), 
            dtype=h5py.string_dtype(encoding='utf-8'),
            compression="gzip", 
            chunks=(1024,)
        )
        
        dset_lengths = h5_file.create_dataset(
            'seq_len', 
            shape=(0,), 
            maxshape=(None,), 
            dtype='int32',
            compression="gzip", 
            chunks=(1024,)
        )
        
        # 创建统计数据组
        stats_group = h5_file.create_group('statistics')
        stats_group.attrs['model'] = args.model
        stats_group.attrs['total_sequences_processed'] = 0
        stats_group.attrs['avg_sequence_length'] = stats['avg_length']
        stats_group.attrs['max_sequence_length'] = stats['max_length']
        
        # 批处理参数
        buffer_size = args.buffer_size
        processed_count = 0
        batch_count = 0
        total_processing_time = 0
        
        # 流式处理
        seq_buffer = []
        fasta_reader = stream_fasta_reader(args.input, args.max_sequences)
        
        with tqdm(desc="Processing sequences", unit="seq") as pbar:
            for name, seq in fasta_reader:
                seq_buffer.append((name, seq))
                
                # 当缓冲区达到指定大小时，进行处理
                if len(seq_buffer) >= buffer_size:
                    start_time = time.time()
                    
                    # 使用自适应批次处理
                    embeddings, names, lengths = adaptive_batch_processor(
                        seq_buffer, 
                        model, 
                        alphabet, 
                        device, 
                        args.max_length, 
                        rep_layer,
                        initial_batch_size=initial_batch_size,
                        max_batch_size=args.max_batch_size,
                        min_batch_size=args.min_batch_size,
                        memory_safety_factor=args.memory_safety_factor
                    )
                    
                    # 保存到HDF5
                    if embeddings:
                        current_size = dset_emb.shape[0]
                        new_size = current_size + len(embeddings)
                        
                        dset_emb.resize((new_size, hidden_dim))
                        dset_names.resize((new_size,))
                        dset_lengths.resize((new_size,))
                        
                        dset_emb[current_size:new_size] = np.array(embeddings, dtype='float32')
                        dset_names[current_size:new_size] = names
                        dset_lengths[current_size:new_size] = lengths
                        
                        processed_count += len(embeddings)
                        batch_count += 1
                        
                        # 更新进度
                        batch_processing_time = time.time() - start_time
                        total_processing_time += batch_processing_time
                        
                        if batch_count % 10 == 0:
                            avg_time_per_seq = total_processing_time / processed_count
                            remaining_seqs = args.max_sequences - processed_count if args.max_sequences else 0
                            remaining_time = avg_time_per_seq * remaining_seqs if remaining_seqs > 0 else 0
                            
                            logger.info(f"Processed {processed_count} sequences, "
                                      f"batch {batch_count}, "
                                      f"{avg_time_per_seq*1000:.2f}ms/seq, "
                                      f"ETA: {remaining_time/3600:.2f}h")
                    
                    # 清空缓冲区
                    seq_buffer.clear()
                    pbar.update(len(embeddings))
                    
                    # 清理内存
                    torch.cuda.empty_cache()
                    gc.collect()
            
            # 处理剩余的序列
            if seq_buffer:
                start_time = time.time()
                
                embeddings, names, lengths = adaptive_batch_processor(
                    seq_buffer, 
                    model, 
                    alphabet, 
                    device, 
                    args.max_length, 
                    rep_layer,
                    initial_batch_size=initial_batch_size,
                    max_batch_size=args.max_batch_size,
                    min_batch_size=args.min_batch_size,
                    memory_safety_factor=args.memory_safety_factor
                )
                
                if embeddings:
                    current_size = dset_emb.shape[0]
                    new_size = current_size + len(embeddings)
                    
                    dset_emb.resize((new_size, hidden_dim))
                    dset_names.resize((new_size,))
                    dset_lengths.resize((new_size,))
                    
                    dset_emb[current_size:new_size] = np.array(embeddings, dtype='float32')
                    dset_names[current_size:new_size] = names
                    dset_lengths[current_size:new_size] = lengths
                    
                    processed_count += len(embeddings)
                    pbar.update(len(embeddings))
        
        # 更新统计信息
        stats_group.attrs['total_sequences_processed'] = processed_count
        stats_group.attrs['embedding_dimension'] = hidden_dim
        
        # 性能统计
        avg_time_per_seq = total_processing_time / processed_count if processed_count > 0 else 0
        logger.info(f"Completed! Total sequences processed: {processed_count}")
        logger.info(f"Embeddings shape: {dset_emb.shape}")
        logger.info(f"Average processing time: {avg_time_per_seq*1000:.2f}ms per sequence")
        logger.info(f"Total processing time: {total_processing_time/3600:.2f} hours")
        logger.info(f"Output saved to: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Adaptive ESM Embedding Generator for Large FASTA Files')
    parser.add_argument('-i', '--input', help='Path to input FASTA file', required=True)
    parser.add_argument('-o', '--output', help='Output HDF5 file path', default='autoBatch.h5')
    parser.add_argument('-m', '--model', help='Pre-trained model to use', 
                       choices=['ESM1b', 'ESM2_3B', 'ESM2_650M'], default='ESM2_650M')
    parser.add_argument('-g', '--gpuid', help='GPU ID to use', default='0')
    
    # 批次大小参数
    parser.add_argument('-b', '--batch_size', help='Initial batch size for model inference', 
                       type=int, default=32)
    parser.add_argument('--max_batch_size', help='Maximum batch size', 
                       type=int, default=256)
    parser.add_argument('--min_batch_size', help='Minimum batch size', 
                       type=int, default=4)
    parser.add_argument('--auto_batch_size', help='Automatically determine batch size', 
                       action='store_true', default=True)
    parser.add_argument('--target_gpu_memory', help='Target GPU memory usage in GB', 
                       type=float, default=4.0)
    
    # 内存和安全参数
    parser.add_argument('--memory_safety_factor', help='Memory safety factor (0-1)', 
                       type=float, default=0.8)
    
    # 数据处理参数
    parser.add_argument('-B', '--buffer_size', help='Buffer size for sequence accumulation', 
                       type=int, default=5120)
    parser.add_argument('-L', '--max_length', help='Maximum sequence length to process', 
                       type=int, default=1022)
    parser.add_argument('-M', '--max_sequences', help='Maximum number of sequences to process', 
                       type=int, default=None)
    parser.add_argument('--sample_size', help='Number of sequences to sample for statistics', 
                       type=int, default=10000)
    
    args = parser.parse_args()
    
    # 检查输入文件
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)
    
    # 验证参数
    if args.batch_size < args.min_batch_size:
        args.batch_size = args.min_batch_size
    if args.batch_size > args.max_batch_size:
        args.batch_size = args.max_batch_size
    
    logger.info(f"Starting adaptive batch processing with:")
    logger.info(f"  Model: {args.model}")
    logger.info(f"  Input: {args.input}")
    logger.info(f"  Batch size range: {args.min_batch_size}-{args.max_batch_size}")
    logger.info(f"  Auto batch size: {args.auto_batch_size}")
    logger.info(f"  Target GPU memory: {args.target_gpu_memory}GB")
    
    main(args)

   # python gen_emb_parrallel.py -i SMAGs.fasta -m ESM2_3B --auto_batch_size