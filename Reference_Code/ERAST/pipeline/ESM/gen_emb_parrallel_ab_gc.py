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
import psutil
import threading
import queue
import warnings
from collections import deque
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')
logger = logging.getLogger(__name__)

# 忽略警告
warnings.filterwarnings('ignore')

@dataclass
class MemoryStats:
    """内存统计"""
    gpu_allocated_gb: float = 0.0
    gpu_reserved_gb: float = 0.0
    gpu_free_gb: float = 0.0
    gpu_peak_gb: float = 0.0
    system_available_gb: float = 0.0
    system_used_percent: float = 0.0
    timestamp: float = 0.0

class PeakPerformanceMemoryManager:
    """峰值性能内存管理器 - 确保GPU始终以最高效率运行"""
    
    def __init__(self, device_id=0, target_utilization=0.85):
        self.device_id = device_id
        self.target_utilization = target_utilization  # 目标GPU利用率
        self.last_cleanup_time = time.time()
        self.cleanup_interval = 30  # 清理间隔(秒)
        self.sequence_counter = 0
        self.batch_history = deque(maxlen=100)  # 记录最近批次信息
        self.performance_history = deque(maxlen=50)  # 性能历史
        self.optimal_batch_size = None
        self.memory_stats_history = deque(maxlen=100)
        
        # 自动垃圾回收线程
        self.cleanup_thread = None
        self.stop_cleanup = threading.Event()
        self.cleanup_queue = queue.Queue()
        
        logger.info(f"初始化峰值性能内存管理器 (目标利用率: {target_utilization*100:.0f}%)")
    
    def start_auto_cleanup(self):
        """启动自动垃圾回收线程"""
        self.cleanup_thread = threading.Thread(
            target=self._auto_cleanup_worker,
            name="AutoCleanup",
            daemon=True
        )
        self.cleanup_thread.start()
        logger.info("启动自动垃圾回收线程")
    
    def stop_auto_cleanup(self):
        """停止自动垃圾回收"""
        self.stop_cleanup.set()
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
    
    def _auto_cleanup_worker(self):
        """自动垃圾回收工作线程"""
        while not self.stop_cleanup.is_set():
            try:
                # 检查是否需要清理
                current_time = time.time()
                if current_time - self.last_cleanup_time > self.cleanup_interval:
                    stats = self.get_memory_stats()
                    
                    # 如果GPU利用率过低或过高，进行优化
                    if stats and stats.gpu_free_gb > 4.0:  # 空闲内存太多
                        self.optimize_for_speed("underutilized")
                    elif stats and stats.gpu_free_gb < 0.5:  # 内存紧张
                        self.optimize_for_memory("overutilized")
                    
                    self.last_cleanup_time = current_time
                
                time.sleep(1)  # 每秒检查一次
                
            except Exception as e:
                logger.error(f"自动清理线程错误: {e}")
    
    def get_memory_stats(self) -> Optional[MemoryStats]:
        """获取当前内存统计"""
        if not torch.cuda.is_available():
            return None
        
        try:
            # GPU内存
            free, total = torch.cuda.mem_get_info(self.device_id)
            allocated = torch.cuda.memory_allocated(self.device_id)
            reserved = torch.cuda.memory_reserved(self.device_id)
            max_allocated = torch.cuda.max_memory_allocated(self.device_id)
            
            # 系统内存
            system_memory = psutil.virtual_memory()
            
            stats = MemoryStats(
                gpu_allocated_gb=allocated / (1024**3),
                gpu_reserved_gb=reserved / (1024**3),
                gpu_free_gb=free / (1024**3),
                gpu_peak_gb=max_allocated / (1024**3),
                system_available_gb=system_memory.available / (1024**3),
                system_used_percent=system_memory.percent,
                timestamp=time.time()
            )
            
            self.memory_stats_history.append(stats)
            return stats
            
        except Exception as e:
            logger.debug(f"获取内存统计失败: {e}")
            return None
    
    def optimize_for_speed(self, reason="periodic"):
        """
        为速度优化 - 当GPU利用率不足时调用
        返回: 清理释放的内存(GB)
        """
        logger.debug(f"速度优化触发: {reason}")
        
        before_stats = self.get_memory_stats()
        if not before_stats:
            return 0.0
        
        # 轻度清理策略
        freed_memory = 0.0
        
        # 1. 清理Python垃圾
        gc.collect()
        
        # 2. 清空CUDA缓存
        torch.cuda.empty_cache()
        
        # 3. 重置内存统计
        torch.cuda.reset_peak_memory_stats()
        
        after_stats = self.get_memory_stats()
        if after_stats:
            freed_memory = before_stats.gpu_allocated_gb - after_stats.gpu_allocated_gb
        
        logger.debug(f"速度优化完成, 释放 {freed_memory:.2f}GB 内存")
        return freed_memory
    
    def optimize_for_memory(self, reason="low_memory"):
        """
        为内存优化 - 当内存紧张时调用
        返回: 清理释放的内存(GB)
        """
        logger.info(f"内存优化触发: {reason}")
        
        before_stats = self.get_memory_stats()
        if not before_stats:
            return 0.0
        
        freed_memory = 0.0
        
        # 激进清理策略
        for _ in range(3):  # 多次尝试
            # 1. 强制垃圾回收
            for _ in range(3):
                gc.collect()
            
            # 2. 清空所有缓存
            torch.cuda.empty_cache()
            
            # 3. 重置设备内存
            torch.cuda.reset_peak_memory_stats()
            
            # 4. 等待GPU同步
            torch.cuda.synchronize()
        
        after_stats = self.get_memory_stats()
        if after_stats:
            freed_memory = before_stats.gpu_allocated_gb - after_stats.gpu_allocated_gb
        
        logger.info(f"内存优化完成, 释放 {freed_memory:.2f}GB 内存")
        return freed_memory
    
    def adaptive_batch_size_adjustment(self, 
                                     current_batch_size: int,
                                     batch_processing_time: float,
                                     sequences_processed: int) -> int:
        """
        自适应调整批次大小
        基于历史性能数据智能调整
        """
        # 记录批次性能
        self.batch_history.append({
            'batch_size': current_batch_size,
            'processing_time': batch_processing_time,
            'sequences': sequences_processed,
            'time_per_seq': batch_processing_time / sequences_processed if sequences_processed > 0 else 0
        })
        
        if len(self.batch_history) < 5:
            return current_batch_size
        
        # 分析最近批次性能
        recent_batches = list(self.batch_history)[-5:]
        avg_time_per_seq = np.mean([b['time_per_seq'] for b in recent_batches])
        
        # 如果平均时间增加，可能需要调整批次大小
        if len(self.performance_history) > 0:
            last_avg_time = self.performance_history[-1]
            time_change = avg_time_per_seq - last_avg_time
            
            # 性能下降超过20%，调整批次大小
            if time_change > last_avg_time * 0.2:
                new_batch_size = max(4, int(current_batch_size * 0.8))
                logger.info(f"性能下降 {time_change*1000:.1f}ms/seq, 调整批次: {current_batch_size} -> {new_batch_size}")
                return new_batch_size
        
        self.performance_history.append(avg_time_per_seq)
        
        # 检查内存使用，决定是否可以增加批次
        stats = self.get_memory_stats()
        if stats and stats.gpu_free_gb > 2.0:  # 有足够空闲内存
            # 缓慢增加批次大小
            new_batch_size = min(512, int(current_batch_size * 1.1))
            if new_batch_size != current_batch_size:
                logger.info(f"内存充足, 尝试增加批次: {current_batch_size} -> {new_batch_size}")
            return new_batch_size
        
        return current_batch_size
    
    def get_optimal_batch_size(self, avg_seq_length: float, model_hidden_dim: int) -> int:
        """计算最佳批次大小"""
        if self.optimal_batch_size is not None:
            return self.optimal_batch_size
        
        stats = self.get_memory_stats()
        if not stats:
            return 32  # 默认值
        
        # 基于内存计算最佳批次大小
        # 经验公式: 每个token约需 0.1MB 内存(包含中间激活)
        tokens_per_seq = avg_seq_length + 2
        memory_per_seq_gb = (tokens_per_seq * model_hidden_dim * 4 * 20) / (1024**3)  # 保守估计
        
        # 目标使用 80% 的GPU内存
        target_memory_gb = stats.gpu_peak_gb * 0.8 if stats.gpu_peak_gb > 0 else 4.0
        
        optimal = int(target_memory_gb / memory_per_seq_gb)
        optimal = max(4, min(optimal, 512))  # 限制在4-512之间
        
        self.optimal_batch_size = optimal
        logger.info(f"计算最佳批次大小: {optimal} (基于 {avg_seq_length:.0f}平均长度, "
                   f"{target_memory_gb:.1f}GB目标内存)")
        
        return optimal

class TensorMemoryPool:
    """张量内存池 - 重用已分配的内存，避免频繁分配释放"""
    
    def __init__(self, device='cuda'):
        self.device = device
        self.pool = {}  # shape -> list of tensors
        self.hits = 0
        self.misses = 0
        self.total_allocated = 0
        
    def get_tensor(self, shape, dtype=torch.float32):
        """从池中获取张量"""
        key = (shape, dtype)
        
        if key in self.pool and self.pool[key]:
            tensor = self.pool[key].pop()
            self.hits += 1
            tensor.zero_()  # 清零
            return tensor
        
        # 池中没有，创建新的
        self.misses += 1
        tensor = torch.zeros(shape, dtype=dtype, device=self.device)
        self.total_allocated += tensor.numel() * tensor.element_size()
        return tensor
    
    def return_tensor(self, tensor):
        """将张量返回池中"""
        if tensor is None:
            return
        
        key = (tuple(tensor.shape), tensor.dtype)
        
        if key not in self.pool:
            self.pool[key] = []
        
        # 限制池大小，避免占用太多内存
        if len(self.pool[key]) < 10:  # 每个形状最多缓存10个
            self.pool[key].append(tensor)
        else:
            del tensor  # 释放
    
    def clear(self):
        """清空内存池"""
        for tensors in self.pool.values():
            for tensor in tensors:
                del tensor
        self.pool.clear()
        gc.collect()
        torch.cuda.empty_cache()
    
    def stats(self):
        """获取统计信息"""
        total_cached = sum(len(tensors) for tensors in self.pool.values())
        hit_rate = self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0
        return {
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': hit_rate,
            'total_cached': total_cached,
            'total_allocated_gb': self.total_allocated / (1024**3)
        }

def peak_performance_batch_processor(
    seq_items: List[Tuple[str, str]], 
    model, 
    alphabet, 
    device='cuda',
    max_length: int = 1022,
    rep_layer: int = 33,
    memory_manager: PeakPerformanceMemoryManager = None,
    tensor_pool: TensorMemoryPool = None
) -> Tuple[List[np.ndarray], List[str], List[int]]:
    """
    峰值性能批处理器 - 结合自适应批次和智能内存管理
    """
    batch_converter = alphabet.get_batch_converter()
    
    all_embeddings = []
    all_names = []
    all_lengths = []
    
    # 初始化
    if memory_manager is None:
        memory_manager = PeakPerformanceMemoryManager()
    
    if tensor_pool is None:
        tensor_pool = TensorMemoryPool(device)
    
    # 按长度排序，提高效率
    seq_items_sorted = sorted(seq_items, key=lambda x: len(x[1]))
    
    # 动态计算初始批次大小
    avg_length = np.mean([min(len(s), max_length) for _, s in seq_items_sorted[:100]])
    hidden_dim = model.embed_dim if hasattr(model, 'embed_dim') else 1280
    initial_batch_size = memory_manager.get_optimal_batch_size(avg_length, hidden_dim)
    
    current_batch_size = initial_batch_size
    total_processed = 0
    
    i = 0
    total_items = len(seq_items_sorted)
    
    with tqdm(total=total_items, desc="Peak Performance Processing", unit="seq") as pbar:
        while i < total_items:
            batch_start_time = time.time()
            
            # 收集当前批次
            batch_items = []
            batch_max_len = 0
            
            for _ in range(current_batch_size):
                if i >= total_items:
                    break
                
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
                
                # 使用内存池处理嵌入
                batch_embeddings = []
                for j in range(token_reprs.size(0)):
                    seq_repr = token_reprs[j, 1:batch_lengths[j]+1, :]
                    mean_repr = seq_repr.mean(dim=0)
                    
                    # 立即移动到CPU，释放GPU内存
                    cpu_emb = mean_repr.cpu().numpy()
                    batch_embeddings.append(cpu_emb)
                
                all_embeddings.extend(batch_embeddings)
                all_names.extend(batch_labels)
                all_lengths.extend(batch_lengths)
                
                total_processed += len(batch_items)
                
                # === 关键：智能内存清理 ===
                batch_processing_time = time.time() - batch_start_time
                
                # 自适应调整批次大小
                new_batch_size = memory_manager.adaptive_batch_size_adjustment(
                    current_batch_size, 
                    batch_processing_time,
                    len(batch_items)
                )
                
                if new_batch_size != current_batch_size:
                    current_batch_size = new_batch_size
                
                # 条件性内存优化
                if total_processed % 100 == 0:  # 每100个序列
                    freed = memory_manager.optimize_for_speed("periodic")
                    if freed > 0.5:  # 如果释放了显著内存
                        logger.debug(f"周期性清理释放 {freed:.2f}GB")
                
                # 返回张量到内存池
                tensor_pool.return_tensor(batch_tokens)
                
                pbar.update(len(batch_items))
                
                # 实时性能显示
                if total_processed % 1000 == 0:
                    seqs_per_sec = 1000 / batch_processing_time if batch_processing_time > 0 else 0
                    logger.info(f"处理 {total_processed:,} 序列, "
                              f"速度: {seqs_per_sec:.1f} seq/s, "
                              f"批次: {current_batch_size}, "
                              f"内存池命中率: {tensor_pool.stats()['hit_rate']:.2%}")
                
            except torch.cuda.OutOfMemoryError:
                logger.error("GPU内存不足! 执行紧急清理...")
                
                # 紧急清理
                freed = memory_manager.optimize_for_memory("oom")
                
                # 清空内存池
                tensor_pool.clear()
                
                # 减少批次大小
                i -= len(batch_items)  # 回退
                current_batch_size = max(4, current_batch_size // 2)
                
                logger.warning(f"批次大小减少到 {current_batch_size}, 释放 {freed:.2f}GB 内存")
    
    logger.info(f"本批次处理完成: {total_processed} 序列")
    logger.info(f"内存池统计: {tensor_pool.stats()}")
    
    return all_embeddings, all_names, all_lengths

def multi_threaded_fasta_reader(fasta_path: str, 
                               max_sequences: int = None,
                               buffer_size: int = 10000) -> queue.Queue:
    """
    多线程FASTA读取器 - 实现IO与计算分离
    """
    def reader_worker(fasta_path, output_queue, max_sequences, stop_event):
        """读取线程工作函数"""
        current_name = ""
        current_seq = []
        count = 0
        
        try:
            with open(fasta_path, 'r') as f:
                for line in f:
                    if stop_event.is_set():
                        break
                        
                    line = line.strip()
                    if not line:
                        continue
                    
                    if line.startswith('>'):
                        # 保存前一条序列
                        if current_name:
                            seq_str = ''.join(current_seq)
                            if seq_str:
                                output_queue.put((current_name, seq_str))
                                count += 1
                                if max_sequences and count >= max_sequences:
                                    break
                        
                        # 开始新序列
                        current_name = line[1:].split()[0]
                        current_seq = []
                    else:
                        current_seq.append(line)
                    
                    # 控制队列大小，避免内存占用过多
                    while output_queue.qsize() > buffer_size * 2:
                        time.sleep(0.1)
                
                # 最后一条序列
                if current_name and current_seq and not stop_event.is_set():
                    seq_str = ''.join(current_seq)
                    if seq_str:
                        output_queue.put((current_name, seq_str))
            
            logger.info(f"FASTA读取完成: {count} 序列")
            
        except Exception as e:
            logger.error(f"FASTA读取错误: {e}")
        finally:
            output_queue.put(None)  # 结束信号
    
    # 创建队列和事件
    seq_queue = queue.Queue(maxsize=buffer_size)
    stop_event = threading.Event()
    
    # 启动读取线程
    reader_thread = threading.Thread(
        target=reader_worker,
        args=(fasta_path, seq_queue, max_sequences, stop_event),
        name="FASTAReader",
        daemon=True
    )
    reader_thread.start()
    
    return seq_queue, stop_event, reader_thread

def main(args):
    """主函数 - 峰值性能版本"""
    
    logger.info("=" * 60)
    logger.info("峰值性能 ESM 嵌入生成器启动")
    logger.info("=" * 60)
    
    # 设置GPU
    if args.gpuid and torch.cuda.is_available():
        torch.cuda.set_device(int(args.gpuid))
        device = torch.device(f"cuda:{args.gpuid}")
        
        # 初始化峰值性能内存管理器
        memory_manager = PeakPerformanceMemoryManager(
            device_id=int(args.gpuid),
            target_utilization=args.target_utilization
        )
        
        # 启动自动清理线程
        if args.auto_cleanup:
            memory_manager.start_auto_cleanup()
        
        # 初始化张量内存池
        tensor_pool = TensorMemoryPool(device)
        
        stats = memory_manager.get_memory_stats()
        if stats:
            logger.info(f"GPU {args.gpuid}: {stats.gpu_free_gb:.1f}GB 可用, "
                       f"{stats.gpu_allocated_gb:.1f}GB 已用")
    else:
        device = torch.device("cpu")
        memory_manager = None
        tensor_pool = None
        logger.warning("使用CPU模式 - 性能优化功能受限")
    
    # 加载模型
    logger.info(f"加载模型: {args.model}")
    start_load = time.time()
    
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
        logger.error(f"未知模型: {args.model}")
        sys.exit(1)
    
    model.to(device)
    model.eval()
    
    logger.info(f"模型加载完成: {time.time()-start_load:.1f}秒")
    
    # 创建HDF5文件
    output_path = args.output if args.output else 'embeddings_peak.h5'
    logger.info(f"输出文件: {output_path}")
    with h5py.File(output_path, 'w') as h5_file:
        # 预创建可扩展的数据集
        max_shape = (None, hidden_dim)
        dset_emb = h5_file.create_dataset(
            'embeddings', 
            shape=(0, hidden_dim),
            maxshape=max_shape,
            dtype='float32',
            compression="gzip", 
            chunks=(args.h5_chunk_size, hidden_dim)
        )
        
        dset_names = h5_file.create_dataset(
            'Seq_Name', 
            shape=(0,), 
            maxshape=(None,), 
            dtype=h5py.string_dtype(encoding='utf-8'),
            compression="gzip", 
            chunks=(args.h5_chunk_size,)
        )
        
        dset_lengths = h5_file.create_dataset(
            'seq_len', 
            shape=(0,), 
            maxshape=(None,), 
            dtype='int32',
            compression="gzip", 
            chunks=(args.h5_chunk_size,)
        )
        
        # 使用多线程读取FASTA
        logger.info("启动多线程FASTA读取...")
        seq_queue, stop_event, reader_thread = multi_threaded_fasta_reader(
            args.input, 
            args.max_sequences,
            buffer_size=args.reader_buffer_size
        )
        
        # 主处理循环
        seq_buffer = []
        total_processed = 0
        total_batches = 0
        start_total_time = time.time()
        peak_speed = 0.0
        
        # 预热
        logger.info("预热模型...")
        warmup_batch = torch.zeros((2, 100), dtype=torch.long, device=device)
        with torch.no_grad():
            _ = model(warmup_batch, repr_layers=[rep_layer])
        torch.cuda.synchronize()
        
        logger.info("开始峰值性能处理...")
        
        try:
            with tqdm(desc="峰值性能处理", unit="seq") as pbar:
                while True:
                    # 从队列获取序列
                    try:
                        item = seq_queue.get(timeout=1.0)
                        if item is None:  # 结束信号
                            break
                        
                        seq_buffer.append(item)
                        
                        # 当缓冲区足够大时进行处理
                        if len(seq_buffer) >= args.processing_buffer_size:
                            batch_start_time = time.time()
                            
                            # 使用峰值性能处理器
                            embeddings, names, lengths = peak_performance_batch_processor(
                                seq_buffer, 
                                model, 
                                alphabet, 
                                device, 
                                args.max_length, 
                                rep_layer,
                                memory_manager,
                                tensor_pool
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
                                
                                total_processed += len(embeddings)
                                total_batches += 1
                                
                                # 计算处理速度
                                batch_time = time.time() - batch_start_time
                                batch_speed = len(embeddings) / batch_time
                                peak_speed = max(peak_speed, batch_speed)
                                
                                # 更新进度
                                pbar.update(len(embeddings))
                                pbar.set_postfix({
                                    'speed': f'{batch_speed:.1f} seq/s',
                                    'peak': f'{peak_speed:.1f} seq/s',
                                    'total': f'{total_processed:,}'
                                })
                                
                                # 定期报告
                                if total_batches % args.report_interval == 0:
                                    elapsed = time.time() - start_total_time
                                    avg_speed = total_processed / elapsed
                                    
                                    if memory_manager:
                                        stats = memory_manager.get_memory_stats()
                                        if stats:
                                            logger.info(f"批次 {total_batches}: "
                                                      f"处理 {total_processed:,} 序列, "
                                                      f"平均速度: {avg_speed:.1f} seq/s, "
                                                      f"峰值速度: {peak_speed:.1f} seq/s, "
                                                      f"GPU使用: {stats.gpu_allocated_gb:.1f}GB")
                                    
                                    # 如果速度下降，进行优化
                                    if batch_speed < peak_speed * 0.7:
                                        logger.warning(f"速度下降 ({batch_speed:.1f} vs 峰值 {peak_speed:.1f}), 进行优化...")
                                        if memory_manager:
                                            memory_manager.optimize_for_speed("speed_drop")
                            
                            # 清空缓冲区
                            seq_buffer.clear()
                    
                    except queue.Empty:
                        # 队列空，检查读取线程是否存活
                        if not reader_thread.is_alive():
                            break
                        continue
            
            # 处理剩余的序列
            if seq_buffer:
                embeddings, names, lengths = peak_performance_batch_processor(
                    seq_buffer, 
                    model, 
                    alphabet, 
                    device, 
                    args.max_length, 
                    rep_layer,
                    memory_manager,
                    tensor_pool
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
                    
                    total_processed += len(embeddings)
                    pbar.update(len(embeddings))
        
        except KeyboardInterrupt:
            logger.warning("用户中断处理")
        finally:
            # 清理
            stop_event.set()
            if memory_manager:
                memory_manager.stop_auto_cleanup()
            if tensor_pool:
                tensor_pool.clear()
            
            # 等待读取线程结束
            reader_thread.join(timeout=5)
            
            # 最终报告
            total_time = time.time() - start_total_time
            avg_speed = total_processed / total_time if total_time > 0 else 0
            
            logger.info("=" * 60)
            logger.info("处理完成!")
            logger.info(f"总序列数: {total_processed:,}")
            logger.info(f"总时间: {total_time/3600:.2f} 小时")
            logger.info(f"平均速度: {avg_speed:.1f} 序列/秒")
            logger.info(f"峰值速度: {peak_speed:.1f} 序列/秒")
            logger.info(f"输出文件: {output_path}")
            logger.info("=" * 60)
            
            if memory_manager and tensor_pool:
                mem_stats = memory_manager.get_memory_stats()
                pool_stats = tensor_pool.stats()
                
                logger.info(f"最终GPU内存: {mem_stats.gpu_allocated_gb:.1f}GB 已用")
                logger.info(f"内存池命中率: {pool_stats['hit_rate']:.2%}")
                logger.info(f"内存池节省: {pool_stats['total_allocated_gb']:.2f}GB 分配")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='峰值性能 ESM 嵌入生成器')
    parser.add_argument('-i', '--input', help='输入FASTA文件路径', required=True)
    parser.add_argument('-o', '--output', help='输出HDF5文件路径', default='ab&gc.h5')
    parser.add_argument('-m', '--model', help='预训练模型', 
                       choices=['ESM1b', 'ESM2_3B', 'ESM2_650M'], default='ESM2_650M')
    parser.add_argument('-g', '--gpuid', help='GPU ID', default='0')
    
    # 性能优化参数
    parser.add_argument('--target_utilization', help='目标GPU利用率(0-1)', 
                       type=float, default=0.85)
    parser.add_argument('--auto_cleanup', help='启用自动垃圾回收', 
                       action='store_true', default=True)
    parser.add_argument('--processing_buffer_size', help='处理缓冲区大小', 
                       type=int, default=10000)
    parser.add_argument('--reader_buffer_size', help='读取器缓冲区大小', 
                       type=int, default=50000)
    parser.add_argument('--h5_chunk_size', help='HDF5分块大小', 
                       type=int, default=4096)
    parser.add_argument('--report_interval', help='报告间隔(批次)', 
                       type=int, default=10)
    parser.add_argument('--max_length', help='最大序列长度', 
                       type=int, default=1022)
    parser.add_argument('--max_sequences', help='最大处理序列数', 
                       type=int, default=None)
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        logger.error(f"输入文件不存在: {args.input}")
        sys.exit(1)
    
    # 设置CUDA优化
    torch.backends.cudnn.benchmark = True
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True
    
    main(args)

    # python gen_emb_parrallel_abgc.py -i SMAGs.fasta -m ESM2_3B
    # screen -S esm_processing
    # screen -r esm_processing