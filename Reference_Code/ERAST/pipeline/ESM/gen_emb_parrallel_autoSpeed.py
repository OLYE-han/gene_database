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
import gen_emb_parrallel_ab_gc as peak

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

@dataclass
class SpeedStats:
    """速度统计"""
    sequences_per_sec: float = 0.0
    batch_processing_time: float = 0.0
    batch_size: int = 0
    sequences_processed: int = 0
    timestamp: float = 0.0
    is_slow: bool = False  # 是否低于速度阈值

class SpeedAwarePeakPerformanceMemoryManager(peak.PeakPerformanceMemoryManager):
    """
    速度感知的峰值性能内存管理器
    继承原有功能，增加速度监控
    """
    
    def __init__(self, device_id=0, target_utilization=0.85, speed_threshold=300.0):
        super().__init__(device_id, target_utilization)
        self.speed_threshold = speed_threshold
        self.speed_history = deque(maxlen=10)  # 记录最近10个速度
        self.consecutive_slow_batches = 0
        self.last_speed_cleanup_time = 0
        self.speed_cleanup_cooldown = 30  # 速度清理冷却时间(秒)
        self.peak_speed = 0.0
        
        logger.info(f"速度感知内存管理器初始化: 速度阈值={speed_threshold} seq/s")
    
    def record_speed(self, batch_size: int, processing_time: float, sequences_processed: int) -> SpeedStats:
        """记录批次速度"""
        if processing_time > 0:
            seqs_per_sec = sequences_processed / processing_time
        else:
            seqs_per_sec = 0
        
        # 更新峰值速度
        if seqs_per_sec > self.peak_speed:
            self.peak_speed = seqs_per_sec
        
        # 检查是否低于阈值
        is_slow = seqs_per_sec < self.speed_threshold
        
        stats = SpeedStats(
            sequences_per_sec=seqs_per_sec,
            batch_processing_time=processing_time,
            batch_size=batch_size,
            sequences_processed=sequences_processed,
            timestamp=time.time(),
            is_slow=is_slow
        )
        
        self.speed_history.append(stats)
        
        # 更新连续低速批次计数
        if is_slow:
            self.consecutive_slow_batches += 1
        else:
            self.consecutive_slow_batches = 0
        
        return stats
    
    def should_cleanup_for_speed(self) -> Tuple[bool, str, float]:
        """
        检查是否应该为速度优化而清理内存
        
        返回: (是否需要清理, 原因, 当前速度)
        """
        current_time = time.time()
        
        # 冷却期检查
        if current_time - self.last_speed_cleanup_time < self.speed_cleanup_cooldown:
            return False, f"速度清理冷却期内 (剩余{self.speed_cleanup_cooldown - (current_time - self.last_speed_cleanup_time):.1f}s)", 0.0
        
        # 需要足够的速度历史
        if len(self.speed_history) < 3:
            return False, "速度历史不足", 0.0
        
        # 获取最近速度
        recent_speeds = [s.sequences_per_sec for s in list(self.speed_history)[-3:]]
        current_speed = recent_speeds[-1] if recent_speeds else 0.0
        avg_speed = np.mean(recent_speeds) if recent_speeds else 0.0
        
        # 条件1: 当前速度低于阈值
        if current_speed < self.speed_threshold:
            # 条件2: 平均速度也低于阈值
            if avg_speed < self.speed_threshold:
                # 条件3: 连续多个批次低速
                if self.consecutive_slow_batches >= 3:
                    reason = f"速度连续低速 ({self.consecutive_slow_batches}批次, 当前:{current_speed:.1f} < 阈值:{self.speed_threshold})"
                    return True, reason, current_speed
        
        return False, "速度正常", current_speed
    
    def cleanup_for_speed(self, reason: str, current_speed: float) -> Dict[str, Any]:
        """
        执行速度优化清理
        返回清理统计
        """
        logger.warning(f"🚨 速度触发清理: {reason}")
        
        # 记录清理前状态
        before_stats = self.get_memory_stats()
        before_gpu = before_stats.gpu_allocated_gb if before_stats else 0.0
        start_time = time.time()
        
        # 执行三级清理
        freed_gb = 0.0
        
        # 第一级: Python垃圾回收
        gc.collect()
        
        # 第二级: CUDA缓存清理
        torch.cuda.empty_cache()
        
        # 第三级: 深度清理
        for _ in range(3):
            gc.collect()
        
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()
        torch.cuda.synchronize()
        
        # 记录清理后状态
        after_stats = self.get_memory_stats()
        after_gpu = after_stats.gpu_allocated_gb if after_stats else 0.0
        freed_gb = before_gpu - after_gpu
        
        # 更新最后清理时间
        self.last_speed_cleanup_time = time.time()
        
        # 重置低速计数
        self.consecutive_slow_batches = 0
        
        result = {
            'reason': reason,
            'current_speed': current_speed,
            'speed_threshold': self.speed_threshold,
            'freed_gb': freed_gb,
            'duration': time.time() - start_time,
            'before_gpu_gb': before_gpu,
            'after_gpu_gb': after_gpu,
            'timestamp': time.time()
        }
        
        logger.info(f"✅ 速度优化清理完成: 释放 {freed_gb:.2f}GB, "
                   f"耗时 {result['duration']:.2f}秒")
        
        return result
    
    def get_speed_summary(self) -> Dict[str, Any]:
        """获取速度摘要"""
        if not self.speed_history:
            return {}
        
        recent_stats = list(self.speed_history)[-5:]
        recent_speeds = [s.sequences_per_sec for s in recent_stats]
        
        return {
            'current_speed': recent_speeds[-1] if recent_speeds else 0.0,
            'avg_speed_5': np.mean(recent_speeds) if recent_speeds else 0.0,
            'min_speed_5': np.min(recent_speeds) if recent_speeds else 0.0,
            'peak_speed': self.peak_speed,
            'speed_threshold': self.speed_threshold,
            'consecutive_slow_batches': self.consecutive_slow_batches,
            'speed_history': [s.sequences_per_sec for s in self.speed_history]
        }

def speed_aware_peak_performance_batch_processor(
    seq_items: List[Tuple[str, str]], 
    model, 
    alphabet, 
    device='cuda',
    max_length: int = 1022,
    rep_layer: int = 33,
    memory_manager: SpeedAwarePeakPerformanceMemoryManager = None,
    tensor_pool: peak.TensorMemoryPool = None,
    speed_threshold: float = 300.0
) -> Tuple[List[np.ndarray], List[str], List[int]]:
    """
    速度感知的峰值性能批处理器
    在原有基础上增加速度监控
    """
    batch_converter = alphabet.get_batch_converter()
    
    all_embeddings = []
    all_names = []
    all_lengths = []
    
    # 初始化
    if memory_manager is None:
        memory_manager = SpeedAwarePeakPerformanceMemoryManager(speed_threshold=speed_threshold)
    
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
    
    # 速度清理记录
    speed_cleanup_log = []
    
    with tqdm(total=total_items, desc="Speed-Aware Peak Processing", unit="seq") as pbar:
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
                
                # 计算处理时间
                batch_processing_time = time.time() - batch_start_time
                
                # === 关键：速度监控与清理 ===
                # 记录速度
                speed_stats = memory_manager.record_speed(
                    current_batch_size, batch_processing_time, len(batch_items)
                )
                
                # 检查是否需要为速度清理
                should_clean, reason, current_speed = memory_manager.should_cleanup_for_speed()
                
                if should_clean:
                    # 执行速度优化清理
                    cleanup_result = memory_manager.cleanup_for_speed(reason, current_speed)
                    speed_cleanup_log.append(cleanup_result)
                    
                    # 在进度条显示清理信息
                    pbar.write(f"🧹 速度清理: {reason}, 释放 {cleanup_result['freed_gb']:.2f}GB")
                
                # 原有的自适应批次调整
                new_batch_size = memory_manager.adaptive_batch_size_adjustment(
                    current_batch_size, 
                    batch_processing_time,
                    len(batch_items)
                )
                
                if new_batch_size != current_batch_size:
                    current_batch_size = new_batch_size
                
                # 原有的条件性内存优化
                if total_processed % 100 == 0:  # 每100个序列
                    freed = memory_manager.optimize_for_speed("periodic")
                    if freed > 0.5:  # 如果释放了显著内存
                        logger.debug(f"周期性清理释放 {freed:.2f}GB")
                
                # 返回张量到内存池
                tensor_pool.return_tensor(batch_tokens)
                
                pbar.update(len(batch_items))
                
                # 实时性能显示（增强速度信息）
                if total_processed % 1000 == 0:
                    speed_summary = memory_manager.get_speed_summary()
                    
                    pbar.write(f"📊 速度状态: 当前 {speed_stats.sequences_per_sec:.1f} seq/s, "
                             f"峰值 {speed_summary.get('peak_speed', 0):.1f} seq/s, "
                             f"阈值 {speed_threshold} seq/s, "
                             f"批次 {current_batch_size}")
                
                # 进度条显示速度
                pbar.set_postfix({
                    'speed': f'{speed_stats.sequences_per_sec:.1f} seq/s',
                    'batch': current_batch_size,
                    'peak': f'{memory_manager.peak_speed:.1f}',
                    'thresh': f'{speed_threshold}'
                })
                
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
    
    # 最终报告
    logger.info(f"本批次处理完成: {total_processed} 序列")
    
    # 速度清理统计
    if speed_cleanup_log:
        logger.info(f"速度触发清理 {len(speed_cleanup_log)} 次:")
        total_freed = sum(log['freed_gb'] for log in speed_cleanup_log)
        logger.info(f"  总共释放内存: {total_freed:.2f}GB")
        
        # 显示最近3次清理
        for i, log in enumerate(speed_cleanup_log[-3:]):
            logger.info(f"  清理{i+1}: {log['reason']}, 释放 {log['freed_gb']:.2f}GB")
    
    # 速度摘要
    speed_summary = memory_manager.get_speed_summary()
    if speed_summary:
        logger.info(f"速度摘要: 平均 {speed_summary.get('avg_speed_5', 0):.1f} seq/s, "
                   f"峰值 {speed_summary.get('peak_speed', 0):.1f} seq/s, "
                   f"阈值 {speed_threshold} seq/s")
    
    logger.info(f"内存池统计: {tensor_pool.stats()}")
    
    return all_embeddings, all_names, all_lengths

def main(args):
    """主函数 - 增强速度感知版本"""
    
    logger.info("=" * 60)
    logger.info("速度感知峰值性能 ESM 嵌入生成器")
    logger.info(f"速度阈值: {args.speed_threshold} 序列/秒")
    logger.info("=" * 60)
    
    # 设置GPU
    if args.gpuid and torch.cuda.is_available():
        torch.cuda.set_device(int(args.gpuid))
        device = torch.device(f"cuda:{args.gpuid}")
        
        # 初始化速度感知内存管理器
        memory_manager = SpeedAwarePeakPerformanceMemoryManager(
            device_id=int(args.gpuid),
            target_utilization=args.target_utilization,
            speed_threshold=args.speed_threshold
        )
        
        # 启动自动清理线程
        if args.auto_cleanup:
            memory_manager.start_auto_cleanup()
        
        # 初始化张量内存池
        tensor_pool = peak.TensorMemoryPool(device)
        
        stats = memory_manager.get_memory_stats()
        if stats:
            logger.info(f"GPU {args.gpuid}: {stats.gpu_free_gb:.1f}GB 可用, "
                       f"{stats.gpu_allocated_gb:.1f}GB 已用")
    else:
        device = torch.device("cpu")
        memory_manager = None
        tensor_pool = None
        logger.warning("使用CPU模式 - 速度优化功能受限")
    
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
    output_path = args.output if args.output else 'embeddings_speed_aware.h5'
    logger.info(f"输出文件: {output_path}")
    
    with h5py.File(output_path, 'w') as h5_file:
        # 创建数据集
        dset_emb = h5_file.create_dataset(
            'embeddings', 
            shape=(0, hidden_dim),
            maxshape=(None, hidden_dim),
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
        seq_queue, stop_event, reader_thread = peak.multi_threaded_fasta_reader(
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
        speed_cleanup_count = 0
        
        # 预热
        logger.info("预热模型...")
        warmup_batch = torch.zeros((2, 100), dtype=torch.long, device=device)
        with torch.no_grad():
            _ = model(warmup_batch, repr_layers=[rep_layer])
        torch.cuda.synchronize()
        
        logger.info("开始速度感知处理...")
        
        try:
            with tqdm(desc="速度感知处理", unit="seq") as pbar:
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
                            
                            # 使用速度感知处理器
                            embeddings, names, lengths = speed_aware_peak_performance_batch_processor(
                                seq_buffer, 
                                model, 
                                alphabet, 
                                device, 
                                args.max_length, 
                                rep_layer,
                                memory_manager,
                                tensor_pool,
                                args.speed_threshold
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
                                
                                # 显示速度状态
                                speed_status = "🟢"
                                if batch_speed < args.speed_threshold * 0.7:
                                    speed_status = "🔴"
                                elif batch_speed < args.speed_threshold:
                                    speed_status = "🟡"
                                
                                pbar.set_postfix({
                                    'speed': f'{batch_speed:.1f} seq/s',
                                    'peak': f'{peak_speed:.1f} seq/s',
                                    'status': speed_status,
                                    'total': f'{total_processed:,}'
                                })
                                
                                # 定期报告
                                if total_batches % args.report_interval == 0:
                                    elapsed = time.time() - start_total_time
                                    avg_speed = total_processed / elapsed
                                    
                                    if memory_manager:
                                        speed_summary = memory_manager.get_speed_summary()
                                        mem_stats = memory_manager.get_memory_stats()
                                        
                                        if speed_summary and mem_stats:
                                            logger.info(f"批次 {total_batches}: "
                                                      f"处理 {total_processed:,} 序列, "
                                                      f"当前速度: {batch_speed:.1f} seq/s, "
                                                      f"平均速度: {avg_speed:.1f} seq/s, "
                                                      f"峰值速度: {speed_summary.get('peak_speed', 0):.1f} seq/s, "
                                                      f"GPU使用: {mem_stats.gpu_allocated_gb:.1f}GB")
                                    
                                    # 如果平均速度持续低于阈值，警告
                                    if avg_speed < args.speed_threshold * 0.6:
                                        logger.warning(f"⚠️ 平均速度偏低: {avg_speed:.1f} < {args.speed_threshold*0.6:.1f}")
                            
                            # 清空缓冲区
                            seq_buffer.clear()
                    
                    except queue.Empty:
                        # 队列空，检查读取线程是否存活
                        if not reader_thread.is_alive():
                            break
                        continue
            
            # 处理剩余的序列
            if seq_buffer:
                embeddings, names, lengths = speed_aware_peak_performance_batch_processor(
                    seq_buffer, 
                    model, 
                    alphabet, 
                    device, 
                    args.max_length, 
                    rep_layer,
                    memory_manager,
                    tensor_pool,
                    args.speed_threshold
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
            logger.info("✅ 处理完成!")
            logger.info(f"总序列数: {total_processed:,}")
            logger.info(f"总时间: {total_time/3600:.2f} 小时")
            logger.info(f"平均速度: {avg_speed:.1f} 序列/秒")
            logger.info(f"峰值速度: {peak_speed:.1f} 序列/秒")
            
            if memory_manager:
                speed_summary = memory_manager.get_speed_summary()
                if speed_summary:
                    logger.info(f"速度摘要: 平均 {speed_summary.get('avg_speed_5', 0):.1f} seq/s, "
                              f"阈值 {args.speed_threshold} seq/s")
            
            logger.info(f"输出文件: {output_path}")
            logger.info("=" * 60)
            
            # 性能分析
            if total_processed > 0 and avg_speed > 0:
                efficiency = (avg_speed / args.speed_threshold) * 100
                logger.info(f"效率分析: 达到阈值的 {efficiency:.1f}%")
                
                if efficiency < 70:
                    logger.warning("🔧 优化建议: 考虑降低速度阈值或检查系统资源")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='速度感知峰值性能 ESM 嵌入生成器')
    parser.add_argument('-i', '--input', help='输入FASTA文件路径', required=True)
    parser.add_argument('-o', '--output', help='输出HDF5文件路径', default='ab&gc_speed.h5')
    parser.add_argument('-m', '--model', help='预训练模型', 
                       choices=['ESM1b', 'ESM2_3B', 'ESM2_650M'], default='ESM2_650M')
    parser.add_argument('-g', '--gpuid', help='GPU ID', default='0')
    
    # 速度监控参数
    parser.add_argument('--speed_threshold', help='速度阈值(序列/秒)', 
                       type=float, default=300.0)
    
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