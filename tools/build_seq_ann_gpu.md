# 单卡 H100：200M 底库，随机 200K query 的全局精确 top100

## 当前默认距离：欧式距离（L2）

`sampled` 搜索现在默认 `--metric euclidean`，直接使用原始向量，不做单位长度归一化。
遍历整个底库，以平方 L2 筛选 top100，再开平方保存实际欧式距离，按距离升序排列并排除自身。
这是穷举搜索，数值仍受所选浮点精度影响；FP32 的范数/点积公式对极近向量可能存在
消减误差，负的平方距离会截为 0。需要更高数值精度时可使用 `--precision float64`。

已经完成的 prepare 和 sample 无需重跑，直接运行：

`sample` 和 `sampled` 搜索允许完成后删除 `prepare_progress.json`：缺失时仍会校验
manifest、已发布的底库分片，以及搜索所需的 query 缓存和源文件身份。
若该文件存在且标记 `complete=false`，仍拒绝运行。请保留 `manifest.json`、底库、
query 缓存及其配置和 `sample_progress.json`，搜索会使用这些文件。

如果清理后只剩 `queries.npy`，使用原始参数恢复抽样元数据：

```bash
/HOME/nsccgz_zgchen/nsccgz_zgchen_8/.conda/envs/build_dataset/bin/python -s -u \
  gene_database/tools/build_seq_ann_gpu.py recover-sample \
  --workers 16 --query-count 200000 --k 100 --seed 20261013
```

该命令不导入 PyTorch，不重新写入 query 向量。它按原 seed 重建 ID，用 CPU worker
逐条比对缓存向量和底库对应行，全部一致后恢复 ID、配置与完成标记。核对会发生随机
底库读取，耗时取决于存储；打开分片前和每核对一个 chunk 后都会打印进度。
如 seed、底库或缓存不匹配则拒绝完成；中断后可重跑恢复命令，重新核对全部 query。
恢复过程中不要并发修改或使用这套缓存。完成后再运行下面的 search。
这些元数据仅占少量空间（200K 个 int64 ID 约 1.6 MB），不应作为冗余删除。

```bash
CUDA_VISIBLE_DEVICES=0 /HOME/nsccgz_zgchen/nsccgz_zgchen_8/.conda/envs/build_dataset/bin/python -s -u \
  gene_database/tools/build_seq_ann_gpu.py search --algorithm sampled \
  --metric euclidean --query-count 200000 --k 100 --seed 20261013
```

原有 `sampled_search_q200000_k100_seed20261013/` 中的 query 缓存保持共享；
欧式结果和搜索状态写入该目录下的 `euclidean/`，包括 `knn/*.npz`、`progress.json`、
`search_config.json` 和（运行 benchmark 时）`benchmark.json`。
NPZ 保留 `query_start`、`query_end`、`query_ids`、`ids`、`distances`，新增 `metric` 字段。
`distances` 是欧式距离，不是平方距离；欧式搜索配置明确记录 `normalized=false`、
`squared_distances=false`。FP32 下 200K × 100 的 ID 和距离约 160 MB（未计压缩）。

下文保留的旧目录名称及 prepare manifest 的 `metric='cosine'` 是历史兼容信息，
底库和 query 缓存均为原始向量；本次搜索度量以欧式目录中的 `search_config.json` 为准。
显式指定 `--metric cosine` 可继续使用旧余弦结果目录；旧 `directed` / `symmetric`
算法目前仅支持 cosine，需要显式指定该参数。下文旧结果示例的 `knn/` 和搜索状态路径，
用于欧式结果时应加上 `euclidean/` 层级。

脚本 `build_seq_ann_gpu.py` 默认选取 `gene_database/Data/Embedded/emb_1B` 中前
200,000,000 条向量，按分片数字编号、分片内行号排序。新输出目录默认是
`gene_database/Data/seq_200M_cosine_top50_16parts`，相对脚本位置解析。

## 运行

```bash
source /APP/u22/ai_x86/anaconda3/2023.09/etc/profile.d/conda.sh
conda activate build_dataset
python gene_database/tools/build_seq_ann_gpu.py prepare --parts 16 --workers 16
python gene_database/tools/build_seq_ann_gpu.py sample --workers 16 --query-count 200000 --k 100 --seed 20261013
CUDA_VISIBLE_DEVICES=0 python gene_database/tools/build_seq_ann_gpu.py self-test
CUDA_VISIBLE_DEVICES=0 python gene_database/tools/build_seq_ann_gpu.py search --algorithm sampled --query-count 200000 --k 100 --seed 20261013
```

环境需要 Python 3.10+、NumPy、h5py、CUDA 版 PyTorch；已创建的全局 conda 用户环境
为 `~/.conda/envs/build_dataset`，PyTorch 2.6.0+cu124 与 H100 的 CUDA 12.4 驱动匹配。
`prepare` 和 `sample` 无需 GPU，`sample` 不导入 PyTorch、不初始化 CUDA。
`search` 是单 GPU 进程。`--workers` 控制 prepare/sample 的 CPU 并行度。

## 独立 CPU sample 阶段（必须先于 sampled search）

已有 200M 底库时直接运行 sample，不需重做 prepare：

```bash
# CPU 节点：生成固定随机 ID、并行提取并缓存 query
python gene_database/tools/build_seq_ann_gpu.py sample --workers 16 --query-count 200000 --k 100 --seed 20261013
# GPU 节点：使用相同共享数据路径和参数
CUDA_VISIBLE_DEVICES=0 python gene_database/tools/build_seq_ann_gpu.py search --algorithm sampled --query-count 200000 --k 100 --seed 20261013
```

sample 按底库分片划分任务，最多 16 个进程独立提取对应的 query，每个进程写自己的
`query_parts/part_XX.npy` 和断点。默认每提取 `--sample-chunk-rows 1024` 条就保存断点、
打印进度。query 按全局 ID 排序以改善读取局部性；源数据仍是分散读取，CPU 并行不能
保证消除 HDD/共享存储的随机访问延迟。

初始化时主进程只检查 manifest、准备状态和源文件大小/修改时间，不串行打开全部底库
的 NPY 文件头。日志以 `[sample:init]` 显示当前步骤；底库文件头校验和 mmap 由提取
worker 并行完成，打开前打印 `[sample:worker XX] Opening base header/mmap: ...`。
初始化尚未启动定时进度，源文件的 `stat` 也可能受共享文件系统延迟影响。
若停在 `Opening base header/mmap`，说明正在打开文件或读取文件头，并非读取整个
底库或等待 GPU；仅凭停顿不能确定存储故障。

若 shell 环境混乱，或回溯中出现 `~/.local/lib/python.../site-packages`，可以直接指定
已创建环境的解释器，并用 `-s` 禁用用户级包、`-u` 禁用输出缓冲：

```bash
/HOME/nsccgz_zgchen/nsccgz_zgchen_8/.conda/envs/build_dataset/bin/python -s -u \
  gene_database/tools/build_seq_ann_gpu.py sample \
  --workers 16 --query-count 200000 --k 100 --seed 20261013 --progress-interval 5
```

主进程默认每 **10 秒**打印一次总体进度；可用 `--progress-interval 5` 调整。
显示当前阶段（extract / merge / finalize / complete）、数量/百分比、完成的分片数、
本次运行平均速度、耗时、预计剩余时间及距离上次数量变化的时长。等待磁盘时也有定时提示。
提取阶段按已落盘断点统计，未满一个 chunk 时数量不会变化；未产生新进度时 ETA 为 `--`，
速度统计不计续跑之前完成的行。MB/s 表示有效向量数据量，不是实际磁盘读取带宽。
merge 的百分比单独从 0 开始；finalize 的 100% 仍需等待落盘和校验，只有 complete 才算完成。
详细状态同步写入 `sample_progress.json`，包括 `phase`、`percent`、`eta_seconds` 等。

```bash
python gene_database/tools/build_seq_ann_gpu.py sample --workers 16 --progress-interval 5
```

所有 query 子文件完成后，CPU 顺序合并成 `queries.npy`，再原子发布完整缓存。
200K × 2560 维 float32 约 **2.048 GB**。独立子缓存会保留用于恢复，另占约 2.048 GB；
含合并文件共约 4.096 GB。准备期间有临时文件和进度文件。
中断后运行同一个 sample 命令即可继续；已完成子文件不重写，合并中断则重新合并。
可调整 workers/sample-chunk-rows，不改变抽样参数。

search/benchmark 要求 `sample_progress.json` 已完成、`queries.npy` 存在且缓存配置匹配；
**不会自动重新抽样或从 base 随机提取 query**。它只顺序读取 query 缓存并上传 GPU。
旧任务已存在的 `query_ids.npy` 会继续复用，保证 query 集合不变。
缓存记录源 base 文件大小/修改时间及 query ID 摘要；源数据改变会拒绝复用过期缓存。

这只移除了 GPU 任务中的抽样和 query 随机提取。200M 底库约 2 TB，仍需分块读取、
CPU→GPU 传输、归一化与结果保存，无法保证 GPU 全程 100% 利用率或完全没有 CPU/I/O。
启动日志会依次显示缓存验证、PyTorch 初始化、query 上传/归一化以及开始全库扫描。

已通过 CPU 并行缓存及断点恢复测试，确认 sample 不导入 torch；FP32/FP64 top100
测试也已通过，并在测试中禁止 search 调用旧 gather 方法，确认其使用缓存向量。
验证在 CPU 上完成，GPU 吞吐和显存仍需在 H100 节点实测。

## 当前默认：随机 200K query × 完整 200M 底库，top100

默认 `--algorithm sampled --query-count 200000 --k 100 --seed 20261013`。
从完整准备好的底库中**均匀无放回**抽取 query，然后按全局 ID 排序，方便分片访问。
排序不改变随机子集。每个 query 仍搜索整个 200M 底库，排除自身，不是只搜索抽样的 200K 条。
保留跨不同 ID 的重复向量，距离、精度和同分约定与后文一致。

已有 16 个 base 文件可以直接复用，**无需重新 prepare**。为复用既有数据，
默认底库目录仍保留旧名称 `seq_200M_cosine_top50_16parts`；名称不限制新的 k。
抽样搜索结果单独保存在：

```text
seq_200M_cosine_top50_16parts/
  sampled_search_q200000_k100_seed20261013/
    sampling_config.json       # 底库、随机种子、抽样规则
    query_ids.npy              # 200K 条 query 的原始全局 ID，int64
    queries.npy                # 与 query_ids 按行对应的连续 float32 query 矩阵
    query_cache_config.json    # query 摘要、维度及底库文件身份
    sample_progress.json       # CPU sample 完成状态
    query_parts/               # 各 CPU worker 的独立缓存/断点
    search_config.json         # 精度、分块和 query ID 校验摘要
    progress.json              # queries_done / total_queries / complete
    knn/
      000000000_000200000.npz   # 默认一次处理全部 200K query
```

每个结果文件含 `query_start`、`query_end`、`query_ids`、`ids`、`distances`。
其中 start/end 是**抽样列表中的位置**，不是底库 ID；`query_ids[j]` 才是第 j 行 query
在完整底库中的全局 ID，`ids[j]` 是其 100 个全局邻居 ID。结果按距离升序排列。
FP32 top100 本体约 **160 MB**，FP64 约 **240 MB**；query ID 文件约 **1.6 MB**。
无需对称模式约 120 GB 的状态文件。

query ID 和 query 向量在 sample 阶段持久化，search 恢复时复用并检查；已保存结果批次会跳过。
更换抽样数量、k 或种子会使用新的结果目录。恢复相同任务时需保持精度与分块参数一致。
默认 query 批大小 1,048,576，大于 200K，因此只扫描底库一次。
200K × 200M 为 **4×10^13** 个向量对、2560 维约 **2.048×10^17 FLOP**，
比原始全量有向搜索减少约 1000 倍点积量（相较上三角模式约 500 倍）。
总耗时仍包含底库读取、top100 和数据传输，需实测。

`benchmark --algorithm sampled` 最多选 1024 条已抽样 query，仍扫描完整底库；
结果不保存为正式邻居表，且 I/O 固定开销使时间不能直接按 query 数量线性外推。
正式任务建议直接 search，避免重复扫描底库。

```python
from pathlib import Path
import numpy as np
root = Path('gene_database/Data/seq_200M_cosine_top50_16parts')
for path in sorted((root / 'sampled_search_q200000_k100_seed20261013/knn').glob('*.npz')):
    with np.load(path) as result:
        query_ids = result['query_ids']
        neighbor_ids = result['ids']
        distances = result['distances']
```

下文对称/有向模式为可选的全量 query 算法。它们已不再是默认搜索模式；
若需要旧 top50，显式传入 `--k 50`。

## prepare：16 个进程分别写 16 个文件

200M 条均分为 16 个连续 ID 区间，每个文件 12,500,000 条、2560 维 float32，
大小约 **128 GB**（十进制），共约 **2.048 TB**。不生成或合并为单个 `base.npy`。
每个进程独占自己的数据文件和进度文件，无进程间写入锁，不通过主进程传输向量。
单进程只保留一块读取缓冲，默认 `--copy-rows 16384`，约 160 MiB；另有校验临时数组
和 HDF5 缓存。16 个进程通常需要数 GB 到十余 GB 的主机内存余量，具体取决于分片。

```text
seq_200M_cosine_top50_16parts/
  manifest.json
  prepare_progress.json
  base/
    part_00.npy                 # 全局 ID [0, 12500000)
    part_00.progress.json
    part_01.npy                 # 全局 ID [12500000, 25000000)
    ...
    part_15.npy                 # 全局 ID [187500000, 200000000)
  symmetric_search/knn/
    part_00/
      000000000000_000001048576.npz
      ...
    ...
    part_15/
      ...
  symmetric_search/config.json
  symmetric_search/progress.json
```

构建中的文件为 `part_XX.partial.npy`。每读完一个源 HDF5 与当前输出分片的交集，
只对本进程的文件 fsync，并原子更新该分片的 `rows_done`。整个输出分片完成后，
原子改名为 `part_XX.npy`。每个完成的源文件交集会输出本分片行数及 MB/s。
主进程仅汇总已完成的输出文件数；正在构建的精细进度见各自 `progress.json`。
支持源分片跨输出文件边界、最后一个源分片截断、无法整除时输出分片相差一行。

中断后运行相同 prepare 命令，会跳过已完成文件，未完成文件从其独立断点继续。
可调整 `--workers` 和 `--copy-rows`，但不可修改总行数、源目录、向量字段或 `--parts`。
准备过程中源文件不可被其他程序修改。零向量、NaN/Inf 会报错，不会标记对应文件完成。

**同一输出目录只启动一个 prepare 或 search 入口。** 入口内部已分配互不重叠的
文件所有权；移除文件锁不代表允许多次启动相同入口同时写相同文件。

## 可选 search：上三角对称计算

显式使用 `--algorithm symmetric`，直接复用已准备好的 16 个 base 文件，无需重新 prepare。
单卡只访问大块对 `(i,j), i<=j`；默认 192 个大块对应 **18,528 个块对**。
每个非对角 tile 只做一次矩阵乘法，其行、列分别更新两侧向量的 top50。
对角大块内部也按上三角小块遍历；最小对角小块仍使用完整方阵 GEMM 并排除自身。
实际点积向量对数为 `(N*N + sum(对角小块行数的平方))/2`，默认规模下接近减半。
top50 两侧更新仍需执行，因此不能保证总耗时减半。

```bash
CUDA_VISIBLE_DEVICES=0 python gene_database/tools/build_seq_ann_gpu.py search --algorithm symmetric
CUDA_VISIBLE_DEVICES=0 python gene_database/tools/build_seq_ann_gpu.py benchmark --algorithm symmetric
```

新模式所有搜索文件放入数据目录下的 `symmetric_search/`，不覆盖旧有向搜索的结果：

```text
symmetric_search/
  config.json
  progress.json
  state/                       # 各大块已提交的 top50 中间状态
  knn/part_00/*.npz             # 最终结果，字段与旧模式相同
  ...
  knn/part_15/*.npz
```

每个块对完成后，先把两侧新状态写入各自版本文件并 fsync，再原子提交
`progress.json` 中的版本引用与下一个块对游标。提交前崩溃会重算当前块对，
提交后崩溃则从下一块对继续，避免重复合并候选。恢复时清理未提交的状态版本。
这保证进程中断恢复，不是对任意设备/文件系统掉电的保证。

第 i 行块对 `(i,i)...(i,last)` 全部完成后，才发布第 i 个大块的最终结果。
后续大块的状态仍是中间结果，不能用于评测。仅当
`symmetric_search/progress.json` 的 `complete=true` 时表示全量完成。
其中 `completed_pairs` 是已提交块对数，`rows_done` 是已最终完成的连续 query 行数。
中断后使用相同命令恢复，不可改变块大小、k 或精度；可以改变 `--no-prefetch`。
同一输出目录只启动一个对称搜索进程。

中间状态保存 int64 ID：200M × 50，FP32 约 **120 GB**、FP64 约 **160 GB**。
另需最终结果 FP32 **80 GB** / FP64 **120 GB** 及约 1.3 GB（默认 FP32）的事务临时空间。
完成后保留状态，以便恢复缺失的最终结果。默认 FP32 累计状态写入量约 **22 TB**，
这是对称计算可恢复更新的额外 I/O 成本。

对称模式同时驻留两个向量大块（默认 FP32 共约 20 GiB），另有 FP64 归一化临时数组、
两侧 top50、距离 tile 和 CUDA 工作区。预读额外占用约一个大块的主机内存。
显存不足时，在首次正式搜索前减小 `--query-rows` 或 `--tile-rows`。
FP32/FP64、关闭 TF32、自身排除、同分处理约定与下述旧实现一致。

对称 benchmark 测一个非对角大块对（只有一个大块时测其上三角），包含读取、归一化、
点积和两侧 top50，**不包含事务状态写入**，输出至 `symmetric_search/benchmark.json`。
外推时间不是端到端保证；以正式搜索的进度为准。H100 显存和实际提速尚需 GPU 实测。

## 旧 search：有向全扫描（显式 --algorithm directed）

以下描述仅适用于 `search --algorithm directed` / `benchmark --algorithm directed`。
旧模式的 `knn/`、`search_config.json`、`search_progress.json` 与新对称模式完全独立。

每个 query 分片再切成可装入 GPU 的批次，每批默认 **1,048,576** 条 query。
对于每批 query，依次扫描 **全部 16 个 base 文件**，每次读取 65,536 条数据库向量，
GPU 上按 2,048 条 query 的 tile 计算点积；各数据库块的 top50 不断合并，最终得到
全局 top50。不是只在 query 所在的分片内搜索。

与此前实现相比：

- 后台线程提前读取下一数据库块，与当前 GPU 计算重叠，读入主机内存的预取深度有界。
  可用 `--no-prefetch` 关闭，以测量本机预取效果。
- query 批次从 262,144 提高到 1,048,576；在 200M/16 份设置下，全库扫描次数从旧版
  单文件的 **763 次降为 192 次**，减少重复读取、传输和归一化。
- 结果按 query 所属文件和 query 批次保存，分别断点续跑。

仍然使用穷举点积，默认 FP32，关闭 TF32/FP16/BF16；归一化采用 FP64 后转换至所选精度。
`--precision float64` 可提高点积精度。“精确”指所选浮点精度的穷举结果，无近似候选筛选；
极近距离的排序仍可能受舍入影响。距离为 `1 - cosine_similarity`，按升序输出，
只排除相同全局 ID。内容相同但 ID 不同的向量仍可作为邻居；同分 ID 顺序不固定。

默认参数适合 H100 80GB 上的 2560 维数据，但峰值显存还受精度、CUDA 工作区等影响。
query FP32 本体约 10 GiB，归一化有 FP64 临时数组；FP32 距离 tile 约 512 MiB，
另有 base、top50 和结果合并缓冲。显存不足时减小 `--query-rows`，例如 524288，
或减小 `--tile-rows`。开始正式检索后保持计算块大小、精度和 k 不变。

```bash
# 扫描全数据库处理一批 query，仅测速，不保存邻居结果
CUDA_VISIBLE_DEVICES=0 python gene_database/tools/build_seq_ann_gpu.py benchmark --algorithm directed
# 如果显存不足，可在开始正式检索前选用更小 query 批次
CUDA_VISIBLE_DEVICES=0 python gene_database/tools/build_seq_ann_gpu.py search --algorithm directed --query-rows 524288
```

每个 `.npz` 有 `query_start`、`query_end`、`ids`、`distances`，query 范围为全局半开区间。
`ids` 为 `(批次行数, 50)` uint32 全局 ID；`distances` 为同形状 FP32（或所选 FP64）。
保存时先写临时文件，fsync 后原子改名。中断后用原命令继续，已完成批次跳过，
当前未完成批次需重新扫描全部数据库。只有 `search_progress.json` 的 `complete=true`
才表示全部 query 完成。

全量 top50 FP32 约 **80 GB**（uint32 ID + float32 distance），FP64 约 **120 GB**。
仍需约 **4×10^16** 个向量对比较、2560 维点积约 **2.048×10^20 FLOP**；分文件不减少
这个计算量。默认逻辑读取量约 192 × 2.048 TB = **393.2 TB**，实际磁盘读取受页缓存影响。
prepare 加速也受共享存储带宽限制；不保证进程数对应线性提速。请以真实节点测速为准。

## 旧输出与小规模验证

新版默认新目录 `_16parts`，不会覆盖或删除旧 `seq_200M_cosine_top50/base.npy`。
旧单文件的 prepare 断点不能直接当作 16 文件断点；新版会拒绝向旧 manifest 目录写入。
原始 HDF5 不变。若旧任务仍在运行，先正常停止，避免两套任务争抢存储带宽。
已准备完整的旧单文件数据可由读取层加载，但已有旧 search 配置/结果布局不能直接混用。

```bash
python gene_database/tools/test_prepare_seq_ann.py
python gene_database/tools/test_search_seq_ann.py
python gene_database/tools/test_symmetric_seq_ann.py
python gene_database/tools/test_sampled_seq_ann.py
python gene_database/tools/test_sampling_stage.py
python gene_database/tools/build_seq_ann_gpu.py prepare --rows 10000 --parts 16 --workers 16 --output gene_database/Data/seq_10k_16parts_check
python gene_database/tools/build_seq_ann_gpu.py sample --output gene_database/Data/seq_10k_16parts_check --query-count 1000 --k 100 --workers 16
CUDA_VISIBLE_DEVICES=0 python gene_database/tools/build_seq_ann_gpu.py search --output gene_database/Data/seq_10k_16parts_check --query-count 1000 --k 100 --query-rows 512 --db-rows 1024 --tile-rows 128
```

准备测试覆盖 16 个不等长文件、跨源文件边界、数字编号排序、末尾截断、串并行结果一致、
部分文件恢复、预读顺序、非法向量和旧目录保护。`self-test` 对比 NumPy FP64 全矩阵，
覆盖 16 文件的全局邻居、跨分片重复向量、自身排除、两种精度、预读开关与不整除分块。
上述准备测试、搜索结果保存/续跑测试和数值 self-test 均已在当前环境通过；
新增对称测试也已通过：验证 FP32/FP64 全局精确结果、实际点积计数、单对角块、
缺失最终结果恢复及两侧写入/提交前后崩溃恢复。`self-test` 同时验证对称 GPU 核心
（有 GPU 时使用 GPU，否则 CPU）。
搜索数值验证使用 CPU。尚未在 H100 上执行性能或峰值显存测试，请先运行 GPU self-test
和 benchmark 确认实际节点表现。

## 读取结果和序列映射

```python
import json
from pathlib import Path
import numpy as np
root = Path('gene_database/Data/seq_200M_cosine_top50_16parts')
manifest = json.loads((root / 'manifest.json').read_text())
for part in manifest['parts']:
    vectors = np.load(root / part['file'], mmap_mode='r')
    # vectors[j] 的全局 ID = part['start'] + j
for path in sorted((root / 'symmetric_search/knn').glob('part_*/*.npz')):
    with np.load(path) as result:
        start = int(result['query_start'])
        ids, distances = result['ids'], result['distances']
        # ids[j] 是全局 query ID start+j 的 50 个邻居 ID。
```

源分片映射保存在 `manifest['shards']`：找到满足 `start <= ID < start + rows`
的源分片，原始行号为 `ID - start`，可以据此读取 `Seq_Name` 和 `seq_len`。
