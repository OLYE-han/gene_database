# Pyramid 分布式 HNSW 参考代码

## 来源与定位

- 获取日期：2026-10-14。
- 本地代码目录：`Pyramid/`。
- 上游仓库：<https://github.com/dhaura/anns>。
- 固定提交：`e07a490485b0713b4f3fb3be65742a14d7a05cf2`。
- 获取方式：默认分支浅克隆，保留 Git 元数据和上游源码，未修改源码。
- 上游 README 将相关实现标注为 **Distributed HNSW (Pyramid Approach)**。

**这是第三方的 C++ / MPI / hnswlib 实现，不是已确认的原论文作者官方源码，也不是完整的可部署数据库。** 本次检索未能确认官方公开仓库；查阅的 arXiv v1 论文未提供源码链接。不能据此断言作者从未公开源码。

对应论文：Shiyuan Deng, Xiao Yan, Kelvin K.W. Ng, Chenyu Jiang, James Cheng，*Pyramid: A General Framework for Distributed Similarity Search on Large-scale Datasets*，IEEE Big Data 2019。

- DOI：<https://doi.org/10.1109/BigData47090.2019.9006219>
- 预印本：<https://arxiv.org/abs/1906.10602>（预印本标题较短）
- 本地预印本：`Pyramid_paper_1906.10602.pdf`

## 建议阅读入口

以 `Pyramid/hnsw/pyramid_hnsw_hnswlib_v2.1.cpp` 为主要入口：

| 内容 | 代码入口 |
| --- | --- |
| 数据采样 | `sample_input` |
| k-means 中心及 Meta-HNSW 构建 | `main` 中 `cv::kmeans`、`meta_hnsw` |
| 中心到进程的分组 | `greedy_grouping` |
| 数据分区与查询路由 | `distribute_data_matrix`，使用 `searchKnn`、`MPI_Alltoallv` |
| 各进程本地 HNSW | `main` 中 `local_hnsw` |
| 查询结果合并 | `MPI_Reduce` 与 `MPI_MINLOC` |

其他参考：

- `Pyramid/hnsw/pyramid_hnsw_hnswlib_v1.cpp`、`v1.1.cpp`、`v2.cpp`：历史实现变体。
- `Pyramid/hnsw/dist_hnsw_hnswlib_v0.1.cpp`：朴素分布式 HNSW 对照。
- `Pyramid/hnsw/scripts/`：运行脚本、扩展性与参数敏感性实验。
- `Pyramid/README.md`：上游编译和 MPI 运行说明。

## 与论文的差异和复用边界

1. 此实现使用 k-means 中心构建 Meta-HNSW，再以贪心方法分组；原论文描述的是采样 Meta-HNSW 底层图的平衡图划分。不要将两者视为完全相同的分区算法。
2. 此实现使用 MPI 通信。原论文中的 Kafka 查询队列、ZooKeeper 故障检测、恢复及副本调度不能由这份代码直接获得。
3. v2.1 使用 L2 距离，并执行 top-1 查询；不能直接视为论文所有相似度函数及通用 top-k 的实现。
4. v2.1 用输入数据自身作为查询，以返回 ID 是否等于原 ID 计算 `recall`。该指标不是独立查询集上的标准 recall@k。
5. v2.1 将数据 ID 转为 float 参与传输；用于超大数据集前需改成整数传输，避免超过 float 精确整数范围后的 ID 损失。
6. 上游未附顶层 LICENSE；外部依赖具有各自许可证。

## 依赖与验证情况

- 依赖 MPI、OpenCV 和 hnswlib；本次只保存参考源码，没有安装依赖、编译或运行基准。
- 源码通过 `../../hnswlib/hnswlib/hnswlib.h` 引入 hnswlib，按当前目录布局要求 `Reference_Code/hnswlib/` 存在，或后续修改 include 配置。
- 上游 README 的一个通用运行命令在输入路径前多了 `.`；应以源码读取的参数为准：`input_filepath input_size dimension global_sample_size m branching_factor M ef_construction output_filepath`。
- v2.1 访问 `argv[9]`，但检查条件为 `argc < 9`；运行前应修正为 `argc < 10`，并补齐参数范围检查。
- 已检查固定提交、工作树状态和 Git 对象完整性。下载成功不代表该实现已通过运行验证。

本地查看版本：

```bash
git -C gene_database/Reference_Code/Pyramid rev-parse HEAD
git -C gene_database/Reference_Code/Pyramid status --short
```
