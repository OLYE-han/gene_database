# 第三方算法与许可证

DHDGS 的原生模块链接本地 `../Reference_Code/RaBitQ-Library` 的 core target，未修改该仓库。该目录是主仓库中的 vendor 快照，未带独立 Git 仓库，不能将主仓库 HEAD 当作上游提交。验证时主仓库 HEAD 为 `3789ed915a04e199cda0870a17b2cfdeae96fd13`，该参考目录的 Git tree 为 `b770750ecf68ab668b14cc1ff2964e8a0902261f`；参考目录与该 tree 无差异。

- [RaBitQ-Library](https://github.com/VectorDB-NTU/RaBitQ-Library)：使用 `rabitqlib/core/api.hpp` 的稳定量化 API、SplitPacked 布局和 FHT/Kac 旋转。源码与许可证保留在[参考库](../Reference_Code/RaBitQ-Library)及其 [LICENSE](../Reference_Code/RaBitQ-Library/LICENSE)。
- HNSW 使用参考库自带的 `rabitqlib/third/hnswlib`，保留其 [LICENSE](../Reference_Code/RaBitQ-Library/include/rabitqlib/third/hnswlib/LICENSE)。
- 参考库所带 Eigen、FHT 等组件的声明和许可证保留在原目录；分发构建产物时应一并保留适用的第三方声明。
- `cpp/native.cpp` 中的 Vamana 构建器是 DHDGS 的小规模实现，借鉴 DiskANN/Vamana 的贪心构图与 RobustPrune 思路；没有将其标识为上游 DiskANN 构建器，也没有复用 DiskANN 的 PQ 文件格式。

相关论文与算法设计出处见 [DESIGN.md](DESIGN.md)。CMake 当前固定 core-only、关闭上游 index/fastscan/OpenMP 与 native-optimization，以缩小初始验证范围；这些编译开关尚未作为运行时实验选项开放。
