# intel-30 代码迁移记录：2026-10-19

已将当前持久服务使用的 release `05b5f3cf0079fd9ca3dd` 的 `gene_database` 实际移动到 `/sharenvme/xulihan/gene_database`。该目标是真实目录。原路径 `/sharenvme/xulihan_dhdgs/managed_cluster_20261019/releases/05b5f3cf0079fd9ca3dd/gene_database` 保留指向新目录的兼容软链接，以支持历史构建缓存和部署记录。

代码目录包含 `DHDGS` 和 `Reference_Code`，647 个文件的 SHA-256 在迁移前后全部一致。没有覆盖代码或修改参考库。当前部署的 `run_data.sh` 及本机 `managed_cluster_20261019/deployment_node2.json` 已指向新目录；后者修改前备份为 `deployment_node2.before.json`。

索引仍在 `/sharenvme/xulihan_dhdgs/seq2m_full_20261017/index/shard_2`。共享 Python 环境仍在 `/sharenvme/xulihan_dhdgs/seq2m_full_20261017/gene_database/DHDGS/.venv`。旧的 pilot/full 实验代码与结果保留在各自目录。本次移动的是当前持久服务版本，未合并多个历史版本。未来使用管理工具 `prepare` 新版本仍会创建独立 release，本次迁移不改变部署工具的发布规则。

已验证新代码路径下原生模块加载、配置解析和索引 manifest；真实启动 data-2 成功监听 `192.168.33.30:20853`，随后正常停止，退出码为 0。未启动其他节点、未发起查询或基准测试、未留下运行中的测试服务。

在本机登录并查看代码：

```sh
ssh intel-30
cd /sharenvme/xulihan/gene_database
ls
cd DHDGS
```

在 intel-30 前台启动数据服务，仍使用已经更新的启动脚本：

```sh
/sharenvme/xulihan_dhdgs/managed_cluster_20261019/run_data.sh
```

服务启动后需按原管理流程由 head 同步配置后恢复查询。不要与另一份 data-2 同时启动。

本目录保存 [迁移脚本](migrate.py)、[迁移日志](migration.log)、[启动验证脚本](verify_startup.py) 和 [启动结果](startup.log)。脚本为一次性操作，拒绝覆盖已经存在的目标目录。远端 `/sharenvme/xulihan/relocation_20261019/` 保存文件哈希、原启动脚本备份及验证记录。
