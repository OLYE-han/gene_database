# 实验日志流程

围绕同一个问题的一轮实验在 [TestLog.md](../TestLog.md) 中只保留一条汇总，规则见 [AGENTS.md](../AGENTS.md)。一轮可以包含多种配置、重复测量、失败重试、微基准和功能回归；这些不各自新增主日志条目。日志工具在运行边界和离线分析阶段执行，不进入查询热路径。

1. 在 `test/<轮次名>/experiment.yaml` 保存整轮元数据，`id` 和 `round_id` 均为稳定的轮次 ID。说明问题、假设、固定配置与期望效果；`variables` 列出本轮改变的变量、取值及对应测试，联合改动须注明，不能声称为单变量消融。新问题使用新轮次；同一问题的补充测试继续更新原条目。
2. 每次独立测试使用唯一 `id` 和输出目录，`round_id` 指向本轮。`experiment()` 记录真实起止时间和成功/异常状态到各次测试的 `experiment_log.json`。`id != round_id` 时不会写入主日志；不能把子测试元数据的 `id` 改成轮次 ID，否则会覆盖整轮结论。
3. 用整轮 `experiment()` 包裹测试集合与清理；分析和报告完成后，在同一条主日志汇总变量变化、关键结果、失败/重试、代码变更、报告/图表与下一步建议。使用整轮元数据更新：

   ```sh
   .venv/bin/python tools/experiment_log.py --metadata test/<轮次名>/experiment.yaml
   ```

   重生成报告更新原 ID，不新增条目；原始测量时间保持不变。不能因一个子测试成功就把整轮标为完成，也不能将某次测试结果替代整轮汇总。
4. 交付前核对主日志与实际产物一致。失败/中断保留在本轮汇总及对应证据中；SIGKILL/断电后检查未结束记录，未知时间明确注明。HEAD 不代表未提交源码，另留源码哈希或快照，不记录认证信息。

新 runner 的最小接入（从 DHDGS 目录运行）：

```python
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path('tools').resolve()))
from experiment_log import experiment, record

# round_metadata 来自本轮 YAML，须含所有必要字段及 variables。
root = Path('test/<轮次名>')
with experiment(round_metadata, root / 'experiment_log.json') as whole:
    for case in cases:
        run = dict(round_metadata, id=f"{round_metadata['id']}_{case['name']}",
                   round_id=round_metadata['id'], configuration=case['configuration'],
                   results='待分析', artifacts=['本次测试目录，报告待生成'])
        with experiment(run, root / 'cases' / case['name'] / 'experiment_log.json'):
            run_and_cleanup(case)  # 查询计时位于此函数内部。
    whole['results'] = '测量完成，完整结论待离线分析'

# 离线补齐整轮结果/变量/产物后发布，同一个 round id 只占一条主日志。
summary = json.loads((root / 'experiment_log.json').read_text())
summary.update(status='已完成', results=round_results, artifacts=report_links)
summary = record(summary)
(root / 'experiment_log.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
```

已有网络 API、TCP/RDMA 与网络并行 runner、回归入口和报告脚本的子记录均指定所属轮次。各目录的 `experiment.yaml` 保留整轮汇总；历史脚本若再用于新问题，应新建目录和元数据，不能复用历史结论。`record()` 不自动推断问题归属，也不从子测试推断父轮次状态。单独执行子测试入口时，调用方仍须维护整轮生命周期和最终汇总。

日志工具使用离线文件锁和原子替换保护主日志，避免不同轮次互相覆盖；这不是查询系统中的锁。旧的逐测试主日志归档到相应轮次目录的 `RUN_LOG_ARCHIVE.md`，主日志保留变量汇总及归档链接，不删除失败证据。
