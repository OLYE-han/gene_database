# 实验日志流程

围绕同一个问题的一轮实验在 [TestLog.md](../TestLog.md) 中只保留一行，按问题归组、组内按开始时间升序排列，内容为时间、状态、一句话结论与报告链接，规则见 [AGENTS.md](../AGENTS.md)。一轮可以包含多种配置、重复测量、失败重试、微基准和功能回归；这些不各自新增主日志条目。完整元数据和详细日志保存在各轮目录及 `test/log_archive/rounds/`，日志工具只在运行边界和离线分析阶段执行。

0. 阅读 [Research.md](../Research.md)，关联已有问题或先登记新问题。在本轮元数据中保存 `research_ids`（例如 `[R001, R002]`），并在 `purpose` 及报告中写明研究ID、对照和验收条件。研究ID用于跨轮次追踪；不能将它直接当作每轮日志ID。
1. 在 `test/<轮次名>/experiment.yaml` 保存整轮元数据，`id` 和 `round_id` 均为稳定的轮次 ID。说明问题、假设、固定配置与期望效果；`variables` 列出本轮改变的变量、取值及对应测试，联合改动须注明，不能声称为单变量消融。另填 `log_group`（复用主日志问题组）、`summary`（一句话概括目的/结果及关键限制）、`report`（相对DHDGS目录的具体报告路径）；运行中报告未生成时省略report或链接计划并设 `report_label: 计划（报告待生成）`。新问题使用新轮次；同一轮补充测试更新原条目，一轮涉及多个研究ID也仅选择一个主要问题组。
2. 每次独立测试使用唯一 `id` 和输出目录，`round_id` 指向本轮。`experiment()` 记录真实起止时间和成功/异常状态到各次测试的 `experiment_log.json`。`id != round_id` 时不会写入主日志；不能把子测试元数据的 `id` 改成轮次 ID，否则会覆盖整轮结论。
3. 用整轮 `experiment()` 包裹测试集合与清理；分析和报告完成后，更新完整元数据以及一句话 `summary`、实际 `report`。工具将变量变化、失败/重试、代码变更、图表与建议写到详细记录，主日志只更新一行。使用整轮元数据更新：

   ```sh
   .venv/bin/python tools/experiment_log.py --metadata test/<轮次名>/experiment.yaml
   ```

   重生成报告更新原 ID，不新增条目；原始测量时间保持不变。此时可先维持“测量结束，待分析”，完成下列研究回填、设计记录和交付核对后再将整轮标为“已完成”。不能因一个子测试成功就把整轮标为完成，也不能将某次测试结果替代整轮汇总。
4. 报告生成后，在Research中每个相关问题下追加本轮尝试记录，包含变量、结果/限制及报告/TestLog链接；一轮涉及多个问题也不增加TestLog条目。失败、无收益及未确定结果必须回填。仅作设计讨论或历史整理时注明类型与日期，不冒充本轮新测量。
5. 若方案验证可行，更新 [DESIGN.md](../DESIGN.md) 中的最终实现、参数、适用范围、实验依据和研究ID；再将对应研究问题或具体优化标记为“已解决”，写明日期及DESIGN位置。局部改进不关闭未解决的主问题；负结果可结束该轮实验而继续保留问题。`experiment_log.py`保留附加的research_ids元数据，但不自动更新Research、DESIGN或判断成功。
6. 交付前核对Research、DESIGN、主日志与实际产物一致。失败/中断保留在本轮汇总及对应证据中；SIGKILL/断电后检查未结束记录，未知时间明确注明。HEAD 不代表未提交源码，另留源码哈希或快照，不记录认证信息。出现回归应重新打开研究条目，不能覆盖删除原有结论。

新 runner 的最小接入（从 DHDGS 目录运行）：

```python
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path('tools').resolve()))
from experiment_log import experiment, record

# round_metadata 来自本轮 YAML，须含所有必要字段及 variables。
# 额外保存 research_ids，例如 ['R001']；purpose / artifacts 同时写入对应链接。
root = Path('test/<轮次名>')
with experiment(round_metadata, root / 'experiment_log.json') as whole:
    for case in cases:
        run = dict(round_metadata, id=f"{round_metadata['id']}_{case['name']}",
                   round_id=round_metadata['id'], configuration=case['configuration'],
                   results='待分析', artifacts=['本次测试目录，报告待生成'])
        with experiment(run, root / 'cases' / case['name'] / 'experiment_log.json'):
            run_and_cleanup(case)  # 查询计时位于此函数内部。
    whole['results'] = '测量完成，完整结论待离线分析'

# 离线补齐报告，回填Research尝试；若验证成功则更新DESIGN及解决状态。
# 再发布整轮结果/变量/产物，同一个 round id 只占一条主日志。
summary = json.loads((root / 'experiment_log.json').read_text())
summary.update(status='已完成', results=round_results, artifacts=report_links,
               log_group='查询接纳、锁等待与长期稳定性',
               summary=one_sentence_conclusion, report='test/<轮次名>/REPORT.md',
               report_label='报告')
summary = record(summary)
(root / 'experiment_log.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
```

已有网络 API、TCP/RDMA 与网络并行 runner、回归入口和报告脚本的子记录均指定所属轮次。各目录的 `experiment.yaml` 保留整轮汇总；历史脚本若再用于新问题，应新建目录和元数据，不能复用历史结论。`record()` 不自动推断问题归属，也不从子测试推断父轮次状态。单独执行子测试入口时，调用方仍须维护整轮生命周期和最终汇总。

日志工具使用离线文件锁和原子替换保护主日志，避免不同轮次互相覆盖；这不是查询系统中的锁。每轮完整Markdown和JSON保存到 `test/log_archive/rounds/<round_id>.{md,json}`，旧标题锚点和稳定轮次ID保持可跳转。2026-11-03整理前的全文见[历史日志](log_archive/TestLog_20261103.md)，旧逐测试记录仍见各轮 `RUN_LOG_ARCHIVE.md`；主日志不重复长篇元数据，不删除失败证据。没有显式摘要的旧调用会生成有长度限制的摘要，收尾时仍应人工核对关键负面结论。

研究台账是持续记录，实验日志是一轮执行记录，DESIGN是已验证方法的设计依据；三者互相链接但不互相替代。文档、链接核验和历史回填不触发新性能实验日志。Research的新问题/尝试模板见 [模板](../Research.md#template)。


## 长时实验的数据记录（2026-11-03）

长时或高负载回放默认不保存逐查询的完整原始结果。用YAML配置时间窗口、样本率和样本上限：所有查询参与计数、成功/失败、延迟直方图、召回及所需资源指标汇总，仅稀疏保存核对时间戳所需字段；副本生命周期事件单独记录。监控优先使用轻量累计计数，避免反复导出保留查询状态。

空完成窗口必须保留为0 QPS，延迟和召回留空；无副本命中时回源率标为不适用。分别给出成功吞吐和包含失败的完成速率，并说明发送窗口、排空时间、分位近似精度、失败及条件指标分母。保留可重绘的汇总/精简捕获、配置和源码哈希；完成校验后可删除重复的临时监控大文件。不要为了曲线好看删除停顿或失败区间。示例：[30分钟自然流](lb_sustained_20261103/REPORT.md)。
