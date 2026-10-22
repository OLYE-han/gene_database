"""Summarize already measured phase distributions and export effective plans."""
import json
import statistics
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent
d = json.loads((ROOT / 'comparison.json').read_text())
groups = {g['label']: g for g in d['groups']}
phases = {}
keys = ['receive_queue_validation', 'admission_registration', 'admission_state_lock_wait',
        'compute_queue_setup', 'search_loop', 'completion_command_queue', 'result_outbound_queue']
for label, group in groups.items():
    records = [json.loads((ROOT / 'cases' / name / 'timings.json').read_text()) for name in group['cases']]
    phases[label] = {k: statistics.median(r['branch'][k]['mean'] for r in records) for k in keys}
(ROOT / 'phase_comparison.json').write_text(json.dumps(phases, indent=2))
sample = json.loads((ROOT / 'p99/selected.json').read_text())
case = Path(sample['source_case'])
data_plan = yaml.safe_load((case / 'plan.yaml').read_text())
head_plan = {**data_plan, 'dhdgs': yaml.safe_load((case / 'head_effective.yaml').read_text())}
for name, value in [('expanded_layout.yaml', groups['expanded']['layout']),
                    ('expanded_data_plan.yaml', data_plan), ('expanded_head_plan.yaml', head_plan)]:
    (ROOT / name).write_text(yaml.safe_dump(value, sort_keys=False))

lines = ['## 扩容收益与等待位置', '',
         '同为每节点64搜索线程，网络4→16的三轮QPS中位数变化+6.16%（6300.61→6688.88），各轮p99中位数变化+13.32%（111.42→126.27ms）。三轮QPS范围分别5238.57–6382.07与5947.46–7012.16，存在重叠，尚不能证明稳定或统计显著收益。之前32搜索线程配置本轮另有一次2458QPS慢轮，完整保留。', '',
         '因此，“扩大搜索线程后，更多网络处理能力可能有益”有一定实验支持；“网络数量必须与搜索线程等比例增长”没有得到证明。128搜索线程、16循环的短测未优于128搜索线程、4循环。', '',
         '下表为三轮各自的每分支均值再取中位数，单位ms；锁等待是接纳注册的子区间，不重复相加。', '',
         '|阶段|64搜索/网络4|64搜索/网络16|', '|---|---:|---:|']
labels = {'receive_queue_validation': '接收排队及校验', 'admission_registration': '接纳与注册',
          'admission_state_lock_wait': '其中接纳状态锁等待', 'compute_queue_setup': '计算队列及设置',
          'search_loop': '搜索循环', 'completion_command_queue': '完成命令队列',
          'result_outbound_queue': '结果发送队列'}
for k in keys:
    lines.append(f"|{labels[k]}|{phases['matched_net4'][k]:.3f}|{phases['expanded'][k]:.3f}|")
lines += ['', '接收等待降低的同时，接纳状态锁等待升高，说明等待仍受共享状态影响，不能仅依据搜索池大小给网络池设置固定比例。锁获取的墙钟等待也包含线程被调度恢复的时间，不能全当成锁持有者的CPU耗时。', '',
          '实际数据进程线程数从103增至199，顶层从54增至138。扩容布局角色比例：数据“搜索:请求handler:接收reactor:派发TX:报告TX:控制”为64:64:16:16:16:1，另含16个管理handler和监测/辅助线程；顶层“路由:合并:报告handler:接收reactor:入口TX:租约TX”为16:4:64:16:16:16。', '',
          'p99跨节点样本query_id=146842、job=18145延迟126.264ms。父节点Intel-35在9.629ms释放搜索线程并独立发报告；子节点Intel-32接收等待73.754ms、接纳26.104ms（其中状态锁等待25.312ms）、搜索12.445ms。父节点跨机派发至ACK的100.121ms与子节点接纳前长等待相符，不能将它整体解释为网络传输时间。', '',
          '所有19轮共280000查询均成功，Recall@10约97.03%，网络计数完整性标记均为true，内存暂存无溢出。三机实验端口已关闭。配置另存expanded_layout.yaml、expanded_data_plan.yaml、expanded_head_plan.yaml，生产默认值未修改。', '',
          '后续自适应策略应将接收排队与接纳锁等待共同作为反馈：排队高且共享状态等待低时试探增加handler/循环；若锁等待上升、QPS无稳定收益或p99恶化，则回退。这里是基于本轮结果的设计建议，尚未实现自动按锁等待调节的策略。', '']
text = '\n'.join(lines)
(ROOT / 'FINDINGS.md').write_text(text)
p = ROOT / 'REPORT.md'
p.write_text(p.read_text().split('\n## 扩容收益与等待位置')[0] + '\n' + text)
print('Saved expanded plans and phase comparison')
