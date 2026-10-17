"""Generate derived summaries; leave original experiment reports unchanged."""
from pathlib import Path
import csv,hashlib,json
from dhdgs.metrics import summarize_query_timings,summarize_per_query_timings,export_timing_csvs
root=Path(__file__).resolve().parent
source=root.parent/'recall_targets_20261016T171501+0800'
selection=json.loads((source/'final_selection.json').read_text())['selection']
processed={};targets=[];manifest=[]
for chosen in selection:
    if chosen['status']!='selected':continue
    name=chosen['name']
    if name not in processed:
        p=Path(chosen['validation_report']);report=json.loads(p.read_text())
        summary=summarize_query_timings(report['queries'])
        per_query=summarize_per_query_timings(report['queries'])
        out=root/name;out.mkdir(exist_ok=True)
        export_timing_csvs(out,summary,per_query)
        (out/'timing_summary.json').write_text(json.dumps({'query_timing_summary':summary,'per_query_timing_means':per_query},indent=2))
        manifest.append({'configuration':name,'source_report':str(p),'source_report_sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
        processed[name]=summary
        assert summary['wall_means_additive']
        assert abs(sum(v['mean_ms'] for v in summary['wall_stages'].values())-report['latency_ms']['mean'])<1e-6
        assert summary['wall_sample_count']==512 and len(per_query)==256
        assert all(q['wall_sample_count']==2 for q in per_query)
    summary=processed[name]
    for category in ('wall_stages','cumulative_work'):
        for stage,values in summary[category].items():
            targets.append({'target_recall':chosen['target'],'configuration':name,'category':category,'stage':stage,**values})
with (root/'target_stage_means.csv').open('x',newline='') as f:
    writer=csv.DictWriter(f,fieldnames=list(targets[0]));writer.writeheader();writer.writerows(targets)
(root/'source_manifest.json').write_text(json.dumps(manifest,indent=2))
(root/'target_stage_means.json').write_text(json.dumps(targets,indent=2))
lines=['# 查询各阶段平均耗时','',
'为上一轮召回目标实验补充的派生汇总，不修改原始报告、不重新执行这些查询。来源实验：recall_targets_20261016T171501+0800；每个不同配置有 256 条查询、每条重复 2 次，共 512 次测量请求。','',
'## 端到端阶段（平均每请求，ms）','',
'| 目标召回 | 预处理/HNSW/入口估距 | 数据节点执行及控制等待 | 顶层合并及生命周期清理 | 合计 |','|---|---|---|---|---|']
for chosen in selection:
    if chosen['status']!='selected':continue
    s=processed[chosen['name']]
    values=[s['wall_stages'][key]['mean_ms'] for key in ('routing_wall_ms','peer_execution_and_control_wait_ms','head_merge_and_lifecycle_ms')]
    lines.append(f"| {chosen['target']:.2f} | {values[0]:.4f} | {values[1]:.4f} | {values[2]:.4f} | {sum(values):.4f} |")
lines+=['',
'上表为互不重叠的客户端观察阶段，均值可以相加。数据节点执行阶段包含本地搜索、跨节点通信、排队、结果回传和控制等待；不能将其全部视为纯计算或纯网络时间。入口路由目前没有进一步拆分 HNSW 与入口估距。','',
'## 数据节点累计工作（平均每请求，ms）','',
'| 目标召回 | pread | RaBitQ 估距 | 原向量精确距离 | 扩展服务 wall 总量 | 分支搜索线程 CPU |','|---|---|---|---|---|---|']
for chosen in selection:
    if chosen['status']!='selected':continue
    work=processed[chosen['name']]['cumulative_work']
    values=[work[key]['mean_ms'] for key in ('pread_ms','rabitq_ms','exact_distance_ms','service_ms','server_thread_cpu_ms')]
    lines.append('| '+f"{chosen['target']:.2f}"+' | '+' | '.join(f'{v:.4f}' if v is not None else '未测量' for v in values)+' |')
lines+=['',
'这些工作时间在不同节点可以重叠，service_ms 还包含 pread/估距/精排等子项，CPU 时间与 wall 时间也不能相加。因此累计工作项不提供端到端占比。pread 包含缓存访问，不等于物理 SSD 延迟。','',
'## 输出字段','',
'- query_stage_means.csv：整个配置的平均值。','- per_query_stage_means.csv：按 query_row 分组，对该查询的多次测量求平均。',
'- sample_count 是该项实际有效样本数；mean_ms 是算术均值；total_ms 是累计量。失败请求不混入成功耗时均值，未知值保留 null/CSV 空格。',
'- mean_query_fraction = mean(阶段耗时/该次总延迟)；fraction_of_total_latency = sum(阶段耗时)/sum(总延迟)，两者含义不同。',
'- 数据资源计数不完整的请求不纳入累计工作均值；完整 wall 分解仍可独立汇总。',
'- 原始报告路径和 SHA256 见 source_manifest.json；该派生汇总的代码见 summarize_archived.py。','',
'## 文件导航','',
'- [按目标汇总 CSV](target_stage_means.csv)、[汇总 JSON](target_stage_means.json)。']
for name in processed:lines.append(f'- {name}：[配置平均值]({name}/query_stage_means.csv)、[单查询重复均值]({name}/per_query_stage_means.csv)、[完整 JSON]({name}/timing_summary.json)。')
(root/'SUMMARY.md').write_text('\n'.join(lines)+'\n')
print(json.dumps({'targets':len(selection),'distinct_configurations':len(processed),'example_target_099':processed[selection[-1]['name']]},indent=2))
