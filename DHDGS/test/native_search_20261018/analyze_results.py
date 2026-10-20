"""Independently audit raw records and derive corrected resource summaries."""
import gzip
import hashlib
import json
from pathlib import Path
import sys
import numpy as np
import yaml

ROOT=Path(__file__).resolve().parent;PROJECT=ROOT.parent.parent
sys.path[:0]=[str(PROJECT/'tools'),str(PROJECT/'python')]
from deploy_cluster import save,digest
from dhdgs.data import Dataset,load_complete_npy


def main():
    completed=json.loads((ROOT/'completed.json').read_text())
    sources=json.loads((ROOT/'sources.json').read_text())
    for name,sha in sources['sources'].items():assert digest(PROJECT/name)==sha,name
    assert digest(next((PROJECT/'python/dhdgs').glob('_native*.so')))==sources['native']
    assert digest(ROOT/'executed_run_experiment.py')==sources['runner']
    selection=json.loads((ROOT/'query_selection.json').read_text())
    plan=yaml.safe_load((ROOT/'experiment.yaml').read_text())
    dataset=Dataset(plan['dhdgs']['paths']['dataset'])
    queries=load_complete_npy(Path(plan['dhdgs']['query']['vectors']))
    distance_checks=0
    summaries=[]
    for folder in sorted([*ROOT.glob('calibration/b*'),*ROOT.glob('validation/*')]):
        run=folder/'run'
        for entry in json.loads((run/'archive.json').read_text()):
            with gzip.open(run/(entry['file']+'.gz'),'rb') as f:
                assert hashlib.sha256(f.read()).hexdigest()==entry['uncompressed_sha256']
        with gzip.open(run/'report.json.gz','rt') as f:report=json.load(f)
        s=json.loads((folder/'summary.json').read_text())
        expected=selection['calibration' if folder.parent.name=='calibration' else 'validation']
        assert sorted(q['query_row'] for q in report['queries'])==expected
        with gzip.open(run/'queries.jsonl.gz','rt') as f:
            assert [json.loads(line) for line in f]==report['queries']
        assert all(r['stopped'] and r['exit_code'] in (0,-15) for r in json.loads((run/'services.json').read_text())['services'])
        for q in report['queries']:
            assert q['status']=='ok' and q['stats']['metrics_complete']
            assert q['recall']==len(set(q['ids'])&set(q['truth_ids']))/10
            assert q['stats']['expanded']<=s['budget'] and q['stats']['node_revisit_count']==0
            assert q['stats']['control_errors']==0 and q['stats']['router_expand_rpc_count']==0
            reports=q['branch_reports'];assert len({b['node_id'] for b in reports})==len(reports)
            for b in reports:
                assert b['work_grant']==b['work_spent']+b['work_unused']+sum(ch['work_grant'] for ch in b['children'])
                if s['model']=='native':assert b['native_search_loop'] and b['compute_thread_migrations']==0
                for ch in b['children']:
                    assert sum(d['remote'] and d['decision_id']==ch['decision_id'] and d['selected_node']==ch['node_id'] for d in b['decisions'])==1
        # Independent float64 subtraction on original 2560-D dataset values.
        for q in sorted(report['queries'],key=lambda q:q['query_row'])[:32]:
            x=dataset.take(np.array(q['ids'],dtype='u8')).astype('f8')
            delta=x-queries[q['query_row']].astype('f8')
            np.testing.assert_allclose(q['squared_distances'],np.einsum('ij,ij->i',delta,delta),rtol=1e-12,atol=1e-10)
            distance_checks+=len(q['ids'])
        # TCP snapshots have worker=None, because all clients share the same
        # data services. The initial collector incorrectly filtered worker==0.
        nodes=report['node_process_load'];assert len(nodes)==3
        s['nodes']=nodes;s['cpu_cores_sum']=sum(n['mean_cpu_cores'] for n in nodes)
        s['physical_read_bytes']=sum(n['process_deltas']['storage_read_bytes'] for n in nodes)
        s['resource_summary_correction']='Recomputed from archived TCP process snapshots (worker=None); raw measurements unchanged'
        s['native_compute_threads_by_node']={str(sid):sorted({b['compute_thread_id'] for q in report['queries'] for b in q['branch_reports'] if b['node_id']==sid and 'compute_thread_id' in b}) for sid in range(3)}
        s['io_submission_query_counts_since_start']={str(n['shard_id']):n.get('peer_lifecycle_after',{}).get('io',{}).get('submission_query_counts',{}) for n in nodes}
        save(folder/'summary.json',s);summaries.append(s)
    assert len(summaries)==completed['points']
    assert sum(s['queries'] for s in summaries)==completed['requests']
    save(ROOT/'comparison.json',summaries)
    save(ROOT/'verification.json',{'passed':True,'points':len(summaries),'queries':sum(s['queries'] for s in summaries),
         'original_vector_exact_distance_checks':distance_checks,
         'archives_verified':True,'sources_verified':True,'all_services_stopped':True,
         'resource_summary_corrected_from_raw_records':True})
    lines=['# C++ 完整搜索循环：完整 2M 索引验证','',
           '范围：Intel-35 单机、三个独立 TCP 数据进程；完整 2,000,000 × 2560 索引，原始 L2、RaBitQ。buffered I/O，OS 缓存不清空。不是三台物理机器的新版本性能结果。','',
           '每分支一个固定计算线程；不同查询并行。Python 仍负责协议和聚合 I/O 边界。Native 不在候选/距离循环中运行 Python。', '',
           '| 配置 | 预算/frontier | 线程/节点 | 客户端并发 | 查询数 | Recall@10 | QPS | 平均 ms | p99 ms | 数据进程总用核 |',
           '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for s in summaries:
        lines.append(f"| {s['name']} | {s['budget']}/{s['frontier']} | {s['threads']} | {s['concurrency']} | {s['queries']} | {s['recall']:.6f} | {s['qps']:.2f} | {s['latency_ms']['mean']:.2f} | {s['latency_ms']['p99']:.2f} | {s['cpu_cores_sum']:.3f} |")
    lines+=['','128 条校准查询与 1024 条验证查询互不重叠。预算按校准集合中达标配置的最低 p99、其次 QPS 选择，再执行四组验证。各配置仅一个窗口；不能声称统计显著性或全局最优。','',
            '源码、原生模块、实验参数、原始报告/逐查询压缩哈希、召回重算、预算、单次节点访问、派发批准与服务退出独立核验通过，见 verification.json。',
            '初次采集脚本重复重命名真值计时字段而退出，记录保存在 collector_attempt1。正式运行的初始节点资源汇总误筛 worker==0，已依据原始 TCP worker=None 三节点快照修正；原始数据未改。executed_run_experiment.py 保留实际执行版本。',
            '所有查询成功且指标完整。详细逐查询指标、阶段统计、节点资源 CSV 和 report.json.gz / queries.jsonl.gz 均在各组 run/ 下。',
            '最终 79 项回归通过，含固定线程归属、跨查询 I/O 聚合、TCP、取消、预算以及 io_uring 等价性。原子统计移除应用互斥锁，生命周期与缓冲管理的必要同步保留。', '']
    existing=(ROOT/'SUMMARY.md').read_text() if (ROOT/'SUMMARY.md').exists() else ''
    appendix='\n## 单数据节点 CPU 并行诊断'+existing.split('\n## 单数据节点 CPU 并行诊断',1)[1] if '\n## 单数据节点 CPU 并行诊断' in existing else ''
    (ROOT/'SUMMARY.md').write_text('\n'.join(lines)+appendix)
    print(json.dumps({'verified':len(summaries),'queries':completed['requests']}))


if __name__=='__main__':main()
