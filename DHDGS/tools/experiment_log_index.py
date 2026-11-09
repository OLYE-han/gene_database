"""Compact, grouped experiment index; detailed records stay outside TestLog.md."""
import html
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlsplit

MARKER = '<!-- testlog:compact-v1 -->'
HEADER = ('# DHDGS 实验日志\n\n' + MARKER + '\n\n'
          '按问题归组，组内按开始时间从早到晚排列；时间为 Asia/Shanghai（UTC+08:00），'
          '每轮一行，子测试见报告，详细配置、改动及失败记录见对应详细日志。\n')
BLOCK = re.compile(r'<!-- experiment:([A-Za-z0-9_.-]+):start -->(.*?)'
                   r'<!-- experiment:\1:end -->', re.S)


def atomic_write(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                         delete=False) as out:
            name = out.name
            out.write(text)
            out.flush()
            os.fsync(out.fileno())
        os.replace(name, path)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def slug(title):
    return re.sub(r'[^\w\- ]', '', title.lower()).replace(' ', '-')


def relocate_links(text, source_dir, target_dir):
    def replace(m):
        dest = m[2]
        if dest.startswith(('#', '/')) or urlsplit(dest).scheme:
            return m[0]
        path, sep, fragment = dest.partition('#')
        relative = os.path.relpath(Path(source_dir) / path, target_dir)
        return f'[{m[1]}]({relative}{sep}{fragment})'
    return re.sub(r'\[([^\]]*)\]\(([^\s)]+)\)', replace, text)


def group_for(data):
    if data.get('log_group'):
        return data['log_group']
    research = data.get('research_ids', [])
    if not research:
        research = re.findall(r'R\d{3}', str(data.get('purpose', '')))
    names = {'R001': '负载均衡：其他实验', 'R002': '查询接纳、锁等待与长期稳定性',
             'R003': 'SSD访问与存储', 'R004': '搜索线程、并发与CPU资源',
             'R005': '搜索预算与跨节点扩展', 'R006': '网络通信与动态扩缩容',
             'R007': 'RDMA数据访问', 'R008': '评测接口与实验流程',
             'R009': '距离与量化质量', 'R010': '图导航与访问正确性'}
    return names.get(research[0], '其他实验（待归类）') if research else '其他实验（待归类）'


def one_line(value, limit=220):
    text = re.sub(r'\s+', ' ', str(value)).strip()
    return text if len(text) <= limit else text[:limit].rstrip() + '…'


def make_entry(data, old=None):
    old = old or {}
    summary = data.get('summary')
    if not summary:
        # Do not silently retain an obsolete curated conclusion when results change.
        summary = f"{data['title']}：{one_line(data['results'], 150)}"
    summary = one_line(summary)
    summary = summary.replace('。', '；').rstrip('；。 ')
    report = data.get('report')
    if not report:
        links = re.findall(r'\[[^\]]*\]\(([^)]+)\)', str(data['artifacts']))
        report = next((p for p in links if Path(p.split('#')[0]).name.upper()
                       in ('REPORT.MD', 'CONCLUSION.MD')), None)
    aliases = list(dict.fromkeys(old.get('aliases', []) +
                   [slug(f"{data['title']}（{data['id']}）")]))
    return dict(id=data['id'], group=data.get('log_group') or old.get('group') or group_for(data),
                started_at=data['started_at'], status=data['status'], summary=summary,
                report=report, report_label=data.get('report_label', '报告'), aliases=aliases)


def parse(content, legacy=None):
    entries = []
    for m in BLOCK.finditer(content):
        meta = re.search(r'<!-- testlog-entry:(.*?) -->', m[2])
        if not meta:
            if legacy is None:
                raise ValueError(f'Legacy block {m[1]} needs migration; refusing to discard it')
            entry = legacy(m)
        else:
            entry = json.loads(meta[1])
        if entry['id'] != m[1]:
            raise ValueError('Journal entry identity mismatch')
        entries.append(entry)
    if 2 * len(entries) != content.count('<!-- experiment:'):
        raise ValueError('Unbalanced journal blocks')
    if len({e['id'] for e in entries}) != len(entries):
        raise ValueError('Duplicate journal ids')
    return entries


def sort_key(entry):
    date = re.search(r'\d{4}-\d\d-\d\d(?:T\d\d:\d\d:\d\d)?', entry['started_at'])
    return (date[0] if date else '9999', entry['id'])


def render(entries, preamble=HEADER):
    groups = {}
    for entry in sorted(entries, key=sort_key):
        groups.setdefault(entry['group'], []).append(entry)
    lines = [preamble.rstrip(), '']
    for group, members in groups.items():
        lines += [f'## {one_line(group)}', '']
        for e in members:
            ident = e['id']
            date = sort_key(e)[0].replace('T', ' ')
            if date == '9999':
                date = '时间未采集'
            # Full machine-readable metadata stays on one hidden line.
            meta = json.dumps(e, ensure_ascii=False, separators=(',', ':')).replace('-->', '--\\u003e')
            anchors = ''.join(f'<a id="{html.escape(a, quote=True)}"></a>'
                              for a in dict.fromkeys([ident] + e['aliases']))
            details = f'test/log_archive/rounds/{ident}.md'
            report = f"[{e.get('report_label', '报告')}]({e['report']})" if e.get('report') else '无独立报告/待生成'
            lines += [f'<!-- experiment:{ident}:start -->', f'<!-- testlog-entry:{meta} -->',
                      f'- {anchors}**{date}**（{one_line(e["status"])}）：{e["summary"]}（{report}；[详细记录]({details})）。',
                      f'<!-- experiment:{ident}:end -->', '']
    return '\n'.join(lines)


def publish(data, content, path, detailed_block):
    directory = path.parent / 'test/log_archive/rounds'

    def legacy(m):
        # A runner imported before migration may still write the old format.
        # Preserve its latest detail and normalize only its index entry.
        saved = directory / (m[1] + '.json')
        previous = json.loads(saved.read_text()) if saved.exists() else {}
        previous.pop('summary', None)
        previous.pop('report', None)
        previous.pop('report_label', None)
        previous.update(id=m[1])
        heading = re.search(r'^## (.*)', m[2], re.M)
        previous['title'] = heading[1].removesuffix(f'（{m[1]}）') if heading else m[1]
        for key, label in [('started_at', '开始时间'), ('ended_at', '结束时间'),
                           ('status', '状态'), ('results', '结果与限制'),
                           ('artifacts', '报告、图表与证据'), ('purpose', '目的 / 要解决的问题'),
                           ('hypothesis', '假设'), ('expected', '期望效果'),
                           ('configuration', '环境与配置 / 对照'), ('variables', '本轮改变的变量'),
                           ('code_changes', '代码改动'), ('revision', '登记时 Git HEAD'),
                           ('next_steps', '下一步建议'), ('updated_at', '日志更新时间')]:
            value = re.search(r'\*\*' + label + r'\*\*[:：]?\s*(.*?)(?=\n\*\*|\Z)', m[2], re.S)
            previous[key] = value[1].strip() if value else '未知'
        atomic_write(directory / (m[1] + '.md'), relocate_links(m[0], path.parent, directory))
        atomic_write(saved, json.dumps(previous, ensure_ascii=False, indent=2) + '\n')
        return make_entry(previous)

    entries = parse(content, legacy)
    old = next((e for e in entries if e['id'] == data['id']), None)
    entry = make_entry(data, old)
    detailed = directory / (data['id'] + '.md')
    atomic_write(detailed, relocate_links(detailed_block, path.parent, directory))
    atomic_write(directory / (data['id'] + '.json'),
                 json.dumps(data, ensure_ascii=False, indent=2) + '\n')
    entries = [e for e in entries if e['id'] != data['id']] + [entry]
    # Keep migration/retention notices and archive navigation intact.
    preamble = content.split('\n## ', 1)[0]
    return render(entries, preamble)
