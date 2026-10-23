"""Experiment lifecycle journal; never call from a measured query path."""
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import argparse
import fcntl
import json
import os
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
HEADER = "# DHDGS 实验日志\n\n时间使用 Asia/Shanghai（UTC+08:00）。围绕同一问题的一轮实验只保留一条汇总，包含各次测试改变的变量、结果及负面结论。\n单次测试的时间、配置和失败证据保存在 test/ 下；历史补记不推测缺失的起止时间。\n"
FIELDS = (
    ("status", "状态"), ("started_at", "开始时间"), ("ended_at", "结束时间"),
    ("purpose", "目的 / 要解决的问题"), ("hypothesis", "假设"),
    ("expected", "期望效果"), ("configuration", "环境与配置 / 对照"),
    ("variables", "本轮改变的变量"),
    ("code_changes", "代码改动"), ("revision", "登记时 Git HEAD"),
    ("results", "结果与限制"), ("artifacts", "报告、图表与证据"),
    ("next_steps", "下一步建议"), ("updated_at", "日志更新时间"),
)


def now():
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")


def revision():
    result = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"],
                            capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else "未知"


def record(metadata, log_path=None, *, round_id=None):
    """Publish one round summary; child runs only return local evidence metadata.

    A run keeps its own id and timestamps. When round_id differs from id, the
    caller saves the returned metadata in its run directory, without touching
    TestLog.md. No parent result or lifecycle is inferred from a child run.
    """
    data = dict(metadata)
    ident = data["id"]
    parent = round_id if round_id is not None else data.get("round_id", ident)
    for value in (ident, parent):
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
            raise ValueError("Experiment and round ids must use letters, digits, _, . or -")
    data["round_id"] = parent
    for key in ("title", "purpose", "hypothesis", "expected", "configuration",
                "code_changes", "results", "artifacts", "next_steps", "status"):
        if not data.get(key):
            raise ValueError(f"Missing experiment field: {key}")
    data["updated_at"] = now()
    data.setdefault("revision", revision())
    data.setdefault("started_at", "未知，未采集墙钟起始时间")
    data.setdefault("ended_at", "未知，未采集墙钟结束时间")
    if parent != ident:
        return data
    start, end = f"<!-- experiment:{ident}:start -->", f"<!-- experiment:{ident}:end -->"
    lines = [start, f"## {data['title']}（{ident}）", ""]
    for key, label in FIELDS:
        if key == "variables" and not data.get(key):
            continue  # Historical summaries may express variables in configuration.
        value = data[key]
        if isinstance(value, list):
            lines += [f"**{label}**", ""] + [f"- {item}" for item in value] + [""]
        else:
            lines += [f"**{label}**：{value}", ""]
    block = "\n".join(lines) + end + "\n"
    if start in "\n".join(str(v) for v in data.values()) or end in "\n".join(str(v) for v in data.values()):
        raise ValueError("Reserved journal marker in metadata")
    path = Path(log_path) if log_path else ROOT / "TestLog.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    # Offline journal writers only; this lock is unrelated to search threads.
    with path.with_suffix(path.suffix + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        content = path.read_text() if path.exists() else HEADER
        if start in content:
            a = content.index(start)
            b = content.index(end, a) + len(end)
            content = content[:a] + block.rstrip("\n") + content[b:]
        else:
            content = content.rstrip() + "\n\n" + block
        name = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as output:
                name = output.name
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
            os.replace(name, path)
        finally:
            if name and os.path.exists(name):
                os.unlink(name)
    return data


@contextmanager
def experiment(metadata, evidence, log_path=None, *, round_id=None):
    """Track a round or child run, including cleanup, outside query timing."""
    evidence = Path(evidence)
    evidence.parent.mkdir(parents=True, exist_ok=True)
    data = dict(metadata, status="运行中", started_at=now(), ended_at="尚未结束")

    def persist():
        data.update(record(data, log_path, round_id=round_id))
        evidence.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")

    persist()
    try:
        yield data
    except BaseException as exc:
        data.update(status="中断" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "失败",
                    results=f"{type(exc).__name__}: {exc}；已保留失败状态，不作为成功性能结论。")
        raise
    else:
        data["status"] = "测量结束，待分析"
    finally:
        data["ended_at"] = now()
        persist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", required=True, help="YAML or JSON experiment record")
    parser.add_argument("--log", type=Path, default=ROOT / "TestLog.md")
    parser.add_argument("--round-id", help="Parent round; a differing id saves only local evidence")
    args = parser.parse_args()
    import yaml
    source = Path(args.metadata)
    metadata = record(yaml.safe_load(source.read_text()), args.log, round_id=args.round_id)
    if metadata['round_id'] != metadata['id']:
        if source.suffix == '.json':
            source.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + '\n')
        else:
            source.write_text(yaml.safe_dump(metadata, allow_unicode=True, sort_keys=False))


if __name__ == "__main__":
    main()
