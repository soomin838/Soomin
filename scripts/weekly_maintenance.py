from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _prune_jsonl(path: Path, keep_days: int = 30) -> int:
    if not path.exists():
        return 0
    edge = datetime.now(timezone.utc) - timedelta(days=max(1, int(keep_days)))
    kept: list[str] = []
    removed = 0
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            row = json.loads(raw)
        except Exception:
            kept.append(raw)
            continue
        ts = str(row.get("ts_utc", "") or "").strip()
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            kept.append(raw)
            continue
        if dt >= edge:
            kept.append(raw)
        else:
            removed += 1
    path.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
    return removed


def _prune_dir(path: Path, keep_days: int = 14) -> int:
    if not path.exists():
        return 0
    edge = datetime.now(timezone.utc) - timedelta(days=max(1, int(keep_days)))
    removed = 0
    for file in path.rglob("*"):
        if not file.is_file():
            continue
        mtime = datetime.fromtimestamp(file.stat().st_mtime, tz=timezone.utc)
        if mtime < edge:
            try:
                file.unlink()
                removed += 1
            except Exception:
                continue
    return removed


def run(root: Path) -> dict:
    logs = root / "storage" / "logs"
    prompt_packs = root / "storage" / "prompt_packs"
    cache_dirs = [
        root / "storage" / "state" / "library_cache",
        root / "storage" / "temp_images",
    ]
    report = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "jsonl_pruned": {},
        "files_removed": {},
    }
    for name in ["workflow_perf.jsonl", "qa_timing.jsonl", "publisher_upload.jsonl", "ollama_calls.jsonl"]:
        p = logs / name
        report["jsonl_pruned"][name] = _prune_jsonl(p, keep_days=45)
    report["files_removed"]["prompt_packs"] = _prune_dir(prompt_packs, keep_days=21)
    for d in cache_dirs:
        report["files_removed"][str(d)] = _prune_dir(d, keep_days=14)

    out = logs / "weekly_maintenance_report.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[1]
    result = run(repo_root)
    print(json.dumps(result, ensure_ascii=False, indent=2))

