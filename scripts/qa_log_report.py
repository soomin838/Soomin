from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _parse_ts(raw: str) -> datetime | None:
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    path = root / "storage" / "logs" / "qa_runtime.jsonl"
    if not path.exists():
        print(f"log not found: {path}")
        return

    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=72)
    recent = [r for r in rows if (_parse_ts(str(r.get("ts_utc", ""))) or now) >= since]

    by_part = Counter(str(r.get("part", "")) for r in recent)
    by_event = Counter(str(r.get("event", "")) for r in recent)
    failures = [r for r in recent if str(r.get("event")) == "run_failed"]
    publishes = [r for r in recent if str(r.get("event")) == "run_finished"]
    success_count = sum(1 for r in publishes if str(r.get("status", "")).lower() == "success")

    out = {
        "window_hours": 72,
        "total_events": len(recent),
        "parts": dict(by_part),
        "events": dict(by_event),
        "publish_runs": len(publishes),
        "publish_success": success_count,
        "publish_success_rate": round((success_count / len(publishes)) * 100, 2) if publishes else 0.0,
        "failure_count": len(failures),
        "failure_classes": dict(Counter(str(f.get("error_class", "unknown")) for f in failures)),
        "last_event_utc": recent[-1]["ts_utc"] if recent else None,
    }

    out_path = root / "storage" / "logs" / "qa_runtime_report.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"report written: {out_path}")
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
