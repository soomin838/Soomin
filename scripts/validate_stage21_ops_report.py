from __future__ import annotations

import csv
import json
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent


def _build_sample_metrics(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    statuses = ["success", "skipped", "hold", "failed", "success", "success", "hold", "skipped", "success", "failed"]
    topics = ["security", "policy", "platform", "default", "ai", "mobile", "security", "policy", "chips", "privacy"]
    reasons = [
        ["ok"],
        ["entropy_fail:trigram_ratio"],
        ["internal_links_failed", "r2_upload_exception"],
        ["publish_error"],
        ["ctr_risk_low_visual_density"],
        ["ok"],
        ["r2_public_url_invalid"],
        ["ledger_skip"],
        ["ok"],
        ["internal_links_failed"],
    ]
    rows: list[dict] = []
    for i in range(10):
        ts = (now - timedelta(hours=i * 8)).isoformat()
        rows.append(
            {
                "ts_utc": ts,
                "run_id": f"run-{i+1:02d}",
                "status": statuses[i],
                "reason_codes": reasons[i],
                "topic_cluster": topics[i],
                "focus_keywords": [topics[i], "update", "coverage"],
                "seo_slug": f"{topics[i]}-update-coverage-{i+1}",
                "title": f"{topics[i].title()} update {i+1}",
                "published_url": f"https://blog.example.com/{topics[i]}-{i+1}" if statuses[i] == "success" else "",
                "publish_at_utc": ts,
                "images_count": (i % 4) + 1,
                "internal_links_count": (i % 3),
                "related_links_count": (i % 2) + 1,
                "ctr_risk_low_visual_density": bool(i % 4 == 0),
                "entropy_ok": bool(i % 5 != 1),
            }
        )
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return len(rows)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="stage21_ops_report_") as td:
        temp_root = Path(td).resolve()
        metrics_path = temp_root / "storage" / "logs" / "run_metrics.jsonl"
        input_rows = _build_sample_metrics(metrics_path)

        cmd = [
            sys.executable,
            str((ROOT / "scripts" / "generate_ops_report.py").resolve()),
            "--days",
            "7",
            "--root",
            str(temp_root),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if proc.returncode != 0:
            raise AssertionError("Case1 failed: generate_ops_report.py did not exit cleanly.")

        md_path = temp_root / "storage" / "reports" / "weekly_ops_report.md"
        csv_path = temp_root / "storage" / "reports" / "weekly_ops_report.csv"
        if not md_path.exists() or not csv_path.exists():
            raise AssertionError("Case2 failed: expected report files were not generated.")

        md_text = md_path.read_text(encoding="utf-8")
        required_sections = ["## Summary", "## Top reason codes", "## Topic breakdown", "## Recommendations"]
        for section in required_sections:
            if section not in md_text:
                raise AssertionError(f"Case3 failed: markdown missing section: {section}")

        with csv_path.open("r", encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
        if len(rows) != input_rows:
            raise AssertionError(f"Case4 failed: csv row count mismatch ({len(rows)} != {input_rows}).")

        print("Case 1 PASS: generate_ops_report script runs in local-only mode")
        print("Case 2 PASS: markdown/csv outputs are created")
        print("Case 3 PASS: markdown includes required sections")
        print("Case 4 PASS: csv row count matches input metrics")
        print("Stage-21 ops report validation passed.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
