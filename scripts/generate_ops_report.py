from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.ops_report import read_run_metrics, render_markdown, summarize, write_csv  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate weekly ops report from run_metrics.jsonl")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days (default: 7)")
    parser.add_argument(
        "--root",
        type=str,
        default=str(ROOT),
        help="Repository root path (default: current repo root)",
    )
    args = parser.parse_args()

    repo_root = Path(str(args.root or ROOT)).resolve()
    days = max(1, int(args.days or 7))
    metrics_path = (repo_root / "storage" / "logs" / "run_metrics.jsonl").resolve()
    reports_dir = (repo_root / "storage" / "reports").resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)

    metrics = read_run_metrics(metrics_path, days=days)
    summary = summarize(metrics)
    md = render_markdown(summary, metrics)

    md_path = (reports_dir / "weekly_ops_report.md").resolve()
    csv_path = (reports_dir / "weekly_ops_report.csv").resolve()

    md_path.write_text(md, encoding="utf-8")
    write_csv(metrics, csv_path)

    print(f"Generated markdown: {md_path}")
    print(f"Generated csv: {csv_path}")
    print(f"Rows analyzed: {len(metrics)} (days={days})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
