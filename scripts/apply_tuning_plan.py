from __future__ import annotations

import argparse
from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.ops_report import read_run_metrics, summarize  # noqa: E402
from core.tuning_apply import apply_patch, should_apply  # noqa: E402


def _r2_fail_count(summary: dict) -> int:
    counter = dict(summary.get("reason_counter", {}) or {})
    total = 0
    for code, count in counter.items():
        token = str(code or "")
        if ("r2_upload" in token) or token.startswith("r2_") or ("missing_r2_config" in token):
            total += int(count or 0)
    return int(total)


def main() -> int:
    parser = argparse.ArgumentParser(description="Guarded auto-apply for tuning_plan.yaml")
    parser.add_argument("--days", type=int, default=7, help="Lookback window for run metrics (default: 7)")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate guards without applying (default mode)")
    parser.add_argument("--auto", action="store_true", help="Apply patch when guard rules pass")
    parser.add_argument(
        "--root",
        type=str,
        default=str(ROOT),
        help="Repository root path (default: current repo root)",
    )
    args = parser.parse_args()

    repo_root = Path(str(args.root or ROOT)).resolve()
    days = max(1, int(args.days or 7))
    dry_run = bool(args.dry_run or (not args.auto))

    tuning_path = (repo_root / "storage" / "reports" / "tuning_plan.yaml").resolve()
    if not tuning_path.exists():
        print("SKIPPED (missing_tuning_plan)")
        return 0
    try:
        plan = yaml.safe_load(tuning_path.read_text(encoding="utf-8")) or {}
    except Exception:
        print("SKIPPED (invalid_tuning_plan_yaml)")
        return 0
    if not isinstance(plan, dict):
        print("SKIPPED (invalid_tuning_plan_shape)")
        return 0

    metrics_path = (repo_root / "storage" / "logs" / "run_metrics.jsonl").resolve()
    metrics_window = read_run_metrics(metrics_path, days=days)
    summary_window = summarize(metrics_window)
    metrics_3d = read_run_metrics(metrics_path, days=3)
    summary_3d = summarize(metrics_3d)

    guard_summary = {
        "total_runs": int(summary_window.get("total_runs", 0) or 0),
        "recent_3d_total_runs": int(summary_3d.get("total_runs", 0) or 0),
        "recent_3d_failed_count": int(summary_3d.get("failed_count", 0) or 0),
        "r2_fail_count": int(_r2_fail_count(summary_window)),
        "reason_counter": dict(summary_window.get("reason_counter", {}) or {}),
    }
    can_apply, reason = should_apply(plan, guard_summary)
    if not can_apply:
        print(f"SKIPPED ({reason})")
        return 0

    if dry_run:
        print(f"SKIPPED (dry_run:{reason})")
        return 0

    settings_path = (repo_root / "config" / "settings.yaml").resolve()
    patch = dict(plan.get("patch", {}) or {})
    ok = apply_patch(settings_path=settings_path, patch=patch)
    if ok:
        print("APPLIED")
    else:
        print("SKIPPED (apply_failed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
