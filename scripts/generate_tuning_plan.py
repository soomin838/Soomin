from __future__ import annotations

import argparse
from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.tuning_plan import build_tuning_plan, render_tuning_plan_md  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate tuning_plan.yaml/md from run metrics")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days (default: 7)")
    parser.add_argument(
        "--root",
        type=str,
        default=str(ROOT),
        help="Repository root path (default: current repo root)",
    )
    args = parser.parse_args()

    repo_root = Path(str(args.root or ROOT)).resolve()
    window_days = max(1, int(args.days or 7))
    reports_dir = (repo_root / "storage" / "reports").resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)

    plan = build_tuning_plan(repo_root, days=window_days)
    md = render_tuning_plan_md(plan)

    yaml_path = (reports_dir / "tuning_plan.yaml").resolve()
    md_path = (reports_dir / "tuning_plan.md").resolve()

    yaml_path.write_text(yaml.safe_dump(plan, sort_keys=False, allow_unicode=True), encoding="utf-8")
    md_path.write_text(md, encoding="utf-8")

    print(f"Generated tuning yaml: {yaml_path}")
    print(f"Generated tuning markdown: {md_path}")
    print(f"Window days: {window_days}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
