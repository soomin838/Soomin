from __future__ import annotations

import json
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.tuning_apply import rollback_last_backup  # noqa: E402


def _write_settings(temp_root: Path, *, target_images: int = 5) -> Path:
    payload = {
        "timezone": "Asia/Seoul",
        "gemini": {"api_key": ""},
        "visual": {
            "image_provider": "library",
            "enable_gemini_image_generation": False,
            "target_images_per_post": target_images,
            "max_banner_images": 1,
            "max_inline_images": 4,
        },
        "publish": {
            "image_hosting_backend": "r2",
            "min_images_required": 1,
            "max_images_per_post": 5,
            "r2": {
                "endpoint_url": "https://example-r2-endpoint.invalid",
                "bucket": "dummy-bucket",
                "access_key_id": "dummy-access",
                "secret_access_key": "dummy-secret",
                "public_base_url": "https://example-r2-public.invalid",
                "prefix": "news",
            },
        },
        "entropy_check": {"enabled": True, "trigram_max_ratio": 0.05},
        "internal_links": {
            "enabled": True,
            "body_link_count": 1,
            "related_link_count": 2,
            "overlap_threshold": 0.4,
            "canonical_internal_host": "blog.example.com",
        },
        "blogger": {"credentials_path": "config/credentials.json", "blog_id": "dummy-blog"},
        "indexing": {"service_account_path": "config/indexing.json"},
    }
    cfg_dir = temp_root / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "settings.yaml"
    cfg_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return cfg_path


def _write_tuning_plan(temp_root: Path, *, risk: str = "low") -> Path:
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "window_days": 7,
        "signals": {
            "ctr_risk_ratio": 0.35,
            "entropy_fail_count": 3,
            "internal_links_failed": 2,
            "r2_fail_count": 0,
            "success_rate": 0.7,
        },
        "patch": {
            "visual": {"target_images_per_post": 6, "max_inline_images": 5},
            "entropy_check": {"trigram_max_ratio": 0.06},
            "internal_links": {"overlap_threshold": 0.35},
            "publish": {"min_images_required": 0},
        },
        "why": ["test patch"],
        "risk": risk,
    }
    path = temp_root / "storage" / "reports" / "tuning_plan.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return path


def _write_metrics(
    temp_root: Path,
    *,
    total_runs: int = 12,
    failed_in_last_3d: int = 1,
    include_r2_fail: bool = False,
) -> Path:
    now = datetime.now(timezone.utc)
    path = temp_root / "storage" / "logs" / "run_metrics.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []
    # 6 runs in recent 3 days (12-hour spacing)
    recent_runs = 6
    for i in range(total_runs):
        ts = now - timedelta(hours=12 * i)
        is_recent = i < recent_runs
        status = "failed" if (is_recent and i < failed_in_last_3d) else "success"
        reason_codes = ["ok"]
        if status == "failed":
            reason_codes = ["publish_error"]
        if include_r2_fail and i == 0:
            reason_codes.append("r2_upload_exception")
        rows.append(
            {
                "ts_utc": ts.isoformat(),
                "run_id": f"stage23-{i+1:02d}",
                "status": status,
                "reason_codes": reason_codes,
                "topic_cluster": "security",
                "focus_keywords": ["security", "patch", "update"],
                "seo_slug": f"security-patch-update-{i+1}",
                "title": f"Security update {i+1}",
                "published_url": f"https://blog.example.com/p/{i+1}" if status == "success" else "",
                "publish_at_utc": ts.isoformat(),
                "images_count": 2,
                "internal_links_count": 1,
                "related_links_count": 1,
                "ctr_risk_low_visual_density": False,
                "entropy_ok": status == "success",
            }
        )
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


def _run_apply(root: Path, *, auto: bool = True) -> str:
    cmd = [
        sys.executable,
        str((ROOT / "scripts" / "apply_tuning_plan.py").resolve()),
        "--root",
        str(root),
        "--days",
        "7",
    ]
    if auto:
        cmd.append("--auto")
    else:
        cmd.append("--dry-run")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    out = (proc.stdout or "").strip()
    return out


def _load_settings(settings_path: Path) -> dict:
    return yaml.safe_load(settings_path.read_text(encoding="utf-8")) or {}


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="stage23_apply_") as td:
        root = Path(td).resolve()
        settings_path = _write_settings(root, target_images=5)
        _write_tuning_plan(root, risk="low")
        _write_metrics(root, total_runs=12, failed_in_last_3d=1, include_r2_fail=False)

        # 1) low risk + enough runs -> apply success
        out = _run_apply(root, auto=True)
        if "APPLIED" not in out:
            raise AssertionError(f"Case1 failed: expected APPLIED, got: {out}")

        # 4) settings actually changed
        changed = _load_settings(settings_path)
        if int(changed.get("visual", {}).get("target_images_per_post", 0)) != 6:
            raise AssertionError("Case4 failed: settings.yaml was not updated by apply.")

        # 5) rollback works
        if not rollback_last_backup(settings_path):
            raise AssertionError("Case5 failed: rollback_last_backup returned False.")
        rolled = _load_settings(settings_path)
        if int(rolled.get("visual", {}).get("target_images_per_post", 0)) != 5:
            raise AssertionError("Case5 failed: rollback did not restore previous settings.")

        # 2) risk=medium -> skip
        _write_tuning_plan(root, risk="medium")
        _write_settings(root, target_images=5)
        _write_metrics(root, total_runs=12, failed_in_last_3d=1, include_r2_fail=False)
        out2 = _run_apply(root, auto=True)
        if "SKIPPED (risk_not_low)" not in out2:
            raise AssertionError(f"Case2 failed: expected risk_not_low skip, got: {out2}")

        # 3) failed ratio high -> skip
        _write_tuning_plan(root, risk="low")
        _write_settings(root, target_images=5)
        _write_metrics(root, total_runs=12, failed_in_last_3d=3, include_r2_fail=False)
        out3 = _run_apply(root, auto=True)
        if "SKIPPED (failed_ratio_3d_too_high)" not in out3:
            raise AssertionError(f"Case3 failed: expected failed_ratio_3d_too_high skip, got: {out3}")

        print("Case 1 PASS: low-risk plan applies when guard conditions pass")
        print("Case 2 PASS: medium-risk plan is blocked")
        print("Case 3 PASS: high recent failed ratio blocks auto-apply")
        print("Case 4 PASS: settings.yaml is updated on apply")
        print("Case 5 PASS: rollback_last_backup restores previous settings")
        print("Stage-23 auto-apply validation passed.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
