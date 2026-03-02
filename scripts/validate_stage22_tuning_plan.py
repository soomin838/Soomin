from __future__ import annotations

import json
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent


def _write_settings(temp_root: Path) -> Path:
    payload = {
        "timezone": "Asia/Seoul",
        "gemini": {"api_key": ""},
        "visual": {
            "image_provider": "library",
            "enable_gemini_image_generation": False,
            "target_images_per_post": 5,
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


def _write_metrics(temp_root: Path) -> None:
    path = temp_root / "storage" / "logs" / "run_metrics.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(12):
        status = "success" if i % 3 != 0 else "hold"
        reason_codes: list[str] = []
        if i < 6:
            reason_codes.append("ctr_risk_low_visual_density")
        if i in {1, 2, 5}:
            reason_codes.append("entropy_fail:trigram_ratio")
        if i in {3, 7}:
            reason_codes.append("internal_links_failed")
        if i in {4, 8}:
            reason_codes.append("r2_upload_exception")
        rows.append(
            {
                "ts_utc": (now - timedelta(hours=i * 10)).isoformat(),
                "run_id": f"stage22-{i+1:02d}",
                "status": status,
                "reason_codes": reason_codes,
                "topic_cluster": "security" if i % 2 == 0 else "policy",
                "focus_keywords": ["security", "patch", "update"],
                "seo_slug": f"security-patch-update-{i+1}",
                "title": f"Security update {i+1}",
                "published_url": f"https://blog.example.com/p/{i+1}" if status == "success" else "",
                "publish_at_utc": (now - timedelta(hours=i * 10)).isoformat(),
                "images_count": 2,
                "internal_links_count": 1,
                "related_links_count": 1,
                "ctr_risk_low_visual_density": bool(i < 6),
                "entropy_ok": bool(i not in {1, 2, 5}),
            }
        )
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="stage22_tuning_") as td:
        temp_root = Path(td).resolve()
        _write_settings(temp_root)
        _write_metrics(temp_root)

        cmd = [
            sys.executable,
            str((ROOT / "scripts" / "generate_tuning_plan.py").resolve()),
            "--days",
            "7",
            "--root",
            str(temp_root),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if proc.returncode != 0:
            raise AssertionError("Case1 failed: generate_tuning_plan.py did not exit cleanly.")

        yaml_path = temp_root / "storage" / "reports" / "tuning_plan.yaml"
        md_path = temp_root / "storage" / "reports" / "tuning_plan.md"
        if not yaml_path.exists() or not md_path.exists():
            raise AssertionError("Case2 failed: tuning plan outputs were not created.")

        plan = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        patch = dict(plan.get("patch", {}) or {})
        required_top = ["visual", "entropy_check", "internal_links", "publish"]
        missing = [k for k in required_top if k not in patch]
        if missing:
            raise AssertionError(f"Case3 failed: expected patch keys missing: {missing}")

        md_text = md_path.read_text(encoding="utf-8")
        for section in ("## Signals", "## Proposed Patch", "## Why", "## Risk"):
            if section not in md_text:
                raise AssertionError(f"Case4 failed: markdown missing section: {section}")

        print("Case 1 PASS: tuning plan generator script runs in local mode")
        print("Case 2 PASS: tuning_plan.yaml and tuning_plan.md are generated")
        print("Case 3 PASS: patch includes expected keys from ops signals")
        print("Case 4 PASS: markdown contains required sections")
        print("Stage-22 tuning plan validation passed.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
