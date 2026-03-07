from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


def _r2_fail_count_from_reason_counter(reason_counter: dict[str, Any]) -> int:
    total = 0
    for code, count in (reason_counter or {}).items():
        token = str(code or "")
        if ("r2_upload" in token) or token.startswith("r2_") or ("missing_r2_config" in token):
            try:
                total += int(count or 0)
            except Exception:
                continue
    return int(total)


def should_apply(plan: dict, summary: dict) -> tuple[bool, str]:
    payload = dict(plan or {})
    guard = dict(summary or {})

    risk = str(payload.get("risk", "") or "").strip().lower()
    if risk != "low":
        return False, "risk_not_low"

    total_runs = int(guard.get("total_runs", 0) or 0)
    if total_runs < 10:
        return False, "insufficient_total_runs"

    recent_total = int(guard.get("recent_3d_total_runs", 0) or 0)
    recent_failed = int(guard.get("recent_3d_failed_count", 0) or 0)
    if recent_total <= 0:
        return False, "insufficient_recent_3d_runs"
    failed_ratio_3d = float(recent_failed / recent_total)
    if failed_ratio_3d >= 0.40:
        return False, "failed_ratio_3d_too_high"

    r2_fail_count = int(guard.get("r2_fail_count", 0) or 0)
    if r2_fail_count <= 0:
        reason_counter = dict(guard.get("reason_counter", {}) or {})
        r2_fail_count = _r2_fail_count_from_reason_counter(reason_counter)
    if r2_fail_count > 0:
        return False, "r2_fail_detected"

    return True, "guards_passed"


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(base or {})
    for key, value in (patch or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(dict(out.get(key) or {}), value)
        else:
            out[key] = value
    return out


def _backup_dir(settings_path: Path) -> Path:
    cfg = Path(settings_path).resolve()
    return (cfg.parent.parent / "storage" / "backups").resolve()


def apply_patch(settings_path: Path, patch: dict) -> bool:
    cfg_path = Path(settings_path).resolve()
    if not cfg_path.exists():
        return False
    try:
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return False
    if not isinstance(raw, dict):
        return False
    patch_obj = dict(patch or {})
    if not patch_obj:
        return True

    backup_dir = _backup_dir(cfg_path)
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = (backup_dir / f"settings_{stamp}.yaml").resolve()
    suffix = 1
    while backup_path.exists():
        backup_path = (backup_dir / f"settings_{stamp}_{suffix}.yaml").resolve()
        suffix += 1

    try:
        backup_path.write_text(cfg_path.read_text(encoding="utf-8"), encoding="utf-8")
    except Exception:
        return False

    merged = _deep_merge(raw, patch_obj)
    try:
        cfg_path.write_text(
            yaml.safe_dump(merged, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        return True
    except Exception:
        return False


def rollback_last_backup(settings_path: Path) -> bool:
    cfg_path = Path(settings_path).resolve()
    backup_dir = _backup_dir(cfg_path)
    if not backup_dir.exists():
        return False
    backups = sorted(
        [p for p in backup_dir.glob("settings_*.yaml") if p.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not backups:
        return False
    latest = backups[0]
    try:
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(latest.read_text(encoding="utf-8"), encoding="utf-8")
        return True
    except Exception:
        return False
