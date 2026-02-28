from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _appdata_root() -> Path:
    raw = str(os.getenv("APPDATA", "") or "").strip()
    if raw:
        return Path(raw).resolve()
    return (Path.home() / "AppData" / "Roaming").resolve()


def _backup_root() -> Path:
    return (_appdata_root() / "RezeroAgent" / "secret_backups").resolve()


def _target_files(project_root: Path) -> list[dict[str, Any]]:
    root = Path(project_root).resolve()
    app_cfg = (_appdata_root() / "RezeroAgent" / "config" / "settings.yaml").resolve()
    app_token = (_appdata_root() / "RezeroAgent" / "config" / "blogger_token.json").resolve()
    app_client = (_appdata_root() / "RezeroAgent" / "config" / "client_secrets.json").resolve()
    raw = [
        {"name": "repo_settings", "path": (root / "config" / "settings.yaml").resolve()},
        {"name": "appdata_settings", "path": app_cfg},
        {"name": "repo_blogger_token", "path": (root / "config" / "blogger_token.json").resolve()},
        {"name": "repo_client_secrets", "path": (root / "config" / "client_secrets.json").resolve()},
        {"name": "appdata_blogger_token", "path": app_token},
        {"name": "appdata_client_secrets", "path": app_client},
    ]
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in raw:
        p = Path(row.get("path")).resolve()
        key = str(p).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": row.get("name", "file"), "path": p})
    return out


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _combined_hash(items: list[dict[str, Any]]) -> str:
    h = hashlib.sha256()
    for row in items:
        h.update(str(row.get("name", "")).encode("utf-8", errors="ignore"))
        h.update(str(row.get("sha256", "")).encode("utf-8", errors="ignore"))
    return h.hexdigest()


def backup_runtime_secrets(project_root: Path, *, force: bool = False) -> dict[str, Any]:
    root = Path(project_root).resolve()
    backup_root = _backup_root()
    state_path = backup_root / "backup_state.json"
    latest_dir = backup_root / "latest"
    backup_root.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    for target in _target_files(root):
        name = str(target.get("name", "") or "").strip()
        path = Path(target.get("path")).resolve()
        if not path.exists():
            missing.append(name)
            continue
        try:
            rows.append(
                {
                    "name": name,
                    "source_path": str(path),
                    "sha256": _sha256_file(path),
                    "size": int(path.stat().st_size),
                }
            )
        except Exception:
            missing.append(name)

    combo = _combined_hash(rows)
    prev_hash = ""
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(state, dict):
                prev_hash = str(state.get("last_hash", "") or "").strip()
        except Exception:
            prev_hash = ""

    if (not force) and combo and prev_hash and combo == prev_hash:
        return {
            "status": "unchanged",
            "backup_root": str(backup_root),
            "tracked_files": len(rows),
            "missing": missing,
        }

    stamp = _utc_now_iso()
    snap_dir = backup_root / stamp
    snap_dir.mkdir(parents=True, exist_ok=True)

    # refresh latest mirror
    if latest_dir.exists():
        try:
            shutil.rmtree(latest_dir)
        except Exception:
            pass
    latest_dir.mkdir(parents=True, exist_ok=True)

    copied: list[dict[str, Any]] = []
    for row in rows:
        src = Path(str(row.get("source_path", "") or ""))
        if not src.exists():
            continue
        safe_name = str(row.get("name", "file") or "file").strip()
        ext = src.suffix or ".bin"
        snap_file = snap_dir / f"{safe_name}{ext}"
        latest_file = latest_dir / f"{safe_name}{ext}"
        try:
            shutil.copy2(src, snap_file)
            shutil.copy2(src, latest_file)
            copied.append(
                {
                    "name": safe_name,
                    "snapshot_path": str(snap_file),
                    "latest_path": str(latest_file),
                    "source_path": str(src),
                    "sha256": str(row.get("sha256", "") or ""),
                    "size": int(row.get("size", 0) or 0),
                }
            )
        except Exception:
            continue

    manifest = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "snapshot": str(snap_dir),
        "latest": str(latest_dir),
        "files": copied,
        "missing": missing,
    }
    (snap_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (latest_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    state = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "last_hash": combo,
        "last_snapshot": str(snap_dir),
        "last_latest": str(latest_dir),
        "tracked_files": int(len(copied)),
    }
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    # Keep only newest 20 snapshots.
    snaps = sorted(
        [p for p in backup_root.iterdir() if p.is_dir() and p.name not in {"latest"}],
        key=lambda p: p.name,
    )
    for old in snaps[:-20]:
        try:
            shutil.rmtree(old)
        except Exception:
            pass

    return {
        "status": "ok",
        "backup_root": str(backup_root),
        "snapshot": str(snap_dir),
        "tracked_files": len(copied),
        "missing": missing,
    }


def restore_runtime_secrets(project_root: Path, *, from_latest: bool = True) -> dict[str, Any]:
    root = Path(project_root).resolve()
    backup_root = _backup_root()
    src_dir = backup_root / "latest" if from_latest else backup_root
    manifest_path = src_dir / "manifest.json"
    if not manifest_path.exists():
        return {"status": "missing_manifest", "path": str(manifest_path)}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {"status": "manifest_parse_failed", "path": str(manifest_path)}
    files = list(payload.get("files", []) or [])
    restored = 0
    for row in files:
        latest_path = Path(str(row.get("latest_path", "") or "")).resolve()
        source_path = Path(str(row.get("source_path", "") or "")).resolve()
        if not latest_path.exists():
            continue
        try:
            source_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(latest_path, source_path)
            restored += 1
        except Exception:
            continue
    return {
        "status": "ok" if restored > 0 else "no_files_restored",
        "restored": int(restored),
        "source": str(src_dir),
    }
