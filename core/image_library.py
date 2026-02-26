from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .image_optimizer import optimize_for_library
from .visual import ImageAsset


LIB_ROOT = Path("assets/library")


CATEGORY_MAP = {
    "windows": "windows",
    "win11": "windows",
    "mac": "mac",
    "macos": "mac",
    "iphone": "iphone",
    "ios": "iphone",
    "galaxy": "galaxy",
    "android": "galaxy",
    "network": "network",
    "wifi": "network",
    "audio": "audio",
    "sound": "audio",
    "speaker": "audio",
    "microphone": "audio",
}


def detect_category(title: str) -> str:
    lower = str(title or "").lower()
    for key, cat in CATEGORY_MAP.items():
        if key in lower:
            return cat
    return "generic"


def _usage_state_path(root: Path) -> Path:
    return (root / "storage" / "state" / "image_library_usage.json").resolve()


def _load_usage(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _save_usage(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _collect_images(folder: Path) -> list[Path]:
    out: list[Path] = []
    for ext in ("*.png", "*.jpg", "*.jpeg", "*.webp"):
        out.extend(folder.glob(ext))
    return [p for p in out if p.is_file()]


def _fallback_paths(root: Path) -> list[Path]:
    out: list[Path] = []
    for rel in ("assets/fallback/banner.png", "assets/fallback/inline.png"):
        p = (root / rel).resolve()
        if p.exists():
            out.append(p)
    return out


def pick_images(title: str, min_count: int = 2, root: Path | None = None) -> list[ImageAsset]:
    project_root = (root or Path(__file__).resolve().parent.parent).resolve()
    lib_root = (project_root / LIB_ROOT).resolve()
    cache_dir = (project_root / "storage" / "state" / "library_cache").resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    category = detect_category(title)
    category_dir = (lib_root / category).resolve()
    generic_dir = (lib_root / "generic").resolve()

    candidates: list[Path] = []
    candidates.extend(_collect_images(category_dir) if category_dir.exists() else [])
    if len(candidates) < int(min_count):
        candidates.extend(_collect_images(generic_dir) if generic_dir.exists() else [])

    usage_path = _usage_state_path(project_root)
    usage = _load_usage(usage_path)
    counts = dict((usage.get("counts", {}) or {}))

    def _usage_key(p: Path) -> str:
        try:
            return str(p.resolve().relative_to(project_root)).replace("\\", "/")
        except Exception:
            return str(p.resolve()).replace("\\", "/")

    scored: list[tuple[int, float, Path]] = []
    for p in candidates:
        key = _usage_key(p)
        use_count = int(counts.get(key, 0) or 0)
        scored.append((use_count, random.random(), p))
    scored.sort(key=lambda t: (t[0], t[1]))

    selected_paths = [p for _, _, p in scored[: max(2, int(min_count))]]
    if len(selected_paths) < max(2, int(min_count)):
        fallbacks = _fallback_paths(project_root)
        for fb in fallbacks:
            if fb not in selected_paths:
                selected_paths.append(fb)
            if len(selected_paths) >= max(2, int(min_count)):
                break

    now = datetime.now(timezone.utc).isoformat()
    used_map = dict((usage.get("last_used", {}) or {}))
    assets: list[ImageAsset] = []
    for p in selected_paths[: max(2, int(min_count))]:
        use_path = p
        try:
            if p.exists() and int(p.stat().st_size) > (2 * 1024 * 1024):
                opt_path = optimize_for_library(
                    p,
                    cache_dir / f"{p.stem}_opt",
                    max_width=1200,
                    max_kb=220,
                )
                if opt_path.exists():
                    use_path = opt_path
        except Exception:
            use_path = p

        key = _usage_key(p)
        counts[key] = int(counts.get(key, 0) or 0) + 1
        used_map[key] = now
        assets.append(
            ImageAsset(
                path=use_path,
                alt=f"Troubleshooting process diagram for {str(title or '').strip()}.",
                anchor_text="",
                source_kind="library",
                source_url=f"local://library/{category}/{use_path.name}",
                license_note="Local reusable asset",
            )
        )

    usage["counts"] = counts
    usage["last_used"] = used_map
    _save_usage(usage_path, usage)
    return assets
