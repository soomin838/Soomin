from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .settings import AppSettings


def _tail_lines(path: Path, limit: int = 20) -> list[str]:
    try:
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return lines[-max(1, int(limit)) :]
    except Exception:
        return []


def diagnose_visual_settings(settings: AppSettings, root: Path) -> dict[str, Any]:
    root_path = Path(root).resolve()
    cfg_path = root_path / "config" / "settings.yaml"
    warnings_path = root_path / "storage" / "logs" / "settings_warnings.log"

    raw: dict[str, Any] = {}
    try:
        if cfg_path.exists():
            loaded = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
            if isinstance(loaded, dict):
                raw = dict(loaded)
    except Exception:
        raw = {}

    raw_visual = dict(raw.get("visual", {}) or {})
    raw_images = dict(raw.get("images", {}) or {})
    has_images_block = bool(raw_images)
    has_visual_block = bool(raw_visual)

    visual_provider = str(getattr(settings.visual, "image_provider", "") or "").strip().lower() or "unknown"
    visual_enable = bool(getattr(settings.visual, "enable_gemini_image_generation", False))
    target_images = int(getattr(settings.visual, "target_images_per_post", 0) or 0)
    max_banner = int(getattr(settings.visual, "max_banner_images", 0) or 0)
    max_inline = int(getattr(settings.visual, "max_inline_images", 0) or 0)

    api_key_raw = str(getattr(settings.gemini, "api_key", "") or "").strip()
    has_gemini_api_key = bool(api_key_raw and api_key_raw != "GEMINI_API_KEY")

    min_images_required = int(getattr(settings.publish, "min_images_required", 0) or 0)
    max_images_per_post = int(getattr(settings.publish, "max_images_per_post", 0) or 0)
    thumbnail_preflight_only = bool(getattr(settings.publish, "thumbnail_preflight_only", False))
    thumbnail_preflight_max_cycles = int(getattr(settings.publish, "thumbnail_preflight_max_cycles", 0) or 0)

    settings_warnings_tail = _tail_lines(warnings_path, limit=20)
    warning_joined = "\n".join(settings_warnings_tail).lower()

    raw_visual_provider = str(raw_visual.get("image_provider", "") or "").strip().lower()
    raw_visual_enable = raw_visual.get("enable_gemini_image_generation", None)
    override_suspected = False
    if has_images_block and visual_provider == "library" and (not visual_enable):
        override_suspected = True
    if has_visual_block:
        if raw_visual_provider and raw_visual_provider != visual_provider:
            override_suspected = True
        if isinstance(raw_visual_enable, bool) and raw_visual_enable != visual_enable:
            override_suspected = True
    if "both visual.* and images.* are present" in warning_joined:
        override_suspected = True

    blockers: list[str] = []
    if not visual_enable:
        blockers.append("enable_gemini_image_generation=false")
    if not has_gemini_api_key:
        blockers.append("missing_gemini_api_key")
    if visual_provider == "library":
        blockers.append("image_provider=library")
    if min_images_required > 0 and ((not visual_enable) or (visual_provider == "library")):
        blockers.append("min_images_required>0 but generation disabled")
    if thumbnail_preflight_only:
        blockers.append("thumbnail_preflight_only=true")
    if max_images_per_post <= 0 or max_images_per_post < min_images_required or max_images_per_post < max(1, target_images):
        blockers.append("max_images_per_post too low")

    recommend_fix: list[str] = []
    if "enable_gemini_image_generation=false" in blockers:
        recommend_fix.append("set visual.enable_gemini_image_generation=true")
    if "missing_gemini_api_key" in blockers:
        recommend_fix.append("provide GEMINI_API_KEY via env or config")
    if "image_provider=library" in blockers:
        recommend_fix.append("set visual.image_provider=gemini for generation path")
    if "min_images_required>0 but generation disabled" in blockers:
        recommend_fix.append("align min_images_required with enabled generation mode")
    if "thumbnail_preflight_only=true" in blockers:
        recommend_fix.append("set publish.thumbnail_preflight_only=false for normal publish runs")
    if "max_images_per_post too low" in blockers:
        recommend_fix.append("set publish.max_images_per_post >= target_images_per_post and min_images_required")
    if override_suspected:
        recommend_fix.append("review images.* compatibility override; migrate to visual.* only")

    seen_fix: set[str] = set()
    dedup_fix: list[str] = []
    for row in recommend_fix:
        key = str(row or "").strip()
        if not key or key in seen_fix:
            continue
        seen_fix.add(key)
        dedup_fix.append(key)

    can_attempt_generation = len(blockers) == 0

    return {
        "visual.image_provider": visual_provider,
        "visual.enable_gemini_image_generation": bool(visual_enable),
        "visual.target_images_per_post": int(target_images),
        "visual.max_banner_images": int(max_banner),
        "visual.max_inline_images": int(max_inline),
        "gemini.api_key_present": bool(has_gemini_api_key),
        "publish.min_images_required": int(min_images_required),
        "publish.max_images_per_post": int(max_images_per_post),
        "publish.thumbnail_preflight_only": bool(thumbnail_preflight_only),
        "publish.thumbnail_preflight_max_cycles": int(thumbnail_preflight_max_cycles),
        "images_block_present_in_raw": bool(has_images_block),
        "visual_block_present_in_raw": bool(has_visual_block),
        "images_block_runtime_override_suspected": bool(override_suspected),
        "settings_warnings_log_path": str(warnings_path),
        "settings_warnings_tail": settings_warnings_tail,
        "can_attempt_generation": bool(can_attempt_generation),
        "blockers": blockers,
        "recommend_fix": dedup_fix,
    }

