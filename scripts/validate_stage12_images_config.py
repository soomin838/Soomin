from __future__ import annotations

import os
import tempfile
from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.settings import load_settings  # noqa: E402
from core.visual_diagnostics import diagnose_visual_settings  # noqa: E402


def _write_settings(temp_root: Path, payload: dict) -> Path:
    cfg_dir = temp_root / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "settings.yaml"
    cfg_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return cfg_path


def case_visual_truth_preserved() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory(prefix="stage12_case1_") as td:
        temp_root = Path(td)
        cfg_path = _write_settings(
            temp_root,
            {
                "timezone": "America/New_York",
                "gemini": {"api_key": ""},
                "visual": {
                    "image_provider": "gemini",
                    "enable_gemini_image_generation": True,
                    "target_images_per_post": 5,
                    "max_banner_images": 1,
                    "max_inline_images": 4,
                    "image_request_interval_seconds": 20,
                },
                "images": {
                    "provider": "library",
                    "banner_count": 1,
                    "inline_count": 4,
                },
                "publish": {
                    "min_images_required": 0,
                    "max_images_per_post": 5,
                },
            },
        )
        settings = load_settings(cfg_path)
        ok = bool(settings.visual.enable_gemini_image_generation is True)
        if not ok:
            raise AssertionError("Case1 failed: visual.enable_gemini_image_generation must remain True.")
        return ok, str(settings.visual.image_provider)


def case_env_key_removes_missing_blocker() -> list[str]:
    prev = os.environ.get("GEMINI_API_KEY")
    try:
        os.environ["GEMINI_API_KEY"] = "dummy-stage12-key"
        with tempfile.TemporaryDirectory(prefix="stage12_case2_") as td:
            temp_root = Path(td)
            cfg_path = _write_settings(
                temp_root,
                {
                    "timezone": "America/New_York",
                    "gemini": {"api_key": ""},
                    "visual": {
                        "image_provider": "gemini",
                        "enable_gemini_image_generation": True,
                        "target_images_per_post": 5,
                        "max_banner_images": 1,
                        "max_inline_images": 4,
                    },
                    "publish": {"min_images_required": 0, "max_images_per_post": 5},
                },
            )
            settings = load_settings(cfg_path)
            diag = diagnose_visual_settings(settings, temp_root)
            blockers = list(diag.get("blockers", []) or [])
            if "missing_gemini_api_key" in blockers:
                raise AssertionError("Case2 failed: env GEMINI_API_KEY should remove missing_gemini_api_key blocker.")
            return blockers
    finally:
        if prev is None:
            os.environ.pop("GEMINI_API_KEY", None)
        else:
            os.environ["GEMINI_API_KEY"] = prev


def case_visual_overrides_images() -> tuple[str, bool]:
    with tempfile.TemporaryDirectory(prefix="stage12_case3_") as td:
        temp_root = Path(td)
        cfg_path = _write_settings(
            temp_root,
            {
                "timezone": "America/New_York",
                "gemini": {"api_key": "dummy-inline-key"},
                "visual": {
                    "image_provider": "gemini",
                    "enable_gemini_image_generation": True,
                    "target_images_per_post": 5,
                    "max_banner_images": 1,
                    "max_inline_images": 4,
                },
                "images": {
                    "provider": "library",
                    "banner_count": 1,
                    "inline_count": 4,
                },
                "publish": {"min_images_required": 0, "max_images_per_post": 5},
            },
        )
        settings = load_settings(cfg_path)
        if settings.visual.enable_gemini_image_generation is not True:
            raise AssertionError("Case3 failed: images.* must not force visual enable to False.")
        if str(settings.visual.image_provider or "").strip().lower() != "gemini":
            raise AssertionError("Case3 failed: images.* must not override visual.image_provider.")
        return str(settings.visual.image_provider), bool(settings.visual.enable_gemini_image_generation)


def main() -> int:
    c1_ok, c1_provider = case_visual_truth_preserved()
    c2_blockers = case_env_key_removes_missing_blocker()
    c3_provider, c3_enable = case_visual_overrides_images()
    print("Case 1 OK: visual block enable=true is preserved at runtime")
    print(f"  enable={c1_ok}, provider={c1_provider}")
    print("Case 2 OK: env GEMINI_API_KEY removes missing_gemini_api_key blocker")
    print(f"  blockers={c2_blockers}")
    print("Case 3 OK: visual.* takes precedence when visual.* and images.* coexist")
    print(f"  provider={c3_provider}, enable={c3_enable}")
    print("Stage-12 image settings bridge validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

