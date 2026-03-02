from __future__ import annotations

import os
import tempfile
from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.brain import DraftPost  # noqa: E402
from core.settings import load_settings  # noqa: E402
from core.visual import VisualPipeline  # noqa: E402
import core.visual as visual_module  # noqa: E402


def _write_settings(temp_root: Path) -> Path:
    cfg = {
        "timezone": "Asia/Seoul",
        "gemini": {
            "api_key": "",
        },
        "visual": {
            "image_provider": "library",
            "enable_gemini_image_generation": False,
            "target_images_per_post": 5,
            "max_banner_images": 1,
            "max_inline_images": 4,
            "image_request_interval_seconds": 20,
        },
        "publish": {
            "image_hosting_backend": "r2",
            "min_images_required": 0,
            "max_images_per_post": 5,
            "r2": {
                "endpoint_url": "https://example-r2-endpoint.invalid",
                "bucket": "dummy-bucket",
                "access_key_id": "dummy-access",
                "secret_access_key": "dummy-secret",
                "public_base_url": "https://example-r2-public.invalid",
                "prefix": "news",
                "cache_control": "public, max-age=31536000, immutable",
            },
        },
    }
    cfg_dir = temp_root / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "settings.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return cfg_path


def _sample_draft() -> DraftPost:
    html = (
        "<h2>What Happened</h2>"
        "<p>A platform update changed verification timing for account security checks and policy notices.</p>"
        "<h2>Why It Matters</h2>"
        "<p>Normal users may see delayed prompts and revised account notices across connected apps.</p>"
        "<h2>Sources</h2>"
        "<ul><li><a href=\"https://example.com/update\">example.com</a></li></ul>"
    )
    return DraftPost(
        title="Platform verification update changes policy timing",
        alt_titles=[],
        html=html,
        summary="Verification and policy timing changed across account workflows.",
        score=80,
        source_url="https://example.com/update",
        extracted_urls=["https://example.com/update"],
    )


def main() -> int:
    env_prev = os.environ.get("R2_DRY_RUN")
    req_post_prev = visual_module.requests.post
    req_get_prev = visual_module.requests.get
    net_calls: dict[str, int] = {"post": 0, "get": 0}

    def _guard_post(*args, **kwargs):  # type: ignore[no-untyped-def]
        net_calls["post"] += 1
        raise AssertionError("requests.post should not be called in Stage-13 validation")

    def _guard_get(*args, **kwargs):  # type: ignore[no-untyped-def]
        net_calls["get"] += 1
        raise AssertionError("requests.get should not be called in Stage-13 validation")

    os.environ["R2_DRY_RUN"] = "1"
    visual_module.requests.post = _guard_post
    visual_module.requests.get = _guard_get

    try:
        with tempfile.TemporaryDirectory(prefix="stage13_images_") as td:
            temp_root = Path(td).resolve()
            cfg_path = _write_settings(temp_root)
            settings = load_settings(cfg_path)
            vp = VisualPipeline(
                temp_dir=temp_root / "storage" / "temp_images",
                session_dir=temp_root / "storage" / "sessions",
                visual_settings=settings.visual,
                gemini_api_key=settings.gemini.api_key,
                r2_config=settings.publish.r2,
            )
            draft = _sample_draft()

            assets = vp.build(draft)
            if len(assets) < 1:
                raise AssertionError("Scenario 1 failed: build() must return at least one image asset.")
            base = str(settings.publish.r2.public_base_url or "").rstrip("/")
            for i, asset in enumerate(assets):
                src = str(getattr(asset, "source_url", "") or "").strip()
                if not src.startswith(base + "/"):
                    raise AssertionError(f"Scenario 1 failed: asset[{i}] source_url is not R2 public URL: {src}")

            thumbs = vp.ensure_generated_thumbnail(draft, images=[])
            if not thumbs:
                raise AssertionError("Scenario 2 failed: ensure_generated_thumbnail() returned empty list.")
            thumb = thumbs[0]
            thumb_src = str(getattr(thumb, "source_url", "") or "").strip()
            if not thumb_src.startswith(base + "/"):
                raise AssertionError(f"Scenario 2 failed: thumbnail source_url is not R2 public URL: {thumb_src}")
            if Path(getattr(thumb, "path", "")).exists():
                raise AssertionError("Scenario 2 failed: thumbnail path should not persist on local disk.")

            if net_calls["post"] != 0 or net_calls["get"] != 0:
                raise AssertionError(
                    f"Scenario 3 failed: network calls detected post={net_calls['post']} get={net_calls['get']}"
                )

            print("Scenario 1 PASS: build() returns R2 public URLs in dry-run mode")
            print(f"  assets={len(assets)}")
            print("Scenario 2 PASS: ensure_generated_thumbnail() returns R2 URL without local persistent file")
            print(f"  thumbnail_source={thumb_src}")
            print("Scenario 3 PASS: Gemini/HTTP image generation path was not called")
            print(f"  requests_post_calls={net_calls['post']}, requests_get_calls={net_calls['get']}")
            print("Stage-13 Cloudflare image pipeline validation passed.")
            return 0
    finally:
        visual_module.requests.post = req_post_prev
        visual_module.requests.get = req_get_prev
        if env_prev is None:
            os.environ.pop("R2_DRY_RUN", None)
        else:
            os.environ["R2_DRY_RUN"] = env_prev


if __name__ == "__main__":
    raise SystemExit(main())
