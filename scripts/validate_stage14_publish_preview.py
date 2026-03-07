from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.brain import DraftPost  # noqa: E402
from re_core.publisher import Publisher  # noqa: E402
from re_core.settings import load_settings  # noqa: E402
from re_core.visual import VisualPipeline  # noqa: E402
import re_core.visual as visual_module  # noqa: E402


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
        "blogger": {
            "credentials_path": "config/credentials.json",
            "blog_id": "dummy-blog",
        },
        "indexing": {
            "service_account_path": "config/indexing.json",
        },
    }
    cfg_dir = temp_root / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "settings.yaml"
    cfg_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return cfg_path


def _sample_draft() -> DraftPost:
    html = (
        "<h2>What Happened</h2>"
        "<p>Platform policy timing changed and users may see revised account checks.</p>"
        "<h2>Why It Matters</h2>"
        "<p>The update can affect login prompts and account notices for normal users.</p>"
        "<h2>Sources</h2>"
        "<ul><li><a href=\"https://example.com/release\">example.com</a></li></ul>"
    )
    return DraftPost(
        title="Platform policy update shifts account check timing",
        alt_titles=[],
        html=html,
        summary="Policy and account check timing changed in this update.",
        score=84,
        source_url="https://example.com/release",
        extracted_urls=["https://example.com/release"],
    )


def main() -> int:
    env_prev = os.environ.get("R2_DRY_RUN")
    req_post_prev = visual_module.requests.post
    req_get_prev = visual_module.requests.get
    calls = {"post": 0, "get": 0}

    def _guard_post(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["post"] += 1
        raise AssertionError("requests.post should not be called in Stage-14 preview validation")

    def _guard_get(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["get"] += 1
        raise AssertionError("requests.get should not be called in Stage-14 preview validation")

    os.environ["R2_DRY_RUN"] = "1"
    visual_module.requests.post = _guard_post
    visual_module.requests.get = _guard_get

    try:
        with tempfile.TemporaryDirectory(prefix="stage14_preview_") as td:
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
            images = vp.build(draft)
            images = vp.ensure_generated_thumbnail(draft, images=images)
            if not images:
                raise AssertionError("Scenario 1 failed: image list is empty.")

            for idx, asset in enumerate(images):
                if Path(getattr(asset, "path", "")).exists():
                    raise AssertionError(
                        f"Scenario 1 failed: image[{idx}] local file should not exist in r2-only mode."
                    )

            publisher = Publisher(
                credentials_path=temp_root / "config" / "credentials.json",
                blog_id="dummy-blog",
                service_account_path=temp_root / "config" / "indexing.json",
                image_hosting_backend=settings.publish.image_hosting_backend,
                r2_config=settings.publish.r2,
                max_banner_images=settings.visual.max_banner_images,
                max_inline_images=settings.visual.max_inline_images,
                min_required_images=settings.publish.min_images_required,
                semantic_html_enabled=True,
            )
            preview_html = publisher.build_dry_run_html(draft.html, images)
            img_src = re.findall(r'<img\b[^>]*\bsrc="([^"]+)"', preview_html, flags=re.IGNORECASE)
            if len(img_src) < 1:
                raise AssertionError("Scenario 2 failed: preview html has no <img src> entries.")
            base = str(settings.publish.r2.public_base_url or "").rstrip("/")
            if not all(str(src).startswith(base + "/") for src in img_src):
                raise AssertionError(
                    "Scenario 2 failed: preview image src must use R2 public URL for all inserted images."
                )

            if calls["post"] != 0 or calls["get"] != 0:
                raise AssertionError(
                    f"Scenario 3 failed: network calls detected post={calls['post']} get={calls['get']}"
                )

            print("Scenario 1 PASS: r2-only images are generated without local persistent files")
            print(f"  images={len(images)}")
            print("Scenario 2 PASS: build_dry_run_html inserts R2 source_url-based <img> tags")
            print(f"  img_count={len(img_src)}")
            print("Scenario 3 PASS: no network image-generation calls occurred")
            print(f"  requests_post_calls={calls['post']}, requests_get_calls={calls['get']}")
            print("Stage-14 publish preview validation passed.")
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
