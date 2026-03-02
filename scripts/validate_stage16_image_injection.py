from __future__ import annotations

import re
import tempfile
from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.publisher import Publisher  # noqa: E402
from core.settings import load_settings  # noqa: E402
from core.visual import ImageAsset  # noqa: E402
from core.workflow import AgentWorkflow  # noqa: E402


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
            "min_images_required": 0,
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
        "blogger": {"credentials_path": "config/credentials.json", "blog_id": "dummy-blog"},
        "indexing": {"service_account_path": "config/indexing.json"},
    }
    cfg_dir = temp_root / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "settings.yaml"
    cfg_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return cfg_path


def _sample_html() -> str:
    return (
        "<h2>Quick Take</h2>"
        "<p>Paragraph 1 for quick take.</p>"
        "<p>Paragraph 2 for quick take.</p>"
        "<h2>What Happened</h2>"
        "<p>Paragraph 1 for happened section.</p>"
        "<p>Paragraph 2 for happened section.</p>"
        "<h2>Why It Matters</h2>"
        "<p>Paragraph 1 for impact section.</p>"
        "<p>Paragraph 2 for impact section.</p>"
        "<h2>What To Do Now</h2>"
        "<p>Paragraph 1 for actions.</p>"
        "<p>Paragraph 2 for actions.</p>"
        "<h2>Sources</h2>"
        "<ul><li><a href=\"https://example.com/source\">example.com</a></li></ul>"
    )


def _make_images(count: int) -> list[ImageAsset]:
    out: list[ImageAsset] = []
    for i in range(1, count + 1):
        out.append(
            ImageAsset(
                path=Path(f"virtual_{i:02d}.png"),
                alt=f"Image {i}",
                anchor_text="",
                source_kind="generated_r2",
                source_url=f"https://example-r2-public.invalid/news/content/image_{i:02d}.png",
                license_note="test",
            )
        )
    return out


def _build_workflow_stub(temp_root: Path, settings) -> AgentWorkflow:
    publisher = Publisher(
        credentials_path=temp_root / "config" / "credentials.json",
        blog_id="dummy-blog",
        service_account_path=temp_root / "config" / "indexing.json",
        image_hosting_backend=settings.publish.image_hosting_backend,
        r2_config=settings.publish.r2,
        max_banner_images=settings.visual.max_banner_images,
        max_inline_images=settings.visual.max_inline_images,
        min_required_images=settings.publish.min_images_required,
    )
    wf = AgentWorkflow.__new__(AgentWorkflow)
    wf.settings = settings
    wf.publisher = publisher
    return wf


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="stage16_injection_") as td:
        temp_root = Path(td).resolve()
        cfg_path = _write_settings(temp_root)
        settings = load_settings(cfg_path)
        wf = _build_workflow_stub(temp_root, settings)

        html = _sample_html()
        images3 = _make_images(3)
        injected = wf._inject_images_into_html(html, images3)  # noqa: SLF001

        img_tags = re.findall(r"<img\b[^>]*\bsrc=", injected, flags=re.IGNORECASE)
        if len(img_tags) < 3:
            raise AssertionError("Case1 failed: injected html must contain at least 3 <img> tags.")

        first_img = re.search(r"<img\b", injected, flags=re.IGNORECASE)
        first_h2 = re.search(r"<h2\b", injected, flags=re.IGNORECASE)
        if not first_img or not first_h2 or not (first_img.start() < first_h2.start()):
            raise AssertionError("Case2 failed: first image must appear before first <h2>.")

        consecutive = re.search(
            r"(<p[^>]*>\s*<img\b[^>]*>\s*</p>\s*){2,}",
            injected,
            flags=re.IGNORECASE,
        )
        if consecutive:
            raise AssertionError("Case3 failed: consecutive image-only paragraphs detected.")

        note_low = wf._apply_ctr_visual_density_note("", _make_images(2))  # noqa: SLF001
        if "ctr_risk_low_visual_density" not in note_low:
            raise AssertionError("Case4 failed: low visual density token missing for <=2 images.")

        print("Case 1 PASS: 3+ images produce 3+ img tags in injected HTML")
        print(f"  img_count={len(img_tags)}")
        print("Case 2 PASS: first image is inserted above first <h2>")
        print("Case 3 PASS: no consecutive image-only blocks")
        print("Case 4 PASS: ctr_risk_low_visual_density token added for <=2 images")
        print("Stage-16 image injection validation passed.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
