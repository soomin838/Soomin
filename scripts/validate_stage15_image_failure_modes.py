from __future__ import annotations

import os
import tempfile
from pathlib import Path
import re
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.brain import DraftPost  # noqa: E402
from re_core.publisher import Publisher  # noqa: E402
from re_core.settings import load_settings  # noqa: E402
from re_core.visual import ImageAsset, VisualPipeline  # noqa: E402
from re_core.workflow import AgentWorkflow  # noqa: E402
import re_core.visual as visual_module  # noqa: E402


def _write_settings(temp_root: Path, public_base_url: str) -> Path:
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
            "min_images_required": 1,
            "max_images_per_post": 5,
            "r2": {
                "endpoint_url": "https://example-r2-endpoint.invalid",
                "bucket": "dummy-bucket",
                "access_key_id": "dummy-access",
                "secret_access_key": "dummy-secret",
                "public_base_url": public_base_url,
                "prefix": "news",
                "cache_control": "public, max-age=31536000, immutable",
            },
        },
        "blogger": {
            "credentials_path": "config/credentials.json",
            "blog_id": "dummy-blog",
        },
        "indexing": {"service_account_path": "config/indexing.json"},
    }
    cfg_dir = temp_root / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "settings.yaml"
    cfg_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return cfg_path


def _sample_draft() -> DraftPost:
    html = (
        "<h2>What Happened</h2>"
        "<p>A platform update changed account verification and policy messaging timing for users.</p>"
        "<h2>Why It Matters</h2>"
        "<p>Normal users may notice revised prompts in sign-in and account security workflows.</p>"
        "<h2>Sources</h2>"
        "<ul><li><a href=\"https://example.com/update\">example.com</a></li></ul>"
    )
    return DraftPost(
        title="Platform account policy timing update",
        alt_titles=[],
        html=html,
        summary="Verification and policy timing changed in this release.",
        score=82,
        source_url="https://example.com/update",
        extracted_urls=["https://example.com/update"],
    )


def _build_workflow_stub(temp_root: Path, settings, visual: VisualPipeline) -> AgentWorkflow:
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
    wf = AgentWorkflow.__new__(AgentWorkflow)
    wf.settings = settings
    wf.publisher = publisher
    wf.visual = visual
    wf._workflow_perf_path = temp_root / "storage" / "logs" / "workflow_perf.jsonl"
    wf._workflow_perf_run_id = "stage15-validate"
    return wf


def _read_lines(path: Path) -> list[str]:
    try:
        if not path.exists():
            return []
        return path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return []


def main() -> int:
    env_prev = os.environ.get("R2_DRY_RUN")
    req_post_prev = visual_module.requests.post
    req_get_prev = visual_module.requests.get
    calls = {"post": 0, "get": 0}

    def _guard_post(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["post"] += 1
        raise AssertionError("requests.post should not be called in Stage-15 validation")

    def _guard_get(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["get"] += 1
        raise AssertionError("requests.get should not be called in Stage-15 validation")

    os.environ["R2_DRY_RUN"] = "1"
    visual_module.requests.post = _guard_post
    visual_module.requests.get = _guard_get
    try:
        with tempfile.TemporaryDirectory(prefix="stage15_images_") as td:
            temp_root = Path(td).resolve()
            draft = _sample_draft()

            cfg_ok = _write_settings(temp_root / "case1", public_base_url="https://example-r2-public.invalid")
            settings_ok = load_settings(cfg_ok)
            vp_ok = VisualPipeline(
                temp_dir=(temp_root / "case1" / "storage" / "temp_images"),
                session_dir=(temp_root / "case1" / "storage" / "sessions"),
                visual_settings=settings_ok.visual,
                gemini_api_key=settings_ok.gemini.api_key,
                r2_config=settings_ok.publish.r2,
            )
            images_ok = vp_ok.build(draft)
            base = str(settings_ok.publish.r2.public_base_url or "").rstrip("/")
            if len(images_ok) < 1:
                raise AssertionError("Case1 failed: expected at least one generated image.")
            for idx, img in enumerate(images_ok):
                src = str(getattr(img, "source_url", "") or "").strip()
                if not src.startswith(base + "/"):
                    raise AssertionError(f"Case1 failed: image[{idx}] source_url does not match R2 base.")
            print("Case 1 PASS: valid r2 config returns images with public source_url")
            print(f"  images={len(images_ok)}")

            cfg_bad = _write_settings(temp_root / "case2", public_base_url="")
            settings_bad = load_settings(cfg_bad)
            vp_bad = VisualPipeline(
                temp_dir=(temp_root / "case2" / "storage" / "temp_images"),
                session_dir=(temp_root / "case2" / "storage" / "sessions"),
                visual_settings=settings_bad.visual,
                gemini_api_key=settings_bad.gemini.api_key,
                r2_config=settings_bad.publish.r2,
            )
            images_bad = vp_bad.build(draft)
            if images_bad:
                raise AssertionError("Case2 failed: expected no images when R2 public_base_url is missing.")
            reason_codes = set(vp_bad.get_last_reason_codes())
            if "missing_r2_config" not in reason_codes:
                raise AssertionError("Case2 failed: missing_r2_config reason code not recorded.")
            visual_log = temp_root / "case2" / "storage" / "logs" / "visual_pipeline.jsonl"
            log_text = "\n".join(_read_lines(visual_log))
            if "reason_code" not in log_text or "missing_r2_config" not in log_text:
                raise AssertionError("Case2 failed: visual log does not include missing_r2_config reason_code.")

            wf_bad = _build_workflow_stub(temp_root / "case2", settings_bad, vp_bad)
            note_bad = wf_bad._annotate_image_pipeline_diagnostics(  # noqa: SLF001
                note="",
                stage="case2",
                images=[],
                preflight_thumb_src="",
                required_images=1,
            )
            if "r2_config_missing" not in note_bad:
                raise AssertionError("Case2 failed: workflow diagnostic note missing r2_config_missing token.")
            print("Case 2 PASS: missing r2 config is detected with standard reason codes/tokens")
            print(f"  reason_codes={sorted(reason_codes)}")

            wf_ok = _build_workflow_stub(temp_root / "case1", settings_ok, vp_ok)
            invalid_asset = ImageAsset(
                path=(temp_root / "case1" / "storage" / "temp_images" / "virtual_invalid.png"),
                alt="invalid host test",
                anchor_text="",
                source_kind="generated_r2",
                source_url="https://invalid-host.example/path/image.png",
                license_note="test",
            )
            preflight = wf_ok._preflight_thumb_src_from_images([invalid_asset])  # noqa: SLF001
            if preflight:
                raise AssertionError("Case3 failed: invalid host source_url must not pass preflight src validation.")
            note_invalid = wf_ok._annotate_image_pipeline_diagnostics(  # noqa: SLF001
                note="",
                stage="case3",
                images=[invalid_asset],
                preflight_thumb_src=preflight,
                required_images=1,
            )
            if "thumbnail_src_invalid_host" not in note_invalid:
                raise AssertionError("Case3 failed: expected thumbnail_src_invalid_host diagnostic token.")
            if "r2_public_url_invalid" not in note_invalid:
                raise AssertionError("Case3 failed: expected r2_public_url_invalid diagnostic token.")
            print("Case 3 PASS: invalid thumbnail host is blocked and diagnostic tokens are added")

            if calls["post"] != 0 or calls["get"] != 0:
                raise AssertionError(
                    f"Network call detected during validation post={calls['post']} get={calls['get']}"
                )
            print("Stage-15 image failure-mode validation passed.")
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
