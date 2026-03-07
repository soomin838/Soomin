from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from googleapiclient.discovery import build

from re_core.publisher import Publisher
from re_core.settings import load_settings
from re_core.visual import VisualPipeline


@dataclass
class FixRow:
    post_id: str
    status: str
    title: str
    broken_count: int
    replaced_count: int
    skipped_reason: str = ""


def _load_runtime_settings(project_root: Path):
    app_cfg = Path(os.path.expandvars(r"%APPDATA%\RezeroAgent\config\settings.yaml"))
    if app_cfg.exists():
        return load_settings(app_cfg), app_cfg
    repo_cfg = project_root / "config" / "settings.yaml"
    return load_settings(repo_cfg), repo_cfg


def _iter_img_src(html: str) -> list[str]:
    return [m.group(1).strip() for m in re.finditer(r'<img[^>]+src="([^"]+)"', html or "", flags=re.IGNORECASE)]


def _is_broken_image_url(url: str) -> tuple[bool, str]:
    src = str(url or "").strip()
    if not src:
        return True, "empty_src"
    if src.lower().startswith("data:image/"):
        return False, "data_uri"
    try:
        r = requests.get(src, timeout=20, allow_redirects=True, stream=True, headers={"User-Agent": "RezeroAgent/1.0"})
    except Exception as exc:
        return True, f"request_error:{exc}"
    ctype = str(r.headers.get("content-type", "")).lower()
    if r.status_code >= 400:
        return True, f"http_{r.status_code}"
    if "image/" not in ctype:
        return True, f"non_image_content_type:{ctype}"
    return False, "ok"


def _context_around_src(html: str, src: str, title: str) -> str:
    idx = (html or "").find(src)
    if idx < 0:
        return title
    left = max(0, idx - 240)
    right = min(len(html), idx + 240)
    raw = (html[left:right] or "")
    raw = re.sub(r"<[^>]+>", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw[:260] or title


def _replace_one_src(html: str, old_src: str, new_src: str) -> str:
    return re.sub(
        re.escape(f'src="{old_src}"'),
        f'src="{new_src}"',
        html,
        count=1,
        flags=re.IGNORECASE,
    )


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    settings, settings_path = _load_runtime_settings(project_root)
    app_root = settings_path.parent.parent if "AppData" in str(settings_path) else project_root

    publisher = Publisher(
        credentials_path=app_root / settings.blogger.credentials_path,
        blog_id=settings.blogger.blog_id,
        service_account_path=app_root / settings.indexing.service_account_path,
        image_hosting_backend=settings.publish.image_hosting_backend,
        gcs_bucket_name=settings.publish.gcs_bucket_name,
        gcs_public_base_url=settings.publish.gcs_public_base_url,
    )
    visual = VisualPipeline(
        temp_dir=app_root / "storage" / "temp_images",
        session_dir=app_root / "storage" / "sessions",
        visual_settings=settings.visual,
        gemini_api_key=settings.gemini.api_key,
    )

    creds = publisher._oauth_credentials()  # noqa: SLF001
    publisher._ensure_valid_token(creds)  # noqa: SLF001
    service = build("blogger", "v3", credentials=creds)

    posts = publisher.fetch_posts_for_export(statuses=["live", "scheduled"], limit=200, include_bodies=True)
    rows: list[FixRow] = []
    total_replaced = 0
    generated_index_seed = 7000

    for post in posts:
        html = str(post.content or "")
        if not html:
            rows.append(FixRow(post_id=post.post_id, status=post.status, title=post.title, broken_count=0, replaced_count=0, skipped_reason="empty_content"))
            continue
        srcs = _iter_img_src(html)
        if not srcs:
            rows.append(FixRow(post_id=post.post_id, status=post.status, title=post.title, broken_count=0, replaced_count=0, skipped_reason="no_img_tag"))
            continue

        broken_srcs: list[str] = []
        for src in srcs:
            broken, _ = _is_broken_image_url(src)
            if broken:
                broken_srcs.append(src)

        if not broken_srcs:
            rows.append(FixRow(post_id=post.post_id, status=post.status, title=post.title, broken_count=0, replaced_count=0, skipped_reason="all_ok"))
            continue

        replaced = 0
        patched_html = html
        for i, bad_src in enumerate(broken_srcs):
            role = "thumbnail" if i == 0 else "content"
            context = _context_around_src(patched_html, bad_src, post.title)
            prompt = visual._fallback_prompt(context, post.title, variation_index=i + 1, role=role)  # noqa: SLF001
            asset = visual._generate_image_with_pollinations(  # noqa: SLF001
                prompt=prompt,
                index=generated_index_seed,
                paragraph=context,
                keyword=post.title,
                role=role,
            )
            generated_index_seed += 1
            if asset is None:
                continue
            visual._optimize_image_for_seo(asset.path, role=role)  # noqa: SLF001
            hosted = publisher._upload_images([asset], creds)  # noqa: SLF001
            new_src = str(hosted.get(str(asset.path), "") or "").strip()
            if not new_src:
                continue
            patched_html = _replace_one_src(patched_html, bad_src, new_src)
            replaced += 1
            total_replaced += 1

        if replaced > 0 and patched_html != html:
            try:
                service.posts().patch(
                    blogId=settings.blogger.blog_id,
                    postId=post.post_id,
                    body={"content": patched_html},
                ).execute()
            except Exception:
                service.posts().update(
                    blogId=settings.blogger.blog_id,
                    postId=post.post_id,
                    body={"id": post.post_id, "title": post.title, "content": patched_html},
                ).execute()

        rows.append(
            FixRow(
                post_id=post.post_id,
                status=post.status,
                title=post.title,
                broken_count=len(broken_srcs),
                replaced_count=replaced,
                skipped_reason="" if replaced > 0 else "replacement_failed_or_no_upload_url",
            )
        )

    report = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "settings_path": str(settings_path),
        "post_count_scanned": len(posts),
        "total_replaced": total_replaced,
        "rows": [asdict(r) for r in rows],
    }
    out = app_root / "storage" / "logs" / "repair_broken_images_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out)
    print(json.dumps({"post_count_scanned": len(posts), "total_replaced": total_replaced}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

