from __future__ import annotations

import mimetypes
import re
import base64
import html as html_lib
import tempfile
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import requests
from PIL import Image
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from zoneinfo import ZoneInfo

from .visual import ImageAsset


@dataclass
class PublishResult:
    url: str
    post_id: str


@dataclass
class BlogPostItem:
    post_id: str
    title: str
    url: str
    status: str
    published: str
    updated: str
    content: str


class Publisher:
    def __init__(
        self,
        credentials_path: Path,
        blog_id: str,
        service_account_path: Path,
        image_hosting_backend: str = "blogger_media",
        gcs_bucket_name: str = "",
        gcs_public_base_url: str = "",
        max_banner_images: int = 1,
        max_inline_images: int = 2,
        semantic_html_enabled: bool = True,
        strict_thumbnail_blogger_media: bool = True,
        thumbnail_data_uri_allowed: bool = False,
        auto_allow_data_uri_on_blogger_405: bool = True,
    ) -> None:
        self.credentials_path = credentials_path
        self.blog_id = blog_id
        self.service_account_path = service_account_path
        self.image_hosting_backend = (image_hosting_backend or "blogger_media").strip().lower()
        self.gcs_bucket_name = (gcs_bucket_name or "").strip()
        self.gcs_public_base_url = (gcs_public_base_url or "").strip().rstrip("/")
        self.max_banner_images = max(1, int(max_banner_images or 1))
        self.max_inline_images = max(0, int(max_inline_images or 0))
        self.semantic_html_enabled = bool(semantic_html_enabled)
        self.strict_thumbnail_blogger_media = bool(strict_thumbnail_blogger_media)
        self.thumbnail_data_uri_allowed = bool(thumbnail_data_uri_allowed)
        self.auto_allow_data_uri_on_blogger_405 = bool(auto_allow_data_uri_on_blogger_405)
        self._last_upload_report: dict = {}
        self._indexing_scope = "https://www.googleapis.com/auth/indexing"
        self._upload_log_path = (
            self.credentials_path.parent.parent / "storage" / "logs" / "publisher_upload.jsonl"
        ).resolve()
        self._thumbnail_gate_log_path = (
            self.credentials_path.parent.parent / "storage" / "logs" / "thumbnail_gate.jsonl"
        ).resolve()
        self._upload_log_path.parent.mkdir(parents=True, exist_ok=True)
        self._thumbnail_gate_log_path.parent.mkdir(parents=True, exist_ok=True)

    def publish_post(
        self,
        title: str,
        html_body: str,
        images: list[ImageAsset],
        labels: list[str],
        publish_at: datetime | None = None,
        existing_draft_post_id: str | None = None,
        meta_description: str | None = None,
        preflight_thumbnail_src: str | None = None,
    ) -> PublishResult:
        creds = self._oauth_credentials()
        service = build("blogger", "v3", credentials=creds)
        clean_title = self._normalize_text_entities(title)
        clean_html = self._normalize_html_entities(html_body)
        clean_html = self._clean_html_tags(clean_html)
        preflight_src = str(preflight_thumbnail_src or "").strip()
        preflight_is_data = preflight_src.lower().startswith("data:image/")
        if images:
            if not (
                preflight_src
                and (
                    self._is_blogger_media_url(preflight_src)
                    or (preflight_is_data and self.thumbnail_data_uri_allowed)
                )
            ):
                preflight_src = self.preflight_thumbnail_blogger_media(images[0], creds=creds, max_attempts=2)
        lede_seed = self._first_text_paragraph(clean_html)
        post_html = self._merge_images(
            clean_html,
            images,
            creds,
            preflight_thumbnail_src=preflight_src,
        )
        pre_semantic_img_count = len(re.findall(r"<img\b[^>]*\bsrc=", post_html, flags=re.IGNORECASE))
        if self.semantic_html_enabled:
            post_html = self._semanticize_article_html(post_html, lede_hint=lede_seed)
            post_semantic_img_count = len(re.findall(r"<img\b[^>]*\bsrc=", post_html, flags=re.IGNORECASE))
            if post_semantic_img_count == 0 or post_semantic_img_count < pre_semantic_img_count:
                self._log_upload_event(
                    {
                        "event": "semanticize_image_repair_count",
                        "before_img_count": int(pre_semantic_img_count),
                        "after_img_count": int(post_semantic_img_count),
                    }
                )
                post_html = self.build_dry_run_html(post_html, images)
            try:
                self._assert_html_image_integrity(
                    post_html,
                    min_images=2,
                    require_no_figcaption=True,
                    strict_intro_alt=True,
                    allow_data_uri=bool(self.thumbnail_data_uri_allowed),
                    require_blogger_hosts=True,
                )
            except Exception as exc:
                self._log_upload_event(
                    {
                        "event": "semanticize_image_repair",
                        "reason": str(exc),
                    }
                )
                # Re-insert a deterministic preview image block as one-shot repair.
                post_html = self.build_dry_run_html(post_html, images)
        post_html += self._author_schema()
        seo_description = self._normalize_meta_description(meta_description)
        self._assert_english_only_payload(
            title=clean_title,
            html=post_html,
            labels=labels,
            meta_description=seo_description,
        )

        payload = {
            "title": clean_title,
            "content": post_html,
            "labels": labels,
        }
        if seo_description:
            payload["customMetaData"] = seo_description
        target_post_id = str(existing_draft_post_id or "").strip()
        if publish_at is None:
            if target_post_id:
                try:
                    service.posts().update(
                        blogId=self.blog_id,
                        postId=target_post_id,
                        body=payload,
                    ).execute()
                    result = service.posts().publish(
                        blogId=self.blog_id,
                        postId=target_post_id,
                    ).execute()
                    post_id = str(result.get("id", target_post_id))
                    self._assert_post_contains_images(service=service, post_id=post_id)
                    return PublishResult(url=result.get("url", ""), post_id=str(result.get("id", target_post_id)))
                except Exception:
                    target_post_id = ""
            result = service.posts().insert(blogId=self.blog_id, body=payload, isDraft=False).execute()
            post_id = str(result.get("id", "") or "")
            if post_id:
                self._assert_post_contains_images(service=service, post_id=post_id)
            return PublishResult(
                url=self._normalize_public_url(result.get("url", "")),
                post_id=str(result.get("id", "")),
            )

        post_id = target_post_id
        if post_id:
            try:
                service.posts().update(
                    blogId=self.blog_id,
                    postId=post_id,
                    body=payload,
                ).execute()
            except Exception:
                post_id = ""
        if not post_id:
            draft = service.posts().insert(blogId=self.blog_id, body=payload, isDraft=True).execute()
            post_id = str(draft.get("id", ""))
        # Blogger publish API expects RFC3339-style date-time.
        result = service.posts().publish(
            blogId=self.blog_id,
            postId=post_id,
            publishDate=publish_at.isoformat(),
        ).execute()
        published_id = str(result.get("id", post_id) or post_id)
        self._assert_post_contains_images(service=service, post_id=published_id)
        return PublishResult(
            url=self._normalize_public_url(result.get("url", "")),
            post_id=str(result.get("id", "")),
        )

    def build_dry_run_html(self, html_body: str, images: list[ImageAsset]) -> str:
        """
        Build a deterministic preview HTML in dry-run mode without network upload.
        Ensures regression checks can verify at least one <img> tag exists.
        """
        out = str(html_body or "")
        if not images:
            return out
        intro_text = self._first_paragraph_text(out)
        first = images[0]
        src1 = self._file_to_data_uri(first.path)
        if src1:
            alt1 = self._regen_alt_if_too_similar(first.alt, intro_text)
            out = self._insert_banner_at_top(out, self._image_block(src1, alt1))
        if len(images) > 1:
            second = images[1]
            src2 = self._file_to_data_uri(second.path)
            if src2:
                alt2 = self._regen_alt_if_too_similar(second.alt, intro_text)
                out = self._insert_inline_between_fix2_fix3(out, self._image_block(src2, alt2))
        self._assert_html_image_integrity(out, min_images=2, require_no_figcaption=True, strict_intro_alt=True)
        return out

    def publish_existing_draft(
        self,
        post_id: str,
        publish_at: datetime | None = None,
        title: str | None = None,
        html_body: str | None = None,
        labels: list[str] | None = None,
        meta_description: str | None = None,
    ) -> PublishResult:
        """Publish an already-created Blogger draft (optionally patching fields first)."""
        target_post_id = str(post_id or "").strip()
        if not target_post_id:
            raise RuntimeError("기존 draft post_id가 필요합니다.")
        creds = self._oauth_credentials()
        service = build("blogger", "v3", credentials=creds)

        payload: dict = {}
        if title is not None:
            payload["title"] = self._normalize_text_entities(title)
        if html_body is not None:
            clean_html = self._normalize_html_entities(html_body)
            clean_html = self._clean_html_tags(clean_html)
            if self.semantic_html_enabled:
                clean_html = self._semanticize_article_html(
                    clean_html,
                    lede_hint=self._first_text_paragraph(clean_html),
                )
            payload["content"] = clean_html
        if labels is not None:
            payload["labels"] = labels
        if meta_description is not None:
            payload["customMetaData"] = self._normalize_meta_description(meta_description)
        self._assert_english_only_payload(
            title=str(payload.get("title", "") or ""),
            html=str(payload.get("content", "") or ""),
            labels=list(payload.get("labels", []) or []),
            meta_description=str(payload.get("customMetaData", "") or ""),
        )

        if payload:
            service.posts().update(
                blogId=self.blog_id,
                postId=target_post_id,
                body=payload,
            ).execute()

        if publish_at is None:
            result = service.posts().publish(
                blogId=self.blog_id,
                postId=target_post_id,
            ).execute()
        else:
            result = service.posts().publish(
                blogId=self.blog_id,
                postId=target_post_id,
                publishDate=publish_at.isoformat(),
            ).execute()
        self._assert_post_contains_images(
            service=service,
            post_id=str(result.get("id", target_post_id) or target_post_id),
        )
        return PublishResult(
            url=self._normalize_public_url(result.get("url", "")),
            post_id=str(result.get("id", target_post_id)),
        )

    def save_draft_checkpoint(
        self,
        title: str,
        html_body: str,
        labels: list[str],
        stage: str = "working",
        reason: str = "",
        draft_post_id: str | None = None,
    ) -> PublishResult:
        """Save a recoverable Blogger draft snapshot for the current run stage."""
        creds = self._oauth_credentials()
        service = build("blogger", "v3", credentials=creds)
        clean_title = self._normalize_text_entities(title)
        clean_html = self._clean_html_tags(self._normalize_html_entities(html_body))
        stage_name = re.sub(r"[^a-z0-9_-]", "_", str(stage or "working").strip().lower()) or "working"
        banner = (
            f'<p><em>WIP checkpoint: {stage_name}'
            + (f" | reason: {html_lib.escape(str(reason or '')[:180])}" if reason else "")
            + "</em></p>"
        )
        payload = {
            "title": f"[WIP:{stage_name}] {clean_title}"[:180],
            "content": banner + clean_html + self._author_schema(),
            "labels": list(dict.fromkeys([*(labels or [])])),
        }
        target_post_id = str(draft_post_id or "").strip()
        if target_post_id:
            try:
                updated = service.posts().update(
                    blogId=self.blog_id,
                    postId=target_post_id,
                    body=payload,
                ).execute()
                return PublishResult(
                    url=self._normalize_public_url(str(updated.get("url", "") or "")),
                    post_id=str(updated.get("id", target_post_id) or target_post_id),
                )
            except Exception:
                target_post_id = ""
        draft = service.posts().insert(blogId=self.blog_id, body=payload, isDraft=True).execute()
        return PublishResult(
            url=self._normalize_public_url(str(draft.get("url", "") or "")),
            post_id=str(draft.get("id", "") or ""),
        )

    def can_notify_indexing(self) -> bool:
        """
        Auto capability check:
        - service account key exists, or
        - OAuth token includes indexing scope.
        """
        try:
            if self.service_account_path.exists():
                return True
        except Exception:
            pass
        try:
            creds = self._oauth_credentials()
            scopes = set(getattr(creds, "scopes", []) or [])
            return self._indexing_scope in scopes
        except Exception:
            return False

    def notify_indexing(self, url: str) -> None:
        normalized_url = self._normalize_public_url(url)
        token_value = ""
        if self.service_account_path.exists():
            credentials = service_account.Credentials.from_service_account_file(
                str(self.service_account_path),
                scopes=[self._indexing_scope],
            )
            token = credentials.with_scopes([self._indexing_scope])
            token.refresh(Request())
            token_value = str(token.token or "")
        else:
            creds = self._oauth_credentials()
            scopes = set(getattr(creds, "scopes", []) or [])
            if self._indexing_scope not in scopes:
                raise RuntimeError(
                    "indexing_credentials_missing: service_account.json 또는 indexing scope 토큰이 필요합니다."
                )
            self._ensure_valid_token(creds)
            token_value = str(getattr(creds, "token", "") or "")
            if not token_value:
                raise RuntimeError("indexing_oauth_token_missing")

        response = requests.post(
            "https://indexing.googleapis.com/v3/urlNotifications:publish",
            headers={
                "Authorization": f"Bearer {token_value}",
                "Content-Type": "application/json",
            },
            json={"url": normalized_url, "type": "URL_UPDATED"},
            timeout=30,
        )
        response.raise_for_status()

    def _oauth_credentials(self):
        from google.oauth2.credentials import Credentials

        if self.credentials_path.suffix.lower() == ".json":
            # Use scopes embedded in token JSON to avoid invalid_scope refresh failures
            # when legacy tokens were issued with a narrower scope set.
            return Credentials.from_authorized_user_file(str(self.credentials_path))
        raise RuntimeError("Unsupported credentials path for Blogger OAuth")

    def _normalize_text_entities(self, text: str) -> str:
        out = str(text or "")
        for _ in range(2):
            dec = html_lib.unescape(out)
            if dec == out:
                break
            out = dec
        return out

    def _normalize_html_entities(self, html: str) -> str:
        out = str(html or "")
        for _ in range(2):
            dec = html_lib.unescape(out)
            if dec == out:
                break
            out = dec
        return out

    def _normalize_meta_description(self, description: str | None) -> str:
        raw = str(description or "").strip()
        if not raw:
            return ""
        out = self._normalize_text_entities(raw)
        out = re.sub(r"\s+", " ", out).strip()
        if len(out) > 160:
            out = out[:157].rstrip(" ,.;:") + "..."
        return out

    def _contains_hangul(self, text: str) -> bool:
        return bool(re.search(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", str(text or "")))

    def _assert_english_only_payload(
        self,
        title: str,
        html: str,
        labels: list[str],
        meta_description: str,
    ) -> None:
        chunks = [
            ("title", str(title or "")),
            ("html", str(html or "")),
            ("labels", " ".join(str(x or "") for x in (labels or []))),
            ("meta_description", str(meta_description or "")),
        ]
        for key, value in chunks:
            if not value:
                continue
            if self._contains_hangul(value):
                raise RuntimeError(f"english_only_gate_failed:{key}:hangul_detected")

    def _clean_html_tags(self, html: str) -> str:
        out = str(html or "")
        # Remove active tags and obvious placeholder leak text before publish.
        out = re.sub(r"<script\\b[^>]*>.*?</script>", "", out, flags=re.IGNORECASE | re.DOTALL)
        out = re.sub(r"<style\\b[^>]*>.*?</style>", "", out, flags=re.IGNORECASE | re.DOTALL)
        # Keep body markup semantic and lightweight: remove inline styles and JS handlers.
        out = re.sub(r"\sstyle=\"[^\"]*\"", "", out, flags=re.IGNORECASE)
        out = re.sub(r"\sstyle='[^']*'", "", out, flags=re.IGNORECASE)
        out = re.sub(r"\son[a-z]+\s*=\s*\"[^\"]*\"", "", out, flags=re.IGNORECASE)
        out = re.sub(r"\son[a-z]+\s*=\s*'[^']*'", "", out, flags=re.IGNORECASE)
        out = re.sub(
            r"(section context visual|concept visual|supporting chart|focused screenshot)",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"<p[^>]*>\s*illustration\s+showing[^<]*</p>",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"https?://(?:www\.)?google\.com/[^\s\"<]*",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"(?m)^\s*#{1,6}\s+(.+)$",
            "",
            out,
        )
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out.strip()

    def _merge_images(
        self,
        html_body: str,
        images: list[ImageAsset],
        creds,
        preflight_thumbnail_src: str = "",
    ) -> str:
        if not images:
            raise RuntimeError("이미지 자산이 비어 있습니다. retry required")

        self._log_upload_event(
            {
                "event": "merge_images_start",
                "images_in": len(images or []),
                "hosted_urls_count": 0,
                "image_hosting_backend": str(self.image_hosting_backend or ""),
            }
        )
        src_map: dict[str, str] = {}
        if preflight_thumbnail_src and images:
            src_map[str(images[0].path)] = str(preflight_thumbnail_src).strip()
        hosted_urls = self._upload_images(images=images, creds=creds)
        for k, v in dict(hosted_urls or {}).items():
            clean = str(v or "").strip()
            if clean:
                src_map[str(k)] = clean
        self._log_upload_event(
            {
                "event": "merge_images_start",
                "images_in": len(images or []),
                "hosted_urls_count": len(src_map),
                "image_hosting_backend": str(self.image_hosting_backend or ""),
            }
        )
        thumbnail = images[0]
        thumbnail_kind = (getattr(thumbnail, "source_kind", "") or "").strip().lower()
        if thumbnail_kind not in {"gemini", "generated"}:
            raise RuntimeError("Thumbnail must be generated image. retry required")
        thumb_src = str(preflight_thumbnail_src or "").strip()
        thumb_is_data = thumb_src.lower().startswith("data:image/")
        if not thumb_src:
            thumb_src = src_map.get(str(thumbnail.path), "")
            thumb_is_data = str(thumb_src).strip().lower().startswith("data:image/")
        if not thumb_src or (
            (not self._is_blogger_media_url(thumb_src))
            and not (thumb_is_data and self.thumbnail_data_uri_allowed)
        ):
            thumb_src = self._recover_thumbnail_blogger_src(
                thumbnail=thumbnail,
                images=images,
                src_map=src_map,
                creds=creds,
            )
            thumb_is_data = str(thumb_src).strip().lower().startswith("data:image/")
        if not thumb_src and not self.strict_thumbnail_blogger_media:
            thumb_src = self._pick_relaxed_thumbnail_src(src_map=src_map, images=images)
        if (
            thumb_src
            and str(thumb_src).strip().lower().startswith("data:image/")
            and (not self.thumbnail_data_uri_allowed)
        ):
            self._log_upload_event(
                {
                    "event": "thumbnail_data_uri_blocked",
                    "path": str(getattr(thumbnail, "path", "") or ""),
                }
            )
            thumb_src = ""
        if (
            thumb_src
            and self.strict_thumbnail_blogger_media
            and (not self._is_blogger_media_url(str(thumb_src)))
            and not (str(thumb_src).strip().lower().startswith("data:image/") and self.thumbnail_data_uri_allowed)
        ):
            self._log_upload_event(
                {
                    "event": "thumbnail_non_blogger_blocked",
                    "thumb_src": str(thumb_src)[:220],
                }
            )
            thumb_src = ""
        if not thumb_src:
            self._log_upload_event(
                {
                    "event": "thumbnail_hosting_gate_failed",
                    "blog_id": self.blog_id,
                    "token_scopes": self._token_scopes(creds),
                    "token_expiry": str(getattr(creds, "expiry", "") or ""),
                    "last_upload_report": dict(self._last_upload_report or {}),
                }
            )
            raise RuntimeError("publish failed - missing thumbnail image url. retry required")

        # Keep diagnostics for missing uploads, but do not hard-fail before final post verification.
        missing_paths = [str(img.path) for img in images if str(img.path) not in src_map]
        if missing_paths:
            self._log_upload_event(
                {
                    "event": "image_upload_partial",
                    "missing_paths": missing_paths[:8],
                    "uploaded": len(src_map),
                    "requested": len(images),
                }
            )

        # Never render the same image URL repeatedly in one post.
        unique_images: list[ImageAsset] = []
        seen_src: set[str] = set()
        for image in images:
            src = str(src_map.get(str(image.path), "") or "").strip()
            if not src:
                continue
            if src in seen_src:
                continue
            seen_src.add(src)
            unique_images.append(image)
        images = unique_images

        # Final insertion policy (fixed):
        # - banner image: exactly 1 (top, before first H2)
        # - inline image: exactly 1 (between Fix 2 and Fix 3 if possible)
        html = str(html_body or "")
        intro_text = self._first_paragraph_text(html)
        thumbnail.alt = self._regen_alt_if_too_similar(thumbnail.alt, intro_text)
        banner_block = self._image_block(thumb_src, thumbnail.alt)
        html = self._insert_banner_at_top(html, banner_block)

        if len(images) > 1:
            inline = images[1]
            inline_src = str(src_map.get(str(inline.path), "") or "").strip()
            if not inline_src:
                try:
                    inline_src = self._file_to_data_uri(inline.path)
                except Exception:
                    inline_src = ""
            if not inline_src:
                inline_src = self._fallback_asset_data_uri(role="inline")
            if inline_src and (inline_src.startswith("https://") or inline_src.startswith("http://")):
                inline.alt = self._regen_alt_if_too_similar(inline.alt, intro_text)
                inline_block = self._image_block(inline_src, inline.alt)
                html = self._insert_inline_between_fix2_fix3(html, inline_block)
            elif inline_src and inline_src.startswith("data:"):
                inline.alt = self._regen_alt_if_too_similar(inline.alt, intro_text)
                inline_block = self._image_block(inline_src, inline.alt)
                html = self._insert_inline_between_fix2_fix3(html, inline_block)
        else:
            inline_fallback_src = self._fallback_asset_data_uri(role="inline")
            if inline_fallback_src:
                inline_alt = self._regen_alt_if_too_similar("Troubleshooting process diagram.", intro_text)
                html = self._insert_inline_between_fix2_fix3(
                    html,
                    self._image_block(inline_fallback_src, inline_alt),
                )

        img_count = len(re.findall(r"<img\b[^>]*\bsrc=", html, flags=re.IGNORECASE))
        self._log_upload_event(
            {
                "event": "merge_images_inserted",
                "img_count_after_insert": img_count,
                "banner_inserted": bool(re.search(r"<img\b[^>]*\bsrc=", banner_block, flags=re.IGNORECASE)),
                "inline_inserted": img_count >= 2,
            }
        )
        if img_count < 2:
            self._log_upload_event(
                {
                    "event": "go_live_gate_fail",
                    "reason": "insufficient_html_images_before_submit",
                    "html_img_count": img_count,
                    "html_preview_500chars": str(html or "")[:500],
                }
            )
            raise RuntimeError(f"publish failed - missing images before submit ({img_count}/2)")
        try:
            self._assert_html_image_integrity(
                html,
                min_images=2,
                require_no_figcaption=True,
                strict_intro_alt=True,
                allow_data_uri=bool(self.thumbnail_data_uri_allowed),
                require_blogger_hosts=True,
            )
        except Exception as exc:
            self._log_upload_event(
                {
                    "event": "go_live_gate_fail",
                    "reason": str(exc),
                    "html_img_count": len(re.findall(r"<img\b[^>]*\bsrc=", html, flags=re.IGNORECASE)),
                    "html_preview_500chars": str(html or "")[:500],
                }
            )
            raise
        return html

    def _recover_thumbnail_blogger_src(
        self,
        thumbnail: ImageAsset,
        images: list[ImageAsset],
        src_map: dict[str, str],
        creds,
    ) -> str:
        # 1) Retry thumbnail-only preflight path first.
        try:
            src = self.preflight_thumbnail_blogger_media(thumbnail, creds=creds, max_attempts=2)
            if src and (
                self._is_blogger_media_url(src)
                or (str(src).strip().lower().startswith("data:image/") and self.thumbnail_data_uri_allowed)
            ):
                src_map[str(thumbnail.path)] = src
                return src
        except Exception:
            pass

        # 2) Keep strict Blogger hosting, but allow another generated image URL only in relaxed mode.
        if self.strict_thumbnail_blogger_media:
            return ""
        for img in images:
            kind = (getattr(img, "source_kind", "") or "").strip().lower()
            if kind not in {"gemini", "generated"}:
                continue
            src = str(src_map.get(str(img.path), "") or "").strip()
            if src and self._is_blogger_media_url(src):
                return src
        return ""

    def _pick_relaxed_thumbnail_src(self, src_map: dict[str, str], images: list[ImageAsset]) -> str:
        # 1) Prefer Blogger-hosted URLs from known image order.
        for img in images:
            src = str(src_map.get(str(img.path), "") or "").strip()
            if src and self._is_blogger_media_url(src):
                return src
        # 2) Final pass over map values.
        for src in src_map.values():
            clean = str(src or "").strip()
            if clean and self._is_blogger_media_url(clean):
                return clean
        return ""

    def get_last_upload_report(self) -> dict:
        return dict(self._last_upload_report or {})

    def _upload_images(self, images: list[ImageAsset], creds) -> dict[str, str]:
        backend = (self.image_hosting_backend or "blogger_media").strip().lower()
        requested = len(images or [])
        if backend == "gcs":
            hosted = self._upload_images_to_gcs(images)
            self._last_upload_report = self._compose_upload_report(
                backend="gcs",
                requested=requested,
                hosted=hosted,
            )
            return hosted
        if backend in {"blogger_media", "blogger", "blogger_server"}:
            hosted = self._upload_images_to_blogger_media(images, creds)
            self._last_upload_report = self._compose_upload_report(
                backend="blogger_media",
                requested=requested,
                hosted=hosted,
                fallback_backend="",
            )
            return hosted
        if backend in {"drive", "photos"}:
            raise RuntimeError("drive_backend_disabled")
        raise RuntimeError(f"지원하지 않는 이미지 호스팅 백엔드: {backend}")

    def _compose_upload_report(
        self,
        backend: str,
        requested: int,
        hosted: dict[str, str],
        fallback_backend: str = "",
    ) -> dict:
        hosts: list[str] = []
        seen: set[str] = set()
        for url in hosted.values():
            try:
                host = (urlparse(str(url)).netloc or "").lower()
            except Exception:
                host = ""
            if not host or host in seen:
                continue
            seen.add(host)
            hosts.append(host)
        return {
            "backend": backend,
            "requested": int(max(0, requested)),
            "uploaded": int(len(hosted or {})),
            "hosts": hosts,
            "fallback_backend": fallback_backend,
        }

    def _upload_images_to_blogger_media(self, images: list[ImageAsset], creds) -> dict[str, str]:
        # Blogger v3 has no public binary media endpoint; use a temporary draft-post
        # roundtrip so Blogger stores images on its own media infrastructure.
        hosted: dict[str, str] = {}
        for idx, image in enumerate(images, start=1):
            path = image.path
            if not path.exists():
                continue
            mime, _ = mimetypes.guess_type(path.name)
            mime = mime or "image/png"
            role = "thumbnail" if idx == 1 else "content"
            upload_path, upload_mime, cleanup_path = self._prepare_blogger_upload_asset(
                path,
                mime,
                role=role,
            )
            try:
                uploaded = self._upload_via_blogger_endpoint(upload_path, upload_mime, creds)
                if uploaded:
                    hosted[str(path)] = uploaded
                    continue
                src = self._upload_via_temp_draft_roundtrip(upload_path, upload_mime, creds, idx=idx)
                if not src:
                    continue
                if (not self._is_blogger_media_url(src)) and not (
                    str(src).strip().lower().startswith("data:image/") and self.thumbnail_data_uri_allowed
                ):
                    continue
                hosted[str(path)] = src
            except Exception:
                continue
            finally:
                if cleanup_path and cleanup_path.exists():
                    try:
                        cleanup_path.unlink(missing_ok=True)
                    except Exception:
                        pass
        return hosted

    def _prepare_blogger_upload_asset(
        self,
        path: Path,
        mime: str,
        role: str = "content",
    ) -> tuple[Path, str, Path | None]:
        safe_mime = (mime or "").lower().strip()
        role_key = str(role or "content").strip().lower()
        target_width = 1200 if role_key == "thumbnail" else 960
        # Upload-time resize policy:
        # - thumbnail: max 1200px width
        # - content: max 960px width
        # also normalize unsupported mime to PNG for Blogger reliability.
        try:
            with Image.open(path) as im:
                working = im
                if im.width > target_width:
                    ratio = target_width / float(im.width)
                    new_height = max(1, int(im.height * ratio))
                    working = im.resize((target_width, new_height), Image.Resampling.LANCZOS)

                # Keep supported mime when possible, otherwise convert to PNG.
                if safe_mime in {"image/jpeg", "image/jpg"}:
                    with tempfile.NamedTemporaryFile(
                        delete=False,
                        suffix=".jpg",
                        prefix="rz_blog_",
                        dir=str(path.parent),
                    ) as tmp:
                        tmp_path = Path(tmp.name)
                    img = working.convert("RGB") if working.mode not in {"RGB", "L"} else working
                    img.save(tmp_path, format="JPEG", quality=84, optimize=True, progressive=True)
                    return tmp_path, "image/jpeg", tmp_path

                if safe_mime == "image/png":
                    with tempfile.NamedTemporaryFile(
                        delete=False,
                        suffix=".png",
                        prefix="rz_blog_",
                        dir=str(path.parent),
                    ) as tmp:
                        tmp_path = Path(tmp.name)
                    working.save(tmp_path, format="PNG", optimize=True, compress_level=9)
                    return tmp_path, "image/png", tmp_path

                if safe_mime == "image/gif":
                    # GIF is already accepted; keep as-is unless resized path is needed.
                    if working is im:
                        return path, safe_mime, None
                    with tempfile.NamedTemporaryFile(
                        delete=False,
                        suffix=".gif",
                        prefix="rz_blog_",
                        dir=str(path.parent),
                    ) as tmp:
                        tmp_path = Path(tmp.name)
                    working.save(tmp_path, format="GIF")
                    return tmp_path, "image/gif", tmp_path

                # Unsupported mime (webp/avif/etc.) => PNG.
                with tempfile.NamedTemporaryFile(
                    delete=False,
                    suffix=".png",
                    prefix="rz_blog_",
                    dir=str(path.parent),
                ) as tmp:
                    tmp_path = Path(tmp.name)
                mode = "RGBA" if "A" in (working.mode or "") else "RGB"
                converted = working.convert(mode)
                converted.save(tmp_path, format="PNG", optimize=True, compress_level=9)
            return tmp_path, "image/png", tmp_path
        except Exception:
            return path, safe_mime or "image/png", None

    def _upload_via_blogger_endpoint(self, path: Path, mime: str, creds) -> str:
        details = self._upload_via_blogger_endpoint_detailed(path=path, mime=mime, creds=creds)
        return str(details.get("extracted_url", "") or "").strip()

    def _upload_via_blogger_endpoint_detailed(self, path: Path, mime: str, creds) -> dict[str, Any]:
        endpoint = "https://www.blogger.com/upload-image.g"
        response = None
        details: dict[str, Any] = {
            "endpoint": endpoint,
            "file": str(getattr(path, "name", "")),
            "mime": str(mime or ""),
            "status_code": 0,
            "response_preview": "",
            "extracted_url": "",
            "extracted_host": "",
            "reason_code": "",
            "ok": False,
        }
        try:
            self._ensure_valid_token(creds)
            with path.open("rb") as fh:
                response = requests.post(
                    endpoint,
                    params={
                        "blogID": self.blog_id,
                        "source": "post",
                        "zx": datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f"),
                    },
                    headers={"Authorization": f"Bearer {creds.token}"},
                    files={"image": (path.name, fh, mime)},
                    timeout=60,
                )
        except Exception as exc:
            msg = str(exc or "")
            details["status_code"] = "exception"
            details["response_preview"] = msg[:800]
            lower = msg.lower()
            if "invalid_scope" in lower:
                details["reason_code"] = "invalid_scope"
            elif "timeout" in lower:
                details["reason_code"] = "timeout"
            else:
                details["reason_code"] = "http_4xx_or_5xx"
            self._last_upload_report = dict(details)
            self._log_upload_event({"event": "blogger_media_upload_exception", **details})
            return details
        details["status_code"] = int(getattr(response, "status_code", 0) or 0)
        details["response_preview"] = str(getattr(response, "text", "") or "")[:800]
        if response.status_code not in {200, 201}:
            lower = str(response.text or "").lower()
            details["reason_code"] = "invalid_scope" if ("invalid_scope" in lower or "insufficientpermission" in lower) else "http_4xx_or_5xx"
            self._last_upload_report = dict(details)
            self._log_upload_event({"event": "blogger_media_upload_failed", **details})
            return details
        extracted, parse_source = self._extract_upload_url_from_response(str(response.text or ""))
        details["extracted_url"] = extracted
        details["extracted_host"] = (urlparse(extracted).netloc or "").lower() if extracted else ""
        if extracted:
            details["ok"] = True
            if self._is_blogger_media_url(extracted):
                details["reason_code"] = ""
            else:
                details["reason_code"] = "non_blogger_host"
            self._last_upload_report = dict(details)
            self._log_upload_event({"event": "blogger_media_upload_ok", "parse_source": parse_source, **details})
            return details
        details["reason_code"] = "response_parse_failed"
        self._last_upload_report = dict(details)
        self._log_upload_event({"event": "blogger_media_upload_no_url", "parse_source": parse_source, **details})
        return details

    def _upload_via_temp_draft_roundtrip(
        self,
        path: Path,
        mime: str,
        creds,
        idx: int = 1,
        *,
        insert_as_draft: bool = True,
        publish_after_insert: bool = True,
        prefer_reader: bool = False,
        poll_count: int = 8,
        poll_delay_sec: float = 1.2,
    ) -> str:
        """
        Recovery path when upload-image.g fails.
        Strategy:
          1) create temp post with data URI (draft or direct)
          2) publish it when needed
          3) refetch post content and extract first <img src>
          4) delete temp post
        Returns first image src (Blogger URL or data URI), otherwise "".
        """
        service = build("blogger", "v3", credentials=creds)
        post_id = ""
        try:
            data_uri = self._image_data_uri(path, mime)
            payload = {
                "title": f"[asset] {datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')} {idx}",
                "content": f'<p><img src="{data_uri}" alt="asset {idx}" /></p>',
            }
            draft = service.posts().insert(
                blogId=self.blog_id,
                body=payload,
                isDraft=bool(insert_as_draft),
            ).execute()
            post_id = str(draft.get("id", "") or "")
            if not post_id:
                return ""

            if publish_after_insert:
                try:
                    service.posts().publish(blogId=self.blog_id, postId=post_id).execute()
                except Exception:
                    pass

            preferred_view = "READER" if prefer_reader else "ADMIN"
            secondary_view = "ADMIN" if prefer_reader else "READER"
            polls = max(1, int(poll_count or 1))
            delay = max(0.2, float(poll_delay_sec or 1.2))
            first_data_uri = ""
            for _ in range(polls):
                try:
                    fetched = service.posts().get(
                        blogId=self.blog_id,
                        postId=post_id,
                        view=preferred_view,
                    ).execute()
                    src = self._extract_first_img_src(str(fetched.get("content", "") or ""))
                    if src and self._is_blogger_media_url(src):
                        return src
                    if src and src.lower().startswith("data:image/") and not first_data_uri:
                        first_data_uri = src
                except Exception:
                    pass

                try:
                    fetched2 = service.posts().get(
                        blogId=self.blog_id,
                        postId=post_id,
                        view=secondary_view,
                    ).execute()
                    src2 = self._extract_first_img_src(str(fetched2.get("content", "") or ""))
                    if src2 and self._is_blogger_media_url(src2):
                        return src2
                    if src2 and src2.lower().startswith("data:image/") and not first_data_uri:
                        first_data_uri = src2
                except Exception:
                    pass

                time.sleep(delay)
            return first_data_uri
        except Exception:
            return ""
        finally:
            if post_id:
                try:
                    service.posts().delete(blogId=self.blog_id, postId=post_id).execute()
                except Exception:
                    pass

    def _extract_upload_url_from_response(self, text: str) -> tuple[str, str]:
        raw = str(text or "")
        patterns = [
            r"https://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*blogger\.googleusercontent\.com[^\s\"'<>]*",
            r"https://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*bp\.blogspot\.com[^\s\"'<>]*",
        ]
        for pat in patterns:
            m = re.search(pat, raw, flags=re.IGNORECASE)
            if m:
                return str(m.group(0) or "").strip(), "regex"
        try:
            parsed = json.loads(raw)
            url = self._search_url_in_json(parsed)
            if url:
                return url, "json"
        except Exception:
            pass
        m = re.search(
            r"src=[\"'](https://[^\"']*(?:blogger\.googleusercontent\.com|bp\.blogspot\.com)[^\"']*)[\"']",
            raw,
            flags=re.IGNORECASE,
        )
        if m:
            return str(m.group(1) or "").strip(), "html_src"
        return "", "none"

    def _search_url_in_json(self, value: Any) -> str:
        key_hints = {"url", "imageurl", "link", "contenturl", "src", "mediaurl"}
        stack: list[Any] = [value]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                for k, v in node.items():
                    try:
                        key = str(k or "").strip().lower()
                    except Exception:
                        key = ""
                    if isinstance(v, str):
                        vv = v.strip()
                        if vv.startswith("https://") and (
                            "blogger.googleusercontent.com" in vv.lower() or "bp.blogspot.com" in vv.lower()
                        ):
                            return vv
                        if key in key_hints and vv.startswith("http"):
                            return vv
                    elif isinstance(v, (dict, list, tuple)):
                        stack.append(v)
            elif isinstance(node, (list, tuple)):
                for item in node:
                    if isinstance(item, (dict, list, tuple)):
                        stack.append(item)
                    elif isinstance(item, str):
                        vv = item.strip()
                        if vv.startswith("https://") and (
                            "blogger.googleusercontent.com" in vv.lower() or "bp.blogspot.com" in vv.lower()
                        ):
                            return vv
        return ""

    def preflight_thumbnail_blogger_media(self, thumbnail: ImageAsset, creds=None, *, max_attempts: int = 2) -> str:
        path = Path(getattr(thumbnail, "path", ""))
        if not path.exists():
            self._log_thumbnail_gate_event(
                {
                    "event": "thumbnail_gate_result",
                    "ok": False,
                    "reason_code": "file_missing",
                    "source_path": str(path),
                }
            )
            raise RuntimeError("thumbnail_preflight_failed:file_missing")
        if creds is None:
            creds = self._oauth_credentials()

        last_reason = "missing_extracted_url"
        for attempt_no in range(1, max(1, int(max_attempts)) + 1):
            cleanup_path: Path | None = None
            upload_path = path
            upload_mime = mimetypes.guess_type(path.name)[0] or "image/png"
            resized_w = 0
            resized_h = 0
            file_size = 0
            try:
                if attempt_no == 1:
                    upload_path, upload_mime, cleanup_path = self._prepare_blogger_upload_asset(
                        path,
                        upload_mime,
                        role="thumbnail",
                    )
                else:
                    upload_path, upload_mime, cleanup_path = self._prepare_thumbnail_jpeg_asset(path, quality=82)

                file_size = int(upload_path.stat().st_size) if upload_path.exists() else 0
                try:
                    with Image.open(upload_path) as im:
                        resized_w, resized_h = int(im.width), int(im.height)
                except Exception:
                    resized_w, resized_h = 0, 0

                self._log_thumbnail_gate_event(
                    {
                        "event": "thumbnail_upload_attempt",
                        "attempt_no": attempt_no,
                        "source_path": str(path),
                        "prepared_path": str(upload_path),
                        "mime": str(upload_mime or ""),
                        "file_size_bytes": file_size,
                        "resized_width": resized_w,
                        "resized_height": resized_h,
                        "token_scopes": self._token_scopes(creds),
                        "token_expiry": str(getattr(creds, "expiry", "") or ""),
                    }
                )

                if file_size < 10 * 1024:
                    last_reason = "file_too_small"
                    self._log_thumbnail_gate_event(
                        {
                            "event": "thumbnail_gate_result",
                            "attempt_no": attempt_no,
                            "ok": False,
                            "reason_code": last_reason,
                        }
                    )
                    continue

                details = self._upload_via_blogger_endpoint_detailed(upload_path, upload_mime, creds)
                extracted_url = str(details.get("extracted_url", "") or "").strip()
                extracted_host = str(details.get("extracted_host", "") or "").strip().lower()
                reason_code = str(details.get("reason_code", "") or "").strip()
                self._log_thumbnail_gate_event(
                    {
                        "event": "thumbnail_upload_response",
                        "attempt_no": attempt_no,
                        "status_code": details.get("status_code", 0),
                        "response_preview": str(details.get("response_preview", "") or "")[:800],
                        "extracted_url": extracted_url,
                        "extracted_host": extracted_host,
                    }
                )

                if extracted_url and self._is_blogger_media_url(extracted_url):
                    self._log_thumbnail_gate_event(
                        {
                            "event": "thumbnail_gate_result",
                            "attempt_no": attempt_no,
                            "ok": True,
                            "reason_code": "",
                            "thumbnail_url": extracted_url,
                        }
                    )
                    return extracted_url

                # Blogger endpoint may fail in some environments (e.g. 405).
                # Recovery path: temp-post roundtrip with multiple strategies.
                endpoint_status = int(details.get("status_code", 0) or 0)
                strategy_rows = [
                    (
                        "draft_publish_admin",
                        {
                            "insert_as_draft": True,
                            "publish_after_insert": True,
                            "prefer_reader": False,
                            "poll_count": 8,
                            "poll_delay_sec": 1.0,
                        },
                    ),
                    (
                        "draft_publish_reader",
                        {
                            "insert_as_draft": True,
                            "publish_after_insert": True,
                            "prefer_reader": True,
                            "poll_count": 8,
                            "poll_delay_sec": 1.0,
                        },
                    ),
                    (
                        "direct_admin",
                        {
                            "insert_as_draft": False,
                            "publish_after_insert": False,
                            "prefer_reader": False,
                            "poll_count": 6,
                            "poll_delay_sec": 0.8,
                        },
                    ),
                    (
                        "direct_reader",
                        {
                            "insert_as_draft": False,
                            "publish_after_insert": False,
                            "prefer_reader": True,
                            "poll_count": 6,
                            "poll_delay_sec": 0.8,
                        },
                    ),
                ]
                roundtrip_data_uri = ""
                for strategy_idx, (strategy_name, strategy_kwargs) in enumerate(strategy_rows, start=1):
                    roundtrip_url = self._upload_via_temp_draft_roundtrip(
                        upload_path,
                        upload_mime,
                        creds,
                        idx=(attempt_no * 10) + strategy_idx,
                        **strategy_kwargs,
                    )
                    roundtrip_host = (urlparse(roundtrip_url).netloc or "").lower() if roundtrip_url else ""
                    self._log_thumbnail_gate_event(
                        {
                            "event": "thumbnail_upload_roundtrip_response",
                            "attempt_no": attempt_no,
                            "strategy": strategy_name,
                            "extracted_url": roundtrip_url,
                            "extracted_host": roundtrip_host,
                            "endpoint_status": endpoint_status,
                        }
                    )
                    if roundtrip_url and self._is_blogger_media_url(roundtrip_url):
                        self._log_thumbnail_gate_event(
                            {
                                "event": "thumbnail_gate_result",
                                "attempt_no": attempt_no,
                                "ok": True,
                                "reason_code": f"temp_post_publish_roundtrip:{strategy_name}",
                                "thumbnail_url": roundtrip_url,
                            }
                        )
                        return roundtrip_url
                    if roundtrip_url and str(roundtrip_url).strip().lower().startswith("data:image/"):
                        roundtrip_data_uri = roundtrip_url
                        if self.thumbnail_data_uri_allowed:
                            self._log_thumbnail_gate_event(
                                {
                                    "event": "thumbnail_gate_result",
                                    "attempt_no": attempt_no,
                                    "ok": True,
                                    "reason_code": f"data_uri_roundtrip_allowed:{strategy_name}",
                                    "thumbnail_url": roundtrip_url[:120],
                                }
                            )
                            return roundtrip_url
                        if self.auto_allow_data_uri_on_blogger_405 and endpoint_status == 405:
                            self.thumbnail_data_uri_allowed = True
                            self._log_thumbnail_gate_event(
                                {
                                    "event": "thumbnail_data_uri_auto_allowed",
                                    "attempt_no": attempt_no,
                                    "strategy": strategy_name,
                                    "endpoint_status": endpoint_status,
                                }
                            )
                            self._log_thumbnail_gate_event(
                                {
                                    "event": "thumbnail_gate_result",
                                    "attempt_no": attempt_no,
                                    "ok": True,
                                    "reason_code": f"data_uri_roundtrip_auto_allowed_405:{strategy_name}",
                                    "thumbnail_url": roundtrip_url[:120],
                                }
                            )
                            return roundtrip_url
                if (
                    (not roundtrip_data_uri)
                    and self.auto_allow_data_uri_on_blogger_405
                    and endpoint_status == 405
                ):
                    synthesized_data_uri = self._image_data_uri(upload_path, upload_mime)
                    if synthesized_data_uri:
                        self.thumbnail_data_uri_allowed = True
                        self._log_thumbnail_gate_event(
                            {
                                "event": "thumbnail_data_uri_synthesized",
                                "attempt_no": attempt_no,
                                "endpoint_status": endpoint_status,
                                "ok": True,
                            }
                        )
                        self._log_thumbnail_gate_event(
                            {
                                "event": "thumbnail_gate_result",
                                "attempt_no": attempt_no,
                                "ok": True,
                                "reason_code": "data_uri_synthesized_auto_allowed_405",
                            }
                        )
                        return synthesized_data_uri
                if roundtrip_data_uri and not self.thumbnail_data_uri_allowed:
                    last_reason = "thumbnail_data_uri_not_allowed"
                    self._log_thumbnail_gate_event(
                        {
                            "event": "thumbnail_gate_result",
                            "attempt_no": attempt_no,
                            "ok": False,
                            "reason_code": last_reason,
                        }
                    )
                    continue

                if not extracted_url:
                    last_reason = reason_code or "missing_extracted_url"
                elif extracted_host and (not self._is_blogger_media_url(extracted_url)):
                    last_reason = "non_blogger_host"
                else:
                    last_reason = reason_code or "response_parse_failed"

                self._log_thumbnail_gate_event(
                    {
                        "event": "thumbnail_gate_result",
                        "attempt_no": attempt_no,
                        "ok": False,
                        "reason_code": last_reason,
                    }
                )
            except requests.Timeout:
                last_reason = "timeout"
                self._log_thumbnail_gate_event(
                    {
                        "event": "thumbnail_gate_result",
                        "attempt_no": attempt_no,
                        "ok": False,
                        "reason_code": last_reason,
                    }
                )
            except Exception as exc:
                msg = str(exc or "")
                lower = msg.lower()
                if "invalid_scope" in lower or "insufficientpermission" in lower:
                    last_reason = "invalid_scope"
                else:
                    last_reason = "http_4xx_or_5xx"
                self._log_thumbnail_gate_event(
                    {
                        "event": "thumbnail_gate_result",
                        "attempt_no": attempt_no,
                        "ok": False,
                        "reason_code": last_reason,
                        "error": msg[:220],
                    }
                )
            finally:
                if cleanup_path and cleanup_path.exists():
                    try:
                        cleanup_path.unlink(missing_ok=True)
                    except Exception:
                        pass

        raise RuntimeError(f"thumbnail_preflight_failed:{last_reason}")

    def _prepare_thumbnail_jpeg_asset(self, path: Path, quality: int = 82) -> tuple[Path, str, Path]:
        q = max(60, min(95, int(quality or 82)))
        with Image.open(path) as im:
            working = im
            target_width = 1200
            if im.width > target_width:
                ratio = target_width / float(im.width)
                new_height = max(1, int(im.height * ratio))
                working = im.resize((target_width, new_height), Image.Resampling.LANCZOS)
            with tempfile.NamedTemporaryFile(
                delete=False,
                suffix=".jpg",
                prefix="rz_thumb_",
                dir=str(path.parent),
            ) as tmp:
                tmp_path = Path(tmp.name)
            rgb = working.convert("RGB") if working.mode not in {"RGB", "L"} else working
            rgb.save(tmp_path, format="JPEG", quality=q, optimize=True, progressive=True)
        return tmp_path, "image/jpeg", tmp_path

    def _log_thumbnail_gate_event(self, payload: dict) -> None:
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            **(payload or {}),
        }
        try:
            with self._thumbnail_gate_log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _assert_post_contains_images(self, service, post_id: str) -> None:
        pid = str(post_id or "").strip()
        if not pid:
            raise RuntimeError("publish failed - missing post id")
        post = service.posts().get(
            blogId=self.blog_id,
            postId=pid,
            view="ADMIN",
        ).execute()
        content = str(post.get("content", "") or "")
        src_values = re.findall(r"<img\b[^>]*\bsrc=\"([^\"]+)\"", content, flags=re.IGNORECASE)
        valid_http_src = [
            s for s in src_values
            if str(s).strip() and (
                str(s).strip().lower().startswith("https://")
                or str(s).strip().lower().startswith("http://")
            )
        ]
        valid_data_src = [
            s for s in src_values
            if str(s).strip().lower().startswith("data:image/")
        ]
        valid_total = len(valid_http_src) + (len(valid_data_src) if self.thumbnail_data_uri_allowed else 0)
        if valid_total < 2:
            self._log_upload_event(
                {
                    "event": "go_live_gate_fail",
                    "reason": "publish_missing_images_after_submit",
                    "post_id": pid,
                    "html_img_count": len(src_values),
                    "html_preview_500chars": content[:500],
                }
            )
            self._log_upload_event(
                {
                    "event": "publish_missing_images",
                    "post_id": pid,
                    "img_count": len(src_values),
                    "valid_http_src_count": len(valid_http_src),
                    "valid_data_src_count": len(valid_data_src),
                    "title": str(post.get("title", "") or "")[:180],
                    "content_preview": content[:500],
                }
            )
            raise RuntimeError("publish failed - missing images")
        self._assert_html_image_integrity(
            content,
            min_images=2,
            require_no_figcaption=True,
            strict_intro_alt=True,
            allow_data_uri=bool(self.thumbnail_data_uri_allowed),
            require_blogger_hosts=True,
        )

    def _log_upload_event(self, payload: dict) -> None:
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            **(payload or {}),
        }
        try:
            with self._upload_log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _first_paragraph_text(self, html: str) -> str:
        m = re.search(r"<p[^>]*>(.*?)</p>", str(html or ""), flags=re.IGNORECASE | re.DOTALL)
        if not m:
            return ""
        txt = re.sub(r"<[^>]+>", " ", str(m.group(1) or ""))
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def _token_set(self, text: str) -> set[str]:
        return {
            t.lower()
            for t in re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", str(text or ""))
            if t
        }

    def _jaccard_similarity(self, a: str, b: str) -> float:
        sa = self._token_set(a)
        sb = self._token_set(b)
        if not sa or not sb:
            return 0.0
        inter = len(sa & sb)
        uni = len(sa | sb)
        return float(inter) / float(max(1, uni))

    def _alt_template_pool(self) -> list[str]:
        return [
            "Minimal diagram explaining the main troubleshooting steps.",
            "Practical workflow diagram for the section topic.",
            "Clean process diagram showing a simplified fix sequence.",
            "Concept diagram highlighting a repeatable troubleshooting pattern.",
            "Visual summary of a practical device fix routine.",
            "Diagram for a real-world implementation scenario.",
            "Simple process diagram focused on practical execution.",
            "Structured problem-solving flow diagram.",
            "Infographic-style process for operational decision clarity.",
            "Implementation-order diagram for beginner-friendly fixes.",
            "Lightweight troubleshooting flow diagram.",
            "Troubleshooting process visual for non-technical readers.",
        ]

    def _regen_alt_if_too_similar(self, alt: str, intro_text: str) -> str:
        base = re.sub(r"\s+", " ", str(alt or "")).strip()
        intro = re.sub(r"\s+", " ", str(intro_text or "")).strip()
        threshold = 0.75
        pool = self._alt_template_pool()
        random_order = list(pool)
        import random as _random
        _random.shuffle(random_order)
        candidate = base or (random_order[0] if random_order else "Troubleshooting diagram.")
        tries = 0
        while tries < 5:
            sim = self._jaccard_similarity(intro, candidate)
            if sim < threshold:
                return candidate[:180]
            candidate = random_order[tries % len(random_order)] if random_order else "Troubleshooting diagram."
            tries += 1
        return "Troubleshooting diagram."

    def _insert_banner_before_first_h2(self, html: str, block: str) -> str:
        src = str(html or "")
        m = re.search(r"<h2\b[^>]*>", src, flags=re.IGNORECASE)
        if not m:
            return block + "\n" + src
        return src[: m.start()] + block + "\n" + src[m.start() :]

    def _insert_banner_at_top(self, html: str, block: str) -> str:
        src = str(html or "").strip()
        if not src:
            return str(block or "")
        if re.search(r"^\s*<article\b[^>]*>", src, flags=re.IGNORECASE):
            m = re.search(r"^\s*<article\b[^>]*>", src, flags=re.IGNORECASE)
            if m:
                return src[: m.end()] + "\n" + block + "\n" + src[m.end() :]
        return block + "\n" + src

    def _insert_inline_between_fix2_fix3(self, html: str, block: str) -> str:
        src = str(html or "")
        fix2 = re.search(r"<h[23]\b[^>]*>\s*Fix\s*2\b.*?</h[23]>", src, flags=re.IGNORECASE | re.DOTALL)
        fix3 = re.search(r"<h[23]\b[^>]*>\s*Fix\s*3\b.*?</h[23]>", src, flags=re.IGNORECASE | re.DOTALL)
        if fix2 and fix3 and fix3.start() > fix2.end():
            return src[: fix3.start()] + block + "\n" + src[fix3.start() :]

        # Fallback: insert after the second H2 block.
        h2_iter = list(re.finditer(r"<h2\b[^>]*>", src, flags=re.IGNORECASE))
        if len(h2_iter) >= 2:
            idx = h2_iter[1].end()
            return src[:idx] + "\n" + block + src[idx:]
        return src + "\n" + block

    def _image_data_uri(self, path: Path, mime: str) -> str:
        raw = path.read_bytes()
        payload = base64.b64encode(raw).decode("ascii")
        return f"data:{mime};base64,{payload}"

    def _extract_first_img_src(self, html: str) -> str:
        m = re.search(r'<img[^>]+src="([^"]+)"', html or "", flags=re.IGNORECASE)
        if not m:
            return ""
        return str(m.group(1) or "").strip()

    def _is_blogger_media_url(self, url: str) -> bool:
        host = (urlparse(url).netloc or "").lower()
        if not host:
            return False
        allow = (
            "blogger.googleusercontent.com",
            "bp.blogspot.com",
        )
        return any(host.endswith(h) for h in allow)

    def _file_to_data_uri(self, path: Path) -> str:
        try:
            if not path.exists():
                return ""
            mime, _ = mimetypes.guess_type(path.name)
            mime = mime or "image/png"
            payload = base64.b64encode(path.read_bytes()).decode("ascii")
            return f"data:{mime};base64,{payload}"
        except Exception:
            return ""

    def _fallback_asset_data_uri(self, role: str = "thumbnail") -> str:
        rel = "assets/fallback/banner.png" if str(role or "").lower() == "thumbnail" else "assets/fallback/inline.png"
        root = Path(__file__).resolve().parent.parent
        asset = (root / rel).resolve()
        return self._file_to_data_uri(asset)

    def _image_block(self, src: str, alt: str) -> str:
        safe_alt = html_lib.unescape(alt or "article image").replace('"', "&quot;")
        return (
            '<figure class="rz-figure">'
            f'<img src="{src}" alt="{safe_alt}" loading="lazy" referrerpolicy="no-referrer" />'
            "</figure>"
        )

    def _distribute_leftovers_across_paragraphs(
        self,
        html: str,
        leftovers: list[ImageAsset],
        hosted_urls: dict[str, str],
    ) -> str:
        blocks: list[tuple[ImageAsset, str]] = []
        for image in leftovers:
            src = hosted_urls.get(str(image.path))
            if src:
                blocks.append((image, self._image_block(src, image.alt)))
        if not blocks:
            return html
        parts = re.split(r"(<p[^>]*>.*?</p>)", html, flags=re.IGNORECASE | re.DOTALL)
        paragraph_indices = [i for i, part in enumerate(parts) if part.lower().startswith("<p")]
        if not paragraph_indices:
            return html + "\n" + "\n".join(block for _, block in blocks)

        text_paragraphs = [i for i in paragraph_indices if not self._is_image_paragraph(parts[i])]
        if not text_paragraphs:
            return html + "\n" + "\n".join(block for _, block in blocks)

        reserved_positions: set[int] = set()
        placements: list[tuple[int, str]] = []
        for image, block in blocks:
            pos = self._pick_best_insert_position(
                parts=parts,
                candidate_indices=text_paragraphs,
                anchor_text=str(getattr(image, "anchor_text", "") or ""),
                reserved=reserved_positions,
            )
            if pos is None:
                continue
            reserved_positions.add(pos)
            placements.append((pos, block))

        if not placements:
            return html + "\n" + "\n".join(block for _, block in blocks)

        offsets: dict[int, list[str]] = {}
        for pos, block in placements:
            offsets.setdefault(pos, []).append(block)

        out: list[str] = []
        for i, part in enumerate(parts):
            out.append(part)
            if i in offsets:
                out.extend(offsets[i])
        return "\n".join(out)

    def _pick_best_insert_position(
        self,
        parts: list[str],
        candidate_indices: list[int],
        anchor_text: str,
        reserved: set[int],
    ) -> int | None:
        if not candidate_indices:
            return None
        anchor_tokens = self._tokenize_text(anchor_text)
        best_idx: int | None = None
        best_score = -1.0
        for idx in candidate_indices:
            if idx in reserved:
                continue
            # Avoid image-image adjacency after insertion.
            next_idx = idx + 1
            if next_idx < len(parts) and self._is_image_paragraph(parts[next_idx]):
                continue
            txt = self._paragraph_plain_text(parts[idx])
            if not txt:
                continue
            text_tokens = self._tokenize_text(txt)
            lower_txt = txt.lower()
            if anchor_tokens:
                overlap = len(anchor_tokens & text_tokens)
                union = max(1, len(anchor_tokens | text_tokens))
                context_score = overlap / union
            else:
                context_score = 0.0
            section_boost = 0.0
            if re.search(r"\b(quick take|experiment|what i learned|checklist|summary|final take)\b", lower_txt):
                section_boost = 0.25
            context_window = " ".join(
                [
                    str(parts[idx - 1] if idx - 1 >= 0 else ""),
                    str(parts[idx - 2] if idx - 2 >= 0 else ""),
                ]
            ).lower()
            if re.search(
                r"\b(experiment|what i learned|checklist|summary|result|takeaway|quick take)\b",
                context_window,
            ):
                section_boost = max(section_boost, 0.22)
            # Spread images across article body instead of clustering.
            if reserved:
                nearest = min(abs(idx - r) for r in reserved)
            else:
                nearest = 99
            spread_score = min(1.0, nearest / 8.0)
            score = (context_score * 0.70) + (spread_score * 0.15) + section_boost
            if score > best_score:
                best_score = score
                best_idx = idx
        if best_idx is not None:
            return best_idx

        # Final fallback: first available non-image paragraph.
        for idx in candidate_indices:
            if idx not in reserved:
                return idx
        return None

    def _rebalance_adjacent_image_blocks(self, html: str) -> str:
        """
        Ensure two image-only paragraphs are not placed back-to-back.
        Moves later image blocks to the next available text paragraph.
        """
        parts = re.split(
            r"(<(?:p|figure)[^>]*>.*?</(?:p|figure)>)",
            html or "",
            flags=re.IGNORECASE | re.DOTALL,
        )
        if len(parts) < 3:
            return html

        def prev_meaningful(i: int) -> int | None:
            j = i
            while j >= 0:
                if str(parts[j] or "").strip():
                    return j
                j -= 1
            return None

        def next_meaningful(i: int) -> int | None:
            j = i
            while j < len(parts):
                if str(parts[j] or "").strip():
                    return j
                j += 1
            return None

        moved = True
        guard = 0
        while moved and guard < 16:
            guard += 1
            moved = False
            i = 0
            while i < len(parts):
                if not self._is_image_paragraph(parts[i]):
                    i += 1
                    continue
                pidx = prev_meaningful(i - 1)
                if pidx is None or (not self._is_image_paragraph(parts[pidx])):
                    i += 1
                    continue

                target_insert: int | None = None
                for j in range(i + 1, len(parts)):
                    if not str(parts[j] or "").strip():
                        continue
                    if self._is_image_paragraph(parts[j]):
                        continue
                    if not str(parts[j]).lstrip().lower().startswith("<p"):
                        continue
                    nidx = next_meaningful(j + 1)
                    if nidx is not None and self._is_image_paragraph(parts[nidx]):
                        continue
                    target_insert = j + 1
                    break

                if target_insert is None:
                    i += 1
                    continue
                block = parts.pop(i)
                if target_insert > i:
                    target_insert -= 1
                parts.insert(target_insert, block)
                moved = True
            # restart scan after a move pass
        return "\n".join(parts)

    def _is_image_paragraph(self, html: str) -> bool:
        block = str(html or "")
        return bool(
            re.search(
                r"^\s*(<p[^>]*>\s*<img\b[^>]*>\s*</p>|<figure[^>]*>\s*<img\b[^>]*>.*?</figure>)\s*$",
                block,
                flags=re.IGNORECASE | re.DOTALL,
            )
        )

    def _semanticize_article_html(self, html_body: str, lede_hint: str = "") -> str:
        out = str(html_body or "").strip()
        if not out:
            return out
        if re.search(r"<article\b[^>]*class=\"[^\"]*rz-post", out, flags=re.IGNORECASE):
            return out
        # Body HTML must start from H2 in Blogger (title is H1 outside body).
        out = re.sub(r"<h1\b", "<h2", out, flags=re.IGNORECASE)
        out = re.sub(r"</h1>", "</h2>", out, flags=re.IGNORECASE)
        # Normalize related heading to a stable SEO-friendly section title.
        out = re.sub(
            r"<h3[^>]*>\s*Related Reading\s*</h3>",
            "<h2>More Fix Guides You Might Like</h2>",
            out,
            flags=re.IGNORECASE,
        )

        h2_re = re.compile(r"(<h2[^>]*>.*?</h2>)", flags=re.IGNORECASE | re.DOTALL)
        chunks = h2_re.split(out)
        preface = (chunks[0] if chunks else "").strip()
        sections: list[tuple[str, str]] = []
        for i in range(1, len(chunks), 2):
            heading = chunks[i]
            body = chunks[i + 1] if i + 1 < len(chunks) else ""
            sections.append((heading, body))

        quick_take_para = ""
        for heading, body in sections:
            heading_txt = self._paragraph_plain_text(heading)
            if re.search(r"\bquick take\b", heading_txt, flags=re.IGNORECASE):
                quick_take_para = self._first_text_paragraph(body)
                break

        article_parts: list[str] = ['<article class="rz-post">']
        if preface:
            lede = re.sub(r"\s+", " ", str(lede_hint or "")).strip()[:320]
            if not lede:
                lede = self._first_text_paragraph(preface)[:320]
            if lede and quick_take_para and self._jaccard_similarity(lede, quick_take_para) >= 0.75:
                # Policy: Option A - remove duplicated lede when it overlaps with Quick Take.
                lede = ""
            if lede:
                article_parts.append('<header class="rz-post-header">')
                article_parts.append(f'<p class="rz-lede">{html_lib.escape(lede)}</p>')
                article_parts.append("</header>")
            article_parts.append("<!-- RZ-SECTION:INTRO-START -->")
            article_parts.append('<section id="intro" class="rz-section">')
            article_parts.append(preface)
            article_parts.append("</section>")
            article_parts.append("<!-- RZ-SECTION:INTRO-END -->")

        if not sections:
            article_parts.append("<!-- RZ-SECTION:BODY-START -->")
            article_parts.append('<section id="body" class="rz-section">')
            article_parts.append(out)
            article_parts.append("</section>")
            article_parts.append("<!-- RZ-SECTION:BODY-END -->")
            article_parts.append("</article>")
            return "\n".join(article_parts)

        used_ids: set[str] = set()
        for heading, body in sections:
            title_txt = self._paragraph_plain_text(heading) or "section"
            section_id = re.sub(r"[^a-z0-9]+", "-", title_txt.lower()).strip("-")[:42] or "section"
            base_id = section_id
            suffix = 2
            while section_id in used_ids:
                section_id = f"{base_id}-{suffix}"
                suffix += 1
            used_ids.add(section_id)
            is_related = bool(re.search(r"more fix guides you might like|more experiments you might like|related posts", title_txt, flags=re.IGNORECASE))
            section_class = "rz-related" if is_related else "rz-section"
            marker = section_id.upper().replace("-", "_")
            article_parts.append(f"<!-- RZ-SECTION:{marker}-START -->")
            article_parts.append(f'<section id="{section_id}" class="{section_class}">')
            article_parts.append(heading)
            article_parts.append(body)
            article_parts.append("</section>")
            article_parts.append(f"<!-- RZ-SECTION:{marker}-END -->")

        article_parts.append("</article>")
        return "\n".join(article_parts)

    def _first_text_paragraph(self, html: str) -> str:
        src = str(html or "")
        src = re.sub(r"<(figure|img|script|style)\b[^>]*>.*?</\1>", " ", src, flags=re.IGNORECASE | re.DOTALL)
        src = re.sub(r"<img\b[^>]*>", " ", src, flags=re.IGNORECASE)
        m = re.search(r"<p[^>]*>(.*?)</p>", src, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            return ""
        txt = re.sub(r"<[^>]+>", " ", str(m.group(1) or ""))
        txt = html_lib.unescape(txt)
        return re.sub(r"\s+", " ", txt).strip()

    def _assert_html_image_integrity(
        self,
        html: str,
        min_images: int = 1,
        require_no_figcaption: bool = True,
        strict_intro_alt: bool = True,
        allow_data_uri: bool = True,
        require_blogger_hosts: bool = False,
    ) -> None:
        content = str(html or "")
        if re.search(r"(?m)^\s{0,3}#{1,6}\s+\S+", content):
            raise RuntimeError("publish failed - markdown_heading_detected")
        src_values = re.findall(r"<img\b[^>]*\bsrc=\"([^\"]+)\"", content, flags=re.IGNORECASE)
        valid_src: list[str] = []
        for src in src_values:
            s = str(src or "").strip()
            if not s:
                continue
            lower = s.lower()
            if lower.startswith("data:image/"):
                if allow_data_uri:
                    valid_src.append(s)
                continue
            if lower.startswith("https://") or lower.startswith("http://"):
                if require_blogger_hosts and (not self._is_blogger_media_url(s)):
                    raise RuntimeError(f"publish failed - non_blogger_image_host:{(urlparse(s).netloc or '').lower()}")
                valid_src.append(s)
        if len(valid_src) < max(1, int(min_images)):
            raise RuntimeError(f"publish failed - missing images ({len(valid_src)}/{int(min_images)})")
        if require_no_figcaption and re.search(r"<figcaption\b", content, flags=re.IGNORECASE):
            raise RuntimeError("publish failed - figcaption_not_allowed")
        if strict_intro_alt:
            intro = self._first_text_paragraph(content)
            alt_values = re.findall(r"<img\b[^>]*\balt=\"([^\"]*)\"", content, flags=re.IGNORECASE)
            for idx, alt in enumerate(alt_values, start=1):
                sim = self._jaccard_similarity(intro, str(alt or ""))
                if sim >= 0.75:
                    raise RuntimeError(f"publish failed - intro_alt_similarity_high(idx={idx},sim={sim:.3f})")

    def _paragraph_plain_text(self, html: str) -> str:
        text = re.sub(r"<[^>]+>", " ", str(html or ""))
        text = html_lib.unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _tokenize_text(self, text: str) -> set[str]:
        tokens = re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", str(text or "").lower())
        stop = {
            "this",
            "that",
            "with",
            "from",
            "into",
            "about",
            "have",
            "has",
            "was",
            "were",
            "will",
            "would",
            "could",
            "should",
            "your",
            "their",
            "there",
            "what",
            "when",
            "where",
            "while",
            "after",
            "before",
            "because",
            "which",
        }
        return {t for t in tokens if t not in stop}

    def _token_scopes(self, creds) -> list[str]:
        scope_raw = getattr(creds, "scopes", None)
        if isinstance(scope_raw, (list, tuple)):
            return [str(s).strip() for s in scope_raw if str(s).strip()]
        # tokeninfo fallback for runtime-confirmed scopes.
        try:
            response = requests.get(
                "https://oauth2.googleapis.com/tokeninfo",
                params={"access_token": getattr(creds, "token", "")},
                timeout=20,
            )
            if response.status_code != 200:
                return []
            data = response.json() or {}
            txt = str(data.get("scope", "")).strip()
            return [s for s in txt.split() if s]
        except Exception:
            return []

    def _upload_images_to_gcs(self, images: list[ImageAsset]) -> dict[str, str]:
        if not self.gcs_bucket_name:
            raise RuntimeError(
                "GCS 버킷이 설정되지 않았습니다. config/settings.yaml의 publish.gcs_bucket_name을 입력하세요."
            )
        if not self.service_account_path.exists():
            raise RuntimeError(
                f"GCS 서비스 계정 키 파일을 찾을 수 없습니다: {self.service_account_path}"
            )

        try:
            from google.cloud import storage  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "google-cloud-storage 패키지가 설치되지 않았습니다. requirements 설치 후 다시 시도하세요."
            ) from exc

        creds = service_account.Credentials.from_service_account_file(str(self.service_account_path))
        project = getattr(creds, "project_id", None) or None
        client = storage.Client(credentials=creds, project=project)
        bucket = client.bucket(self.gcs_bucket_name)

        hosted: dict[str, str] = {}
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        for idx, image in enumerate(images, start=1):
            path = image.path
            if not path.exists():
                continue
            mime, _ = mimetypes.guess_type(path.name)
            mime = mime or "image/png"
            safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", path.name)
            blob_name = f"rezero/{stamp}/{idx:02d}_{safe_name}"
            blob = bucket.blob(blob_name)
            try:
                blob.cache_control = "public, max-age=31536000, immutable"
                blob.upload_from_filename(str(path), content_type=mime)
                # If bucket uses object ACL, this makes object public.
                # With uniform bucket-level access, access is controlled by bucket IAM/policy.
                try:
                    blob.make_public()
                except Exception:
                    pass
                hosted[str(path)] = self._gcs_public_url(blob_name)
            except Exception:
                continue
        return hosted

    def _gcs_public_url(self, blob_name: str) -> str:
        if self.gcs_public_base_url:
            return f"{self.gcs_public_base_url}/{quote(blob_name)}"
        return f"https://storage.googleapis.com/{self.gcs_bucket_name}/{quote(blob_name)}"

    def _author_schema(self) -> str:
        # Disabled by policy:
        # - Hard QA gate forbids internal/debug-like tokens such as jobTitle/sameAs leakage.
        # - Keep publish payload minimal and deterministic.
        return ""

    def fetch_live_snapshot(
        self,
        horizon_hours: int,
        timezone_name: str = "UTC",
    ) -> dict:
        """Read real blog state from Blogger API (live today + scheduled window)."""
        creds = self._oauth_credentials()
        self._ensure_valid_token(creds)

        now_utc = datetime.now(timezone.utc)
        tzinfo = self._safe_tz(timezone_name)
        now_local = now_utc.astimezone(tzinfo)
        start_local = datetime(
            year=now_local.year,
            month=now_local.month,
            day=now_local.day,
            tzinfo=tzinfo,
        )
        end_local = start_local + timedelta(days=1)
        start_today_utc = start_local.astimezone(timezone.utc)
        end_today_utc = end_local.astimezone(timezone.utc)
        end_window_utc = now_utc + timedelta(hours=max(24, int(horizon_hours)))

        live_items = self._list_posts_by_status(
            creds=creds,
            status="live",
            start_utc=start_today_utc,
            end_utc=end_today_utc,
            max_pages=8,
        )
        scheduled_items = self._list_posts_by_status(
            creds=creds,
            status="scheduled",
            start_utc=now_utc,
            end_utc=end_window_utc,
            max_pages=8,
        )

        normalized: list[dict] = []
        for item in scheduled_items:
            publish_at = str(item.get("published", "") or "").strip()
            if not publish_at:
                continue
            normalized.append(
                {
                    "publish_at": publish_at,
                    "post_id": str(item.get("id", "") or ""),
                    "title": str(item.get("title", "") or ""),
                    "published_url": str(item.get("url", "") or ""),
                }
            )
        live_titles = [
            str(item.get("title", "") or "").strip()
            for item in live_items
            if str(item.get("title", "") or "").strip()
        ]
        live_rows: list[dict] = []
        for item in live_items:
            pid = str(item.get("id", "") or "").strip()
            ttl = str(item.get("title", "") or "").strip()
            if not pid and not ttl:
                continue
            live_rows.append(
                {
                    "post_id": pid,
                    "title": ttl,
                    "published_url": str(item.get("url", "") or "").strip(),
                }
            )

        return {
            "source": "blogger",
            "today_live_posts": int(len(live_items)),
            "today_live_titles": live_titles[:100],
            "today_live_items": live_rows[:300],
            "scheduled_in_horizon": int(len(normalized)),
            "scheduled_items": normalized,
            "window_start_utc": now_utc.isoformat(),
            "window_end_utc": end_window_utc.isoformat(),
        }

    def fetch_posts_for_export(
        self,
        statuses: list[str] | None = None,
        limit: int = 20,
        include_bodies: bool = True,
    ) -> list[BlogPostItem]:
        """Fetch real-time blog posts from Blogger for PDF export."""
        creds = self._oauth_credentials()
        self._ensure_valid_token(creds)

        valid = {"live", "scheduled", "draft"}
        requested = [s.strip().lower() for s in (statuses or ["live"]) if s]
        requested = [s for s in requested if s in valid]
        if not requested:
            requested = ["live"]

        limit = max(1, int(limit))
        per_status_max = max(limit, 30)
        per_status_max = min(per_status_max, 500)

        out: list[dict] = []
        for status in requested:
            rows = self._list_posts_by_status(
                creds=creds,
                status=status,
                start_utc=None,
                end_utc=None,
                max_pages=10,
                fetch_bodies=bool(include_bodies),
                max_results=per_status_max,
            )
            for row in rows:
                if not isinstance(row, dict):
                    continue
                copied = dict(row)
                copied["status"] = status
                out.append(copied)

        out.sort(key=self._post_sort_key, reverse=True)
        posts: list[BlogPostItem] = []
        for row in out[:limit]:
            posts.append(
                BlogPostItem(
                    post_id=str(row.get("id", "") or ""),
                    title=str(row.get("title", "") or ""),
                    url=str(row.get("url", "") or ""),
                    status=str(row.get("status", "") or ""),
                    published=str(row.get("published", "") or ""),
                    updated=str(row.get("updated", "") or ""),
                    content=str(row.get("content", "") or ""),
                )
            )
        return posts

    def fetch_status_counts(self, statuses: list[str] | None = None) -> dict[str, int]:
        """Return real-time post counts by status from Blogger API."""
        creds = self._oauth_credentials()
        self._ensure_valid_token(creds)

        valid = {"live", "scheduled", "draft"}
        requested = [s.strip().lower() for s in (statuses or ["live", "scheduled"]) if s]
        requested = [s for s in requested if s in valid]
        if not requested:
            requested = ["live", "scheduled"]

        out: dict[str, int] = {}
        for status in requested:
            rows = self._list_posts_by_status(
                creds=creds,
                status=status,
                start_utc=None,
                end_utc=None,
                max_pages=20,
                fetch_bodies=False,
                max_results=500,
            )
            out[status] = int(len(rows))
        return out

    def fetch_recent_titles(
        self,
        limit: int = 10,
        statuses: list[str] | None = None,
    ) -> list[str]:
        """Fetch recent real Blogger titles for duplicate prevention."""
        creds = self._oauth_credentials()
        self._ensure_valid_token(creds)

        valid = {"live", "scheduled", "draft"}
        requested = [s.strip().lower() for s in (statuses or ["live", "scheduled"]) if s]
        requested = [s for s in requested if s in valid]
        if not requested:
            requested = ["live", "scheduled"]

        limit = max(1, int(limit))
        per_status = max(limit, 20)
        per_status = min(per_status, 100)
        rows: list[dict] = []
        for status in requested:
            batch = self._list_posts_by_status(
                creds=creds,
                status=status,
                start_utc=None,
                end_utc=None,
                max_pages=4,
                fetch_bodies=False,
                max_results=per_status,
            )
            for item in batch:
                if isinstance(item, dict):
                    rows.append(item)

        rows.sort(key=self._post_sort_key, reverse=True)
        out: list[str] = []
        seen: set[str] = set()
        for row in rows:
            title = str(row.get("title", "") or "").strip()
            if not title:
                continue
            key = title.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(title)
            if len(out) >= limit:
                break
        return out

    def fetch_recent_live_urls(self, days: int = 14, limit: int = 260) -> list[dict]:
        """Fetch recent live post URLs within N days (for preflight indexing sync)."""
        creds = self._oauth_credentials()
        self._ensure_valid_token(creds)

        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(days=max(1, int(days)))
        max_rows = max(20, min(int(limit), 500))
        rows = self._list_posts_by_status(
            creds=creds,
            status="live",
            start_utc=start_utc,
            end_utc=now_utc,
            max_pages=8,
            fetch_bodies=False,
            max_results=max_rows,
        )
        rows.sort(key=self._post_sort_key, reverse=True)
        out: list[dict] = []
        seen: set[str] = set()
        for row in rows:
            url = self._normalize_public_url(str(row.get("url", "") or "").strip())
            if not url:
                continue
            key = url.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "url": url,
                    "title": str(row.get("title", "") or "").strip(),
                    "published": str(row.get("published", "") or "").strip(),
                }
            )
            if len(out) >= max_rows:
                break
        return out

    def inspect_url(self, site_url: str, inspection_url: str) -> dict:
        """
        Query Search Console URL Inspection API for a specific page URL.
        Requires OAuth token with Search Console scope and verified site permission.
        """
        target_site = str(site_url or "").strip()
        target_url = self._normalize_public_url(str(inspection_url or "").strip())
        if not target_site:
            raise RuntimeError("search_console_site_url_missing")
        if not target_url:
            raise RuntimeError("inspection_url_missing")

        creds = self._oauth_credentials()
        self._ensure_valid_token(creds)
        last_exc: Exception | None = None
        for site in self._site_url_candidates(target_site, target_url):
            for inspect_url in self._inspection_url_candidates(target_url):
                try:
                    response = requests.post(
                        "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect",
                        headers={
                            "Authorization": f"Bearer {creds.token}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "inspectionUrl": inspect_url,
                            "siteUrl": site,
                            "languageCode": "en-US",
                        },
                        timeout=45,
                    )
                    response.raise_for_status()
                    payload = response.json() or {}
                    return payload if isinstance(payload, dict) else {}
                except Exception as exc:
                    last_exc = exc
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("url_inspection_unknown_error")

    def _normalize_public_url(self, url: str) -> str:
        raw = str(url or "").strip()
        if not raw:
            return ""
        try:
            parsed = urlparse(raw)
            host = str(parsed.netloc or "").lower()
            if parsed.scheme == "http" and host.endswith("blogspot.com"):
                return "https://" + raw.split("://", 1)[1]
        except Exception:
            pass
        return raw

    def _site_url_candidates(self, site_url: str, inspection_url: str) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        base = str(site_url or "").strip()
        if base:
            if base.startswith("http://") or base.startswith("https://"):
                if not base.endswith("/"):
                    base += "/"
            if base not in seen:
                seen.add(base)
                out.append(base)
        host = ""
        for src in (inspection_url, site_url):
            try:
                host = str(urlparse(str(src or "")).netloc or "").strip()
            except Exception:
                host = ""
            if host:
                break
        if host:
            for cand in (f"https://{host}/", f"http://{host}/", f"sc-domain:{host}"):
                if cand not in seen:
                    seen.add(cand)
                    out.append(cand)
        return out or [base]

    def _inspection_url_candidates(self, url: str) -> list[str]:
        norm = self._normalize_public_url(url)
        out = [norm] if norm else []
        if url and url not in out:
            out.append(url)
        return out

    def inspection_verdict(self, payload: dict) -> str:
        try:
            root = dict(payload or {})
            result = dict(root.get("inspectionResult") or {})
            idx = dict(result.get("indexStatusResult") or {})
            verdict = str(idx.get("verdict", "") or "").strip()
            coverage = str(idx.get("coverageState", "") or "").strip()
            if verdict and coverage:
                return f"{verdict}|{coverage}"[:220]
            if verdict:
                return verdict[:220]
            if coverage:
                return coverage[:220]
        except Exception:
            pass
        return "unknown"

    def fetch_latest_wip_draft(
        self,
        max_age_hours: int = 168,
        include_content: bool = True,
    ) -> dict:
        """Return latest resumable WIP draft metadata/content, or {} when not found."""
        creds = self._oauth_credentials()
        self._ensure_valid_token(creds)

        rows = self._list_posts_by_status(
            creds=creds,
            status="draft",
            start_utc=None,
            end_utc=None,
            max_pages=8,
            fetch_bodies=bool(include_content),
            max_results=100,
        )
        now = datetime.now(timezone.utc)
        age_limit = now - timedelta(hours=max(1, int(max_age_hours)))
        candidates: list[tuple[datetime, dict]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title", "") or "").strip()
            labels_raw = row.get("labels", []) or []
            labels = [str(x).strip() for x in labels_raw if str(x).strip()]
            stage = self._infer_wip_stage(title=title, labels=labels)
            if not stage:
                continue
            updated_dt = self._post_sort_key(row)
            if updated_dt < age_limit:
                continue
            post_id = str(row.get("id", "") or "").strip()
            if not post_id:
                continue
            candidates.append(
                (
                    updated_dt,
                    {
                        "post_id": post_id,
                        "title": title,
                        "content": str(row.get("content", "") or "") if include_content else "",
                        "labels": labels,
                        "stage": stage,
                        "updated": updated_dt.isoformat(),
                        "url": str(row.get("url", "") or "").strip(),
                    },
                )
            )
        if not candidates:
            return {}
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    def _infer_wip_stage(self, title: str, labels: list[str]) -> str:
        for label in labels or []:
            key = str(label or "").strip().lower()
            if key == "wip":
                continue
            if key.startswith("stage-"):
                stage = key.split("stage-", 1)[1].strip()
                if stage:
                    return stage
        t = str(title or "").strip()
        m = re.match(r"\[WIP:([a-zA-Z0-9_-]+)\]\s*", t)
        if m:
            return str(m.group(1) or "").strip().lower()
        return ""

    def _list_posts_by_status(
        self,
        creds,
        status: str,
        start_utc: datetime | None,
        end_utc: datetime | None,
        max_pages: int = 6,
        fetch_bodies: bool = False,
        max_results: int = 500,
    ) -> list[dict]:
        endpoint = f"https://www.googleapis.com/blogger/v3/blogs/{self.blog_id}/posts"
        items: list[dict] = []
        page_token = None
        pages = 0
        while pages < max_pages:
            pages += 1
            params = {
                "status": status,
                "fetchBodies": "true" if fetch_bodies else "false",
                "view": "ADMIN",
                "maxResults": max(1, min(int(max_results), 500)),
            }
            if start_utc is not None:
                params["startDate"] = start_utc.isoformat()
            if end_utc is not None:
                params["endDate"] = end_utc.isoformat()
            if page_token:
                params["pageToken"] = page_token
            response = requests.get(
                endpoint,
                params=params,
                headers={"Authorization": f"Bearer {creds.token}"},
                timeout=45,
            )
            response.raise_for_status()
            payload = response.json() or {}
            page_items = payload.get("items", []) or []
            for post in page_items:
                if isinstance(post, dict):
                    items.append(post)
            page_token = payload.get("nextPageToken")
            if not page_token:
                break
        return items

    def _ensure_valid_token(self, creds) -> None:
        def _raise_scope_error(exc: Exception) -> None:
            raise RuntimeError(
                "OAuth 토큰 스코프가 현재 요청과 맞지 않습니다. 설정 > Google 로그인으로 토큰을 다시 연결해 주세요."
            ) from exc

        try:
            if getattr(creds, "expired", False) or not getattr(creds, "token", None):
                creds.refresh(Request())
        except Exception as exc:
            msg = str(exc or "").lower()
            if "invalid_scope" in msg:
                _raise_scope_error(exc)
            # Fallback to force-refresh when token state is unknown.
            try:
                creds.refresh(Request())
            except Exception as exc2:
                msg2 = str(exc2 or "").lower()
                if "invalid_scope" in msg2:
                    _raise_scope_error(exc2)
                raise

    def _safe_tz(self, timezone_name: str):
        try:
            return ZoneInfo(timezone_name)
        except Exception:
            return timezone.utc

    def _post_sort_key(self, row: dict) -> datetime:
        raw = str((row or {}).get("published") or (row or {}).get("updated") or "").strip()
        if not raw:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
