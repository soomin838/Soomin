import logging
import random
import time
import hashlib
import re
import json
from datetime import datetime, timezone
from typing import Any, Callable
from pathlib import Path

import requests

from ..visual import VisualPipeline, ImageAsset
from ..brain import DraftPost
from ..settings import AppSettings
from ..news_pack_picker import NewsPackPicker
from ..thumbnail_overlay import ThumbnailOverlayRenderer
from ..publisher import Publisher
from ..r2_uploader import R2Config, upload_file as r2_upload_file
from ..story_profile import infer_story_profile, overlay_label_for_story
import base64

logger = logging.getLogger("agent.media_manager")

class MediaManagerService:
    """
    Manages image generation, duplication checks, and uploads for both standard and news posts.
    """
    def __init__(
        self, 
        root: Path,
        settings: AppSettings,
        visual: VisualPipeline,
        news_picker: NewsPackPicker,
        overlay_renderer: ThumbnailOverlayRenderer,
        publisher: Publisher,
        news_manifest: Any
    ):
        self.root = root
        self.settings = settings
        self.visual = visual
        self.news_picker = news_picker
        self.overlay_renderer = overlay_renderer
        self.publisher = publisher
        self.news_manifest = news_manifest

    def _log_media_perf(self, event: str, payload: dict[str, Any] | None = None) -> None:
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": str(event or "").strip() or "media_event",
        }
        row.update(dict(payload or {}))
        path = self.root / "storage" / "logs" / "media_manager_perf.jsonl"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def prepare_post_images(
        self, 
        draft: DraftPost, 
        prompt_plan: dict[str, Any] | None = None,
        target_count: int = 5
    ) -> list[ImageAsset]:
        """
        Orchestrates the visual pipeline for standard posts.
        """
        logger.info(f"Preparing images for standard post: {draft.title} (target={target_count})")
        
        try:
            images = self.visual.build(draft=draft, prompt_plan=prompt_plan)
            images = self.visual.ensure_generated_thumbnail(draft=draft, images=images, prompt_plan=prompt_plan)
            
            if len(images) < target_count:
                images = self.visual.fill_missing_generated_images(
                    draft=draft, 
                    images=images, 
                    target_images=target_count
                )
            
            images = self.visual.ensure_unique_assets(images)
            if len(images) > target_count:
                images = images[:target_count]
                
            logger.info(f"Successfully prepared {len(images)} images")
            return images
        except Exception as e:
            logger.error(f"Failed to prepare standard post images: {e}", exc_info=True)
            return []

    def _news_jit_generation_enabled(self) -> bool:
        visual_provider = str(getattr(self.settings.visual, "image_provider", "") or "").strip().lower()
        visual_enable = bool(getattr(self.settings.visual, "enable_gemini_image_generation", False))
        force_jit = bool(getattr(getattr(self.settings, "news_pack", None), "force_jit_generation", False))
        if force_jit:
            return True
        return visual_enable and visual_provider not in {"", "library"}

    def _prefer_local_generated_news_visuals(self, *, draft: DraftPost, category: str, tags: list[str]) -> bool:
        profile = infer_story_profile(
            title=str(getattr(draft, "title", "") or ""),
            snippet=str(getattr(draft, "summary", "") or ""),
            category=str(category or ""),
        )
        if not profile.tech_story:
            return True
        lowered_tags = {str(x or "").strip().lower() for x in (tags or []) if str(x or "").strip()}
        return bool({"consumer_review", "air_purifier", "energy_drink", "wellness", "home"} & lowered_tags)

    def _allow_library_fallback(self) -> bool:
        return bool(getattr(self.settings.visual, "allow_library_fallback", False))

    def _news_inline_anchor_texts(self, draft: DraftPost, count: int) -> list[str]:
        paragraphs = self.visual._extract_paragraphs(str(getattr(draft, "html", "") or ""))  # noqa: SLF001
        selected = self.visual._select_target_paragraphs(paragraphs, count)  # noqa: SLF001
        anchors = [re.sub(r"\s+", " ", str(x or "").strip()) for x in (selected or []) if str(x or "").strip()]
        fallback = re.sub(r"\s+", " ", str(getattr(draft, "summary", "") or getattr(draft, "title", "") or "").strip())
        while len(anchors) < max(0, int(count)):
            if fallback:
                anchors.append(fallback[:260])
            else:
                anchors.append(str(getattr(draft, "title", "") or "article context").strip())
        return anchors[: max(0, int(count))]

    def _news_inline_prompt(self, *, story_profile: Any, title: str, anchor_text: str, index: int) -> str:
        prompt = (
            f"{story_profile.scene_hint}. Article title: {title}. "
            f"Section focus: {re.sub(r'\\s+', ' ', str(anchor_text or '').strip())[:240]}. "
            f"{self._news_inline_angle_hint(index)}. "
            "Realistic editorial support photo, natural objects, believable lighting. "
            "No readable text, no letters, no numbers, no logo, no watermark, "
            "no wireframe lines, no abstract line art, no chart, no screenshot."
        )
        return re.sub(r"\s+", " ", prompt).strip()[:900]

    def _append_news_pack_assets(
        self,
        *,
        images: list[ImageAsset],
        packed: Any,
        category: str,
        title: str,
        target_count: int,
        notes: list[str],
    ) -> list[ImageAsset]:
        need_thumb_pack = len(images) == 0
        added_inline = 0
        if need_thumb_pack and isinstance(getattr(packed, "thumb_bg", None), dict):
            thumb_row = dict(getattr(packed, "thumb_bg", None) or {})
            hook_candidates = list(thumb_row.get("hook_candidates", []) or [])
            if title:
                hook_candidates.append(str(title))
            thumb_row["hook_candidates"] = [str(x or "").strip() for x in hook_candidates if str(x or "").strip()][:4]
            thumb_asset = self._render_news_thumb_overlay(
                thumb_row=thumb_row,
                category=category,
                title=title,
            )
            if thumb_asset is None:
                thumb_asset = self._news_pack_record_to_asset(thumb_row, 0)
            if thumb_asset is not None:
                images.append(thumb_asset)
                notes.append("thumb_pack_fallback=1")
        for idx, row in enumerate(list(getattr(packed, "inline_bg", []) or []), start=1):
            asset = self._news_pack_record_to_asset(row, 1000 + idx)
            if asset is None:
                continue
            images.append(asset)
            added_inline += 1
            if len(images) >= target_count:
                break
        notes.append(f"inline_pack_fill_count={int(added_inline)}")
        return images

    def prepare_news_images(
        self,
        draft: DraftPost,
        category: str,
        tags: list[str],
        target_count: int,
        min_required: int,
        seed_tick_fn: Callable[..., Any]
    ) -> tuple[list[ImageAsset], list[str]]:
        """
        JIT Real-time image generation via Pollinations (Flux model).
        Strictly enforces 1 Hero Image per news post to avoid rate limits and improve page LCP.
        """
        started_at = time.perf_counter()
        logger.info(f"Generating JIT news hero image for: {draft.title} (category={category})")
        notes: list[str] = []
        story_profile = infer_story_profile(
            title=str(getattr(draft, "title", "") or ""),
            snippet=str(getattr(draft, "summary", "") or ""),
            category=str(category or ""),
        )
        prefer_local_generated = self._prefer_local_generated_news_visuals(draft=draft, category=category, tags=tags)

        packed = None
        if self._allow_library_fallback():
            try:
                packed = self.news_picker.pick_for_post(
                    tags=tags,
                    thumb_count=1,
                    inline_count=max(0, int(target_count) - 1),
                )
            except Exception as exc:
                notes.append(f"news_pack_prefetch_failed={str(exc)[:120]}")

        if prefer_local_generated and not self._news_jit_generation_enabled():
            notes.append(f"story_visual_mode=local_{story_profile.topic_slug}")
            images = self.visual.ensure_generated_thumbnail(draft=draft, images=[], prompt_plan=None)
            if len(images) < target_count:
                images = self.visual.fill_missing_generated_images(
                    draft=draft,
                    images=images,
                    target_images=target_count,
                    min_retry_attempts=max(8, target_count * 4),
                )
            images = self.visual.ensure_unique_assets(images)[:target_count]
            if len(images) >= max(1, int(min_required or 1)):
                self._log_media_perf(
                    "prepare_news_images_done",
                    {
                        "ok": True,
                        "images_count": int(len(images)),
                        "target_count": int(target_count),
                        "min_required": int(min_required),
                        "elapsed_ms": int(round((time.perf_counter() - started_at) * 1000)),
                        "mode": "local_story_fallback",
                        "story_topic": str(story_profile.topic_slug or ""),
                    },
                )
                return images, notes
            notes.append("local_story_visual_shortfall")

        # Define the base prompt for JIT generation
        prompt = (
            f"{story_profile.scene_hint}. Title context: {draft.title}. "
            "High quality editorial feature image."
        )
        
        providers = list(self.visual._build_generation_providers("thumbnail"))  # noqa: SLF001

        images: list[ImageAsset] = []
        if (not self._news_jit_generation_enabled()) or (not providers):
            reason = "generation_disabled_by_settings" if not self._news_jit_generation_enabled() else "no_jit_provider_available"
            notes.append(f"jit_generation_skipped={reason}")
            try:
                images = self.visual.ensure_generated_thumbnail(draft=draft, images=[], prompt_plan=None)
                if len(images) < target_count:
                    images = self.visual.fill_missing_generated_images(
                        draft=draft,
                        images=images,
                        target_images=target_count,
                        min_retry_attempts=max(8, (target_count - len(images)) * 4),
                    )
                images = self.visual.ensure_unique_assets(images)[:target_count]
                notes.append(f"visual_generated_fill_count={int(len(images))}")
            except Exception as exc:
                notes.append(f"visual_generated_fill_failed={str(exc)[:120]}")
            if (len(images) < target_count) and packed is not None:
                images = self._append_news_pack_assets(
                    images=images,
                    packed=packed,
                    category=category,
                    title=draft.title,
                    target_count=target_count,
                    notes=notes,
                )
                images = self.visual.ensure_unique_assets(images)[:target_count]
            self._log_media_perf(
                "prepare_news_images_done",
                {
                    "ok": bool(images),
                    "images_count": int(len(images)),
                    "target_count": int(target_count),
                    "min_required": int(min_required),
                    "elapsed_ms": int(round((time.perf_counter() - started_at) * 1000)),
                    "mode": "pack_first",
                },
            )
            return images, notes
        
        # Phase 1: The Hero Image (Thumbnail with Overlay)
        hero_bytes = None
        for provider in providers:
            provider_started_at = time.perf_counter()
            try:
                res = provider.generate_image(prompt=prompt, width=1280, height=720)
                hero_bytes = res.image_bytes
                notes.append(f"hero_gen_ok={provider.name}")
                self._log_media_perf(
                    "news_hero_generate",
                    {
                        "provider": str(provider.name),
                        "ok": True,
                        "elapsed_ms": int(round((time.perf_counter() - provider_started_at) * 1000)),
                    },
                )
                break
            except Exception as e:
                notes.append(f"hero_gen_failed_{provider.name}={str(e)}")
                self._log_media_perf(
                    "news_hero_generate",
                    {
                        "provider": str(provider.name),
                        "ok": False,
                        "error": str(e)[:160],
                        "elapsed_ms": int(round((time.perf_counter() - provider_started_at) * 1000)),
                    },
                )
        
        if hero_bytes:
            out_dir = self.root / "storage" / "temp_images" / "jit_generated"
            out_dir.mkdir(parents=True, exist_ok=True)
            token = hashlib.sha1(f"hero|{draft.title}|{time.time()}".encode("utf-8", errors="ignore")).hexdigest()[:12]
            hero_path = out_dir / f"hero_base_{token}.png"
            hero_path.write_bytes(hero_bytes)
            
            thumb_asset = self._render_news_thumb_overlay(
                thumb_row={"local_path": str(hero_path), "kind": "thumb_final", "hook_candidates": [draft.title]},
                category=category,
                title=draft.title
            )
            if thumb_asset:
                images.append(thumb_asset)

        # Phase 2: Supplemental Inline Images (Plain)
        remaining = target_count - len(images)
        if remaining > 0 and hero_bytes:
            logger.info(f"Attempting to generate {remaining} supplemental inline images...")
            anchors = self._news_inline_anchor_texts(draft, remaining)
            for i, anchor_text in enumerate(anchors):
                inline_bytes = None
                provider_used = ""
                inline_total_started_at = time.perf_counter()
                inline_prompt = self._news_inline_prompt(
                    story_profile=story_profile,
                    title=draft.title,
                    anchor_text=anchor_text,
                    index=i,
                )
                for provider in self.visual._build_generation_providers("content"):  # noqa: SLF001
                    provider_started_at = time.perf_counter()
                    try:
                        res = provider.generate_image(prompt=inline_prompt, width=1152, height=768)
                        inline_bytes = res.image_bytes
                        provider_used = str(getattr(provider, "name", "") or "")
                        notes.append(f"inline_{i}_gen_ok={provider.name}")
                        self._log_media_perf(
                            "news_inline_generate",
                            {
                                "index": int(i),
                                "provider": str(provider.name),
                                "ok": True,
                                "elapsed_ms": int(round((time.perf_counter() - provider_started_at) * 1000)),
                            },
                        )
                        break
                    except Exception as exc:
                        self._log_media_perf(
                            "news_inline_generate",
                            {
                                "index": int(i),
                                "provider": str(provider.name),
                                "ok": False,
                                "error": str(exc)[:160],
                                "elapsed_ms": int(round((time.perf_counter() - provider_started_at) * 1000)),
                            },
                        )
                        continue
                
                if inline_bytes:
                    out_dir = self.root / "storage" / "temp_images" / "jit_generated"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    token = hashlib.sha1(f"inline_{i}|{draft.title}|{time.time()}".encode("utf-8", errors="ignore")).hexdigest()[:12]
                    inline_path = out_dir / f"inline_{i}_{token}.png"
                    inline_path.write_bytes(inline_bytes)
                    try:
                        self.visual._optimize_image_for_seo(inline_path, role="content")  # noqa: SLF001
                    except Exception:
                        pass
                    
                    images.append(ImageAsset(
                        path=inline_path, 
                        alt=f"Editorial support image for {draft.title[:120]}",
                        anchor_text=str(anchor_text or ""),
                        source_kind=f"generated_{provider_used or 'inline'}",
                        slot_role="content",
                    ))
                self._log_media_perf(
                    "news_inline_generate_total",
                    {
                        "index": int(i),
                        "ok": bool(inline_bytes),
                        "elapsed_ms": int(round((time.perf_counter() - inline_total_started_at) * 1000)),
                    },
                )

        if len(images) < target_count and packed is not None:
            images = self._append_news_pack_assets(
                images=images,
                packed=packed,
                category=category,
                title=draft.title,
                target_count=target_count,
                notes=notes,
            )

        images = self.visual.ensure_unique_assets(images)[:target_count]

        if len(images) < target_count:
            try:
                before_fill = len(images)
                images = self.visual.fill_missing_generated_images(
                    draft=draft,
                    images=images,
                    target_images=target_count,
                    min_retry_attempts=max(8, (target_count - len(images)) * 4),
                )
                images = self.visual.ensure_unique_assets(images)[:target_count]
                notes.append(f"generated_fill_added={max(0, len(images) - before_fill)}")
            except Exception as e:
                notes.append(f"inline_local_fill_failed={str(e)[:120]}")
        
        if not images:
            notes.append("all_jit_providers_failed_completely")
            self._log_media_perf(
                "prepare_news_images_done",
                {
                    "ok": False,
                    "images_count": 0,
                    "target_count": int(target_count),
                    "elapsed_ms": int(round((time.perf_counter() - started_at) * 1000)),
                },
            )
            return [], notes
            
        logger.info(f"Successfully prepared {len(images)} JIT news images.")
        self._log_media_perf(
            "prepare_news_images_done",
            {
                "ok": True,
                "images_count": int(len(images)),
                "target_count": int(target_count),
                "min_required": int(min_required),
                "elapsed_ms": int(round((time.perf_counter() - started_at) * 1000)),
            },
        )
        return images, notes

    def mark_news_pack_used(self, images: list[ImageAsset], post_id: str) -> None:
        """
        Marks the news pack images as used in the manifest.
        """
        for image in images or []:
            if str(getattr(image, "source_kind", "") or "").strip() != "news_pack":
                continue
            src = str(getattr(image, "source_url", "") or "").strip()
            if not src:
                continue
            # Note: We assume the caller or the service has already validated the URL if needed.
            # Using publisher helper if accessible.
            if self.publisher._is_allowed_image_url(src, allow_data_uri=False):
                self.news_manifest.mark_used(r2_url=src, used_by_post_id=str(post_id or ""))



    def _news_pack_record_to_asset(self, row: dict[str, Any], index: int, materialize_remote: bool = False) -> ImageAsset | None:
        if not isinstance(row, dict):
            return None
        src_url = str(row.get("r2_url", "") or "").strip()
        
        # Using publisher's helper (passed in)
        if src_url and self.publisher._is_allowed_image_url(src_url, allow_data_uri=False):
            if materialize_remote:
                local_copy = self._download_remote_asset(src_url=src_url, index=index)
                if local_copy is not None:
                    kind = str(row.get("kind", "inline_bg") or "inline_bg").strip().lower()
                    alt = "Illustration related to the article topic" if kind == "thumb_final" else "Supporting image related to the article topic"
                    slot_role = "thumbnail" if "thumb" in kind else "content"
                    return ImageAsset(path=local_copy, alt=alt, source_kind="news_pack", source_url=src_url, slot_role=slot_role)
            kind = str(row.get("kind", "inline_bg") or "inline_bg").strip().lower()
            alt = "Illustration related to the article topic" if kind == "thumb_final" else "Supporting image related to the article topic"
            virtual = (self.root / "storage" / "temp_images" / f"news_pack_virtual_{int(index):02d}.png").resolve()
            virtual.parent.mkdir(parents=True, exist_ok=True)
            virtual.write_bytes(base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="))
            slot_role = "thumbnail" if "thumb" in kind else "content"
            return ImageAsset(path=virtual, alt=alt, source_kind="news_pack", source_url=src_url, slot_role=slot_role)
            
        local_raw = str(row.get("local_path", "") or "").strip()
        if not local_raw:
            return None
            
        local = Path(local_raw)
        if not local.is_absolute():
            local = (self.root / local_raw).resolve()
        if not local.exists():
            return None
            
        kind = str(row.get("kind", "inline_bg") or "inline_bg").strip().lower()
        alt = "Illustration related to the article topic" if kind == "thumb_final" else "Supporting image related to the article topic"
        slot_role = "thumbnail" if "thumb" in kind else "content"
        return ImageAsset(path=local, alt=alt, source_kind="news_pack", source_url=src_url, slot_role=slot_role)

    def _download_remote_asset(self, *, src_url: str, index: int) -> Path | None:
        clean = str(src_url or "").strip()
        if not clean:
            return None
        try:
            response = requests.get(clean, timeout=30, headers={"Accept": "image/*"})
            if response.status_code >= 400:
                return None
            content_type = str(response.headers.get("content-type", "") or "").lower()
            if "image/" not in content_type:
                return None
            data = bytes(response.content or b"")
            if len(data) < 2048:
                return None
            suffix = ".png" if "png" in content_type else ".jpg"
            out_path = (self.root / "storage" / "temp_images" / f"news_pack_overlay_src_{int(index):02d}{suffix}").resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(data)
            return out_path
        except Exception:
            return None

    def _render_news_thumb_overlay(
        self,
        *,
        thumb_row: dict[str, Any],
        category: str,
        title: str,
    ) -> ImageAsset | None:
        started_at = time.perf_counter()
        if not bool(getattr(self.settings.news_pack, "thumb_overlay_enabled", True)):
            return self._news_pack_record_to_asset(thumb_row, 0)
            
        base_asset = self._news_pack_record_to_asset(thumb_row, 0, materialize_remote=True)
        if not base_asset:
            return None
            
        hooks: list[str] = []
        for h in (thumb_row.get("hook_candidates", []) if isinstance(thumb_row.get("hook_candidates", []), list) else []):
            txt = re.sub(r"[^A-Za-z0-9\s]", " ", str(h or "").upper())
            txt = re.sub(r"\s+", " ", txt).strip()
            if txt and txt not in hooks:
                hooks.append(txt)
                
        hook = self._build_overlay_hook(title=title, category=category, hooks=hooks)
        out_dir = self.root / "storage" / "temp_images" / "news_thumb_final"
        out_dir.mkdir(parents=True, exist_ok=True)
        
        token = hashlib.sha1(f"{title}|{datetime.now(timezone.utc).isoformat()}".encode("utf-8", errors="ignore")).hexdigest()[:12]
        out_path = out_dir / f"thumb_final_{token}.png"
        
        rendered = self.overlay_renderer.render(
            source_path=base_asset.path,
            hook_text=hook,
            tag_label=self._display_label_for_category(category=category, title=title),
            output_path=out_path
        )
        
        if not rendered:
            self._log_media_perf(
                "news_thumb_overlay_render",
                {"ok": False, "reason": "render_failed", "elapsed_ms": int(round((time.perf_counter() - started_at) * 1000))},
            )
            return base_asset
            
        # R2 Setup and Upload
        r2_cfg = R2Config(
            endpoint_url=str(getattr(self.settings.publish.r2, "endpoint_url", "") or "").strip(),
            bucket=str(getattr(self.settings.publish.r2, "bucket", "") or "").strip(),
            access_key_id=str(getattr(self.settings.publish.r2, "access_key_id", "") or "").strip(),
            secret_access_key=str(getattr(self.settings.publish.r2, "secret_access_key", "") or "").strip(),
            public_base_url=str(getattr(self.settings.publish.r2, "public_base_url", "") or "").strip().rstrip("/"),
            prefix=str(getattr(self.settings.visual, "generated_r2_prefix", "generated") or "generated").strip() or "generated",
            cache_control=str(getattr(self.settings.publish.r2, "cache_control", "public, max-age=31536000, immutable") or "public, max-age=31536000, immutable").strip(),
        )
        
        upload_started_at = time.perf_counter()
        try:
            r2_url = r2_upload_file(
                root=self.root,
                cfg=r2_cfg,
                file_path=rendered,
                category="thumb_final",
            )
            self._log_media_perf(
                "news_thumb_overlay_upload",
                {
                    "ok": True,
                    "elapsed_ms": int(round((time.perf_counter() - upload_started_at) * 1000)),
                    "url": str(r2_url or "")[:220],
                },
            )
        except Exception as exc:
            self._log_media_perf(
                "news_thumb_overlay_upload",
                {
                    "ok": False,
                    "error": str(exc)[:160],
                    "elapsed_ms": int(round((time.perf_counter() - upload_started_at) * 1000)),
                },
            )
            return ImageAsset(
                path=rendered,
                alt="Editorial thumbnail illustration for this tech news article.",
                source_kind="generated_thumb_overlay",
                source_url="",
                license_note="NewsPack overlay thumbnail (Local Fallback)",
                slot_role="thumbnail",
            )
        
        if not self.publisher._is_allowed_image_url(str(r2_url), allow_data_uri=False):
            logger.warning(f"thumb_final_r2_host_invalid: {r2_url}")
            self._log_media_perf(
                "news_thumb_overlay_render",
                {
                    "ok": False,
                    "reason": "r2_host_invalid",
                    "elapsed_ms": int(round((time.perf_counter() - started_at) * 1000)),
                },
            )
            # Fallback to local if R2 fails validation
            return ImageAsset(
                path=rendered, 
                alt="Editorial thumbnail illustration for this tech news article.",
                source_kind="generated_thumb_overlay",
                source_url="",
                license_note="NewsPack overlay thumbnail (Local Fallback)",
                slot_role="thumbnail",
            )

        sha1 = hashlib.sha1(rendered.read_bytes()).hexdigest()
        
        # Append to manifest
        self.news_manifest.append(
            {
                "kind": "thumb_final",
                "tags": [str(category or "platform").lower()],
                "provider": "local_overlay",
                "prompt": f"overlay:{hook}",
                "prompt_hash": hashlib.sha1(f"overlay:{hook}".encode("utf-8", errors="ignore")).hexdigest(),
                "local_path": "",
                "r2_key": str(r2_url).split(r2_cfg.public_base_url.rstrip("/") + "/", 1)[-1],
                "r2_url": str(r2_url),
                "sha1": sha1,
                "width": 1280,
                "height": 720,
                "status": "ready",
                "used_at": "",
                "used_by": "",
                "used_count": 0,
                "source_mode": "thumb_overlay",
                "alt_text_template": "Editorial thumbnail illustration for this tech news article.",
                "caption_template": "",
                "overlay_hook_used": str(hook or "")[:80],
                "hook_candidates": hooks[:3],
                "style_tags": ["yt_clean", "overlay"],
            }
        )
        
        try:
            rendered.unlink(missing_ok=True)
        except Exception:
            pass
        virtual_path = (self.root / "storage" / "temp_images" / f"news_pack_virtual_thumb_{token}.png").resolve()
        virtual_path.parent.mkdir(parents=True, exist_ok=True)
        virtual_path.write_bytes(base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="))
        self._log_media_perf(
            "news_thumb_overlay_render",
            {
                "ok": True,
                "elapsed_ms": int(round((time.perf_counter() - started_at) * 1000)),
                "hook": str(hook or "")[:80],
            },
        )
            
        return ImageAsset(
            path=virtual_path,
            alt="Editorial thumbnail illustration for this tech news article.",
            anchor_text="",
            source_kind="generated_thumb_overlay",
            source_url=str(r2_url),
            license_note="NewsPack overlay thumbnail",
            slot_role="thumbnail",
        )

    def _build_overlay_hook(self, *, title: str, category: str, hooks: list[str]) -> str:
        for raw in (hooks or []):
            cleaned = self._compress_overlay_phrase(raw)
            if cleaned:
                return cleaned
        derived = self._compress_overlay_phrase(title)
        if derived:
            return derived
        return self.visual.pick_thumbnail_hook(category=category, title=title)

    def _news_inline_angle_hint(self, index: int) -> str:
        variants = [
            "wide contextual scene with infrastructure and environment detail",
            "closer operational view with human scale and tactile details",
            "alternate perspective showing workflow context and ambient motion",
            "supporting scene focused on systems, devices, or public setting context",
        ]
        return variants[max(0, int(index)) % len(variants)]

    def _compress_overlay_phrase(self, text: str) -> str:
        raw = re.sub(r"\[[^\]]*\]", " ", str(text or ""))
        raw = re.sub(r"\s+", " ", raw).strip()
        if not raw:
            return ""

        segments = [seg.strip() for seg in re.split(r"[:|\-]+", raw) if str(seg or "").strip()]
        ordered_segments = segments if segments else [raw]
        stopwords = {
            "a",
            "an",
            "and",
            "for",
            "from",
            "how",
            "impact",
            "in",
            "of",
            "on",
            "practical",
            "risks",
            "takeaways",
            "the",
            "to",
            "update",
            "users",
            "what",
            "who",
            "why",
            "with",
        }
        words: list[str] = []
        for segment in ordered_segments:
            tokens = re.findall(r"[A-Za-z0-9']+", segment)
            for token in tokens:
                lowered = token.lower()
                if lowered in stopwords:
                    continue
                if token.isdigit() and len(token) <= 2:
                    continue
                words.append(token)
            if len(words) >= 4:
                break

        if len(words) < 2:
            words = re.findall(r"[A-Za-z0-9']+", raw)

        if "ban" in raw.lower() and all(w.lower() not in {"ban", "banned"} for w in words):
            words.append("ban")

        compact: list[str] = []
        for token in words:
            if len(compact) >= 4:
                break
            if any(ch.isdigit() for ch in token):
                compact.append(token.upper())
            elif len(token) <= 3:
                compact.append(token.upper())
            else:
                compact.append(token.title())
        return " ".join(compact[:4]).strip()

    def _display_label_for_category(self, *, category: str, title: str) -> str:
        return overlay_label_for_story(title=title, category=category)
