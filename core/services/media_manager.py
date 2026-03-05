import logging
import random
import time
import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Callable
from pathlib import Path

from ..visual import VisualPipeline, ImageAsset
from ..brain import DraftPost
from ..settings import AppSettings
from ..news_pack_picker import NewsPackPicker
from ..thumbnail_overlay import ThumbnailOverlayRenderer
from ..publisher import Publisher
from ..r2_uploader import R2Config, upload_file as r2_upload_file

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
        Orchestrates image selection and processing for news posts.
        """
        logger.info(f"Preparing news images for: {draft.title} (target={target_count}, category={category})")
        
        required_inline = max(0, target_count - 1)
        
        picked_pack, emergency_notes = self._pick_news_pack_with_emergency_fill(
            tags=tags,
            required_inline=required_inline,
            target_images=target_count,
            seed_tick_fn=seed_tick_fn
        )
        
        images: list[ImageAsset] = []
        thumb_row = dict(getattr(picked_pack, "thumb_bg", {}) or {})
        inline_rows = list(getattr(picked_pack, "inline_bg", []) or [])
        
        # 1. Handle Thumbnail
        if thumb_row:
            thumb_asset = self._render_news_thumb_overlay(
                thumb_row=thumb_row,
                category=category,
                title=draft.title
            )
            if thumb_asset:
                images.append(thumb_asset)
        
        # 2. Handle Inline Images
        inline_cap = max(0, target_count - (1 if images else 0))
        for idx, row in enumerate(inline_rows[:inline_cap], start=1):
            asset = self._news_pack_record_to_asset(dict(row or {}), idx)
            if asset:
                images.append(asset)
        
        # 3. Final cleanup and validation
        if len(images) > target_count:
            images = images[:target_count]
            
        logger.info(f"Successfully prepared {len(images)} news images")
        return images, emergency_notes

    def mark_news_pack_used(self, images: list[ImageAsset], post_id: str) -> None:
        """
        Marks the news pack images as used in the manifest.
        """
        for image in images or []:
            src = str(getattr(image, "source_url", "") or "").strip()
            if not src:
                continue
            # Note: We assume the caller or the service has already validated the URL if needed.
            # Using publisher helper if accessible.
            if self.publisher._is_allowed_image_url(src, allow_data_uri=False):
                self.news_manifest.mark_used(r2_url=src, used_by_post_id=str(post_id or ""))

    def _pick_news_pack_with_emergency_fill(
        self,
        tags: list[str],
        required_inline: int,
        target_images: int,
        seed_tick_fn: Callable[..., Any]
    ) -> tuple[Any, list[str]]:
        picked = self.news_picker.pick_for_post(
            tags=tags,
            thumb_count=1,
            inline_count=required_inline,
        )
        thumb_row = dict(getattr(picked, "thumb_bg", {}) or {})
        inline_rows = list(getattr(picked, "inline_bg", []) or [])
        
        if thumb_row and len(inline_rows) >= required_inline:
            return picked, []
            
        notes: list[str] = []
        max_fill = max(1, int(getattr(self.settings.news_pack, "emergency_fill_max_items", 3) or 3))
        
        for idx in range(1, max_fill + 1):
            tick = seed_tick_fn(force=True, min_interval_sec=0)
            status = str((tick or {}).get("status", "unknown"))
            kind = str((tick or {}).get("kind", ""))
            provider = str((tick or {}).get("provider", ""))
            reason = str((tick or {}).get("reason", "")).strip().lower()
            failure_kind = str((tick or {}).get("failure_kind", "")).strip().lower()
            
            notes.append(f"emergency_fill_{idx}={status}:{kind}:{provider}" + (f":{failure_kind or reason}" if (failure_kind or reason) else ""))
            
            if failure_kind == "service_rate_limited" or "rate_limit" in reason:
                notes.append("emergency_fill_stopped=service_rate_limited")
                break
                
            picked = self.news_picker.pick_for_post(tags=tags, thumb_count=1, inline_count=required_inline)
            thumb_row = dict(getattr(picked, "thumb_bg", {}) or {})
            inline_rows = list(getattr(picked, "inline_bg", []) or [])
            
            if thumb_row and len(inline_rows) >= required_inline:
                notes.append(f"emergency_fill_recovered={(1 if thumb_row else 0) + len(inline_rows)}/{target_images}")
                return picked, notes
                
            if idx < max_fill:
                time.sleep(random.uniform(0.6, 1.4))
                
        return picked, notes

    def _news_pack_record_to_asset(self, row: dict[str, Any], index: int) -> ImageAsset | None:
        if not isinstance(row, dict):
            return None
        src_url = str(row.get("r2_url", "") or "").strip()
        
        # Using publisher's helper (passed in)
        if src_url and self.publisher._is_allowed_image_url(src_url, allow_data_uri=False):
            kind = str(row.get("kind", "inline_bg") or "inline_bg").strip().lower()
            alt = "Editorial thumbnail illustration" if kind == "thumb_final" else "Editorial illustration"
            virtual = (self.root / "storage" / "temp_images" / f"news_pack_virtual_{int(index):02d}.png").resolve()
            return ImageAsset(path=virtual, alt=alt, source_kind="news_pack", source_url=src_url)
            
        local_raw = str(row.get("local_path", "") or "").strip()
        if not local_raw:
            return None
            
        local = Path(local_raw)
        if not local.is_absolute():
            local = (self.root / local_raw).resolve()
        if not local.exists():
            return None
            
        kind = str(row.get("kind", "inline_bg") or "inline_bg").strip().lower()
        alt = "Editorial thumbnail illustration" if kind == "thumb_final" else "Editorial illustration"
        return ImageAsset(path=local, alt=alt, source_kind="news_pack", source_url=src_url)

    def _render_news_thumb_overlay(
        self,
        *,
        thumb_row: dict[str, Any],
        category: str,
        title: str,
    ) -> ImageAsset | None:
        if not bool(getattr(self.settings.news_pack, "thumb_overlay_enabled", True)):
            return self._news_pack_record_to_asset(thumb_row, 0)
            
        base_asset = self._news_pack_record_to_asset(thumb_row, 0)
        if not base_asset:
            return None
            
        hooks: list[str] = []
        for h in (thumb_row.get("hook_candidates", []) if isinstance(thumb_row.get("hook_candidates", []), list) else []):
            txt = re.sub(r"[^A-Za-z0-9\s]", " ", str(h or "").upper())
            txt = re.sub(r"\s+", " ", txt).strip()
            if txt and txt not in hooks:
                hooks.append(txt)
                
        hook = hooks[0] if hooks else self.visual.pick_thumbnail_hook(category=category, title=title)
        out_dir = self.root / "storage" / "temp_images" / "news_thumb_final"
        out_dir.mkdir(parents=True, exist_ok=True)
        
        token = hashlib.sha1(f"{title}|{datetime.now(timezone.utc).isoformat()}".encode("utf-8", errors="ignore")).hexdigest()[:12]
        out_path = out_dir / f"thumb_final_{token}.png"
        
        rendered = self.overlay_renderer.render(
            source_path=base_asset.path,
            hook_text=hook,
            tag_label=category or "tech",
            output_path=out_path
        )
        
        if not rendered:
            return base_asset
            
        # R2 Setup and Upload
        r2_cfg = R2Config(
            endpoint_url=str(getattr(self.settings.publish.r2, "endpoint_url", "") or "").strip(),
            bucket=str(getattr(self.settings.publish.r2, "bucket", "") or "").strip(),
            access_key_id=str(getattr(self.settings.publish.r2, "access_key_id", "") or "").strip(),
            secret_access_key=str(getattr(self.settings.publish.r2, "secret_access_key", "") or "").strip(),
            public_base_url=str(getattr(self.settings.publish.r2, "public_base_url", "") or "").strip().rstrip("/"),
            prefix=str(getattr(self.settings.news_pack, "r2_prefix", "news_pack") or "news_pack").strip() or "news_pack",
            cache_control=str(getattr(self.settings.publish.r2, "cache_control", "public, max-age=31536000, immutable") or "public, max-age=31536000, immutable").strip(),
        )
        
        r2_url = r2_upload_file(
            root=self.root,
            cfg=r2_cfg,
            file_path=rendered,
            category="thumb_final",
        )
        
        if not self.publisher._is_allowed_image_url(str(r2_url), allow_data_uri=False):
            logger.warning(f"thumb_final_r2_host_invalid: {r2_url}")
            # Fallback to local if R2 fails validation
            return ImageAsset(
                path=rendered, 
                alt="Editorial thumbnail illustration for this tech news article.",
                source_kind="news_pack",
                source_url="",
                license_note="NewsPack overlay thumbnail (Local Fallback)"
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
            
        return ImageAsset(
            path=(self.root / "storage" / "temp_images" / f"news_pack_virtual_thumb_{token}.png").resolve(),
            alt="Editorial thumbnail illustration for this tech news article.",
            anchor_text="",
            source_kind="news_pack",
            source_url=str(r2_url),
            license_note="NewsPack overlay thumbnail",
        )
