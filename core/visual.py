from __future__ import annotations

import base64
import hashlib
import json
import os
import random
import re
import shutil
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

import requests
import numpy as np
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps

from .brain import DraftPost
from .settings import VisualSettings

try:
    import mediapipe as mp  # type: ignore
except Exception:
    mp = None


@dataclass
class ImageAsset:
    path: Path
    alt: str
    anchor_text: str = ""
    source_kind: str = "generated"
    source_url: str = ""
    license_note: str = ""


class VisualPipeline:
    def __init__(
        self,
        temp_dir: Path,
        session_dir: Path,
        visual_settings: VisualSettings,
        gemini_api_key: str,
    ) -> None:
        self.temp_dir = temp_dir
        self.session_dir = session_dir
        self.visual_settings = visual_settings
        self.gemini_api_key = gemini_api_key
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.image_cache_dir = (self.temp_dir.parent.parent / str(getattr(self.visual_settings, "cache_dir", "storage/image_cache"))).resolve()
        self.image_cache_dir.mkdir(parents=True, exist_ok=True)
        self._visual_log_path = self.temp_dir.parent / "logs" / "visual_pipeline.jsonl"
        self._visual_log_path.parent.mkdir(parents=True, exist_ok=True)
        self._models_cache: tuple[datetime, dict[str, set[str]]] | None = None
        self._image_model_blocked_until: dict[str, datetime] = {}
        self._last_image_request_at: datetime | None = None
        self._current_run_hashes: set[str] = set()
        self._current_run_embeddings: list[np.ndarray] = []
        self._mediapipe_available = bool(mp is not None)
        self._duplicate_phash_hamming_threshold = 20
        self._duplicate_embedding_cosine_threshold = 0.993
        self._context_similarity_min_threshold = 0.11
        self._run_marker = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    def build(self, draft: DraftPost, prompt_plan: dict[str, Any] | None = None) -> list[ImageAsset]:
        self._current_run_hashes = set()
        self._current_run_embeddings = []
        provider = str(getattr(self.visual_settings, "image_provider", "pollinations") or "pollinations").strip().lower()
        target = max(1, int(self.visual_settings.target_images_per_post))
        body_slots = max(0, target - 1)
        self._log_visual_event(
            {
                "event": "visual_build_start",
                "provider": provider,
                "target_images_per_post": int(target),
                "target_banner": 1,
                "target_inline": int(body_slots),
            }
        )
        if provider in {"pollinations", "pollination"}:
            # Pollinations API-only mode: key is optional depending on provider plan.
            # Never hard-fail image generation solely due to empty key.
            pass
        # Generated-only policy:
        # - Total images per post = target_images_per_post
        # - 1 thumbnail is created in ensure_generated_thumbnail()
        # - Remaining slots are content images from contextual prompts
        paragraphs = self._extract_paragraphs(draft.html)
        selected_paragraphs = self._select_target_paragraphs(paragraphs, body_slots)

        # API usage optimization: prompt generation is local keyword-based (no Gemini call).
        prompts = self._generate_prompts_with_gemini(selected_paragraphs, draft.title)
        inline_plan_prompt = re.sub(
            r"\s+",
            " ",
            str((prompt_plan or {}).get("inline_prompt", "") or "").strip(),
        )
        self._log_visual_event(
            {
                "event": "visual_prompt_plan",
                "source": str((prompt_plan or {}).get("source", "fallback") or "fallback"),
                "banner_prompt_len": len(str((prompt_plan or {}).get("banner_prompt", "") or "")),
                "inline_prompt_len": len(inline_plan_prompt),
            }
        )
        if inline_plan_prompt:
            if prompts:
                prompts[0] = inline_plan_prompt
            else:
                prompts = [inline_plan_prompt]
        assets: list[ImageAsset] = []

        for i, paragraph in enumerate(selected_paragraphs):
            prompt = prompts[i] if i < len(prompts) else self._fallback_prompt(paragraph, draft.title)
            generated = self._generate_image_with_pollinations(
                prompt,
                i + 1,
                paragraph,
                draft.title,
                role="content",
            )
            if generated is not None:
                assets.append(generated)
            if len(assets) >= body_slots:
                break

        # Fill remaining body slots with contextual retry prompts.
        no_progress_count = 0
        seed_contexts = selected_paragraphs if selected_paragraphs else [draft.summary or draft.title]
        cursor = 0
        while len(assets) < body_slots:
            idx = len(assets) + 1
            context = seed_contexts[cursor % len(seed_contexts)] if seed_contexts else (draft.summary or draft.title)
            cursor += 1
            prompt = self._fallback_prompt(context or draft.summary, draft.title)
            generated = self._generate_image_with_pollinations(
                prompt,
                idx,
                context or "",
                draft.title,
                role="content",
            )
            if generated is not None:
                assets.append(generated)
                no_progress_count = 0
                continue

            no_progress_count += 1
            if no_progress_count >= 3:
                break

        assets = self.ensure_unique_assets(assets)
        valid_assets: list[ImageAsset] = []
        for asset in assets:
            if self._optimize_image_for_seo(asset.path, role="content"):
                valid_assets.append(asset)
            else:
                self._log_visual_event(
                    {
                        "event": "image_asset_dropped",
                        "reason": "missing_or_optimize_failed",
                        "path": str(getattr(asset, "path", "")),
                        "role": "content",
                    }
                )
        result = valid_assets[:body_slots]
        reasons: list[str] = []
        if len(result) < body_slots:
            reasons.append(f"inline_shortfall({len(result)}/{body_slots})")
        self._log_visual_event(
            {
                "event": "visual_build_result",
                "generated_count": len(result),
                "paths": [str(getattr(x, "path", "")) for x in result[:3]],
                "reasons_if_missing": reasons,
            }
        )
        return result

    def force_screenshots(self, urls: list[str], title: str) -> list[ImageAsset]:
        # Spec policy: screenshot collection disabled.
        return []

    def ensure_generated_thumbnail(
        self,
        draft: DraftPost,
        images: list[ImageAsset],
        prompt_plan: dict[str, Any] | None = None,
    ) -> list[ImageAsset]:
        if not images:
            images = []
        # If there is already a generated thumbnail candidate, move it to index 0.
        for idx, asset in enumerate(images):
            kind = (getattr(asset, "source_kind", "") or "").strip().lower()
            if kind in {"gemini", "generated", "pollinations"} and Path(getattr(asset, "path", "")).exists():
                if idx != 0:
                    images.insert(0, images.pop(idx))
                self._optimize_image_for_seo(images[0].path, role="thumbnail")
                return images

        # Generate a dedicated thumbnail when screenshot-only set is returned.
        prompt = re.sub(
            r"\s+",
            " ",
            str((prompt_plan or {}).get("banner_prompt", "") or "").strip(),
        )
        if not prompt:
            prompt = self._build_thumbnail_prompt(draft)
        generated = self._generate_image_with_pollinations(
            prompt=prompt,
            index=0,
            paragraph=(draft.summary or draft.title),
            keyword=draft.title,
            role="thumbnail",
        )
        if generated is None:
            generated = self._fallback_asset_for_role(role="thumbnail", index=0)
        if generated is None:
            raise RuntimeError("Generated thumbnail creation failed. retry required")
        generated.anchor_text = ""
        generated.alt = self._build_alt_text(draft.title, "thumbnail")
        if not self._optimize_image_for_seo(generated.path, role="thumbnail"):
            raise RuntimeError("Generated thumbnail file missing after creation. retry required")
        images.insert(0, generated)
        return images

    def ensure_unique_assets(self, images: list[ImageAsset]) -> list[ImageAsset]:
        return self._dedupe_assets_by_content(images)

    def fill_missing_generated_images(
        self,
        draft: DraftPost,
        images: list[ImageAsset],
        target_images: int,
        min_retry_attempts: int = 5,
    ) -> list[ImageAsset]:
        """
        Top-up only missing image slots.
        Example: if target=5 and current=4, generate only 1 additional body image.
        """
        current = self._dedupe_assets_by_content(list(images or []))
        target = max(1, int(target_images or 0))
        if len(current) >= target:
            return current[:target]

        missing = target - len(current)
        paragraphs = self._extract_paragraphs(draft.html)
        seed_contexts = paragraphs if paragraphs else [draft.summary or draft.title]
        if not seed_contexts:
            seed_contexts = [draft.title or "device troubleshooting"]

        cursor = 0
        no_progress = 0
        # Use high index offset to avoid filename collisions with existing generated_XX files.
        file_index_seed = 100 + len(current)

        retry_budget = max(int(min_retry_attempts or 5), missing * 6)
        attempts = 0
        while missing > 0 and attempts < retry_budget:
            attempts += 1
            context = seed_contexts[cursor % len(seed_contexts)] if seed_contexts else (draft.summary or draft.title)
            cursor += 1
            prompt = self._fallback_prompt(context or draft.summary, draft.title)
            generated = self._generate_image_with_pollinations(
                prompt=prompt,
                index=file_index_seed,
                paragraph=context or "",
                keyword=draft.title,
                role="content",
            )
            file_index_seed += 1
            if generated is None:
                no_progress += 1
                if no_progress >= 4:
                    no_progress = 0
                continue
            cand_hash = self._file_sha1(generated.path)
            if cand_hash and any(cand_hash == self._file_sha1(img.path) for img in current):
                try:
                    generated.path.unlink(missing_ok=True)
                except Exception:
                    pass
                continue
            if not self._optimize_image_for_seo(generated.path, role="content"):
                continue
            current.append(generated)
            missing -= 1
            no_progress = 0

        return self._dedupe_assets_by_content(current)[:target]

    def _file_sha1(self, path: Path) -> str:
        try:
            if not path.exists() or not path.is_file():
                return ""
            h = hashlib.sha1()
            with path.open("rb") as fh:
                for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                    if not chunk:
                        break
                    h.update(chunk)
            return h.hexdigest()
        except Exception:
            return ""

    def _dedupe_assets_by_content(self, images: list[ImageAsset]) -> list[ImageAsset]:
        out: list[ImageAsset] = []
        seen_hashes: set[str] = set()
        for img in (images or []):
            try:
                p = Path(getattr(img, "path", ""))
            except Exception:
                p = Path("")
            if not str(p) or (not p.exists()) or (not p.is_file()):
                continue
            digest = self._file_sha1(p)
            if digest and digest in seen_hashes:
                continue
            if digest:
                seen_hashes.add(digest)
            out.append(img)
        return out

    def _extract_paragraphs(self, html: str) -> list[str]:
        chunks = re.findall(r"<p[^>]*>(.*?)</p>", html, flags=re.IGNORECASE | re.DOTALL)
        paragraphs: list[str] = []
        for c in chunks:
            text = re.sub(r"<[^>]+>", " ", c)
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) >= 60:
                paragraphs.append(text)
        return paragraphs

    def _select_target_paragraphs(self, paragraphs: list[str], target: int) -> list[str]:
        if target <= 0 or not paragraphs:
            return []

        cleaned: list[str] = []
        seen_text: set[str] = set()
        for p in paragraphs:
            t = re.sub(r"\s+", " ", str(p or "").strip())
            if not t or t in seen_text:
                continue
            seen_text.add(t)
            cleaned.append(t)
        if len(cleaned) <= target:
            return cleaned

        picked_idx: list[int] = []
        picked_set: set[int] = set()

        def pick_index(i: int) -> None:
            if i < 0 or i >= len(cleaned) or i in picked_set:
                return
            picked_set.add(i)
            picked_idx.append(i)

        def _has_spacing(i: int, min_gap: int = 2) -> bool:
            if not picked_idx:
                return True
            return all(abs(i - p) >= min_gap for p in picked_idx)

        # 2 fixed "suitable" positions: early and middle section.
        early_idx = min(len(cleaned) - 1, max(1, len(cleaned) // 4))
        middle_idx = min(len(cleaned) - 1, max(early_idx + 1, (len(cleaned) * 3) // 5))
        pick_index(early_idx)
        if len(picked_idx) < target:
            pick_index(middle_idx)

        # Remaining positions: contextual relevance score.
        scored: list[tuple[int, int]] = []
        for i, p in enumerate(cleaned):
            if i in picked_set:
                continue
            score = len(p)
            if self._needs_screenshot(p):
                score += 35
            if re.search(r"\b(step|checklist|tip|example|result|before|after)\b", p.lower()):
                score += 12
            scored.append((score, i))
        scored.sort(key=lambda x: x[0], reverse=True)
        for _, i in scored:
            if len(picked_idx) >= target:
                break
            # Prefer non-adjacent insertion points to avoid image clustering.
            if _has_spacing(i, min_gap=2):
                pick_index(i)

        # If still short, relax spacing constraint as a fallback.
        if len(picked_idx) < target:
            for _, i in scored:
                if len(picked_idx) >= target:
                    break
                pick_index(i)

        # Preserve natural reading order for insertion flow.
        picked_idx.sort()
        return [cleaned[i] for i in picked_idx[:target]]

    def _needs_screenshot(self, paragraph: str) -> bool:
        # Spec policy: no screenshot capture path.
        return False

    def _capture_screenshots(self, urls: list[str], keyword: str) -> list[ImageAsset]:
        self._log_visual_event(
            {
                "event": "screenshot_path_blocked",
                "reason": "policy_disabled",
                "url_count": len(urls or []),
            }
        )
        return []

    def _prepare_english_urls(self, urls: list[str]) -> list[str]:
        out: list[str] = []
        seen = set()
        for u in urls:
            normalized = self._force_english_url(str(u or "").strip())
            if not normalized:
                continue
            if not self._is_capture_allowed(normalized):
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
        return out[:6]

    def _is_capture_allowed(self, url: str) -> bool:
        try:
            host = (urlparse(url).netloc or "").lower()
        except Exception:
            return False
        disallow_hosts = (
            "reddit.com",
            "x.com",
            "twitter.com",
            "facebook.com",
            "instagram.com",
            "tiktok.com",
            "pinterest.com",
            "naver.com",
            "daum.net",
        )
        return not any(h in host for h in disallow_hosts)

    def _force_english_url(self, url: str) -> str | None:
        if not url:
            return None
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return None

        host = parsed.netloc.lower()
        path = parsed.path.lower()
        if host.endswith(".kr") or "/ko/" in path or path.startswith("/ko"):
            return None

        params = dict(parse_qsl(parsed.query, keep_blank_values=True))
        for k in ["hl", "lang", "locale"]:
            if params.get(k, "").lower().startswith("ko"):
                params[k] = "en"
        params.setdefault("hl", "en")
        params.setdefault("lang", "en")
        query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=query))

    def _detect_focus_bbox(self, page) -> tuple[int, int, int, int] | None:
        js = """
() => {
  const candidates = [
    "main",
    "article",
    "[role='main']",
    ".markdown-body",
    ".content",
    ".container",
    "#content"
  ];
  for (const sel of candidates) {
    const el = document.querySelector(sel);
    if (!el) continue;
    const r = el.getBoundingClientRect();
    if (r.width > 420 && r.height > 180) {
      return {x: Math.max(0, r.x), y: Math.max(0, r.y), w: r.width, h: r.height};
    }
  }
  return null;
}
"""
        try:
            rect = page.evaluate(js)
            if not rect:
                return None
            x = int(rect.get("x", 0))
            y = int(rect.get("y", 0))
            w = int(rect.get("w", 0))
            h = int(rect.get("h", 0))
            if w <= 0 or h <= 0:
                return None
            return (x, y, w, h)
        except Exception:
            return None

    def _build_editorial_screenshot(
        self,
        raw_path: Path,
        out_path: Path,
        bbox: tuple[int, int, int, int] | None,
        highlight_boxes: list[tuple[int, int, int, int]] | None = None,
    ) -> None:
        with Image.open(raw_path).convert("RGBA") as im:
            if bbox is None:
                im.convert("RGB").save(out_path)
                return

            x, y, w, h = bbox
            # Full-page context first, then highlight the important zone.
            # 50% dim outside the focus area for clear visual hierarchy.
            overlay = Image.new("RGBA", im.size, (0, 0, 0, 128))
            draw = ImageDraw.Draw(overlay)
            focus = self._expanded_crop_box(im.width, im.height, x, y, w, h)
            draw.rectangle(focus, fill=(0, 0, 0, 0))
            draw.rectangle(focus, outline=(55, 120, 255, 215), width=6)
            for rect in (highlight_boxes or [])[:6]:
                hx, hy, hw, hh = rect
                if hw <= 0 or hh <= 0:
                    continue
                left = max(0, hx - 4)
                top = max(0, hy - 4)
                right = min(im.width, hx + hw + 4)
                bottom = min(im.height, hy + hh + 4)
                draw.rectangle([left, top, right, bottom], outline=(255, 194, 52, 235), width=4)
                draw.rectangle([left, top, right, bottom], fill=(255, 194, 52, 28))
            styled = Image.alpha_composite(im, overlay)
            styled.convert("RGB").save(out_path)

    def _detect_highlight_boxes(self, page) -> list[tuple[int, int, int, int]]:
        js = """
() => {
  const seen = new Set();
  const clampRect = (r) => ({
    x: Math.max(0, Math.round(r.x)),
    y: Math.max(0, Math.round(r.y)),
    w: Math.max(0, Math.round(r.width)),
    h: Math.max(0, Math.round(r.height)),
  });
  const uniqPush = (arr, r) => {
    const key = [r.x, r.y, r.w, r.h].join(':');
    if (seen.has(key)) return;
    seen.add(key);
    arr.push(r);
  };
  const accept = (r) => (r.w >= 60 && r.h >= 20 && r.w <= 900 && r.h <= 220);

  const price = [];
  const cta = [];
  const header = [];

  const all = Array.from(document.querySelectorAll('body *'));
  for (const el of all) {
    if (!el || !el.getBoundingClientRect) continue;
    const txt = (el.innerText || el.textContent || '').trim();
    if (!txt) continue;
    const rect = clampRect(el.getBoundingClientRect());
    if (!accept(rect)) continue;

    if (/(\\$\\s?\\d|usd|price|pricing|per month|\\/mo)/i.test(txt)) {
      uniqPush(price, rect);
      if (price.length >= 2) break;
    }
  }

  const ctaNodes = document.querySelectorAll('button,[role=\"button\"],a[href],input[type=\"button\"],input[type=\"submit\"]');
  for (const el of ctaNodes) {
    if (!el || !el.getBoundingClientRect) continue;
    const txt = (el.innerText || el.textContent || el.value || '').trim();
    const rect = clampRect(el.getBoundingClientRect());
    if (!accept(rect)) continue;
    if (/(start|get started|try|download|install|learn more|sign up|buy|continue|submit|open)/i.test(txt) || txt.length > 0) {
      uniqPush(cta, rect);
      if (cta.length >= 2) break;
    }
  }

  const headNodes = document.querySelectorAll('h1,h2,header,[role=\"heading\"]');
  for (const el of headNodes) {
    if (!el || !el.getBoundingClientRect) continue;
    const rect = clampRect(el.getBoundingClientRect());
    if (!accept(rect)) continue;
    uniqPush(header, rect);
    if (header.length >= 2) break;
  }

  const ordered = [...price, ...cta, ...header];
  return ordered.slice(0, 3);
}
"""
        try:
            rows = page.evaluate(js) or []
            out: list[tuple[int, int, int, int]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                x = int(row.get("x", 0))
                y = int(row.get("y", 0))
                w = int(row.get("w", 0))
                h = int(row.get("h", 0))
                if w <= 0 or h <= 0:
                    continue
                out.append((x, y, w, h))
            return out
        except Exception:
            return []

    def _expanded_crop_box(
        self,
        img_w: int,
        img_h: int,
        x: int,
        y: int,
        w: int,
        h: int,
    ) -> tuple[int, int, int, int]:
        pad_w = int(w * 0.2)
        pad_h = int(h * 0.25)
        left = max(0, x - pad_w)
        top = max(0, y - pad_h)
        right = min(img_w, x + w + pad_w)
        bottom = min(img_h, y + h + pad_h)

        # Keep a stable 16:9 frame.
        target_ratio = 16.0 / 9.0
        cur_w = max(1, right - left)
        cur_h = max(1, bottom - top)
        cur_ratio = cur_w / cur_h
        if cur_ratio > target_ratio:
            new_h = int(cur_w / target_ratio)
            delta = max(0, new_h - cur_h)
            top = max(0, top - delta // 2)
            bottom = min(img_h, bottom + delta - delta // 2)
        else:
            new_w = int(cur_h * target_ratio)
            delta = max(0, new_w - cur_w)
            left = max(0, left - delta // 2)
            right = min(img_w, right + delta - delta // 2)

        return (left, top, right, bottom)

    def _find_local_chrome_like(self) -> list[str]:
        local_app = os.environ.get("LOCALAPPDATA", "")
        explicit = os.environ.get("CHROME_PATH", "")
        which_chrome = shutil.which("chrome")
        which_edge = shutil.which("msedge")
        path_env_hits = [p for p in os.environ.get("PATH", "").split(";") if p]
        candidates = [
            explicit,
            which_chrome or "",
            which_edge or "",
            os.path.join(local_app, "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(local_app, "Microsoft", "Edge", "Application", "msedge.exe"),
            os.path.join(os.environ.get("PROGRAMFILES", r"C:\Program Files"), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.environ.get("PROGRAMFILES", r"C:\Program Files"), "Microsoft", "Edge", "Application", "msedge.exe"),
            os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"), "Microsoft", "Edge", "Application", "msedge.exe"),
        ]
        for p in path_env_hits:
            candidates.append(os.path.join(p, "chrome.exe"))
            candidates.append(os.path.join(p, "msedge.exe"))

        unique: list[str] = []
        seen = set()
        for path in candidates:
            norm = str(path or "").strip().strip('"')
            if not norm or norm in seen:
                continue
            seen.add(norm)
            if Path(norm).exists():
                unique.append(norm)
        return unique

    def _launch_browser_context(self, playwright):
        launch_kwargs = {
            "headless": True,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        context_kwargs = {
            "viewport": {"width": 1280, "height": 720},
            "locale": "en-US",
            "timezone_id": "America/Los_Angeles",
            "extra_http_headers": {"Accept-Language": "en-US,en;q=0.9"},
        }

        attempts: list[str] = []

        # 1) Explicit executable candidates.
        for exe in self._find_local_chrome_like():
            try:
                browser = playwright.chromium.launch(**launch_kwargs, executable_path=exe)
                ctx = browser.new_context(**context_kwargs)
                return browser, ctx, f"executable_path:{exe}"
            except Exception as exc:
                attempts.append(f"exe:{exe} -> {type(exc).__name__}")

        # 2) Playwright channel launch for installed Chrome/Edge.
        for channel in ["chrome", "msedge"]:
            try:
                browser = playwright.chromium.launch(**launch_kwargs, channel=channel)
                ctx = browser.new_context(**context_kwargs)
                return browser, ctx, f"channel:{channel}"
            except Exception as exc:
                attempts.append(f"channel:{channel} -> {type(exc).__name__}")

        # 3) Final fallback to playwright-managed chromium.
        try:
            browser = playwright.chromium.launch(**launch_kwargs)
            ctx = browser.new_context(**context_kwargs)
            return browser, ctx, "bundled:chromium"
        except Exception as exc:
            attempts.append(f"bundled:chromium -> {type(exc).__name__}")

        return None, None, " | ".join(attempts[:8])

    def _is_english_page(self, page) -> bool:
        try:
            payload = page.evaluate(
                """() => {
                    const lang = (document.documentElement && document.documentElement.lang) || '';
                    const txt = (document.body && document.body.innerText) ? document.body.innerText.slice(0, 20000) : '';
                    return { lang, txt };
                }"""
            )
            lang = str((payload or {}).get("lang", "")).lower()
            txt = str((payload or {}).get("txt", ""))
            if lang and not lang.startswith("en"):
                return False
            if not txt:
                return True
            hangul = len(re.findall(r"[ㄱ-ㅎㅏ-ㅣ가-힣]", txt))
            latin = len(re.findall(r"[A-Za-z]", txt))
            # Strict rule requested by user: no Korean text allowed in screenshot pages.
            if hangul > 0:
                return False
            if latin < 80:
                return False
            return True
        except Exception:
            return False

    def _generate_prompts_with_gemini(self, paragraphs: list[str], title: str) -> list[str]:
        if not paragraphs:
            return []
        self._log_visual_event(
            {
                "event": "prompt_generation_local_only",
                "reason": "gemini_disabled_for_image_prompts",
                "count": int(len(paragraphs)),
            }
        )
        return [
            self._build_local_keyword_prompt(
                paragraph=p,
                title=title,
                variation_index=i + 1,
                role="section",
            )
            for i, p in enumerate(paragraphs)
        ]

    def _build_local_keyword_prompt(
        self,
        paragraph: str,
        title: str,
        variation_index: int,
        role: str = "section",
    ) -> str:
        keywords = self._extract_local_visual_keywords(paragraph, title, max_keywords=6)
        keyword_block = ", ".join(keywords) if keywords else "device, troubleshooting, fix, reliability"
        context_hint = self._context_snippet(paragraph)[:260]
        base = (
            "Create one realistic photo-style image based on this article context. "
            f"Keywords: {keyword_block}. "
            "Keep it natural and specific to the context. "
            "No text, no letters, no logo, no watermark."
        )
        if context_hint:
            base += f" Context hint: {context_hint}."
        return re.sub(r"\s+", " ", base).strip()[:900]

    def _extract_local_visual_keywords(self, paragraph: str, title: str, max_keywords: int = 6) -> list[str]:
        stop = {
            "this",
            "that",
            "with",
            "from",
            "about",
            "into",
            "under",
            "over",
            "after",
            "before",
            "while",
            "where",
            "when",
            "what",
            "which",
            "would",
            "could",
            "should",
            "there",
            "their",
            "they",
            "have",
            "has",
            "had",
            "were",
            "was",
            "been",
            "being",
            "your",
            "ours",
            "team",
            "teams",
            "guide",
            "today",
            "why",
            "how",
        }
        source = f"{title or ''} {paragraph or ''}".lower()
        tokens = re.findall(r"[a-z][a-z0-9-]{2,}", source)
        counts = Counter(t for t in tokens if t not in stop and not t.isdigit())
        if not counts:
            return []
        ranked = sorted(counts.items(), key=lambda x: (-x[1], -len(x[0]), x[0]))
        out: list[str] = []
        for token, _ in ranked:
            if token in out:
                continue
            out.append(token)
            if len(out) >= max_keywords:
                break
        return out

    def _generate_image_with_gemini(
        self,
        prompt: str,
        index: int,
        paragraph: str,
        keyword: str,
        role: str = "content",
    ) -> ImageAsset | None:
        # 정책: 이미지 생성은 Pollinations(gptimage) API-only.
        # 로컬/타 모델 폴백 없이 Pollinations 경로로만 처리한다.
        prompt = self._enforce_no_text_rule(prompt)
        provider = str(getattr(self.visual_settings, "image_provider", "pollinations") or "pollinations").strip().lower()
        if provider not in {"pollinations", "pollination"}:
            self._log_visual_event(
                {
                    "event": "image_provider_forced",
                    "index": index,
                    "requested_provider": provider,
                    "forced_provider": "pollinations",
                }
            )
        return self._generate_image_with_pollinations(prompt, index, paragraph, keyword, role=role)

    def _generate_image_with_pollinations(
        self,
        prompt: str,
        index: int,
        paragraph: str,
        keyword: str,
        role: str = "content",
    ) -> ImageAsset | None:
        if not bool(getattr(self.visual_settings, "pollinations_enabled", True)):
            self._log_visual_event(
                {
                    "event": "pollinations_failed",
                    "reason": "feature_disabled",
                    "index": index,
                    "status": "disabled",
                }
            )
            return self._fallback_asset_for_role(role=role, index=index)

        base_url = str(getattr(self.visual_settings, "pollinations_base_url", "") or "").strip().rstrip("/")
        if not base_url:
            self._log_visual_event(
                {
                    "event": "pollinations_failed",
                    "reason": "missing_base_url",
                    "index": index,
                    "status": "missing_base_url",
                }
            )
            return self._fallback_asset_for_role(role=role, index=index)

        model = self._resolve_pollinations_model(role=role)
        role_key = str(role or "").strip().lower()
        is_thumbnail = role_key == "thumbnail"
        # Keep prompt policy simple and context-first.
        prompt_text = re.sub(r"\s+", " ", str(prompt or "")).strip()
        if not prompt_text:
            prompt_text = self._fallback_prompt(paragraph, keyword, variation_index=index, role=role)
        prompt_text = self._enforce_no_text_rule(prompt_text)
        width, height = (1280, 720) if str(role or "").lower() == "thumbnail" else (1152, 648)
        # Stable cache key policy: sha1(model + size + prompt)
        prompt_suffix = re.sub(r"\s+", " ", str(getattr(self.visual_settings, "prompt_suffix", "") or "").strip())
        cache_key = hashlib.sha1(
            f"{model}|{width}x{height}|{prompt_text}|{prompt_suffix}".encode("utf-8", errors="ignore")
        ).hexdigest()
        cache_path = self.image_cache_dir / f"{cache_key}.png"
        if cache_path.exists():
            try:
                if cache_path.stat().st_size < 5 * 1024:
                    cache_path.unlink(missing_ok=True)
                else:
                    cached_out = self.temp_dir / f"generated_{index:02d}_cache_{cache_key[:10]}.png"
                    shutil.copy2(cache_path, cached_out)
                    self._log_visual_event(
                        {
                            "event": "pollinations_image_cache_hit",
                            "index": index,
                            "model": model,
                            "role": role_key,
                            "cache_path": str(cache_path),
                        }
                    )
                    return ImageAsset(
                        path=cached_out,
                        alt=self._build_alt_text(keyword, "cached generated image"),
                        anchor_text=paragraph,
                        source_kind="pollinations",
                        source_url=f"pollinations://{model}",
                        license_note="Generated by Pollinations.ai (cached).",
                    )
            except Exception:
                pass
        base_params = {
            "model": model,
            "width": str(width),
            "height": str(height),
            "safe": "true",
            "enhance": "true",
            "nologo": "true",
            "seed": str(random.randint(1, 2_000_000_000)),
        }

        key = str(getattr(self.visual_settings, "pollinations_api_key", "") or "").strip()
        if key and key.upper() in {"POLLINATIONS_API_KEY", "YOUR_POLLINATIONS_API_KEY"}:
            key = ""
        primary_auth: tuple[str, dict[str, str], dict[str, str]] = ("anonymous", {}, {})
        auth_modes: list[tuple[str, dict[str, str], dict[str, str]]] = [primary_auth]
        if key:
            auth_modes.append(("key_query", {"key": key}, {}))
            auth_modes.append(("key_bearer", {}, {"Authorization": f"Bearer {key}"}))
        best_candidate_path: Path | None = None
        best_candidate_score = -1.0
        best_candidate_reason = ""
        best_candidate_metrics: dict[str, float | int | str | bool] = {}

        # Policy: deterministic three-stage retry.
        # 1) original prompt
        # 2) same prompt + different seed
        # 3) simplified prompt + different seed
        max_generation_attempts = 3
        for gen_try in range(1, max_generation_attempts + 1):
            # Throttle per generation attempt (not per auth mode).
            self._respect_image_interval()
            params_seed = dict(base_params)
            params_seed["seed"] = str(random.randint(1, 2_000_000_000))
            attempt_prompt = prompt_text
            prompt_mode = "original"
            if gen_try == 2:
                prompt_mode = "seed_refresh"
            elif gen_try >= 3:
                attempt_prompt = self._enforce_no_text_rule(
                    self._simplify_prompt_for_retry(prompt_text, retry_index=gen_try, role=role_key)
                )
                prompt_mode = "simplified"
            encoded_prompt = quote(attempt_prompt, safe="")
            endpoint = f"{base_url}/image/{encoded_prompt}"
            mode_queue: list[tuple[str, dict[str, str], dict[str, str]]] = list(auth_modes)
            for mode, extra_params, extra_headers in mode_queue:
                self._log_visual_event(
                    {
                        "event": "pollinations_request",
                        "index": index,
                        "model": model,
                        "role": role_key,
                        "width": width,
                        "height": height,
                        "prompt_hash": cache_key[:16],
                        "attempt": gen_try,
                        "auth_mode": mode,
                        "status": "requesting",
                    }
                )
                try:
                    params = dict(params_seed)
                    params.update(extra_params)
                    headers = {"Accept": "image/*"}
                    headers.update(extra_headers)
                    response = requests.get(
                        endpoint,
                        params=params,
                        headers=headers,
                        timeout=max(5, int(getattr(self.visual_settings, "pollinations_timeout_sec", 30) or 30)),
                    )
                    self._last_image_request_at = datetime.now(timezone.utc)
                    content_type = str(response.headers.get("content-type", "")).lower()
                    if response.status_code == 200 and content_type.startswith("image/") and response.content:
                        ext = ".png"
                        if "jpeg" in content_type or "jpg" in content_type:
                            ext = ".jpg"
                        elif "webp" in content_type:
                            ext = ".webp"
                        unique_token = f"{self._run_marker}_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
                        filename = self.temp_dir / (
                            f"generated_{index:02d}_pollinations_t{gen_try}_{mode}_{unique_token}{ext}"
                        )
                        filename.write_bytes(response.content)
                        try:
                            if filename.stat().st_size < 5 * 1024:
                                self._log_visual_event(
                                    {
                                        "event": "pollinations_image_too_small",
                                        "index": index,
                                        "model": model,
                                        "auth_mode": mode,
                                        "attempt": gen_try,
                                        "size_bytes": int(filename.stat().st_size),
                                    }
                                )
                                filename.unlink(missing_ok=True)
                                continue
                        except Exception:
                            pass

                        self._log_visual_event(
                            {
                                "event": "pollinations_success",
                                "index": index,
                                "status": 200,
                                "model": model,
                                "auth_mode": mode,
                                "saved_path": str(filename),
                                "size_bytes": int(filename.stat().st_size),
                                "attempt": gen_try,
                                "prompt_mode": prompt_mode,
                            }
                        )
                        ok, reason, metrics = self._validate_generated_asset(
                            path=filename,
                            role=role,
                            prompt=attempt_prompt,
                            context=paragraph,
                            retry_index=gen_try,
                            source_model=model,
                        )
                        if not ok:
                            candidate_score = self._rejected_candidate_score(reason=reason, metrics=metrics)
                            self._log_visual_event(
                                {
                                    "event": "generated_image_rejected",
                                    "provider": "pollinations",
                                    "index": index,
                                    "model": model,
                                    "auth_mode": mode,
                                    "attempt": gen_try,
                                    "prompt_mode": prompt_mode,
                                    "reason": reason,
                                    "metrics": metrics,
                                    "candidate_score": candidate_score,
                                }
                            )
                            if candidate_score is not None and candidate_score > best_candidate_score:
                                if best_candidate_path is not None:
                                    try:
                                        best_candidate_path.unlink(missing_ok=True)
                                    except Exception:
                                        pass
                                best_candidate_path = filename
                                best_candidate_score = float(candidate_score)
                                best_candidate_reason = str(reason)
                                best_candidate_metrics = dict(metrics or {})
                            else:
                                try:
                                    filename.unlink(missing_ok=True)
                                except Exception:
                                    pass
                            continue
                        try:
                            shutil.copy2(filename, cache_path)
                        except Exception:
                            pass
                        return ImageAsset(
                            path=filename,
                            alt=self._build_alt_text(keyword, "generated image"),
                            anchor_text=paragraph,
                            source_kind="pollinations",
                            source_url=f"pollinations://{model}",
                            license_note="Generated by Pollinations.ai.",
                        )

                    body_preview = ""
                    if not content_type.startswith("image/"):
                        body_preview = (response.text or "")[:260]
                    self._log_visual_event(
                        {
                            "event": "pollinations_failed",
                            "index": index,
                            "status": int(response.status_code),
                            "model": model,
                            "auth_mode": mode,
                            "content_type": content_type,
                            "response_preview": body_preview,
                            "attempt": gen_try,
                            "prompt_mode": prompt_mode,
                            "prompt_hash": cache_key[:16],
                        }
                    )
                except Exception as exc:
                    self._log_visual_event(
                        {
                            "event": "pollinations_failed",
                            "index": index,
                            "status": "exception",
                            "model": model,
                            "auth_mode": mode,
                            "exception": str(exc),
                            "attempt": gen_try,
                            "prompt_mode": prompt_mode,
                            "prompt_hash": cache_key[:16],
                        }
                    )
                    continue

        if best_candidate_path is not None and best_candidate_path.exists():
            self._log_visual_event(
                {
                    "event": "generated_image_best_effort_accept",
                    "provider": "pollinations",
                    "index": index,
                    "model": model,
                    "path": str(best_candidate_path),
                    "reason": best_candidate_reason,
                    "metrics": best_candidate_metrics,
                    "score": round(float(best_candidate_score), 4),
                }
            )
            return ImageAsset(
                path=best_candidate_path,
                alt=self._build_alt_text(keyword, "generated image"),
                anchor_text=paragraph,
                source_kind="pollinations",
                source_url=f"pollinations://{model}",
                license_note="Generated by Pollinations.ai.",
            )
        fallback = self._fallback_asset_for_role(role=role_key, index=index)
        if fallback is not None:
            return fallback
        return None

    def _simplify_prompt_for_retry(self, prompt: str, retry_index: int, role: str) -> str:
        text = re.sub(r"\s+", " ", str(prompt or "")).strip()
        text = re.sub(
            r"\b(two people in frame|small team of three people|group shot|crowded scene)\b",
            "single subject or no people",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(r"\bshallow depth of field\b", "natural depth of field", text, flags=re.IGNORECASE)
        text = re.sub(r"\bcinematic\b", "editorial", text, flags=re.IGNORECASE)
        text = re.sub(r"\bultra-high\b", "high", text, flags=re.IGNORECASE)

        addons: list[str] = [
            "Use one clear focal subject in a clean realistic environment.",
            "Avoid complex hand poses and avoid crowded interactions.",
            "Prefer simple real-world objects and natural composition.",
            "No text, no watermark, no logo.",
        ]
        if str(role or "").lower() == "thumbnail":
            addons.append("Keep center-weighted composition for thumbnail readability.")
        if int(retry_index) >= 4:
            addons.append("If people are present, use one person only with relaxed posture.")
        return re.sub(r"\s+", " ", f"{text} {' '.join(addons)}").strip()[:900]

    def _validate_generated_asset(
        self,
        path: Path,
        role: str,
        prompt: str,
        context: str,
        retry_index: int = 1,
        source_model: str = "",
    ) -> tuple[bool, str, dict]:
        metrics: dict[str, float | int | str | bool] = {
            "role": str(role or ""),
            "mediapipe_available": bool(self._mediapipe_available),
        }
        try:
            with Image.open(path) as im_raw:
                im = im_raw.convert("RGB")
        except Exception as exc:
            return False, f"invalid_image_file:{exc}", metrics

        w, h = im.size
        metrics["width"] = int(w)
        metrics["height"] = int(h)
        if w <= 0 or h <= 0:
            return False, "invalid_image_size", metrics
        # User-requested policy:
        # disable local quality gate checks for
        # - resolution/aspect ratio
        # - blur/sharpness
        # - monotony(color_std/edge_density)
        # - duplicate hash/embedding
        # - human anatomy (mediapipe)
        # - non-photoreal style prompt guard
        return True, "ok_local_gate_disabled", metrics

    def _min_sharpness_threshold(self, retry_index: int, role: str, source_model: str) -> float:
        # Retry-aware threshold profile:
        # t1=8.5 -> t2=7.5 -> t3=6.8 -> t4+=6.2 (base)
        # Then apply model/role adjustments.
        steps = [8.5, 7.5, 6.8, 6.2]
        idx = max(1, int(retry_index)) - 1
        base = steps[min(idx, len(steps) - 1)]
        model = str(source_model or "").lower()
        if "flux" in model:
            # Flux outputs tend to be softer; keep quality while avoiding pathological reject loops.
            base -= 0.8
        if str(role or "").strip().lower() == "thumbnail":
            # Thumbnail needs slightly clearer first impression.
            base += 0.25
        return max(5.4, float(base))

    def _rejected_candidate_score(self, reason: str, metrics: dict[str, float | int | str | bool]) -> float | None:
        # Best-of-retry only for near-pass blur failures.
        if str(reason or "") != "too_blurry":
            return None
        try:
            sharp = float(metrics.get("sharpness", 0.0) or 0.0)
            edge = float(metrics.get("edge_density", 0.0) or 0.0)
            color = float(metrics.get("color_std", 0.0) or 0.0)
        except Exception:
            return None
        if sharp < 4.9:
            return None
        # Weighted proxy for "closest to acceptable" visual quality.
        return (sharp * 12.0) + (edge * 1000.0) + (color * 0.08)

    def _perceptual_hash(self, image: Image.Image) -> str:
        try:
            small = image.convert("L").resize((16, 16), Image.Resampling.BICUBIC)
            arr = np.asarray(small, dtype=np.float32)
            med = float(np.median(arr))
            bits = (arr > med).astype(np.uint8).flatten().tolist()
            return "".join("1" if b else "0" for b in bits)
        except Exception:
            return ""

    def _hamming_distance_bits(self, left: str, right: str) -> int:
        if not left or not right:
            return 9999
        n = min(len(left), len(right))
        return sum(1 for i in range(n) if left[i] != right[i]) + abs(len(left) - len(right))

    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        try:
            denom = float(np.linalg.norm(a) * np.linalg.norm(b))
            if denom <= 1e-9:
                return 0.0
            return float(np.dot(a, b) / denom)
        except Exception:
            return 0.0

    def _image_embedding_vector(
        self,
        image: Image.Image,
        sharpness: float,
        color_std: float,
        edge_density: float,
        sat_mean: float,
    ) -> np.ndarray:
        rgb = np.asarray(image.convert("RGB"), dtype=np.float32) / 255.0
        hsv = np.asarray(image.convert("HSV"), dtype=np.float32)
        gray = np.asarray(image.convert("L"), dtype=np.float32) / 255.0

        brightness = float(gray.mean())
        contrast = float(gray.std())
        saturation = float((hsv[:, :, 1] / 255.0).mean())
        warm_ratio = float((rgb[:, :, 0] > rgb[:, :, 2]).mean())
        cool_ratio = float((rgb[:, :, 2] > rgb[:, :, 0]).mean())

        r = rgb[:, :, 0]
        g = rgb[:, :, 1]
        b = rgb[:, :, 2]
        skin = (
            (r > 0.35)
            & (g > 0.2)
            & (b > 0.15)
            & (r > g)
            & (g > b * 0.7)
            & ((r - g) > 0.02)
        )
        skin_ratio = float(skin.mean())

        gx = float(np.abs(gray[:, 1:] - gray[:, :-1]).mean())
        gy = float(np.abs(gray[1:, :] - gray[:-1, :]).mean())

        h, w = gray.shape
        cy0 = max(0, int(h * 0.28))
        cy1 = min(h, int(h * 0.72))
        cx0 = max(0, int(w * 0.28))
        cx1 = min(w, int(w * 0.72))
        center = gray[cy0:cy1, cx0:cx1] if cy1 > cy0 and cx1 > cx0 else gray
        center_focus = float(center.std())

        return np.asarray(
            [
                brightness,
                contrast,
                saturation,
                min(1.0, edge_density * 18.0),
                warm_ratio,
                cool_ratio,
                min(1.0, skin_ratio * 12.0),
                min(1.0, sharpness / 45.0),
                min(1.0, color_std / 95.0),
                min(1.0, gx * 12.0),
                min(1.0, gy * 12.0),
                min(1.0, center_focus * 8.0),
            ],
            dtype=np.float32,
        )

    def _context_embedding_vector(self, prompt: str, context: str) -> np.ndarray:
        text = f"{prompt or ''} {context or ''}".lower()
        vec = np.zeros(12, dtype=np.float32)

        def has(*tokens: str) -> bool:
            return any(t in text for t in tokens)

        if has("problem", "risk", "bottleneck", "failed", "issue"):
            vec[0] += 0.28
            vec[1] += 0.74
            vec[2] += 0.30
            vec[5] += 0.70
        if has("result", "success", "improve", "breakthrough", "win"):
            vec[0] += 0.78
            vec[1] += 0.50
            vec[2] += 0.45
            vec[4] += 0.70
        if has("process", "workflow", "checklist", "step"):
            vec[1] += 0.66
            vec[3] += 0.68
            vec[9] += 0.60
            vec[10] += 0.60
        if has("dashboard", "chart", "metrics", "screen", "ui", "console", "graph"):
            vec[3] += 0.85
            vec[9] += 0.72
            vec[10] += 0.72
        if has("meeting", "team", "manager", "colleague", "office worker", "people", "person"):
            vec[6] += 0.82
            vec[11] += 0.50
        if has("strategy", "decision", "plan", "roadmap"):
            vec[11] += 0.68
            vec[1] += 0.48
        if has("laptop", "desktop", "monitor", "workspace", "keyboard"):
            vec[3] += 0.52
            vec[9] += 0.46
            vec[10] += 0.46
        return np.clip(vec, 0.0, 1.0)

    def _check_human_anatomy(self, image: Image.Image) -> tuple[bool, bool, str]:
        if not self._mediapipe_available or mp is None:
            return (False, True, "mediapipe_unavailable")
        try:
            rgb = np.asarray(image.convert("RGB"), dtype=np.uint8)
        except Exception:
            return (False, True, "array_conversion_failed")

        face_count = 0
        pose_landmarks = None
        hand_landmarks = []
        try:
            with mp.solutions.face_detection.FaceDetection(
                model_selection=0,
                min_detection_confidence=0.45,
            ) as fd:
                face_res = fd.process(rgb)
                face_count = len(face_res.detections or []) if face_res else 0
        except Exception:
            face_count = 0
        try:
            with mp.solutions.pose.Pose(
                static_image_mode=True,
                model_complexity=1,
                enable_segmentation=False,
                min_detection_confidence=0.45,
            ) as pose:
                pose_res = pose.process(rgb)
                pose_landmarks = getattr(pose_res, "pose_landmarks", None)
        except Exception:
            pose_landmarks = None
        try:
            with mp.solutions.hands.Hands(
                static_image_mode=True,
                max_num_hands=4,
                min_detection_confidence=0.5,
            ) as hands:
                hand_res = hands.process(rgb)
                hand_landmarks = list(getattr(hand_res, "multi_hand_landmarks", []) or [])
        except Exception:
            hand_landmarks = []

        human_present = bool(face_count > 0 or pose_landmarks is not None or hand_landmarks)
        if not human_present:
            return (False, True, "no_human_detected")

        if pose_landmarks is not None and hand_landmarks:
            try:
                marks = pose_landmarks.landmark
                pose_wrists = []
                for idx in (15, 16):
                    lm = marks[idx]
                    if float(getattr(lm, "visibility", 0.0) or 0.0) >= 0.25:
                        pose_wrists.append((float(lm.x), float(lm.y)))
                if pose_wrists:
                    diag = (image.width ** 2 + image.height ** 2) ** 0.5
                    for hand in hand_landmarks:
                        w0 = hand.landmark[0]
                        hx, hy = float(w0.x), float(w0.y)
                        min_dist = min((((hx - px) ** 2 + (hy - py) ** 2) ** 0.5) for px, py in pose_wrists)
                        pixel_dist = min_dist * diag
                        # Very far wrist offset is a strong detached-limb signal.
                        if pixel_dist > max(image.width, image.height) * 0.42:
                            return (True, False, "detached_hand_suspect")
            except Exception:
                pass

        return (True, True, "ok")

    def _resolve_pollinations_model(self, role: str = "content") -> str:
        # Policy: all generated images use gptimage.
        requested = (
            str(getattr(self.visual_settings, "pollinations_thumbnail_model", "") or "").strip()
            if str(role or "").lower().strip() == "thumbnail"
            else str(getattr(self.visual_settings, "pollinations_content_model", "") or "").strip()
        )
        if requested and requested.lower() != "gptimage":
            self._log_visual_event(
                {
                    "event": "pollinations_model_forced",
                    "requested": requested,
                    "forced": "gptimage",
                    "role": str(role or "content"),
                }
            )
        return "gptimage"

    def _build_safety_settings(self) -> list[dict[str, str]]:
        # Keep legal-safe defaults while avoiding over-blocking on benign editorial visuals.
        return [
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_ONLY_HIGH"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_ONLY_HIGH"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_ONLY_HIGH"},
        ]

    def _respect_image_interval(self) -> None:
        # Free-tier stabilization: enforce at least 20s gap between image calls.
        min_gap = max(20, int(getattr(self.visual_settings, "image_request_interval_seconds", 20) or 20))
        if self._last_image_request_at is None:
            return
        delta = (datetime.now(timezone.utc) - self._last_image_request_at).total_seconds()
        if delta < min_gap:
            time.sleep(max(0.0, float(min_gap - delta)))

    def _log_visual_event(self, payload: dict) -> None:
        try:
            row = dict(payload or {})
            row["ts"] = datetime.now(timezone.utc).isoformat()
            with self._visual_log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _resolve_image_model_candidates(self, role: str = "content") -> list[str]:
        preferred = str(getattr(self.visual_settings, "gemini_image_model", "") or "").strip()
        model_methods = self._list_model_methods()
        discovered = [
            m for m in model_methods.keys() if ("image" in m.lower() or "imagen" in m.lower())
        ]

        def rank(m: str) -> tuple[int, str]:
            lower = m.lower()
            score = 90
            if "imagen-3.0-generate-001" in lower:
                score = 1
            elif "imagen-3.0-fast-001" in lower:
                score = 2
            elif "imagen-3.0-capability-001" in lower:
                score = 3
            elif "imagen-4.0-fast-generate-001" in lower:
                score = 12
            elif "imagen-4.0-generate-001" in lower:
                score = 14
            elif "imagen-4.0" in lower:
                score = 18
            elif "gemini-2.0-flash-exp-image-generation" in lower:
                score = 30
            elif "gemini-3-pro-image-preview" in lower:
                score = 35
            elif "gemini-2.5-flash-image" in lower:
                score = 12
            return (score, lower)

        if str(role or "").lower().strip() == "thumbnail":
            manual_seed = [
                preferred,
                "models/imagen-3.0-generate-001",
                "models/imagen-3.0-fast-001",
                "models/imagen-3.0-capability-001",
                "imagen-4.0-generate-001",
                "gemini-3-pro-image-preview",
                "gemini-2.0-flash-exp-image-generation",
            ]
        else:
            manual_seed = [
                "models/imagen-3.0-fast-001",
                preferred,
                "models/imagen-3.0-generate-001",
                "models/imagen-3.0-capability-001",
                "imagen-4.0-fast-generate-001",
                "gemini-2.5-flash-image",
                "gemini-2.0-flash-exp-image-generation",
            ]

        ranked_discovered = sorted(set(discovered), key=rank)
        ordered: list[str] = []
        seen_models: set[str] = set()
        disallow: set[str] = set()

        def push(name: str) -> None:
            m = self._normalize_model_id(str(name or "").strip())
            if not m:
                return
            if m in seen_models:
                return
            seen_models.add(m)
            if "preview-06-06" in m.lower():
                self._log_visual_event(
                    {
                        "event": "gemini_image_model_skip",
                        "model": m,
                        "reason": "deprecated_preview_model",
                    }
                )
                return
            if m in disallow:
                self._log_visual_event(
                    {
                        "event": "gemini_image_model_skip",
                        "model": m,
                        "reason": "disallowed_by_policy",
                    }
                )
                return
            methods = model_methods.get(m, set())
            if "generateContent" in methods:
                ordered.append(f"generateContent:{m}")
            elif "predict" in methods:
                ordered.append(f"predict:{m}")
            else:
                # Skip unknown model IDs to prevent predictable 404 spam.
                self._log_visual_event(
                    {
                        "event": "gemini_image_model_skip",
                        "model": m,
                        "reason": "unavailable_in_list_models",
                    }
                )
                return

        for m in manual_seed:
            push(m)
        for m in ranked_discovered:
            push(m)
        if not ordered:
            self._log_visual_event(
                {
                    "event": "gemini_image_skip",
                    "reason": "no_valid_image_model_candidates",
                }
            )
        return ordered[:8]

    def _parse_model_candidate(self, candidate: str) -> tuple[str, str]:
        raw = str(candidate or "").strip()
        if ":" not in raw:
            return ("generateContent", self._normalize_model_id(raw))
        method, model = raw.split(":", 1)
        method = method.strip() or "generateContent"
        model = self._normalize_model_id(model.strip())
        return (method, model)

    def _normalize_model_id(self, model: str) -> str:
        value = str(model or "").strip()
        if value.lower().startswith("models/"):
            value = value.split("/", 1)[1].strip()
        return value

    def _build_alt_text(self, subject: str, context: str) -> str:
        s = re.sub(r"\s+", " ", str(subject or "").strip()) or "the workflow"
        templates = [
            "Minimal diagram explaining the main troubleshooting steps.",
            "Practical workflow diagram aligned with this section.",
            "Clean process diagram showing a simplified fix sequence.",
            "Concept diagram highlighting a repeatable troubleshooting pattern.",
            "Visual summary of a practical device fix routine.",
            "Diagram for a real-world implementation scenario.",
            "Simple process diagram focused on practical workflow execution.",
            "Structured problem-solving flow diagram.",
            "Infographic-style process for operational decision clarity.",
            "Implementation-order diagram for beginner-friendly fixes.",
            "Lightweight troubleshooting flow diagram.",
            f"Troubleshooting process visual related to {s}.",
        ]
        pick = random.choice(templates)
        return re.sub(r"\s+", " ", pick).strip()[:180]

    def _fallback_asset_for_role(self, role: str, index: int) -> ImageAsset | None:
        is_thumb = str(role or "").strip().lower() == "thumbnail"
        rel = (
            str(getattr(self.visual_settings, "fallback_banner", "assets/fallback/banner.png"))
            if is_thumb
            else str(getattr(self.visual_settings, "fallback_inline", "assets/fallback/inline.png"))
        )
        src = (self.temp_dir.parent.parent / rel).resolve()
        if not src.exists():
            try:
                src.parent.mkdir(parents=True, exist_ok=True)
                self._create_runtime_fallback_image(src, role="thumbnail" if is_thumb else "inline")
            except Exception:
                # Guaranteed fallback creation path even when Pillow rendering fails.
                try:
                    tiny_png = base64.b64decode(
                        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8Xw8AAoMBgA6n7QkAAAAASUVORK5CYII="
                    )
                    src.write_bytes(tiny_png)
                except Exception:
                    return None
        if not src.exists():
            return None
        ext = src.suffix or ".png"
        dst = self.temp_dir / f"generated_{index:02d}_fallback{ext}"
        try:
            shutil.copy2(src, dst)
        except Exception:
            return None
        self._log_visual_event(
            {
                "event": "pollinations_image_fallback_local",
                "role": "thumbnail" if is_thumb else "inline",
                "source": str(src),
                "target": str(dst),
            }
        )
        return ImageAsset(
            path=dst,
            alt=self._build_alt_text("troubleshooting workflow", "fallback image"),
            anchor_text="",
            source_kind="pollinations",
            source_url="local://fallback",
            license_note="Local fallback image.",
        )

    def _create_runtime_fallback_image(self, path: Path, role: str = "inline") -> None:
        width, height = (1280, 720) if str(role).lower() == "thumbnail" else (1152, 648)
        bg_top = (241, 245, 255)
        bg_bottom = (220, 234, 255)
        img = Image.new("RGB", (width, height), bg_top)
        draw = ImageDraw.Draw(img)
        for y in range(height):
            t = y / max(1, height - 1)
            r = int((1 - t) * bg_top[0] + t * bg_bottom[0])
            g = int((1 - t) * bg_top[1] + t * bg_bottom[1])
            b = int((1 - t) * bg_top[2] + t * bg_bottom[2])
            draw.line([(0, y), (width, y)], fill=(r, g, b))
        draw.rounded_rectangle(
            [int(width * 0.08), int(height * 0.15), int(width * 0.92), int(height * 0.85)],
            radius=28,
            fill=(255, 255, 255),
            outline=(185, 198, 230),
            width=3,
        )
        # Text-free fallback: abstract geometry only.
        draw.rounded_rectangle(
            [int(width * 0.17), int(height * 0.30), int(width * 0.83), int(height * 0.40)],
            radius=16,
            fill=(205, 223, 246),
            outline=(178, 198, 226),
            width=2,
        )
        draw.rounded_rectangle(
            [int(width * 0.17), int(height * 0.46), int(width * 0.73), int(height * 0.56)],
            radius=16,
            fill=(214, 232, 251),
            outline=(184, 205, 231),
            width=2,
        )
        draw.rounded_rectangle(
            [int(width * 0.17), int(height * 0.62), int(width * 0.79), int(height * 0.72)],
            radius=16,
            fill=(223, 238, 253),
            outline=(191, 212, 238),
            width=2,
        )
        draw.ellipse(
            [int(width * 0.76), int(height * 0.22), int(width * 0.86), int(height * 0.34)],
            fill=(195, 216, 245),
            outline=(173, 197, 230),
            width=2,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        img.save(path, format="PNG", optimize=True)

    def _extract_image_payload(self, data: dict, method: str) -> tuple[bytes | None, str]:
        if method == "predict":
            for pred in (data.get("predictions", []) or []):
                if not isinstance(pred, dict):
                    continue
                b64 = (
                    pred.get("bytesBase64Encoded")
                    or pred.get("b64_json")
                    or pred.get("data")
                    or ""
                )
                if b64:
                    mime = str(pred.get("mimeType", "image/png") or "image/png")
                    try:
                        return (base64.b64decode(b64), mime)
                    except Exception:
                        continue
            return (None, "image/png")

        parts = data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
        for part in parts:
            inline = part.get("inline_data") or part.get("inlineData")
            if inline and inline.get("data"):
                mime = inline.get("mime_type") or inline.get("mimeType") or "image/png"
                try:
                    return (base64.b64decode(inline["data"]), str(mime))
                except Exception:
                    return (None, str(mime))
        return (None, "image/png")

    def _list_model_methods(self) -> dict[str, set[str]]:
        now = datetime.now(timezone.utc)
        if self._models_cache is not None:
            ts, items = self._models_cache
            if (now - ts).total_seconds() < 6 * 3600:
                return items
        if not self.gemini_api_key:
            return {}
        try:
            r = requests.get(
                "https://generativelanguage.googleapis.com/v1beta/models",
                params={"key": self.gemini_api_key},
                timeout=30,
            )
            r.raise_for_status()
            models = r.json().get("models", []) or []
            out: dict[str, set[str]] = {}
            for m in models:
                name = str(m.get("name", ""))
                if not name.startswith("models/"):
                    continue
                methods = m.get("supportedGenerationMethods", []) or []
                model_name = name.split("/", 1)[1]
                out[model_name] = {str(x) for x in methods if str(x)}
            self._models_cache = (now, out)
            return out
        except Exception:
            return {}

    def _build_chart_asset(self, prompt: str, index: int, paragraph: str, keyword: str) -> ImageAsset:
        labels = ["A", "B", "C", "D"]
        values = [random.randint(40, 100) for _ in labels]
        path = self.temp_dir / f"chart_{index:02d}.png"

        w, h = 1280, 720
        margin = 90
        chart_top = 120
        chart_bottom = h - 140
        chart_height = chart_bottom - chart_top
        bar_gap = 38
        bar_width = int((w - (margin * 2) - (bar_gap * (len(labels) - 1))) / len(labels))

        img = Image.new("RGB", (w, h), (245, 248, 255))
        draw = ImageDraw.Draw(img)
        title_font = ImageFont.load_default()
        body_font = ImageFont.load_default()

        draw.text((margin, 56), f"Insight Chart {index}", fill=(25, 35, 58), font=title_font)
        draw.line([(margin, chart_bottom), (w - margin, chart_bottom)], fill=(140, 150, 178), width=2)

        max_val = max(100, max(values))
        colors = [(94, 129, 255), (122, 208, 255), (126, 224, 179), (255, 185, 116)]
        for i, (label, val) in enumerate(zip(labels, values)):
            x0 = margin + i * (bar_width + bar_gap)
            x1 = x0 + bar_width
            bar_h = int((val / float(max_val)) * chart_height)
            y0 = chart_bottom - bar_h
            y1 = chart_bottom
            draw.rounded_rectangle([x0, y0, x1, y1], radius=10, fill=colors[i % len(colors)])
            draw.text((x0 + int(bar_width * 0.35), chart_bottom + 12), label, fill=(52, 66, 96), font=body_font)
            draw.text((x0 + int(bar_width * 0.3), y0 - 18), str(val), fill=(40, 54, 86), font=body_font)

        draw.text((margin, h - 56), "Local fallback chart (PIL renderer)", fill=(98, 112, 140), font=body_font)
        img.save(path)

        return ImageAsset(
            path=path,
            alt=self._build_alt_text(keyword, "comparison chart"),
            anchor_text=paragraph,
            source_kind="chart",
            source_url="local://chart",
            license_note="Generated locally by RezeroAgent.",
        )

    def _build_screenshot_variant(
        self,
        assets: list[ImageAsset],
        index: int,
        paragraph: str,
        keyword: str,
    ) -> ImageAsset | None:
        screenshot_assets = [a for a in assets if a.source_kind == "screenshot" and a.path.exists()]
        if not screenshot_assets:
            return None
        base_asset = random.choice(screenshot_assets)
        base = base_asset.path
        out = self.temp_dir / f"capture_variant_{index:02d}.png"
        try:
            with Image.open(base).convert("RGB") as im:
                w, h = im.size
                crop_w = int(w * random.uniform(0.72, 0.9))
                crop_h = int(h * random.uniform(0.72, 0.9))
                left = random.randint(0, max(0, w - crop_w))
                top = random.randint(0, max(0, h - crop_h))
                crop = im.crop((left, top, left + crop_w, top + crop_h))
                draw = ImageDraw.Draw(crop)
                draw.rectangle(
                    [4, 4, crop.width - 5, crop.height - 5],
                    outline=(41, 121, 255),
                    width=4,
                )
                crop.save(out)
            return ImageAsset(
                path=out,
                alt=self._build_alt_text(keyword, "highlighted screenshot"),
                anchor_text=paragraph,
                source_kind="screenshot",
                source_url=base_asset.source_url or "local://screenshot-variant",
                license_note="Derived from captured screenshot for editorial focus.",
            )
        except Exception:
            return None

    def _build_context_panel_asset(self, index: int, paragraph: str, keyword: str) -> ImageAsset:
        path = self.temp_dir / f"context_panel_{index:02d}.png"
        w, h = 1280, 720
        img = Image.new("RGB", (w, h), (234, 241, 250))
        draw = ImageDraw.Draw(img)

        # Subtle layered background for a clean editorial card.
        draw.rectangle([0, 0, w, h], fill=(234, 241, 250))
        draw.ellipse([w - 420, -140, w + 200, 360], fill=(210, 226, 247))
        draw.ellipse([-220, h - 260, 360, h + 160], fill=(214, 236, 229))
        draw.rounded_rectangle([84, 84, w - 84, h - 84], radius=34, fill=(248, 251, 255), outline=(186, 203, 224), width=3)

        title = (keyword or "Editorial context").strip()[:72]
        text = self._context_snippet(paragraph or keyword)
        font_title = ImageFont.load_default()
        font_body = ImageFont.load_default()

        draw.text((132, 132), title, fill=(20, 31, 49), font=font_title)
        wrapped = self._wrap_text(text, 70)
        y = 192
        for line in wrapped[:14]:
            draw.text((132, y), line, fill=(57, 73, 92), font=font_body)
            y += 28
        draw.text((132, h - 132), "Context panel generated for this section.", fill=(97, 112, 132), font=font_body)

        img.save(path)
        return ImageAsset(
            path=path,
            alt=self._build_alt_text(keyword, "context panel"),
            anchor_text=paragraph,
            source_kind="context",
            source_url="local://context-panel",
            license_note="Generated locally by RezeroAgent.",
        )

    def _context_snippet(self, paragraph: str) -> str:
        text = re.sub(r"\s+", " ", (paragraph or "")).strip()
        if not text:
            return "Operational summary for this section."
        sentences = re.split(r"(?<=[.!?])\s+", text)
        snippet = " ".join(sentences[:2]).strip() if sentences else text
        return snippet[:420]

    def _wrap_text(self, text: str, max_chars: int) -> list[str]:
        words = text.split()
        if not words:
            return []
        out: list[str] = []
        cur = ""
        for w in words:
            nxt = (cur + " " + w).strip()
            if len(nxt) <= max_chars:
                cur = nxt
            else:
                if cur:
                    out.append(cur)
                cur = w
        if cur:
            out.append(cur)
        return out

    def _fallback_prompt(
        self,
        paragraph: str,
        title: str,
        variation_index: int = 0,
        role: str = "section",
    ) -> str:
        context_excerpt = re.sub(r"\s+", " ", str(paragraph or "").strip())[:300]
        base = (
            "Create one realistic photo-style image from this context: "
            f"\"{context_excerpt}\". Post title: {title}. "
            "Use a concrete scene that matches the context, with natural lighting and materials. "
            "People are optional. "
            "No text, no letters, no logos, no watermark."
        )
        return re.sub(r"\s+", " ", base).strip()[:900]

    def _build_thumbnail_prompt(self, draft: DraftPost) -> str:
        summary = re.sub(r"\s+", " ", str(draft.summary or "")).strip()
        if not summary:
            summary = re.sub(r"\s+", " ", str(draft.title or "")).strip()
        base = (
            "Create a realistic thumbnail image for this blog topic. "
            "16:9 landscape composition, high-resolution, clear focal point, clean background. "
            f"Title context: {draft.title}. "
            f"Summary context: {summary[:280]}. "
            "No text, no letters, no numbers, no logos, no watermark."
        )
        return re.sub(r"\s+", " ", base).strip()[:900]

    def _enforce_no_text_rule(self, prompt: str) -> str:
        """
        Hard prompt suffix to reduce accidental text rendering in generated images.
        """
        base = re.sub(r"\s+", " ", str(prompt or "").strip())
        strict = (
            "ABSOLUTE RULE: image must contain zero readable text. "
            "Do not render letters, words, numbers, logos, trademarks, watermark, captions, UI labels, or signs. "
            "If any surface could show text, keep it blank or abstract."
        )
        suffix = re.sub(
            r"\s+",
            " ",
            str(getattr(self.visual_settings, "prompt_suffix", "") or "").strip(),
        )
        out = f"{base} {strict} {suffix}".strip()
        return re.sub(r"\s+", " ", out)[:1100]

    def _build_canva_thumbnail_prompt(
        self,
        base_prompt: str,
        paragraph: str,
        title: str,
        target_text: str,
    ) -> str:
        core_summary = re.sub(r"\s+", " ", str(paragraph or title or "").strip())[:280]
        # User-requested template is quoted and then specialized with target words.
        template = (
            "[Style: Professional Tech Magazine Cover, Minimalist, High-end Photography]\n"
            f"[Subject: A conceptual and high-quality visual representation of {core_summary}]\n"
            "Composition: Center-focused, cinematic lighting, shallow depth of field (bokeh background).\n"
            "- Elements: Incorporate sleek tech textures (brushed metal, glass, soft LED glows).\n"
            "Lighting: Soft studio lighting with a clean gradient background.\n"
            "- Color Palette: Low-saturation base with one bold accent color tied to the topic.\n"
            "Aspect Ratio: 16:9.\n"
            "[Final Instruction: Ensure the image looks like a professional 4K thumbnail for a high-end business and technology news site.]"
        )
        out = f"{template}\n{base_prompt}\n"
        out += "No text or words inside the image."
        return re.sub(r"\s+", " ", out).strip()[:1300]

    def _extract_impact_keywords(self, title: str, max_words: int = 2) -> list[str]:
        via_llm = self._extract_impact_keywords_with_gemini(title, max_words=max_words)
        if via_llm:
            return via_llm[:max_words]
        return self._extract_impact_keywords_fallback(title, max_words=max_words)

    def _extract_impact_keywords_with_gemini(self, title: str, max_words: int = 2) -> list[str]:
        if not self.gemini_api_key:
            return []
        prompt = (
            "Pick the most impactful 1-2 words from this title for a thumbnail text hook. "
            "Return strict JSON only: {\"words\": [\"WORD1\", \"WORD2\"]}. "
            "Rules: uppercase English words only, no punctuation, no explanation.\n"
            f"Title: {title}"
        )
        for model in self._candidate_text_models():
            endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            try:
                response = requests.post(
                    endpoint,
                    params={"key": self.gemini_api_key},
                    json={"contents": [{"parts": [{"text": prompt}]}]},
                    timeout=45,
                )
                if response.status_code != 200:
                    continue
                data = response.json()
                text = "\n".join(
                    part.get("text", "")
                    for part in data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
                    if "text" in part
                )
                match = re.search(r"\{.*\}", text, flags=re.DOTALL)
                if not match:
                    continue
                parsed = json.loads(match.group(0))
                words = parsed.get("words", [])
                if not isinstance(words, list):
                    continue
                out: list[str] = []
                for w in words:
                    token = re.sub(r"[^A-Za-z0-9]", "", str(w or "").strip()).upper()
                    if len(token) < 3:
                        continue
                    if token not in out:
                        out.append(token)
                    if len(out) >= max_words:
                        break
                if out:
                    return out
            except Exception:
                continue
        return []

    def _extract_impact_keywords_fallback(self, title: str, max_words: int = 2) -> list[str]:
        stop = {
            "THE",
            "THIS",
            "THAT",
            "WITH",
            "FROM",
            "YOUR",
            "ABOUT",
            "GUIDE",
            "INTRO",
            "WHAT",
            "WHY",
            "HOW",
            "WHEN",
            "WHERE",
            "TODAY",
        }
        tokens = re.findall(r"[A-Za-z][A-Za-z0-9]{2,}", str(title or ""))
        ranked = sorted(
            {t.upper() for t in tokens if t.upper() not in stop},
            key=lambda x: (-len(x), x),
        )
        if not ranked:
            return ["INSIGHT"]
        return ranked[:max_words]

    def _apply_canva_thumbnail_overlay(self, path: Path, target_text: str) -> None:
        try:
            image = Image.open(path).convert("RGB")
            # Low-saturation, high-contrast Canva-like base.
            gray = ImageOps.grayscale(image).convert("RGB")
            blended = Image.blend(image, gray, 0.48)
            contrasted = ImageEnhance.Contrast(blended).enhance(1.18)
            canvas = contrasted.convert("RGBA")
            draw = ImageDraw.Draw(canvas)
            w, h = canvas.size
            panel_h = int(h * 0.33)
            y0 = h - panel_h
            draw.rectangle([(0, y0), (w, h)], fill=(14, 18, 30, 210))

            fonts = [
                "C:/Windows/Fonts/segoeuib.ttf",
                "C:/Windows/Fonts/arialbd.ttf",
                "C:/Windows/Fonts/malgunbd.ttf",
            ]
            title_font = None
            for fp in fonts:
                try:
                    title_font = ImageFont.truetype(fp, max(46, w // 18))
                    break
                except Exception:
                    continue
            if title_font is None:
                title_font = ImageFont.load_default()

            text_value = re.sub(r"\s+", " ", str(target_text or "").strip()).upper()[:40]
            x = int(w * 0.04)
            y = y0 + int(panel_h * 0.20)
            draw.text((x, y), text_value, font=title_font, fill=(255, 255, 255, 255))
            canvas.convert("RGB").save(path, quality=92)
        except Exception:
            return

    def _verify_thumbnail_text_with_gemini_vision(self, image_path: Path, target_text: str) -> bool:
        # Disabled by policy: no text is requested in thumbnails.
        return True
        if not self.gemini_api_key:
            return False
        if not image_path.exists():
            return False
        try:
            raw = image_path.read_bytes()
        except Exception:
            return False
        if not raw:
            return False
        mime = "image/png" if image_path.suffix.lower() == ".png" else "image/jpeg"
        prompt = (
            "Extract the text from this image. "
            f'Does it match "{target_text}" exactly? '
            "Answer only YES or NO."
        )
        models = self._candidate_text_models()
        b64 = base64.b64encode(raw).decode("ascii")
        for model in models:
            try:
                endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
                payload = {
                    "contents": [
                        {
                            "parts": [
                                {"text": prompt},
                                {"inline_data": {"mime_type": mime, "data": b64}},
                            ]
                        }
                    ]
                }
                response = requests.post(
                    endpoint,
                    params={"key": self.gemini_api_key},
                    json=payload,
                    timeout=60,
                )
                if response.status_code != 200:
                    self._log_visual_event(
                        {
                            "event": "thumbnail_ocr_probe",
                            "model": model,
                            "status": int(response.status_code),
                            "body": (response.text or "")[:260],
                        }
                    )
                    continue
                data = response.json()
                text = "\n".join(
                    part.get("text", "")
                    for part in data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
                    if "text" in part
                )
                decision = re.sub(r"\s+", " ", text).strip().upper()
                if "YES" in decision and "NO" not in decision:
                    return True
                if "NO" in decision:
                    return False
            except Exception:
                continue
        return False

    def _candidate_text_models(self) -> list[str]:
        preferred = str(getattr(self.visual_settings, "gemini_prompt_model", "") or "").strip()
        ordered = [
            preferred,
            "gemini-2.5-flash",
            "gemini-2.5-flash-lite",
            "gemini-2.0-flash",
            "gemini-1.5-flash",
            "gemini-1.5-flash-latest",
        ]
        available = self._list_model_methods()
        seen: set[str] = set()
        out: list[str] = []
        for raw_model in ordered:
            model = self._normalize_model_id(raw_model)
            if not model or model in seen:
                continue
            seen.add(model)
            methods = available.get(model, set())
            if methods and "generateContent" not in methods:
                continue
            out.append(model)
        if not out and available:
            for model, methods in available.items():
                lower = model.lower()
                if "generatecontent" not in {m.lower() for m in methods}:
                    continue
                if "gemini" not in lower:
                    continue
                if "flash" in lower or "pro" in lower:
                    out.append(model)
                if len(out) >= 5:
                    break
        return out[:6]

    def _enforce_cinematic_realism(
        self,
        base_prompt: str,
        role: str = "section",
        variation_index: int = 0,
        context: str = "",
        title: str = "",
    ) -> str:
        base = re.sub(r"\s+", " ", str(base_prompt or "")).strip()
        cinematic = (
            "Cinematic Realism priority: ultra-high. "
            "Editorial magazine photo style, natural skin/material texture, subtle film grain, "
            "physically plausible light bounce, balanced contrast, true-to-life color grading, "
            "35mm-50mm lens feel, layered depth of field."
        )
        quality_guard = (
            "Quality guardrails: avoid glossy plastic CGI look, avoid cartoon style, avoid anime style, "
            "avoid vector illustration, avoid 3D render look, avoid over-sharpened edges, "
            "avoid neon over-saturation, no text, no words, no letters, no logos, no watermark."
        )
        human_guard = (
            "People are optional. If people are present: no detached limbs, no floating hands, "
            "no missing head, realistic anatomy only."
        )
        framing = (
            "Framing: cinematic composition with one clear focal subject and realistic environment context."
            if role == "thumbnail"
            else "Framing: contextual storytelling shot with realistic environment details aligned to paragraph context."
        )
        diversity = self._build_diversity_variant(
            variation_index=variation_index,
            role=role,
            context=context,
            title=title,
        )
        out = f"{base} {cinematic} {human_guard} {framing} {diversity} {quality_guard}"
        return re.sub(r"\s+", " ", out).strip()[:900]

    def _build_diversity_variant(
        self,
        variation_index: int,
        role: str,
        context: str,
        title: str,
    ) -> str:
        seed_text = f"{variation_index}|{role}|{context[:120]}|{title[:120]}"
        seed_int = int(hashlib.sha1(seed_text.encode("utf-8", errors="ignore")).hexdigest()[:8], 16)
        rng = random.Random(seed_int)

        intent = self._paragraph_intent(context, title)
        scenes = self._context_scene_pool(context)
        if not scenes:
            scenes = self._intent_scene_pool(intent)
        if not scenes:
            scenes = [
                "city street at golden hour",
                "quiet home interior with natural light",
                "coffee shop corner with candid atmosphere",
                "modern laboratory workstation",
                "server room aisle with practical lighting",
                "retail shelf and checkout environment",
                "workshop bench with tools",
                "public transit station concourse",
                "classroom or training room setup",
                "library reading area",
            ]
        actions = [
            "observing a practical task in progress",
            "reviewing information on a screen",
            "setting up or adjusting a tool",
            "comparing two options side by side",
            "capturing notes in a notebook",
            "checking device output after a change",
            "walking through a process step",
            "validating a result with focused attention",
            "showing before/after contrast in one scene",
            "finalizing a task with clear outcome cues",
        ]
        camera_angles = [
            "close-up portrait",
            "medium shot",
            "wide environmental shot",
            "over-the-shoulder angle",
            "three-quarter profile shot",
            "side profile composition",
        ]
        lighting = [
            "soft morning daylight",
            "neutral ambient indoor lighting",
            "warm desk-lamp key light",
            "late-afternoon window light",
            "soft studio-like diffused light",
            "cool high-clarity practical light",
        ]
        wardrobe = [
            "casual everyday clothing",
            "minimal neutral outfit",
            "practical workwear",
            "simple contemporary style",
        ]
        accent = [
            "teal accent",
            "amber accent",
            "blue accent",
            "coral accent",
            "mint accent",
        ]

        people_phrase = rng.choice(
            [
                "no people in frame",
                "one person in frame",
                "two people in frame",
                "small team of three people",
            ]
        )
        if str(role).lower() == "thumbnail" and rng.random() < 0.35:
            people_phrase = "no people in frame"
        scene = rng.choice(scenes)
        action = rng.choice(actions)
        angle = rng.choice(camera_angles)
        light = rng.choice(lighting)
        wear = rng.choice(wardrobe)
        acc = rng.choice(accent)
        return (
            f"Variation profile: intent={intent}; {people_phrase}; scene={scene}; action={action}; "
            f"camera={angle}; lighting={light}; wardrobe={wear}; color accent={acc}; "
            "background must differ from typical previous shot style."
        )

    def _paragraph_intent(self, context: str, title: str) -> str:
        text = f"{context or ''} {title or ''}".lower()
        rules = [
            ("problem", r"\b(problem|pain|risk|issue|fails?|friction|bottleneck)\b"),
            ("comparison", r"\b(compare|versus|vs|alternative|option|trade-?off)\b"),
            ("process", r"\b(step|process|workflow|setup|checklist|framework)\b"),
            ("result", r"\b(result|impact|outcome|gain|improve|boost|saved)\b"),
            ("strategy", r"\b(strategy|plan|decision|roadmap|priority|metric)\b"),
        ]
        for name, pattern in rules:
            if re.search(pattern, text):
                return name
        return "general"

    def _intent_scene_pool(self, intent: str) -> list[str]:
        pools = {
            "problem": [
                "cluttered home desk with unresolved tasks",
                "busy operations area showing workflow friction",
                "late-evening workspace under pressure",
                "screen with warning states and pending items",
            ],
            "comparison": [
                "side-by-side device comparison setup",
                "split visual showing two contrasting approaches",
                "decision table with notes and references",
                "A/B-style scene with clear contrast cues",
            ],
            "process": [
                "step-by-step setup sequence on a work surface",
                "checklist-and-tools arrangement before execution",
                "structured planning board with ordered phases",
                "organized station prepared for a repeatable process",
            ],
            "result": [
                "clean and simplified environment after optimization",
                "clear before/after outcome composition",
                "focused deep-work scene in bright natural light",
                "calm wrap-up scene with completed tasks",
            ],
            "strategy": [
                "roadmap planning scene with visual milestones",
                "priority mapping on a planning board",
                "metric review table with charts and annotations",
                "strategy session in a neutral meeting space",
            ],
            "general": [
                "clean real-world environment tied to the topic",
                "minimal setup with one clear focal object",
                "practical everyday setting with believable details",
                "documentary-style scene with natural light",
            ],
        }
        return list(pools.get(intent, pools["general"]))

    def _context_scene_pool(self, context: str) -> list[str]:
        text = f" {str(context or '').lower()} "
        buckets: list[list[str]] = []

        def has_any(words: list[str]) -> bool:
            return any(f" {w} " in text for w in words)

        if has_any(["travel", "flight", "airport", "hotel", "commute", "train"]):
            buckets.append(
                [
                    "airport terminal waiting zone",
                    "train station platform scene",
                    "hotel desk with travel setup",
                    "in-transit workspace with carry-on gear",
                ]
            )
        if has_any(["home", "remote", "kitchen", "living room", "family", "personal"]):
            buckets.append(
                [
                    "home desk by a window",
                    "kitchen table with practical setup",
                    "living room side table work corner",
                    "small apartment workspace",
                ]
            )
        if has_any(["code", "developer", "software", "api", "server", "cloud", "database"]):
            buckets.append(
                [
                    "developer desk with terminal glow",
                    "server room aisle with status lights",
                    "operations dashboard screen setup",
                    "modern dev lab environment",
                ]
            )
        if has_any(["design", "brand", "creative", "photo", "video", "editing"]):
            buckets.append(
                [
                    "creative studio desk with color tools",
                    "editing station with timeline on display",
                    "camera and lighting gear workspace",
                    "design review table with visual mockups",
                ]
            )
        if has_any(["finance", "budget", "cost", "price", "revenue", "adsense"]):
            buckets.append(
                [
                    "financial planning desk with charts",
                    "calculator and report review setup",
                    "budget board with trend lines",
                    "performance review scene with notebooks",
                ]
            )
        if has_any(["health", "medical", "wellness", "sleep", "fitness"]):
            buckets.append(
                [
                    "wellness-focused home routine scene",
                    "health journal and device setup",
                    "morning routine environment with natural light",
                    "calm workspace break area",
                ]
            )
        if has_any(["education", "learn", "course", "student", "training"]):
            buckets.append(
                [
                    "classroom desk with learning materials",
                    "online study setup with notes",
                    "training room with presentation screen",
                    "library research table",
                ]
            )
        if has_any(["retail", "store", "customer", "sales", "shop", "ecommerce"]):
            buckets.append(
                [
                    "retail checkout counter scene",
                    "store shelf management view",
                    "customer service desk setup",
                    "packing station for online orders",
                ]
            )

        merged: list[str] = []
        seen: set[str] = set()
        for group in buckets:
            for scene in group:
                key = scene.strip().lower()
                if key in seen:
                    continue
                seen.add(key)
                merged.append(scene)
        return merged

    def _optimize_image_for_seo(self, path: Path, role: str = "content") -> bool:
        role_key = str(role or "").strip().lower()
        target_width = 1200 if role_key == "thumbnail" else 960
        try:
            if not path.exists() or not path.is_file():
                self._log_visual_event(
                    {
                        "event": "image_optimize_skip",
                        "reason": "missing_file",
                        "path": str(path),
                        "role": role_key,
                    }
                )
                return False
            with Image.open(path) as image:
                working = image
                if image.width > target_width:
                    ratio = target_width / float(image.width)
                    new_height = max(1, int(image.height * ratio))
                    working = image.resize((target_width, new_height), Image.Resampling.LANCZOS)

                ext = path.suffix.lower()
                if ext in {".jpg", ".jpeg"}:
                    if working.mode not in {"RGB", "L"}:
                        working = working.convert("RGB")
                    working.save(path, quality=84, optimize=True, progressive=True)
                    return True
                if ext == ".png":
                    working.save(path, optimize=True, compress_level=9)
                    return True
                if ext == ".webp":
                    if working.mode not in {"RGB", "L"}:
                        working = working.convert("RGB")
                    working.save(path, quality=84, method=6)
                    return True
                if working.mode not in {"RGB", "L"}:
                    working = working.convert("RGB")
                working.save(path, quality=84, optimize=True)
                return True
        except Exception as exc:
            self._log_visual_event(
                {
                    "event": "image_optimize_failed",
                    "reason": str(exc),
                    "path": str(path),
                    "role": role_key,
                }
            )
            return False

    def _resize_max_width(self, path: Path, width: int) -> None:
        with Image.open(path) as image:
            if image.width <= width:
                return
            ratio = width / float(image.width)
            height = int(image.height * ratio)
            resized = image.resize((width, height), Image.Resampling.LANCZOS)
            resized.save(path)
