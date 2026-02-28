from __future__ import annotations

import hashlib
import json
import random
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image

from .image_optimizer import optimize_for_library
from .news_pack_manifest import NewsPackManifest
from .news_pack_prompt_factory import NewsPackPromptFactory
from .news_pack_providers import (
    BadResponseError,
    GeminiImageProvider,
    PollinationsProvider,
    ProviderResult,
    RateLimitError,
    TemporaryProviderError,
)
from .news_pack_scheduler import NewsPackScheduler
from .news_pack_state import NewsPackState, NewsPackStateStore
from .r2_uploader import R2Config, upload_file as r2_upload_file
from .settings import NewsPackSettings


class NewsPackSeeder:
    def __init__(
        self,
        *,
        root: Path,
        settings: NewsPackSettings,
        ollama_client=None,
        gemini_api_key: str = "",
        gemini_model: str = "gemini-2.0-flash",
        r2_config: Any | None = None,
    ) -> None:
        self.root = root
        self.settings = settings
        self.ollama_client = ollama_client
        self.gemini_api_key = str(gemini_api_key or "").strip()
        self.gemini_model = str(gemini_model or "gemini-2.0-flash").strip()
        self.r2_config = self._normalize_r2_config(r2_config)
        self.state_store = NewsPackStateStore(
            root=root,
            state_path=str(settings.state_path or "storage/state/news_pack_state.json"),
        )
        self.manifest = NewsPackManifest(
            root=root,
            manifest_path=str(settings.manifest_path or "storage/state/news_pack_manifest.jsonl"),
        )
        self.prompt_factory = NewsPackPromptFactory(ollama_client=ollama_client)
        self.scheduler = NewsPackScheduler(
            interval_minutes_base=int(getattr(settings, "interval_minutes_base", 150) or 150),
            interval_minutes_jitter=int(getattr(settings, "interval_minutes_jitter", 45) or 45),
        )
        self.log_path = (root / "storage" / "logs" / "news_pack_seeder.jsonl").resolve()
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_rotate_max_bytes = 50 * 1024 * 1024
        self._log_rotate_keep = 10

    def seed_one_tick(self, force: bool = False) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        state = self.state_store.load()
        if not bool(getattr(self.settings, "enabled", True)):
            return self._result("disabled", state)
        if not force and not self.state_store.should_run(state, now_utc=now):
            return self._result("not_due", state)

        ready_thumb = int(self.manifest.ready_count(kind="thumb_bg"))
        ready_inline = int(self.manifest.ready_count(kind="inline_bg"))
        state = self._sync_mode_from_inventory(state=state, ready_thumb=ready_thumb, ready_inline=ready_inline)
        target_total = max(1, int(getattr(self.settings, "daily_target_total", 10) or 10))
        if state.mode != "bootstrap" and int(state.generated_total or 0) >= target_total:
            state.next_run_at_utc = self.scheduler.next_day_start_et(now_utc=now).isoformat()
            state.last_run_at_utc = now.isoformat()
            state.last_error = ""
            self.state_store.save(state)
            return self._result(
                "daily_complete",
                state,
                extra={"ready_thumb_bg": ready_thumb, "ready_inline_bg": ready_inline},
            )

        kind = self._pick_kind(state=state, ready_thumb=ready_thumb, ready_inline=ready_inline)
        tags = self._pick_tags()
        seed = random.randint(1000, 999999)
        recent_rows = self.manifest.get_recent(kind=kind, tags=tags, limit=24)
        recent_prompt_hashes = [
            str((row or {}).get("prompt_hash", "") or "").strip()
            for row in recent_rows
            if str((row or {}).get("prompt_hash", "") or "").strip()
        ][:20]
        recent_style_tags: list[str] = []
        for row in recent_rows:
            for tag in ((row or {}).get("style_tags", []) or []):
                txt = str(tag or "").strip().lower()
                if txt and txt not in recent_style_tags:
                    recent_style_tags.append(txt)
                if len(recent_style_tags) >= 20:
                    break
            if len(recent_style_tags) >= 20:
                break
        prompt_pack = self.prompt_factory.build(
            tags=tags,
            kind=kind,
            seed=seed,
            context={
                "date_et": state.date_et,
                "generated_total": int(state.generated_total or 0),
                "mode": str(state.mode or "normal"),
                "ready_thumb_bg": int(ready_thumb),
                "ready_inline_bg": int(ready_inline),
                "recent_prompt_hashes": recent_prompt_hashes,
                "recent_style_tags": recent_style_tags,
            },
        )
        prompt_hash = hashlib.sha1(prompt_pack.background_prompt.encode("utf-8", errors="ignore")).hexdigest()
        if self.manifest.has_today_duplicate(prompt_hash=prompt_hash):
            state.next_run_at_utc = self._compute_next_run(state=state, failure_kind="")
            state.last_run_at_utc = now.isoformat()
            state.last_error = "duplicate_prompt_hash_today"
            self.state_store.save(state)
            return self._result("duplicate_skip", state, extra={"kind": kind, "tags": tags})

        result, failure_kind, error_text, attempts = self._run_provider_chain(
            prompt=prompt_pack.background_prompt,
            kind=kind,
            seed=seed,
            state=state,
        )
        self._log(
            {
                "event": "provider_chain_result",
                "kind": kind,
                "tags": tags,
                "attempts": attempts,
                "failure_kind": failure_kind,
                "error": str(error_text or "")[:220],
                "provider": str(getattr(result, "provider", "") or ""),
            }
        )
        if result is None:
            state.consecutive_failures = int(state.consecutive_failures or 0) + 1
            state.last_error = str(error_text or "seed_failed")[:220]
            state.last_run_at_utc = now.isoformat()
            if failure_kind == "service_rate_limited":
                state.last_rate_limit_at = now.isoformat()
            state.next_run_at_utc = self._compute_next_run(state=state, failure_kind=failure_kind)
            self.state_store.save(state)
            self.manifest.append(
                {
                    "kind": kind,
                    "tags": tags,
                    "provider": "none",
                    "prompt": prompt_pack.background_prompt,
                    "prompt_hash": prompt_hash,
                    "local_path": "",
                    "r2_key": "",
                    "r2_url": "",
                    "sha1": "",
                    "width": 0,
                    "height": 0,
                    "byte_size": 0,
                    "status": "failed",
                    "used_at": "",
                    "used_by": "",
                    "used_count": 0,
                    "source_mode": "bootstrap" if state.mode == "bootstrap" else "seeded",
                    "quality_flags": [str(failure_kind or "seed_failed")],
                    "alt_text_template": "Editorial support graphic for this section.",
                    "caption_template": "",
                    "overlay_hook_used": "",
                    "error": str(error_text or "")[:220],
                }
            )
            self.manifest.mark_failed(prompt_hash=prompt_hash, reason=failure_kind or "seed_failed")
            self._log(
                {
                    "event": "seed_fail",
                    "kind": kind,
                    "tags": tags,
                    "failure_kind": failure_kind,
                    "error": str(error_text or "")[:220],
                    "next_run_at_utc": state.next_run_at_utc,
                }
            )
            return self._result(
                "failed",
                state,
                extra={"kind": kind, "reason": state.last_error, "failure_kind": str(failure_kind or "")},
            )

        try:
            local_path, width, height, sha1, byte_size, quality_flags = self._store_local_asset(result=result, kind=kind)
            if self.manifest.has_today_duplicate(sha1=sha1):
                state.next_run_at_utc = self._compute_next_run(state=state, failure_kind="")
                state.last_run_at_utc = now.isoformat()
                state.last_error = "duplicate_image_sha1_today"
                self.state_store.save(state)
                return self._result("duplicate_skip", state, extra={"kind": kind, "tags": tags})
            r2_url, r2_key = self._upload_to_r2(local_path=local_path, kind=kind)
        except Exception as exc:
            state.consecutive_failures = int(state.consecutive_failures or 0) + 1
            state.last_error = str(exc)[:220]
            state.last_run_at_utc = now.isoformat()
            state.next_run_at_utc = self._compute_next_run(state=state, failure_kind="temporary_server_error")
            self.state_store.save(state)
            self._log(
                {
                    "event": "seed_store_or_upload_fail",
                    "kind": kind,
                    "error": str(exc)[:220],
                    "next_run_at_utc": state.next_run_at_utc,
                }
            )
            return self._result(
                "failed",
                state,
                extra={"kind": kind, "reason": state.last_error, "failure_kind": "temporary_server_error"},
            )

        state.consecutive_failures = 0
        state.last_error = ""
        state.last_run_at_utc = now.isoformat()
        state.next_run_at_utc = self._compute_next_run(state=state, failure_kind="")
        state.generated_total = int(state.generated_total or 0) + 1
        state.last_success_provider = str(getattr(result, "provider", "") or "")
        if state.mode == "bootstrap":
            state.bootstrap_generated_today = int(state.bootstrap_generated_today or 0) + 1
            state.last_bootstrap_at = now.isoformat()
        if kind == "thumb_bg":
            state.generated_thumb_bg = int(state.generated_thumb_bg or 0) + 1
        elif kind == "inline_bg":
            state.generated_inline_bg = int(state.generated_inline_bg or 0) + 1
        if result.provider == "gemini":
            state.gemini_fallback_used_today = int(state.gemini_fallback_used_today or 0) + 1
        self.state_store.save(state)

        self.manifest.append(
            {
                "kind": kind,
                "tags": tags,
                "provider": result.provider,
                "prompt": prompt_pack.background_prompt,
                "prompt_hash": prompt_hash,
                "local_path": str(local_path),
                "r2_key": r2_key,
                "r2_url": r2_url,
                "sha1": sha1,
                "width": int(width),
                "height": int(height),
                "byte_size": int(byte_size),
                "status": "ready",
                "used_at": "",
                "used_by": "",
                "used_count": 0,
                "source_mode": "bootstrap" if state.mode == "bootstrap" else "seeded",
                "quality_flags": quality_flags,
                "alt_text_template": "Editorial support graphic for this section.",
                "caption_template": "",
                "overlay_hook_used": "",
                "hook_candidates": list(prompt_pack.hook_candidates or []),
                "style_tags": list(prompt_pack.style_tags or []),
                "palette_hint": str(getattr(prompt_pack, "palette_hint", "") or ""),
                "composition_hint": str(getattr(prompt_pack, "composition_hint", "") or ""),
                "density_hint": str(getattr(prompt_pack, "density_hint", "") or ""),
                "mood_hint": str(getattr(prompt_pack, "mood_hint", "") or ""),
            }
        )
        self._log(
            {
                "event": "seed_ok",
                "kind": kind,
                "tags": tags,
                "provider": result.provider,
                "r2_url": r2_url,
                "next_run_at_utc": state.next_run_at_utc,
                "mode": state.mode,
                "ready_thumb_bg": ready_thumb,
                "ready_inline_bg": ready_inline,
            }
        )
        return self._result("seeded", state, extra={"kind": kind, "r2_url": r2_url, "provider": result.provider})

    def _run_provider_chain(
        self,
        *,
        prompt: str,
        kind: str,
        seed: int,
        state: NewsPackState,
    ) -> tuple[ProviderResult | None, str, str, list[dict[str, Any]]]:
        width, height = (1280, 720) if str(kind).lower() == "thumb_bg" else (960, 540)
        order = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings, "provider_order", []) or [])
            if str(x or "").strip()
        ]
        if not order:
            order = ["pollinations_auth", "pollinations_anon", "gemini"]
        # Enforce provider sequence so auth 429 always falls through to anon first.
        normalized: list[str] = []
        for name in order:
            if name not in normalized:
                normalized.append(name)
        if "pollinations_auth" in normalized and "pollinations_anon" not in normalized:
            auth_idx = normalized.index("pollinations_auth")
            normalized.insert(auth_idx + 1, "pollinations_anon")
        canonical = ["pollinations_auth", "pollinations_anon", "gemini"]
        ordered: list[str] = [name for name in canonical if name in normalized]
        for name in normalized:
            if name not in ordered:
                ordered.append(name)
        order = ordered
        pollinations_service_limited = False
        attempts: list[dict[str, Any]] = []
        provider_try_order = list(order)

        def _next_provider(idx: int) -> str:
            if idx + 1 < len(order):
                return str(order[idx + 1] or "").strip().lower()
            return ""

        for idx, provider_name in enumerate(order):
            if provider_name == "pollinations_auth":
                key = str(getattr(self.settings, "pollinations_api_key", "") or "").strip()
                if not key:
                    attempts.append(
                        {
                            "provider": provider_name,
                            "status": "skip",
                            "reason": "missing_api_key",
                            "provider_try_order": provider_try_order,
                            "provider_failed": provider_name,
                            "fail_reason": "missing_api_key",
                            "next_provider": _next_provider(idx),
                        }
                    )
                    continue
                provider = PollinationsProvider(
                    auth=True,
                    api_key=key,
                    timeout_sec=int(getattr(self.settings, "pollinations_timeout_sec", 35) or 35),
                )
            elif provider_name == "pollinations_anon":
                provider = PollinationsProvider(
                    auth=False,
                    api_key="",
                    timeout_sec=int(getattr(self.settings, "pollinations_timeout_sec", 35) or 35),
                )
            elif provider_name == "gemini":
                if pollinations_service_limited:
                    attempts.append(
                        {
                            "provider": provider_name,
                            "status": "skip",
                            "reason": "pollinations_service_rate_limited",
                        }
                    )
                    continue
                if not bool(getattr(self.settings, "gemini_fallback_enabled", True)):
                    attempts.append({"provider": provider_name, "status": "skip", "reason": "gemini_disabled"})
                    continue
                cap = max(0, int(getattr(self.settings, "gemini_fallback_daily_cap", 1) or 1))
                if int(state.gemini_fallback_used_today or 0) >= cap:
                    attempts.append({"provider": provider_name, "status": "skip", "reason": "gemini_daily_cap"})
                    continue
                provider = GeminiImageProvider(
                    api_key=self.gemini_api_key,
                    model=self.gemini_model,
                    timeout_sec=max(20, int(getattr(self.settings, "pollinations_timeout_sec", 35) or 35)),
                )
            else:
                continue

            try:
                result = provider.generate_image(prompt=prompt, width=width, height=height, seed=seed)
                attempts.append(
                    {
                        "provider": provider_name,
                        "status": "ok",
                        "reason": "",
                        "provider_try_order": provider_try_order,
                        "next_provider": "",
                    }
                )
                return result, "", "", attempts
            except RateLimitError as exc:
                reason = self._classify_rate_limit(provider_name=provider_name, error_text=str(exc))
                next_provider = _next_provider(idx)
                attempts.append(
                    {
                        "provider": provider_name,
                        "status": "fail",
                        "reason": reason,
                        "provider_try_order": provider_try_order,
                        "provider_failed": provider_name,
                        "fail_reason": reason,
                        "next_provider": next_provider,
                    }
                )
                # CASE A: auth 429 must fall through to anon.
                if provider_name == "pollinations_auth":
                    continue
                # CASE B: anon 429 means service rate limit; stop pollinations and do not run Gemini.
                if provider_name == "pollinations_anon":
                    pollinations_service_limited = True
                    break
                return None, "service_rate_limited", str(exc)[:220], attempts
            except BadResponseError as exc:
                reason = self._classify_bad_response(provider_name=provider_name, error_text=str(exc))
                next_provider = _next_provider(idx)
                attempts.append(
                    {
                        "provider": provider_name,
                        "status": "fail",
                        "reason": reason,
                        "provider_try_order": provider_try_order,
                        "provider_failed": provider_name,
                        "fail_reason": reason,
                        "next_provider": next_provider,
                    }
                )
                # CASE A: auth failure 401/403/429/530/5xx/non-image => fall through to anon.
                if provider_name == "pollinations_auth":
                    continue
                # CASE B: anon 429 must stop and must not continue to Gemini.
                if provider_name == "pollinations_anon" and reason == "429":
                    pollinations_service_limited = True
                    break
                continue
            except TemporaryProviderError as exc:
                reason = self._classify_temporary(provider_name=provider_name, error_text=str(exc))
                next_provider = _next_provider(idx)
                attempts.append(
                    {
                        "provider": provider_name,
                        "status": "fail",
                        "reason": reason,
                        "provider_try_order": provider_try_order,
                        "provider_failed": provider_name,
                        "fail_reason": reason,
                        "next_provider": next_provider,
                    }
                )
                # CASE A/B: timeout or 530/5xx should flow to the next provider.
                continue
            except Exception as exc:
                reason = "temporary_server_error"
                attempts.append(
                    {
                        "provider": provider_name,
                        "status": "fail",
                        "reason": reason,
                        "provider_try_order": provider_try_order,
                        "provider_failed": provider_name,
                        "fail_reason": reason,
                        "next_provider": _next_provider(idx),
                        "error": str(exc)[:160],
                    }
                )
                continue

        if pollinations_service_limited:
            return None, "service_rate_limited", "pollinations_rate_limited", attempts
        last_reason = ""
        last_provider = ""
        for row in reversed(attempts):
            if str(row.get("status", "")) == "fail":
                last_reason = str(row.get("reason", "") or "")
                last_provider = str(row.get("provider", "") or "")
                break
        if not last_reason:
            last_reason = "temporary_server_error"
        failure_kind = self._reason_to_failure_kind(provider_name=last_provider, reason=last_reason)
        return None, failure_kind, last_reason, attempts

    def _classify_rate_limit(self, *, provider_name: str, error_text: str) -> str:
        _ = error_text
        if provider_name.startswith("pollinations"):
            return "429"
        return "429"

    def _classify_bad_response(self, *, provider_name: str, error_text: str) -> str:
        text = str(error_text or "").strip().lower()
        if "http_401" in text:
            return "401"
        if "http_403" in text:
            return "403"
        if "http_429" in text:
            return "429"
        if "http_530" in text:
            return "530"
        if "http_5" in text:
            return "5xx"
        if "non_image_response" in text or "image_too_small" in text:
            return "invalid_image_payload"
        return "temporary_server_error"

    def _classify_temporary(self, *, provider_name: str, error_text: str) -> str:
        _ = provider_name
        text = str(error_text or "").strip().lower()
        if "timeout" in text:
            return "timeout"
        if "http_530" in text:
            return "530"
        if "http_5" in text:
            return "5xx"
        return "temporary_server_error"

    def _reason_to_failure_kind(self, *, provider_name: str, reason: str) -> str:
        p = str(provider_name or "").strip().lower()
        r = str(reason or "").strip().lower()
        if r == "429":
            if p == "pollinations_anon":
                return "service_rate_limited"
            if p == "pollinations_auth":
                return "auth_quota_exhausted"
            return "service_rate_limited"
        if r in {"401", "403"}:
            return "auth_failed"
        if r == "invalid_image_payload":
            return "invalid_image_payload"
        if r in {"timeout"}:
            return "timeout"
        if r in {"530", "5xx", "temporary_server_error"}:
            return "temporary_server_error"
        return "temporary_server_error"

    def _store_local_asset(self, *, result: ProviderResult, kind: str) -> tuple[Path, int, int, str, int, list[str]]:
        date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sha1 = hashlib.sha1(result.image_bytes).hexdigest()
        ext = self._mime_to_ext(result.mime)
        raw_dir = (self.root / "assets" / "news_pack_cache" / str(kind) / date_key).resolve()
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / f"{sha1}_raw{ext}"
        raw_path.write_bytes(result.image_bytes)

        max_width = 1280 if str(kind).lower() == "thumb_bg" else 960
        optimized = optimize_for_library(raw_path, raw_dir / sha1, max_width=max_width, max_kb=260)
        width = 0
        height = 0
        byte_size = 0
        quality_flags: list[str] = []
        try:
            with Image.open(optimized) as im:
                width, height = int(im.width), int(im.height)
                byte_size = int(optimized.stat().st_size)
                if width < 640 or height < 360:
                    quality_flags.append("tiny_resolution")
                if not self._image_has_content(im):
                    quality_flags.append("nearly_blank")
        except Exception:
            quality_flags.append("invalid_image_payload")
        try:
            if raw_path.exists() and raw_path != optimized:
                raw_path.unlink(missing_ok=True)
        except Exception:
            pass
        if byte_size <= 0:
            try:
                byte_size = int(optimized.stat().st_size)
            except Exception:
                byte_size = 0
        return optimized, width, height, sha1, byte_size, sorted(set(quality_flags))

    def _image_has_content(self, image: Image.Image) -> bool:
        try:
            rgb = image.convert("RGB")
            sample = rgb.resize((48, 27))
            colors = sample.getcolors(maxcolors=4096) or []
            return len(colors) >= 6
        except Exception:
            return True

    def _upload_to_r2(self, *, local_path: Path, kind: str) -> tuple[str, str]:
        if not bool(getattr(self.settings, "r2_upload_enabled", True)):
            raise RuntimeError("r2_upload_disabled")
        if not self.r2_config.endpoint_url or not self.r2_config.bucket or not self.r2_config.public_base_url:
            raise RuntimeError("r2_missing_config")
        cfg = replace(
            self.r2_config,
            prefix=str(getattr(self.settings, "r2_prefix", "news_pack") or "news_pack").strip() or "news_pack",
        )
        category = str(kind or "inline_bg").strip().lower() or "inline_bg"
        url = r2_upload_file(root=self.root, cfg=cfg, file_path=local_path, category=category)
        if not str(url or "").startswith("https://"):
            raise RuntimeError("r2_invalid_url")
        public_host = self._r2_host(cfg.public_base_url)
        url_host = self._r2_host(url)
        if not public_host or not url_host or public_host != url_host:
            raise RuntimeError("r2_url_invalid_host")
        object_key = str(url).split(cfg.public_base_url.rstrip("/") + "/", 1)[-1]
        return str(url), object_key

    def _r2_host(self, url: str) -> str:
        from urllib.parse import urlparse

        try:
            return (urlparse(str(url or "")).netloc or "").lower()
        except Exception:
            return ""

    def _compute_next_run(self, *, state: NewsPackState, failure_kind: str = "") -> str:
        now = datetime.now(timezone.utc)
        kind = str(failure_kind or "").strip().lower()
        if kind in {"service_rate_limited", "auth_quota_exhausted"}:
            return self.scheduler.compute_backoff(failure_kind="rate_limit", now_utc=now).isoformat()
        if kind in {"invalid_image_payload"}:
            return self.scheduler.compute_backoff(failure_kind="bad_response", now_utc=now).isoformat()
        if kind:
            return self.scheduler.compute_backoff(failure_kind="temporary", now_utc=now).isoformat()
        if str(state.mode or "").strip().lower() == "bootstrap":
            lo = int(getattr(self.settings, "bootstrap_min_interval_minutes", 10) or 10)
            hi = int(getattr(self.settings, "bootstrap_max_interval_minutes", 20) or 20)
            return self.scheduler.compute_bootstrap_next(now_utc=now, min_minutes=lo, max_minutes=hi).isoformat()
        return self.scheduler.compute_next_run(now_utc=now).isoformat()

    def _sync_mode_from_inventory(self, *, state: NewsPackState, ready_thumb: int, ready_inline: int) -> NewsPackState:
        min_thumb = max(1, int(getattr(self.settings, "min_ready_thumb_bg", 20) or 20))
        min_inline = max(1, int(getattr(self.settings, "min_ready_inline_bg", 60) or 60))
        target_thumb = max(min_thumb, int(getattr(self.settings, "target_ready_thumb_bg", 40) or 40))
        target_inline = max(min_inline, int(getattr(self.settings, "target_ready_inline_bg", 120) or 120))
        if ready_thumb < min_thumb or ready_inline < min_inline:
            state.mode = "bootstrap"
        elif state.mode == "bootstrap" and ready_thumb >= target_thumb and ready_inline >= target_inline:
            state.mode = "normal"
        return state

    def _mime_to_ext(self, mime: str) -> str:
        lower = str(mime or "").lower()
        if "jpeg" in lower or "jpg" in lower:
            return ".jpg"
        if "webp" in lower:
            return ".webp"
        return ".png"

    def _pick_kind(self, *, state: NewsPackState, ready_thumb: int, ready_inline: int) -> str:
        min_thumb = max(1, int(getattr(self.settings, "min_ready_thumb_bg", 20) or 20))
        min_inline = max(1, int(getattr(self.settings, "min_ready_inline_bg", 60) or 60))
        target_thumb = max(min_thumb, int(getattr(self.settings, "target_ready_thumb_bg", 40) or 40))
        target_inline = max(min_inline, int(getattr(self.settings, "target_ready_inline_bg", 120) or 120))
        if ready_thumb < min_thumb:
            return "thumb_bg"
        if ready_inline < min_inline:
            return "inline_bg"
        if str(state.mode or "").strip().lower() == "bootstrap":
            if ready_thumb < target_thumb:
                return "thumb_bg"
            return "inline_bg"
        if ready_thumb < target_thumb and (ready_thumb <= ready_inline // 3):
            return "thumb_bg"
        if ready_inline < target_inline:
            return "inline_bg"
        thumb_target = max(0, int(getattr(self.settings, "daily_target_thumb_bg", 4) or 4))
        inline_target = max(0, int(getattr(self.settings, "daily_target_inline_bg", 6) or 6))
        if int(state.generated_thumb_bg or 0) < thumb_target:
            return "thumb_bg"
        if int(state.generated_inline_bg or 0) < inline_target:
            return "inline_bg"
        return "inline_bg"

    def _pick_tags(self) -> list[str]:
        weights = {"security": 3, "policy": 2, "ai": 2, "platform": 1, "mobile": 1, "chips": 1}
        configured = [str(x or "").strip().lower() for x in (getattr(self.settings, "tags", []) or []) if str(x or "").strip()]
        pool = configured or list(weights.keys())
        bag: list[str] = []
        for tag in pool:
            w = max(1, int(weights.get(tag, 1)))
            bag.extend([tag] * w)
        random.shuffle(bag)
        first = bag[0] if bag else "platform"
        second = bag[1] if len(bag) > 1 else random.choice(pool) if pool else "ai"
        if second == first and len(pool) > 1:
            for t in pool:
                if t != first:
                    second = t
                    break
        return [first, second]

    def _normalize_r2_config(self, raw: Any | None) -> R2Config:
        if isinstance(raw, R2Config):
            return raw
        data: dict[str, Any] = {}
        if isinstance(raw, dict):
            data = dict(raw)
        elif raw is not None:
            data = {
                "endpoint_url": getattr(raw, "endpoint_url", ""),
                "bucket": getattr(raw, "bucket", ""),
                "access_key_id": getattr(raw, "access_key_id", ""),
                "secret_access_key": getattr(raw, "secret_access_key", ""),
                "public_base_url": getattr(raw, "public_base_url", ""),
                "prefix": getattr(raw, "prefix", "news_pack"),
                "cache_control": getattr(raw, "cache_control", "public, max-age=31536000, immutable"),
            }
        return R2Config(
            endpoint_url=str(data.get("endpoint_url", "") or "").strip(),
            bucket=str(data.get("bucket", "") or "").strip(),
            access_key_id=str(data.get("access_key_id", "") or "").strip(),
            secret_access_key=str(data.get("secret_access_key", "") or "").strip(),
            public_base_url=str(data.get("public_base_url", "") or "").strip().rstrip("/"),
            prefix=str(data.get("prefix", "news_pack") or "news_pack").strip() or "news_pack",
            cache_control=str(
                data.get("cache_control", "public, max-age=31536000, immutable")
                or "public, max-age=31536000, immutable"
            ).strip(),
        )

    def _result(self, status: str, state: NewsPackState, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {
            "status": str(status or ""),
            "date_et": str(state.date_et or ""),
            "generated_total": int(state.generated_total or 0),
            "generated_thumb_bg": int(state.generated_thumb_bg or 0),
            "generated_inline_bg": int(state.generated_inline_bg or 0),
            "gemini_fallback_used_today": int(state.gemini_fallback_used_today or 0),
            "next_run_at_utc": str(state.next_run_at_utc or ""),
            "consecutive_failures": int(state.consecutive_failures or 0),
            "last_error": str(state.last_error or ""),
            "mode": str(state.mode or "normal"),
            "last_success_provider": str(state.last_success_provider or ""),
            "bootstrap_generated_today": int(state.bootstrap_generated_today or 0),
        }
        payload.update(dict(extra or {}))
        return payload

    def _log(self, payload: dict[str, Any]) -> None:
        row = {"ts_utc": datetime.now(timezone.utc).isoformat()}
        row.update(dict(payload or {}))
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            self._rotate_log_if_needed(self.log_path)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _rotate_log_if_needed(self, path: Path) -> None:
        try:
            if not path.exists():
                return
            if path.stat().st_size < int(self._log_rotate_max_bytes):
                return
            first = path.with_suffix(path.suffix + ".1")
            if first.exists():
                for idx in range(self._log_rotate_keep, 0, -1):
                    src = path.with_suffix(path.suffix + f".{idx}")
                    dst = path.with_suffix(path.suffix + f".{idx + 1}")
                    if not src.exists():
                        continue
                    if idx >= self._log_rotate_keep:
                        src.unlink(missing_ok=True)
                        continue
                    src.replace(dst)
            path.replace(first)
        except Exception:
            return
