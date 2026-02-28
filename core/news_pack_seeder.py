from __future__ import annotations

import hashlib
import json
import random
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
        self.state_store = NewsPackStateStore(root=root, state_path=str(settings.state_path or "storage/state/news_pack_state.json"))
        self.manifest = NewsPackManifest(root=root, manifest_path=str(settings.manifest_path or "storage/state/news_pack_manifest.jsonl"))
        self.prompt_factory = NewsPackPromptFactory(ollama_client=ollama_client)
        self.scheduler = NewsPackScheduler(
            interval_minutes_base=int(getattr(settings, "interval_minutes_base", 150) or 150),
            interval_minutes_jitter=int(getattr(settings, "interval_minutes_jitter", 45) or 45),
        )
        self.log_path = (root / "storage" / "logs" / "news_pack_seeder.jsonl").resolve()
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def seed_one_tick(self, force: bool = False) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        state = self.state_store.load()
        if not bool(getattr(self.settings, "enabled", True)):
            return self._result("disabled", state)
        if not force and not self.state_store.should_run(state, now_utc=now):
            return self._result("not_due", state)

        target_total = max(1, int(getattr(self.settings, "daily_target_total", 10) or 10))
        if int(state.generated_total or 0) >= target_total:
            state.next_run_at_utc = self.scheduler.next_day_start_et(now_utc=now).isoformat()
            state.last_run_at_utc = now.isoformat()
            state.last_error = ""
            self.state_store.save(state)
            return self._result("daily_complete", state)

        kind = self._pick_kind(state)
        tags = self._pick_tags()
        seed = random.randint(1000, 999999)
        prompt_pack = self.prompt_factory.build(
            tags=tags,
            kind=kind,
            seed=seed,
            context={"date_et": state.date_et, "generated_total": int(state.generated_total or 0)},
        )
        prompt_hash = hashlib.sha1(prompt_pack.background_prompt.encode("utf-8", errors="ignore")).hexdigest()
        if self.manifest.has_today_duplicate(prompt_hash=prompt_hash):
            state.next_run_at_utc = self.scheduler.compute_next_run(now_utc=now).isoformat()
            state.last_run_at_utc = now.isoformat()
            state.last_error = "duplicate_prompt_hash_today"
            self.state_store.save(state)
            return self._result("duplicate_skip", state, extra={"kind": kind, "tags": tags})

        result, failure_kind, error_text = self._run_provider_chain(
            prompt=prompt_pack.background_prompt,
            kind=kind,
            seed=seed,
            state=state,
        )
        if result is None:
            state.consecutive_failures = int(state.consecutive_failures or 0) + 1
            state.last_error = str(error_text or "seed_failed")[:220]
            state.last_run_at_utc = now.isoformat()
            if int(state.consecutive_failures or 0) >= max(1, int(getattr(self.settings, "max_consecutive_failures", 5) or 5)):
                next_run = self.scheduler.next_day_start_et(now_utc=now)
            else:
                next_run = self.scheduler.compute_backoff(failure_kind=failure_kind, now_utc=now)
            state.next_run_at_utc = next_run.isoformat()
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
                    "status": "failed",
                    "used_at": "",
                    "used_by": "",
                    "error": str(error_text or "")[:220],
                }
            )
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
            return self._result("failed", state, extra={"kind": kind, "reason": state.last_error})

        try:
            local_path, width, height, sha1 = self._store_local_asset(result=result, kind=kind)
            if self.manifest.has_today_duplicate(sha1=sha1):
                state.next_run_at_utc = self.scheduler.compute_next_run(now_utc=now).isoformat()
                state.last_run_at_utc = now.isoformat()
                state.last_error = "duplicate_image_sha1_today"
                self.state_store.save(state)
                return self._result("duplicate_skip", state, extra={"kind": kind, "tags": tags})
            r2_url, r2_key = self._upload_to_r2(local_path=local_path, kind=kind)
        except Exception as exc:
            state.consecutive_failures = int(state.consecutive_failures or 0) + 1
            state.last_error = str(exc)[:220]
            state.last_run_at_utc = now.isoformat()
            state.next_run_at_utc = self.scheduler.compute_backoff(failure_kind="temporary", now_utc=now).isoformat()
            self.state_store.save(state)
            self._log(
                {
                    "event": "seed_store_or_upload_fail",
                    "kind": kind,
                    "error": str(exc)[:220],
                    "next_run_at_utc": state.next_run_at_utc,
                }
            )
            return self._result("failed", state, extra={"kind": kind, "reason": state.last_error})

        state.consecutive_failures = 0
        state.last_error = ""
        state.last_run_at_utc = now.isoformat()
        state.next_run_at_utc = self.scheduler.compute_next_run(now_utc=now).isoformat()
        state.generated_total = int(state.generated_total or 0) + 1
        if kind == "thumb_bg":
            state.generated_thumb_bg = int(state.generated_thumb_bg or 0) + 1
        else:
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
                "status": "ready",
                "used_at": "",
                "used_by": "",
                "hook_candidates": list(prompt_pack.hook_candidates or []),
                "style_tags": list(prompt_pack.style_tags or []),
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
    ) -> tuple[ProviderResult | None, str, str]:
        width, height = (1280, 720) if str(kind).lower() == "thumb_bg" else (960, 540)
        order = [str(x or "").strip().lower() for x in (getattr(self.settings, "provider_order", []) or []) if str(x or "").strip()]
        if not order:
            order = ["pollinations_auth", "pollinations_anon", "gemini"]
        pollinations_429 = False
        errors: list[str] = []

        for provider_name in order:
            if provider_name == "pollinations_auth":
                key = str(getattr(self.settings, "pollinations_api_key", "") or "").strip()
                if not key:
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
                if pollinations_429:
                    continue
                if not bool(getattr(self.settings, "gemini_fallback_enabled", True)):
                    continue
                cap = max(0, int(getattr(self.settings, "gemini_fallback_daily_cap", 1) or 1))
                if int(state.gemini_fallback_used_today or 0) >= cap:
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
                return result, "", ""
            except RateLimitError as exc:
                if provider_name.startswith("pollinations"):
                    pollinations_429 = True
                    errors.append(f"{provider_name}:{str(exc)}")
                    break
                errors.append(f"{provider_name}:{str(exc)}")
            except BadResponseError as exc:
                errors.append(f"{provider_name}:{str(exc)}")
                continue
            except TemporaryProviderError as exc:
                errors.append(f"{provider_name}:{str(exc)}")
                continue
            except Exception as exc:
                errors.append(f"{provider_name}:{str(exc)[:120]}")
                continue

        if pollinations_429:
            return None, "rate_limit", ";".join(errors)[:220]
        if any("non_image_response" in e or "image_too_small" in e for e in errors):
            return None, "bad_response", ";".join(errors)[:220]
        return None, "temporary", ";".join(errors)[:220]

    def _store_local_asset(self, *, result: ProviderResult, kind: str) -> tuple[Path, int, int, str]:
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
        try:
            from PIL import Image

            with Image.open(optimized) as im:
                width, height = int(im.width), int(im.height)
        except Exception:
            pass
        try:
            if raw_path.exists() and raw_path != optimized:
                raw_path.unlink(missing_ok=True)
        except Exception:
            pass
        return optimized, width, height, sha1

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

    def _mime_to_ext(self, mime: str) -> str:
        lower = str(mime or "").lower()
        if "jpeg" in lower or "jpg" in lower:
            return ".jpg"
        if "webp" in lower:
            return ".webp"
        return ".png"

    def _pick_kind(self, state: NewsPackState) -> str:
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
        data = {}
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
            cache_control=str(data.get("cache_control", "public, max-age=31536000, immutable") or "public, max-age=31536000, immutable").strip(),
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
        }
        payload.update(dict(extra or {}))
        return payload

    def _log(self, payload: dict[str, Any]) -> None:
        row = {"ts_utc": datetime.now(timezone.utc).isoformat()}
        row.update(dict(payload or {}))
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

