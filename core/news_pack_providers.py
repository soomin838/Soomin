from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import requests


class RateLimitError(RuntimeError):
    pass


class TemporaryProviderError(RuntimeError):
    pass


class BadResponseError(RuntimeError):
    pass


@dataclass
class ProviderResult:
    image_bytes: bytes
    mime: str
    provider: str
    meta: dict[str, Any]


class PollinationsProvider:
    def __init__(self, *, auth: bool, api_key: str = "", timeout_sec: int = 35) -> None:
        self.auth = bool(auth)
        self.api_key = str(api_key or "").strip()
        self.timeout_sec = max(10, int(timeout_sec or 35))
        self.base_url = "https://image.pollinations.ai/prompt/"

    @property
    def name(self) -> str:
        return "pollinations_auth" if self.auth else "pollinations_anon"

    def generate_image(self, *, prompt: str, width: int = 1280, height: int = 720, seed: int | None = None) -> ProviderResult:
        clean_prompt = re.sub(r"\s+", " ", str(prompt or "").strip())
        if not clean_prompt:
            raise BadResponseError("empty_prompt")
        url = self.base_url + quote(clean_prompt, safe="")
        params: dict[str, Any] = {
            "width": max(256, int(width or 1280)),
            "height": max(256, int(height or 720)),
            "nologo": "true",
            "nofeed": "true",
        }
        if seed is not None:
            params["seed"] = int(seed)
        headers = {"Accept": "image/*"}
        if self.auth and self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
            headers["x-api-key"] = self.api_key
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=self.timeout_sec)
        except requests.Timeout as exc:
            raise TemporaryProviderError("timeout") from exc
        except Exception as exc:
            raise TemporaryProviderError(str(exc)[:180] or "request_error") from exc
        if resp.status_code == 429:
            raise RateLimitError("rate_limit_429")
        if resp.status_code >= 500:
            raise TemporaryProviderError(f"http_{resp.status_code}")
        if resp.status_code >= 400:
            raise BadResponseError(f"http_{resp.status_code}")
        content_type = str(resp.headers.get("content-type", "") or "").lower()
        if "image/" not in content_type:
            preview = (resp.text or "")[:180]
            raise BadResponseError(f"non_image_response:{preview}")
        data = bytes(resp.content or b"")
        if len(data) < 5 * 1024:
            raise BadResponseError(f"image_too_small:{len(data)}")
        return ProviderResult(
            image_bytes=data,
            mime=content_type.split(";")[0].strip() or "image/png",
            provider=self.name,
            meta={"status_code": int(resp.status_code), "bytes": len(data)},
        )


class AirforceImageProvider:
    def __init__(
        self,
        *,
        api_key: str,
        model: str = "imagen-4",
        base_url: str = "https://api.airforce",
        timeout_sec: int = 45,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.model = str(model or "imagen-4").strip()
        self.base_url = str(base_url or "https://api.airforce").strip().rstrip("/")
        self.timeout_sec = max(15, int(timeout_sec or 45))

    @property
    def name(self) -> str:
        return "airforce_imagen4"

    def generate_image(self, *, prompt: str, width: int = 1280, height: int = 720, seed: int | None = None) -> ProviderResult:
        if not self.api_key:
            raise TemporaryProviderError("missing_airforce_key")
        clean_prompt = re.sub(r"\s+", " ", str(prompt or "").strip())
        if not clean_prompt:
            raise BadResponseError("empty_prompt")
        endpoint = f"{self.base_url}/v1/images/generations"
        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": clean_prompt,
            # api.airforce imagen endpoints are most stable with square sizes.
            "size": "1024x1024",
            "n": 1,
            "response_format": "b64_json",
        }
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
        }
        try:
            resp = requests.post(endpoint, headers=headers, json=payload, timeout=self.timeout_sec)
        except requests.Timeout as exc:
            raise TemporaryProviderError("airforce_timeout") from exc
        except Exception as exc:
            raise TemporaryProviderError(str(exc)[:180] or "airforce_request_error") from exc

        try:
            data = resp.json() or {}
        except json.JSONDecodeError as exc:
            data = {}
            if resp.status_code >= 400:
                raise BadResponseError(f"airforce_http_{resp.status_code}") from exc
            raise BadResponseError("airforce_non_json") from exc

        error_obj = data.get("error") if isinstance(data, dict) else None
        if isinstance(error_obj, dict):
            code = str(error_obj.get("code", "") or "").strip().lower()
            message = str(error_obj.get("message", "") or "").strip().lower()
            if resp.status_code == 429 or code == "429" or "rate limit" in message:
                raise RateLimitError("airforce_rate_limit_429")
            if resp.status_code in {401, 403} or code in {"401", "403"} or "invalid api key" in message:
                forced_code = 401 if "invalid api key" in message else int(resp.status_code or 401)
                raise BadResponseError(f"airforce_http_{forced_code}")
            if "model not found" in message:
                raise BadResponseError("airforce_model_not_found")
            if resp.status_code >= 500:
                raise TemporaryProviderError(f"airforce_http_{resp.status_code}")
            raise BadResponseError(f"airforce_api_error:{message[:120]}")

        if resp.status_code == 429:
            raise RateLimitError("airforce_rate_limit_429")
        if resp.status_code >= 500:
            raise TemporaryProviderError(f"airforce_http_{resp.status_code}")
        if resp.status_code >= 400:
            raise BadResponseError(f"airforce_http_{resp.status_code}")

        rows = data.get("data", []) if isinstance(data, dict) else []
        if not isinstance(rows, list) or not rows:
            raise BadResponseError("airforce_empty_image_payload")

        for row in rows:
            if not isinstance(row, dict):
                continue
            encoded = str(row.get("b64_json", "") or "").strip()
            if encoded:
                try:
                    raw = base64.b64decode(encoded)
                    if len(raw) >= 5 * 1024:
                        return ProviderResult(
                            image_bytes=raw,
                            mime="image/jpeg",
                            provider=self.name,
                            meta={"status_code": int(resp.status_code), "bytes": len(raw), "model": self.model},
                        )
                except Exception:
                    pass
            image_url = str(row.get("url", "") or "").strip()
            if image_url:
                try:
                    img_resp = requests.get(image_url, headers={"Accept": "image/*"}, timeout=self.timeout_sec)
                    if img_resp.status_code >= 400:
                        continue
                    content_type = str(img_resp.headers.get("content-type", "") or "").lower()
                    if "image/" not in content_type:
                        continue
                    raw = bytes(img_resp.content or b"")
                    if len(raw) < 5 * 1024:
                        continue
                    return ProviderResult(
                        image_bytes=raw,
                        mime=content_type.split(";")[0].strip() or "image/png",
                        provider=self.name,
                        meta={"status_code": int(resp.status_code), "bytes": len(raw), "model": self.model},
                    )
                except Exception:
                    continue

        raise BadResponseError("airforce_empty_image_payload")


class GeminiImageProvider:
    def __init__(self, *, api_key: str, model: str = "gemini-2.0-flash", timeout_sec: int = 45) -> None:
        self.api_key = str(api_key or "").strip()
        self.model = str(model or "gemini-2.0-flash").strip()
        self.timeout_sec = max(15, int(timeout_sec or 45))

    @property
    def name(self) -> str:
        return "gemini"

    def generate_image(self, *, prompt: str, width: int = 1280, height: int = 720, seed: int | None = None) -> ProviderResult:
        if not self.api_key:
            raise TemporaryProviderError("missing_gemini_key")
        clean_prompt = re.sub(r"\s+", " ", str(prompt or "").strip())
        if not clean_prompt:
            raise BadResponseError("empty_prompt")
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": (
                                f"{clean_prompt}. "
                                f"Target size {int(width)}x{int(height)}. "
                                "No text, no letters, no numbers, no logo, no watermark."
                            )
                        }
                    ]
                }
            ],
            "generationConfig": {"responseModalities": ["IMAGE"]},
        }
        try:
            resp = requests.post(endpoint, params={"key": self.api_key}, json=payload, timeout=self.timeout_sec)
        except requests.Timeout as exc:
            raise TemporaryProviderError("gemini_timeout") from exc
        except Exception as exc:
            raise TemporaryProviderError(str(exc)[:180] or "gemini_request_error") from exc

        if resp.status_code == 429:
            raise RateLimitError("gemini_rate_limit_429")
        if resp.status_code >= 500:
            raise TemporaryProviderError(f"gemini_http_{resp.status_code}")
        if resp.status_code >= 400:
            raise BadResponseError(f"gemini_http_{resp.status_code}")
        try:
            data = resp.json() or {}
        except json.JSONDecodeError as exc:
            raise BadResponseError("gemini_non_json") from exc

        raw, mime = self._extract_image_payload(data)
        if not raw:
            raise BadResponseError("gemini_empty_image_payload")
        if len(raw) < 5 * 1024:
            raise BadResponseError(f"gemini_image_too_small:{len(raw)}")
        return ProviderResult(
            image_bytes=raw,
            mime=mime,
            provider="gemini",
            meta={"status_code": int(resp.status_code), "bytes": len(raw)},
        )

    def _extract_image_payload(self, data: dict[str, Any]) -> tuple[bytes | None, str]:
        for candidate in (data.get("candidates", []) or []):
            content = candidate.get("content", {}) if isinstance(candidate, dict) else {}
            parts = content.get("parts", []) if isinstance(content, dict) else []
            for part in parts:
                if not isinstance(part, dict):
                    continue
                inline = part.get("inline_data") or part.get("inlineData") or {}
                if not isinstance(inline, dict):
                    continue
                encoded = str(inline.get("data", "") or "").strip()
                if not encoded:
                    continue
                mime = str(inline.get("mime_type", "") or inline.get("mimeType", "") or "image/png")
                try:
                    return base64.b64decode(encoded), mime
                except Exception:
                    continue
        return None, "image/png"

