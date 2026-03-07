from __future__ import annotations

import json
import re
from pathlib import Path

from .settings import AppSettings


def _is_valid_gemini_key(value: str) -> bool:
    return bool(re.fullmatch(r"AIza[0-9A-Za-z_-]{20,}", value.strip()))


def _is_valid_blogger_blog_id(value: str) -> bool:
    return bool(re.fullmatch(r"\d{8,30}", value.strip()))


def _resolve(root: Path, value: str) -> Path:
    p = Path(value)
    return p if p.is_absolute() else (root / p)


def validate_secrets(settings: AppSettings) -> list[str]:
    """
    Validate runtime secrets. Returns a list of issues to be handled as HOLD.
    No exceptions should escape this validator.
    """
    issues: list[str] = []

    backend = str(getattr(settings.publish, "image_hosting_backend", "") or "").strip().lower()
    r2 = getattr(settings.publish, "r2", None)
    if backend == "r2":
        endpoint_url = str(getattr(r2, "endpoint_url", "") or "").strip()
        bucket = str(getattr(r2, "bucket", "") or "").strip()
        access_key_id = str(getattr(r2, "access_key_id", "") or "").strip()
        secret_access_key = str(getattr(r2, "secret_access_key", "") or "").strip()
        public_base_url = str(getattr(r2, "public_base_url", "") or "").strip()
        if not endpoint_url:
            issues.append("missing_r2_endpoint_url")
        if not bucket:
            issues.append("missing_r2_bucket")
        if not access_key_id:
            issues.append("missing_r2_access_key_id")
        if not secret_access_key:
            issues.append("missing_r2_secret_access_key")
        if not public_base_url:
            issues.append("missing_r2_public_base_url")

    gemini_key = str(getattr(settings.gemini, "api_key", "") or "").strip()
    gemini_required = not bool(getattr(settings.budget, "free_mode", True))
    if gemini_required:
        if not gemini_key:
            issues.append("missing_gemini_api_key")
        elif not _is_valid_gemini_key(gemini_key):
            issues.append("invalid_gemini_api_key_format")

    return issues


def validate_runtime_settings(root: Path, settings: AppSettings) -> list[str]:
    errors: list[str] = []

    if not _is_valid_blogger_blog_id(settings.blogger.blog_id or ""):
        errors.append("Blogger blog_id is invalid. Use numeric blog id only.")

    token_path = _resolve(root, settings.blogger.credentials_path)
    if not token_path.exists():
        errors.append("Missing blogger token file: blogger_token.json")
    else:
        try:
            payload = json.loads(token_path.read_text(encoding="utf-8"))
            required = ["client_id", "client_secret", "refresh_token", "token_uri"]
            missing = [k for k in required if not payload.get(k)]
            if missing:
                errors.append(f"blogger_token.json missing required keys: {', '.join(missing)}")
        except Exception:
            errors.append("blogger_token.json is not valid JSON.")

    gemini_key = (settings.gemini.api_key or "").strip()
    gemini_required = (not settings.budget.free_mode) or settings.visual.enable_gemini_image_generation
    if gemini_required:
        if not gemini_key:
            errors.append("Gemini API key is required in current mode.")
        elif not _is_valid_gemini_key(gemini_key):
            errors.append("Gemini API key format is invalid.")

    target_images = int(settings.visual.target_images_per_post or 0)
    if target_images < 1:
        errors.append("target_images_per_post must be >= 1")
    if target_images > 20:
        errors.append("target_images_per_post should be <= 20")

    errors.extend([f"secret_preflight:{x}" for x in validate_secrets(settings)])
    return errors


def validate_runtime_warnings(root: Path, settings: AppSettings) -> list[str]:
    warnings: list[str] = []

    if bool(getattr(getattr(settings, "worldmonitor", None), "enabled", True)):
        worldmonitor_key = str(getattr(getattr(settings, "worldmonitor", None), "api_key", "") or "").strip()
        if not worldmonitor_key:
            warnings.append("WorldMonitor API key is missing. RSS fallback mode will remain active.")

    integrations = getattr(settings, "integrations", None)
    if bool(getattr(integrations, "search_console_enabled", False)):
        site_url = str(getattr(integrations, "search_console_site_url", "") or "").strip()
        if not site_url:
            warnings.append("Search Console site URL is empty. Search learning will stay disabled.")
        token_path = _resolve(root, settings.blogger.credentials_path)
        if token_path.exists():
            try:
                payload = json.loads(token_path.read_text(encoding="utf-8"))
                scopes = payload.get("scopes", [])
                if isinstance(scopes, list) and scopes:
                    scope_values = {str(item or "").strip().lower() for item in scopes if str(item or "").strip()}
                    if (
                        "https://www.googleapis.com/auth/webmasters.readonly" not in scope_values
                        and "https://www.googleapis.com/auth/webmasters" not in scope_values
                    ):
                        warnings.append(
                            "Search Console OAuth scope is missing. Reconnect Google login with webmasters.readonly."
                        )
            except Exception:
                warnings.append("Search Console OAuth scopes could not be verified from blogger token.")

    return warnings

