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


def validate_runtime_settings(root: Path, settings: AppSettings) -> list[str]:
    errors: list[str] = []

    if not _is_valid_blogger_blog_id(settings.blogger.blog_id or ""):
        errors.append("Blogger Blog ID 형식이 올바르지 않습니다. 숫자만 입력하세요.")

    token_path = _resolve(root, settings.blogger.credentials_path)
    if not token_path.exists():
        errors.append("blogger_token.json 파일을 찾을 수 없습니다.")
    else:
        try:
            payload = json.loads(token_path.read_text(encoding="utf-8"))
            required = ["client_id", "client_secret", "refresh_token", "token_uri"]
            missing = [k for k in required if not payload.get(k)]
            if missing:
                errors.append(f"blogger_token.json 필수 항목 누락: {', '.join(missing)}")
        except Exception:
            errors.append("blogger_token.json 파일이 유효한 JSON이 아닙니다.")

    gemini_key = (settings.gemini.api_key or "").strip()
    gemini_required = (not settings.budget.free_mode) or settings.visual.enable_gemini_image_generation
    if gemini_required:
        if not gemini_key:
            errors.append("Gemini API Key가 필요합니다. (무료모드 해제 또는 이미지 생성 사용 중)")
        elif not _is_valid_gemini_key(gemini_key):
            errors.append("Gemini API Key 형식이 올바르지 않습니다.")

    target_images = int(settings.visual.target_images_per_post or 0)
    if target_images < 1:
        errors.append("포스트당 이미지 수는 1 이상이어야 합니다.")
    if target_images > 20:
        errors.append("포스트당 이미지 수는 20 이하를 권장합니다.")

    return errors
