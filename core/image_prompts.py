from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

@dataclass(frozen=True)
class PromptPack:
    category: str
    prompt: str
    negative: str

NEGATIVE_DEFAULT = (
    "no text, no letters, no numbers, no logos, no watermark, "
    "no brand names, no UI text, "
    "no fire, no smoke, no explosion, no hazard, no injury, "
    "no physical damage, no broken hardware, no cracked screen"
)

# 뉴스 컨셉에 맞는 다양한 스타일 정의
STYLE_VARIANTS = [
    "editorial digital illustration, high contrast, vibrant accents, professional journalism style",
    "cinematic tech photography, shallow depth of field, natural lighting, premium product showcase",
    "modern isometric 3D render, clean gradients, soft shadows, futuristic atmosphere",
    "minimalist conceptual art, metaphorical representation, sleek corporate aesthetics",
    "high-quality architectural tech office setting, clean lines, professional environment"
]

def get_style_by_context(context: str) -> str:
    # 키워드를 기반으로 일관되지만 다양한 스타일 선택
    index = int(hashlib.md5(context.encode()).hexdigest(), 16) % len(STYLE_VARIANTS)
    return STYLE_VARIANTS[index]

def month_primary_category(rotation_order: list[str] | None = None, month: int | None = None) -> str:
    order = [x.strip().lower() for x in (rotation_order or ["windows", "mac", "iphone", "galaxy"]) if x.strip()]
    if not order:
        order = ["windows", "mac", "iphone", "galaxy"]
    m = month if month is not None else datetime.now(ET).month
    return order[(m - 1) % len(order)]

def vector_prompt_for_category(category: str, context_keyword: str = "") -> PromptPack:
    """
    기존의 기하학적 벡터 스타일을 탈피하고, 뉴스 본문 맥락에 맞는 고품질 프롬프트를 생성합니다.
    """
    c = (category or "generic").strip().lower()
    style = get_style_by_context(context_keyword or c)
    
    # 본문 키워드가 있으면 이를 적극 반영, 없으면 카테고리별 뉴셜 비주얼 생성
    target = context_keyword if context_keyword else f"latest {c} technology update"
    
    prompt_base = f"{style}. Scene: A high-end visual representing {target}."
    
    if c == "windows":
        prompt = f"{prompt_base} Featuring a sleek modern workstation, subtle software interface elements, and a sense of innovation."
    elif c == "mac":
        prompt = f"{prompt_base} Featuring a premium laptop in a professional setting, clean aluminum textures, and elegant design."
    elif c == "iphone":
        prompt = f"{prompt_base} Featuring a cutting-edge smartphone display, vivid colors, and fluid motion."
    elif c == "galaxy":
        prompt = f"{prompt_base} Featuring a top-tier Android device, innovative mobile tech, and sophisticated visuals."
    else:
        prompt = f"{prompt_base} A generic but premium device representing modern technological advancement."

    # '해결' 관련 문구가 아닌 '분석/뉴스' 관련 문구 추가
    prompt += " The image should feel like a cover for a leading tech news magazine. Dynamic and professional."
    
    return PromptPack(c, prompt, NEGATIVE_DEFAULT)
