from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .image_optimizer import optimize_for_library
from .image_prompts import month_primary_category
from .pollinations_client import generate_image


UTC = ZoneInfo("UTC")
ET = ZoneInfo("America/New_York")

STATE_REL = Path("storage/state/daily_vector_state.json")
LIB_ROOT = Path("assets/library")


def _state_path(root: Path) -> Path:
    return (root / STATE_REL).resolve()


def _load_state(root: Path) -> dict:
    path = _state_path(root)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(root: Path, data: dict) -> None:
    path = _state_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_iso_dt(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _normalize_title(text: str) -> str:
    t = re.sub(r"\s+", " ", str(text or "")).strip()
    return t[:160]


def _extract_yesterday_titles_et(rows: list[dict[str, Any]], now_utc: datetime) -> list[str]:
    now_et = now_utc.astimezone(ET)
    target_date = (now_et - timedelta(days=1)).date()
    out: list[str] = []
    seen: set[str] = set()
    for row in (rows or []):
        title = _normalize_title(str((row or {}).get("title", "") or ""))
        if not title:
            continue
        published_raw = str((row or {}).get("published", "") or "")
        published_dt = _parse_iso_dt(published_raw)
        if published_dt is None:
            continue
        if published_dt.astimezone(ET).date() != target_date:
            continue
        key = title.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(title)
        if len(out) >= 12:
            break
    return out


def _default_dynamic_prompt(category: str, titles: list[str]) -> tuple[str, str]:
    normalized_titles = [_normalize_title(x) for x in (titles or []) if _normalize_title(x)]
    if normalized_titles:
        topics = "; ".join(normalized_titles[:6])
        scene = (
            f"Create a flat vector illustration for {category} software troubleshooting topics inspired by: {topics}. "
            "Show abstract settings panels, checklist flow, and neutral warning icons."
        )
    else:
        scene = (
            f"Create a flat vector illustration for {category} software troubleshooting. "
            "Show abstract settings panels, checklist flow, and neutral warning icons."
        )
    prompt = (
        "flat vector illustration, soft pastel palette, rounded shapes, minimal shading, clean modern ui-inspired design, "
        "subtle gradient background, centered composition. "
        + scene
        + " Software troubleshooting context only."
    )
    negative = (
        "no text, no letters, no numbers, no logos, no watermark, no brand names, no UI text, "
        "no fire, no smoke, no explosion, no hazard, no injury, no physical damage, no broken hardware, no cracked screen"
    )
    return prompt, negative


def _build_prompt_with_ollama(
    *,
    category: str,
    titles: list[str],
    ollama_manager: Any = None,
    ollama_client: Any = None,
) -> tuple[str, str]:
    fallback_prompt, fallback_negative = _default_dynamic_prompt(category, titles)
    if ollama_manager is None or ollama_client is None:
        return fallback_prompt, fallback_negative
    try:
        server_ok, _ = ollama_manager.ensure_server_running()
        if not server_ok:
            return fallback_prompt, fallback_negative
        if hasattr(ollama_manager, "ensure_model_available"):
            model_name = str(getattr(ollama_client, "model", "qwen2.5:3b") or "qwen2.5:3b").strip()
            if not ollama_manager.ensure_model_available(model_name):
                return fallback_prompt, fallback_negative
        system_prompt = (
            "You create image-generation prompts for a software troubleshooting blog. "
            "Return ONLY JSON with keys: prompt, negative. "
            "Rules: flat vector style, pastel, rounded shapes, no text in image, no logo, no watermark, "
            "no fire, no smoke, no explosion, no hazard, no injury, no physical damage."
        )
        payload = {
            "category": category,
            "yesterday_titles": list(titles or [])[:12],
            "target_style": "flat vector, modern, troubleshooting diagram mood",
            "constraints": {
                "software_only": True,
                "no_text": True,
                "no_hazards": True,
            },
        }
        data = ollama_client.generate_json(
            system_prompt=system_prompt,
            user_payload=payload,
            purpose="daily_vector_prompt",
        )
        prompt = re.sub(r"\s+", " ", str((data or {}).get("prompt", "") or "")).strip()
        negative = re.sub(r"\s+", " ", str((data or {}).get("negative", "") or "")).strip()
        if not prompt:
            return fallback_prompt, fallback_negative
        if "flat vector" not in prompt.lower():
            prompt = f"flat vector illustration, {prompt}"
        if not negative:
            negative = fallback_negative
        return prompt, negative
    except Exception:
        return fallback_prompt, fallback_negative


def run_daily_vector_if_needed(
    *,
    root: Path,
    rotation_order: list[str] | None,
    titles_provider: Callable[[], list[dict[str, Any]]] | None = None,
    ollama_manager: Any = None,
    ollama_client: Any = None,
) -> Path | None:
    """
    root: project root (same as AgentWorkflow.root)
    rotation_order: topics.rotation_order
    """
    root = Path(root).resolve()
    today = datetime.now(UTC).date().isoformat()
    state = _load_state(root)
    if state.get("last_attempt_date_utc") == today:
        return None
    if state.get("last_date_utc") == today:
        return None

    state["last_attempt_date_utc"] = today
    _save_state(root, state)

    month_cat = month_primary_category(rotation_order=rotation_order, month=datetime.now(ET).month)
    recent_rows: list[dict[str, Any]] = []
    if callable(titles_provider):
        try:
            pulled = titles_provider() or []
            if isinstance(pulled, list):
                recent_rows = [x for x in pulled if isinstance(x, dict)]
        except Exception:
            recent_rows = []
    yesterday_titles = _extract_yesterday_titles_et(recent_rows, datetime.now(UTC))
    prompt, negative = _build_prompt_with_ollama(
        category=month_cat,
        titles=yesterday_titles,
        ollama_manager=ollama_manager,
        ollama_client=ollama_client,
    )

    temp_dir = (root / "storage" / "temp_images").resolve()
    try:
        raw = generate_image(prompt, negative, out_dir=temp_dir)
    except Exception as exc:
        state["last_error"] = str(exc)[:240]
        _save_state(root, state)
        return None

    out_dir = (root / LIB_ROOT / month_cat).resolve()
    name = f"vec_{today}_{uuid.uuid4().hex[:8]}"
    try:
        saved = optimize_for_library(raw, out_dir / name, max_width=1200, max_kb=220)
    except Exception as exc:
        state["last_error"] = str(exc)[:240]
        _save_state(root, state)
        return None

    state["last_date_utc"] = today
    state["last_category"] = month_cat
    state["yesterday_title_count"] = int(len(yesterday_titles))
    state["last_saved_path"] = str(saved)
    state["last_error"] = ""
    _save_state(root, state)
    return saved
