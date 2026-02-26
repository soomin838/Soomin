from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .image_optimizer import optimize_for_library
from .image_prompts import month_primary_category, vector_prompt_for_category
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


def run_daily_vector_if_needed(*, root: Path, rotation_order: list[str] | None) -> Path | None:
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
    pack = vector_prompt_for_category(month_cat)

    temp_dir = (root / "storage" / "temp_images").resolve()
    try:
        raw = generate_image(pack.prompt, pack.negative, out_dir=temp_dir)
    except Exception as exc:
        state["last_error"] = str(exc)[:240]
        _save_state(root, state)
        return None

    out_dir = (root / LIB_ROOT / pack.category).resolve()
    name = f"vec_{today}_{uuid.uuid4().hex[:8]}"
    try:
        saved = optimize_for_library(raw, out_dir / name, max_width=1200, max_kb=220)
    except Exception as exc:
        state["last_error"] = str(exc)[:240]
        _save_state(root, state)
        return None

    state["last_date_utc"] = today
    state["last_category"] = pack.category
    state["last_saved_path"] = str(saved)
    state["last_error"] = ""
    _save_state(root, state)
    return saved
