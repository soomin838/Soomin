from __future__ import annotations

import json
import random
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable


TITLE_PATTERNS: tuple[tuple[str, str], ...] = (
    ("numeric", "5 things to know about {topic} ({year})"),
    ("numeric", "{topic}: 3 practical takeaways for users"),
    ("question", "What does {topic} mean for normal users right now?"),
    ("question", "Is {topic} a real shift or a short-term rollout?"),
    ("analysis", "{topic} explained: what changed and why it matters"),
    ("analysis", "{topic} update: impact, risks, and what to watch next"),
)

_BANNED_REPLACEMENTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bguaranteed\b", re.IGNORECASE), ""),
    (re.compile(r"\bproven\b", re.IGNORECASE), ""),
    (re.compile(r"\bmust\b", re.IGNORECASE), "should"),
    (re.compile(r"\bscam\b", re.IGNORECASE), "risk alert"),
)

_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "these",
    "those",
    "about",
    "into",
    "over",
    "under",
    "after",
    "before",
    "when",
    "your",
    "their",
    "update",
    "news",
}

_FACET_HINT = {
    "impact": "for daily workflows",
    "timeline": "over the next few weeks",
    "official": "from confirmed updates",
    "risk": "before risk grows",
    "market": "across the market",
    "user_angle": "for everyday users",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _setting(settings: dict[str, Any] | Any, key: str, default: Any) -> Any:
    if isinstance(settings, dict):
        return settings.get(key, default)
    return getattr(settings, key, default)


def normalize_title(text: str) -> str:
    title = str(text or "")
    for pattern, repl in _BANNED_REPLACEMENTS:
        title = pattern.sub(repl, title)
    title = re.sub(r"\s+", " ", title).strip()
    title = re.sub(r"\.{4,}", "...", title)
    title = re.sub(r"(?<!\.)\.\.(?!\.)", ".", title)
    title = re.sub(r"([!?,:;])\1{1,}", r"\1", title)
    title = re.sub(r"\s+([!?.,:;])", r"\1", title)
    title = re.sub(r"\(\s*\)", "", title)
    title = re.sub(r"\s{2,}", " ", title).strip(" -,:;")
    return title


def clamp_title_length(title: str, min_chars: int, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", str(title or "").strip())
    min_chars = max(10, _safe_int(min_chars, 45))
    max_chars = max(min_chars + 5, _safe_int(max_chars, 70))
    if not text:
        text = "Tech update: what changed and why it matters"
    if len(text) > max_chars:
        cut_target = max(8, max_chars - 3)
        trimmed = text[:cut_target].rstrip(" -,:;")
        if " " in trimmed:
            candidate = trimmed.rsplit(" ", 1)[0].strip(" -,:;")
            if candidate and len(candidate) >= max(8, min_chars - 6):
                trimmed = candidate
        trimmed = re.sub(r"\b(and|or|for|with|to|of|in)\s*$", "", trimmed, flags=re.IGNORECASE).strip(" -,:;")
        text = trimmed + "..."
    if len(text) < min_chars:
        suffixes = [" - what to know", " right now", " for users"]
        for suffix in suffixes:
            if len(text) >= min_chars:
                break
            probe = f"{text}{suffix}"
            if len(probe) <= max_chars:
                text = probe
        while len(text) < min_chars:
            probe = f"{text} update"
            if len(probe) > max_chars:
                break
            text = probe
    if len(text) > max_chars:
        text = text[:max_chars].rstrip(" -,:;")
    return normalize_title(text)


def extract_topic_tokens(title: str, max_tokens: int = 4) -> list[str]:
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9'-]+", str(title or ""))
    out: list[str] = []
    seen: set[str] = set()
    limit = max(1, _safe_int(max_tokens, 4))
    for token in tokens:
        cleaned = token.strip("-'").lower()
        if len(cleaned) < 3:
            continue
        if cleaned in _STOPWORDS:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
        if len(out) >= limit:
            break
    return out


def _topic_phrase(base_title: str, category: str) -> str:
    tokens = extract_topic_tokens(base_title, max_tokens=4)
    if tokens:
        phrase = " ".join(tokens)
    else:
        fallback = re.sub(r"[^a-z0-9 ]+", " ", str(category or "").lower()).strip()
        phrase = fallback or "this platform update"
    phrase = re.sub(r"\s+", " ", phrase).strip()
    if not phrase:
        phrase = "this platform update"
    return phrase


def _apply_facet_hint(title: str, facet: str, pattern_id: int) -> str:
    key = str(facet or "").strip().lower()
    hint = _FACET_HINT.get(key, "")
    if not hint:
        return title
    text = str(title or "")
    # Keep emphasis soft: append subtle context to analysis/question patterns.
    if int(pattern_id) in {3, 4, 5}:
        if "?" in text:
            return re.sub(r"\?\s*$", f", {hint}?", text)
        return f"{text} {hint}"
    return text


def build_title_candidates(
    *,
    base_title: str,
    category: str,
    facet: str,
    cluster_id: str,
    stable_hash_fn: Callable[[str], int],
) -> list[str]:
    _ = cluster_id
    _ = stable_hash_fn
    topic = _topic_phrase(base_title=base_title, category=category)
    year = str(_utc_now().year)
    candidates: list[str] = []
    for pattern_id, (_, template) in enumerate(TITLE_PATTERNS):
        title = template.format(topic=topic, year=year)
        title = _apply_facet_hint(title, facet, pattern_id)
        title = normalize_title(title)
        if title:
            candidates.append(title)
    while len(candidates) < 6:
        candidates.append(normalize_title(f"{topic} update: what changed and what to watch"))
    return candidates


def _default_state() -> dict[str, Any]:
    return {"version": 1, "updated_at_utc": _utc_now().isoformat(), "clusters": {}}


def _prune_state(state: dict[str, Any], ttl_days: int) -> dict[str, Any]:
    out = dict(state or {})
    clusters = out.get("clusters", {})
    if not isinstance(clusters, dict):
        clusters = {}
    cutoff = _utc_now() - timedelta(days=max(1, _safe_int(ttl_days, 14)))
    pruned: dict[str, Any] = {}
    for cluster_id, raw in clusters.items():
        row = dict(raw or {}) if isinstance(raw, dict) else {}
        updated = _parse_utc(str(row.get("updated_at_utc", "") or ""))
        if updated is None:
            continue
        if updated < cutoff:
            continue
        try:
            pid = int(row.get("last_pattern_id", -1))
        except Exception:
            pid = -1
        if pid < 0:
            continue
        pruned[str(cluster_id or "")] = {
            "last_pattern_id": int(pid),
            "updated_at_utc": updated.isoformat(),
        }
    out["clusters"] = pruned
    out["version"] = 1
    out["updated_at_utc"] = _utc_now().isoformat()
    return out


def _load_state(state_path: Path, ttl_days: int) -> tuple[dict[str, Any], bool]:
    path = Path(state_path).resolve()
    try:
        if not path.exists():
            return _default_state(), True
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return _default_state(), False
        return _prune_state(payload, ttl_days), True
    except Exception:
        return _default_state(), False


def _save_state(state_path: Path, state: dict[str, Any], ttl_days: int) -> bool:
    path = Path(state_path).resolve()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = _prune_state(dict(state or {}), ttl_days)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def _normalized_ratios(settings: dict[str, Any] | Any) -> tuple[float, float, float]:
    n = max(0.0, _safe_float(_setting(settings, "numeric_ratio", 0.40), 0.40))
    q = max(0.0, _safe_float(_setting(settings, "question_ratio", 0.20), 0.20))
    a = max(0.0, _safe_float(_setting(settings, "analysis_ratio", 0.40), 0.40))
    total = n + q + a
    if total <= 0.0:
        return 0.40, 0.20, 0.40
    return n / total, q / total, a / total


def _pattern_id_from_seed(seed: int, settings: dict[str, Any] | Any, patterns_total: int) -> int:
    numeric_ratio, question_ratio, _ = _normalized_ratios(settings)
    x = (abs(int(seed)) % 10000) / 10000.0
    family = "analysis"
    if x < numeric_ratio:
        family = "numeric"
    elif x < (numeric_ratio + question_ratio):
        family = "question"
    family_ids: dict[str, list[int]] = {"numeric": [0, 1], "question": [2, 3], "analysis": [4, 5]}
    ids = [pid for pid in family_ids.get(family, [0, 1, 2, 3, 4, 5]) if pid < patterns_total]
    if not ids:
        ids = [idx for idx in range(patterns_total)]
    pick = abs(int(seed) // 9973) % len(ids)
    return int(ids[pick])


def choose_diverse_title(
    *,
    base_title: str,
    cluster_id: str,
    facet: str,
    category: str,
    run_start_minute: str,
    stable_hash_fn: Callable[[str], int],
    state_path: Path,
    settings: dict[str, Any] | Any,
) -> dict[str, Any]:
    patterns_total = max(1, min(6, _safe_int(_setting(settings, "patterns_total", 6), 6)))
    ttl_days = max(1, _safe_int(_setting(settings, "cluster_pattern_ttl_days", 14), 14))
    min_chars = max(20, _safe_int(_setting(settings, "min_title_chars", 45), 45))
    max_chars = max(min_chars + 5, _safe_int(_setting(settings, "max_title_chars", 70), 70))

    base = normalize_title(base_title)
    if not base:
        base = "Tech update: what changed and why it matters"
    candidates = build_title_candidates(
        base_title=base,
        category=category,
        facet=facet,
        cluster_id=cluster_id,
        stable_hash_fn=stable_hash_fn,
    )
    candidates = list(candidates[:patterns_total])
    while len(candidates) < patterns_total:
        candidates.append(normalize_title(f"{base} - what to know now"))

    cluster_key = str(cluster_id or "").strip() or "cluster-none"
    facet_key = str(facet or "").strip().lower() or "impact"
    run_key = str(run_start_minute or "").strip()
    seed = int(stable_hash_fn(f"{cluster_key}|{facet_key}|{run_key}"))
    selected_pattern = _pattern_id_from_seed(seed, settings, patterns_total)

    state, state_ok = _load_state(Path(state_path), ttl_days)
    if state_ok:
        clusters = state.get("clusters", {})
        if not isinstance(clusters, dict):
            clusters = {}
            state["clusters"] = clusters
        row = dict(clusters.get(cluster_key, {}) or {})
        try:
            last_pattern = int(row.get("last_pattern_id", -1))
        except Exception:
            last_pattern = -1
        if last_pattern == selected_pattern:
            selected_pattern = (selected_pattern + 1) % patterns_total
        clusters[cluster_key] = {
            "last_pattern_id": int(selected_pattern),
            "updated_at_utc": _utc_now().isoformat(),
        }
        _save_state(Path(state_path), state, ttl_days)

    selected_title = clamp_title_length(candidates[selected_pattern], min_chars, max_chars)
    if not selected_title:
        selected_title = clamp_title_length(base, min_chars, max_chars)

    alt_count = 2 + (abs(int(seed)) % 4)
    alt_count = max(2, min(5, alt_count))
    remaining = [candidates[idx] for idx in range(len(candidates)) if idx != selected_pattern]
    rng = random.Random(abs(int(seed)) ^ 0x51F15E)
    rng.shuffle(remaining)
    alt_titles: list[str] = []
    for row in remaining:
        alt = clamp_title_length(normalize_title(row), min_chars, max_chars)
        if (not alt) or (alt == selected_title) or (alt in alt_titles):
            continue
        alt_titles.append(alt)
        if len(alt_titles) >= alt_count:
            break
    if len(alt_titles) < 2:
        filler = clamp_title_length(f"{base} - what to know now", min_chars, max_chars)
        if filler and filler != selected_title and filler not in alt_titles:
            alt_titles.append(filler)
    if len(alt_titles) < 2:
        fallback = clamp_title_length("Tech update explained: what changed and why it matters", min_chars, max_chars)
        if fallback and fallback != selected_title and fallback not in alt_titles:
            alt_titles.append(fallback)

    return {
        "title": selected_title,
        "alt_titles": alt_titles[:5],
        "pattern_id": int(selected_pattern),
        "candidates": [clamp_title_length(normalize_title(x), min_chars, max_chars) for x in candidates],
    }
