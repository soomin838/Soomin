from __future__ import annotations

import hashlib
import json
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Callable
from urllib.parse import urlparse


FACET_POOL = (
    "impact",
    "timeline",
    "official",
    "risk",
    "market",
    "user_angle",
)
TOP_FACET_COUNT = 4
ROTATION_WINDOW = 4
FACET_TTL_DAYS = 30

FACET_EMPHASIS_HINTS = {
    "impact": "Give extra weight to who is affected first and the practical ripple effects in daily workflows.",
    "timeline": "Emphasize sequence, milestones, and what changed between earlier and latest updates.",
    "official": "Prioritize confirmed statements, release notes, and clearly attributed evidence.",
    "risk": "Highlight credible downside scenarios, safeguards, and low-regret checks.",
    "market": "Surface vendor, partner, and ecosystem reaction with concrete business implications.",
    "user_angle": "Write from the reader's lived experience and immediate usability concerns.",
}

FACET_OPTIONAL_SECTION_PRIORITY = {
    "impact": ("why_it_matters", "what_to_watch_next", "key_details", "timeline", "risks", "background_context"),
    "timeline": ("timeline", "what_to_watch_next", "key_details", "background_context", "risks", "why_it_matters"),
    "official": ("key_details", "background_context", "timeline", "what_to_watch_next", "risks", "why_it_matters"),
    "risk": ("risks", "what_to_watch_next", "key_details", "timeline", "why_it_matters", "background_context"),
    "market": ("why_it_matters", "background_context", "what_to_watch_next", "timeline", "key_details", "risks"),
    "user_angle": ("why_it_matters", "key_details", "what_to_watch_next", "timeline", "background_context", "risks"),
}

_CATEGORY_SPECIFIC = {"security", "policy", "platform", "sec"}


def sha256_int(value: str) -> int:
    digest = hashlib.sha256(str(value or "").encode("utf-8", errors="ignore")).hexdigest()
    return int(digest, 16)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: str) -> datetime | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def normalize_facet(value: str) -> str:
    key = re.sub(r"\s+", "_", str(value or "").strip().lower())
    if key in FACET_POOL:
        return key
    aliases = {
        "user": "user_angle",
        "users": "user_angle",
        "officials": "official",
        "risky": "risk",
    }
    out = aliases.get(key, "")
    return out if out in FACET_POOL else ""


def normalize_category(value: str) -> str:
    key = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if "security" in key or key == "sec":
        return "security"
    if "policy" in key:
        return "policy"
    if "platform" in key:
        return "platform"
    return key or "platform"


def _coerce_retry(value: object) -> int | None:
    if value is None:
        return None
    try:
        out = int(value)
    except Exception:
        return None
    return max(0, out)


def _unique_facets(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        key = normalize_facet(item)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def heuristic_facet_candidates(*, title: str, body: str, category: str) -> list[str]:
    cat = normalize_category(category)
    text = f"{str(title or '')} {str(body or '')}".lower()
    score = {facet: 0.0 for facet in FACET_POOL}

    score["impact"] += 1.0
    score["user_angle"] += 1.0
    score["official"] += 0.8
    score["risk"] += 0.8
    score["timeline"] += 0.6
    score["market"] += 0.6

    if cat == "security":
        score["risk"] += 2.6
        score["official"] += 1.8
        score["impact"] += 1.4
    elif cat == "policy":
        score["official"] += 2.4
        score["impact"] += 1.8
        score["timeline"] += 1.2
    elif cat == "platform":
        score["user_angle"] += 2.4
        score["impact"] += 1.7
        score["timeline"] += 1.0

    keyword_map = {
        "timeline": ("timeline", "roadmap", "phased", "rollout", "schedule", "when", "next week"),
        "official": ("official", "statement", "announced", "press release", "release notes", "confirmed", "according to"),
        "risk": ("risk", "warning", "incident", "outage", "breach", "vulnerability", "cve", "caution"),
        "market": ("market", "investor", "stock", "industry", "partner", "competitor", "pricing"),
        "user_angle": ("users", "customer", "consumer", "creator", "team", "workflow", "daily use"),
        "impact": ("impact", "affected", "who", "change", "disruption", "benefit", "cost"),
    }
    for facet, tokens in keyword_map.items():
        for token in tokens:
            if token in text:
                score[facet] += 0.9

    ordered = sorted(FACET_POOL, key=lambda x: (-float(score.get(x, 0.0)), x))
    return list(ordered)


def merge_facet_candidates(*, llm_candidates: list[str] | None, heuristic_candidates: list[str] | None) -> list[str]:
    merged = _unique_facets(list(llm_candidates or []))
    merged.extend(_unique_facets(list(heuristic_candidates or [])))
    merged = _unique_facets(merged)
    for facet in FACET_POOL:
        if facet not in merged:
            merged.append(facet)
    return merged


def deterministic_top_facets(*, facet_seed: int, candidate_pool: list[str], top_n: int = TOP_FACET_COUNT) -> list[str]:
    pool = _unique_facets(list(candidate_pool or []))
    for facet in FACET_POOL:
        if facet not in pool:
            pool.append(facet)
    rng = random.Random(int(facet_seed))
    rng.shuffle(pool)
    take = max(1, min(int(top_n), len(pool)))
    return pool[:take]


def reorder_optional_sections_for_facet(*, optional_sections: list[str], facet: str, seed: int) -> list[str]:
    pool = [str(x or "").strip().lower() for x in optional_sections if str(x or "").strip()]
    pool = list(dict.fromkeys(pool))
    rng = random.Random(int(seed) ^ 0x9E3779B1)
    rng.shuffle(pool)
    priority = [x for x in FACET_OPTIONAL_SECTION_PRIORITY.get(normalize_facet(facet), ()) if x in pool]
    tail = [x for x in pool if x not in priority]
    return priority + tail


def _is_specific_action_category(category: str) -> bool:
    key = normalize_category(category)
    if key in _CATEGORY_SPECIFIC:
        return True
    return any(tok in key for tok in ("security", "policy", "platform", "sec"))


def deterministic_action_count(*, event_id: str, run_start_minute: str, stable_hash_fn: Callable[[str], int]) -> int:
    seed = int(stable_hash_fn(f"{str(event_id or '').strip()}{str(run_start_minute or '').strip()}|what_to_do"))
    return 3 + int(seed % 4)


def _action_pool(category: str, source_url: str) -> list[str]:
    host = (urlparse(str(source_url or "")).netloc or "").lower()
    source_label = host or "the official source"
    specific = [
        "Verify the exact version and rollout timestamp in your tenant or device inventory.",
        "Scope impact first: identify which user groups, regions, or app versions are affected.",
        "Check official release notes and advisories before changing production-wide settings.",
        "Test one controlled pilot change, then compare logs and user-facing behavior.",
        "Prepare rollback criteria with a clear trigger and owner before broad rollout.",
        "Record confirmed facts versus assumptions so teams do not act on rumor-only signals.",
        "Set a short re-check window (for example 24 hours) for new official updates.",
        f"Track source updates from {source_label} and map each update to an internal action owner.",
    ]
    generic = [
        "Read the latest official update summary and note what is confirmed versus pending.",
        "Check whether your current setup matches the affected version or feature scope.",
        "Run a low-risk test on one environment before applying broad changes.",
        "Write down one observable success signal for each action to avoid guesswork.",
        "Pause rollout if user impact worsens and keep a rollback path ready.",
        "Re-check status after the next vendor update to avoid stale assumptions.",
        "Share a short internal update so stakeholders align on facts and next steps.",
        "Capture unresolved questions and assign owners for follow-up verification.",
    ]
    if _is_specific_action_category(category):
        return specific
    return generic


def build_action_items(
    *,
    category: str,
    facet: str,
    title: str,
    source_url: str,
    action_count: int,
    seed: int,
) -> list[str]:
    pool = list(_action_pool(category, source_url))
    facet_hint = normalize_facet(facet)
    if facet_hint == "impact":
        pool.append("Prioritize fixes for the user segment with the highest operational impact first.")
    elif facet_hint == "timeline":
        pool.append("Track update milestones in time order so teams know what changed and when.")
    elif facet_hint == "official":
        pool.append("Attribute each decision to a confirmed source statement or release note.")
    elif facet_hint == "risk":
        pool.append("Document worst-case scenarios and define stop conditions before the next rollout step.")
    elif facet_hint == "market":
        pool.append("Check partner/vendor ecosystem notices for downstream compatibility or pricing impact.")
    elif facet_hint == "user_angle":
        pool.append("Validate the change in a real user flow, not only in an admin or test dashboard.")
    if title:
        pool.append(f"Keep a one-line status update tied to '{str(title).strip()[:70]}' after each check.")

    unique_pool: list[str] = []
    seen: set[str] = set()
    for row in pool:
        key = re.sub(r"\s+", " ", str(row or "").strip().lower())
        if not key or key in seen:
            continue
        seen.add(key)
        unique_pool.append(str(row).strip())

    rng = random.Random(int(seed) ^ 0xA5A5A5A5)
    rng.shuffle(unique_pool)
    need = max(3, min(6, int(action_count)))
    picked = unique_pool[:need]
    while len(picked) < need:
        picked.append(f"Add one more verification checkpoint before broad rollout ({len(picked) + 1}).")
    return picked


def what_to_do_section_html(action_items: list[str]) -> str:
    rows = [
        f"<li>{escape(re.sub(r'\s+', ' ', str(item or '').strip()))}</li>"
        for item in (action_items or [])
        if str(item or "").strip()
    ]
    if not rows:
        rows = [
            "<li>Review official updates.</li>",
            "<li>Run one controlled verification.</li>",
            "<li>Monitor impact before full rollout.</li>",
        ]
    return "<h2>What To Do Now</h2><ul>" + "".join(rows) + "</ul>"


def ensure_what_to_do_now_section(*, html: str, action_items: list[str]) -> str:
    src = str(html or "")
    section_html = what_to_do_section_html(action_items)
    section_re = re.compile(
        r"<h2[^>]*>\s*What\s*To\s*Do\s*Now\s*</h2>.*?(?=<h2\b|$)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    if section_re.search(src):
        return section_re.sub(section_html, src, count=1)
    sources_re = re.compile(r"<h2[^>]*>\s*Sources\s*</h2>", flags=re.IGNORECASE)
    match = sources_re.search(src)
    if match:
        return src[: match.start()] + section_html + src[match.start() :]
    return src + section_html


def facet_emphasis_hint(facet: str) -> str:
    return FACET_EMPHASIS_HINTS.get(normalize_facet(facet), FACET_EMPHASIS_HINTS["impact"])


@dataclass(frozen=True)
class FacetContext:
    facet_seed: int
    top_facets: list[str]
    selected_facet: str
    selected_index: int
    retry_index_raw: int
    retry_index_effective: int
    action_count: int
    action_items: list[str]
    llm_candidates_used: list[str]
    source: str

    def as_dict(self) -> dict[str, object]:
        return {
            "facet_seed": int(self.facet_seed),
            "top_facets": list(self.top_facets),
            "selected_facet": str(self.selected_facet),
            "selected_index": int(self.selected_index),
            "retry_index_raw": int(self.retry_index_raw),
            "retry_index_effective": int(self.retry_index_effective),
            "action_count": int(self.action_count),
            "action_items": list(self.action_items),
            "llm_candidates_used": list(self.llm_candidates_used),
            "source": str(self.source),
        }


class FacetRotationStore:
    def __init__(self, path: Path, ttl_days: int = FACET_TTL_DAYS) -> None:
        self.path = Path(path).resolve()
        self.ttl_days = max(1, int(ttl_days))

    def _load_state(self) -> tuple[dict[str, object], bool]:
        try:
            if not self.path.exists():
                return {"version": 1, "events": {}}, True
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return {"version": 1, "events": {}}, True
            events = payload.get("events", {})
            if not isinstance(events, dict):
                payload["events"] = {}
            self._prune_expired(payload)
            return payload, True
        except Exception:
            return {"version": 1, "events": {}}, False

    def _save_state(self, state: dict[str, object]) -> bool:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            state = dict(state or {})
            state["version"] = 1
            state["updated_at_utc"] = _utc_now().isoformat()
            self.path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            return True
        except Exception:
            return False

    def _prune_expired(self, state: dict[str, object]) -> None:
        cutoff = _utc_now() - timedelta(days=self.ttl_days)
        raw_events = state.get("events", {})
        if not isinstance(raw_events, dict):
            state["events"] = {}
            return
        clean: dict[str, object] = {}
        for event_id, row in raw_events.items():
            entry = dict(row or {}) if isinstance(row, dict) else {}
            updated = _parse_utc(str(entry.get("updated_at_utc", "") or ""))
            if updated is None:
                history = entry.get("history", [])
                if isinstance(history, list) and history:
                    updated = _parse_utc(str((history[-1] or {}).get("at_utc", "") or ""))
            if updated is not None and updated < cutoff:
                continue
            clean[str(event_id)] = entry
        state["events"] = clean

    def next_retry_index(self, event_id: str) -> int:
        state, ok = self._load_state()
        if not ok:
            return 0
        events = state.get("events", {})
        if not isinstance(events, dict):
            return 0
        row = events.get(str(event_id), {})
        if not isinstance(row, dict):
            return 0
        try:
            last = int(row.get("last_retry_index", -1) or -1)
        except Exception:
            last = -1
        return max(0, last + 1)

    def record(self, *, event_id: str, run_start_minute: str, context: FacetContext) -> bool:
        state, ok = self._load_state()
        if not ok:
            return False
        events = state.get("events", {})
        if not isinstance(events, dict):
            events = {}
            state["events"] = events
        key = str(event_id or "").strip()
        if not key:
            return False
        row = dict(events.get(key, {}) or {})
        history = row.get("history", [])
        if not isinstance(history, list):
            history = []
        history.append(
            {
                "at_utc": _utc_now().isoformat(),
                "run_start_minute": str(run_start_minute or ""),
                "facet_seed": int(context.facet_seed),
                "top_facets": list(context.top_facets),
                "selected_facet": str(context.selected_facet),
                "selected_index": int(context.selected_index),
                "retry_index_raw": int(context.retry_index_raw),
                "retry_index_effective": int(context.retry_index_effective),
                "source": str(context.source),
            }
        )
        row.update(
            {
                "updated_at_utc": _utc_now().isoformat(),
                "last_run_start_minute": str(run_start_minute or ""),
                "last_retry_index": int(context.retry_index_raw),
                "last_retry_index_effective": int(context.retry_index_effective),
                "last_selected_facet": str(context.selected_facet),
                "last_selected_index": int(context.selected_index),
                "history": history[-32:],
            }
        )
        events[key] = row
        return self._save_state(state)


def resolve_facet_context(
    *,
    event_id: str,
    run_start_minute: str,
    title: str,
    body: str,
    category: str,
    source_url: str = "",
    retry_index: object = None,
    llm_candidates: list[str] | None = None,
    state_path: Path | None = None,
    stable_hash_fn: Callable[[str], int] = sha256_int,
) -> FacetContext:
    event_key = str(event_id or "").strip() or "unknown-event"
    run_key = str(run_start_minute or "").strip() or "unknown-minute"
    facet_seed = int(stable_hash_fn(f"{event_key}{run_key}"))

    heuristics = heuristic_facet_candidates(title=title, body=body, category=category)
    llm_clean = _unique_facets(list(llm_candidates or []))
    candidate_pool = merge_facet_candidates(llm_candidates=llm_clean, heuristic_candidates=heuristics)
    top_facets = deterministic_top_facets(facet_seed=facet_seed, candidate_pool=candidate_pool, top_n=TOP_FACET_COUNT)
    if not top_facets:
        top_facets = list(FACET_POOL[:TOP_FACET_COUNT])

    requested_retry = _coerce_retry(retry_index)
    resolved_retry = requested_retry
    source = "retry_index"
    store: FacetRotationStore | None = None
    if resolved_retry is None:
        source = "state"
        if state_path is not None:
            store = FacetRotationStore(path=Path(state_path), ttl_days=FACET_TTL_DAYS)
            resolved_retry = store.next_retry_index(event_key)
        else:
            resolved_retry = 0
    if resolved_retry is None:
        resolved_retry = 0
    effective_retry = int(resolved_retry) % ROTATION_WINDOW
    picked_index = int(effective_retry) % len(top_facets)
    picked_facet = str(top_facets[picked_index])

    action_count = deterministic_action_count(
        event_id=event_key,
        run_start_minute=run_key,
        stable_hash_fn=stable_hash_fn,
    )
    action_seed = int(stable_hash_fn(f"{event_key}{run_key}|actions"))
    action_items = build_action_items(
        category=category,
        facet=picked_facet,
        title=title,
        source_url=source_url,
        action_count=action_count,
        seed=action_seed,
    )

    context = FacetContext(
        facet_seed=facet_seed,
        top_facets=list(top_facets),
        selected_facet=picked_facet,
        selected_index=int(picked_index),
        retry_index_raw=int(resolved_retry),
        retry_index_effective=int(effective_retry),
        action_count=int(action_count),
        action_items=action_items,
        llm_candidates_used=llm_clean,
        source=source,
    )

    if store is None and state_path is not None:
        store = FacetRotationStore(path=Path(state_path), ttl_days=FACET_TTL_DAYS)
    if store is not None:
        store.record(event_id=event_key, run_start_minute=run_key, context=context)
    return context
