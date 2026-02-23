from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

from .settings import GeminiSettings, TopicGrowthSettings


class TopicGrower:
    def __init__(
        self,
        root: Path,
        seeds_path: Path,
        gemini: GeminiSettings,
        topic_growth: TopicGrowthSettings,
    ) -> None:
        self.root = root
        self.seeds_path = seeds_path
        self.gemini = gemini
        self.topic_growth = topic_growth
        self.state_path = root / "storage" / "logs" / "topic_growth_state.json"
        self.audit_path = root / "storage" / "logs" / "topic_growth_audit.jsonl"
        self.seeds_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)

    def maybe_grow(self, existing_titles: list[str]) -> tuple[bool, str]:
        if not self.topic_growth.enabled:
            return False, "topic growth disabled"
        if not self.gemini.api_key or self.gemini.api_key == "GEMINI_API_KEY":
            return False, "gemini api key missing"

        day = datetime.now(timezone.utc).date().isoformat()
        state = self._load_state()
        count = int(state.get(day, 0))
        target_daily = max(1, int(self.topic_growth.daily_new_topics))
        remaining = max(0, target_daily - count)
        if remaining <= 0:
            return False, "daily topic growth limit reached"

        known_titles = {t.strip().lower() for t in existing_titles if t}
        for item in self._load_seed_items():
            title = str(item.get("title", "")).strip().lower()
            if title:
                known_titles.add(title)

        generated = self._generate_safe_topics(
            existing_titles=sorted(known_titles),
            n=max(remaining, 5),
        )
        if not generated:
            self._audit("rejected", {"reason": "model returned invalid topic batch"})
            return False, "no safe topics returned"

        accepted: list[dict] = []
        for topic in generated:
            title_key = str(topic.get("title", "")).strip().lower()
            if not title_key or title_key in known_titles:
                continue
            self._append_seed(topic)
            known_titles.add(title_key)
            accepted.append(topic)
            if len(accepted) >= remaining:
                break

        if not accepted:
            self._audit("rejected", {"reason": "all generated topics were duplicates/invalid"})
            return False, "no unique topic added"

        state[day] = count + len(accepted)
        self._save_state(state)
        self._audit("accepted_batch", {"count": len(accepted), "topics": accepted[:10]})
        return True, f"{len(accepted)} topics added"

    def _generate_safe_topics(self, existing_titles: list[str], n: int = 5) -> list[dict]:
        endpoint = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self.gemini.model}:generateContent"
        )
        n = max(1, int(n))
        prompt = (
            "Generate NEW English blog topic ideas for US/UK/global general workers.\n"
            "Goal: expand topic pool into adjacent areas not already covered.\n"
            "Every topic must be a completely new angle that does NOT exist in the current pool.\n"
            "Audience focus: AI productivity, practical life hacks, beginner-friendly tech news.\n"
            "Include mainstream business angles from global innovators (Apple, Tesla, Google, Microsoft, Amazon, NVIDIA) "
            "when possible, emphasizing practical productivity lessons.\n"
            "Hard rules:\n"
            "1) Legal, safe, all-ages, non-harmful.\n"
            "2) No illegal/harmful/adult/hate/scam content.\n"
            "3) Practical and high click potential for mainstream readers.\n"
            "3-1) Prefer positive success-story framing over criticism.\n"
            "4) Must avoid overlap with existing pool and recent titles.\n"
            f"Existing topic pool and recent titles: {existing_titles[:180]}\n"
            f"Return strict JSON only: {{\"topics\":[...{n} items...]}} where each item has keys\n"
            "{\"title\":str,\"body\":str,\"score\":int,\"url\":str,"
            "\"is_legal\":bool,\"is_all_ages\":bool,\"reason\":str}\n"
            "Keep each body to 2-4 sentences in natural English."
        )
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.95, "topP": 0.95, "maxOutputTokens": 4096},
            "systemInstruction": {
                "parts": [{"text": self.gemini.editor_persona}],
            },
        }
        try:
            resp = requests.post(endpoint, params={"key": self.gemini.api_key}, json=body, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            text = "\n".join(
                part.get("text", "")
                for part in data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
                if "text" in part
            )
            parsed = self._extract_json(text)
        except Exception:
            parsed = {}

        rows = parsed.get("topics", []) if isinstance(parsed, dict) else []
        if not isinstance(rows, list):
            rows = []

        out: list[dict] = []
        for row in rows[: max(n * 3, 15)]:
            if not isinstance(row, dict):
                continue
            topic = self._normalize_topic(row)
            if topic is None:
                continue
            out.append(topic)
        return out[: max(n, 5)]

    def _normalize_topic(self, payload: dict) -> dict | None:
        if not bool(payload.get("is_legal")):
            return None
        if not bool(payload.get("is_all_ages")):
            return None
        title = str(payload.get("title", "")).strip()
        body_text = str(payload.get("body", "")).strip()
        url = str(payload.get("url", "")).strip() or "https://example.com/topic-growth"
        score = int(payload.get("score", self.topic_growth.min_seed_score))
        if not title or not body_text:
            return None
        if score < int(self.topic_growth.min_seed_score):
            return None
        if self._contains_blocked_terms(title + " " + body_text):
            return None
        return {
            "source": "topic_growth",
            "title": title,
            "body": body_text,
            "score": max(int(self.topic_growth.min_seed_score), min(score, 100)),
            "url": url,
        }

    def _append_seed(self, topic: dict) -> None:
        suffix = self.seeds_path.suffix.lower()
        if suffix == ".json":
            current = self._load_seed_items()
            current.append(topic)
            self.seeds_path.write_text(
                json.dumps(current, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            return
        # Default fallback: JSONL append.
        line = json.dumps(topic, ensure_ascii=False)
        with self.seeds_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def _load_seed_items(self) -> list[dict]:
        if not self.seeds_path.exists():
            return []
        suffix = self.seeds_path.suffix.lower()
        if suffix == ".json":
            try:
                raw = json.loads(self.seeds_path.read_text(encoding="utf-8"))
            except Exception:
                return []
            if isinstance(raw, list):
                return [r for r in raw if isinstance(r, dict)]
            if isinstance(raw, dict) and isinstance(raw.get("topics"), list):
                return [r for r in raw.get("topics", []) if isinstance(r, dict)]
            return []

        out: list[dict] = []
        for line in self.seeds_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                out.append(item)
        return out

    def _load_state(self) -> dict:
        if not self.state_path.exists():
            return {}
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_state(self, state: dict) -> None:
        self.state_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

    def _audit(self, event: str, payload: dict) -> None:
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "payload": payload,
        }
        with self.audit_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _extract_json(self, text: str) -> dict:
        match = re.search(r"\{.*\}", text or "", flags=re.DOTALL)
        if not match:
            return {}
        try:
            return json.loads(match.group(0))
        except Exception:
            return {}

    def _contains_blocked_terms(self, text: str) -> bool:
        lower = (text or "").lower()
        blocked = [
            "exploit", "malware", "weapon", "bomb", "drugs", "adult", "porn", "gambling",
            "fraud", "scam", "counterfeit", "hate", "violence", "deepfake crime",
        ]
        return any(w in lower for w in blocked)
