"""Example usage for the GDELT-based news collector."""
from __future__ import annotations

import json
import logging
import sys
import threading
from queue import Queue
from pathlib import Path

if __package__ in {None, ""}:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from re_core.services.news_collector import (
    clean_news_data,
    expand_keywords,
    fetch_news,
    fetch_trending_topics,
)


logging.basicConfig(level=logging.INFO)


def _configure_output() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="backslashreplace")
            except Exception:
                pass


def _print_json(value: object) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=True))


def _call_with_timeout(label: str, func, *args, timeout: float = 45.0):
    queue: Queue[tuple[str, object]] = Queue(maxsize=1)

    def runner() -> None:
        try:
            queue.put(("ok", func(*args)))
        except Exception as exc:  # pragma: no cover - developer example guard
            queue.put(("error", exc))

    thread = threading.Thread(target=runner, name=f"example_{label}", daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    if thread.is_alive():
        print(f"[warn] {label} timed out after {timeout:.0f}s")
        return []
    if queue.empty():
        print(f"[warn] {label} returned no result")
        return []
    status, payload = queue.get()
    if status == "error":
        print(f"[warn] {label} failed: {payload}")
        return []
    return payload


def main() -> None:
    _configure_output()

    print("=== Keyword expansion ===")
    _print_json(expand_keywords("AI"))

    print("\n=== Single-topic fetch ===")
    ai_news = _call_with_timeout("fetch_news", fetch_news, "AI", 10, timeout=45.0)
    _print_json(ai_news[:3])

    print("\n=== Cleaned manual sample ===")
    sample = ai_news + ai_news[:1]
    cleaned = clean_news_data(sample)
    _print_json(cleaned[:3])

    print("\n=== Trending topics ===")
    trending = _call_with_timeout("fetch_trending_topics", fetch_trending_topics, timeout=60.0)
    _print_json(trending[:2])


if __name__ == "__main__":
    main()
