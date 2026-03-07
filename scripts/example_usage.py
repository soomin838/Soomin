"""Example usage for the GDELT-based news collector."""
from __future__ import annotations

import json
import logging

from re_core.services.news_collector import (
    clean_news_data,
    expand_keywords,
    fetch_news,
    fetch_trending_topics,
)


logging.basicConfig(level=logging.INFO)


def main() -> None:
    print("=== Keyword expansion ===")
    print(expand_keywords("AI"))

    print("\n=== Single-topic fetch ===")
    ai_news = fetch_news("AI", 10)
    print(json.dumps(ai_news[:3], indent=2, ensure_ascii=False))

    print("\n=== Cleaned manual sample ===")
    sample = ai_news + ai_news[:1]
    cleaned = clean_news_data(sample)
    print(json.dumps(cleaned[:3], indent=2, ensure_ascii=False))

    print("\n=== Trending topics ===")
    trending = fetch_trending_topics()
    print(json.dumps(trending[:2], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
