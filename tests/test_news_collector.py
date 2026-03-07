from __future__ import annotations

import unittest
from unittest.mock import patch

from re_core.services.news_collector import clean_news_data, fetch_trending_topics


class NewsCollectorTests(unittest.TestCase):
    def test_clean_news_data_deduplicates_and_normalizes_dates(self) -> None:
        rows = [
            {
                "title": "AI systems expand in banking",
                "url": "https://example.com/a",
                "domain": "example.com",
                "seendate": "20260307123000",
                "excerpt": "Banks are testing new AI workflows.",
            },
            {
                "title": "AI systems expand in banking",
                "url": "https://example.com/a",
                "domain": "example.com",
                "seendate": "20260307123000",
                "excerpt": "Duplicate row",
            },
            {
                "title": "",
                "url": "https://example.com/b",
                "domain": "example.com",
            },
        ]
        cleaned = clean_news_data(rows)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["source"], "example.com")
        self.assertTrue(cleaned[0]["published_date"].startswith("2026-03-07T12:30:00"))
        self.assertEqual(cleaned[0]["provider"], "gdelt")
        self.assertEqual(cleaned[0]["topic"], "")

    def test_fetch_trending_topics_deduplicates_across_topics(self) -> None:
        fake_results = {
            "AI": [
                {"title": "AI item", "url": "https://example.com/shared", "source": "example.com", "published_date": "", "summary": "a"},
                {"title": "AI unique", "url": "https://example.com/ai", "source": "example.com", "published_date": "", "summary": "b"},
            ],
            "technology": [
                {"title": "Tech shared", "url": "https://example.com/shared", "source": "example.com", "published_date": "", "summary": "c"},
                {"title": "Tech unique", "url": "https://example.com/tech", "source": "example.com", "published_date": "", "summary": "d"},
            ],
        }

        def fake_fetch_news(topic: str, max_records: int = 20):  # noqa: ARG001
            return list(fake_results.get(topic, []))

        with patch("re_core.services.news_collector.fetch_news", side_effect=fake_fetch_news):
            groups = fetch_trending_topics()

        self.assertGreaterEqual(len(groups), 2)
        urls = [article["url"] for group in groups for article in group["articles"]]
        self.assertEqual(urls.count("https://example.com/shared"), 1)
        self.assertTrue(all(article["provider"] == "gdelt" for group in groups for article in group["articles"]))
        self.assertTrue(all(article["topic"] for group in groups for article in group["articles"]))


if __name__ == "__main__":
    unittest.main()
