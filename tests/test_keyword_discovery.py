from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from re_core.services.keyword_discovery import KeywordDiscovery


class KeywordDiscoveryTests(unittest.TestCase):
    def test_classifies_opportunities_by_thresholds(self) -> None:
        rows = [
            {
                "date": "2026-03-01",
                "query": "mullvad vpn ad ban",
                "page": "https://example.com/a",
                "clicks": 12,
                "impressions": 500,
                "ctr": 0.024,
                "position": 9.5,
            },
            {
                "date": "2026-03-01",
                "query": "energy drinks ranking",
                "page": "https://example.com/b",
                "clicks": 3,
                "impressions": 450,
                "ctr": 0.006,
                "position": 15.0,
            },
            {
                "date": "2026-03-01",
                "query": "air purifier shift",
                "page": "https://example.com/c",
                "clicks": 0,
                "impressions": 260,
                "ctr": 0.0,
                "position": 18.0,
            },
        ]
        tmp = tempfile.mkdtemp()
        try:
            engine = KeywordDiscovery(
                db_path=Path(tmp) / "search_console.sqlite3",
                fetch_rows_callback=lambda *args: rows,
            )
            opportunities = engine.run(start_date="2026-03-01", end_date="2026-03-07")
            del engine
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        action_types = [item.action_type for item in opportunities]
        self.assertIn("supporting_post", action_types)
        self.assertIn("title_rewrite", action_types)
        self.assertIn("intent_fix", action_types)


if __name__ == "__main__":
    unittest.main()
