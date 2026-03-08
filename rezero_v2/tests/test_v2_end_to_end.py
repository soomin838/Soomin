from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from rezero_v2.core.orchestrator.run_engine import RunEngine
from rezero_v2.tests.helpers import FakeBloggerClient, FakeGDELTClient, FakePollinationsClient, FakeSearchConsoleClient, article, make_settings_file


class V2EndToEndTest(unittest.TestCase):
    def test_end_to_end_stage_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings_path = make_settings_file(root / "config" / "settings.yaml")
            groups = [
                {
                    "topic": "ai",
                    "articles": [
                        article(
                            title="OpenAI releases new model for developers",
                            summary="OpenAI released a new model today. The model adds better coding help. Pricing details and rollout timing were confirmed by the company.",
                        )
                    ],
                }
            ]
            engine = RunEngine(
                root,
                settings_path,
                overrides={
                    "gdelt_client": FakeGDELTClient(groups),
                    "search_console_client": FakeSearchConsoleClient([]),
                    "blogger_client": FakeBloggerClient("published"),
                    "pollinations_client": FakePollinationsClient(),
                },
            )
            result = engine.run_once(dry_run=True)
            self.assertEqual(result.summary.runtime_version, "v2")
            self.assertEqual(result.summary.result, "success")
            self.assertTrue(result.summary.final_stage in {"feedback_stage"})
            self.assertTrue(any(row["stage_name"] == "publish_stage" for row in result.stage_results))
