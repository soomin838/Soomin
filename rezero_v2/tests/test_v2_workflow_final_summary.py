from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from rezero_v2.core.orchestrator.run_engine import RunEngine
from rezero_v2.tests.helpers import FakeBloggerClient, FakeGDELTClient, FakePollinationsClient, FakeSearchConsoleClient, article, make_settings_file


class V2WorkflowFinalSummaryTest(unittest.TestCase):
    def test_explicit_workflow_final_result_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings_path = make_settings_file(root / "config" / "settings.yaml")
            groups = [{"topic": "ai", "articles": [article(title="OpenAI ships developer update", summary="OpenAI shipped a developer update today. The company confirmed pricing and rollout details. Users can compare the changes immediately.")] }]
            engine = RunEngine(
                root,
                settings_path,
                overrides={
                    "gdelt_client": FakeGDELTClient(groups),
                    "search_console_client": FakeSearchConsoleClient([]),
                    "blogger_client": FakeBloggerClient(),
                    "pollinations_client": FakePollinationsClient(),
                },
            )
            result = engine.run_once(dry_run=True)
            self.assertEqual(result.summary.runtime_version, "v2")
            self.assertTrue(result.summary.reason_code)
            self.assertIsInstance(result.summary.stage_timings_ms, dict)
