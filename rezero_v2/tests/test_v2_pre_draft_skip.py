from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from rezero_v2.core.orchestrator.run_engine import RunEngine
from rezero_v2.tests.helpers import FakeGDELTClient, article, make_settings_file


class V2PreDraftSkipTest(unittest.TestCase):
    def test_pre_draft_skip_on_weak_grounding(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings_path = make_settings_file(root / "config" / "settings.yaml")
            groups = [{"topic": "chips", "articles": [article(title="Chip project update", summary="Chip project update.", topic="chips")]}]
            engine = RunEngine(root, settings_path, overrides={"gdelt_client": FakeGDELTClient(groups)})
            result = engine.run_once(force_content_type="hot", dry_run=True)
            self.assertEqual(result.summary.result, "skipped")
            self.assertIn(result.summary.reason_code, {"pre_draft_source_fact_density_too_low", "pre_draft_entity_overlap_too_low", "pre_draft_low_grounding"})
