from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from rezero_v2.core.orchestrator.run_engine import RunEngine
from rezero_v2.tests.helpers import FakeGDELTClient, article, make_settings_file


class V2MixedDomainSkipTest(unittest.TestCase):
    def test_mixed_domain_skip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings_path = make_settings_file(root / "config" / "settings.yaml")
            groups = [
                {
                    "topic": "semiconductor",
                    "articles": [
                        article(
                            title="Delhi-NCR Schools Bolster India's Semiconductor Ambitions",
                            summary="Schools and students were highlighted in the report while the semiconductor mission was mentioned as a broader backdrop.",
                            source="ianslive.in",
                            topic="semiconductor",
                        )
                    ],
                }
            ]
            engine = RunEngine(root, settings_path, overrides={"gdelt_client": FakeGDELTClient(groups)})
            result = engine.run_once(dry_run=True)
            self.assertEqual(result.summary.result, "skipped")
            self.assertIn(result.summary.reason_code, {"mixed_domain_education_tech_but_tech_not_dominant", "mixed_domain_requires_explicit_tech_angle", "all_candidates_rejected"})
