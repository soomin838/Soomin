from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from PySide6.QtCore import QCoreApplication

from rezero_v2.app.controllers.run_controller import V2RunController
from rezero_v2.core.orchestrator.run_engine import RunEngine
from rezero_v2.tests.helpers import FakeBloggerClient, FakeGDELTClient, FakePollinationsClient, FakeSearchConsoleClient, article, make_settings_file


class V2UiFinalReasonBindingTest(unittest.TestCase):
    def test_ui_state_receives_final_reason(self) -> None:
        app = QCoreApplication.instance() or QCoreApplication([])
        self.assertIsNotNone(app)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings_path = make_settings_file(root / "config" / "settings.yaml")
            groups = [{"topic": "ai", "articles": [article(title="OpenAI releases developer update", summary="OpenAI released a developer update today. Pricing and rollout details were confirmed by the company.")] }]
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
            controller = V2RunController(root, settings_path, engine=engine)
            controller.run_once_sync(dry_run=True)
            state = controller.refresh_state()
            self.assertIsNotNone(state.latest_final_summary)
            self.assertTrue(state.latest_final_summary.get("reason_code"))
