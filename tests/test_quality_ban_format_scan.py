from __future__ import annotations

import unittest

from re_core.quality import ContentQAGate
from re_core.settings import QualitySettings


class QualityBanFormatScanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = ContentQAGate(settings=QualitySettings(), authority_links=[])

    def test_data_uri_payload_does_not_trigger_faq_ban(self) -> None:
        html = (
            '<p>Clear body copy about repairability.</p>'
            '<figure><img src="data:image/png;base64,AAAFAQBBBCCC" alt="editorial image" /></figure>'
            '<p>Another paragraph with no FAQ section.</p>'
        )
        failed, detail = self.gate._detect_forbidden_phrase_or_format(
            text=self.gate._to_text(html),
            html=html,
        )
        self.assertFalse(failed)
        self.assertEqual(detail, "")

    def test_visible_faq_heading_still_triggers_ban(self) -> None:
        html = "<h2>FAQ</h2><p>Question style block</p>"
        failed, detail = self.gate._detect_forbidden_phrase_or_format(
            text=self.gate._to_text(html),
            html=html,
        )
        self.assertTrue(failed)
        self.assertEqual(detail, "ban_format:FAQ")


if __name__ == "__main__":
    unittest.main()
