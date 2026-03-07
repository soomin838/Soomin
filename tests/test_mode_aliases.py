from __future__ import annotations

import unittest

from re_core.settings import AppSettings, is_news_mode, is_troubleshoot_mode


class ModeAliasTests(unittest.TestCase):
    def test_news_aliases_map_to_news_mode(self) -> None:
        for mode in ("news_interpretation", "news_interpretation_only", "tech_news_only"):
            with self.subTest(mode=mode):
                settings = AppSettings()
                settings.content_mode.mode = mode
                self.assertTrue(is_news_mode(settings))
                self.assertFalse(is_troubleshoot_mode(settings))


if __name__ == "__main__":
    unittest.main()
