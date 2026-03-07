from __future__ import annotations

import unittest
from unittest.mock import patch

from re_core.services.worldmonitor_client import WorldMonitorClient
from re_core.settings import WorldMonitorSettings


class _FakeResponse:
    def __init__(self, status_code: int, payload=None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class WorldMonitorClientTests(unittest.TestCase):
    def test_retries_auth_then_falls_back_to_legacy(self) -> None:
        settings = WorldMonitorSettings(enabled=True, api_key="secret", prefer_api=True)
        responses = [
            _FakeResponse(401, text="unauthorized"),
            _FakeResponse(404, text="not found"),
            _FakeResponse(200, payload=[{"title": "Legacy item", "url": "https://example.com/x"}]),
        ]
        with patch("requests.Session.get", side_effect=responses):
            client = WorldMonitorClient(settings)
            items = client.fetch_feed_digest()
        self.assertEqual(len(items), 1)
        status = client.last_status.get("feed_digest")
        self.assertIsNotNone(status)
        self.assertTrue(status.ok)
        self.assertEqual(status.auth_mode, "none")

    def test_missing_key_uses_legacy_mode(self) -> None:
        settings = WorldMonitorSettings(enabled=True, api_key="", prefer_api=True)
        with patch(
            "requests.Session.get",
            return_value=_FakeResponse(200, payload=[{"title": "Legacy item", "url": "https://example.com/y"}]),
        ) as mocked_get:
            client = WorldMonitorClient(settings)
            items = client.fetch_feed_digest()
        self.assertEqual(len(items), 1)
        self.assertEqual(mocked_get.call_count, 1)


if __name__ == "__main__":
    unittest.main()
