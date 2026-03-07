from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from re_core.prompt_factory import PromptFactory, PROMPT_PACK_BUILD_SIGNATURE, PROMPT_PACK_VERSION


class PromptFactoryCacheGuardTests(unittest.TestCase):
    def test_stale_marker_pack_is_purged_and_rebuilt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            today = datetime.now(timezone.utc).date().isoformat()
            pack_path = root / "storage" / "prompt_packs" / "rewrite_to_actionable" / f"{today}.json"
            pack_path.parent.mkdir(parents=True, exist_ok=True)
            pack_path.write_text(
                json.dumps(
                    {
                        "purpose": "rewrite_to_actionable",
                        "system": "Write from the reader's lived experience.",
                        "user": "Source frame: keep a one-line status update tied to the article.",
                        "style_variant_id": "v1",
                        "must_include": [],
                        "ban_tokens": [],
                        "temperature": 0.7,
                        "top_p": 0.9,
                        "persona_id": "stale",
                        "version": PROMPT_PACK_VERSION,
                        "build_signature": PROMPT_PACK_BUILD_SIGNATURE,
                        "created_at_utc": datetime.now(timezone.utc).isoformat(),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            factory = PromptFactory(root)
            pack = factory.get_pack("rewrite_to_actionable")

            self.assertEqual(pack.version, PROMPT_PACK_VERSION)
            self.assertEqual(pack.build_signature, PROMPT_PACK_BUILD_SIGNATURE)
            self.assertNotIn("source frame:", pack.user.lower())
            self.assertNotIn("lived experience", pack.system.lower())
            payload = json.loads(pack_path.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("build_signature"), PROMPT_PACK_BUILD_SIGNATURE)

    def test_wrong_build_signature_is_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            today = datetime.now(timezone.utc).date().isoformat()
            pack_path = root / "storage" / "prompt_packs" / "headline" / f"{today}.json"
            pack_path.parent.mkdir(parents=True, exist_ok=True)
            pack_path.write_text(
                json.dumps(
                    {
                        "purpose": "headline",
                        "system": "Old prompt",
                        "user": "Old prompt",
                        "style_variant_id": "v1",
                        "must_include": [],
                        "ban_tokens": [],
                        "temperature": 0.5,
                        "top_p": 0.9,
                        "persona_id": "old",
                        "version": PROMPT_PACK_VERSION,
                        "build_signature": "outdated",
                        "created_at_utc": datetime.now(timezone.utc).isoformat(),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            pack = PromptFactory(root).get_pack("headline")
            self.assertEqual(pack.build_signature, PROMPT_PACK_BUILD_SIGNATURE)
            self.assertIn("ad-safe", pack.system.lower())


if __name__ == "__main__":
    unittest.main()
