from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.clickbait_sanitizer import sanitize_clickbait_terms  # noqa: E402


def _assert_no_clickbait(text: str) -> None:
    banned = r"\b(shocking|disaster|scam|fraud|criminal|exposed|destroyed|caught)\b"
    parts = re.split(r"(<[^>]+>)", str(text or ""))
    visible = "".join(part for part in parts if not (part.startswith("<") and part.endswith(">")))
    if re.search(banned, visible, flags=re.IGNORECASE):
        raise AssertionError("clickbait token still present after sanitize")


def main() -> int:
    html = (
        "<h2>Quick update</h2>"
        "<p>This was a shocking incident and a disaster for users.</p>"
        "<p>Some reports called it a scam or fraud, and others said one criminal was caught.</p>"
        '<p>Reference URL text should remain in href: <a href="https://example.com/scam-report">source</a>.</p>'
        "<p>Another sentence says service was exposed then destroyed.</p>"
    )
    out, replaced = sanitize_clickbait_terms(html)

    _assert_no_clickbait(out)
    expected_tokens = {"shocking", "disaster", "scam", "fraud", "criminal", "caught", "exposed", "destroyed"}
    if set(replaced) != expected_tokens:
        raise AssertionError(f"unexpected replaced set: {set(replaced)}")

    if "notable incident" not in out:
        raise AssertionError("expected replacement for shocking -> notable")
    if "major issue for users" not in out:
        raise AssertionError("expected replacement for disaster -> major issue")
    if "scheme or misconduct" not in out:
        raise AssertionError("expected replacement for scam/fraud")
    if not re.search(r"\billegal\b.*\breported\b", out, flags=re.IGNORECASE):
        raise AssertionError("expected replacement for criminal/caught")
    if "revealed then disrupted" not in out:
        raise AssertionError("expected replacement for exposed/destroyed")

    # href attribute should not be modified.
    if 'href="https://example.com/scam-report"' not in out:
        raise AssertionError("href attribute was unexpectedly modified")

    print("PASS: clickbait sanitizer neutralizes body tokens and keeps href intact")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
