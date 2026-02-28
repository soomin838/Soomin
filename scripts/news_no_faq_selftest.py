from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.quality import ContentQAGate
from core.settings import QualitySettings


def run() -> int:
    gate = ContentQAGate(QualitySettings(), authority_links=[])
    clean_html = (
        "<h2>Quick Take</h2><p>Summary</p>"
        "<h2>What Happened</h2><p>Details</p>"
    )
    faq_html = clean_html + "<h2>FAQ</h2><p>Q: test</p>"

    clean = gate.evaluate(clean_html, title="News test", domain="tech_news_explainer", phase="pre_images")
    bad = gate.evaluate(faq_html, title="News test", domain="tech_news_explainer", phase="pre_images")

    clean_has_faq = "faq_detected" in (clean.hard_failures or [])
    bad_has_faq = "faq_detected" in (bad.hard_failures or [])
    print(
        f"clean_ok={clean.ok if hasattr(clean, 'ok') else not clean.has_hard_failure} "
        f"clean_has_faq={clean_has_faq} "
        f"bad_has_faq={bad_has_faq}"
    )
    return 0 if (not clean_has_faq and bad_has_faq) else 1


if __name__ == "__main__":
    raise SystemExit(run())
