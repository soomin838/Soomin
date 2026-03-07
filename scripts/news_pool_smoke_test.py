from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.settings import load_settings
from re_core.workflow import AgentWorkflow


def main() -> int:
    settings = load_settings(ROOT / "config" / "settings.yaml")
    wf = AgentWorkflow(root=ROOT, settings=settings)
    print(wf._refresh_news_pool_if_needed(force=True))  # noqa: SLF001
    claimed = wf._claim_news_item(force_refresh_once=False)  # noqa: SLF001
    if not claimed:
        print("claim=none")
        return 1
    cid = int((claimed or {}).get("id", 0) or 0)
    print(f"claimed_id={cid} title={(claimed or {}).get('title', '')}")
    rolled = wf.news_pool_store.rollback_claim(cid)
    print(f"rollback={rolled}")
    return 0 if rolled else 2


if __name__ == "__main__":
    raise SystemExit(main())
