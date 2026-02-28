from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.settings import load_settings
from core.workflow import AgentWorkflow


def main() -> int:
    settings = load_settings(ROOT / "config" / "settings.yaml")
    wf = AgentWorkflow(root=ROOT, settings=settings)
    note = wf._refresh_news_pool_if_needed(force=True)  # noqa: SLF001
    queued = wf.news_pool_store.queued_count(days=max(1, int(settings.sources.news_pool_days or 7)))
    print(f"refresh_note={note}")
    print(f"queued_count={queued}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
