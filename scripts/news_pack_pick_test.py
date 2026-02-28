from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.news_pack_picker import NewsPackPicker
from core.settings import load_settings


def main() -> int:
    settings = load_settings(ROOT / "config" / "settings.yaml")
    picker = NewsPackPicker(
        root=ROOT,
        manifest_path=settings.news_pack.manifest_path,
    )
    result = picker.pick_for_post(tags=["platform", "ai"], thumb_count=1, inline_count=4)
    payload = {
        "thumb_exists": bool(result.thumb_bg),
        "inline_count": len(result.inline_bg),
        "total_selected": len(result.all_images),
        "thumb_url": str((result.thumb_bg or {}).get("r2_url", "") if isinstance(result.thumb_bg, dict) else ""),
        "inline_urls": [str((x or {}).get("r2_url", "") or "") for x in (result.inline_bg or [])[:4]],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
