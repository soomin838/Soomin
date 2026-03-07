from __future__ import annotations

import random
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.settings import load_settings
from re_core.visual import VisualPipeline


def main() -> int:
    settings = load_settings(ROOT / "config" / "settings.yaml")
    visual = VisualPipeline(
        temp_dir=ROOT / "storage" / "temp_images",
        session_dir=ROOT / "storage" / "sessions",
        visual_settings=settings.visual,
        gemini_api_key=settings.gemini.api_key,
    )
    lib_root = ROOT / "assets" / "library"
    out_dir = ROOT / "storage" / "logs" / "thumbnail_overlay_test"
    out_dir.mkdir(parents=True, exist_ok=True)
    candidates = []
    for ext in ("*.png", "*.jpg", "*.jpeg", "*.webp"):
        candidates.extend(list(lib_root.rglob(ext)))
    candidates = [p for p in candidates if p.is_file()]
    if not candidates:
        print("no_library_images")
        return 1
    random.shuffle(candidates)
    picks = candidates[:5]
    for idx, src in enumerate(picks, start=1):
        dst = out_dir / f"overlay_{idx:02d}{src.suffix.lower() or '.png'}"
        shutil.copy2(src, dst)
        hook = visual.pick_thumbnail_hook("platform", src.stem)
        visual.apply_news_thumbnail_overlay(dst, hook)
        print(f"ok {dst.name} hook={hook}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
