from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.thumbnail_overlay import ThumbnailOverlayRenderer


def _find_source_image() -> Path | None:
    candidates = []
    for ext in ("*.png", "*.jpg", "*.jpeg", "*.webp"):
        candidates.extend((ROOT / "assets" / "news_pack_cache" / "thumb_bg").glob(f"**/{ext}"))
    if not candidates:
        for ext in ("*.png", "*.jpg", "*.jpeg", "*.webp"):
            candidates.extend((ROOT / "assets" / "library" / "generic").glob(ext))
    for p in candidates:
        if p.is_file():
            return p
    return None


def main() -> int:
    source = _find_source_image()
    if source is None:
        print(json.dumps({"status": "no_source_image"}, ensure_ascii=False, indent=2))
        return 0
    out_dir = ROOT / "storage" / "logs" / "thumb_overlay_test"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"overlay_{source.stem}.png"
    renderer = ThumbnailOverlayRenderer(
        style="yt_clean",
        font_paths=["C:/Windows/Fonts/segoeuib.ttf", "C:/Windows/Fonts/arialbd.ttf"],
        max_words=3,
    )
    rendered = renderer.render(
        source_path=source,
        hook_text="WHAT CHANGED",
        tag_label="platform",
        output_path=out_path,
    )
    print(
        json.dumps(
            {"status": "ok", "source": str(source), "rendered": str(rendered)},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
