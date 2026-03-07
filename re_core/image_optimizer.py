from __future__ import annotations

import os
import tempfile
from pathlib import Path

from PIL import Image


def optimize_for_library(src: Path, dst_no_ext: Path, *, max_width: int = 1200, max_kb: int = 220) -> Path:
    dst_no_ext.parent.mkdir(parents=True, exist_ok=True)

    with Image.open(src) as im:
        im = im.convert("RGBA")

        if im.width > max_width:
            ratio = max_width / float(im.width)
            new_h = max(1, int(im.height * ratio))
            im = im.resize((max_width, new_h), Image.Resampling.LANCZOS)

        tmp_png_fd, tmp_png_raw = tempfile.mkstemp(suffix=".png")
        os.close(tmp_png_fd)
        tmp_png = Path(tmp_png_raw)
        try:
            try:
                q = im.quantize(colors=256, method=Image.MEDIANCUT).convert("RGBA")
            except Exception:
                q = im
            q.save(tmp_png, format="PNG", optimize=True, compress_level=9)
            if tmp_png.stat().st_size <= max_kb * 1024:
                out = dst_no_ext.with_suffix(".png")
                tmp_png.replace(out)
                return out
        finally:
            try:
                tmp_png.unlink(missing_ok=True)
            except Exception:
                pass

        rgb = Image.new("RGB", im.size, (255, 255, 255))
        rgb.paste(im, mask=im.split()[-1])

        for quality in (82, 78, 74, 70, 66):
            tmp_jpg_fd, tmp_jpg_raw = tempfile.mkstemp(suffix=".jpg")
            os.close(tmp_jpg_fd)
            tmp_jpg = Path(tmp_jpg_raw)
            try:
                rgb.save(tmp_jpg, format="JPEG", quality=quality, optimize=True, progressive=True)
                if tmp_jpg.stat().st_size <= max_kb * 1024:
                    out = dst_no_ext.with_suffix(".jpg")
                    tmp_jpg.replace(out)
                    return out
            finally:
                try:
                    tmp_jpg.unlink(missing_ok=True)
                except Exception:
                    pass

        out = dst_no_ext.with_suffix(".jpg")
        rgb.save(out, format="JPEG", quality=66, optimize=True, progressive=True)
        return out
