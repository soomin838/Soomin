from __future__ import annotations

import io
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
import yaml
from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
PROMPT_DIR = ROOT / "ui" / "assets" / "prompts"
ASSET_ROOT = ROOT / "ui" / "assets" / "generated"
LOG_PATH = ROOT / "storage" / "logs" / "ui_asset_generation.jsonl"
MANIFEST_PATH = ASSET_ROOT / "manifest.json"
MODEL_ID = "gptimage"


def _log(payload: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    row = dict(payload or {})
    row["ts"] = datetime.now(timezone.utc).isoformat()
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _load_json(path: Path) -> list[dict]:
    try:
        return list(json.loads(path.read_text(encoding="utf-8")) or [])
    except Exception:
        return []


def _load_settings() -> dict:
    path = ROOT / "config" / "settings.yaml"
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _unlink_if_exists(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def _reset_generated_dirs() -> None:
    for name in ("icons", "backgrounds", "animations"):
        d = ASSET_ROOT / name
        d.mkdir(parents=True, exist_ok=True)
        for p in d.glob("*"):
            if p.is_file():
                _unlink_if_exists(p)


def _pollinations_image(
    *,
    prompt: str,
    width: int,
    height: int,
    api_key: str,
    base_url: str,
    timeout: int = 90,
) -> tuple[Image.Image | None, dict]:
    if not api_key or api_key.upper() in {"POLLINATIONS_API_KEY", "YOUR_POLLINATIONS_API_KEY"}:
        return None, {"reason": "missing_api_key"}
    encoded = quote(prompt, safe="")
    endpoint = f"{base_url.rstrip('/')}/image/{encoded}"
    try:
        resp = requests.get(
            endpoint,
            params={
                "model": MODEL_ID,
                "width": str(width),
                "height": str(height),
                "safe": "true",
                "enhance": "true",
                "nologo": "true",
                "seed": str(random.randint(1, 2_000_000_000)),
                "key": api_key,
            },
            headers={"Accept": "image/*"},
            timeout=timeout,
        )
        ctype = str(resp.headers.get("content-type", "")).lower()
        if resp.status_code != 200:
            return None, {"reason": "http_error", "status": int(resp.status_code), "body": (resp.text or "")[:260]}
        if not ctype.startswith("image/"):
            return None, {
                "reason": "non_image_response",
                "status": int(resp.status_code),
                "content_type": ctype,
                "body": (resp.text or "")[:260],
            }
        with Image.open(io.BytesIO(resp.content)).convert("RGBA") as im:
            return im.copy(), {"status": 200, "content_type": ctype}
    except Exception as exc:
        return None, {"reason": "exception", "error": str(exc)}


def _save_image(image: Image.Image, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.suffix.lower() in {".jpg", ".jpeg"}:
        image.convert("RGB").save(out_path, quality=90, optimize=True, progressive=True)
    else:
        image.save(out_path)


def _generate_one_asset(
    *,
    asset_type: str,
    name: str,
    prompt: str,
    width: int,
    height: int,
    out_path: Path,
    api_key: str,
    base_url: str,
    extra: dict | None = None,
) -> dict:
    image, meta = _pollinations_image(
        prompt=prompt,
        width=width,
        height=height,
        api_key=api_key,
        base_url=base_url,
    )
    if image is None:
        _unlink_if_exists(out_path)
        row = {
            "type": asset_type,
            "name": name,
            "path": str(out_path.relative_to(ROOT)),
            "status": "error",
            "provider": "pollinations",
            "model": MODEL_ID,
            "error": meta,
        }
        if extra:
            row.update(extra)
        _log({"event": "ui_asset_failed", **row})
        return row
    _save_image(image, out_path)
    row = {
        "type": asset_type,
        "name": name,
        "path": str(out_path.relative_to(ROOT)),
        "status": "generated",
        "provider": "pollinations",
        "model": MODEL_ID,
    }
    if extra:
        row.update(extra)
    _log({"event": "ui_asset_generated", **row})
    return row


def _generate_sequence(
    *,
    seq_name: str,
    base_prompt: str,
    frame_count: int,
    width: int,
    height: int,
    out_dir: Path,
    api_key: str,
    base_url: str,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    frame_rows: list[dict] = []
    frame_paths: list[str] = []
    failures = 0
    for idx in range(frame_count):
        out = out_dir / f"{seq_name}_{idx:02d}.png"
        prompt = (
            f"{base_prompt} "
            f"Frame {idx + 1} of {frame_count}. "
            "Keep same character identity and style as previous frame. "
            "No text."
        )
        row = _generate_one_asset(
            asset_type="animation_frame",
            name=f"{seq_name}_{idx:02d}",
            prompt=prompt,
            width=width,
            height=height,
            out_path=out,
            api_key=api_key,
            base_url=base_url,
            extra={"sequence": seq_name, "frame_index": idx, "frame_count": frame_count},
        )
        frame_rows.append(row)
        if row.get("status") == "generated":
            frame_paths.append(str(out.relative_to(ROOT)))
        else:
            failures += 1
        # Free-tier 안정화
        time.sleep(1.5)

    seq_status = "generated" if failures == 0 else ("partial" if frame_paths else "error")
    summary = {
        "type": "animation",
        "name": seq_name,
        "status": seq_status,
        "provider": "pollinations",
        "model": MODEL_ID,
        "frame_count": frame_count,
        "generated_frames": len(frame_paths),
        "paths": frame_paths,
        "failures": failures,
    }
    _log({"event": "ui_asset_sequence_summary", **summary})
    return summary


def main() -> int:
    ASSET_ROOT.mkdir(parents=True, exist_ok=True)
    _reset_generated_dirs()
    settings = _load_settings()
    visual = dict(settings.get("visual", {}) or {})

    env_key = os.getenv("POLLINATIONS_API_KEY", "").strip()
    api_key = env_key or str(visual.get("pollinations_api_key", "") or "").strip()
    base_url = str(visual.get("pollinations_base_url", "https://gen.pollinations.ai") or "https://gen.pollinations.ai")

    manifest: list[dict] = []

    if not api_key or api_key.upper() in {"POLLINATIONS_API_KEY", "YOUR_POLLINATIONS_API_KEY"}:
        msg = "Pollinations API key is missing. API-only policy blocks local fallback."
        _log({"event": "ui_asset_generation_blocked", "reason": "missing_api_key"})
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "asset_count": 0,
            "api_only": True,
            "provider": "pollinations",
            "model": MODEL_ID,
            "error": {"reason": "missing_api_key", "message": msg},
            "assets": [],
        }
        MANIFEST_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(msg)
        print(f"Manifest: {MANIFEST_PATH}")
        return 0

    icon_specs = _load_json(PROMPT_DIR / "icons.json")
    for spec in icon_specs:
        name = str((spec or {}).get("name", "") or "").strip()
        prompt = str((spec or {}).get("prompt", "") or "").strip()
        if not name or not prompt:
            continue
        out = ASSET_ROOT / "icons" / f"{name}.png"
        manifest.append(
            _generate_one_asset(
                asset_type="icon",
                name=name,
                prompt=prompt,
                width=512,
                height=512,
                out_path=out,
                api_key=api_key,
                base_url=base_url,
            )
        )
        time.sleep(1.2)

    bg_specs = _load_json(PROMPT_DIR / "backgrounds.json")
    for spec in bg_specs:
        name = str((spec or {}).get("name", "") or "").strip()
        prompt = str((spec or {}).get("prompt", "") or "").strip()
        if not name or not prompt:
            continue
        out = ASSET_ROOT / "backgrounds" / f"{name}.png"
        row = _generate_one_asset(
            asset_type="background",
            name=name,
            prompt=prompt,
            width=1920,
            height=1080,
            out_path=out,
            api_key=api_key,
            base_url=base_url,
        )
        # 강제로 규격 보정
        if row.get("status") == "generated":
            try:
                with Image.open(out).convert("RGBA") as im:
                    im.resize((1920, 1080), Image.Resampling.LANCZOS).save(out)
            except Exception as exc:
                row["status"] = "error"
                row["error"] = {"reason": "resize_failed", "error": str(exc)}
                _unlink_if_exists(out)
        manifest.append(row)
        time.sleep(1.2)

    mascot_specs = _load_json(PROMPT_DIR / "mascot.json")
    mascot_prompt = next(
        (str((s or {}).get("prompt", "") or "").strip() for s in mascot_specs if "mascot" in str((s or {}).get("name", "") or "")),
        "Cute minimal mascot character for a macOS widget, transparent background, no text.",
    )
    manifest.append(
        _generate_sequence(
            seq_name="mascot_idle",
            base_prompt=mascot_prompt,
            frame_count=6,
            width=768,
            height=768,
            out_dir=ASSET_ROOT / "animations",
            api_key=api_key,
            base_url=base_url,
        )
    )

    pipeline_specs = _load_json(PROMPT_DIR / "pipeline.json")
    pipeline_prompt = next(
        (str((s or {}).get("prompt", "") or "").strip() for s in pipeline_specs if "pipeline" in str((s or {}).get("name", "") or "")),
        "Progress pipeline animation icons with glowing dots, minimal Apple-style, transparent background, no text.",
    )
    manifest.append(
        _generate_sequence(
            seq_name="pipeline_glow",
            base_prompt=pipeline_prompt,
            frame_count=5,
            width=1024,
            height=256,
            out_dir=ASSET_ROOT / "animations",
            api_key=api_key,
            base_url=base_url,
        )
    )

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "asset_count": len(manifest),
        "api_only": True,
        "provider": "pollinations",
        "model": MODEL_ID,
        "assets": manifest,
    }
    MANIFEST_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"UI asset generation complete: {len(manifest)} entries")
    print(f"Manifest: {MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        _log({"event": "ui_asset_generation_unhandled_error", "error": str(exc)})
        print(f"UI asset generation failed: {exc}")
        raise SystemExit(0)
