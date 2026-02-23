from __future__ import annotations

import os
from pathlib import Path

import yaml


def _load_key() -> str:
    app_cfg = Path(os.environ.get("APPDATA", "")) / "RezeroAgent" / "config" / "settings.yaml"
    if app_cfg.exists():
        raw = yaml.safe_load(app_cfg.read_text(encoding="utf-8")) or {}
        key = str((raw.get("gemini") or {}).get("api_key") or "").strip()
        if key:
            return key
    return str(os.environ.get("GEMINI_API_KEY", "")).strip()


def main() -> int:
    key = _load_key()
    if not key:
        print("NO_API_KEY")
        return 1

    try:
        import google.generativeai as genai  # type: ignore

        genai.configure(api_key=key)
        for model in genai.list_models():
            name = getattr(model, "name", "") or ""
            print(name)
        return 0
    except Exception as exc:
        print(f"GENAI_SDK_FAILED: {exc}")
        print("FALLBACK: requests /v1beta/models")
        import requests

        r = requests.get(
            "https://generativelanguage.googleapis.com/v1beta/models",
            params={"key": key},
            timeout=40,
        )
        print(f"HTTP_STATUS={r.status_code}")
        if r.status_code != 200:
            print((r.text or "")[:500])
            return 2
        for item in (r.json().get("models", []) or []):
            print(item.get("name", ""))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

