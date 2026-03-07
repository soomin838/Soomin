from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.news_pack_seeder import NewsPackSeeder
from re_core.ollama_client import OllamaClient
from re_core.settings import load_settings


def main() -> int:
    settings = load_settings(ROOT / "config" / "settings.yaml")
    ollama = OllamaClient(
        settings.local_llm,
        log_path=ROOT / "storage" / "logs" / "ollama_calls.jsonl",
    )
    seeder = NewsPackSeeder(
        root=ROOT,
        settings=settings.news_pack,
        ollama_client=ollama,
        gemini_api_key=settings.gemini.api_key,
        gemini_model=settings.gemini.model,
        r2_config=settings.publish.r2,
    )
    result = seeder.seed_one_tick(force=True)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
