from __future__ import annotations

import json
from typing import Any

import requests


class OllamaClient:
    def __init__(self, *, base_url: str, model: str, timeout_sec: int = 20) -> None:
        self.base_url = str(base_url or 'http://127.0.0.1:11434').rstrip('/')
        self.model = str(model or 'qwen2.5:3b').strip()
        self.timeout_sec = max(3, int(timeout_sec or 20))

    def generate_json(self, *, system_prompt: str, payload: dict[str, Any], purpose: str) -> dict[str, Any]:
        body = {
            'model': self.model,
            'stream': False,
            'format': 'json',
            'messages': [
                {'role': 'system', 'content': str(system_prompt or '')},
                {'role': 'user', 'content': json.dumps(payload, ensure_ascii=False)},
            ],
        }
        response = requests.post(f"{self.base_url}/api/chat", json=body, timeout=self.timeout_sec)
        response.raise_for_status()
        data = response.json() or {}
        message = (((data.get('message') or {}).get('content')) or '').strip()
        if not message:
            raise RuntimeError(f'ollama_empty_response:{purpose}')
        parsed = json.loads(message)
        if not isinstance(parsed, dict):
            raise RuntimeError(f'ollama_non_dict_json:{purpose}')
        return parsed
