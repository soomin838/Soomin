from __future__ import annotations

import json
from typing import Any

import requests


class GeminiClient:
    def __init__(self, *, api_key: str, model: str, timeout_sec: int = 45) -> None:
        self.api_key = str(api_key or '').strip()
        self.model = str(model or 'gemini-2.0-flash').strip()
        self.timeout_sec = max(10, int(timeout_sec or 45))

    def generate_text(self, *, system_prompt: str, user_payload: dict[str, Any]) -> str:
        if not self.api_key:
            raise RuntimeError('gemini_api_key_missing')
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"
        body = {
            'systemInstruction': {'parts': [{'text': str(system_prompt or '')}]},
            'contents': [{'role': 'user', 'parts': [{'text': json.dumps(user_payload, ensure_ascii=False)}]}],
            'generationConfig': {'temperature': 0.7, 'topP': 0.9},
        }
        response = requests.post(endpoint, json=body, timeout=self.timeout_sec)
        response.raise_for_status()
        data = response.json() or {}
        for candidate in data.get('candidates', []) or []:
            for part in (((candidate or {}).get('content') or {}).get('parts') or []):
                text = str((part or {}).get('text', '') or '').strip()
                if text:
                    return text
        raise RuntimeError('gemini_empty_text')
