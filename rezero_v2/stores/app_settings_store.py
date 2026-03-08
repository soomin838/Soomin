from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from re_core.settings import AppSettings, load_settings


@dataclass
class V2RuntimeConfig:
    default_version: str = 'v1'
    v2_enabled: bool = True
    mix_hot: int = 2
    mix_search_derived: int = 2
    mix_evergreen: int = 1
    hot_min: int = 700
    hot_max: int = 1000
    search_derived_min: int = 1100
    search_derived_max: int = 1500
    evergreen_min: int = 1600
    evergreen_max: int = 2200
    pollinations_model: str = 'flux'
    allow_inline_optional: bool = True
    allow_reuse: bool = False
    allow_library_fallback: bool = False
    allow_news_pack_fallback: bool = False


class AppSettingsStore:
    def __init__(self, settings_path: Path) -> None:
        self.settings_path = Path(settings_path).resolve()

    def load_app_settings(self) -> AppSettings:
        return load_settings(self.settings_path)

    def load_v2_config(self) -> V2RuntimeConfig:
        settings = self.load_app_settings()
        mix = getattr(getattr(settings, 'v2', None), 'content_mix', None)
        lengths = getattr(getattr(settings, 'v2', None), 'content_lengths', None)
        image_policy = getattr(getattr(settings, 'v2', None), 'image_policy', None)
        runtime = getattr(settings, 'runtime', None)
        return V2RuntimeConfig(
            default_version=str(getattr(runtime, 'default_version', 'v1') or 'v1'),
            v2_enabled=bool(getattr(runtime, 'v2_enabled', True)),
            mix_hot=int(getattr(mix, 'hot', 2) or 2),
            mix_search_derived=int(getattr(mix, 'search_derived', 2) or 2),
            mix_evergreen=int(getattr(mix, 'evergreen', 1) or 1),
            hot_min=int(getattr(lengths, 'hot_min', 700) or 700),
            hot_max=int(getattr(lengths, 'hot_max', 1000) or 1000),
            search_derived_min=int(getattr(lengths, 'search_derived_min', 1100) or 1100),
            search_derived_max=int(getattr(lengths, 'search_derived_max', 1500) or 1500),
            evergreen_min=int(getattr(lengths, 'evergreen_min', 1600) or 1600),
            evergreen_max=int(getattr(lengths, 'evergreen_max', 2200) or 2200),
            pollinations_model=str(getattr(image_policy, 'model', 'flux') or 'flux'),
            allow_inline_optional=bool(getattr(image_policy, 'allow_inline_optional', True)),
            allow_reuse=bool(getattr(image_policy, 'allow_reuse', False)),
            allow_library_fallback=bool(getattr(image_policy, 'allow_library_fallback', False)),
            allow_news_pack_fallback=bool(getattr(image_policy, 'allow_news_pack_fallback', False)),
        )

    def save_v2_config(self, config: V2RuntimeConfig) -> None:
        raw = yaml.safe_load(self.settings_path.read_text(encoding='utf-8')) or {}
        raw.setdefault('runtime', {})
        raw['runtime']['default_version'] = str(config.default_version or 'v1')
        raw['runtime']['v2_enabled'] = bool(config.v2_enabled)
        raw.setdefault('v2', {})
        raw['v2']['content_mix'] = {'hot': int(config.mix_hot), 'search_derived': int(config.mix_search_derived), 'evergreen': int(config.mix_evergreen)}
        raw['v2']['content_lengths'] = {'hot_min': int(config.hot_min), 'hot_max': int(config.hot_max), 'search_derived_min': int(config.search_derived_min), 'search_derived_max': int(config.search_derived_max), 'evergreen_min': int(config.evergreen_min), 'evergreen_max': int(config.evergreen_max)}
        raw['v2']['image_policy'] = {'provider': 'pollinations', 'model': str(config.pollinations_model or 'flux'), 'allow_inline_optional': bool(config.allow_inline_optional), 'allow_reuse': bool(config.allow_reuse), 'allow_library_fallback': bool(config.allow_library_fallback), 'allow_news_pack_fallback': bool(config.allow_news_pack_fallback)}
        self.settings_path.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding='utf-8')
