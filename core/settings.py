from __future__ import annotations

from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ScheduleSettings:
    interval_hours: float = 2.4
    min_interval_hours: float = 1.0
    max_interval_hours: float = 4.5
    jitter_minutes: int = 30
    max_retry_backoff_minutes: int = 30


@dataclass
class SourceSettings:
    mode: str = "mixed"
    seeds_path: str = "storage/seeds/seeds.json"
    stackexchange_site: str = "superuser"
    stackexchange_tagged: str = "windows-11;macos;iphone;android;audio;drivers;networking"
    stackexchange_min_score: int = 3
    hn_min_score: int = 30
    github_repos: list[str] = field(default_factory=list)
    github_min_reactions: int = 2
    github_token: str = ""
    max_candidates: int = 20


@dataclass
class GeminiSettings:
    api_key: str = ""
    model: str = "gemini-2.0-flash"
    fallback_models: list[str] = field(
        default_factory=lambda: [
            "gemini-2.0-flash",
            "gemini-2.5-flash-lite",
            "gemini-2.5-flash",
        ]
    )
    min_publish_score: int = 70
    editor_persona: str = (
        "You are a native English tech influencer writing for US/UK/global office workers. "
        "Your tone is human, clear, practical, and experience-driven."
    )
    max_calls_per_day: int = 50
    max_calls_per_run: int = 3
    min_request_interval_seconds: int = 30


@dataclass
class VisualSettings:
    target_images_per_post: int = 2
    max_banner_images: int = 1
    max_inline_images: int = 1
    image_provider: str = "pollinations"
    screenshot_priority_keywords: list[str] = field(default_factory=list)
    enable_gemini_image_generation: bool = False
    gemini_image_model: str = "models/imagen-3.0-generate-001"
    gemini_prompt_model: str = "gemini-2.0-flash"
    allow_chart_fallback: bool = False
    image_request_interval_seconds: int = 20
    pollinations_enabled: bool = True
    pollinations_api_key: str = ""
    pollinations_base_url: str = "https://gen.pollinations.ai"
    pollinations_thumbnail_model: str = "gptimage"
    pollinations_content_model: str = "gptimage"
    pollinations_timeout_sec: int = 30
    thumbnail_ocr_verify: bool = False
    cache_dir: str = "storage/image_cache"
    fallback_banner: str = "assets/fallback/banner.png"
    fallback_inline: str = "assets/fallback/inline.png"


@dataclass
class BudgetSettings:
    free_mode: bool = True
    dry_run: bool = False
    daily_post_limit: int = 3
    daily_gemini_call_limit: int = 50


@dataclass
class PublishSettings:
    use_blogger_schedule: bool = True
    min_delay_minutes: int = 10
    max_delay_minutes: int = 45
    queue_horizon_hours: int = 120
    target_queue_size: int = 25
    random_min_gap_floor_minutes: int = 180
    random_min_gap_ceiling_minutes: int = 360
    daily_publish_cap: int = 5
    image_hosting_backend: str = "blogger_media"
    gcs_bucket_name: str = ""
    gcs_public_base_url: str = ""
    mobile_duplicate_block_enabled: bool = False
    enable_semantic_html: bool = True
    related_posts_min: int = 2
    related_posts_max: int = 3
    time_window_start: str = "09:00"
    time_window_end: str = "23:00"
    randomness_level: str = "medium"
    min_gap_minutes: int = 180
    quiet_hours_enabled: bool = True
    quiet_hours_start: str = "02:00"
    quiet_hours_end: str = "07:00"
    buffer_target_days: int = 5
    buffer_min_days: int = 3
    schedule_horizon_days: int = 14


@dataclass
class QualitySettings:
    enabled: bool = True
    strict_mode: bool = True
    min_quality_score: int = 91
    qa_retry_max_passes: int = 0
    humanity_enabled: bool = True
    humanity_weight_percent: int = 20
    humanity_min_soft_score: int = 70
    humanity_hard_fail_block: bool = True
    min_word_count: int = 1400
    max_word_count: int = 1900
    min_h2: int = 6
    min_h3: int = 2
    min_list_items: int = 6
    min_external_links: int = 2
    min_authority_links: int = 2
    min_external_links_tech_troubleshoot: int = 1
    min_authority_links_tech_troubleshoot: int = 1
    banned_markers: list[str] = field(
        default_factory=lambda: [
            "delve",
            "comprehensive",
            "cutting-edge",
            "in conclusion",
        ]
    )
    llm_judge_enabled: bool = True
    llm_judge_min_score: int = 70
    prompt_leak_patterns: list[str] = field(
        default_factory=lambda: [
            "real-time trend focus",
            "write plain business language",
            "you are a system that",
            "for generated image context",
            "for quick take you are",
            "section context visual",
            "concept visual",
            "supporting chart",
        ]
    )
    disallowed_terms_office_experiment: list[str] = field(
        default_factory=lambda: [
            "rollback",
            "rollback criteria",
            "incident count",
            "staging",
            "prod parity",
            "sre",
            "deployment pipeline",
            "latency budget",
            "on-call",
            "incident severity",
        ]
    )
    disallowed_terms_tech_troubleshoot: list[str] = field(
        default_factory=lambda: [
            "why everyone is talking",
            "productivity breakthrough",
            "innovation culture",
            "team morale",
            "office influencer",
            "viral trend",
            "executive context",
            "authority snapshot",
            "action framework",
            "decision criteria",
        ]
    )
    require_story_block: bool = True
    require_story_block_min_count: int = 1
    prompt_example_section_titles: list[str] = field(
        default_factory=lambda: [
            "prompt example",
            "prompt examples",
            "example prompt",
            "try this",
            "prompt snippets",
        ]
    )
    meta_block_start: str = "[[META]]"
    meta_block_end: str = "[[/META]]"
    partial_fix_enabled: bool = True
    partial_fix_story_first: bool = True
    ban_phrases: list[str] = field(
        default_factory=lambda: [
            "why everyone is talking",
            "in today's fast-paced world",
        ]
    )
    ban_formats: list[str] = field(default_factory=lambda: ["FAQ", "Q:", "A:"])
    forbid_screenshot_mentions: bool = True
    sensitive_topics_hard_filter: bool = True
    max_question_marks_in_row: int = 2
    fail_if_intro_matches_alt: bool = True
    alt_similarity_threshold: float = 0.75
    banned_debug_patterns: list[str] = field(
        default_factory=lambda: [
            "workflow checkpoint stage",
            "av reference context",
            "jobtitle",
            "sameas",
            "selected topic",
            "source trending_entities",
        ]
    )


@dataclass
class TopicGrowthSettings:
    enabled: bool = True
    daily_new_topics: int = 10
    min_seed_score: int = 75


@dataclass
class KeywordPoolSettings:
    enabled: bool = True
    daily_target: int = 100
    refill_threshold: int = 20
    active_pool_max: int = 250
    pick_per_run: int = 5
    retry_per_run_when_under_target: int = 0


@dataclass
class IntegrationSettings:
    enabled: bool = True
    refresh_minutes: int = 15
    adsense_enabled: bool = True
    analytics_enabled: bool = True
    search_console_enabled: bool = True
    ga4_property_id: str = ""
    search_console_site_url: str = ""


@dataclass
class BloggerSettings:
    blog_id: str = ""
    credentials_path: str = "config/blogger_token.json"


@dataclass
class IndexingSettings:
    enabled: bool = True
    service_account_path: str = "config/service_account.json"
    daily_quota: int = 200


@dataclass
class ContentPolicySettings:
    language: str = "en-US"
    enforce_english_only: bool = True
    reading_level: str = "US_G7_G10"
    min_words: int = 1400
    max_words: int = 1900


@dataclass
class TopicsPolicySettings:
    monthly_rotation_enabled: bool = True
    rotation_order: list[str] = field(default_factory=lambda: ["windows", "mac", "iphone", "galaxy"])
    audio_posts_enabled: bool = True
    audio_posts_frequency_days: int = 7


@dataclass
class LLMPolicySettings:
    provider: str = "gemini"
    max_calls_per_post: int = 3
    enable_refine_loop: bool = False
    enable_judge_post: bool = False
    enable_image_generation: bool = False


@dataclass
class LocalLLMSettings:
    enabled: bool = True
    provider: str = "ollama"
    model: str = "qwen2.5:3b"
    base_url: str = "http://127.0.0.1:11434"
    num_ctx: int = 2048
    num_thread: int = 2
    max_loaded_models: int = 1
    num_parallel: int = 1
    install_if_missing: bool = True
    pull_model_if_missing: bool = True
    request_timeout_sec: int = 60
    max_calls_per_post: int = 2


@dataclass
class ImageSourcesPolicySettings:
    reddit_enabled: bool = True
    stackoverflow_enabled: bool = True
    forums_enabled: bool = False
    templates_enabled: bool = True


@dataclass
class ImagesPolicySettings:
    provider: str = "pollinations"
    banner_count: int = 1
    inline_count: int = 1
    cache_dir: str = "storage/image_cache"
    fallback_banner: str = "assets/fallback/banner.png"
    fallback_inline: str = "assets/fallback/inline.png"


@dataclass
class InternalLinksPolicySettings:
    enabled: bool = True
    body_link_count: int = 1
    related_link_count: int = 2
    overlap_threshold: float = 0.4


@dataclass
class KeywordsPolicySettings:
    db_path: str = "storage/keywords.sqlite"
    refill_threshold_per_device: int = 100
    avoid_reuse_days: int = 30
    sources: ImageSourcesPolicySettings = field(default_factory=ImageSourcesPolicySettings)


@dataclass
class AppSettings:
    timezone: str = "America/New_York"
    schedule: ScheduleSettings = field(default_factory=ScheduleSettings)
    sources: SourceSettings = field(default_factory=SourceSettings)
    gemini: GeminiSettings = field(default_factory=GeminiSettings)
    visual: VisualSettings = field(default_factory=VisualSettings)
    budget: BudgetSettings = field(default_factory=BudgetSettings)
    publish: PublishSettings = field(default_factory=PublishSettings)
    quality: QualitySettings = field(default_factory=QualitySettings)
    topic_growth: TopicGrowthSettings = field(default_factory=TopicGrowthSettings)
    keyword_pool: KeywordPoolSettings = field(default_factory=KeywordPoolSettings)
    integrations: IntegrationSettings = field(default_factory=IntegrationSettings)
    blogger: BloggerSettings = field(default_factory=BloggerSettings)
    indexing: IndexingSettings = field(default_factory=IndexingSettings)
    content: ContentPolicySettings = field(default_factory=ContentPolicySettings)
    topics: TopicsPolicySettings = field(default_factory=TopicsPolicySettings)
    llm: LLMPolicySettings = field(default_factory=LLMPolicySettings)
    local_llm: LocalLLMSettings = field(default_factory=LocalLLMSettings)
    images: ImagesPolicySettings = field(default_factory=ImagesPolicySettings)
    internal_links: InternalLinksPolicySettings = field(default_factory=InternalLinksPolicySettings)
    keywords: KeywordsPolicySettings = field(default_factory=KeywordsPolicySettings)
    authority_links: list[str] = field(default_factory=list)
    windows: dict[str, Any] = field(default_factory=dict)


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _construct_dc(dc_type, data: dict[str, Any] | None):
    raw = data or {}
    allowed = {f.name for f in fields(dc_type)}
    filtered = {k: v for k, v in raw.items() if k in allowed}
    return dc_type(**filtered)


def load_settings(path: Path) -> AppSettings:
    raw = _load_yaml(path)
    quality_raw = dict(raw.get("quality", {}) or {})
    qa_raw = raw.get("qa", {}) or {}
    content_raw = dict(raw.get("content", {}) or {})
    topics_raw = dict(raw.get("topics", {}) or {})
    publishing_raw = dict(raw.get("publishing", {}) or {})
    llm_raw = dict(raw.get("llm", {}) or {})
    local_llm_raw = dict(raw.get("local_llm", {}) or {})
    images_raw = dict(raw.get("images", {}) or {})
    internal_links_raw = dict(raw.get("internal_links", {}) or {})
    keywords_raw = dict(raw.get("keywords", {}) or {})
    keyword_sources_raw = dict((keywords_raw.get("sources", {}) or {}))

    if isinstance(qa_raw, dict):
        if "prompt_leak_patterns" in qa_raw:
            quality_raw["prompt_leak_patterns"] = qa_raw.get("prompt_leak_patterns", [])
        disallowed = qa_raw.get("disallowed_terms", {}) or {}
        if isinstance(disallowed, dict) and "office_experiment" in disallowed:
            quality_raw["disallowed_terms_office_experiment"] = disallowed.get("office_experiment", [])
        if isinstance(disallowed, dict) and "tech_troubleshoot" in disallowed:
            quality_raw["disallowed_terms_tech_troubleshoot"] = disallowed.get("tech_troubleshoot", [])
        story = qa_raw.get("require_story_block", {}) or {}
        if isinstance(story, dict):
            if "enabled" in story:
                quality_raw["require_story_block"] = bool(story.get("enabled"))
            if "min_count" in story:
                try:
                    quality_raw["require_story_block_min_count"] = int(story.get("min_count"))
                except Exception:
                    pass
        if "fail_if_intro_matches_alt" in qa_raw:
            quality_raw["fail_if_intro_matches_alt"] = bool(qa_raw.get("fail_if_intro_matches_alt"))
        if "alt_similarity_threshold" in qa_raw:
            try:
                quality_raw["alt_similarity_threshold"] = float(qa_raw.get("alt_similarity_threshold"))
            except Exception:
                pass
        if "banned_debug_patterns" in qa_raw:
            quality_raw["banned_debug_patterns"] = [
                str(x).strip() for x in (qa_raw.get("banned_debug_patterns") or []) if str(x).strip()
            ]

    # Mirror spec blocks into runtime-compatible legacy settings.
    if content_raw:
        quality_raw["min_word_count"] = int(content_raw.get("min_words", quality_raw.get("min_word_count", 1400)))
        quality_raw["max_word_count"] = int(content_raw.get("max_words", quality_raw.get("max_word_count", 1900)))
    if llm_raw:
        raw.setdefault("gemini", {})
        raw["gemini"]["max_calls_per_run"] = int(llm_raw.get("max_calls_per_post", raw["gemini"].get("max_calls_per_run", 3)))
        if "enable_image_generation" in llm_raw:
            raw.setdefault("visual", {})
            raw["visual"]["enable_gemini_image_generation"] = bool(llm_raw.get("enable_image_generation"))
    if images_raw:
        raw.setdefault("visual", {})
        banner_count = int(images_raw.get("banner_count", 1))
        inline_count = int(images_raw.get("inline_count", 1))
        raw["visual"]["target_images_per_post"] = max(1, banner_count + inline_count)
        raw["visual"]["max_banner_images"] = max(1, banner_count)
        raw["visual"]["max_inline_images"] = max(0, inline_count)
        raw["visual"]["cache_dir"] = str(images_raw.get("cache_dir", "storage/image_cache"))
        raw["visual"]["fallback_banner"] = str(images_raw.get("fallback_banner", "assets/fallback/banner.png"))
        raw["visual"]["fallback_inline"] = str(images_raw.get("fallback_inline", "assets/fallback/inline.png"))
        # Strict split: images provider is always pollinations.
        raw["visual"]["image_provider"] = "pollinations"
        pollinations_raw = dict(images_raw.get("pollinations", {}) or {})
        pollinations_model = str(pollinations_raw.get("model", "gptimage") or "gptimage").strip() or "gptimage"
        raw["visual"]["pollinations_thumbnail_model"] = "gptimage" if pollinations_model.lower() != "gptimage" else pollinations_model
        raw["visual"]["pollinations_content_model"] = "gptimage" if pollinations_model.lower() != "gptimage" else pollinations_model
        if "timeout_sec" in pollinations_raw:
            try:
                raw["visual"]["pollinations_timeout_sec"] = max(5, int(pollinations_raw.get("timeout_sec", 30)))
            except Exception:
                pass
    if publishing_raw:
        raw.setdefault("budget", {})
        raw.setdefault("publish", {})
        raw["budget"]["daily_post_limit"] = int(publishing_raw.get("posts_to_generate_per_day", raw["budget"].get("daily_post_limit", 3)))
        raw["publish"]["daily_publish_cap"] = int(publishing_raw.get("posts_to_publish_per_day", raw["publish"].get("daily_publish_cap", 2)))
        bdays = int(publishing_raw.get("buffer_target_days", 5))
        raw["publish"]["buffer_target_days"] = bdays
        raw["publish"]["buffer_min_days"] = int(publishing_raw.get("buffer_min_days", 3))
        horizon_days = int(publishing_raw.get("schedule_horizon_days", max(14, bdays)))
        raw["publish"]["schedule_horizon_days"] = max(1, horizon_days)
        raw["publish"]["queue_horizon_hours"] = max(24, int(raw["publish"]["schedule_horizon_days"]) * 24)
        raw["publish"]["target_queue_size"] = max(1, int(raw["publish"]["daily_publish_cap"]) * max(1, bdays))
        raw["publish"]["time_window_start"] = str(publishing_raw.get("time_window_start", "09:00"))
        raw["publish"]["time_window_end"] = str(publishing_raw.get("time_window_end", "23:00"))
        raw["publish"]["randomness_level"] = str(publishing_raw.get("randomness_level", "medium"))
        mgap = int(publishing_raw.get("min_gap_minutes", 180))
        raw["publish"]["min_gap_minutes"] = mgap
        raw["publish"]["random_min_gap_floor_minutes"] = mgap
        raw["publish"]["random_min_gap_ceiling_minutes"] = max(mgap + 120, mgap)
        raw["publish"]["quiet_hours_enabled"] = bool(publishing_raw.get("quiet_hours_enabled", True))
        raw["publish"]["quiet_hours_start"] = str(publishing_raw.get("quiet_hours_start", "02:00"))
        raw["publish"]["quiet_hours_end"] = str(publishing_raw.get("quiet_hours_end", "07:00"))
    if internal_links_raw:
        raw.setdefault("publish", {})
        raw["publish"]["related_posts_min"] = int(internal_links_raw.get("related_link_count", 2))
        raw["publish"]["related_posts_max"] = int(internal_links_raw.get("related_link_count", 2))
    if keywords_raw:
        raw.setdefault("keyword_pool", {})
        raw["keyword_pool"]["daily_target"] = int(keywords_raw.get("refill_threshold_per_device", 100))
        raw["keyword_pool"]["pick_per_run"] = max(1, int(raw.get("publishing", {}).get("posts_to_publish_per_day", 2)))

    return AppSettings(
        timezone=raw.get("timezone", "America/New_York"),
        schedule=_construct_dc(ScheduleSettings, raw.get("schedule", {})),
        sources=_construct_dc(SourceSettings, raw.get("sources", {})),
        gemini=_construct_dc(GeminiSettings, raw.get("gemini", {})),
        visual=_construct_dc(VisualSettings, raw.get("visual", {})),
        budget=_construct_dc(BudgetSettings, raw.get("budget", {})),
        publish=_construct_dc(PublishSettings, raw.get("publish", {})),
        quality=_construct_dc(QualitySettings, quality_raw),
        topic_growth=_construct_dc(TopicGrowthSettings, raw.get("topic_growth", {})),
        keyword_pool=_construct_dc(KeywordPoolSettings, raw.get("keyword_pool", {})),
        integrations=_construct_dc(IntegrationSettings, raw.get("integrations", {})),
        blogger=_construct_dc(BloggerSettings, raw.get("blogger", {})),
        indexing=_construct_dc(IndexingSettings, raw.get("indexing", {})),
        content=_construct_dc(ContentPolicySettings, content_raw),
        topics=_construct_dc(TopicsPolicySettings, topics_raw),
        llm=_construct_dc(LLMPolicySettings, llm_raw),
        local_llm=_construct_dc(LocalLLMSettings, local_llm_raw),
        images=_construct_dc(ImagesPolicySettings, images_raw),
        internal_links=_construct_dc(InternalLinksPolicySettings, internal_links_raw),
        keywords=KeywordsPolicySettings(
            db_path=str(keywords_raw.get("db_path", "storage/keywords.sqlite")),
            refill_threshold_per_device=int(keywords_raw.get("refill_threshold_per_device", 100)),
            avoid_reuse_days=int(keywords_raw.get("avoid_reuse_days", 30)),
            sources=_construct_dc(ImageSourcesPolicySettings, keyword_sources_raw),
        ),
        authority_links=raw.get("authority_links", []),
        windows=raw.get("windows", {"use_task_scheduler": True}),
    )

