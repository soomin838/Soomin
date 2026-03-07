from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
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
class MonthlySchedulerSettings:
    enabled: bool = True
    timezone: str = "America/New_York"
    output_dir: str = "storage/schedules"
    publish_slots_per_day: int = 5
    buffer_slots_min: int = 1
    buffer_slots_max: int = 2
    consume_hold_slots: bool = False


@dataclass
class SourceSettings:
    mode: str = "news_pool"
    seeds_path: str = "storage/seeds/seeds.json"
    stackexchange_site: str = "superuser"
    stackexchange_tagged: str = "windows-11;macos;iphone;android;audio;drivers;networking"
    stackexchange_sites: list[dict[str, str]] = field(
        default_factory=lambda: [
            {"site": "superuser", "tagged": "windows-11;wifi;bluetooth;audio;drivers;networking"},
            {"site": "apple", "tagged": "iphone;ios;macos;wifi;bluetooth;airpods;audio"},
            {"site": "android", "tagged": "android;wifi;bluetooth;audio;battery;updates"},
        ]
    )
    stackexchange_min_score: int = 3
    hn_min_score: int = 30
    github_repos: list[str] = field(default_factory=list)
    github_min_reactions: int = 2
    github_token: str = ""
    max_candidates: int = 20
    news_pool_days: int = 7
    news_pool_min_items: int = 80
    news_pool_max_items: int = 800
    news_pool_refresh_interval_minutes: int = 120
    news_pool_background_tick_enabled: bool = True
    news_pool_background_tick_minutes: int = 30
    news_pool_background_tick_jitter_sec: int = 20
    news_pool_background_max_feeds_per_tick: int = 5
    news_pool_feeds: list[str] = field(
        default_factory=lambda: [
            "https://techcrunch.com/feed/",
            "https://www.theverge.com/rss/index.xml",
            "https://www.wired.com/feed/rss",
            "https://feeds.arstechnica.com/arstechnica/index",
            "https://venturebeat.com/feed/",
            "https://www.cisa.gov/uscert/ncas/alerts.xml",
            "https://www.cisa.gov/uscert/ncas/current-activity.xml",
            "https://aws.amazon.com/blogs/security/feed/",
            "https://security.googleblog.com/feeds/posts/default",
            "https://news.ycombinator.com/rss",
        ]
    )
    news_pool_keywords_allow: list[str] = field(default_factory=list)
    news_pool_keywords_block: list[str] = field(default_factory=list)
    news_pool_source_weights: dict[str, float] = field(default_factory=dict)
    news_pool_pick_top_k: int = 60
    news_pool_keep_used_days: int = 30


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
    target_images_per_post: int = 5
    max_banner_images: int = 1
    max_inline_images: int = 4
    image_provider: str = "generated"
    screenshot_priority_keywords: list[str] = field(default_factory=list)
    enable_gemini_image_generation: bool = True
    gemini_image_model: str = "models/imagen-3.0-generate-001"
    gemini_prompt_model: str = "gemini-2.0-flash"
    allow_chart_fallback: bool = False
    image_request_interval_seconds: int = 20
    provider_order: list[str] = field(
        default_factory=lambda: ["airforce_imagen4", "gemini", "pollinations_auth", "pollinations_anon"]
    )
    airforce_enabled: bool = True
    airforce_api_key: str = ""
    airforce_base_url: str = "https://api.airforce"
    airforce_image_model: str = "imagen-4"
    airforce_timeout_sec: int = 45
    pollinations_enabled: bool = False
    pollinations_api_key: str = ""
    pollinations_base_url: str = ""
    pollinations_thumbnail_model: str = ""
    pollinations_content_model: str = ""
    pollinations_timeout_sec: int = 0
    allow_library_fallback: bool = False
    allow_rendered_fallback: bool = False
    generated_r2_prefix: str = "generated"
    thumbnail_ocr_verify: bool = False
    cache_dir: str = "storage/image_cache"
    fallback_banner: str = "assets/fallback/banner.png"
    fallback_inline: str = "assets/fallback/inline.png"
    prompt_suffix: str = "no text, no letters, no numbers, no logos, no watermark"
    thumbnail_text_overlay_enabled: bool = True
    thumbnail_text_style: str = "yt_clean"
    thumbnail_text_max_words: int = 4


@dataclass
class NewsPackSettings:
    enabled: bool = True
    daily_target_total: int = 10
    daily_target_thumb_bg: int = 4
    daily_target_inline_bg: int = 6
    interval_minutes_base: int = 150
    interval_minutes_jitter: int = 45
    bootstrap_min_interval_minutes: int = 10
    bootstrap_max_interval_minutes: int = 20
    min_ready_thumb_bg: int = 20
    min_ready_inline_bg: int = 60
    target_ready_thumb_bg: int = 40
    target_ready_inline_bg: int = 120
    emergency_fill_max_items: int = 3
    max_consecutive_failures: int = 5
    provider_order: list[str] = field(
        default_factory=lambda: ["airforce_imagen4", "pollinations_auth", "pollinations_anon", "gemini"]
    )
    airforce_api_key: str = ""
    airforce_base_url: str = "https://api.airforce"
    airforce_image_model: str = "imagen-4"
    airforce_timeout_sec: int = 45
    pollinations_api_key: str = ""
    pollinations_timeout_sec: int = 35
    gemini_fallback_enabled: bool = True
    gemini_fallback_daily_cap: int = 1
    allow_gemini_on_pollinations_rate_limit: bool = False
    r2_upload_enabled: bool = True
    r2_prefix: str = "news_pack"
    manifest_path: str = "storage/state/news_pack_manifest.jsonl"
    state_path: str = "storage/state/news_pack_state.json"
    thumb_hook_max_words: int = 4
    thumb_overlay_enabled: bool = True
    thumb_overlay_style: str = "yt_clean"
    thumb_overlay_font_paths: list[str] = field(
        default_factory=lambda: [
            "C:/Windows/Fonts/segoeuib.ttf",
            "C:/Windows/Fonts/arialbd.ttf",
        ]
    )
    tags: list[str] = field(
        default_factory=lambda: ["security", "policy", "ai", "platform", "mobile", "chips"]
    )


@dataclass
class BudgetSettings:
    free_mode: bool = True
    dry_run: bool = False
    daily_post_limit: int = 3
    daily_gemini_call_limit: int = 50


@dataclass
class PublishR2Settings:
    endpoint_url: str = ""
    bucket: str = ""
    access_key_id: str = ""
    secret_access_key: str = ""
    public_base_url: str = ""
    prefix: str = "library"
    cache_control: str = "public, max-age=31536000, immutable"


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
    image_hosting_backend: str = "r2"
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
    allow_inline_fallback_publish: bool = False
    allow_banner_fallback_publish: bool = True
    strict_thumbnail_blogger_media: bool = True
    thumbnail_data_uri_allowed: bool = False
    auto_allow_data_uri_on_blogger_405: bool = False
    thumbnail_preflight_only: bool = False
    thumbnail_preflight_max_cycles: int = 6
    thumbnail_preflight_retry_delay_sec: int = 8
    min_images_required: int = 0
    max_images_per_post: int = 5
    r2: PublishR2Settings = field(default_factory=PublishR2Settings)


@dataclass
class LedgerSettings:
    enabled: bool = True
    ttl_days: int = 90
    path: str = "storage/ledger/publish_ledger.jsonl"


@dataclass
class WorkflowSettings:
    retry_enabled: bool = True
    retry_max_attempts_per_event: int = 4
    retry_debounce_seconds: list[int] = field(default_factory=lambda: [0, 30, 120, 600])
    retry_reset_on_success: bool = True


@dataclass
class WatchdogSettings:
    enabled: bool = True
    max_same_hard_failure_streak: int = 3
    max_event_wallclock_minutes: int = 20
    max_event_total_attempts: int = 6
    max_global_holds_per_hour: int = 12
    max_provider_530_streak: int = 6
    max_provider_429_streak: int = 4
    backoff_on_provider_failure_minutes: dict[str, list[int]] = field(
        default_factory=lambda: {
            "http_530": [30, 60, 120],
            "http_429": [5, 15, 30],
        }
    )


@dataclass
class ReadabilitySettings:
    enabled: bool = True
    max_sentence_words: int = 25
    paragraph_sentence_min: int = 2
    paragraph_sentence_max: int = 5
    repeated_sentence_starter_max: int = 3
    transition_repeat_max: int = 1


@dataclass
class TitleDiversitySettings:
    enabled: bool = True
    patterns_total: int = 6
    cluster_pattern_ttl_days: int = 14
    numeric_ratio: float = 0.40
    question_ratio: float = 0.20
    analysis_ratio: float = 0.40
    min_title_chars: int = 45
    max_title_chars: int = 70


@dataclass
class SourceNaturalizationSettings:
    enabled: bool = True
    max_inline_attributions_per_article: int = 3
    allow_raw_urls_in_body: bool = False
    max_sources_list_items: int = 6
    require_sources_section: bool = True


@dataclass
class EntropyCheckSettings:
    enabled: bool = True
    trigram_max_ratio: float = 0.05
    starter_max_repeats: int = 3
    duplicate_h2_max: int = 0
    max_rewrite_attempts: int = 1


@dataclass
class QualitySettings:
    enabled: bool = True
    strict_mode: bool = True
    qa_mode: str = "full"
    min_quality_score: int = 91
    qa_retry_max_passes: int = 0
    humanity_enabled: bool = True
    humanity_weight_percent: int = 20
    humanity_min_soft_score: int = 70
    humanity_hard_fail_block: bool = True
    min_word_count: int = 1800
    max_word_count: int = 2200
    min_h2: int = 5
    min_h3: int = 0
    min_list_items: int = 3
    min_external_links: int = 1
    min_authority_links: int = 1
    min_external_links_news_interpretation: int = 1
    min_authority_links_news_interpretation: int = 0
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
            "source frame:",
            "main tradeoff:",
            "write from the reader's lived experience",
            "keep a one-line status update tied to",
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
    disallowed_terms_news_interpretation: list[str] = field(
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
    required_title_tokens_any: list[str] = field(
        default_factory=lambda: [
            "not working",
            "fix",
            "error",
            "after update",
        ]
    )


@dataclass
class ActionabilityGateSettings:
    enabled: bool = True
    min_steps: int = 8
    min_word_count: int = 1400
    max_generic_ratio: float = 0.012


@dataclass
class GenerationSettings:
    mode: str = "local_first"  # local_first | hybrid | cloud_first
    gemini_daily_budget_calls: int = 12
    gemini_only_on_fail: bool = True


@dataclass
class TopicGrowthSettings:
    enabled: bool = True
    daily_new_topics: int = 10
    min_seed_score: int = 75


@dataclass
class WorldMonitorSettings:
    enabled: bool = False
    api_key: str = ""
    prefer_api: bool = False
    timeout_sec: int = 15


@dataclass
class PolicyGateSettings:
    enabled: bool = True


@dataclass
class SearchLearningSettings:
    enabled: bool = True
    lookback_days: int = 14
    collection_interval_hours: int = 24
    max_rows_per_day: int = 50000


@dataclass
class SearchIntentSettings:
    enabled: bool = True
    provider: str = "ollama_then_rules"
    timeout_sec: int = 15


@dataclass
class StructureRandomizationSettings:
    enabled: bool = True
    similarity_threshold: float = 0.75
    fingerprint_ttl_days: int = 30
    max_attempts: int = 3


@dataclass
class ContentAllocationSettings:
    enabled: bool = True
    mix_hot: int = 2
    mix_search_derived: int = 2
    mix_evergreen: int = 1


@dataclass
class KeywordPoolSettings:
    enabled: bool = True
    daily_target: int = 100
    refill_threshold: int = 20
    active_pool_max: int = 250
    pick_per_run: int = 1
    retry_per_run_when_under_target: int = 0


@dataclass
class TopicPoolSettings:
    target_size: int = 200
    min_size: int = 140
    refill_batch: int = 80
    avoid_reuse_days: int = 30
    per_run_pick: int = 1


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
class ContentModeSettings:
    mode: str = "news_interpretation"
    allowed_devices: list[str] = field(default_factory=lambda: ["windows", "mac", "iphone", "galaxy"])
    banned_topic_keywords: list[str] = field(
        default_factory=lambda: [
            "shocking",
            "disaster",
            "scam",
            "fraud",
            "criminal",
            "exposed",
            "destroyed",
            "caught",
        ]
    )
    required_title_tokens_any: list[str] = field(
        default_factory=lambda: [
            "what changed",
            "what it means",
            "who is affected",
            "what to do now",
        ]
    )


def is_news_mode(settings: "AppSettings | None") -> bool:
    try:
        mode = str(getattr(getattr(settings, "content_mode", None), "mode", "") or "").strip().lower()
    except Exception:
        return False
    return mode in {"news_interpretation", "news_interpretation_only", "tech_news_only"}


def is_troubleshoot_mode(settings: "AppSettings | None") -> bool:
    try:
        mode = str(getattr(getattr(settings, "content_mode", None), "mode", "") or "").strip().lower()
    except Exception:
        return True
    return mode not in {"news_interpretation", "news_interpretation_only", "tech_news_only"}


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
    plan_json_enabled: bool = True
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
    provider: str = "gemini"
    banner_count: int = 1
    inline_count: int = 4
    cache_dir: str = "storage/image_cache"
    fallback_banner: str = "assets/fallback/banner.png"
    fallback_inline: str = "assets/fallback/inline.png"


@dataclass
class InternalLinksPolicySettings:
    enabled: bool = True
    body_link_count: int = 1
    related_link_count: int = 2
    overlap_threshold: float = 0.4
    canonical_internal_host: str = ""


@dataclass
class KeywordsPolicySettings:
    db_path: str = "storage/keywords.sqlite"
    refill_threshold_per_device: int = 100
    avoid_reuse_days: int = 30
    sources: ImageSourcesPolicySettings = field(default_factory=ImageSourcesPolicySettings)


@dataclass
class SyncSettings:
    enabled: bool = True
    run_on_startup: bool = True
    interval_hours: int = 24
    purge_deleted_after_days: int = 7
    include_statuses: list[str] = field(default_factory=lambda: ["live", "scheduled"])
    max_results_per_status: int = 500
    max_pages: int = 20
    strict_url_validation: bool = True
    log_path: str = "storage/logs/sync_blogger.jsonl"


@dataclass
class AppSettings:
    timezone: str = "America/New_York"
    workflow: WorkflowSettings = field(default_factory=WorkflowSettings)
    watchdog: WatchdogSettings = field(default_factory=WatchdogSettings)
    schedule: ScheduleSettings = field(default_factory=ScheduleSettings)
    monthly_scheduler: MonthlySchedulerSettings = field(default_factory=MonthlySchedulerSettings)
    sources: SourceSettings = field(default_factory=SourceSettings)
    gemini: GeminiSettings = field(default_factory=GeminiSettings)
    visual: VisualSettings = field(default_factory=VisualSettings)
    news_pack: NewsPackSettings = field(default_factory=NewsPackSettings)
    budget: BudgetSettings = field(default_factory=BudgetSettings)
    publish: PublishSettings = field(default_factory=PublishSettings)
    ledger: LedgerSettings = field(default_factory=LedgerSettings)
    readability: ReadabilitySettings = field(default_factory=ReadabilitySettings)
    title_diversity: TitleDiversitySettings = field(default_factory=TitleDiversitySettings)
    source_naturalization: SourceNaturalizationSettings = field(default_factory=SourceNaturalizationSettings)
    entropy_check: EntropyCheckSettings = field(default_factory=EntropyCheckSettings)
    quality: QualitySettings = field(default_factory=QualitySettings)
    actionability_gate: ActionabilityGateSettings = field(default_factory=ActionabilityGateSettings)
    generation: GenerationSettings = field(default_factory=GenerationSettings)
    topic_growth: TopicGrowthSettings = field(default_factory=TopicGrowthSettings)
    worldmonitor: WorldMonitorSettings = field(default_factory=WorldMonitorSettings)
    policy_gate: PolicyGateSettings = field(default_factory=PolicyGateSettings)
    search_learning: SearchLearningSettings = field(default_factory=SearchLearningSettings)
    search_intent: SearchIntentSettings = field(default_factory=SearchIntentSettings)
    structure_randomization: StructureRandomizationSettings = field(default_factory=StructureRandomizationSettings)
    content_allocation: ContentAllocationSettings = field(default_factory=ContentAllocationSettings)
    keyword_pool: KeywordPoolSettings = field(default_factory=KeywordPoolSettings)
    topic_pool: TopicPoolSettings = field(default_factory=TopicPoolSettings)
    integrations: IntegrationSettings = field(default_factory=IntegrationSettings)
    blogger: BloggerSettings = field(default_factory=BloggerSettings)
    indexing: IndexingSettings = field(default_factory=IndexingSettings)
    content: ContentPolicySettings = field(default_factory=ContentPolicySettings)
    content_mode: ContentModeSettings = field(default_factory=ContentModeSettings)
    topics: TopicsPolicySettings = field(default_factory=TopicsPolicySettings)
    llm: LLMPolicySettings = field(default_factory=LLMPolicySettings)
    local_llm: LocalLLMSettings = field(default_factory=LocalLLMSettings)
    images: ImagesPolicySettings = field(default_factory=ImagesPolicySettings)
    internal_links: InternalLinksPolicySettings = field(default_factory=InternalLinksPolicySettings)
    keywords: KeywordsPolicySettings = field(default_factory=KeywordsPolicySettings)
    sync: SyncSettings = field(default_factory=SyncSettings)
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
    settings_warnings: list[str] = []
    quality_raw = dict(raw.get("quality", {}) or {})
    actionability_raw = dict(raw.get("actionability_gate", {}) or {})
    generation_raw = dict(raw.get("generation", {}) or {})
    topic_pool_raw = dict(raw.get("topic_pool", {}) or {})
    qa_raw = raw.get("qa", {}) or {}
    worldmonitor_raw = dict(raw.get("worldmonitor", {}) or {})
    policy_gate_raw = dict(raw.get("policy_gate", {}) or {})
    search_learning_raw = dict(raw.get("search_learning", {}) or {})
    search_intent_raw = dict(raw.get("search_intent", {}) or {})
    structure_randomization_raw = dict(raw.get("structure_randomization", {}) or {})
    content_allocation_raw = dict(raw.get("content_allocation", {}) or {})
    content_raw = dict(raw.get("content", {}) or {})
    content_mode_raw = dict(raw.get("content_mode", {}) or {})
    topics_raw = dict(raw.get("topics", {}) or {})
    monthly_scheduler_raw = dict(raw.get("monthly_scheduler", {}) or {})
    publishing_raw = dict(raw.get("publishing", {}) or {})
    llm_raw = dict(raw.get("llm", {}) or {})
    local_llm_raw = dict(raw.get("local_llm", {}) or {})
    news_pack_raw = dict(raw.get("news_pack", {}) or {})
    images_raw = dict(raw.get("images", {}) or {})
    internal_links_raw = dict(raw.get("internal_links", {}) or {})
    keywords_raw = dict(raw.get("keywords", {}) or {})
    keyword_sources_raw = dict((keywords_raw.get("sources", {}) or {}))
    sync_raw = dict(raw.get("sync", {}) or {})
    ledger_raw = dict(raw.get("ledger", {}) or {})
    readability_raw = dict(raw.get("readability", {}) or {})
    title_diversity_raw = dict(raw.get("title_diversity", {}) or {})
    source_naturalization_raw = dict(raw.get("source_naturalization", {}) or {})
    entropy_check_raw = dict(raw.get("entropy_check", {}) or {})
    workflow_raw = dict(raw.get("workflow", {}) or {})
    watchdog_raw = dict(raw.get("watchdog", {}) or {})

    if isinstance(qa_raw, dict):
        if "qa_mode" in qa_raw:
            quality_raw["qa_mode"] = str(qa_raw.get("qa_mode", "full") or "full").strip().lower()
        if "prompt_leak_patterns" in qa_raw:
            quality_raw["prompt_leak_patterns"] = qa_raw.get("prompt_leak_patterns", [])
        disallowed = qa_raw.get("disallowed_terms", {}) or {}
        if isinstance(disallowed, dict) and "office_experiment" in disallowed:
            quality_raw["disallowed_terms_office_experiment"] = disallowed.get("office_experiment", [])
        if isinstance(disallowed, dict) and "news_interpretation" in disallowed:
            quality_raw["disallowed_terms_news_interpretation"] = disallowed.get("news_interpretation", [])
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

    if content_mode_raw:
        req_tokens = [
            str(x).strip().lower()
            for x in (content_mode_raw.get("required_title_tokens_any") or [])
            if str(x).strip()
        ]
        if req_tokens:
            quality_raw["required_title_tokens_any"] = req_tokens
        banned_topics = [
            str(x).strip().lower()
            for x in (content_mode_raw.get("banned_topic_keywords") or [])
            if str(x).strip()
        ]
        if banned_topics:
            existing = [
                str(x).strip().lower()
                for x in (quality_raw.get("disallowed_terms_news_interpretation", []) or [])
                if str(x).strip()
            ]
            merged = existing[:]
            for token in banned_topics:
                if token not in merged:
                    merged.append(token)
            quality_raw["disallowed_terms_news_interpretation"] = merged

    # Mirror spec blocks into runtime-compatible legacy settings.
    if content_raw:
        quality_raw["min_word_count"] = int(content_raw.get("min_words", quality_raw.get("min_word_count", 1800)))
        quality_raw["max_word_count"] = int(content_raw.get("max_words", quality_raw.get("max_word_count", 2200)))
    if llm_raw:
        raw.setdefault("gemini", {})
        raw["gemini"]["max_calls_per_run"] = int(llm_raw.get("max_calls_per_post", raw["gemini"].get("max_calls_per_run", 3)))
        if "enable_image_generation" in llm_raw:
            visual_raw_existing = raw.get("visual", {})
            has_visual_explicit = isinstance(visual_raw_existing, dict) and bool(visual_raw_existing)
            if has_visual_explicit:
                settings_warnings.append(
                    "llm.enable_image_generation is ignored because visual.* is source of truth."
                )
            else:
                raw.setdefault("visual", {})
                raw["visual"]["enable_gemini_image_generation"] = bool(llm_raw.get("enable_image_generation"))
    if images_raw:
        visual_raw_existing = raw.get("visual", {})
        has_visual_explicit = isinstance(visual_raw_existing, dict) and bool(visual_raw_existing)
        settings_warnings.append(
            "images.* is legacy; migrate image configuration to visual.*."
        )
        if has_visual_explicit:
            settings_warnings.append(
                "Both visual.* and images.* are present; runtime uses visual.* as source of truth."
            )
        else:
            raw.setdefault("visual", {})
            banner_count = int(images_raw.get("banner_count", 1))
            inline_count = int(images_raw.get("inline_count", 1))
            raw["visual"]["target_images_per_post"] = max(1, banner_count + inline_count)
            raw["visual"]["max_banner_images"] = max(1, banner_count)
            raw["visual"]["max_inline_images"] = max(0, inline_count)
            raw["visual"].setdefault("cache_dir", str(images_raw.get("cache_dir", "storage/image_cache")))
            raw["visual"].setdefault("fallback_banner", str(images_raw.get("fallback_banner", "assets/fallback/banner.png")))
            raw["visual"].setdefault("fallback_inline", str(images_raw.get("fallback_inline", "assets/fallback/inline.png")))
            raw["visual"].setdefault(
                "prompt_suffix",
                str(images_raw.get("prompt_suffix", "no text, no letters, no numbers, no logos, no watermark")),
            )
            src_provider = str(images_raw.get("provider", "generated") or "generated").strip().lower()
            if src_provider not in {"library", "gemini", "generated", "airforce", "pollinations"}:
                settings_warnings.append(
                    f"images.provider={src_provider} is unsupported; defaulting visual.image_provider=generated."
                )
                src_provider = "generated"
            if src_provider == "library":
                raw["visual"]["image_provider"] = "library"
                raw["visual"].setdefault("enable_gemini_image_generation", False)
            else:
                raw["visual"]["image_provider"] = src_provider
                raw["visual"]["enable_gemini_image_generation"] = True
            raw["visual"].setdefault("pollinations_enabled", False)
    raw.setdefault("visual", {})
    raw["visual"].setdefault("provider_order", list(news_pack_raw.get("provider_order", []) or ["airforce_imagen4", "gemini"]))
    raw["visual"].setdefault("airforce_enabled", True)
    raw["visual"].setdefault("airforce_api_key", str(news_pack_raw.get("airforce_api_key", "") or ""))
    raw["visual"].setdefault("airforce_base_url", str(news_pack_raw.get("airforce_base_url", "https://api.airforce") or "https://api.airforce"))
    raw["visual"].setdefault("airforce_image_model", str(news_pack_raw.get("airforce_image_model", "imagen-4") or "imagen-4"))
    raw["visual"].setdefault("airforce_timeout_sec", int(news_pack_raw.get("airforce_timeout_sec", 45) or 45))
    raw["visual"].setdefault("pollinations_api_key", str(news_pack_raw.get("pollinations_api_key", "") or raw["visual"].get("pollinations_api_key", "")))
    raw["visual"].setdefault("allow_library_fallback", False)
    raw["visual"].setdefault("allow_rendered_fallback", False)
    raw["visual"].setdefault("generated_r2_prefix", "generated")
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
        raw["publish"]["allow_inline_fallback_publish"] = bool(
            publishing_raw.get("allow_inline_fallback_publish", raw["publish"].get("allow_inline_fallback_publish", False))
        )
        raw["publish"]["allow_banner_fallback_publish"] = bool(
            publishing_raw.get("allow_banner_fallback_publish", raw["publish"].get("allow_banner_fallback_publish", True))
        )
        raw["publish"]["strict_thumbnail_blogger_media"] = bool(
            publishing_raw.get("strict_thumbnail_blogger_media", raw["publish"].get("strict_thumbnail_blogger_media", True))
        )
        raw["publish"]["thumbnail_data_uri_allowed"] = bool(
            publishing_raw.get("thumbnail_data_uri_allowed", raw["publish"].get("thumbnail_data_uri_allowed", False))
        )
        raw["publish"]["auto_allow_data_uri_on_blogger_405"] = bool(
            publishing_raw.get(
                "auto_allow_data_uri_on_blogger_405",
                raw["publish"].get("auto_allow_data_uri_on_blogger_405", False),
            )
        )
        raw["publish"]["thumbnail_preflight_only"] = bool(
            publishing_raw.get("thumbnail_preflight_only", raw["publish"].get("thumbnail_preflight_only", False))
        )
        raw["publish"]["thumbnail_preflight_max_cycles"] = int(
            publishing_raw.get("thumbnail_preflight_max_cycles", raw["publish"].get("thumbnail_preflight_max_cycles", 6))
        )
        raw["publish"]["thumbnail_preflight_retry_delay_sec"] = int(
            publishing_raw.get("thumbnail_preflight_retry_delay_sec", raw["publish"].get("thumbnail_preflight_retry_delay_sec", 8))
        )
    raw.setdefault("publish", {})
    publish_raw = dict(raw.get("publish", {}) or {})
    raw.setdefault("gemini", {})
    gemini_raw = dict(raw.get("gemini", {}) or {})
    gemini_raw["api_key"] = str(os.getenv("GEMINI_API_KEY") or gemini_raw.get("api_key", "")).strip()
    raw["gemini"] = gemini_raw
    publish_raw.setdefault("r2", {})
    r2_raw = dict(publish_raw.get("r2", {}) or {})
    # ENV-first override policy for secrets.
    r2_raw["endpoint_url"] = str(os.getenv("R2_ENDPOINT_URL") or r2_raw.get("endpoint_url", "")).strip()
    r2_raw["bucket"] = str(os.getenv("R2_BUCKET") or r2_raw.get("bucket", "")).strip()
    r2_raw["access_key_id"] = str(os.getenv("R2_ACCESS_KEY_ID") or r2_raw.get("access_key_id", "")).strip()
    r2_raw["secret_access_key"] = str(os.getenv("R2_SECRET_ACCESS_KEY") or r2_raw.get("secret_access_key", "")).strip()
    r2_raw["public_base_url"] = str(os.getenv("R2_PUBLIC_BASE_URL") or r2_raw.get("public_base_url", "")).strip()
    r2_raw["prefix"] = str(os.getenv("R2_PREFIX") or r2_raw.get("prefix", "library")).strip() or "library"
    r2_raw["cache_control"] = (
        str(os.getenv("R2_CACHE_CONTROL") or r2_raw.get("cache_control", "public, max-age=31536000, immutable")).strip()
        or "public, max-age=31536000, immutable"
    )
    publish_raw["r2"] = r2_raw
    raw["publish"] = publish_raw
    # ENV-first override for optional NewsPack provider secrets.
    news_pack_raw["airforce_api_key"] = str(
        os.getenv("AIRFORCE_API_KEY") or news_pack_raw.get("airforce_api_key", "")
    ).strip()
    news_pack_raw["pollinations_api_key"] = str(
        os.getenv("POLLINATIONS_API_KEY") or news_pack_raw.get("pollinations_api_key", "")
    ).strip()
    if not news_pack_raw["airforce_api_key"]:
        visual_raw = dict(raw.get("visual", {}) or {})
        legacy_key = str(visual_raw.get("pollinations_api_key", "") or "").strip()
        if legacy_key:
            news_pack_raw["airforce_api_key"] = legacy_key
            settings_warnings.append(
                "visual.pollinations_api_key is being reused as news_pack.airforce_api_key; migrate to news_pack.airforce_api_key."
            )
    if internal_links_raw:
        raw.setdefault("publish", {})
        raw["publish"]["related_posts_min"] = int(internal_links_raw.get("related_link_count", 2))
        raw["publish"]["related_posts_max"] = int(internal_links_raw.get("related_link_count", 2))
    if keywords_raw:
        raw.setdefault("keyword_pool", {})
        raw["keyword_pool"]["daily_target"] = int(keywords_raw.get("refill_threshold_per_device", 100))
        raw["keyword_pool"]["pick_per_run"] = max(1, int(raw.get("publishing", {}).get("posts_to_publish_per_day", 2)))

    if settings_warnings:
        try:
            warn_path = (path.parent.parent / "storage" / "logs" / "settings_warnings.log").resolve()
            warn_path.parent.mkdir(parents=True, exist_ok=True)
            with warn_path.open("a", encoding="utf-8") as fh:
                ts = datetime.now(timezone.utc).isoformat()
                for msg in settings_warnings[:5]:
                    fh.write(f"{ts} {msg}\n")
        except Exception:
            pass

    publish_obj = _construct_dc(PublishSettings, raw.get("publish", {}))
    if isinstance(getattr(publish_obj, "r2", None), dict):
        publish_obj.r2 = _construct_dc(PublishR2Settings, publish_obj.r2)
    elif not isinstance(getattr(publish_obj, "r2", None), PublishR2Settings):
        publish_obj.r2 = PublishR2Settings()

    return AppSettings(
        timezone=raw.get("timezone", "America/New_York"),
        workflow=_construct_dc(WorkflowSettings, workflow_raw),
        watchdog=_construct_dc(WatchdogSettings, watchdog_raw),
        schedule=_construct_dc(ScheduleSettings, raw.get("schedule", {})),
        monthly_scheduler=_construct_dc(MonthlySchedulerSettings, monthly_scheduler_raw),
        sources=_construct_dc(SourceSettings, raw.get("sources", {})),
        gemini=_construct_dc(GeminiSettings, raw.get("gemini", {})),
        visual=_construct_dc(VisualSettings, raw.get("visual", {})),
        news_pack=_construct_dc(NewsPackSettings, news_pack_raw),
        budget=_construct_dc(BudgetSettings, raw.get("budget", {})),
        publish=publish_obj,
        ledger=_construct_dc(LedgerSettings, ledger_raw),
        readability=_construct_dc(ReadabilitySettings, readability_raw),
        title_diversity=_construct_dc(TitleDiversitySettings, title_diversity_raw),
        source_naturalization=_construct_dc(SourceNaturalizationSettings, source_naturalization_raw),
        entropy_check=_construct_dc(EntropyCheckSettings, entropy_check_raw),
        quality=_construct_dc(QualitySettings, quality_raw),
        actionability_gate=_construct_dc(ActionabilityGateSettings, actionability_raw),
        generation=_construct_dc(GenerationSettings, generation_raw),
        topic_growth=_construct_dc(TopicGrowthSettings, raw.get("topic_growth", {})),
        worldmonitor=_construct_dc(WorldMonitorSettings, worldmonitor_raw),
        policy_gate=_construct_dc(PolicyGateSettings, policy_gate_raw),
        search_learning=_construct_dc(SearchLearningSettings, search_learning_raw),
        search_intent=_construct_dc(SearchIntentSettings, search_intent_raw),
        structure_randomization=_construct_dc(StructureRandomizationSettings, structure_randomization_raw),
        content_allocation=_construct_dc(ContentAllocationSettings, content_allocation_raw),
        keyword_pool=_construct_dc(KeywordPoolSettings, raw.get("keyword_pool", {})),
        topic_pool=_construct_dc(TopicPoolSettings, topic_pool_raw),
        integrations=_construct_dc(IntegrationSettings, raw.get("integrations", {})),
        blogger=_construct_dc(BloggerSettings, raw.get("blogger", {})),
        indexing=_construct_dc(IndexingSettings, raw.get("indexing", {})),
        content=_construct_dc(ContentPolicySettings, content_raw),
        content_mode=_construct_dc(ContentModeSettings, content_mode_raw),
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
        sync=_construct_dc(SyncSettings, sync_raw),
        authority_links=raw.get("authority_links", []),
        windows=raw.get("windows", {"use_task_scheduler": True}),
    )

