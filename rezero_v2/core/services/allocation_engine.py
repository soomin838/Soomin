from __future__ import annotations

from rezero_v2.core.domain.allocation import AllocationDecision


class AllocationEngine:
    def __init__(self, *, mix_hot: int = 2, mix_search_derived: int = 2, mix_evergreen: int = 1, content_lengths: dict[str, tuple[int, int]] | None = None) -> None:
        self.mix_hot = max(0, int(mix_hot))
        self.mix_search_derived = max(0, int(mix_search_derived))
        self.mix_evergreen = max(0, int(mix_evergreen))
        self.content_lengths = content_lengths or {'hot': (700, 1000), 'search_derived': (1100, 1500), 'evergreen': (1600, 2200)}

    def choose_next_slot(self, *, published_counts: dict[str, int]) -> AllocationDecision:
        targets = {'hot': self.mix_hot, 'search_derived': self.mix_search_derived, 'evergreen': self.mix_evergreen}
        remaining = {key: max(0, targets[key] - int(published_counts.get(key, 0) or 0)) for key in targets}
        order = [key for key, _ in sorted(remaining.items(), key=lambda item: (-item[1], ['hot', 'search_derived', 'evergreen'].index(item[0])))]
        slot_type = next((item for item in order if remaining.get(item, 0) > 0), order[0])
        source_type = {'hot': 'gdelt', 'search_derived': 'search_console', 'evergreen': 'cluster_seed'}.get(slot_type, 'gdelt')
        strategies = {'hot': ('news_explainer', 'timely_explainer', 'source_grounded', 'hero_plus_optional_inline'), 'search_derived': ('search_answer', 'query_match', 'authority_first', 'hero_plus_optional_inline'), 'evergreen': ('evergreen_hub', 'evergreen_utility', 'authority_first', 'hero_only_or_one_inline')}
        generation_mode_hint, title_strategy, source_strategy, image_strategy = strategies[slot_type]
        return AllocationDecision(slot_type=slot_type, source_type=source_type, generation_mode_hint=generation_mode_hint, target_word_range=self.content_lengths[slot_type], title_strategy=title_strategy, source_strategy=source_strategy, image_strategy=image_strategy)
