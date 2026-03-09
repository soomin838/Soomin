from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.guards.story_guard import StoryGuard
from rezero_v2.core.orchestrator.run_engine import RunEngine
from rezero_v2.tests.helpers import FakeGDELTClient, article, make_settings_file


class V2MultilingualNonTechGateTest(unittest.TestCase):
    def test_multilingual_agriculture_mining_story_is_skipped_before_outline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings_path = make_settings_file(root / "config" / "settings.yaml")
            groups = [
                {
                    "topic": "AI",
                    "articles": [
                        article(
                            title="La caída del precio del cacao empuja a agricultores de Ghana a ceder tierras a la minería ilegal",
                            summary="El reporte trata sobre agricultores, caída del precio del cacao, tierras y minería ilegal en Ghana.",
                            source="www.chicagotribune.com",
                            topic="AI",
                        )
                    ],
                }
            ]
            engine = RunEngine(root, settings_path, overrides={"gdelt_client": FakeGDELTClient(groups)})
            result = engine.run_once(dry_run=True)
            self.assertEqual(result.summary.result, "skipped")
            self.assertEqual(result.summary.reason_code, "off_topic_agriculture_mining_story")
            self.assertEqual(result.summary.final_stage, "gate_stage")
            self.assertNotIn("outline_stage", [item["stage_name"] for item in result.stage_results])

    def test_template_similarity_is_not_final_reason_for_obviously_off_topic_story(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings_path = make_settings_file(root / "config" / "settings.yaml")
            groups = [
                {
                    "topic": "AI",
                    "articles": [
                        article(
                            title="La caída del precio del cacao empuja a agricultores de Ghana a ceder tierras a la minería ilegal",
                            summary="El reporte trata sobre agricultores, caída del precio del cacao, tierras y minería ilegal en Ghana.",
                            source="www.chicagotribune.com",
                            topic="AI",
                        )
                    ],
                }
            ]
            engine = RunEngine(root, settings_path, overrides={"gdelt_client": FakeGDELTClient(groups)})
            result = engine.run_once(dry_run=True)
            self.assertNotEqual(result.summary.reason_code, "template_similarity_too_high")
            self.assertIn(result.summary.reason_code, {"off_topic_agriculture_mining_story", "multilingual_non_tech_public_affairs", "non_tech_economic_story_without_explicit_technology_angle"})

    def test_non_english_non_tech_economic_story_is_blocked_in_tech_news_only_mode(self) -> None:
        candidate = Candidate(
            candidate_id="cand-eco-1",
            content_type="hot",
            source_type="gdelt",
            title="El precio de la vivienda sigue presionando a trabajadores en Madrid",
            source_title="El precio de la vivienda sigue presionando a trabajadores en Madrid",
            source_url="https://example.com/economy-story",
            source_domain="example.com",
            source_snippet="La nota trata sobre vivienda, salarios y costo de vida, sin producto ni tecnología explícita.",
            category="AI",
            published_at_utc="2026-03-09T00:00:00+00:00",
            provider="gdelt",
            language="non_english",
            source_headline="El precio de la vivienda sigue presionando a trabajadores en Madrid",
            normalized_source_headline="",
            derived_primary_query="",
            entity_terms=["Madrid"],
            topic_terms=["precio", "vivienda", "trabajadores"],
            tags=["AI"],
            raw_meta={"source_language": "es"},
        )
        decision = StoryGuard().evaluate(candidate, mode="tech_news_only")
        self.assertFalse(decision.allow)
        self.assertEqual(decision.reason_code, "non_tech_economic_story_without_explicit_technology_angle")

    def test_short_ascii_tech_terms_do_not_match_inside_non_tech_words(self) -> None:
        candidate = Candidate(
            candidate_id="cand-caixa-1",
            content_type="hot",
            source_type="gdelt",
            title="La alianza del tejido asociativo y Fundación la Caixa ayudará a 15 . 000 personas en la provincia este año",
            source_title="La alianza del tejido asociativo y Fundación la Caixa ayudará a 15 . 000 personas en la provincia este año",
            source_url="https://example.com/caixa-story",
            source_domain="www.diariosur.es",
            source_snippet="Historia social sobre apoyo provincial y ayuda a personas, sin producto ni software.",
            category="AI",
            published_at_utc="2026-03-09T00:00:00+00:00",
            provider="gdelt",
            language="non_english",
            source_headline="La alianza del tejido asociativo y Fundación la Caixa ayudará a 15 . 000 personas en la provincia este año",
            normalized_source_headline="",
            derived_primary_query="",
            entity_terms=["Caixa"],
            topic_terms=["alianza", "personas", "provincia"],
            tags=["AI"],
            raw_meta={"source_language": "es"},
        )
        decision = StoryGuard().evaluate(candidate, mode="tech_news_only")
        self.assertFalse(decision.allow)
        self.assertNotEqual(decision.reason_code, "accepted")

    def test_v2_defaults_to_tech_news_only_when_content_mode_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings_path = make_settings_file(root / "config" / "settings.yaml")
            data = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
            data.pop("content_mode", None)
            settings_path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
            groups = [
                {
                    "topic": "AI",
                    "articles": [
                        article(
                            title="岳微 ： 保护主义救不了欧洲产业",
                            summary="这是一篇关于保护主义和欧洲产业的公共事务评论，没有明确的技术或产品信号。",
                            source="baijiahao.baidu.com",
                            topic="AI",
                        )
                    ],
                }
            ]
            engine = RunEngine(root, settings_path, overrides={"gdelt_client": FakeGDELTClient(groups)})
            result = engine.run_once(dry_run=True)
            self.assertEqual(result.summary.result, "skipped")
            self.assertIn(result.summary.reason_code, {"multilingual_non_tech_public_affairs", "mixed_domain_requires_explicit_tech_angle"})
            self.assertEqual(result.summary.final_stage, "gate_stage")


if __name__ == "__main__":
    unittest.main()
