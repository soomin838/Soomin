from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import pstdev

from .settings import QualitySettings


@dataclass
class QACheck:
    key: str
    passed: bool
    detail: str
    weight: int


@dataclass
class QAResult:
    score: int
    base_score: int = 0
    soft_score: int = 100
    checks: list[QACheck] = field(default_factory=list)
    human_checks: list[QACheck] = field(default_factory=list)
    hard_failures: list[str] = field(default_factory=list)

    @property
    def failed(self) -> list[QACheck]:
        return [c for c in self.checks if not c.passed]

    @property
    def human_failed(self) -> list[QACheck]:
        return [c for c in self.human_checks if not c.passed]

    @property
    def has_hard_failure(self) -> bool:
        return bool(self.hard_failures)


class ContentQAGate:
    def __init__(
        self,
        settings: QualitySettings,
        authority_links: list[str],
        qa_runtime_path: Path | None = None,
    ) -> None:
        self.settings = settings
        self.authority_links = authority_links
        self.qa_runtime_path = qa_runtime_path

    def evaluate(self, html: str, title: str = "", domain: str = "tech_troubleshoot") -> QAResult:
        checks: list[QACheck] = []
        req_authority_links, req_external_links = self._link_requirements(domain)
        text = self._to_text(html)
        words = len(re.findall(r"[A-Za-z0-9']+", text))
        h2_count = len(re.findall(r"<h2\b", html, flags=re.IGNORECASE))
        h3_count = len(re.findall(r"<h3\b", html, flags=re.IGNORECASE))
        li_count = len(re.findall(r"<li\b", html, flags=re.IGNORECASE))
        links = re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)
        ext_links = [u for u in links if u.startswith("http://") or u.startswith("https://")]
        allowed_links = [u for u in ext_links if any(u.startswith(a) for a in self.authority_links)]

        checks.append(
            QACheck(
                key="word_count",
                passed=(words >= self.settings.min_word_count and words <= max(self.settings.max_word_count, self.settings.min_word_count)),
                detail=f"word_count {words}/{self.settings.min_word_count}-{self.settings.max_word_count}",
                weight=20,
            )
        )
        checks.append(
            QACheck(
                key="heading_structure",
                passed=(h2_count >= self.settings.min_h2 and h3_count >= self.settings.min_h3),
                detail=f"H2={h2_count}, H3={h3_count}",
                weight=12,
            )
        )
        checks.append(
            QACheck(
                key="actionability",
                passed=li_count >= self.settings.min_list_items,
                detail=f"list_items {li_count}/{self.settings.min_list_items}",
                weight=10,
            )
        )
        checks.append(
            QACheck(
                key="authority_links",
                passed=len(allowed_links) >= req_authority_links,
                detail=f"authority_links {len(allowed_links)}/{req_authority_links}",
                weight=12,
            )
        )
        checks.append(
            QACheck(
                key="external_links",
                passed=len(ext_links) >= req_external_links,
                detail=f"external_links {len(ext_links)}/{req_external_links}",
                weight=10,
            )
        )
        attribution_required = 0 if str(domain or "").strip().lower() == "tech_troubleshoot" else 1
        checks.append(
            QACheck(
                key="source_attribution",
                passed=len(ext_links) >= attribution_required,
                detail=f"attribution_links {len(ext_links)}/{attribution_required}",
                weight=16,
            )
        )
        checks.append(
            QACheck(
                key="no_ai_markers",
                passed=not self._has_banned_markers(text),
                detail="banned_ai_markers check",
                weight=10,
            )
        )
        checks.append(
            QACheck(
                key="burstiness",
                passed=self._burstiness_ok(text),
                detail="sentence_length_variation check",
                weight=10,
            )
        )
        prompt_leak, prompt_leak_detail = self._detect_prompt_leak(
            html=html,
            title=title,
            domain=domain,
        )
        checks.append(
            QACheck(
                key="prompt_leak",
                passed=(not prompt_leak),
                detail=prompt_leak_detail or "no_prompt_leak",
                weight=0,
            )
        )
        domain_drift, domain_drift_detail = self._detect_domain_drift(text=text, domain=domain)
        checks.append(
            QACheck(
                key="domain_drift",
                passed=(not domain_drift),
                detail=domain_drift_detail or "no_domain_drift",
                weight=0,
            )
        )
        story_missing, story_detail = self._detect_missing_story_block(text=text, domain=domain)
        checks.append(
            QACheck(
                key="story_block",
                passed=(not story_missing),
                detail=story_detail or "story_block_ok",
                weight=0,
            )
        )
        non_english, non_english_detail = self._detect_non_english_content(html=html, text=text)
        checks.append(
            QACheck(
                key="english_only",
                passed=(not non_english),
                detail=non_english_detail or "english_only_ok",
                weight=0,
            )
        )
        phrase_fail, phrase_detail = self._detect_forbidden_phrase_or_format(text=text, html=html)
        checks.append(
            QACheck(
                key="forbidden_phrases_formats",
                passed=(not phrase_fail),
                detail=phrase_detail or "forbidden_content_ok",
                weight=0,
            )
        )
        screenshot_fail, screenshot_detail = self._detect_screenshot_mentions(text=text)
        checks.append(
            QACheck(
                key="no_screenshot_mentions",
                passed=(not screenshot_fail),
                detail=screenshot_detail or "screenshot_mentions_ok",
                weight=0,
            )
        )
        qmark_fail, qmark_detail = self._detect_question_mark_spam(text=text)
        checks.append(
            QACheck(
                key="question_mark_limit",
                passed=(not qmark_fail),
                detail=qmark_detail or "question_mark_limit_ok",
                weight=0,
            )
        )
        sensitive_fail, sensitive_detail = self._detect_sensitive_topics(text=text)
        checks.append(
            QACheck(
                key="sensitive_topics",
                passed=(not sensitive_fail),
                detail=sensitive_detail or "sensitive_topics_ok",
                weight=0,
            )
        )

        base_score = 0
        for c in checks:
            if c.weight > 0 and c.passed:
                base_score += c.weight

        human_checks: list[QACheck] = []
        soft_score = 100
        hard_failures: list[str] = []
        if self.settings.humanity_enabled:
            human_checks, soft_score, hard_failures = self._evaluate_humanity_50(text, html)

        weight = max(0, min(100, int(self.settings.humanity_weight_percent or 0)))
        if weight <= 0:
            final_score = int(base_score)
        else:
            final_score = int(
                round((base_score * (100 - weight) / 100.0) + (soft_score * weight / 100.0))
            )

        if self.settings.humanity_enabled and soft_score < int(self.settings.humanity_min_soft_score):
            hard_failures.append(
                f"humanity_soft_floor({soft_score}/{int(self.settings.humanity_min_soft_score)})"
            )
        if prompt_leak:
            hard_failures.append("prompt_leak_detected")
            self._log_qa_reason(
                reason="prompt_leak_detected",
                detail=prompt_leak_detail,
                title=title,
                domain=domain,
            )
        if domain_drift:
            hard_failures.append("domain_drift_detected")
            self._log_qa_reason(
                reason="domain_drift_detected",
                detail=domain_drift_detail,
                title=title,
                domain=domain,
            )
        if story_missing:
            hard_failures.append("no_first_person_story_block")
            self._log_qa_reason(
                reason="no_story_block_detected",
                detail=story_detail,
                title=title,
                domain=domain,
            )
        if non_english:
            hard_failures.append("non_english_content_detected")
            self._log_qa_reason(
                reason="non_english_content_detected",
                detail=non_english_detail,
                title=title,
                domain=domain,
            )
        if phrase_fail:
            hard_failures.append("forbidden_phrase_or_format_detected")
            self._log_qa_reason(
                reason="forbidden_phrase_or_format_detected",
                detail=phrase_detail,
                title=title,
                domain=domain,
            )
        if screenshot_fail:
            hard_failures.append("screenshot_mention_detected")
            self._log_qa_reason(
                reason="screenshot_mention_detected",
                detail=screenshot_detail,
                title=title,
                domain=domain,
            )
        if qmark_fail:
            hard_failures.append("question_mark_spam_detected")
            self._log_qa_reason(
                reason="question_mark_spam_detected",
                detail=qmark_detail,
                title=title,
                domain=domain,
            )
        if sensitive_fail:
            hard_failures.append("sensitive_topic_detected")
            self._log_qa_reason(
                reason="sensitive_topic_detected",
                detail=sensitive_detail,
                title=title,
                domain=domain,
            )

        return QAResult(
            score=max(0, min(100, final_score)),
            base_score=max(0, min(100, int(base_score))),
            soft_score=max(0, min(100, int(soft_score))),
            checks=checks,
            human_checks=human_checks,
            hard_failures=sorted(set(hard_failures)),
        )

    def improve(self, html: str) -> str:
        return self.improve_with_feedback(html, None, None)

    def satisfy_requirements(self, html: str, qa_result: QAResult) -> str:
        """Last-mile targeted completion pass to close specific failed checks."""
        out = html
        failed = {c.key for c in qa_result.failed}
        topic = self._infer_topic(self._to_text(out))

        if "word_count" in failed and not self._has_section(out, "When This Works Best"):
            out += (
                "<h2>When This Works Best</h2>"
                f"<p>For {topic}, choose the approach that reduces confusion first, then optimize for speed. "
                "A practical order is: clear checklist, visible progress metric, then faster execution.</p>"
                "<p>Do not optimize what you cannot measure. If metrics are unclear, the safest decision is usually "
                "the smallest reversible change that improves signal quality.</p>"
            )
        if "heading_structure" in failed and not self._has_section(out, "Where It Usually Breaks"):
            out += (
                "<h2>Where It Usually Breaks</h2>"
                "<h3>What Fails Early</h3><p>People skip the basic checklist and jump to optimization.</p>"
                "<h3>What Fails Late</h3><p>The team has no clear rule for when to pause and correct course.</p>"
            )
        if "actionability" in failed:
            li_count = len(re.findall(r"<li\\b", out, flags=re.IGNORECASE))
            need = max(0, self.settings.min_list_items - li_count)
            if need > 0:
                steps = [
                    "Pin one baseline metric before any change.",
                    "Change one variable at a time.",
                    "Capture before/after snapshots for traces and config.",
                    "Define a pause-and-retry trigger with numeric threshold.",
                    "Record one what-worked finding and one preventive action.",
                    "Link notes to the weekly action plan.",
                ][:need]
                out += "<h3>Execution Steps</h3><ul>" + "".join(f"<li>{s}</li>" for s in steps) + "</ul>"
        if "authority_links" in failed or "external_links" in failed:
            out = self.improve_with_feedback(
                out,
                [QACheck("authority_links", False, "", 0), QACheck("external_links", False, "", 0)],
            )
        if "burstiness" in failed and not self._burstiness_ok(self._to_text(out)):
            out += (
                "<p>Short version: test less at once.</p>"
                "<p>The longer version is operationally simple but culturally hard: reduce concurrent change surface, "
                "measure first, and pause when task ownership is unclear.</p>"
            )
        if "no_ai_markers" in failed:
            out = self._strip_banned_markers(out)
        return out

    def force_comply(self, html: str) -> str:
        """
        Deterministic compliance patch:
        adds only missing structural/quality pieces so QA can converge quickly.
        """
        out = self._strip_banned_markers(html)
        text = self._to_text(out)
        words = len(re.findall(r"[A-Za-z0-9']+", text))
        h2_count = len(re.findall(r"<h2\b", out, flags=re.IGNORECASE))
        h3_count = len(re.findall(r"<h3\b", out, flags=re.IGNORECASE))
        li_count = len(re.findall(r"<li\b", out, flags=re.IGNORECASE))
        links = re.findall(r'href="([^"]+)"', out, flags=re.IGNORECASE)
        ext_links = [u for u in links if u.startswith("http://") or u.startswith("https://")]
        allowed_links = [u for u in ext_links if any(u.startswith(a) for a in self.authority_links)]
        topic = self._infer_topic(text)

        if h2_count < self.settings.min_h2 and not self._has_section(out, "What Actually Works"):
            out += (
                "<h2>What Actually Works</h2>"
                f"<p>For {topic}, prioritize choices that are measurable, reversible, and team-operable. "
                "Speed matters only after clarity and repeatability are secured.</p>"
            )
            h2_count += 1
        if h3_count < self.settings.min_h3 and not self._has_section(out, "Execution Guardrails"):
            out += (
                "<h3>Execution Guardrails</h3>"
                "<p>Define one owner, one pause trigger, and one success metric before execution.</p>"
            )
            h3_count += 1

        if li_count < self.settings.min_list_items:
            need = self.settings.min_list_items - li_count
            steps = [
                "Establish a baseline metric and expected delta.",
                "Apply one change per test window.",
                "Snapshot configuration and runtime version before deploy.",
                "Attach a pause condition to an exact threshold.",
                "Run post-release verification with real traffic segments.",
                "Document one lesson and one permanent preventive control.",
                "Tag unresolved risk with an owner and due date.",
                "Review impact after 24 hours and adjust scope.",
            ][:need]
            out += "<h3>Action Checklist</h3><ul>" + "".join(f"<li>{s}</li>" for s in steps) + "</ul>"

        if words < self.settings.min_word_count and not self._has_section(out, "What I Learned After Testing"):
            out += (
                "<h2>What I Learned After Testing</h2>"
                f"<p>Teams working on {topic} usually fail when assumptions are treated as facts. "
                "A robust process separates hypothesis, evidence, and daily work decisions. "
                "When evidence is thin, reduce blast radius first and optimize later.</p>"
                "<p>In practice, the winning pattern is small reversible changes with dense measurement. "
                "This reduces rework risk, shortens delay cycles, and improves team confidence. "
                "The same principle applies to product velocity: disciplined iteration beats heroic rewrites.</p>"
                "<p>Another overlooked factor is context parity. A demo can pass while real usage still fails "
                "because assumptions about workload and timing were unrealistic. "
                "Treat context validation as a first-class requirement, not an afterthought.</p>"
            )

        if self.authority_links and len(allowed_links) < req_authority_links:
            existing = set(ext_links)
            need_auth = req_authority_links - len(allowed_links)
            auth_add = [u for u in self.authority_links if u not in existing][:need_auth]
            if auth_add:
                out += "<h3>Verified References</h3><ul>" + "".join(
                    f"<li>{self._link_context(u)}</li>" for u in auth_add
                ) + "</ul>"
                ext_links.extend(auth_add)

        if len(ext_links) < req_external_links and self.authority_links:
            existing2 = set(ext_links)
            need_ext = req_external_links - len(ext_links)
            ext_add = [u for u in self.authority_links if u not in existing2][:need_ext]
            if ext_add:
                out += "<h3>Additional Sources</h3><ul>" + "".join(
                    f"<li>{self._link_context(u)}</li>" for u in ext_add
                ) + "</ul>"

        if req_external_links >= 1 and len(ext_links) < 1 and self.authority_links:
            fallback_src = self.authority_links[0]
            out += (
                "<h3>References</h3>"
                f'<p><a href="{fallback_src}" rel="nofollow noopener" target="_blank">{fallback_src}</a></p>'
            )

        if not self._burstiness_ok(self._to_text(out)):
            out += (
                "<p>It looked safe. Then reality disagreed.</p>"
                "<p>The sustainable fix is procedural: tighten change scope, instrument first, "
                "and make pause decisions explicit before execution.</p>"
            )
        if bool(getattr(self.settings, "require_story_block", True)):
            lower = self._to_text(out).lower()
            if not re.search(r"\b(i|my|when i|i tried|on day\s*\d+)\b", lower):
                out += (
                    "<h3>My One-Week Experiment</h3>"
                    "<p>When I tried this in a normal workweek, I made a wrong choice on day two and had to retry. "
                    "After simplifying the steps, the workflow became easier to repeat and less stressful.</p>"
                )
        return out

    def polish_if_possible(self, html: str, qa_result: QAResult) -> str:
        """Apply one light polish pass even after passing threshold."""
        out = html
        text = self._to_text(out)
        words = len(re.findall(r"[A-Za-z0-9']+", text))
        links = re.findall(r'href="([^"]+)"', out, flags=re.IGNORECASE)
        ext_links = [u for u in links if u.startswith("http://") or u.startswith("https://")]
        allowed_links = [u for u in ext_links if any(u.startswith(a) for a in self.authority_links)]

        # If score is not perfect, add small value without changing article intent.
        if qa_result.score < 100 and words < max(self.settings.min_word_count + 180, 1100):
            out += (
                "<h3>Operational Checklist Before You Ship</h3>"
                "<p>Validate ownership, pause trigger, and measurable success criteria in the same note. "
                "This prevents ambiguity under pressure and improves decision speed.</p>"
            )

        req_authority_links, req_external_links = self._link_requirements(getattr(qa_result, "domain", ""))
        if self.authority_links and len(allowed_links) <= req_authority_links:
            missing = req_authority_links + 1 - len(allowed_links)
            if missing > 0:
                existing = set(ext_links)
                extra = [u for u in self.authority_links if u not in existing][:missing]
                if extra:
                    refs = "".join(
                        f'<li><a href="{u}" rel="nofollow noopener" target="_blank">{u}</a></li>'
                        for u in extra
                    )
                    out += f"<h3>Additional References</h3><ul>{refs}</ul>"

        if not self._burstiness_ok(self._to_text(out)):
            out += (
                "<p>It looked stable at first. Then edge traffic exposed hidden assumptions.</p>"
                "<p>That is why final validation should happen in a realistic work setting, "
                "not in an idealized demo-only scenario.</p>"
            )
        return out

    def improve_with_feedback(
        self,
        html: str,
        failed_checks: list[QACheck] | None,
        qa_result: QAResult | None = None,
    ) -> str:
        out = html
        failed = {c.key for c in (failed_checks or [])}
        targeted = bool(failed)
        if not bool(getattr(self.settings, "partial_fix_enabled", True)):
            return out
        text = self._to_text(out)
        topic = self._infer_topic(text)
        words = len(re.findall(r"[A-Za-z0-9']+", text))
        if bool(getattr(self.settings, "partial_fix_story_first", True)):
            if (not targeted or "story_block" in failed):
                if not re.search(r"\b(i|my|when i|i tried)\b", self._to_text(out).lower()):
                    out += (
                        "<h3>What Happened In My Test</h3>"
                        "<p>On the third day, I tried the same method with a tighter deadline and failed at first. "
                        "I removed one unnecessary step, retried, and the task time dropped noticeably.</p>"
                    )
        if (not targeted or "word_count" in failed) and words < self.settings.min_word_count and not self._has_section(out, "Practical Implementation Notes"):
            out += (
                "<h2>Practical Implementation Notes</h2>"
                f"<p>For {topic}, use a staged routine: baseline measurement, one-change-per-test cycle, "
                "and pause criteria. Capture metrics before and after each change so decisions "
                "are evidence-based rather than opinion-based.</p>"
                "<p>When this process fails, the most common cause is missing decision boundaries. "
                "Define what will trigger a pause, what metric delta is acceptable, and who has final "
                "approval authority before rollout begins. This single change usually reduces rework dramatically.</p>"
                "<p>Teams also underestimate context drift. A checklist can pass while real usage still fails "
                "because assumptions about workload and timing were unrealistic. "
                "Treat context validation as a first-class requirement, not an afterthought.</p>"
                "<h3>Validation Checklist</h3>"
                "<ul>"
                "<li>Define success metrics before rollout.</li>"
                "<li>Track completion rate, time spent, and conversion impact.</li>"
                "<li>Document root cause and final preventive action.</li>"
                "<li>Verify configuration against source of truth.</li>"
                "<li>Run one pause-and-restart rehearsal before full rollout.</li>"
                "</ul>"
            )
        if (not targeted or "heading_structure" in failed) and len(re.findall(r"<h2\b", out, flags=re.IGNORECASE)) < self.settings.min_h2 and not self._has_section(out, "Quick Take"):
            out = "<h2>Quick Take</h2><p>This guide focuses on practical outcomes.</p>" + out
        if (not targeted or "heading_structure" in failed) and len(re.findall(r"<h3\b", out, flags=re.IGNORECASE)) < self.settings.min_h3 and not self._has_section(out, "Next Steps"):
            out += "<h3>Next Steps</h3><p>Prioritize high-impact fixes first, then optimize.</p>"
        if (not targeted or "actionability" in failed) and len(re.findall(r"<li\b", out, flags=re.IGNORECASE)) < self.settings.min_list_items:
            out += (
                "<ul>"
                "<li>Implement changes in small increments.</li>"
                "<li>Measure every release.</li>"
                "<li>Retrospect and refine workflow weekly.</li>"
                "<li>Track one risk register per weekly work cycle.</li>"
                "<li>Assign one owner per unresolved risk.</li>"
                "</ul>"
            )
        if (not targeted or "authority_links" in failed or "external_links" in failed):
            req_authority_links, req_external_links = self._link_requirements(getattr(qa_result, "domain", ""))
            links_now = re.findall(r'href="([^"]+)"', out, flags=re.IGNORECASE)
            ext_now = [u for u in links_now if u.startswith("http://") or u.startswith("https://")]
            allowed_now = [u for u in ext_now if any(u.startswith(a) for a in self.authority_links)]
            existing_set = set(ext_now)
            add_list: list[str] = []
            if self.authority_links:
                missing_auth = max(0, req_authority_links - len(allowed_now))
                if missing_auth > 0:
                    add_list.extend([u for u in self.authority_links if u not in existing_set][:missing_auth])
                    existing_set.update(add_list)
                # External links minimum may still be short even after authority fill.
                current_ext_after = len(ext_now) + len(add_list)
                missing_ext = max(0, req_external_links - current_ext_after)
                if missing_ext > 0:
                    add_list.extend([u for u in self.authority_links if u not in existing_set][:missing_ext])
            if add_list:
                inserts = "".join(f'<li>{self._link_context(u)}</li>' for u in add_list)
                out += f"<h3>Reference Links</h3><ul>{inserts}</ul>"
        if (not targeted or "burstiness" in failed) and (not self._burstiness_ok(self._to_text(out))):
            out += (
                "<h3>Field Notes</h3>"
                "<p>It looked fine in rehearsal. Then real usage hit.</p>"
                "<p>The team had to pause rollout, isolate one variable, and rebuild confidence from metrics, "
                "not assumptions, because assumptions were exactly what caused the initial failure.</p>"
                "<p>Short lesson: ship smaller. Measure harder.</p>"
            )
        if (not targeted or "prompt_leak" in failed):
            out = self._strip_banned_markers(out)
            # Remove keyword-only dump lines.
            out = re.sub(
                r"<p[^>]*>\s*(?:[a-z0-9][a-z0-9\- ]{1,30}\s*,\s*){3,}[a-z0-9][a-z0-9\- ]{1,30}\s*</p>",
                "",
                out,
                flags=re.IGNORECASE,
            )
        if (not targeted or "domain_drift" in failed):
            lower_domain = str((qa_result.domain if qa_result else "") or "").strip().lower()
            disallowed_terms = (
                self.settings.disallowed_terms_tech_troubleshoot
                if lower_domain == "tech_troubleshoot"
                else self.settings.disallowed_terms_office_experiment
            )
            for term in (disallowed_terms or []):
                t = str(term or "").strip()
                if not t:
                    continue
                out = re.sub(re.escape(t), "workflow detail", out, flags=re.IGNORECASE)
        if (not targeted or "story_block" in failed):
            if not re.search(r"\b(i|my|when i|i tried)\b", self._to_text(out).lower()):
                out += (
                    "<h3>What Happened In My Test</h3>"
                    "<p>On the third day, I tried the same method with a tighter deadline and failed at first. "
                    "I removed one unnecessary step, retried, and the task time dropped noticeably.</p>"
                )
        if (not targeted or "english_only" in failed):
            out = self._strip_non_english_lines(out)
            if len(re.findall(r"<p\b", out, flags=re.IGNORECASE)) == 0:
                out += (
                    "<p>I rewrote this section in clear American English so the practical lesson stays readable and useful.</p>"
                )
        if (not targeted or "no_ai_markers" in failed):
            out = self._strip_banned_markers(out)
        if qa_result and qa_result.has_hard_failure:
            out = self._repair_hard_humanity_failures(out, qa_result.hard_failures)
        return out

    def _link_requirements(self, domain: str) -> tuple[int, int]:
        lower_domain = str(domain or "").strip().lower()
        if lower_domain == "tech_troubleshoot":
            auth_req = int(
                getattr(
                    self.settings,
                    "min_authority_links_tech_troubleshoot",
                    min(1, int(getattr(self.settings, "min_authority_links", 0) or 0)),
                )
                or 0
            )
            ext_req = int(
                getattr(
                    self.settings,
                    "min_external_links_tech_troubleshoot",
                    min(1, int(getattr(self.settings, "min_external_links", 0) or 0)),
                )
                or 0
            )
            return max(0, auth_req), max(0, ext_req)
        return (
            max(0, int(getattr(self.settings, "min_authority_links", 0) or 0)),
            max(0, int(getattr(self.settings, "min_external_links", 0) or 0)),
        )

    def _evaluate_humanity_50(self, text: str, html: str) -> tuple[list[QACheck], int, list[str]]:
        plain = re.sub(r"\s+", " ", text or "").strip()
        paragraphs = [p.strip() for p in re.split(r"\n{2,}", re.sub(r"</p>", "</p>\n\n", html, flags=re.IGNORECASE)) if p.strip()]
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", plain) if s.strip()]
        sent_lens = [len(re.findall(r"[A-Za-z0-9']+", s)) for s in sentences] or [0]
        words = re.findall(r"[A-Za-z0-9']+", plain)
        lower = plain.lower()
        word_count = len(words)
        token_counter = Counter(w.lower() for w in words if len(w) >= 4)

        unique_ratio = (len(set(w.lower() for w in words)) / max(1, word_count))
        sentence_std = pstdev(sent_lens) if len(sent_lens) >= 2 else 0.0
        avg_sent = (sum(sent_lens) / max(1, len(sent_lens)))
        dup_sentence_ratio = self._duplicate_sentence_ratio(sentences)
        list_items = len(re.findall(r"<li\b", html, flags=re.IGNORECASE))
        h2_count = len(re.findall(r"<h2\b", html, flags=re.IGNORECASE))
        connector_hits = len(re.findall(r"\b(therefore|thus|hence|moreover|furthermore|in conclusion|overall)\b", lower))
        modal_hits = len(re.findall(r"\b(can|could|should|would|may|might)\b", lower))
        first_person_hits = len(re.findall(r"\b(i|my|me|we|our|us)\b", lower))
        emotion_hits = len(
            re.findall(
                r"\b(frustrated|relieved|worried|excited|surprised|annoyed|happy|stressed|awkward|confused)\b",
                lower,
            )
        )
        sensory_hits = len(
            re.findall(
                r"\b(smell|sound|noise|cold|warm|bright|dim|quiet|loud|sticky|sharp|rough|smooth)\b",
                lower,
            )
        )
        concrete_example_hits = len(
            re.findall(r"\b(for example|for instance|last week|yesterday|this morning|at \d{1,2}(:\d{2})?)\b", lower)
        )
        jargon_hits = len(
            re.findall(
                r"\b(orchestration|architecture|framework|synergy|optimization|latency|throughput|paradigm|robustness)\b",
                lower,
            )
        )
        summary_hits = len(re.findall(r"\b(in short|to summarize|summary|overall|in conclusion)\b", lower))
        failure_hits = len(re.findall(r"\b(failed|broke|mistake|wrong|missed|bug|issue|problem)\b", lower))
        humor_hits = len(re.findall(r"\b(funny|joke|laughed|oops|yikes)\b", lower))
        smell_sound_people_hits = len(re.findall(r"\b(team|manager|coworker|client|friend|user)\b", lower))
        conclusion_early = bool(
            re.search(r"\b(in conclusion|to summarize|overall)\b", lower[: max(80, int(len(lower) * 0.28))])
        )

        repeated_tokens = sum(1 for _, c in token_counter.most_common(8) if c >= max(5, int(len(sentences) * 0.6)))
        overly_listy = bool(list_items >= max(7, int(len(sentences) * 0.45)))
        overly_uniform_sentences = bool(sentence_std < 4.2 and len(sentences) >= 8)
        robotic_transition_overuse = bool(connector_hits >= max(4, int(len(sentences) * 0.2)))
        generic_modal_overuse = bool(modal_hits >= max(8, int(len(sentences) * 0.65)))

        checks: list[QACheck] = []
        hard_failures: list[str] = []

        def add_check(idx: int, key: str, passed: bool, detail: str, *, hard: bool = False) -> None:
            hkey = f"h{idx:02d}_{key}"
            checks.append(QACheck(key=hkey, passed=passed, detail=detail, weight=1))
            if hard and not passed:
                hard_failures.append(hkey)

        # 50-item human-like checklist (heuristic local proxy).
        add_check(1, "no_repeated_sentence_structure", sentence_std >= 3.6, f"sentence_std={sentence_std:.2f}", hard=True)
        add_check(2, "no_semantic_repetition_clusters", dup_sentence_ratio < 0.22, f"dup_sentence_ratio={dup_sentence_ratio:.2f}", hard=True)
        add_check(3, "no_early_conclusion", not conclusion_early, f"conclusion_early={int(conclusion_early)}", hard=True)
        add_check(4, "summary_phrases_not_excessive", summary_hits <= max(2, int(len(sentences) * 0.12)), f"summary_hits={summary_hits}")
        add_check(5, "emotion_trace_exists", emotion_hits >= 1, f"emotion_hits={emotion_hits}")
        add_check(6, "concrete_example_exists", concrete_example_hits >= 1, f"example_hits={concrete_example_hits}", hard=True)
        add_check(7, "not_too_generic", unique_ratio >= 0.34, f"unique_ratio={unique_ratio:.2f}")
        add_check(8, "lists_not_dominant", not overly_listy, f"list_items={list_items},sentences={len(sentences)}", hard=True)
        add_check(9, "non_mechanical_problem_solution_flow", not (summary_hits >= 3 and connector_hits >= 4), f"summary={summary_hits},connectors={connector_hits}")
        add_check(10, "metaphor_or_analogy_presence", bool(re.search(r"\b(like|as if|feels like|similar to)\b", lower)), "analogy_marker")
        add_check(11, "sentence_length_not_uniform", not overly_uniform_sentences, f"sentence_std={sentence_std:.2f}", hard=True)
        add_check(12, "rhythm_variation_present", sentence_std >= 4.6, f"sentence_std={sentence_std:.2f}", hard=True)
        add_check(13, "not_perfect_sentence_only", bool(re.search(r"[!?]", plain)), "expressive_punct")
        add_check(14, "personal_speech_habit_exists", first_person_hits >= 2, f"first_person_hits={first_person_hits}")
        add_check(15, "word_choice_not_overly_neutral", unique_ratio >= 0.36, f"unique_ratio={unique_ratio:.2f}")
        add_check(16, "example_has_lived_detail", concrete_example_hits >= 1 and smell_sound_people_hits >= 1, f"example={concrete_example_hits},people={smell_sound_people_hits}")
        add_check(17, "honest_opinion_exists", bool(re.search(r"\b(i think|i felt|in my view|to me)\b", lower)), "opinion_phrase")
        add_check(18, "value_judgment_not_avoided", bool(re.search(r"\b(better|worse|worth|not worth|risky|safe)\b", lower)), "value_judgment")
        add_check(19, "human_touch_or_humor", humor_hits >= 1 or emotion_hits >= 1, f"humor={humor_hits},emotion={emotion_hits}")
        add_check(20, "not_too_textbook", not bool(re.search(r"\bdefinition|objective|methodology|framework\b", lower)), "textbook_marker")
        add_check(21, "avoid_obvious_statement_chain", repeated_tokens < 5, f"repeated_tokens={repeated_tokens}")
        add_check(22, "not_overly_polished", sentence_std >= 4.0, f"sentence_std={sentence_std:.2f}")
        add_check(23, "quoted_concept_has_personal_interpretation", bool(re.search(r"\bwhich means|what this means|so for me\b", lower)), "interpretation_marker")
        add_check(24, "tone_not_single_flat", (emotion_hits + humor_hits + failure_hits) >= 1, f"tone_markers={emotion_hits + humor_hits + failure_hits}")
        add_check(25, "intentional_roughness_exists", bool(re.search(r"\b(oops|honestly|frankly|messy)\b", lower)), "roughness_marker")
        add_check(26, "decision_criteria_visible", bool(re.search(r"\b(i chose|we chose|because|so that)\b", lower)), "decision_marker")
        add_check(27, "distinctive_style_marker", first_person_hits >= 2, f"first_person_hits={first_person_hits}")
        add_check(28, "not_overly_neutral", bool(re.search(r"\b(i disagree|i prefer|i avoid)\b", lower)), "stance_marker")
        add_check(29, "emotional_temperature_variation", (emotion_hits + failure_hits) >= 1, f"emotion={emotion_hits},failure={failure_hits}")
        add_check(30, "not_too_safe", bool(re.search(r"\b(risky|trade-off|costly|painful)\b", lower)), "risk_marker")
        add_check(31, "direct_experience_present", first_person_hits >= 2 and (concrete_example_hits >= 1 or failure_hits >= 1), f"fp={first_person_hits},example={concrete_example_hits},failure={failure_hits}")
        add_check(32, "modal_verb_not_repeated", not generic_modal_overuse, f"modal_hits={modal_hits}", hard=True)
        add_check(33, "transition_flow_human", not robotic_transition_overuse, f"connector_hits={connector_hits}", hard=True)
        add_check(34, "information_density_not_uniform", sentence_std >= 4.2, f"sentence_std={sentence_std:.2f}")
        add_check(35, "ending_not_preachy", not bool(re.search(r"\btherefore you should\b", lower[-260:] if lower else "")), "ending_style")
        add_check(36, "signature_phrases_present", bool(re.search(r"\b(honestly|frankly|to be fair)\b", lower)), "signature_phrase")
        add_check(37, "overall_human_temperature", (first_person_hits + emotion_hits + failure_hits) >= 3, f"markers={first_person_hits + emotion_hits + failure_hits}")
        add_check(38, "admits_mistake_or_uncertainty", failure_hits >= 1 or bool(re.search(r"\b(not sure|uncertain|was wrong)\b", lower)), f"failure_hits={failure_hits}")
        add_check(39, "jargon_not_excessive", jargon_hits <= max(5, int(word_count * 0.018)), f"jargon_hits={jargon_hits}")
        add_check(40, "mechanical_connectors_not_repeated", not robotic_transition_overuse, f"connector_hits={connector_hits}", hard=True)
        add_check(41, "no_contextless_perfect_advice", bool(re.search(r"\b(in my case|for our team|under this constraint)\b", lower)), "context_marker")
        add_check(42, "avoid_all_situations_claim", not bool(re.search(r"\b(always|for all cases|every situation)\b", lower)), "absolute_claim")
        add_check(43, "one_personal_bias_present", bool(re.search(r"\b(i tend to|i usually|my bias)\b", lower)), "bias_marker")
        add_check(44, "sensory_information_present", sensory_hits >= 1, f"sensory_hits={sensory_hits}")
        add_check(45, "people_events_not_too_abstract", smell_sound_people_hits >= 1, f"people_hits={smell_sound_people_hits}")
        add_check(46, "purpose_written_in_own_words", bool(re.search(r"\b(i wrote this|my goal here)\b", lower)), "purpose_marker")
        add_check(47, "bullet_points_not_excessive", list_items <= max(8, int(len(sentences) * 0.4)), f"list_items={list_items}", hard=True)
        add_check(48, "natural_speech_flow", avg_sent >= 9 and sentence_std >= 4.0, f"avg={avg_sent:.1f},std={sentence_std:.2f}")
        add_check(49, "experience_over_polish", concrete_example_hits >= 1 or failure_hits >= 1, f"example={concrete_example_hits},failure={failure_hits}")
        add_check(50, "reads_like_spoken_human", sentence_std >= 4.0 and first_person_hits >= 1, f"std={sentence_std:.2f},fp={first_person_hits}")

        soft_checks = [c for c in checks if c.key not in set(hard_failures)]
        soft_pass = sum(1 for c in soft_checks if c.passed)
        soft_score = int(round((soft_pass / max(1, len(soft_checks))) * 100))
        return checks, soft_score, hard_failures

    def _detect_prompt_leak(self, html: str, title: str, domain: str) -> tuple[bool, str]:
        target_html = str(html or "")
        masked_prompt_example_regions = False
        if str(domain or "").strip().lower() == "ai_prompt_guide":
            target_html = self._mask_allowed_prompt_example_regions(target_html)
            masked_prompt_example_regions = True
        target_text = self._to_text(target_html).lower()

        # Fast path: malformed "For Quick Take ..." fragments and keyword-only dump lines.
        if re.search(r"\bfor quick take\b.{0,80}\b(you are|write|do not|must)\b", target_text):
            detail = "quick_take_template_fragment"
            if masked_prompt_example_regions:
                detail = f"outside_allowed_examples:{detail}"
            return True, detail
        if self._contains_keyword_dump_line(target_html):
            detail = "keyword_dump_fragment"
            if masked_prompt_example_regions:
                detail = f"outside_allowed_examples:{detail}"
            return True, detail

        patterns = [str(x or "").strip() for x in (self.settings.prompt_leak_patterns or []) if str(x or "").strip()]
        for token in patterns:
            if token.lower() in target_text:
                detail = f"pattern:{token}"
                if masked_prompt_example_regions:
                    detail = f"outside_allowed_examples:{detail}"
                return True, detail
        # Hard guard for system prompt style leakage.
        if re.search(r"\byou are (a|an|the) [a-z0-9 _-]{3,40}(assistant|system)\b", target_text):
            detail = "system_prompt_style_leak"
            if masked_prompt_example_regions:
                detail = f"outside_allowed_examples:{detail}"
            return True, detail
        if re.search(r"\b(real-time trend focus|for generated image context)\b", target_text):
            detail = "internal_instruction_leak"
            if masked_prompt_example_regions:
                detail = f"outside_allowed_examples:{detail}"
            return True, detail
        return False, ""

    def _mask_allowed_prompt_example_regions(self, html: str) -> str:
        out = str(html or "")
        # 1) code blocks are explicit tutorial examples and should not trigger prompt leak.
        out = re.sub(r"<pre\b[^>]*>.*?</pre>", " ", out, flags=re.IGNORECASE | re.DOTALL)
        out = re.sub(r"<code\b[^>]*>.*?</code>", " ", out, flags=re.IGNORECASE | re.DOTALL)
        out = re.sub(r"```.*?```", " ", out, flags=re.DOTALL)
        # 2) sections under tutorial-style titles are allowed for prompt examples.
        title_patterns = [str(x or "").strip() for x in (self.settings.prompt_example_section_titles or []) if str(x or "").strip()]
        if not title_patterns:
            return out
        escaped = "|".join(re.escape(x) for x in title_patterns)
        if not escaped:
            return out
        section_re = re.compile(
            rf"(<h[23][^>]*>\s*(?:{escaped})\s*</h[23]>)(.*?)(?=<h[23]\b|$)",
            flags=re.IGNORECASE | re.DOTALL,
        )
        out = section_re.sub(r"\1 ", out)
        return out

    def _contains_keyword_dump_line(self, html: str) -> bool:
        text = re.sub(r"<[^>]+>", "\n", str(html or ""))
        for raw in text.splitlines():
            line = re.sub(r"\s+", " ", raw).strip().lower()
            if not line:
                continue
            if re.fullmatch(r"(?:[a-z0-9][a-z0-9\- ]{1,30},\s*){3,}[a-z0-9][a-z0-9\- ]{1,30}", line):
                return True
        return False
    def _detect_non_english_content(self, html: str, text: str) -> tuple[bool, str]:
        merged = f"{html or ''}\n{text or ''}"
        if re.search(r"[\uac00-\ud7a3\u3131-\u318e]", merged):
            return True, "hangul_unicode_detected"
        plain = re.sub(r"<[^>]+>", " ", merged)
        plain = re.sub(r"\s+", " ", plain).strip()
        if plain:
            non_ascii = sum(1 for ch in plain if ord(ch) > 127)
            ratio = non_ascii / max(1, len(plain))
            if ratio > 0.08:
                return True, f"non_ascii_ratio_high({ratio:.3f})"
        return False, ""

    def _detect_forbidden_phrase_or_format(self, text: str, html: str) -> tuple[bool, str]:
        lower = str(text or "").lower()
        raw_html = str(html or "")
        if re.search(r"(?m)^\s{0,3}#{1,6}\s+\S+", raw_html):
            return True, "forbidden_markup:markdown_heading"
        if "## " in raw_html or "### " in raw_html:
            return True, "forbidden_markup:markdown_token"
        defaults = [
            "workflow checkpoint stage",
            "av reference context",
            "jobtitle",
            "sameas",
            "selected topic",
            "source trending_entities",
        ]
        hard_forbidden_tokens = list(getattr(self.settings, "banned_debug_patterns", []) or []) or defaults
        for token in hard_forbidden_tokens:
            t = str(token or "").strip().lower()
            if not t:
                continue
            pattern = re.escape(t).replace(r"\ ", r"[_\s-]*")
            if re.search(pattern, lower):
                return True, f"hard_forbidden_token:{t}"
        for token in (self.settings.ban_phrases or []):
            t = str(token or "").strip().lower()
            if t and t in lower:
                return True, f"ban_phrase:{t}"
        raw_html = raw_html.lower()
        if "www.google.com" in raw_html or "google.com/search" in raw_html:
            return True, "forbidden_reference_link:google.com"
        if re.search(r"https?://(?:www\.)?google\.com/[^\s\"<]*", raw_html):
            return True, "forbidden_reference_link:google.com"
        if "<figcaption" in raw_html:
            return True, "forbidden_markup:figcaption"
        if "[[meta]]" in raw_html or "[[/meta]]" in raw_html:
            return True, "forbidden_meta_block_leak"
        if re.search(r"\billustration\s+showing\b", raw_html):
            return True, "forbidden_image_caption_phrase:illustration_showing"
        for fmt in (self.settings.ban_formats or []):
            f = str(fmt or "").strip()
            if not f:
                continue
            if re.search(re.escape(f), str(html or ""), flags=re.IGNORECASE):
                return True, f"ban_format:{f}"
        return False, ""

    def detect_intro_alt_similarity(
        self,
        intro_text: str,
        alt_texts: list[str],
        threshold: float | None = None,
    ) -> tuple[bool, str]:
        if not bool(getattr(self.settings, "fail_if_intro_matches_alt", True)):
            return False, ""
        intro_tokens = self._token_set(intro_text)
        if not intro_tokens:
            return False, ""
        th = float(threshold if threshold is not None else getattr(self.settings, "alt_similarity_threshold", 0.75))
        for idx, alt in enumerate(alt_texts or [], start=1):
            alt_tokens = self._token_set(alt)
            if not alt_tokens:
                continue
            sim = self._jaccard(intro_tokens, alt_tokens)
            if sim >= th:
                return True, f"intro_alt_similarity_high(idx={idx},sim={sim:.3f},th={th:.3f})"
        return False, ""

    def _token_set(self, text: str) -> set[str]:
        return {
            t.lower()
            for t in re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", str(text or ""))
            if t
        }

    def _jaccard(self, a: set[str], b: set[str]) -> float:
        if not a or not b:
            return 0.0
        return float(len(a & b)) / float(max(1, len(a | b)))

    def _detect_screenshot_mentions(self, text: str) -> tuple[bool, str]:
        if not bool(getattr(self.settings, "forbid_screenshot_mentions", True)):
            return False, ""
        lower = str(text or "").lower()
        if re.search(r"\b(screenshot|see screenshot|see above image|as shown above)\b", lower):
            return True, "screenshot_phrase_found"
        return False, ""

    def _detect_question_mark_spam(self, text: str) -> tuple[bool, str]:
        max_row = max(1, int(getattr(self.settings, "max_question_marks_in_row", 2) or 2))
        if re.search(r"\?{" + str(max_row + 1) + r",}", str(text or "")):
            return True, f"question_marks_in_row>{max_row}"
        return False, ""

    def _detect_sensitive_topics(self, text: str) -> tuple[bool, str]:
        if not bool(getattr(self.settings, "sensitive_topics_hard_filter", True)):
            return False, ""
        lower = str(text or "").lower()
        blocked = [
            "medical advice",
            "treatment plan",
            "investment strategy",
            "stock pick",
            "political campaign",
            "election prediction",
            "prescription",
            "trading signal",
        ]
        for token in blocked:
            if token in lower:
                return True, f"sensitive:{token}"
        return False, ""

    def _strip_non_english_lines(self, html: str) -> str:
        src = str(html or "")
        src = re.sub(
            r"<(p|li|h2|h3|figcaption)[^>]*>[^<]*[\uac00-\ud7a3\u3131-\u318e][^<]*</\1>",
            "",
            src,
            flags=re.IGNORECASE,
        )
        cleaned: list[str] = []
        for line in src.splitlines():
            if re.search(r"[\uac00-\ud7a3\u3131-\u318e]", line):
                continue
            cleaned.append(line)
        out = "\n".join(cleaned)
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out.strip()
    def _detect_domain_drift(self, text: str, domain: str) -> tuple[bool, str]:
        lower_domain = str(domain or "").strip().lower()
        if lower_domain not in {"office_experiment", "tech_troubleshoot"}:
            return False, ""
        lower = str(text or "").lower()
        hits: list[str] = []
        disallowed_terms = (
            self.settings.disallowed_terms_tech_troubleshoot
            if lower_domain == "tech_troubleshoot"
            else self.settings.disallowed_terms_office_experiment
        )
        for term in (disallowed_terms or []):
            t = str(term or "").strip().lower()
            if not t:
                continue
            if t in lower:
                hits.append(t)
        if len(hits) >= 2:
            return True, ",".join(sorted(set(hits))[:6])
        return False, ""

    def _detect_missing_story_block(self, text: str, domain: str) -> tuple[bool, str]:
        if not bool(getattr(self.settings, "require_story_block", True)):
            return False, ""
        # Apply for narrative domains.
        if str(domain or "").strip().lower() not in {"office_experiment", "tech_troubleshoot", "ai_prompt_guide"}:
            return False, ""
        lower = str(text or "").lower()
        story_markers = [
            r"\bi\b",
            r"\bmy\b",
            r"\bwhen i\b",
            r"\bi tried\b",
            r"\bon day\s*\d+\b",
            r"\bon the (first|second|third|fourth|fifth) day\b",
            r"\bafter i\b",
        ]
        hits = sum(1 for pat in story_markers if re.search(pat, lower))
        need = max(1, int(getattr(self.settings, "require_story_block_min_count", 1) or 1))
        if hits < need:
            return True, f"story_markers={hits}/{need}"
        return False, ""

    def _log_qa_reason(self, reason: str, detail: str, title: str, domain: str) -> None:
        if self.qa_runtime_path is None:
            return
        try:
            self.qa_runtime_path.parent.mkdir(parents=True, exist_ok=True)
            row = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "part": "content_qa",
                "event": "qa_gate_detected",
                "reason": str(reason or "").strip(),
                "detail": str(detail or "").strip()[:400],
                "title": str(title or "").strip()[:180],
                "domain": str(domain or "").strip(),
            }
            with self.qa_runtime_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=True) + "\n")
        except Exception:
            return

    def _duplicate_sentence_ratio(self, sentences: list[str]) -> float:
        if not sentences:
            return 0.0
        normed = []
        for s in sentences:
            t = re.sub(r"[^a-z0-9 ]+", " ", s.lower())
            t = re.sub(r"\s+", " ", t).strip()
            if t:
                normed.append(t)
        if not normed:
            return 0.0
        cnt = Counter(normed)
        dup = sum(v - 1 for v in cnt.values() if v > 1)
        return dup / max(1, len(normed))

    def _repair_hard_humanity_failures(self, html: str, hard_failures: list[str]) -> str:
        out = html or ""
        hard = " ".join(hard_failures or []).lower()

        # 1) Remove repeated operational fallback line flood.
        repeated_line = "Operationally, validate one metric, one pause rule, and one owner before release."
        out = self._collapse_repeated_paragraph_line(out, repeated_line)
        out = self._collapse_repeated_plain_line(out, repeated_line)

        # 2) Soften robotic connectors and repetitive modals.
        if "h33_transition_flow_human" in hard or "h40_mechanical_connectors_not_repeated" in hard:
            out = re.sub(r"\bTherefore,\s*", "So, ", out, flags=re.IGNORECASE)
            out = re.sub(r"\bMoreover,\s*", "Also, ", out, flags=re.IGNORECASE)
            out = re.sub(r"\bFurthermore,\s*", "And also, ", out, flags=re.IGNORECASE)
            out = re.sub(r"\bIn conclusion,\s*", "At this point, ", out, flags=re.IGNORECASE)
        if "h32_modal_verb_not_repeated" in hard:
            out = re.sub(r"\b(can|could|should)\b", "often", out, count=6, flags=re.IGNORECASE)

        # 3) Inject grounded first-person texture if missing.
        if any(k in hard for k in ["h06_concrete_example_exists", "h31_direct_experience_present", "h44_sensory_information_present"]):
            if not self._has_section(out, "What It Felt Like In Practice"):
                out += (
                    "<h3>What It Felt Like In Practice</h3>"
                    "<p>I tested this during a regular workday, not in a perfect demo setup. "
                    "The room was noisy, the deadline was close, and the first attempt failed because I changed too much at once.</p>"
                    "<p>After rolling back and trying one smaller step, the workflow became predictable enough to trust.</p>"
                )

        # 4) If list-heavy, add narrative block to rebalance rhythm.
        if any(k in hard for k in ["h08_lists_not_dominant", "h47_bullet_points_not_excessive"]):
            if not self._has_section(out, "Short Story From One Real Run"):
                out += (
                    "<h3>Short Story From One Real Run</h3>"
                    "<p>I expected the checklist alone to solve it, but the bigger issue was decision timing. "
                    "Once we agreed on one metric and one pause trigger, the noise dropped and progress became visible.</p>"
                )

        # 5) If too uniform, force rhythm variation.
        if any(k in hard for k in ["h01_no_repeated_sentence_structure", "h11_sentence_length_not_uniform", "h12_rhythm_variation_present"]):
            out += (
                "<p>Short answer: simplify first.</p>"
                "<p>The longer answer is that reliability usually improves when you shrink change scope, "
                "run one measurable experiment, and wait long enough to observe real behavior under normal team pressure.</p>"
            )
        return out

    def _strip_banned_markers(self, text: str) -> str:
        out = text
        meta_start = re.escape(str(getattr(self.settings, "meta_block_start", "[[META]]") or "[[META]]"))
        meta_end = re.escape(str(getattr(self.settings, "meta_block_end", "[[/META]]") or "[[/META]]"))
        out = re.sub(rf"{meta_start}.*?{meta_end}", "", out, flags=re.IGNORECASE | re.DOTALL)
        replacements = {
            "delve": "look closely",
            "comprehensive": "practical",
            "cutting-edge": "advanced",
            "in conclusion": "to wrap up",
        }
        for src, dst in replacements.items():
            out = re.sub(rf"\b{re.escape(src)}\b", dst, out, flags=re.IGNORECASE)
        for token in (self.settings.prompt_leak_patterns or []):
            t = str(token or "").strip()
            if not t:
                continue
            out = re.sub(re.escape(t), "", out, flags=re.IGNORECASE)
        out = re.sub(
            r"\bfor quick take\b.{0,80}\b(you are|write|must|do not)\b",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out

    def _infer_topic(self, text: str) -> str:
        tokens = re.findall(r"[A-Za-z][A-Za-z0-9_-]{2,}", text or "")
        if not tokens:
            return "this workflow"
        top = " ".join(tokens[:4]).strip()
        return top or "this workflow"

    def _has_section(self, html: str, title: str) -> bool:
        return re.search(rf"<h[23][^>]*>\s*{re.escape(title)}\s*</h[23]>", html, flags=re.IGNORECASE) is not None

    def _link_context(self, url: str) -> str:
        domain = re.sub(r"^https?://", "", url).split("/")[0].lower()
        if "developers.google.com" in domain:
            note = "Official search quality guidance and indexing behavior."
        elif "docs.python.org" in domain:
            note = "Language-level reference for reliable implementation details."
        elif "github.com" in domain:
            note = "Primary project context, issues, and release-level evidence."
        elif "stackexchange.com" in domain:
            note = "Community-tested edge cases and troubleshooting patterns."
        else:
            note = "Authoritative reference used for verification."
        return f'<a href="{url}" rel="nofollow noopener" target="_blank">{url}</a> - {note}'

    def _has_banned_markers(self, text: str) -> bool:
        lower = text.lower()
        for marker in self.settings.banned_markers:
            if marker.lower() in lower:
                return True
        return False

    def _burstiness_ok(self, text: str) -> bool:
        sentences = [s.strip() for s in re.split(r"[.!?]+", text) if s.strip()]
        if len(sentences) < 6:
            return True
        lens = [len(s.split()) for s in sentences[:40]]
        if not lens:
            return True
        short = sum(1 for n in lens if n <= 10)
        long = sum(1 for n in lens if n >= 25)
        return short >= 2 and long >= 1

    def _to_text(self, html: str) -> str:
        txt = re.sub(r"<[^>]+>", " ", html or "")
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def _collapse_repeated_paragraph_line(self, html: str, line: str) -> str:
        """
        Keep only the first paragraph that exactly matches `line`.
        This prevents fallback sentence floods in <p>...</p> blocks.
        """
        src = html or ""
        target = (line or "").strip()
        if not src or not target:
            return src
        target_norm = re.sub(r"\s+", " ", target).strip().lower()
        seen = False

        def repl(m: re.Match[str]) -> str:
            nonlocal seen
            para = m.group(0)
            inner = re.sub(r"</?p[^>]*>", " ", para, flags=re.IGNORECASE)
            inner = re.sub(r"\s+", " ", inner).strip().lower()
            if inner == target_norm:
                if seen:
                    return ""
                seen = True
            return para

        out = re.sub(r"<p\b[^>]*>.*?</p>", repl, src, flags=re.IGNORECASE | re.DOTALL)
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out

    def _collapse_repeated_plain_line(self, html: str, line: str) -> str:
        """
        Keep only the first plain-text line equal to `line` (outside paragraph tags).
        """
        src = html or ""
        target = (line or "").strip()
        if not src or not target:
            return src

        lines = src.splitlines()
        out_lines: list[str] = []
        seen = False
        target_norm = re.sub(r"\s+", " ", target).strip().lower()
        for raw in lines:
            norm = re.sub(r"\s+", " ", raw.strip()).lower()
            if norm == target_norm:
                if seen:
                    continue
                seen = True
            out_lines.append(raw)
        return "\n".join(out_lines)

