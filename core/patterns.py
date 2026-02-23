from __future__ import annotations

from dataclasses import dataclass

from .scout import TopicCandidate


@dataclass(frozen=True)
class WritingPattern:
    key: str
    domain: str
    stage: str
    objective: str
    outline: list[str]


PATTERNS: dict[str, WritingPattern] = {
    "pattern_1_authority_announcement": WritingPattern(
        key="pattern_1_authority_announcement",
        domain="office_experiment",
        stage="decision",
        objective="Leverage official credibility signals and convert high-intent readers.",
        outline=[
            "State official recognition or qualification clearly.",
            "Explain why it matters to the reader's risk.",
            "Show concrete proof assets (numbers, docs, references).",
            "Give practical next-step process and timeline.",
            "Close with one clear CTA.",
        ],
    ),
    "pattern_3_cost_objection": WritingPattern(
        key="pattern_3_cost_objection",
        domain="office_experiment",
        stage="comparison",
        objective="Handle price objections with value and failure-cost framing.",
        outline=[
            "Break down what drives real cost.",
            "Show hidden risk of choosing only by lowest price.",
            "Provide a checklist to compare options fairly.",
            "Present a practical budget range by scenario.",
            "End with a consult CTA and required prep items.",
        ],
    ),
    "pattern_5_failure_prevention": WritingPattern(
        key="pattern_5_failure_prevention",
        domain="office_experiment",
        stage="awareness",
        objective="Use failure patterns to reduce uncertainty and increase trust.",
        outline=[
            "Describe common failure symptoms.",
            "Explain structural root causes.",
            "Provide prevention checklist.",
            "Share safer implementation path.",
            "Conclude with a risk-reduction CTA.",
        ],
    ),
    "pattern_6_process_open": WritingPattern(
        key="pattern_6_process_open",
        domain="office_experiment",
        stage="decision",
        objective="Reduce anxiety by exposing transparent process and milestones.",
        outline=[
            "Explain each project phase and owner.",
            "List deliverables and checkpoints.",
            "Show quality controls and revision scope.",
            "Add timeline and communication rules.",
            "Close with onboarding CTA.",
        ],
    ),
    "pattern_8_case_study": WritingPattern(
        key="pattern_8_case_study",
        domain="office_experiment",
        stage="comparison",
        objective="Prove repeatable results through problem-strategy-outcome structure.",
        outline=[
            "State client context and initial problem.",
            "Show strategy and decision logic.",
            "Quantify measurable outcomes.",
            "Extract reusable lessons.",
            "Offer next action CTA with expected scope.",
        ],
    ),
    "pattern_10_vendor_selection": WritingPattern(
        key="pattern_10_vendor_selection",
        domain="office_experiment",
        stage="comparison",
        objective="Educate readers to choose vendors using decision questions.",
        outline=[
            "List critical vendor-screening questions.",
            "Explain why each question matters.",
            "Provide red-flag answers to avoid.",
            "Suggest scoring rubric.",
            "End with consultation CTA.",
        ],
    ),
    "pattern_11_benchmark": WritingPattern(
        key="pattern_11_benchmark",
        domain="office_experiment",
        stage="awareness",
        objective="Build authority with benchmark and trend analysis.",
        outline=[
            "Present baseline benchmark values.",
            "Compare realistic ranges by segment.",
            "Interpret implications for decision-making.",
            "Provide action checklist.",
            "Close with strategy CTA.",
        ],
    ),
    "pattern_12_schedule_to": WritingPattern(
        key="pattern_12_schedule_to",
        domain="office_experiment",
        stage="decision",
        objective="Use schedule and capacity clarity to trigger timely action.",
        outline=[
            "Disclose current timeline and capacity.",
            "Explain lead-time risks.",
            "Provide preparation checklist before inquiry.",
            "Clarify what happens after submission.",
            "End with urgent but factual CTA.",
        ],
    ),
    "pattern_13_ai_prompt_guide": WritingPattern(
        key="pattern_13_ai_prompt_guide",
        domain="ai_prompt_guide",
        stage="tutorial",
        objective="Teach practical prompt usage with copy-ready examples for normal office users.",
        outline=[
            "Explain the real task context in plain language.",
            "Show one good prompt example and why it works.",
            "Show one bad example and a corrected version.",
            "Provide a quick reusable prompt template.",
            "Close with a safe usage checklist.",
        ],
    ),
}


class PatternEngine:
    def choose(self, candidate: TopicCandidate) -> WritingPattern:
        title = (candidate.title or "").lower()
        body = (candidate.body or "").lower()
        text = f"{title}\n{body}"

        if any(
            k in text
            for k in [
                "prompt",
                "프롬프트",
                "system instruction",
                "instruction template",
                "prompt template",
                "prompt guide",
                "chatgpt prompt",
                "gemini prompt",
                "ai prompt",
            ]
        ):
            return PATTERNS["pattern_13_ai_prompt_guide"]

        if any(k in text for k in ["cost", "price", "budget", "pricing", "견적", "비용"]):
            return PATTERNS["pattern_3_cost_objection"]
        if any(k in text for k in ["fail", "mistake", "broken", "pitfall", "실패", "문제"]):
            return PATTERNS["pattern_5_failure_prevention"]
        if any(k in text for k in ["process", "workflow", "pipeline", "프로세스", "절차"]):
            return PATTERNS["pattern_6_process_open"]
        if any(k in text for k in ["case study", "results", "conversion", "성과", "사례"]):
            return PATTERNS["pattern_8_case_study"]
        if any(k in text for k in ["select", "choose", "vendor", "agency", "업체", "선택"]):
            return PATTERNS["pattern_10_vendor_selection"]
        if any(k in text for k in ["benchmark", "trend", "report", "통계", "지표"]):
            return PATTERNS["pattern_11_benchmark"]
        if any(k in text for k in ["deadline", "launch", "schedule", "일정", "마감"]):
            return PATTERNS["pattern_12_schedule_to"]

        if candidate.source == "github":
            return PATTERNS["pattern_6_process_open"]
        if candidate.source == "hackernews":
            return PATTERNS["pattern_11_benchmark"]
        return PATTERNS["pattern_10_vendor_selection"]
