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
    "pattern_troubleshoot_quickfix": WritingPattern(
        key="pattern_troubleshoot_quickfix",
        domain="tech_troubleshoot",
        stage="fix",
        objective="Deliver a fast troubleshooting path for mainstream users.",
        outline=[
            "Quick Answer with plain-language root cause.",
            "Explain why the issue happens in normal usage.",
            "Provide Fix 1-3 from safest to strongest.",
            "Add one advanced fix and one hardware sanity check.",
            "Close with prevention checklist and next-step CTA.",
        ],
    ),
    "pattern_troubleshoot_update_break": WritingPattern(
        key="pattern_troubleshoot_update_break",
        domain="tech_troubleshoot",
        stage="fix",
        objective="Resolve post-update breakages without risky deep system steps.",
        outline=[
            "Describe the symptom users see right after update.",
            "List update-specific checks and rollback-safe actions.",
            "Show what worked in a first-person test sequence.",
            "Provide prevention steps for the next update cycle.",
            "End with a concise checklist users can reuse.",
        ],
    ),
    "pattern_troubleshoot_connectivity": WritingPattern(
        key="pattern_troubleshoot_connectivity",
        domain="tech_troubleshoot",
        stage="fix",
        objective="Fix Wi-Fi/Bluetooth/network reliability issues quickly.",
        outline=[
            "Clarify if problem is device-side or network-side.",
            "Run Fix 1-3 in strict order to avoid side effects.",
            "Call out one anti-pattern that makes issue worse.",
            "Provide advanced fix only after basic checks pass.",
            "Finish with prevention routine and escalation rule.",
        ],
    ),
    "pattern_troubleshoot_audio": WritingPattern(
        key="pattern_troubleshoot_audio",
        domain="tech_troubleshoot",
        stage="fix",
        objective="Restore sound/mic behavior with minimal friction.",
        outline=[
            "Identify exact audio symptom and affected app/device.",
            "Apply safe baseline checks before changing settings.",
            "Provide tested fixes with expected outcomes.",
            "Add hardware port/cable/device sanity checks.",
            "Close with prevention habits for daily use.",
        ],
    ),
    "pattern_troubleshoot_performance": WritingPattern(
        key="pattern_troubleshoot_performance",
        domain="tech_troubleshoot",
        stage="fix",
        objective="Recover sluggish, freezing, or crashing device behavior.",
        outline=[
            "Define symptom and trigger pattern clearly.",
            "Prioritize fastest high-impact fixes first.",
            "Compare two rejected approaches and why they failed.",
            "Provide one advanced fix with caution boundary.",
            "Finish with a repeatable health-check checklist.",
        ],
    ),
    "pattern_13_ai_prompt_guide": WritingPattern(
        key="pattern_13_ai_prompt_guide",
        domain="ai_prompt_guide",
        stage="tutorial",
        objective="Teach practical prompt usage with copy-ready examples for mainstream users.",
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

        if any(k in text for k in ["wifi", "bluetooth", "network", "internet", "dns", "router"]):
            return PATTERNS["pattern_troubleshoot_connectivity"]
        if any(k in text for k in ["audio", "sound", "mic", "speaker", "headset"]):
            return PATTERNS["pattern_troubleshoot_audio"]
        if any(k in text for k in ["slow", "lag", "freeze", "crash", "performance"]):
            return PATTERNS["pattern_troubleshoot_performance"]
        if any(k in text for k in ["update", "patch", "upgrade", "version", "after update"]):
            return PATTERNS["pattern_troubleshoot_update_break"]
        return PATTERNS["pattern_troubleshoot_quickfix"]
