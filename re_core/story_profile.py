from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import urlparse


_SECURITY_TOKENS = (
    "security",
    "vulnerability",
    "patch",
    "cve",
    "breach",
    "malware",
    "ransomware",
)
_POLICY_TOKENS = (
    "policy",
    "privacy",
    "regulation",
    "ban",
    "consent",
    "tracking",
    "vpn",
)
_AI_TOKENS = (
    "ai",
    "model",
    "openai",
    "anthropic",
    "gemini",
    "copilot",
    "claude",
)
_PLATFORM_TOKENS = (
    "ios",
    "android",
    "iphone",
    "pixel",
    "windows",
    "macos",
    "apple",
    "microsoft",
    "google",
    "browser",
)
_CHIPS_TOKENS = (
    "nvidia",
    "chip",
    "gpu",
    "semiconductor",
    "amd",
    "intel",
)
_HOME_TOKENS = (
    "air purifier",
    "purifier",
    "hepa",
    "filter replacement",
    "allergy",
    "dust",
    "bedroom",
    "living room",
    "humidifier",
    "vacuum",
)
_WELLNESS_TOKENS = (
    "energy drink",
    "celsius",
    "red bull",
    "monster",
    "ghost",
    "taurine",
    "caffeine",
    "pre workout",
    "sugar free",
    "supplement",
)
_CONSUMER_TOKENS = (
    "best ",
    "ranked",
    "review",
    "reviews",
    "tested",
    "versus",
    " vs ",
    "buying guide",
    "worth it",
    "roundup",
    "top ",
)


@dataclass(frozen=True)
class StoryProfile:
    category: str
    topic_slug: str
    overlay_label: str
    scene_hint: str
    search_tags: list[str]
    subject_phrase: str
    decision_frame: str
    operational_line: str
    scenario_line: str
    watch_line: str
    detail_items: list[str]
    questions: list[str]
    comparisons: list[str]
    tech_story: bool


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _lower_blob(*parts: str) -> str:
    return _clean(" ".join(str(x or "") for x in parts)).lower()


def infer_news_category(text: str, explicit: str = "") -> str:
    lower = _lower_blob(explicit, text)
    explicit_norm = normalize_story_category(explicit)
    if explicit_norm:
        return explicit_norm
    if any(tok in lower for tok in _HOME_TOKENS):
        return "home"
    if any(tok in lower for tok in _WELLNESS_TOKENS):
        return "wellness"
    if any(tok in lower for tok in _SECURITY_TOKENS):
        return "security"
    if any(tok in lower for tok in _POLICY_TOKENS):
        return "policy"
    if any(tok in lower for tok in _AI_TOKENS):
        return "ai"
    if any(tok in lower for tok in _CHIPS_TOKENS):
        return "chips"
    if any(tok in lower for tok in _PLATFORM_TOKENS):
        return "platform"
    if any(tok in lower for tok in _CONSUMER_TOKENS):
        return "consumer"
    return "platform"


def normalize_story_category(value: str) -> str:
    key = _clean(value).lower()
    aliases = {
        "sec": "security",
        "privacy": "policy",
        "mobile": "platform",
        "tech": "platform",
        "shopping": "consumer",
        "review": "consumer",
        "reviews": "consumer",
        "beverage": "wellness",
        "drink": "wellness",
        "lifestyle": "consumer",
    }
    key = aliases.get(key, key)
    allowed = {"security", "policy", "ai", "chips", "platform", "home", "wellness", "consumer"}
    return key if key in allowed else ""


def build_story_tags(*, title: str, snippet: str = "", category: str = "") -> list[str]:
    profile = infer_story_profile(title=title, snippet=snippet, category=category)
    return list(profile.search_tags)


def overlay_label_for_story(*, title: str, snippet: str = "", category: str = "") -> str:
    return infer_story_profile(title=title, snippet=snippet, category=category).overlay_label


def looks_like_tech_story(*, title: str, snippet: str = "", category: str = "") -> bool:
    return bool(infer_story_profile(title=title, snippet=snippet, category=category).tech_story)


def filter_relevant_authority_links(
    authority_links: list[str],
    *,
    title: str,
    snippet: str = "",
    category: str = "",
) -> list[str]:
    profile = infer_story_profile(title=title, snippet=snippet, category=category)
    out: list[str] = []
    seen: set[str] = set()
    for link in authority_links or []:
        clean = _clean(link)
        if not clean:
            continue
        try:
            host = (urlparse(clean).netloc or "").lower()
        except Exception:
            host = ""
        if host in seen:
            continue
        if profile.tech_story:
            out.append(clean)
            seen.add(host)
            if len(out) >= 3:
                break
            continue
        if any(tok in host for tok in ("fda.gov", "nih.gov", "epa.gov", "consumerreports.org", "wirecutter.com")):
            out.append(clean)
            seen.add(host)
            if len(out) >= 2:
                break
    return out


def infer_story_profile(*, title: str, snippet: str = "", category: str = "") -> StoryProfile:
    title_clean = _clean(title)
    snippet_clean = _clean(snippet)
    lower = _lower_blob(title_clean, snippet_clean)
    cat = infer_news_category(f"{title_clean} {snippet_clean}", explicit=category)

    topic_slug = "general"
    if any(tok in lower for tok in _WELLNESS_TOKENS):
        topic_slug = "energy_drinks"
        cat = "wellness"
    elif any(tok in lower for tok in _HOME_TOKENS):
        topic_slug = "air_purifiers"
        cat = "home"
    elif "vpn" in lower:
        topic_slug = "privacy_tools"
        cat = "policy"
    elif any(tok in lower for tok in _CONSUMER_TOKENS):
        topic_slug = "consumer_reviews"
        if cat not in {"home", "wellness"}:
            cat = "consumer"
    elif cat == "ai":
        topic_slug = "ai_tools"
    elif cat == "chips":
        topic_slug = "chips"
    elif cat == "security":
        topic_slug = "security_update"
    elif cat == "policy":
        topic_slug = "policy_shift"
    elif cat == "platform":
        topic_slug = "platform_update"

    overlay_label = {
        "security": "SECURITY",
        "policy": "POLICY",
        "ai": "AI",
        "chips": "CHIPS",
        "platform": "PLATFORM",
        "home": "HOME",
        "wellness": "WELLNESS",
        "consumer": "REVIEW",
    }.get(cat, "NEWS")

    if topic_slug == "energy_drinks":
        return StoryProfile(
            category="wellness",
            topic_slug=topic_slug,
            overlay_label=overlay_label,
            scene_hint=(
                "editorial product photo of assorted energy drink cans on a grocery shelf and desk, "
                "chilled aluminum cans, clean highlights, consumer review style, no readable text"
            ),
            search_tags=["wellness", "energy_drink", "beverage", "consumer_review"],
            subject_phrase="energy drink rankings, ingredient tradeoffs, and brand positioning",
            decision_frame=(
                "For shoppers, the real question is not buzz or branding. It is whether the drink changes energy level, crash risk, "
                "ingredient comfort, or price in a way that actually fits an ordinary week."
            ),
            operational_line=(
                "This kind of roundup becomes useful only when it explains caffeine strength, sweetener choice, and value per can "
                "instead of treating every product as the same kind of pick-me-up."
            ),
            scenario_line=(
                "A realistic scenario is a can that looks clean and premium in a ranking, but feels too strong for daily use or too expensive "
                "once you compare caffeine, flavor, and price side by side."
            ),
            watch_line=(
                "The next useful signals are caffeine-label comparisons, price-per-can changes, shopper reaction, and whether reviewers explain "
                "who each drink actually suits."
            ),
            detail_items=[
                "A ranking only helps if it explains who the drink is for, not just who came in first.",
                "Caffeine amount, sweetener choice, and price per serving usually matter more than social buzz.",
                "A workout-focused pick can be a bad fit for commuters, students, or light caffeine users.",
                "Ingredient transparency matters because marketing language often hides how strong a drink really feels.",
            ],
            questions=[
                "What do you actually want: steady focus, workout energy, or the cheapest caffeine?",
                "How much stimulation is too much for an ordinary weekday?",
                "Does the premium can justify the ingredient difference?",
            ],
            comparisons=[
                "A cleaner ingredient list may feel better day to day, but a cheaper can can still win on raw caffeine per dollar.",
                "A sweeter flavor may be easier to drink quickly, but it can also hide just how aggressive the formula feels.",
            ],
            tech_story=False,
        )

    if topic_slug == "air_purifiers":
        return StoryProfile(
            category="home",
            topic_slug=topic_slug,
            overlay_label=overlay_label,
            scene_hint=(
                "editorial home product photo of a premium air purifier running in a sunlit living room, "
                "soft airflow motion, clean interior, consumer review style, no readable text"
            ),
            search_tags=["home", "air_purifier", "indoor_air", "consumer_review"],
            subject_phrase="air purifier rankings, room-size claims, and filter-cost tradeoffs",
            decision_frame=(
                "For home shoppers, the issue is not whether the list looks authoritative. It is whether a recommended unit "
                "actually fits the room, the noise tolerance, and the maintenance budget of a real home."
            ),
            operational_line=(
                "A roundup becomes practical only when it translates airflow claims into bedroom, nursery, or living-room use "
                "and explains how filters, noise, and running cost change the decision."
            ),
            scenario_line=(
                "A realistic scenario is buying a purifier that wins the headline ranking but turns out too loud for a bedroom "
                "or too small for the room that matters most."
            ),
            watch_line=(
                "The next useful signals are updated testing notes, CADR or room-size clarification, long-term owner feedback, "
                "and the real cost of replacement filters."
            ),
            detail_items=[
                "Room-size claims matter only if they match how the purifier will be used in a real home.",
                "Replacement filters, noise, and maintenance cost often matter more than first-look design.",
                "Roundups can flatten differences between bedrooms, living rooms, and allergy-heavy spaces.",
                "A shopping guide is useful only when it explains the tradeoff behind each recommendation.",
            ],
            questions=[
                "Which room or routine would feel the difference first?",
                "Is the quietest model still strong enough for the space you care about?",
                "What ongoing cost shows up after the first month?",
            ],
            comparisons=[
                "Stronger airflow can clean a larger room, but it usually raises noise and filter cost.",
                "A smaller bedroom unit may feel better day to day, even if a larger model wins the spec sheet.",
            ],
            tech_story=False,
        )

    if cat == "consumer":
        return StoryProfile(
            category=cat,
            topic_slug="consumer_reviews",
            overlay_label=overlay_label,
            scene_hint=(
                "editorial shopping photo of tested household products arranged on a clean countertop, "
                "side-by-side comparison, premium magazine style, no readable text"
            ),
            search_tags=["consumer", "product_review", "comparison", "shopping"],
            subject_phrase="product rankings, testing criteria, and shopper tradeoffs",
            decision_frame=(
                "For shoppers, the real issue is whether a recommendation changes value, convenience, maintenance, or fit "
                "for the way they actually live."
            ),
            operational_line=(
                "These stories matter when testing criteria, long-term cost, and daily ease of use matter more than the headline winner."
            ),
            scenario_line=(
                "A realistic scenario is a top-ranked product winning on specs but losing in the one room, routine, or budget limit that matters."
            ),
            watch_line=(
                "The next useful signals are updated testing notes, price movement, warranty terms, and clearer explanations of who each product suits."
            ),
            detail_items=[
                "Testing criteria matter only if readers can map them to real use.",
                "The winning product is not automatically the best value for every household.",
                "Maintenance cost and warranty terms often show up too late in roundup coverage.",
                "The best guide explains fit and tradeoffs, not just the final rank order.",
            ],
            questions=[
                "Which tradeoff would bother you first after the product arrives?",
                "Are you paying for peak performance you will rarely use?",
                "What part of the testing would matter most in your own setup?",
            ],
            comparisons=[
                "The best overall pick may still lose to a simpler model if budget or maintenance matters more.",
                "A product can look stronger on paper yet feel worse once setup, cleaning, or storage enters the picture.",
            ],
            tech_story=False,
        )

    tech_profiles = {
        "security": StoryProfile(
            category="security",
            topic_slug=topic_slug,
            overlay_label=overlay_label,
            scene_hint=(
                "editorial cybersecurity scene with secure login screens, device trust cues, and a modern workspace, "
                "realistic lighting, no readable text"
            ),
            search_tags=["security", "advisory", "patch", "platform"],
            subject_phrase="security advisories, mitigation steps, and patch timing",
            decision_frame="The real issue is exposure: account access, device trust, and how quickly a patch becomes practical.",
            operational_line="Security stories turn practical when a patch, permission change, or safeguard alters day-to-day risk.",
            scenario_line="A realistic scenario is a team delaying an update because no outage is visible, then spending more time on preventable cleanup a day later.",
            watch_line="The next useful signals are confirmed advisories, mitigation guidance, patch cadence, and evidence about whether the issue is spreading.",
            detail_items=[
                "A headline matters only if it changes real risk or real patch timing.",
                "Mitigation steps are often more useful than the first wave of commentary.",
                "Version scope matters because not every device or tenant gets hit the same way.",
                "Small teams need clarity on exposure before they need drama.",
            ],
            questions=[
                "What is the fastest low-risk check you can run today?",
                "Does this change the patch window or only the monitoring plan?",
                "Which account or device group would be exposed first?",
            ],
            comparisons=[
                "Patching immediately lowers exposure, but a controlled pilot can reduce surprise breakage.",
                "A quiet advisory can matter more than a loud headline if the scope is real.",
            ],
            tech_story=True,
        ),
        "policy": StoryProfile(
            category="policy",
            topic_slug=topic_slug,
            overlay_label=overlay_label,
            scene_hint=(
                "editorial policy scene with privacy controls, consent flows, and a clean modern interface, "
                "realistic lighting, no readable text"
            ),
            search_tags=["policy", "privacy", "consent", "platform"],
            subject_phrase="policy changes, defaults, and reader-facing tradeoffs",
            decision_frame="The practical issue is whether defaults, tracking, or access rules quietly change what users can do.",
            operational_line="Policy stories become real when consent flow, access limits, or default settings start to change the interface people actually use.",
            scenario_line="A realistic scenario is a service updating one rule while leaving users unclear about what changed until daily habits start to break.",
            watch_line="The next useful signals are clarification language, enforcement scope, rollout geography, and whether default settings move before users notice.",
            detail_items=[
                "Defaults matter more than headlines when most users never revisit settings.",
                "Clarification language can matter as much as the first announcement.",
                "Enforcement timing is often slower and messier than the first reaction suggests.",
                "The real question is who has to change behavior first.",
            ],
            questions=[
                "What behavior would have to change first for ordinary users?",
                "Is this an immediate rule shift or a staged enforcement move?",
                "Which default matters more than the press release?",
            ],
            comparisons=[
                "A broad policy push gives consistency, while a pilot-first rollout gives safer validation data.",
                "Clearer defaults can reduce confusion, but stricter defaults can also cut flexibility.",
            ],
            tech_story=True,
        ),
        "ai": StoryProfile(
            category="ai",
            topic_slug=topic_slug,
            overlay_label=overlay_label,
            scene_hint=(
                "editorial AI workspace scene with laptops, prompt notes, and soft interface glow, "
                "realistic lighting, no readable text"
            ),
            search_tags=["ai", "tools", "workflow", "platform"],
            subject_phrase="AI tool changes, cost shifts, and workflow tradeoffs",
            decision_frame="The reader-facing question is whether the change improves usefulness or merely moves cost, limits, or reliability around.",
            operational_line="AI stories become real when quality, latency, or price changes what students, workers, and small teams can actually rely on.",
            scenario_line="A realistic scenario is a tool still working on paper but giving weaker results, slower turnaround, or less predictable cost once the update lands.",
            watch_line="The next useful signals are benchmark reality, price movement, user complaints, and competitor response.",
            detail_items=[
                "A model update matters only if it changes output quality, speed, or cost in a measurable way.",
                "Benchmark headlines are weaker than workflow evidence from ordinary use.",
                "Small teams feel reliability swings faster than they feel abstract capability gains.",
                "The most useful explainers connect performance claims to real task fit.",
            ],
            questions=[
                "Should teams update immediately or watch rollout signals first?",
                "Which workflow breaks first if latency or price moves the wrong way?",
                "Is the quality gain obvious enough to justify the new limit or cost?",
            ],
            comparisons=[
                "A faster model can still be a worse fit if quality or limits become less predictable.",
                "A headline feature jump may matter less than a small drop in reliability for everyday use.",
            ],
            tech_story=True,
        ),
        "chips": StoryProfile(
            category="chips",
            topic_slug=topic_slug,
            overlay_label=overlay_label,
            scene_hint=(
                "editorial semiconductor scene with chips, boards, and premium product lighting, no readable text"
            ),
            search_tags=["chips", "gpu", "semiconductor", "market"],
            subject_phrase="chip supply, pricing, and upgrade-timing tradeoffs",
            decision_frame="Chip stories matter when they change upgrade timing, product price, or how long current hardware remains viable.",
            operational_line="These stories turn practical when pricing, availability, and partner product plans start to move together.",
            scenario_line="A realistic scenario is buyers pausing an upgrade because pricing or launch timing suddenly looks less certain than it did last month.",
            watch_line="The next useful signals are pricing movement, roadmap updates, supply commentary, and partner product changes around the announcement.",
            detail_items=[
                "Supply and price matter more than hype if readers are planning a real purchase.",
                "Roadmap updates often signal more than launch-stage marketing.",
                "Partner ecosystem timing can matter as much as the chip itself.",
                "A meaningful shift usually shows up in availability and pricing first.",
            ],
            questions=[
                "Does this change the timing of the next purchase decision?",
                "What would move first: pricing, availability, or partner launches?",
                "Is this a real supply signal or just a headline cycle?",
            ],
            comparisons=[
                "Earlier availability helps buyers, but limited supply can make the first wave less practical than it looks.",
                "A stronger chip may still be a worse buy if the surrounding product stack gets more expensive.",
            ],
            tech_story=True,
        ),
        "platform": StoryProfile(
            category="platform",
            topic_slug=topic_slug,
            overlay_label=overlay_label,
            scene_hint=(
                "editorial product-and-software scene with phone, laptop, and clean interface cues, realistic lighting, no readable text"
            ),
            search_tags=["platform", "update", "software", "workflow"],
            subject_phrase="platform updates, rollout timing, and user-facing tradeoffs",
            decision_frame="The practical issue is whether the update changes daily workflow, trust, or upgrade timing for ordinary readers.",
            operational_line="Platform stories become real when a phone, browser, operating system, or app behaves differently in the middle of a normal day.",
            scenario_line="A realistic scenario is an update that does not break everything at once but slowly degrades one or two habits people rely on every morning.",
            watch_line="The next useful signals are release-note revisions, support acknowledgements, and a clearer pattern of which devices or regions get hit first.",
            detail_items=[
                "A rollout matters only if it changes real behavior, not just feature slides.",
                "Early user reports are directional, not the whole picture.",
                "Version scope and region timing often matter more than the headline.",
                "The best explainers connect the update to routines readers already understand.",
            ],
            questions=[
                "What changes first for regular users this week?",
                "Could this affect background sync, sign-in, or notifications for your setup?",
                "What is the fastest low-risk check you can run today?",
            ],
            comparisons=[
                "Immediate rollout is faster, but staged rollout lowers rollback risk if issues spread.",
                "Waiting a day can delay benefits, but it also reduces disruption for mission-critical workflows.",
            ],
            tech_story=True,
        ),
    }
    return tech_profiles.get(cat, tech_profiles["platform"])
