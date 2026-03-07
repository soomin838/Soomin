from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any

from re_core.scout import TopicCandidate, SourceScout
from re_core.brain import GeminiBrain, DraftPost

logger = logging.getLogger("agent.content_pipeline")

class ContentPipelineService:
    """
    Handles the assembly of content: from scouting external topics to utilizing the brain
    to generate the HTML draft.
    """
    def __init__(self, scout: SourceScout, brain: GeminiBrain):
        self.scout = scout
        self.brain = brain

    def generate_draft(self, candidate: TopicCandidate, pattern_instruction: str, reference_guidance: str, 
                       recent_urls: list[str] | None = None, domain: str = "tech_troubleshoot") -> DraftPost | None:
        """
        Coordinates the brain to generate a full blog draft from a given candidate.
        """
        try:
            logger.info(f"Generating draft for candidate: {candidate.title}")
            
            # Use authority links from candidate meta if available
            authority_links = candidate.meta.get("external_links", [])
            if not isinstance(authority_links, list):
                authority_links = []
                
            draft = self.brain.generate_post(
                candidate=candidate,
                authority_links=authority_links,
                pattern_instruction=pattern_instruction,
                reference_guidance=reference_guidance,
                domain=domain,
            )
            return draft
            
        except Exception as e:
            logger.error(f"Failed to generate draft for {candidate.title}: {e}")
            return None

    def refine_headline(self, generated_summary: str, current_title: str, trending_keywords: list[str] | None = None) -> str:
        """
        Optimizes the headline using the LLM for better CTR.
        """
        try:
            return self.brain.optimize_headline_ctr(
                summary=generated_summary,
                trending_keywords=trending_keywords,
                current_title=current_title
            )
        except Exception as e:
            logger.warning(f"Headline optimization failed: {e}. Falling back to default.")
            return current_title
