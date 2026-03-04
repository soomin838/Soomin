import logging
from typing import Any
from pathlib import Path

from core.visual import VisualPipeline, ImageAsset
from core.brain import DraftPost

logger = logging.getLogger("agent.media_manager")

class MediaManagerService:
    """
    Manages image generation, duplication checks, and uploads.
    """
    def __init__(self, visual: VisualPipeline):
        self.visual = visual

    def prepare_post_images(self, draft: DraftPost, prompt_plan: dict[str, Any] | None = None) -> list[ImageAsset]:
        """
        Orchestrates the visual pipeline to ensure the post has all required images.
        """
        logger.info(f"Preparing images for draft: {draft.title}")
        
        try:
            # Generate the primary inline images and thumbnail
            images = self.visual.build(draft=draft, prompt_plan=prompt_plan)
            images = self.visual.ensure_generated_thumbnail(draft=draft, images=images, prompt_plan=prompt_plan)
            
            # Ensure unique constraints
            images = self.visual.ensure_unique_assets(images)
            logger.info(f"Successfully prepared {len(images)} images")
            return images
            
        except Exception as e:
            logger.error(f"Failed to prepare images: {e}")
            return []
