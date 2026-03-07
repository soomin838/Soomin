from __future__ import annotations

import inspect
import unittest

from re_core.workflow import AgentWorkflow


class WorkflowNewsPipelineTests(unittest.TestCase):
    def test_news_pipeline_stage_order_is_present_in_source(self) -> None:
        source = inspect.getsource(AgentWorkflow._run_once_news_mode_impl)
        intent_idx = source.find("_build_search_intent_bundle")
        outline_idx = source.find("_pick_outline_plan")
        draft_idx = source.find("generate_post_from_outline")
        qa_idx = source.find("_qa_evaluate")
        self.assertGreaterEqual(intent_idx, 0)
        self.assertGreaterEqual(outline_idx, 0)
        self.assertGreaterEqual(draft_idx, 0)
        self.assertGreaterEqual(qa_idx, 0)
        self.assertLess(intent_idx, outline_idx)
        self.assertLess(outline_idx, draft_idx)
        self.assertLess(draft_idx, qa_idx)


if __name__ == "__main__":
    unittest.main()
