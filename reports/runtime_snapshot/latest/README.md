# GPT Runtime Map

This folder is a runtime snapshot from %APPDATA%\RezeroAgent for GPT analysis.

## Key Files
- MANIFEST.md: snapshot timestamp, included items, source log sizes
- logs/*.jsonl: execution/QA/image/publish logs (large logs are tail-only)
- db/DATABASE_SUMMARY.md: sqlite table/row summary
- db/*.sqlite: runtime DB copies
- meta/settings.yaml: active runtime settings
- meta/appdata_config/*: AppData config copy (secret/token files redacted)

## Log Purpose
- logs/workflow_perf.jsonl: stage timing and bottlenecks
- logs/qa_timing.jsonl: QA check timing breakdown
- logs/visual_pipeline.jsonl: image generation retries/failures
- logs/publisher_upload.jsonl: Blogger image upload responses
- logs/thumbnail_gate.jsonl: thumbnail preflight gate reasons
- logs/agent_events.jsonl: run-level result events
- logs/ollama_calls.jsonl: local LLM usage/fallback trace
