# Architecture Q&A (Self-Review)

1. Question: Which sources fit commercialization with lower policy risk?
Answer: Stack Exchange API, Hacker News API, and GitHub API were selected; Reddit default path removed.

2. Question: How do we keep spending at zero?
Answer: Budget guard blocks execution when daily post or Gemini-call limits are reached.

3. Question: How do we reduce API calls while keeping quality?
Answer: One ranking call + one writing call per run (`max_calls_per_run=2`).

4. Question: What if generation fails after one API call?
Answer: Gemini usage is persisted even on failure paths in workflow exception handling.

5. Question: Is packaging reproducible for Windows?
Answer: `scripts/build_windows.ps1` builds `dist/RezeroAgent.exe`; optional installer via Inno Setup script.

6. Question: Can macOS work be restored later?
Answer: Legacy macOS path is intentionally left commented in `main.py` for future reactivation.
