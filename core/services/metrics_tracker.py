import logging
import time
from typing import Any

logger = logging.getLogger("agent.metrics")

class MetricsTrackerService:
    """
    Tracks performance, timings, and runtime metrics for posts being generated.
    Abstracted out of the workflow orchestrator.
    """
    def __init__(self):
        self.metrics: dict[str, Any] = {}
        self._start_time = time.time()

    def start_timing(self, event_name: str):
        self.metrics[f"{event_name}_start"] = time.time()
        logger.debug(f"Metrics: Started timing for {event_name}")

    def end_timing(self, event_name: str):
        if f"{event_name}_start" in self.metrics:
            elapsed = time.time() - self.metrics[f"{event_name}_start"]
            self.metrics[f"{event_name}_elapsed_sec"] = elapsed
            logger.debug(f"Metrics: Ended timing for {event_name} ({elapsed:.2f}s)")

    def record_count(self, key: str, value: int):
        self.metrics[key] = value

    def get_summary(self) -> dict[str, Any]:
        self.metrics["total_runtime_sec"] = time.time() - self._start_time
        return self.metrics
