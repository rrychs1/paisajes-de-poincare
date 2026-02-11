from __future__ import annotations

import time
from collections import defaultdict
from typing import Any, Dict


class Metrics:
    def __init__(self, log_interval_seconds: float = 60.0) -> None:
        self.log_interval_seconds = log_interval_seconds
        self._last_log = time.monotonic()
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._sums: Dict[str, float] = defaultdict(float)
        self._counts: Dict[str, int] = defaultdict(int)

    def inc(self, key: str, value: int = 1) -> None:
        self._counters[key] += value

    def set(self, key: str, value: float) -> None:
        self._gauges[key] = value

    def observe(self, key: str, value: float) -> None:
        self._sums[key] += value
        self._counts[key] += 1

    def should_log(self) -> bool:
        return (time.monotonic() - self._last_log) >= self.log_interval_seconds

    def snapshot(self) -> Dict[str, float]:
        snapshot: Dict[str, float] = {}
        snapshot.update({k: float(v) for k, v in self._counters.items()})
        snapshot.update({k: float(v) for k, v in self._gauges.items()})
        for key, total in self._sums.items():
            count = self._counts.get(key, 0)
            if count:
                snapshot[f"{key}_avg"] = total / count
        return snapshot

    def reset(self) -> None:
        self._counters.clear()
        self._sums.clear()
        self._counts.clear()

    async def log(self, logger, db: Any = None) -> None:
        if not self.should_log():
            return
        snapshot = self.snapshot()
        if snapshot:
            parts = [f"{k}={snapshot[k]:.6g}" for k in sorted(snapshot)]
            logger.info("Metrics %s", " ".join(parts))
            if db is not None and hasattr(db, "save_metrics"):
                try:
                    db_payload = {"ts": time.time(), "metrics": snapshot}
                    await db.save_metrics(db_payload)
                except Exception:
                    logger.debug("Failed to persist metrics snapshot", exc_info=True)
        self._last_log = time.monotonic()
        self.reset()
