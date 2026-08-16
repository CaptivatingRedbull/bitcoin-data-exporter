"""Thread-safe token-bucket rate limiter.

This is what actually enforces `mempool_api.rate_limit` from config.yaml. A
single TokenBucket instance is shared by every caller hitting the same host
budget (the endpoint poller threads and the mining-pool-history paginator),
so the configured requests-per-minute is a budget for the whole app against
that host, not a per-caller allowance.
"""

from __future__ import annotations

import threading
import time


class TokenBucket:
    def __init__(self, requests_per_minute: float, bucket_size: int) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be > 0")
        if bucket_size < 1:
            raise ValueError("bucket_size must be >= 1")

        self._rate_per_second = requests_per_minute / 60.0
        self._capacity = float(bucket_size)
        self._tokens = float(bucket_size)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill_locked(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate_per_second)
            self._last_refill = now

    def acquire(self, stop_event: threading.Event | None = None) -> bool:
        """Block until a token is available, then consume it.

        Returns False if `stop_event` was set while waiting, so a caller can
        abort a request that's no longer wanted (e.g. after a 429 elsewhere)
        instead of firing it once the wait is over. Returns True once a
        token was actually consumed.
        """
        while True:
            with self._lock:
                self._refill_locked()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return True
                wait_seconds = (1 - self._tokens) / self._rate_per_second

            if stop_event is not None:
                if stop_event.wait(timeout=wait_seconds):
                    return False
            else:
                time.sleep(wait_seconds)
