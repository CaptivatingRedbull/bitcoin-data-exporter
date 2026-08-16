"""Rate-limited HTTP GET client shared by every mempool.space caller.

Generalizes the retry/429/timeout handling that mempool_api_parser.py and
mempool_block_pool_history.py each implemented separately. Every request
goes through a TokenBucket first, so the poller, the mining-pool-history
backfill, and any other caller sharing one ApiClient instance all draw from
the same requests-per-minute budget.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

import requests

from btc_parser_app.api.rate_limiter import TokenBucket

logger = logging.getLogger(__name__)


class RateLimited(Exception):
    """Raised when the server returns HTTP 429. Never retried automatically -
    callers are expected to stop the affected loop, per Script_plan.md."""

    def __init__(self, retry_after: str) -> None:
        self.retry_after = retry_after
        super().__init__(f"429 Too Many Requests (Retry-After={retry_after})")


class FetchError(Exception):
    """Raised for anything else that keeps a request from producing JSON:
    connection failures after retries, a non-200/429 status, bad JSON."""


@dataclass
class ApiClient:
    session: requests.Session
    rate_limiter: TokenBucket
    timeout_seconds: float
    max_connection_retries: int
    retry_backoff_seconds: float
    stop_event: threading.Event | None = None

    def get_json(self, url: str) -> Any:
        """GET url and return decoded JSON.

        Raises RateLimited on HTTP 429 and FetchError for anything else
        that isn't a clean 200 + valid JSON body. Blocks on the shared rate
        limiter before every attempt, including retries.
        """
        response = self._get_with_retry(url)
        if response.status_code == 429:
            raise RateLimited(response.headers.get("Retry-After", "unknown"))
        if response.status_code != 200:
            raise FetchError(
                f"{url}: unexpected status {response.status_code}: {response.text[:200]!r}"
            )
        try:
            return response.json()
        except ValueError as exc:
            raise FetchError(f"{url}: failed to decode JSON: {exc}") from exc

    def _get_with_retry(self, url: str) -> requests.Response:
        last_exc: Exception | None = None
        for attempt in range(self.max_connection_retries + 1):
            if not self.rate_limiter.acquire(self.stop_event):
                raise FetchError(
                    f"{url}: aborted while waiting for rate limit (stop requested)"
                )

            try:
                return self.session.get(url, timeout=self.timeout_seconds)
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                if attempt < self.max_connection_retries:
                    logger.warning(
                        "%s: request failed (%s); retrying in %.0fs (attempt %d/%d)",
                        url,
                        exc,
                        self.retry_backoff_seconds,
                        attempt + 1,
                        self.max_connection_retries,
                    )
                    if self.stop_event is not None and self.stop_event.wait(
                        timeout=self.retry_backoff_seconds
                    ):
                        raise FetchError(
                            f"{url}: aborted during retry backoff (stop requested)"
                        ) from exc
                    elif self.stop_event is None:
                        time.sleep(self.retry_backoff_seconds)

        raise FetchError(
            f"{url}: request failed after {self.max_connection_retries + 1} attempt(s): {last_exc} "
            "(connection-level failure - check network/VPN connectivity to this host)"
        )
