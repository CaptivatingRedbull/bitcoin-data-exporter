"""Plain HTTP GET with a few retries on connection-level failures.

Used for the two one-shot GitHub downloads this app makes: the mining-pool
signature dataset (rpc/mining_pools_dataset.py) and the bitcoin-data/
stale-blocks CSV (rpc/stale_blocks_github.py).
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)


class FetchError(Exception):
    """Anything that keeps a request from producing a usable body:
    connection failures after retries, a non-200 status, bad JSON."""


def get_text(
    url: str,
    *,
    headers: dict[str, str],
    timeout_seconds: float,
    max_retries: int = 2,
    retry_backoff_seconds: float = 3.0,
) -> str:
    """GET url and return the body as text. Raises FetchError on failure."""
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout_seconds)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt < max_retries:
                logger.warning(
                    "%s: request failed (%s); retrying in %.0fs (attempt %d/%d)",
                    url,
                    exc,
                    retry_backoff_seconds,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(retry_backoff_seconds)
            continue
        if response.status_code != 200:
            raise FetchError(
                f"{url}: unexpected status {response.status_code}: {response.text[:200]!r}"
            )
        return response.text

    raise FetchError(
        f"{url}: request failed after {max_retries + 1} attempt(s): {last_exc} "
        "(connection-level failure - check network connectivity to this host)"
    )


def get_json(
    url: str,
    *,
    headers: dict[str, str],
    timeout_seconds: float,
    max_retries: int = 2,
    retry_backoff_seconds: float = 3.0,
) -> Any:
    """GET url and return the decoded JSON body. Raises FetchError on failure."""
    text = get_text(
        url,
        headers=headers,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )
    try:
        return json.loads(text)
    except ValueError as exc:
        raise FetchError(f"{url}: failed to decode JSON: {exc}") from exc
