"""Refreshes the mining-pool signature dataset (config/pools-v2.json) that
btc_parser_app.rpc.mining_pools uses to attribute blocks to pools.

A snapshot ships in config/pools-v2.json (from mempool/mining-pools, MIT
licensed), so the RPC parser works out of the box without ever running
this. Call `refresh_if_stale` periodically (default: weekly, matching the
upstream project's own update cadence, see config.yaml) to pick up newly
launched pools and address rotations.

This intentionally does not share the mempool.space TokenBucket: it talks
to raw.githubusercontent.com, a different host with its own limits, so
counting it against the mempool.space budget would just make that budget
harder to reason about for no benefit.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import requests

from btc_parser_app.api.client import ApiClient, FetchError, RateLimited
from btc_parser_app.api.rate_limiter import TokenBucket
from btc_parser_app.common.atomic_write import atomic_replace
from btc_parser_app.config import MiningPoolsDatasetConfig

logger = logging.getLogger(__name__)

# This module makes at most one request per call, so a generous single-slot
# bucket is really just there to reuse ApiClient's retry/timeout handling.
_DATASET_FETCH_RATE_LIMIT = TokenBucket(requests_per_minute=30, bucket_size=1)


def _validate_pools(pools: Any) -> list[dict[str, Any]]:
    if not isinstance(pools, list) or not pools:
        raise ValueError("expected a non-empty JSON list of pool objects")
    for pool in pools:
        if not isinstance(pool, dict) or "name" not in pool:
            raise ValueError(
                "each pool entry must be an object with at least a 'name' field"
            )
    return pools


def fetch_pools_dataset(config: MiningPoolsDatasetConfig) -> list[dict[str, Any]]:
    """Fetch and validate the pool dataset from config.source_url. Raises on
    any failure - callers decide whether to fall back to the local copy."""
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "btc_parser_app-pools-updater/1.0", "Accept": "application/json"}
    )
    client = ApiClient(
        session=session,
        rate_limiter=_DATASET_FETCH_RATE_LIMIT,
        timeout_seconds=20,
        max_connection_retries=2,
        retry_backoff_seconds=3.0,
    )
    data = client.get_json(config.source_url)
    return _validate_pools(data)


def refresh(config: MiningPoolsDatasetConfig) -> None:
    """Unconditionally fetch and overwrite config.local_path.

    Written atomically (temp file + rename): load_pool_matcher reads this
    file back on every rpc-ingest startup, so a crash mid-write here must
    never leave a truncated/invalid JSON file behind - that would silently
    degrade every subsequent block to "unknown" pool matching.
    """
    pools = fetch_pools_dataset(config)
    atomic_replace(
        config.local_path,
        lambda tmp: tmp.write_text(json.dumps(pools, indent=2), encoding="utf-8"),
    )
    logger.info("Refreshed %s: %d pools", config.local_path, len(pools))


def refresh_if_stale(config: MiningPoolsDatasetConfig) -> bool:
    """Refresh config.local_path if it's missing or older than
    refresh_interval_seconds. Returns True if a refresh happened. Failures
    are logged and swallowed - the RPC parser keeps using whatever is
    already on disk rather than crashing over a transient GitHub outage."""
    if config.local_path.exists():
        age_seconds = time.time() - config.local_path.stat().st_mtime
        if age_seconds < config.refresh_interval_seconds:
            logger.debug(
                "%s is %.0fs old (< %.0fs threshold) - skipping refresh",
                config.local_path,
                age_seconds,
                config.refresh_interval_seconds,
            )
            return False

    try:
        refresh(config)
        return True
    except (FetchError, RateLimited, ValueError) as exc:
        if config.local_path.exists():
            logger.warning(
                "Failed to refresh %s (%s) - keeping existing copy.",
                config.local_path,
                exc,
            )
        else:
            logger.error(
                "Failed to fetch %s and no local copy exists: %s",
                config.local_path,
                exc,
            )
        return False
