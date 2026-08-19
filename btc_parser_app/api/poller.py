"""Threaded interval poller for the mempool.space endpoints in config.yaml.

Ported from mempool_api_parser.py, generalized to build its endpoint list
and rate limit from AppConfig instead of module-level constants. One thread
per endpoint, each on its own interval; a 429 on any endpoint sets a shared
stop_event so every other endpoint's thread wakes and halts immediately
instead of finishing out its current sleep.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

import requests

from btc_parser_app.api.client import ApiClient, FetchError, RateLimited, handle_rate_limited
from btc_parser_app.api.mempool_endpoints import PARSER_REGISTRY, PolledAt
from btc_parser_app.api.rate_limiter import TokenBucket
from btc_parser_app.common.csv_writer import write_rows_to_csv
from btc_parser_app.config import EndpointConfig, MempoolApiConfig

logger = logging.getLogger(__name__)


def compute_start_offsets(endpoints: tuple[EndpointConfig, ...]) -> dict[str, float]:
    """Endpoints sharing the same interval get their request *starts* spread
    evenly across that interval (e.g. 4 endpoints on a 60s interval start
    15s apart). Endpoints on their own interval (like the 24h mining pool
    poll) just start near the top of the run - there's nothing to stagger
    against at that timescale."""
    by_interval: dict[float, list[EndpointConfig]] = {}
    for endpoint in endpoints:
        by_interval.setdefault(endpoint.interval_seconds, []).append(endpoint)

    offsets: dict[str, float] = {}
    for interval, group in by_interval.items():
        stagger = interval / len(group)
        for i, endpoint in enumerate(group):
            offsets[endpoint.name] = i * stagger
    return offsets


def fetch_and_write(
    endpoint: EndpointConfig,
    base_url: str,
    client: ApiClient,
    out_dir: Path,
    stop_event: threading.Event,
    rate_limited_event: threading.Event,
) -> None:
    if stop_event.is_set():
        return

    parser = PARSER_REGISTRY.get(endpoint.parser)
    if parser is None:
        logger.error(
            "[%s] no parser registered for '%s' - check config.yaml's endpoints[].parser",
            endpoint.name,
            endpoint.parser,
        )
        return

    url = base_url + endpoint.path
    try:
        data = client.get_json(url)
    except RateLimited as exc:
        handle_rate_limited(exc, f"[{endpoint.name}]", rate_limited_event, stop_event)
        return
    except FetchError as exc:
        logger.warning("[%s] %s", endpoint.name, exc)
        return

    polled_at = PolledAt.now()
    try:
        rows = parser(data, polled_at)
    except Exception as exc:  # noqa: BLE001 - keep the poller alive on bad payloads
        logger.warning("[%s] failed to parse response: %s", endpoint.name, exc)
        return

    write_rows_to_csv(rows, out_dir / f"{endpoint.name}.csv")
    # DEBUG, not INFO: this fires once per endpoint per interval_seconds (5
    # endpoints => a steady trickle of lines that drowns out the
    # actually-interesting INFO lines - startup summary, warnings, a 429).
    # Set logging.level: DEBUG in config.yaml to see these again.
    logger.debug(
        "[%s] wrote %d row(s) @ %s", endpoint.name, len(rows), polled_at.utc_iso
    )


def endpoint_loop(
    endpoint: EndpointConfig,
    start_offset: float,
    base_url: str,
    client: ApiClient,
    out_dir: Path,
    stop_event: threading.Event,
    rate_limited_event: threading.Event,
) -> None:
    """Run one endpoint forever on its own interval. stop_event.wait() is
    used instead of time.sleep() throughout so a 429 on any endpoint wakes
    every other endpoint's thread immediately instead of it sleeping out its
    delay."""
    if stop_event.wait(timeout=start_offset):
        return

    next_due = time.monotonic()
    while not stop_event.is_set():
        fetch_and_write(endpoint, base_url, client, out_dir, stop_event, rate_limited_event)
        if stop_event.is_set():
            return

        next_due += endpoint.interval_seconds
        delay = next_due - time.monotonic()
        if delay < 0:
            # Fell behind schedule (slow response) - resync instead of
            # firing a burst of catch-up requests.
            next_due = time.monotonic()
            delay = 0

        if stop_event.wait(timeout=delay):
            return


def run_poller(
    config: MempoolApiConfig,
    stop_event: threading.Event | None = None,
) -> int:
    """Start one thread per endpoint and block until stop_event is set (by a
    429, or by the caller). Returns a process exit code: 0 for a clean
    caller-requested stop, 1 if a 429 halted the poller.

    stop_event may be caller-owned (an externally constructed Event the
    caller sets for its own shutdown reasons) or the internal default. Either
    way, a separate internal rate_limited_event tracks whether a 429 was the
    actual reason the loop stopped, so a caller-triggered stop is never
    misreported as "rate limited" with exit code 1.
    """
    stop_event = stop_event or threading.Event()
    rate_limited_event = threading.Event()
    config.output_dir.mkdir(parents=True, exist_ok=True)

    offsets = compute_start_offsets(config.endpoints)

    logger.info(
        "Polling %d mempool.space endpoints, each on its own interval:",
        len(config.endpoints),
    )
    for e in config.endpoints:
        logger.info(
            "  - %-24s every %7.0fs (start offset %5.1fs)  %s%s",
            e.name,
            e.interval_seconds,
            offsets[e.name],
            config.base_url,
            e.path,
        )
    logger.info("Output dir: %s", config.output_dir.resolve())
    logger.info(
        "Shared budget: %.1f req/min (bucket size %d). Stops on HTTP 429 from any endpoint.",
        config.rate_limit.requests_per_minute,
        config.rate_limit.bucket_size,
    )

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    rate_limiter = TokenBucket(
        config.rate_limit.requests_per_minute, config.rate_limit.bucket_size
    )
    client = ApiClient(
        session=session,
        rate_limiter=rate_limiter,
        timeout_seconds=config.request_timeout_seconds,
        max_connection_retries=config.max_connection_retries,
        retry_backoff_seconds=config.retry_backoff_seconds,
        stop_event=stop_event,
    )

    threads = [
        threading.Thread(
            target=endpoint_loop,
            args=(
                endpoint,
                offsets[endpoint.name],
                config.base_url,
                client,
                config.output_dir,
                stop_event,
                rate_limited_event,
            ),
            name=f"poll-{endpoint.name}",
            daemon=True,
        )
        for endpoint in config.endpoints
    ]

    for t in threads:
        t.start()

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
        stop_event.set()
        # Daemon threads are torn down (not waited on) at interpreter exit,
        # so without joining here a thread mid fetch_and_write - an in-flight
        # HTTP request or a buffered CSV write - would simply be abandoned
        # instead of finishing, on both this path and the one below.
        for t in threads:
            t.join(timeout=config.request_timeout_seconds)
        return 0

    for t in threads:
        t.join(timeout=config.request_timeout_seconds)

    if rate_limited_event.is_set():
        logger.warning("Stopped: received HTTP 429 (rate limited) from mempool.space.")
        return 1

    logger.info("Stopped: stop_event was set externally.")
    return 0
