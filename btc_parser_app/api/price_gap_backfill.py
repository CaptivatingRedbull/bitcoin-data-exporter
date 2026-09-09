"""Standalone, manually-run backfill for the gap between where the one-time
Kraken CSV history stops and where the live `api-poll` (forwarded to Cribl)
starts covering `prices.csv`. Unlike everything else in this app, this
script takes its parameters on the command line instead of from config.yaml
- it isn't a long-running service and has nothing in common with the
mempool_api endpoint config (no threads, no shared token bucket, a
different host path).

Walks mempool.space's `/api/v1/historical-price?currency=<c>&timestamp=<t>`
endpoint one minute at a time from --start-timestamp to --end-timestamp
(inclusive), writing into the same `<export-dir>/prices.csv` (schema
date_unix,usd,eur) that the Kraken import and the live poller both use -
see btc_parser_app/api/mempool_endpoints.py::parse_prices. The endpoint
snaps a requested timestamp to whatever price point mempool.space actually
has nearest it (exact granularity varies with age), so the row actually
written is keyed by the response's own `time`, not the requested timestamp
- consecutive requested minutes can therefore resolve to the same
already-written row, which is silently skipped rather than duplicated.

Rate limiting is deliberately dumb - a flat `time.sleep()` between requests,
no token bucket - since this is a short, manually-babysat one-off, not a
long-running shared-budget service. A 429 response is never retried: it's
logged and the process exits immediately (EXIT_RATE_LIMITED, matching
api/poller.py's convention), leaving its progress checkpoint exactly where
it stopped.

Restart-safe by design, since a multi-hour run over a slow rate limit will
get Ctrl-C'd or crash sometimes: progress is checkpointed to
<export-dir>/price_gap_backfill_state.csv after every written/skipped
minute. Re-running with the same --start-timestamp resumes from that
checkpoint instead of re-walking from the beginning; a different
--start-timestamp is treated as a new backfill and resets it.

    python backfill_price_gap.py --start-timestamp 1690000000 \
        --end-timestamp 1690003600 --export-dir parser-data/export/api
"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import sys
import time
from pathlib import Path
from typing import Any

import requests

from btc_parser_app.api.client import FetchError, RateLimited
from btc_parser_app.common.csv_writer import (
    csv_parts_exist,
    read_csv_parts,
    read_single_row_csv,
    write_rows_to_csv,
    write_single_row_csv,
)

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://mempool.space"
DEFAULT_CURRENCY = "EUR"  # see module docstring on currency= - EUR responses
# have included both EUR and USD in practice, USD-only requests have not.
DEFAULT_RATE_LIMIT_PER_MINUTE = 10.0
STEP_SECONDS = 60  # matches prices.csv's one-row-per-minute schema

# Mirrors api/poller.py::EXIT_RATE_LIMITED - same sysexits.h EX_TEMPFAIL
# rationale: distinguishable from a generic crash (exit 1) if this is ever
# wrapped by something that restarts on failure.
EXIT_RATE_LIMITED = 75


def _prices_path(export_dir: Path) -> Path:
    return export_dir / "prices.csv"


def _state_path(export_dir: Path) -> Path:
    return export_dir / "price_gap_backfill_state.csv"


def _existing_dates(prices_path: Path) -> set[int]:
    """Already-written date_unix values in prices.csv, across all rotated
    parts - so a minute the live poller or a previous backfill run already
    wrote is never duplicated."""
    if not csv_parts_exist(prices_path):
        return set()
    frame = read_csv_parts(prices_path, columns=["date_unix"])
    if frame.is_empty():
        return set()
    return {int(v) for v in frame["date_unix"].to_list()}


def _load_checkpoint(state_path: Path, start_timestamp: int) -> int:
    """Returns the requested-timestamp to resume from. Only trusts a saved
    checkpoint if it was made for the same --start-timestamp - a different
    start means a different backfill, not a resume, so it's ignored (and
    will be overwritten by the next checkpoint write)."""
    row = read_single_row_csv(state_path)
    if row is None:
        return start_timestamp
    if int(row["range_start"]) != start_timestamp:
        logger.warning(
            "%s: found a checkpoint for a different --start-timestamp (%s) - "
            "starting this run fresh from %s instead of resuming.",
            state_path,
            row["range_start"],
            start_timestamp,
        )
        return start_timestamp
    return int(row["next_timestamp"])


def _save_checkpoint(state_path: Path, start_timestamp: int, next_timestamp: int) -> None:
    write_single_row_csv(
        state_path, {"range_start": start_timestamp, "next_timestamp": next_timestamp}
    )


def fetch_historical_price(
    session: requests.Session,
    base_url: str,
    currency: str,
    timestamp: int,
    timeout_seconds: float,
) -> Any:
    """GET historical-price for one timestamp and return decoded JSON.

    Raises RateLimited on HTTP 429, FetchError for anything else that isn't
    a clean 200 + valid JSON - reusing api/client.py's exception types so a
    429 here means the same thing it does everywhere else in this app, but
    with no retry/token-bucket machinery wrapped around it (see module
    docstring - this is a deliberately dumb, manually-run script)."""
    url = f"{base_url}/api/v1/historical-price?currency={currency}&timestamp={timestamp}"
    response = session.get(url, timeout=timeout_seconds)
    if response.status_code == 429:
        raise RateLimited(response.headers.get("Retry-After", "unknown"))
    if response.status_code != 200:
        raise FetchError(f"{url}: unexpected status {response.status_code}: {response.text[:200]!r}")
    try:
        return response.json()
    except ValueError as exc:
        raise FetchError(f"{url}: failed to decode JSON: {exc}") from exc


def run_backfill(
    start_timestamp: int,
    end_timestamp: int,
    export_dir: Path,
    rate_limit_per_minute: float = DEFAULT_RATE_LIMIT_PER_MINUTE,
    base_url: str = DEFAULT_BASE_URL,
    currency: str = DEFAULT_CURRENCY,
    request_timeout_seconds: float = 20.0,
) -> int:
    """Walks [start_timestamp, end_timestamp] in STEP_SECONDS increments,
    writing new minutes into export_dir/prices.csv. Returns a process exit
    code: 0 on completing the range (or finding nothing left to do),
    EXIT_RATE_LIMITED on a 429, 1 on any other fetch failure - in both
    non-zero cases the checkpoint is left at the last *successful* step, so
    re-running the same command retries the request that failed instead of
    skipping past it.
    """
    if end_timestamp < start_timestamp:
        logger.error(
            "--end-timestamp (%d) must be >= --start-timestamp (%d).",
            end_timestamp,
            start_timestamp,
        )
        return 1

    export_dir.mkdir(parents=True, exist_ok=True)
    prices_path = _prices_path(export_dir)
    state_path = _state_path(export_dir)

    next_timestamp = _load_checkpoint(state_path, start_timestamp)
    if next_timestamp > end_timestamp:
        logger.info(
            "Nothing to do: checkpoint (%d) is already past --end-timestamp (%d).",
            next_timestamp,
            end_timestamp,
        )
        return 0

    existing_dates = _existing_dates(prices_path)
    sleep_seconds = 60.0 / rate_limit_per_minute

    logger.info(
        "Backfilling %s minute-by-minute from %d to %d into %s (currency=%s, %.1f req/min).",
        base_url,
        next_timestamp,
        end_timestamp,
        prices_path,
        currency,
        rate_limit_per_minute,
    )

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    written = 0
    while next_timestamp <= end_timestamp:
        try:
            data = fetch_historical_price(
                session, base_url, currency, next_timestamp, request_timeout_seconds
            )
        except RateLimited as exc:
            logger.error(
                "Rate limited by %s at timestamp %d: %s - stopping "
                "(checkpoint left at %d, re-run the same command to resume).",
                base_url,
                next_timestamp,
                exc,
                next_timestamp,
            )
            return EXIT_RATE_LIMITED
        except FetchError as exc:
            logger.error(
                "Fetch failed at timestamp %d: %s - stopping (checkpoint left "
                "at %d, re-run the same command to resume).",
                next_timestamp,
                exc,
                next_timestamp,
            )
            return 1

        prices = data.get("prices") or []
        if not prices:
            logger.warning("No price data returned for timestamp %d - skipping.", next_timestamp)
        else:
            entry = prices[0]
            date_unix = entry.get("time")
            if date_unix is not None and int(date_unix) not in existing_dates:
                date_unix = int(date_unix)
                write_rows_to_csv(
                    [{"date_unix": date_unix, "usd": entry.get("USD"), "eur": entry.get("EUR")}],
                    prices_path,
                )
                existing_dates.add(date_unix)
                written += 1

        next_timestamp += STEP_SECONDS
        _save_checkpoint(state_path, start_timestamp, next_timestamp)

        if next_timestamp <= end_timestamp:
            time.sleep(sleep_seconds)

    logger.info("Backfill complete: wrote %d new minute(s) to %s.", written, prices_path)
    return 0


def _configure_logging(export_dir: Path) -> None:
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z"
    )
    export_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        export_dir / "price_gap_backfill.log", maxBytes=20 * 1024 * 1024, backupCount=5,
        encoding="utf-8",
    )
    handlers: list[logging.Handler] = [logging.StreamHandler(), file_handler]
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill_price_gap.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--start-timestamp", type=int, required=True,
        help="Unix timestamp to start backfilling from (inclusive) - typically the last "
        "date_unix already covered by the one-time Kraken CSV import.",
    )
    parser.add_argument(
        "--end-timestamp", type=int, required=True,
        help="Unix timestamp to backfill up to (inclusive) - typically the date_unix of "
        "the first minute captured by the live api-poll/Cribl forwarding.",
    )
    parser.add_argument(
        "--export-dir", type=Path, required=True,
        help="Directory for prices.csv and the resume checkpoint file - point this at the "
        "same mempool_api.output_dir the rest of the app uses.",
    )
    parser.add_argument(
        "--rate-limit-per-minute", type=float, default=DEFAULT_RATE_LIMIT_PER_MINUTE,
        help=f"Requests per minute against mempool.space, enforced with a flat sleep between "
        f"requests (no burst allowance). Default: {DEFAULT_RATE_LIMIT_PER_MINUTE}.",
    )
    parser.add_argument(
        "--currency", default=DEFAULT_CURRENCY,
        help=f"currency= query parameter. Default: {DEFAULT_CURRENCY!r}.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _configure_logging(args.export_dir)

    if args.rate_limit_per_minute <= 0:
        print("--rate-limit-per-minute must be > 0", file=sys.stderr)
        return 2

    return run_backfill(
        start_timestamp=args.start_timestamp,
        end_timestamp=args.end_timestamp,
        export_dir=args.export_dir,
        rate_limit_per_minute=args.rate_limit_per_minute,
        base_url=args.base_url,
        currency=args.currency,
    )


if __name__ == "__main__":
    raise SystemExit(main())
