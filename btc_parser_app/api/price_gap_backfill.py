"""Standalone, manually-run backfill for the gap between where the one-time
Kraken CSV history stops and where the live `api-poll` (forwarded to Cribl)
starts covering `prices.csv`. The Kraken import itself stays the starting
point: --start-timestamp is typically the last date_unix it covered. Unlike
everything else in this app, this script takes its parameters on the
command line instead of from config.yaml - it isn't a long-running service
and has nothing in common with the mempool_api endpoint config (no threads,
no shared token bucket, a different host path).

Walks a mempool instance's
`/api/v1/historical-price?currency=<c>&timestamp=<t>` endpoint one minute at
a time from --start-timestamp to --end-timestamp (inclusive), writing into
the same `<export-dir>/prices.csv` (schema date_unix,usd,eur) that the
Kraken import and the live poller both use - see
btc_parser_app/api/mempool_endpoints.py::parse_prices. The endpoint snaps a
requested timestamp to whatever price point it actually has nearest it
(exact granularity varies with age), so the row actually written is keyed
by the response's own `time`, not the requested timestamp - consecutive
requested minutes can therefore resolve to the same already-written row,
which is silently skipped rather than duplicated.

Data source and validation: by default the data comes from the self-hosted
mempool node (DEFAULT_BASE_URL, --rate-limit-per-second, default 1000).
Every --validate-every'th datapoint (default 1 in 1000) is also requested
from the official mempool.space (--validation-base-url), never faster than
VALIDATION_MIN_INTERVAL_SECONDS (1 req/s) - a due validation that would
come too soon is just deferred to the next datapoint. Each comparison is
logged with the USD/EUR difference in percent; if either differs by more
than --validation-warn-percent the line is logged as a WARNING containing
VALIDATION_WARNING_TAG, so `grep PRICE_VALIDATION_WARNING` on the log finds
every suspicious datapoint. Validation is advisory only: it never changes
what gets written, and a failed/rate-limited validation request
(VALIDATION_FAILED_TAG) never stops the backfill.

Rate limiting is deliberately dumb - fixed-interval pacing (the next request
is not sent before 1/rate seconds after the previous one started), no token
bucket - since this is a short, manually-babysat one-off, not a
long-running shared-budget service. A 429 from the data source is never
retried: it's logged and the process exits immediately (EXIT_RATE_LIMITED,
matching api/poller.py's convention), leaving its progress checkpoint at
the request that failed.

Restart-safe by design, since a long run will get Ctrl-C'd or crash
sometimes: progress is checkpointed to
<export-dir>/price_gap_backfill_state.csv. To keep up with ~1000 req/s,
rows and the checkpoint are flushed together in batches (every
FLUSH_EVERY_STEPS minutes or FLUSH_EVERY_SECONDS, whichever comes first)
and on every exit path, including Ctrl-C and fetch errors. Re-running with
the same --start-timestamp resumes from that checkpoint instead of
re-walking from the beginning; a different --start-timestamp is treated as
a new backfill and resets it. A hard kill (SIGKILL, power loss) can lose at
most the last unflushed batch - re-running re-fetches it, and the
date_unix dedupe keeps that from duplicating anything already written.

    python backfill_price_gap.py --start-timestamp 1690000000 \
        --end-timestamp 1690003600 --export-dir parser-data/export/api
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Any

import requests

from btc_parser_app.api.client import FetchError, RateLimited
from btc_parser_app.api.poller import EXIT_RATE_LIMITED
from btc_parser_app.common.csv_writer import (
    csv_parts_exist,
    read_csv_parts,
    read_single_row_csv,
    write_rows_to_csv,
    write_single_row_csv,
)
from btc_parser_app.common.logging_setup import configure_logging
from btc_parser_app.config import LoggingConfig

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://mempool.home.captivatingredbull.org"
DEFAULT_VALIDATION_BASE_URL = "https://mempool.space"
# EUR responses have included both EUR and USD in practice, USD-only
# requests have not.
DEFAULT_CURRENCY = "EUR"
DEFAULT_RATE_LIMIT_PER_SECOND = 1000.0
DEFAULT_VALIDATE_EVERY = 1000
# Both instances serve the same historical-price data, so a matching
# minute should be (near-)identical; 2% leaves room for the two snapping a
# requested timestamp to slightly different points while still catching a
# genuinely wrong row that a 5% threshold would let through on a calm day.
DEFAULT_VALIDATION_WARN_PERCENT = 2.0
VALIDATION_MIN_INTERVAL_SECONDS = 1.0  # official mempool.space: 1 req/s
VALIDATION_RATE_LIMITED_BACKOFF_SECONDS = 60.0
VALIDATION_WARNING_TAG = "PRICE_VALIDATION_WARNING"
VALIDATION_FAILED_TAG = "PRICE_VALIDATION_FAILED"
STEP_SECONDS = 60  # matches prices.csv's one-row-per-minute schema
FLUSH_EVERY_STEPS = 1000
FLUSH_EVERY_SECONDS = 5.0
PROGRESS_LOG_EVERY_SECONDS = 30.0


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
    a clean 200 + valid JSON (including connection errors/timeouts) -
    reusing api/client.py's exception types so a 429 here means the same
    thing it does everywhere else in this app, but with no retry/token-
    bucket machinery wrapped around it (see module docstring - this is a
    deliberately dumb, manually-run script)."""
    url = f"{base_url}/api/v1/historical-price?currency={currency}&timestamp={timestamp}"
    try:
        response = session.get(url, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise FetchError(f"{url}: request failed: {exc}") from exc
    if response.status_code == 429:
        raise RateLimited(response.headers.get("Retry-After", "unknown"))
    if response.status_code != 200:
        raise FetchError(f"{url}: unexpected status {response.status_code}: {response.text[:200]!r}")
    try:
        return response.json()
    except ValueError as exc:
        raise FetchError(f"{url}: failed to decode JSON: {exc}") from exc


def _first_price(data: Any) -> dict[str, Any] | None:
    prices = data.get("prices") if isinstance(data, dict) else None
    return prices[0] if prices else None


def _percent_diff(value: Any, reference: Any) -> float | None:
    """(value - reference) / reference in percent, or None if either side
    is missing/zero and there's nothing meaningful to compare."""
    try:
        value_f, reference_f = float(value), float(reference)
    except (TypeError, ValueError):
        return None
    if reference_f == 0:
        return None
    return (value_f - reference_f) / reference_f * 100.0


def _format_diff(diff: float | None) -> str:
    return "n/a" if diff is None else f"{diff:+.3f}%"


class _Validator:
    """Spot-checks every Nth datapoint against the official mempool.space,
    paced to at most one request per VALIDATION_MIN_INTERVAL_SECONDS. Only
    logs - it never raises and never influences what gets written."""

    def __init__(
        self,
        session: requests.Session,
        base_url: str,
        currency: str,
        every: int,
        warn_percent: float,
        timeout_seconds: float,
    ) -> None:
        self.session = session
        self.base_url = base_url
        self.currency = currency
        self.every = every
        self.warn_percent = warn_percent
        self.timeout_seconds = timeout_seconds
        self.datapoints_seen = 0
        self.pending = False
        self.not_before = 0.0
        self.checked = 0
        self.warnings = 0
        self.failed = 0

    def observe(self, timestamp: int, entry: dict[str, Any]) -> None:
        if self.every <= 0:
            return
        # Validate the very first datapoint too, so even a short run gets
        # at least one check.
        if self.datapoints_seen % self.every == 0:
            self.pending = True
        self.datapoints_seen += 1
        if self.pending and time.monotonic() >= self.not_before:
            self.pending = False
            self._validate(timestamp, entry)

    def _validate(self, timestamp: int, entry: dict[str, Any]) -> None:
        self.not_before = time.monotonic() + VALIDATION_MIN_INTERVAL_SECONDS
        try:
            data = fetch_historical_price(
                self.session, self.base_url, self.currency, timestamp, self.timeout_seconds
            )
        except RateLimited as exc:
            self.failed += 1
            self.not_before = time.monotonic() + VALIDATION_RATE_LIMITED_BACKOFF_SECONDS
            logger.warning(
                "%s: rate limited by %s validating timestamp %d (Retry-After %s) - "
                "pausing validation for %.0fs, backfill continues.",
                VALIDATION_FAILED_TAG,
                self.base_url,
                timestamp,
                exc,
                VALIDATION_RATE_LIMITED_BACKOFF_SECONDS,
            )
            return
        except FetchError as exc:
            self.failed += 1
            logger.warning(
                "%s: could not validate timestamp %d: %s - backfill continues.",
                VALIDATION_FAILED_TAG,
                timestamp,
                exc,
            )
            return

        reference = _first_price(data)
        if reference is None:
            self.failed += 1
            logger.warning(
                "%s: %s returned no price data for timestamp %d - nothing to compare.",
                VALIDATION_FAILED_TAG,
                self.base_url,
                timestamp,
            )
            return

        self.checked += 1
        usd_diff = _percent_diff(entry.get("USD"), reference.get("USD"))
        eur_diff = _percent_diff(entry.get("EUR"), reference.get("EUR"))
        details = (
            f"timestamp={timestamp} dataset time={entry.get('time')} "
            f"usd={entry.get('USD')} eur={entry.get('EUR')} | "
            f"mempool.space time={reference.get('time')} "
            f"usd={reference.get('USD')} eur={reference.get('EUR')} | "
            f"diff usd={_format_diff(usd_diff)} eur={_format_diff(eur_diff)}"
        )
        if any(d is not None and abs(d) > self.warn_percent for d in (usd_diff, eur_diff)):
            self.warnings += 1
            logger.warning(
                "%s: dataset differs from mempool.space by more than %.2f%%: %s",
                VALIDATION_WARNING_TAG,
                self.warn_percent,
                details,
            )
        else:
            logger.info("Validation OK: %s", details)


def run_backfill(
    start_timestamp: int,
    end_timestamp: int,
    export_dir: Path,
    rate_limit_per_second: float = DEFAULT_RATE_LIMIT_PER_SECOND,
    base_url: str = DEFAULT_BASE_URL,
    currency: str = DEFAULT_CURRENCY,
    request_timeout_seconds: float = 20.0,
    validation_base_url: str = DEFAULT_VALIDATION_BASE_URL,
    validate_every: int = DEFAULT_VALIDATE_EVERY,
    validation_warn_percent: float = DEFAULT_VALIDATION_WARN_PERCENT,
) -> int:
    """Walks [start_timestamp, end_timestamp] in STEP_SECONDS increments,
    writing new minutes into export_dir/prices.csv. Returns a process exit
    code: 0 on completing the range (or finding nothing left to do),
    EXIT_RATE_LIMITED on a 429, 1 on any other fetch failure - in both
    non-zero cases everything fetched so far is flushed and the checkpoint
    points at the request that failed, so re-running the same command
    retries it instead of skipping past it.
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
    interval_seconds = 1.0 / rate_limit_per_second
    total_steps = (end_timestamp - next_timestamp) // STEP_SECONDS + 1

    logger.info(
        "Backfilling %s minute-by-minute from %d to %d (%d requests) into %s "
        "(currency=%s, %.1f req/s). Validating 1 in %d datapoints against %s, "
        "warning (%s) above %.2f%% difference.",
        base_url,
        next_timestamp,
        end_timestamp,
        total_steps,
        prices_path,
        currency,
        rate_limit_per_second,
        validate_every,
        validation_base_url,
        VALIDATION_WARNING_TAG,
        validation_warn_percent,
    )

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    validation_session = requests.Session()
    validation_session.headers.update({"Accept": "application/json"})
    validator = _Validator(
        validation_session,
        validation_base_url,
        currency,
        validate_every,
        validation_warn_percent,
        request_timeout_seconds,
    )

    written = 0
    steps_done = 0
    pending_rows: list[dict[str, Any]] = []
    steps_since_flush = 0
    run_started = last_flush = last_progress_log = next_request_at = time.monotonic()

    def flush() -> None:
        nonlocal pending_rows, steps_since_flush, last_flush
        write_rows_to_csv(pending_rows, prices_path)
        pending_rows = []
        steps_since_flush = 0
        last_flush = time.monotonic()
        _save_checkpoint(state_path, start_timestamp, next_timestamp)

    try:
        while next_timestamp <= end_timestamp:
            now = time.monotonic()
            if now < next_request_at:
                time.sleep(next_request_at - now)
            next_request_at = max(now, next_request_at) + interval_seconds

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

            entry = _first_price(data)
            if entry is None:
                logger.warning("No price data returned for timestamp %d - skipping.", next_timestamp)
            else:
                validator.observe(next_timestamp, entry)
                date_unix = entry.get("time")
                if date_unix is not None and int(date_unix) not in existing_dates:
                    date_unix = int(date_unix)
                    pending_rows.append(
                        {"date_unix": date_unix, "usd": entry.get("USD"), "eur": entry.get("EUR")}
                    )
                    existing_dates.add(date_unix)
                    written += 1

            next_timestamp += STEP_SECONDS
            steps_done += 1
            steps_since_flush += 1

            now = time.monotonic()
            if steps_since_flush >= FLUSH_EVERY_STEPS or now - last_flush >= FLUSH_EVERY_SECONDS:
                flush()
            if now - last_progress_log >= PROGRESS_LOG_EVERY_SECONDS:
                last_progress_log = now
                logger.info(
                    "Progress: %d/%d requests (%.1f req/s), %d new row(s), at timestamp %d.",
                    steps_done,
                    total_steps,
                    steps_done / (now - run_started),
                    written,
                    next_timestamp,
                )
    finally:
        # Runs on completion, on the early returns above and on Ctrl-C/any
        # crash alike, so a resume never re-fetches more than it has to.
        flush()
        logger.info(
            "Validation summary: %d checked, %d %s, %d failed/skipped (%s).",
            validator.checked,
            validator.warnings,
            VALIDATION_WARNING_TAG,
            validator.failed,
            VALIDATION_FAILED_TAG,
        )

    logger.info("Backfill complete: wrote %d new minute(s) to %s.", written, prices_path)
    return 0


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
        "--base-url", default=DEFAULT_BASE_URL,
        help=f"mempool instance to take the data from. Default: {DEFAULT_BASE_URL}.",
    )
    parser.add_argument(
        "--rate-limit-per-second", type=float, default=DEFAULT_RATE_LIMIT_PER_SECOND,
        help=f"Max requests per second against --base-url, enforced by fixed-interval "
        f"pacing (no burst allowance). Default: {DEFAULT_RATE_LIMIT_PER_SECOND:g}.",
    )
    parser.add_argument(
        "--currency", default=DEFAULT_CURRENCY,
        help=f"currency= query parameter. Default: {DEFAULT_CURRENCY!r}.",
    )
    parser.add_argument(
        "--validation-base-url", default=DEFAULT_VALIDATION_BASE_URL,
        help=f"Reference instance for spot checks, queried at most once per "
        f"{VALIDATION_MIN_INTERVAL_SECONDS:g}s. Default: {DEFAULT_VALIDATION_BASE_URL}.",
    )
    parser.add_argument(
        "--validate-every", type=int, default=DEFAULT_VALIDATE_EVERY,
        help=f"Validate 1 in N datapoints against --validation-base-url; 0 disables "
        f"validation. Default: {DEFAULT_VALIDATE_EVERY}.",
    )
    parser.add_argument(
        "--validation-warn-percent", type=float, default=DEFAULT_VALIDATION_WARN_PERCENT,
        help=f"Log a {VALIDATION_WARNING_TAG} line when USD or EUR differ from the "
        f"reference by more than this many percent. Default: {DEFAULT_VALIDATION_WARN_PERCENT:g}.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(
        LoggingConfig(level="INFO", log_dir=args.export_dir), component="price_gap_backfill"
    )

    if args.rate_limit_per_second <= 0:
        print("--rate-limit-per-second must be > 0", file=sys.stderr)
        return 2
    if args.validate_every < 0:
        print("--validate-every must be >= 0", file=sys.stderr)
        return 2
    if args.validation_warn_percent < 0:
        print("--validation-warn-percent must be >= 0", file=sys.stderr)
        return 2

    return run_backfill(
        start_timestamp=args.start_timestamp,
        end_timestamp=args.end_timestamp,
        export_dir=args.export_dir,
        rate_limit_per_second=args.rate_limit_per_second,
        base_url=args.base_url.rstrip("/"),
        currency=args.currency,
        validation_base_url=args.validation_base_url.rstrip("/"),
        validate_every=args.validate_every,
        validation_warn_percent=args.validation_warn_percent,
    )


if __name__ == "__main__":
    raise SystemExit(main())
