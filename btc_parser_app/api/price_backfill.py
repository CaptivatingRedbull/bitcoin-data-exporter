"""Keeps pricing.output_dir/price_daily.csv caught up to "yesterday" against
mempool.space's historical-price endpoint (GET .../historical-price?
currency=USD&timestamp=<unix>, which returns the nearest known price to that
timestamp) - see config.yaml's pricing.backfill.

Runs as one more thread inside api-poll (see api/poller.py), sharing the
same ApiClient/TokenBucket as the regular endpoint pollers rather than a
separate rate-limit allowance: every request_interval_seconds it looks for
the single oldest missing UTC day and fetches just that one, going through
the same shared rate.acquire() everything else does. Once caught up it goes
idle (zero requests) until a new gap appears - which is exactly what happens
after a crash/restart, so this loop is also the entire answer to "what fills
gaps left by downtime" without any special-casing on startup.

A day mempool.space has no data for (e.g. one older than its own price
history) is remembered in-memory for this process's lifetime so it isn't
retried every cycle - it isn't written to price_daily.csv, so a restart (or
a Kraken import that later covers it) will naturally retry it.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone

from btc_parser_app.api.client import ApiClient, FetchError, RateLimited, handle_rate_limited
from btc_parser_app.api.price_daily import existing_dates, make_price_row
from btc_parser_app.common.csv_writer import write_rows_to_csv
from btc_parser_app.config import PricingConfig

logger = logging.getLogger(__name__)

_SECONDS_PER_DAY = 86400


def _utc_midnight(dt: datetime) -> int:
    d = dt.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(d.timestamp())


def _yesterday_utc_midnight() -> int:
    # Never backfill "today" - its daily candle isn't closed yet, so
    # historical-price would just return a partial/current-ish price under
    # a day marker that later data disagrees with.
    return _utc_midnight(datetime.now(timezone.utc)) - _SECONDS_PER_DAY


def _next_missing_day(
    known_dates: set[int], start_date_unix: int, unresolved: set[int]
) -> int | None:
    """The oldest UTC day in [start_date_unix, yesterday] that's neither on
    file (known_dates) nor already tried-and-failed this run (unresolved) -
    a real scan, not just "latest known day + 1", so a gap left behind by an
    unresolved day is still found (and retried, once known_dates is
    refreshed on restart) even after a later day gets filled."""
    end = _yesterday_utc_midnight()
    day = start_date_unix
    while day <= end:
        if day not in known_dates and day not in unresolved:
            return day
        day += _SECONDS_PER_DAY
    return None


def _fetch_day(
    config: PricingConfig, base_url: str, client: ApiClient, day_unix: int
) -> dict | None:
    url = f"{base_url}{config.backfill.endpoint_path}?currency={config.backfill.currency}&timestamp={day_unix}"
    data = client.get_json(url)
    prices = (data or {}).get("prices") or []
    if not prices:
        return None
    return prices[0]


def run_price_backfill_loop(
    config: PricingConfig,
    base_url: str,
    client: ApiClient,
    stop_event: threading.Event,
    rate_limited_event: threading.Event,
) -> None:
    price_daily_path = config.output_dir / "price_daily.csv"
    unresolved: set[int] = set()
    # Read once per thread start rather than on every tick - updated
    # in-memory below as rows are written, instead of re-parsing the whole
    # CSV from disk every request_interval_seconds while idle.
    known_dates = existing_dates(price_daily_path)

    logger.info(
        "Price backfill: filling gaps in %s from %s onward (currency=%s), "
        "sharing the mempool_api rate-limit budget.",
        price_daily_path,
        config.backfill.endpoint_path,
        config.backfill.currency,
    )

    while not stop_event.is_set():
        day = _next_missing_day(known_dates, config.backfill.start_date_unix, unresolved)
        if day is None:
            if stop_event.wait(timeout=config.backfill.request_interval_seconds):
                return
            continue

        try:
            price = _fetch_day(config, base_url, client, day)
        except RateLimited as exc:
            handle_rate_limited(exc, "Price backfill", rate_limited_event, stop_event)
            return
        except FetchError as exc:
            logger.warning("Price backfill: %s", exc)
            if stop_event.wait(timeout=config.backfill.request_interval_seconds):
                return
            continue

        if price is None:
            logger.warning(
                "Price backfill: no historical price available for %s - skipping "
                "for this run.",
                datetime.fromtimestamp(day, tz=timezone.utc).date(),
            )
            unresolved.add(day)
        else:
            row = make_price_row(
                day,
                "mempool_backfill",
                usd=price.get("USD"),
                eur=price.get("EUR"),
                gbp=price.get("GBP"),
                cad=price.get("CAD"),
                chf=price.get("CHF"),
                aud=price.get("AUD"),
                jpy=price.get("JPY"),
            )
            write_rows_to_csv([row], price_daily_path)
            known_dates.add(day)
            logger.info(
                "Price backfill: filled %s.",
                datetime.fromtimestamp(day, tz=timezone.utc).date(),
            )

        if stop_event.wait(timeout=config.backfill.request_interval_seconds):
            return
