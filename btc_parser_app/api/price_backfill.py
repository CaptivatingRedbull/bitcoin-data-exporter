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
from pathlib import Path

from btc_parser_app.api.client import ApiClient, FetchError, RateLimited
from btc_parser_app.api.price_daily import latest_date, make_price_row
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
    price_daily_path: Path, start_date_unix: int, unresolved: set[int]
) -> int | None:
    latest = latest_date(price_daily_path)
    day = (latest + _SECONDS_PER_DAY) if latest is not None else start_date_unix
    end = _yesterday_utc_midnight()
    while day in unresolved and day <= end:
        day += _SECONDS_PER_DAY
    return day if day <= end else None


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

    logger.info(
        "Price backfill: filling gaps in %s from %s onward (currency=%s), "
        "sharing the mempool_api rate-limit budget.",
        price_daily_path,
        config.backfill.endpoint_path,
        config.backfill.currency,
    )

    while not stop_event.is_set():
        day = _next_missing_day(price_daily_path, config.backfill.start_date_unix, unresolved)
        if day is None:
            if stop_event.wait(timeout=config.backfill.request_interval_seconds):
                return
            continue

        try:
            price = _fetch_day(config, base_url, client, day)
        except RateLimited as exc:
            logger.error("Price backfill: %s - stopping poller.", exc)
            rate_limited_event.set()
            stop_event.set()
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
                usd=price.get(config.backfill.currency.upper()),
                eur=price.get("EUR"),
                gbp=price.get("GBP"),
                cad=price.get("CAD"),
                chf=price.get("CHF"),
                aud=price.get("AUD"),
                jpy=price.get("JPY"),
            )
            write_rows_to_csv([row], price_daily_path)
            logger.info(
                "Price backfill: filled %s.",
                datetime.fromtimestamp(day, tz=timezone.utc).date(),
            )

        if stop_event.wait(timeout=config.backfill.request_interval_seconds):
            return
