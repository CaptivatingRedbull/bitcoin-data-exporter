"""One-time (but idempotent/re-runnable) bulk import of two Kraken minute-
OHLC CSV exports (XBTUSD_1.csv, XBTEUR_1.csv - the "_1" suffix is Kraken's
1-minute-candle interval) into mempool_api.output_dir/prices.csv - see
config.yaml's pricing.xbtusd_csv_path/pricing.xbteur_csv_path.

prices.csv is also the file the live poller appends to every 60s (see
btc_parser_app.api.mempool_endpoints.parse_prices) - both writers produce
the exact same row shape (date_unix, usd, eur), so historic and live data
live in one minutely file with no separate daily table to reconcile. Run
via `python run.py import-price-history` once; already-imported minutes are
skipped by date_unix, so re-running against a refreshed/extended export
only adds what's new.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from btc_parser_app.common.csv_writer import csv_parts_exist, read_csv_parts, write_rows_to_csv
from btc_parser_app.config import PricingConfig

logger = logging.getLogger(__name__)


def _read_kraken_minute_closes(csv_path: Path) -> dict[int, float]:
    """Reads a Kraken OHLC export (no header row; columns
    unix_timestamp,open,high,low,close,volume,trades) into
    {unix_timestamp: close}. Only the close price is kept - prices.csv's
    schema is just date_unix/usd/eur, not full OHLCV."""
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Kraken minute CSV not found at {csv_path} - set the matching "
            "pricing.xbtusd_csv_path/pricing.xbteur_csv_path in config.yaml "
            "to your downloaded export, or place the file there."
        )

    closes: dict[int, float] = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        for line_no, raw_row in enumerate(csv.reader(f), start=1):
            if not raw_row:
                continue
            try:
                timestamp, _open, _high, _low, close, _volume, _trades = raw_row[:7]
                closes[int(timestamp)] = float(close)
            except ValueError as exc:
                raise ValueError(
                    f"{csv_path}:{line_no}: expected "
                    f"unix_timestamp,open,high,low,close,volume,trades, got {raw_row!r}"
                ) from exc
    return closes


def _existing_dates(prices_path: Path) -> set[int]:
    if not csv_parts_exist(prices_path):
        return set()
    frame = read_csv_parts(prices_path, columns=["date_unix"])
    if frame.is_empty():
        return set()
    return {int(v) for v in frame["date_unix"].to_list()}


def import_price_history(pricing: PricingConfig, prices_path: Path) -> int:
    """Merges pricing.xbtusd_csv_path and pricing.xbteur_csv_path (joined on
    minute timestamp) and appends any minute not already in prices_path.
    Returns the number of rows written.

    A minute present in only one of the two files still gets a row, with
    the other currency left null - same as mempool.space's live endpoint,
    which occasionally omits a currency.
    """
    usd_by_minute = _read_kraken_minute_closes(pricing.xbtusd_csv_path)
    eur_by_minute = _read_kraken_minute_closes(pricing.xbteur_csv_path)

    already_imported = _existing_dates(prices_path)
    minutes = sorted((usd_by_minute.keys() | eur_by_minute.keys()) - already_imported)

    rows = [
        {
            "date_unix": minute,
            "usd": usd_by_minute.get(minute),
            "eur": eur_by_minute.get(minute),
        }
        for minute in minutes
    ]

    if not rows:
        logger.info(
            "Price history import: nothing new in %s / %s (already up to date).",
            pricing.xbtusd_csv_path,
            pricing.xbteur_csv_path,
        )
        return 0

    write_rows_to_csv(rows, prices_path)
    logger.info(
        "Price history import: wrote %d new minute(s) from %s / %s to %s.",
        len(rows),
        pricing.xbtusd_csv_path,
        pricing.xbteur_csv_path,
        prices_path,
    )
    return len(rows)
