"""One-time (but idempotent/re-runnable) bulk import of a Kraken OHLC CSV
export into pricing.output_dir/price_daily.csv - see config.yaml's
pricing.kraken for the expected file (Kraken's historical-data download,
daily/1440-minute candles) and btc_parser_app.api.price_daily for the shared
row schema. Run via `python run.py import-kraken-prices`.

Deliberately does not fetch anything over the network or touch
mempool_api's rate-limit budget - that's price_backfill.py's job, for the
gap between wherever this leaves off and now.
"""

from __future__ import annotations

import csv
import logging

from btc_parser_app.api.price_daily import existing_dates, make_price_row
from btc_parser_app.common.csv_writer import write_rows_to_csv
from btc_parser_app.config import PricingConfig

logger = logging.getLogger(__name__)


def import_kraken_csv(config: PricingConfig) -> int:
    """Reads config.kraken.csv_path (no header row; columns
    unix_timestamp,open,high,low,close,volume,trades - Kraken's standard
    OHLCVT export) and appends any day not already imported to
    price_daily.csv. Returns the number of rows written.

    Safe to re-run: already-imported days are skipped by date_unix, so
    pointing this at a refreshed/extended export only adds what's new.
    """
    csv_path = config.kraken.csv_path
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Kraken CSV not found at {csv_path} - set pricing.kraken.csv_path "
            "in config.yaml to your downloaded export, or place the file there."
        )

    price_daily_path = config.output_dir / "price_daily.csv"
    already_imported = existing_dates(price_daily_path, source="kraken")

    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for line_no, raw_row in enumerate(csv.reader(f), start=1):
            if not raw_row:
                continue
            try:
                timestamp, open_, high, low, close, volume, trades = raw_row[:7]
                date_unix = int(timestamp)
            except ValueError as exc:
                raise ValueError(
                    f"{csv_path}:{line_no}: expected "
                    f"unix_timestamp,open,high,low,close,volume,trades, got {raw_row!r}"
                ) from exc

            if date_unix in already_imported:
                continue
            rows.append(
                make_price_row(
                    date_unix,
                    "kraken",
                    usd=float(close),
                    open_usd=float(open_),
                    high_usd=float(high),
                    low_usd=float(low),
                    close_usd=float(close),
                    volume_btc=float(volume),
                    trades=int(float(trades)),
                )
            )

    if not rows:
        logger.info("Kraken import: nothing new in %s (already up to date).", csv_path)
        return 0

    write_rows_to_csv(rows, price_daily_path)
    logger.info(
        "Kraken import: wrote %d new day(s) from %s to %s.",
        len(rows),
        csv_path,
        price_daily_path,
    )
    return len(rows)
