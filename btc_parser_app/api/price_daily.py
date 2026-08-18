"""Shared row schema + read helpers for pricing.output_dir/price_daily.csv -
one row per UTC calendar day, fed by two independent writers that both
import this module to keep column order identical (write_rows_to_csv just
appends whatever column order each call's DataFrame happens to have, with no
schema check against the existing header - see common/csv_writer.py):

- btc_parser_app.api.kraken_import - one-time bulk import of a Kraken OHLC
  CSV export, source="kraken", the open/high/low/close/volume/trades columns
  populated.
- btc_parser_app.api.price_backfill - ongoing gap-filler against
  mempool.space's historical-price endpoint, source="mempool_backfill", the
  per-currency columns populated instead.

date_unix is always UTC midnight for the day the row represents (Kraken's
1440-interval export already uses UTC-midnight timestamps; price_backfill
computes it explicitly - see its _utc_midnight), so the two sources join
cleanly on date_unix despite covering different columns.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from btc_parser_app.common.csv_writer import csv_parts_exist, read_csv_parts

PRICE_DAILY_COLUMNS = [
    "date_unix",
    "source",
    "usd",
    "eur",
    "gbp",
    "cad",
    "chf",
    "aud",
    "jpy",
    "open_usd",
    "high_usd",
    "low_usd",
    "close_usd",
    "volume_btc",
    "trades",
]


def make_price_row(
    date_unix: int,
    source: str,
    *,
    usd: float | None = None,
    eur: float | None = None,
    gbp: float | None = None,
    cad: float | None = None,
    chf: float | None = None,
    aud: float | None = None,
    jpy: float | None = None,
    open_usd: float | None = None,
    high_usd: float | None = None,
    low_usd: float | None = None,
    close_usd: float | None = None,
    volume_btc: float | None = None,
    trades: int | None = None,
) -> dict[str, Any]:
    """Builds a price_daily.csv row with keys in PRICE_DAILY_COLUMNS order -
    always go through this rather than a bare dict literal, so every writer
    produces the same column order (see module docstring)."""
    return {
        "date_unix": date_unix,
        "source": source,
        "usd": usd,
        "eur": eur,
        "gbp": gbp,
        "cad": cad,
        "chf": chf,
        "aud": aud,
        "jpy": jpy,
        "open_usd": open_usd,
        "high_usd": high_usd,
        "low_usd": low_usd,
        "close_usd": close_usd,
        "volume_btc": volume_btc,
        "trades": trades,
    }


def read_price_daily(price_daily_path: Path) -> pl.DataFrame:
    """Every existing price_daily.csv row (all rotated parts, all sources).
    Empty DataFrame if nothing's been imported/backfilled yet."""
    if not csv_parts_exist(price_daily_path):
        return pl.DataFrame()
    return read_csv_parts(
        price_daily_path,
        columns=["date_unix", "source"],
        schema_overrides={"date_unix": pl.Int64, "source": pl.Utf8},
    )


def existing_dates(price_daily_path: Path, source: str | None = None) -> set[int]:
    """date_unix values already present, optionally filtered to one source
    (e.g. "kraken" for import_kraken_csv's own dedup check)."""
    frame = read_price_daily(price_daily_path)
    if frame.is_empty():
        return set()
    if source is not None:
        frame = frame.filter(pl.col("source") == source)
    return {int(v) for v in frame["date_unix"].to_list()}


def latest_date(price_daily_path: Path) -> int | None:
    """The newest date_unix on file across every source, or None if
    price_daily.csv doesn't exist/has no rows yet."""
    frame = read_price_daily(price_daily_path)
    if frame.is_empty():
        return None
    return int(frame["date_unix"].max())
