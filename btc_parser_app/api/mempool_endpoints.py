"""Row-shaping function for the mempool.space "prices" endpoint.

Ported from the original mempool_api_parser.py. A parser turns a decoded
JSON response into a list of flat, scalar-field dict rows ready for polars.

The registry at the bottom maps config.yaml's `endpoints[].parser` string to
one of these functions - add a new endpoint by writing a `parse_<name>`
function here and referencing `<name>` from config.yaml.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class PolledAt:
    """Timestamp handed to every parser so rows can be stamped with when
    they were fetched, independent of any timestamp the payload itself
    carries.

    Only the unix epoch value is exported to CSV (as_dict()) - Splunk indexes
    straight off an epoch value, so a second ISO-string column would just be
    redundant, slower to index, and another thing to keep in sync. utc_iso is
    kept on the object itself purely for human-readable log lines."""

    utc_iso: str
    unix: int

    @classmethod
    def now(cls) -> PolledAt:
        dt = datetime.now(timezone.utc)
        return cls(utc_iso=dt.isoformat(timespec="seconds"), unix=int(dt.timestamp()))

    def as_dict(self) -> dict[str, Any]:
        return {"polled_at_unix": self.unix}


def parse_prices(data: Any, polled_at: PolledAt) -> list[dict[str, Any]]:
    # Only USD/EUR - the other currencies mempool.space returns (GBP, CAD,
    # CHF, AUD, JPY) aren't used anywhere downstream. date_unix is the
    # price's own timestamp (data["time"]), not polled_at, so this row shape
    # matches exactly what btc_parser_app.api.price_gap_backfill produces
    # when backfilling a historic gap - both write into the same
    # prices.csv, one row per minute either way.
    return [
        {
            "date_unix": data.get("time"),
            "usd": data.get("USD"),
            "eur": data.get("EUR"),
        }
    ]


ParserFn = Callable[[Any, PolledAt], list[dict[str, Any]]]

PARSER_REGISTRY: dict[str, ParserFn] = {
    "prices": parse_prices,
}
