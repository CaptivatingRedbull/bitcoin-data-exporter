"""Row-shaping function for the mempool.space "prices" endpoint.

A parser turns a decoded JSON response into a list of flat, scalar-field
dict rows ready for polars.

The registry at the bottom maps config.yaml's `endpoints[].parser` string to
one of these functions - add a new endpoint by writing a `parse_<name>`
function here and referencing `<name>` from config.yaml.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def parse_prices(data: Any) -> list[dict[str, Any]]:
    # Only USD/EUR - the other currencies mempool.space returns (GBP, CAD,
    # CHF, AUD, JPY) aren't used anywhere downstream. date_unix is the
    # price's own timestamp (data["time"]), not the poll time, so this row
    # shape matches exactly what btc_parser_app.api.price_gap_backfill
    # produces when backfilling a historic gap - both write into the same
    # prices.csv, one row per minute either way.
    return [
        {
            "date_unix": data.get("time"),
            "usd": data.get("USD"),
            "eur": data.get("EUR"),
        }
    ]


ParserFn = Callable[[Any], list[dict[str, Any]]]

PARSER_REGISTRY: dict[str, ParserFn] = {
    "prices": parse_prices,
}
