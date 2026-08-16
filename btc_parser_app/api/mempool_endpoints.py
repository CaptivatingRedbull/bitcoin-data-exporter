"""Row-shaping functions for each mempool.space endpoint.

Ported from the original mempool_api_parser.py. Each parser turns a decoded
JSON response into a list of flat, scalar-field dict rows ready for polars -
one endpoint -> one CSV. A parser returning multiple rows (mining pools)
just means that CSV grows wider in row-count, not column-count.

The registry at the bottom maps config.yaml's `endpoints[].parser` string to
one of these functions - add a new endpoint by writing a `parse_<name>`
function here and referencing `<name>` from config.yaml.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from btc_parser_app.common.json_utils import json_dumps_compact


@dataclass(frozen=True)
class PolledAt:
    """Timestamp attached to every row so events line up across the separate
    per-endpoint CSVs even though each endpoint is polled a few seconds apart.

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


def parse_fees_precise(data: Any, polled_at: PolledAt) -> list[dict[str, Any]]:
    return [
        {
            **polled_at.as_dict(),
            "fastest_fee_sat_vb": data.get("fastestFee"),
            "half_hour_fee_sat_vb": data.get("halfHourFee"),
            "hour_fee_sat_vb": data.get("hourFee"),
            "economy_fee_sat_vb": data.get("economyFee"),
            "minimum_fee_sat_vb": data.get("minimumFee"),
        }
    ]


def parse_mempool(data: Any, polled_at: PolledAt) -> list[dict[str, Any]]:
    # fee_histogram is a [feerate, cumulative_vsize] list with 200+ buckets.
    # We keep count/vsize/fee as first-class scalar columns and stash the
    # histogram itself as a compact JSON string column rather than either
    # dropping it or exploding it into its own CSV with no shared key.
    histogram = data.get("fee_histogram") or []
    return [
        {
            **polled_at.as_dict(),
            "tx_count": data.get("count"),
            "vsize_total": data.get("vsize"),
            "total_fee_sats": data.get("total_fee"),
            "fee_histogram_bucket_count": len(histogram),
            "fee_histogram_json": json_dumps_compact(histogram),
        }
    ]


def parse_prices(data: Any, polled_at: PolledAt) -> list[dict[str, Any]]:
    return [
        {
            **polled_at.as_dict(),
            "price_time_unix": data.get("time"),
            "usd": data.get("USD"),
            "eur": data.get("EUR"),
            "gbp": data.get("GBP"),
            "cad": data.get("CAD"),
            "chf": data.get("CHF"),
            "aud": data.get("AUD"),
            "jpy": data.get("JPY"),
        }
    ]


def parse_difficulty_adjustment(data: Any, polled_at: PolledAt) -> list[dict[str, Any]]:
    return [
        {
            **polled_at.as_dict(),
            "progress_percent": data.get("progressPercent"),
            "difficulty_change_percent": data.get("difficultyChange"),
            "estimated_retarget_date_unix_ms": data.get("estimatedRetargetDate"),
            "remaining_blocks": data.get("remainingBlocks"),
            "remaining_time_seconds": data.get("remainingTime"),
            "previous_retarget_percent": data.get("previousRetarget"),
            "previous_retarget_time_unix": data.get("previousTime"),
            "next_retarget_height": data.get("nextRetargetHeight"),
            "block_time_avg_seconds": data.get("timeAvg"),
            "block_time_adjusted_avg_seconds": data.get("adjustedTimeAvg"),
            "time_offset_seconds": data.get("timeOffset"),
            "expected_blocks": data.get("expectedBlocks"),
        }
    ]


def parse_mining_pools_24h(data: Any, polled_at: PolledAt) -> list[dict[str, Any]]:
    pools = data.get("pools") or []
    base = {
        **polled_at.as_dict(),
        "network_block_count_24h": data.get("blockCount"),
        "hashrate_24h": data.get("lastEstimatedHashrate"),
        "hashrate_3d": data.get("lastEstimatedHashrate3d"),
        "hashrate_1w": data.get("lastEstimatedHashrate1w"),
    }
    return [
        {
            **base,
            "pool_id": pool.get("poolId"),
            "pool_name": pool.get("name"),
            "pool_slug": pool.get("slug"),
            "pool_rank": pool.get("rank"),
            "pool_block_count_24h": pool.get("blockCount"),
            "pool_empty_blocks_24h": pool.get("emptyBlocks"),
            "pool_avg_match_rate": pool.get("avgMatchRate"),
            "pool_avg_fee_delta": pool.get("avgFeeDelta"),
            "pool_link": pool.get("link"),
        }
        for pool in pools
    ]


ParserFn = Callable[[Any, PolledAt], list[dict[str, Any]]]

PARSER_REGISTRY: dict[str, ParserFn] = {
    "fees_precise": parse_fees_precise,
    "mempool": parse_mempool,
    "prices": parse_prices,
    "difficulty_adjustment": parse_difficulty_adjustment,
    "mining_pools_24h": parse_mining_pools_24h,
}
