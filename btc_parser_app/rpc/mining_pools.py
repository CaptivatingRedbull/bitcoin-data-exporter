"""Mining pool extractor for the RPC block parser.

Bitcoin Core's RPC has no concept of "pool identity" - a block just has a
coinbase transaction. Pools identify themselves voluntarily, in one (or
both) of two ways, and this module reconstructs pool_id/pool_name entirely
offline from data getblock verbosity=3 already returns:

1. A tag: pools stamp a short ASCII string into the coinbase transaction's
   scriptSig as a form of self-identification, e.g. "/AntPool/",
   "/ViaBTC/", "/Foundry USA Pool #dropgold/". This is deliberate and
   essentially unambiguous once matched.
2. A payout address: the coinbase transaction's outputs pay a pool's known
   address(es). This still works for blocks with no tag, but it's a weaker
   signal - an address can end up shared or reused in ways a coinbase tag
   never is (e.g. two pools paying out through the same custodian) - so
   it's only used as a fallback when no tag matches.

The signature dataset (config/pools-v2.json) uses the same schema as
mempool.space's own pools-v2.json: [{"id", "name", "link", "tags":
[...ascii substrings...], "addresses": [...base58/bech32 strings...]}, ...].
See btc_parser_app.api.mining_pools_dataset for how it's kept up to date.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PoolInfo:
    pool_id: int | None
    name: str
    link: str | None


@dataclass(frozen=True)
class PoolMatch:
    pool_id: int | None
    pool_name: str | None
    pool_link: str | None
    match_method: str  # "tag" | "address" | "unknown"


UNKNOWN_MATCH = PoolMatch(
    pool_id=None, pool_name=None, pool_link=None, match_method="unknown"
)


def coinbase_hex_to_ascii(coinbase_hex: str | None) -> str:
    """Best-effort decode of the coinbase scriptSig hex to ASCII for tag
    matching. The scriptSig is arbitrary bytes (push opcodes, BIP34 height,
    extranonce, ...), not a text field, so this deliberately ignores
    decode errors rather than failing - any pool tag substring embedded in
    there still comes through fine."""
    if not coinbase_hex:
        return ""
    try:
        return bytes.fromhex(coinbase_hex).decode("latin1", errors="ignore")
    except ValueError:
        return ""


class PoolMatcher:
    """Loads a pools-v2.json-shaped dataset once and matches blocks against
    it in O(1) for addresses and O(number of tags) for the tag scan."""

    def __init__(self, pools: list[dict[str, Any]]) -> None:
        self._tag_index: list[tuple[str, PoolInfo]] = []
        self._address_index: dict[str, PoolInfo] = {}

        for pool in pools:
            info = PoolInfo(
                pool_id=pool.get("id"),
                name=pool.get("name") or "Unknown",
                link=pool.get("link"),
            )
            for tag in pool.get("tags") or []:
                if tag:
                    self._tag_index.append((tag, info))
            for address in pool.get("addresses") or []:
                if address:
                    self._address_index[address] = info

    @classmethod
    def from_file(cls, path: Path) -> "PoolMatcher":
        pools = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(pools, list):
            raise ValueError(f"{path}: expected a JSON list of pool objects")
        return cls(pools)

    @property
    def pool_count(self) -> int:
        return len(
            {info.pool_id for _, info in self._tag_index}
            | {info.pool_id for info in self._address_index.values()}
        )

    def match(self, coinbase_hex: str | None, payout_addresses: list[str]) -> PoolMatch:
        coinbase_ascii = coinbase_hex_to_ascii(coinbase_hex)

        for tag, info in self._tag_index:
            if tag in coinbase_ascii:
                return PoolMatch(info.pool_id, info.name, info.link, "tag")

        for address in payout_addresses:
            info = self._address_index.get(address)
            if info is not None:
                return PoolMatch(info.pool_id, info.name, info.link, "address")

        return UNKNOWN_MATCH


def load_pool_matcher(local_path: Path) -> PoolMatcher | None:
    """Load a PoolMatcher from local_path, degrading to None (rather than
    raising) if the dataset is missing or malformed - callers should treat
    that as "no pool attribution this run", not a hard failure."""
    if not local_path.exists():
        logger.warning(
            "%s not found - mining pool attribution will be empty. "
            "Run `python -m btc_parser_app.cli update-pools-dataset` to fetch it.",
            local_path,
        )
        return None
    try:
        matcher = PoolMatcher.from_file(local_path)
        logger.info(
            "Loaded mining pool signatures from %s (%d pools)",
            local_path,
            matcher.pool_count,
        )
        return matcher
    except (ValueError, json.JSONDecodeError) as exc:
        logger.error(
            "Failed to load %s (%s) - mining pool attribution will be empty.",
            local_path,
            exc,
        )
        return None
