"""Pulls the crowd-sourced bitcoin-data/stale-blocks dataset
(https://github.com/bitcoin-data/stale-blocks) - stale/orphaned chain tips
observed by other node operators' getchaintips, going back further than
what this node's own ~10 peers can still show it. The dataset's own README
requires every row to carry a header alongside its hash ("a hash without a
block header could easily be fake") - stale_blocks.py independently
re-verifies that via common.block_header.validate_header_hash rather than
trusting it blindly.

Headers only: this pipeline parses/validates the raw header bytes entirely
offline (common.block_header) and never hands them to this node via
submitheader. bitcoin Core's checkpoint mechanism categorically rejects any
competing header/block at or below its highest hardcoded checkpoint -
confirmed in production this covers the overwhelming majority of this
dataset, right up to within ~1000 blocks of the live tip - so
submitheader/submitblock could never get most of this data past that wall
anyway. Since only header fields are needed for the export, that wall is
simply irrelevant here.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass

import requests

from btc_parser_app.api.client import ApiClient, FetchError, RateLimited
from btc_parser_app.api.rate_limiter import TokenBucket
from btc_parser_app.config import StaleBlocksGithubConfig

logger = logging.getLogger(__name__)

_HEADERS = {
    "Accept": "text/csv, text/plain",
    "User-Agent": "btc_parser_app-stale-blocks",
}

# This module makes at most one request per call, so a generous single-slot
# bucket is really just there to reuse ApiClient's retry/timeout handling
# (see api/mining_pools_dataset.py, which does the same for its own
# one-shot GitHub-hosted fetch).
_FETCH_RATE_LIMIT = TokenBucket(requests_per_minute=30, bucket_size=1)


class GithubFetchError(Exception):
    """Raised for anything that keeps a GitHub pull from producing usable
    data: network failure, non-200 status, malformed CSV."""


@dataclass(frozen=True)
class GithubHeaderRow:
    height: int
    blockhash: str
    header_hex: str


def fetch_stale_blocks_csv(
    config: StaleBlocksGithubConfig, timeout_seconds: float
) -> list[GithubHeaderRow]:
    session = requests.Session()
    session.headers.update(_HEADERS)
    client = ApiClient(
        session=session,
        rate_limiter=_FETCH_RATE_LIMIT,
        timeout_seconds=timeout_seconds,
        max_connection_retries=2,
        retry_backoff_seconds=3.0,
    )
    try:
        text = client.get_text(config.csv_url)
    except (FetchError, RateLimited) as exc:
        raise GithubFetchError(f"{config.csv_url}: {exc}") from exc

    rows: list[GithubHeaderRow] = []
    reader = csv.DictReader(io.StringIO(text))
    for raw_row in reader:
        try:
            rows.append(
                GithubHeaderRow(
                    height=int(raw_row["height"]),
                    blockhash=str(raw_row["hash"]).lower(),
                    header_hex=str(raw_row["header"]),
                )
            )
        except (KeyError, ValueError) as exc:
            logger.warning("Skipping malformed stale-blocks.csv row %r: %s", raw_row, exc)
    return rows
