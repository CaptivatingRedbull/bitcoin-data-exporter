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

from btc_parser_app.config import StaleBlocksGithubConfig

logger = logging.getLogger(__name__)

_HEADERS = {
    "Accept": "application/vnd.github+json",
    "User-Agent": "btc_parser_app-stale-blocks",
}


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
    try:
        response = requests.get(config.csv_url, timeout=timeout_seconds, headers=_HEADERS)
    except requests.exceptions.RequestException as exc:
        raise GithubFetchError(f"{config.csv_url}: request failed: {exc}") from exc

    if response.status_code != 200:
        raise GithubFetchError(f"{config.csv_url}: unexpected status {response.status_code}")

    rows: list[GithubHeaderRow] = []
    reader = csv.DictReader(io.StringIO(response.text))
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
