"""Internal bookkeeping for the stale-blocks pipeline (btc_parser_app.rpc.
stale_blocks) - NOT Splunk-facing. Splunk gets append-only event exports
written by stale_blocks.py; this is the pipeline's own memory of what it
already knows, so a restart doesn't re-export the same header twice.

registry.csv - one row per known non-active blockhash, keyed by hash
(mutable, rewritten in full on every flush - like reorg_state.
BlockStatusStore, not an append log). Tracks whether a valid header is on
record (status) and which status level was last exported to Splunk, so a
status upgrade (unusable -> header_only, e.g. a header that failed to fetch
on one pass succeeds on a later one) produces exactly one more event
instead of a duplicate.

Headers only, by design: this pipeline never fetches full block/tx data
(see stale_blocks.py's module docstring for why) - a header is either
present and valid, or it isn't. status is monotonic: unusable ->
header_only, never downgraded just because a later, worse sighting (e.g. a
transient RPC failure re-polling a tip we already have a header for) offers
nothing new.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from btc_parser_app.common.atomic_write import atomic_replace

UNUSABLE = "unusable"
HEADER_ONLY = "header_only"

_STATUS_RANK = {UNUSABLE: 0, HEADER_ONLY: 1}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass
class StaleBlockEntry:
    height: int
    blockhash: str
    status: str
    header_hex: str | None
    header_valid: bool | None
    source: str
    chaintip_status: str | None
    branchlen: int | None
    first_seen: str
    last_exported_status: str | None = None


class StaleBlockRegistry:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._entries: dict[str, StaleBlockEntry] = {}
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return
        frame = pl.read_csv(self.path, infer_schema_length=None)
        for row in frame.iter_rows(named=True):
            entry = StaleBlockEntry(
                height=int(row["height"]),
                blockhash=str(row["blockhash"]),
                status=str(row["status"]),
                header_hex=row["header_hex"] or None,
                header_valid=(
                    bool(row["header_valid"]) if row["header_valid"] is not None else None
                ),
                source=str(row["source"]),
                chaintip_status=row["chaintip_status"] or None,
                branchlen=(int(row["branchlen"]) if row["branchlen"] is not None else None),
                first_seen=str(row["first_seen"]),
                last_exported_status=row["last_exported_status"] or None,
            )
            self._entries[entry.blockhash] = entry

    def get(self, blockhash: str) -> StaleBlockEntry | None:
        return self._entries.get(blockhash)

    def upsert_sighting(
        self,
        *,
        height: int,
        blockhash: str,
        status: str,
        header_hex: str | None,
        header_valid: bool | None,
        source: str,
        chaintip_status: str | None,
        branchlen: int | None,
    ) -> StaleBlockEntry:
        """Record/update a sighting of `blockhash`. status only ever moves
        forward (_STATUS_RANK); header_hex/header_valid are only filled in
        if not already known, so a later, worse sighting can never overwrite
        better data already on record."""
        existing = self._entries.get(blockhash)
        if existing is None:
            entry = StaleBlockEntry(
                height=height,
                blockhash=blockhash,
                status=status,
                header_hex=header_hex,
                header_valid=header_valid,
                source=source,
                chaintip_status=chaintip_status,
                branchlen=branchlen,
                first_seen=_now_iso(),
            )
            self._entries[blockhash] = entry
            self._dirty = True
            return entry

        if _STATUS_RANK[status] > _STATUS_RANK[existing.status]:
            existing.status = status
            self._dirty = True
        if header_hex and not existing.header_hex:
            existing.header_hex = header_hex
            existing.header_valid = header_valid
            self._dirty = True
        if chaintip_status is not None and chaintip_status != existing.chaintip_status:
            existing.chaintip_status = chaintip_status
            self._dirty = True
        if branchlen is not None and branchlen != existing.branchlen:
            existing.branchlen = branchlen
            self._dirty = True
        return existing

    def mark_header_status_exported(self, blockhash: str, status: str) -> None:
        entry = self._entries.get(blockhash)
        if entry is not None and entry.last_exported_status != status:
            entry.last_exported_status = status
            self._dirty = True

    def flush(self) -> None:
        if not self._dirty:
            return
        rows = [
            {
                "height": e.height,
                "blockhash": e.blockhash,
                "status": e.status,
                "header_hex": e.header_hex,
                "header_valid": e.header_valid,
                "source": e.source,
                "chaintip_status": e.chaintip_status,
                "branchlen": e.branchlen,
                "first_seen": e.first_seen,
                "last_exported_status": e.last_exported_status,
            }
            for e in sorted(self._entries.values(), key=lambda e: (e.height, e.blockhash))
        ]
        # infer_schema_length=None: rows is the FULL registry every flush
        # (not just this pass's changes), so a column like header_hex is
        # often None for the first ~100 rows (sorted by height,blockhash)
        # and a real hex string later on - polars' default 100-row sample
        # would lock that column into a null-only schema and then crash
        # (ComputeError) the moment it hits the first real string.
        atomic_replace(
            self.path,
            lambda tmp: pl.DataFrame(rows, infer_schema_length=None).write_csv(tmp),
        )
        self._dirty = False
