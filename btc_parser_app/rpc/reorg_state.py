"""State files backing the reorg-aware RPC ingest loop (btc_parser_app.rpc.ingest),
implementing the "RPC Parser Reorg Handling" section of Script_plan.md:

- index/index.csv    - immutable, append-only record of every block ever
                        exported: height,blockhash,previousblockhash. A
                        height can have more than one row if it was ever
                        reorged - this is a log of everything seen, not a
                        current-state table. Rotates into index.000002.csv,
                        etc. once a part hits ~900MB (see common/csv_writer.py)
                        - IndexStore reads every part back on load.
- current.csv        - single row (height,blockhash): the latest
                        successfully processed canonical block.
- latest.csv         - single row (height,blockhash): the current node tip
                        minus rpc.reorg_confirmations.
- block_status.csv   - mutable (height,blockhash,canonical) rows. Only
                        blocks that have ever been detached by a reorg get
                        a row here; anything never touched by a reorg is
                        implicitly canonical and never appears.
- reorg/              - one immutable audit CSV per reorg event
                        (reorg_<timestamp>_<lowest>_<highest>.csv), for
                        debugging only - never read back to determine
                        current state.

Use blockhash as identity throughout: index/ answers "has this block ever
been exported", block_status.csv answers "is this block canonical now".
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from btc_parser_app.common.atomic_write import atomic_replace
from btc_parser_app.common.csv_writer import csv_parts_exist, read_csv_parts, write_rows_to_csv


# =============================================================================
# current.csv / latest.csv - single-row pointer files
# =============================================================================


def read_pointer(path: Path) -> tuple[int, str] | None:
    """Read a single-row (height,blockhash) pointer file, or None if it
    doesn't exist yet (first-ever run)."""
    if not path.exists() or path.stat().st_size == 0:
        return None
    frame = pl.read_csv(path)
    if frame.is_empty():
        return None
    row = frame.row(0, named=True)
    return int(row["height"]), str(row["blockhash"])


def write_pointer(path: Path, height: int, blockhash: str) -> None:
    """Overwrite a single-row (height,blockhash) pointer file. current.csv
    and latest.csv are mutable state, not append logs - unlike index.csv.
    Written atomically (temp file + rename) so a crash mid-write leaves the
    previous, still-valid pointer in place instead of a truncated file that
    would crash read_pointer on the next startup."""
    atomic_replace(
        path,
        lambda tmp: pl.DataFrame([{"height": height, "blockhash": blockhash}]).write_csv(tmp),
    )


# =============================================================================
# index/index.csv - immutable append log, keyed by blockhash
# =============================================================================


@dataclass(frozen=True)
class IndexRow:
    height: int
    blockhash: str
    previousblockhash: str


class IndexStore:
    """In-memory view of index/index.csv. Answers "has this exact blockhash
    ever been exported" in O(1), and gives blockhash -> previousblockhash
    lookups for the reorg ancestor walk-back.

    add() only stages a row in memory - it is NOT written to disk and does
    NOT become visible to contains()/get() until flush() runs. This is
    deliberate: the caller must only flush() after the corresponding
    blocks.csv/transactions.csv rows are durable (see ingest.py's batch
    checkpoints), so index.csv can never claim a block was exported before
    it actually was. Committing eagerly (the old behaviour) meant a crash
    between add() and the next batch flush left index.csv ahead of
    blocks.csv/transactions.csv - on restart, contains() would skip
    re-exporting those heights, permanently losing them."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._by_hash: dict[str, IndexRow] = {}
        self._pending: dict[str, IndexRow] = {}
        self._load()

    def _load(self) -> None:
        if not csv_parts_exist(self.path):
            return
        frame = read_csv_parts(
            self.path,
            schema_overrides={
                "height": pl.Int64,
                "blockhash": pl.Utf8,
                "previousblockhash": pl.Utf8,
            },
        )
        for row in frame.iter_rows(named=True):
            self._register(int(row["height"]), row["blockhash"], row["previousblockhash"])

    def _register(self, height: int, blockhash: str, previousblockhash: str) -> None:
        self._by_hash[blockhash] = IndexRow(height, blockhash, previousblockhash or "")

    def contains(self, blockhash: str) -> bool:
        return blockhash in self._by_hash

    def get(self, blockhash: str) -> IndexRow | None:
        return self._by_hash.get(blockhash)

    def add(self, height: int, blockhash: str, previousblockhash: str) -> None:
        """Stage a row for this blockhash, unless it's already indexed or
        already staged (re-attached blocks after a reorg flip-flop must not
        be exported twice). Call flush() to persist and make visible."""
        if blockhash in self._by_hash or blockhash in self._pending:
            return
        self._pending[blockhash] = IndexRow(height, blockhash, previousblockhash or "")

    def flush(self) -> None:
        """Persist staged rows to index.csv and register them for
        contains()/get(). Call this only after the blocks.csv/
        transactions.csv rows they correspond to are already durable."""
        if not self._pending:
            return
        write_rows_to_csv(
            [
                {"height": r.height, "blockhash": r.blockhash, "previousblockhash": r.previousblockhash}
                for r in self._pending.values()
            ],
            self.path,
        )
        self._by_hash.update(self._pending)
        self._pending.clear()


def seed_index_from_blocks_csv(index: IndexStore, blocks_csv: Path) -> tuple[int, str] | None:
    """Bootstrap index/ from an already-existing blocks.csv - e.g. this is
    not the first-ever run, but current.csv is missing (lost, or this
    output_dir was populated some other way) - so already-exported blocks
    are recognized instead of being re-fetched from genesis. Registers
    every row into `index` and returns the (height, blockhash) of the
    highest block found, or None if blocks.csv doesn't exist / is empty
    (nothing to resume from - start fresh at genesis)."""
    if not csv_parts_exist(blocks_csv):
        return None

    frame = read_csv_parts(
        blocks_csv,
        columns=["height", "hash", "previousblockhash"],
        schema_overrides={
            "height": pl.Int64,
            "hash": pl.Utf8,
            "previousblockhash": pl.Utf8,
        },
    )
    if frame.is_empty():
        return None

    for row in frame.iter_rows(named=True):
        index.add(int(row["height"]), row["hash"], row["previousblockhash"] or "")
    index.flush()

    top = frame.sort("height").row(-1, named=True)
    return int(top["height"]), str(top["hash"])


# =============================================================================
# block_status.csv - mutable canonical-flag table
# =============================================================================


class BlockStatusStore:
    """In-memory view of block_status.csv. Entries are only ever created by
    marking a block non-canonical (set_canonical); a block that has never
    been touched by a reorg simply has no row and is implicitly canonical.
    Call flush() to persist - mutations are batched in memory so a reorg
    affecting many blocks costs one rewrite, not one per block."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._entries: dict[tuple[int, str], bool] = {}
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return
        frame = pl.read_csv(self.path)
        for row in frame.iter_rows(named=True):
            self._entries[(int(row["height"]), str(row["blockhash"]))] = bool(row["canonical"])

    def set_canonical(self, height: int, blockhash: str, canonical: bool) -> None:
        """Create-or-update. Used to mark detached blocks non-canonical -
        this is what puts them in the table in the first place."""
        key = (height, blockhash)
        if self._entries.get(key) != canonical:
            self._entries[key] = canonical
            self._dirty = True

    def set_canonical_if_present(self, height: int, blockhash: str, canonical: bool) -> None:
        """Update-only. Used by the normal loop: a block that was never
        detached has no row and should stay absent, not get one manufactured
        just to say "yes, canonical"."""
        key = (height, blockhash)
        if key in self._entries and self._entries[key] != canonical:
            self._entries[key] = canonical
            self._dirty = True

    def flush(self) -> None:
        """Persist all entries, atomically (temp file + rename) so a crash
        mid-write can't leave block_status.csv truncated."""
        if not self._dirty:
            return
        rows = [
            {"height": height, "blockhash": blockhash, "canonical": canonical}
            for (height, blockhash), canonical in sorted(self._entries.items())
        ]
        atomic_replace(self.path, lambda tmp: pl.DataFrame(rows).write_csv(tmp))
        self._dirty = False


# =============================================================================
# reorg/ - immutable per-event audit logs
# =============================================================================


def write_reorg_log(reorg_dir: Path, lowest: int, highest: int, rows: list[dict]) -> Path:
    """Write reorg/reorg_<timestamp>_<lowest>_<highest>.csv with
    action,height,blockhash rows (action is "detached" or "attached"). Audit
    trail only - never read back to determine current state."""
    reorg_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    path = reorg_dir / f"reorg_{timestamp}_{lowest}_{highest}.csv"
    pl.DataFrame(rows).write_csv(path)
    return path
