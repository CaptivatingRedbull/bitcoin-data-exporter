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
from btc_parser_app.common.csv_writer import (
    MAX_PART_BYTES,
    all_parts,
    csv_parts_exist,
    existing_part_numbers,
    part_path,
    read_csv_lenient,
)

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


CURRENT_INDEX_SCHEMA_VERSION = 2
"""Bumped whenever a new per-block event type is added to what "fully
exported" means for a block (e.g. version 2 added inputs.csv/outputs.csv
alongside blocks.csv/transactions.csv). IndexRow.schema_version records the
version a block was last exported at, so IndexStore.needs_export() can tell
apart a block that's genuinely up to date from one that was indexed under an
older schema and is missing event types added since - and only the latter
gets reprocessed. Rows read back from an index.csv written before this
column existed default to schema_version=1."""


@dataclass(frozen=True)
class IndexRow:
    height: int
    blockhash: str
    previousblockhash: str
    schema_version: int = 1


class IndexStore:
    """In-memory view of index/index.csv. Answers "has this exact blockhash
    already been exported at the current event schema" in O(1) via
    needs_export(), and gives blockhash -> previousblockhash lookups for the
    reorg ancestor walk-back.

    add() only stages a row in memory - it is NOT written to disk and does
    NOT become visible to contains()/needs_export()/get() until flush() runs.
    This is deliberate: the caller must only flush() after the corresponding
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
        # Cached instead of rescanned on every flush() (see flush()) - this
        # one-time scan is the only directory listing IndexStore ever does.
        self._current_part = max(existing_part_numbers(path), default=1)
        self._load()

    def _load(self) -> None:
        if not csv_parts_exist(self.path):
            return
        for part in all_parts(self.path):
            if part.stat().st_size == 0:
                continue
            frame = read_csv_lenient(
                part,
                schema_overrides={
                    "height": pl.Int64,
                    "blockhash": pl.Utf8,
                    "previousblockhash": pl.Utf8,
                    "schema_version": pl.Int64,
                },
            )
            if "schema_version" not in frame.columns:
                # Written before schema_version existed: only blocks.csv/
                # transactions.csv were exported for these rows.
                frame = frame.with_columns(pl.lit(1).alias("schema_version"))
            for row in frame.iter_rows(named=True):
                self._register(
                    int(row["height"]),
                    row["blockhash"],
                    row["previousblockhash"],
                    int(row["schema_version"]),
                )

    def _register(
        self, height: int, blockhash: str, previousblockhash: str, schema_version: int
    ) -> None:
        self._by_hash[blockhash] = IndexRow(
            height, blockhash, previousblockhash or "", schema_version
        )

    def contains(self, blockhash: str) -> bool:
        return blockhash in self._by_hash

    def needs_export(self, blockhash: str) -> bool:
        """True if this block has never been indexed, or was last indexed
        under an older event schema than CURRENT_INDEX_SCHEMA_VERSION and so
        is missing event types added since (e.g. inputs.csv/outputs.csv)."""
        row = self._by_hash.get(blockhash) or self._pending.get(blockhash)
        return row is None or row.schema_version < CURRENT_INDEX_SCHEMA_VERSION

    def get(self, blockhash: str) -> IndexRow | None:
        return self._by_hash.get(blockhash)

    def add(
        self,
        height: int,
        blockhash: str,
        previousblockhash: str,
        schema_version: int = CURRENT_INDEX_SCHEMA_VERSION,
    ) -> None:
        """Stage a row for this blockhash at `schema_version`, unless it's
        already indexed (or staged) at that version or newer - re-attached
        blocks after a reorg flip-flop, or a block re-exported only to catch
        up to a schema bump, must not be exported twice for the same
        version. Call flush() to persist and make visible."""
        existing = self._by_hash.get(blockhash) or self._pending.get(blockhash)
        if existing is not None and existing.schema_version >= schema_version:
            return
        self._pending[blockhash] = IndexRow(
            height, blockhash, previousblockhash or "", schema_version
        )

    def flush(self) -> None:
        """Persist staged rows to index.csv and register them for
        contains()/needs_export()/get(). Call this only after the
        blocks.csv/transactions.csv/inputs.csv/outputs.csv rows they
        correspond to are already durable.

        Appends directly to a part path built from the part number cached in
        __init__ (bumped here on rotation), instead of routing through
        write_rows_to_csv - which re-scans the directory on every call. This
        runs once per block forever in steady-state tip-following, so that
        scan would otherwise happen indefinitely."""
        if not self._pending:
            return
        rows = [
            {
                "height": r.height,
                "blockhash": r.blockhash,
                "previousblockhash": r.previousblockhash,
                "schema_version": r.schema_version,
            }
            for r in self._pending.values()
        ]
        target = part_path(self.path, self._current_part)
        file_exists = target.exists() and target.stat().st_size > 0
        if file_exists and target.stat().st_size >= MAX_PART_BYTES:
            self._current_part += 1
            target = part_path(self.path, self._current_part)
            file_exists = False
        elif file_exists:
            with open(target, "r", encoding="utf-8") as f:
                header = f.readline()
            if "schema_version" not in header:
                # This part predates the schema_version column (written by
                # an older version of this app). Appending new rows - which
                # now carry an extra field - straight onto it would produce
                # a ragged CSV that fails to parse back. Roll to a new part
                # instead, same as an ordinary size-triggered rotation,
                # so each physical part's columns stay self-consistent.
                self._current_part += 1
                target = part_path(self.path, self._current_part)
                file_exists = False
        target.parent.mkdir(parents=True, exist_ok=True)
        frame = pl.DataFrame(rows, infer_schema_length=None)
        with open(target, mode="a", encoding="utf-8", newline="") as f:
            frame.write_csv(f, include_header=not file_exists)
        self._by_hash.update(self._pending)
        self._pending.clear()


def seed_index_from_blocks_csv(
    index: IndexStore, state_blocks_csv: Path, export_blocks_csv: Path
) -> tuple[int, str] | None:
    """Bootstrap index/ from whatever blocks.csv data already exists - e.g.
    this is not the first-ever run, but current.csv is missing (lost, or
    this state_dir was populated some other way) - so already-exported
    blocks are recognized instead of being re-fetched from genesis. Parts
    can be split across state_blocks_csv (the currently-open batched part,
    if any) and export_blocks_csv (every closed/handed-off part - see
    rpc/part_writer.py), so both are read and merged by part number.
    Best-effort only: export_blocks_csv is Splunk-facing, so older parts may
    already be gone by the time this runs - this recovers whatever's still
    on disk, not necessarily the full history. Registers every row found
    into `index` and returns the (height, blockhash) of the highest block
    found, or None if nothing is found anywhere (start fresh at genesis)."""
    parts: dict[int, Path] = {}
    for base in (state_blocks_csv, export_blocks_csv):
        for number in existing_part_numbers(base):
            parts[number] = part_path(base, number)

    frames = [
        pl.read_csv(
            p,
            columns=["height", "hash", "previousblockhash"],
            schema_overrides={
                "height": pl.Int64,
                "hash": pl.Utf8,
                "previousblockhash": pl.Utf8,
            },
        )
        for _, p in sorted(parts.items())
        if p.stat().st_size > 0
    ]
    if not frames:
        return None
    frame = pl.concat(frames, how="vertical_relaxed")
    if frame.is_empty():
        return None

    for row in frame.iter_rows(named=True):
        # schema_version=1: blocks.csv/transactions.csv existing doesn't
        # confirm inputs.csv/outputs.csv (or any future event type) were
        # also exported for these rows, so mark them at the oldest known
        # schema - needs_export() will correctly flag them for reprocessing
        # if the loop ever revisits these heights (e.g. after a reorg, or an
        # operator resetting current.csv to force a backfill).
        index.add(
            int(row["height"]), row["hash"], row["previousblockhash"] or "", schema_version=1
        )
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
        frame = read_csv_lenient(self.path)
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
