"""Durable part-number sequencing and state->export handoff for
blocks.csv/transactions.csv - the two logical CSVs that (a) switch between
two write modes as rpc-ingest catches up to, then keeps following, the tip,
and (b) live under two different roots so nothing not yet safe for Splunk to
delete ever appears in the Splunk-facing directory (see rpc/ingest.py and
config.py's RpcConfig.output_dir/state_dir):

- Batched (there's backlog beyond rpc.reorg_confirmations): rows accumulate
  in memory and get appended to the current part *under state_dir* -
  rotating to a new part once it crosses ~900MB, same as every other CSV
  this app writes (see common/csv_writer.py). The instant a part stops being
  the one being appended to (rotated by size, or superseded by switching to
  atomic mode), it is moved whole - a same-filesystem os.replace(), atomic -
  from state_dir into output_dir. Only then does it become visible to
  Splunk; while it's still being appended to, it never appears under
  output_dir at all.
- Atomic (caught up to within rpc.reorg_confirmations of the tip): one part
  per block, written directly into output_dir - whole, via a temp-file +
  rename (common/atomic_write.py) - so it never touches state_dir and a
  directory-watching consumer (Splunk's batch input) never sees a partially
  written file.

Both modes share one "next part number" counter per logical file, persisted
to a tiny state file (blocks_part_seq.csv / transactions_part_seq.csv, under
state_dir next to current.csv/latest.csv). This is required, not just
convenient: atomic mode hands every part straight to Splunk, so nothing
guarantees a given part is still on disk by the time this app needs to pick
the next number - common/csv_writer.py's scan-the-directory approach only
works for files where the *current* part is always guaranteed present
(nothing else in this app deletes its own output). The counter also
remembers whether the current part is still open for appending (batched,
sitting under state_dir) or already closed and hand-off/written under
output_dir - so if backlog ever reappears after atomic mode has been used
(e.g. after downtime) and the loop drops back to batched mode, it starts a
fresh part under state_dir instead of trying to resume appends into a file
that's already been moved (or written) into output_dir and may already be
gone.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import polars as pl

from btc_parser_app.common.atomic_write import atomic_replace
from btc_parser_app.common.csv_writer import (
    MAX_PART_BYTES,
    existing_part_numbers,
    part_path,
)


def _read_state(path: Path) -> tuple[int, bool] | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    frame = pl.read_csv(path)
    if frame.is_empty():
        return None
    row = frame.row(0, named=True)
    return int(row["current_part"]), bool(row["part_is_open"])


def _write_state(path: Path, current_part: int, part_is_open: bool) -> None:
    atomic_replace(
        path,
        lambda tmp: pl.DataFrame(
            [{"current_part": current_part, "part_is_open": part_is_open}]
        ).write_csv(tmp),
    )


class PartSequencer:
    """Tracks the next part to write for one logical CSV (blocks.csv or
    transactions.csv), backed by a persisted counter file so numbering
    survives restarts and mode switches without ever colliding or reusing a
    number a downstream consumer may already have taken (and deleted)."""

    def __init__(
        self,
        state_base: Path,
        export_base: Path,
        seq_path: Path,
        max_part_bytes: int = MAX_PART_BYTES,
    ) -> None:
        self.state_base = state_base
        self.export_base = export_base
        self._seq_path = seq_path
        self._max_part_bytes = max_part_bytes
        state = _read_state(seq_path)
        if state is None:
            # First-ever use: nothing persisted yet, so seed from whatever's
            # on disk (this scan only ever happens once - see module
            # docstring). state_base wins if it has anything - that's an
            # open, appendable part (e.g. left over from a manual migration
            # into the new state_dir/output_dir layout, or an
            # interrupted run). Otherwise fall back to export_base's highest
            # closed part. Neither existing means a genuinely fresh start.
            state_highest = max(existing_part_numbers(state_base), default=0)
            if state_highest:
                state = (state_highest, True)
            else:
                export_highest = max(existing_part_numbers(export_base), default=0)
                state = (export_highest, False) if export_highest else (1, True)
        self._current_part, self._part_is_open = state

    def _persist(self) -> None:
        _write_state(self._seq_path, self._current_part, self._part_is_open)

    def _close_if_open(self) -> None:
        """Hand off the current part from state_base to export_base, if it's
        still open. A no-op if it's already closed, or was never actually
        written to (allocated but empty - can't happen in practice, since a
        part number is only ever advanced right before writing to it, but
        guarded anyway since a missing source is harmless to skip)."""
        if not self._part_is_open:
            return
        src = part_path(self.state_base, self._current_part)
        if src.exists():
            dst = part_path(self.export_base, self._current_part)
            dst.parent.mkdir(parents=True, exist_ok=True)
            os.replace(src, dst)
        self._part_is_open = False

    def write_batched(self, rows: list[dict[str, Any]]) -> None:
        """Append rows to the current part under state_base, rotating to a
        new part first if it's already at/over the size cap, or if the
        current part number was already closed out (moved to export_base by
        a prior rotation, or written there directly by write_atomic)."""
        if not rows:
            return
        target = part_path(self.state_base, self._current_part)
        needs_new_part = not self._part_is_open or (
            target.exists() and target.stat().st_size >= self._max_part_bytes
        )
        if needs_new_part:
            self._close_if_open()
            self._current_part += 1
            self._part_is_open = True
            target = part_path(self.state_base, self._current_part)

        target.parent.mkdir(parents=True, exist_ok=True)
        frame = pl.DataFrame(rows, infer_schema_length=None)
        file_exists = target.exists() and target.stat().st_size > 0
        with open(target, mode="a", encoding="utf-8", newline="") as f:
            frame.write_csv(f, include_header=not file_exists)
        self._persist()

    def write_atomic(self, rows: list[dict[str, Any]]) -> None:
        """Write rows as a brand-new, complete part directly under
        export_base - always a new part number, written whole (temp file +
        rename) so it never appears partially written to an external
        reader, and never touches state_base at all."""
        if not rows:
            return
        self._close_if_open()
        self._current_part += 1
        self._part_is_open = False
        target = part_path(self.export_base, self._current_part)
        target.parent.mkdir(parents=True, exist_ok=True)
        frame = pl.DataFrame(rows, infer_schema_length=None)
        atomic_replace(target, lambda tmp: frame.write_csv(tmp))
        self._persist()
