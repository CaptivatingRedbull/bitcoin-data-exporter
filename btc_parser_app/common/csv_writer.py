"""Append-only CSV writing shared by the RPC parser and the API fetcher.

Both sides write flat dict rows through the same two functions so on-disk
behaviour (header-once, append thereafter, polars-inferred schema, size-based
rotation) stays identical across the whole app.

Every append-only CSV this app produces (blocks.csv, transactions.csv,
index/index.csv, the stale-blocks exports, peer_attempts.csv, the
mempool.space endpoint CSVs, ...) grows forever, so left alone any of them
can eventually pass 1GB. write_rows_to_csv rotates a logical CSV into
numbered parts instead: `<name>.csv` is the first part, then
`<name>.000002.csv`, `<name>.000003.csv`, ... - each capped at roughly
MAX_PART_BYTES. Rotation is checked once per call (before appending), so a
part can only ever overshoot the cap by the one batch of rows that pushed it
over, never grow unbounded.

Anything that needs to read a logical CSV back (not just append to it) must
use read_csv_parts()/csv_parts_exist() below instead of pl.read_csv()/
Path.exists() directly, or it will silently miss every part after the first.
"""

from __future__ import annotations

import logging
import re
from io import StringIO
from pathlib import Path
from typing import Any

import polars as pl

from btc_parser_app.common.atomic_write import atomic_replace

logger = logging.getLogger(__name__)

# Soft cap per part. Rotation is only checked before a write, so a part can
# briefly exceed this by up to one batch of rows - kept comfortably under
# 1GB to leave headroom for that overshoot.
MAX_PART_BYTES = 900_000_000


def part_path(base_path: Path, part: int) -> Path:
    """base_path with `part` spliced in: part 1 is base_path itself,
    part 2+ is `<stem>.NNNNNN<suffix>` (e.g. blocks.000002.csv)."""
    if part <= 1:
        return base_path
    return base_path.with_name(f"{base_path.stem}.{part:06d}{base_path.suffix}")


def existing_part_numbers(base_path: Path) -> list[int]:
    """Every part number currently on disk for base_path, ascending."""
    numbers = [1] if base_path.exists() else []
    parent = base_path.parent
    if not parent.is_dir():
        return sorted(numbers)
    part_re = re.compile(
        rf"^{re.escape(base_path.stem)}\.(\d{{6}}){re.escape(base_path.suffix)}$"
    )
    for candidate in parent.iterdir():
        match = part_re.match(candidate.name)
        if match:
            numbers.append(int(match.group(1)))
    return sorted(numbers)


def all_parts(base_path: Path) -> list[Path]:
    """Every existing on-disk part of a logical CSV, oldest (part 1) first."""
    return [part_path(base_path, n) for n in existing_part_numbers(base_path)]


def csv_parts_exist(base_path: Path) -> bool:
    """True if any non-empty part of this logical CSV exists on disk."""
    return any(p.stat().st_size > 0 for p in all_parts(base_path))


def read_csv_lenient(path: Path, **read_csv_kwargs: Any) -> pl.DataFrame:
    """pl.read_csv, but recovers from a truncated trailing line instead of
    raising and crashing the whole process. write_rows_to_csv() appends with
    no fsync/atomic-rename (unlike the whole-file state writes in
    atomic_write.py), so a crash mid-append can leave a part's last line
    truncated (e.g. a quoted field cut mid-value) - on the next startup,
    that would otherwise make pl.read_csv raise ComputeError before the app
    even gets to reprocess anything. On that failure, the last line is
    dropped and the read retried once: the row it belonged to was never
    fully durable in the first place, so nothing is lost that survived the
    crash - and the caller's own reprocessing-on-restart logic (e.g.
    IndexStore's contains()/needs_export() gate) picks it back up normally,
    the same as any other row that wasn't flushed before a crash."""
    try:
        return pl.read_csv(path, **read_csv_kwargs)
    except pl.exceptions.ComputeError:
        lines = path.read_text(encoding="utf-8").splitlines()
        if len(lines) <= 1:
            raise
        logger.warning(
            "%s: failed to parse - discarding its last line as a likely "
            "crash-truncated row and retrying.",
            path,
        )
        return pl.read_csv(StringIO("\n".join(lines[:-1]) + "\n"), **read_csv_kwargs)


def read_csv_parts(base_path: Path, **read_csv_kwargs: Any) -> pl.DataFrame:
    """Concatenate every on-disk part of a logical CSV into one frame, in
    part order (oldest/lowest heights first). Returns an empty DataFrame if
    no part exists or every part is empty. kwargs are forwarded to
    pl.read_csv for each part (columns=, schema_overrides=, ...)."""
    frames = [
        read_csv_lenient(part, **read_csv_kwargs)
        for part in all_parts(base_path)
        if part.stat().st_size > 0
    ]
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical_relaxed")


def read_single_row_csv(path: Path) -> dict[str, Any] | None:
    """Read a single-row state/pointer CSV (current.csv, latest.csv, the
    *_part_seq.csv counters, ...) back into a dict, or None if it doesn't
    exist yet or is empty (first-ever run). Shared by every such file's
    read/write pair instead of each hand-rolling the same exists/empty/
    row(0) dance with its own field names."""
    if not path.exists() or path.stat().st_size == 0:
        return None
    frame = pl.read_csv(path)
    if frame.is_empty():
        return None
    return frame.row(0, named=True)


def write_single_row_csv(path: Path, row: dict[str, Any]) -> None:
    """Atomically overwrite a single-row state/pointer CSV with `row`
    (temp file + rename), so a crash mid-write leaves the previous, still-
    valid row in place instead of a truncated file that would crash
    read_single_row_csv on the next startup."""
    atomic_replace(path, lambda tmp: pl.DataFrame([row]).write_csv(tmp))


def _current_part_for_append(base_path: Path, max_part_bytes: int) -> Path:
    numbers = existing_part_numbers(base_path)
    if not numbers:
        return base_path
    latest = part_path(base_path, numbers[-1])
    if latest.stat().st_size >= max_part_bytes:
        return part_path(base_path, numbers[-1] + 1)
    return latest


def write_rows_to_csv(
    rows: list[dict[str, Any]],
    file_path: Path,
    max_part_bytes: int = MAX_PART_BYTES,
) -> None:
    """Append rows to file_path (rotating into a new numbered part first if
    the current part is already at/over max_part_bytes), writing a header
    only if that part is new/empty."""
    if not rows:
        return

    file_path.parent.mkdir(parents=True, exist_ok=True)
    target = _current_part_for_append(file_path, max_part_bytes)
    frame = pl.DataFrame(rows, infer_schema_length=None)
    file_exists = target.exists() and target.stat().st_size > 0

    with open(target, mode="a", encoding="utf-8", newline="") as f:
        frame.write_csv(f, include_header=not file_exists)

