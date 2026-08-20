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

logger = logging.getLogger(__name__)

# Soft cap per part. Rotation is only checked before a write, so a part can
# briefly exceed this by up to one batch of rows - kept comfortably under
# 1GB to leave headroom for that overshoot.
MAX_PART_BYTES = 900_000_000


def _part_path(base_path: Path, part: int) -> Path:
    """base_path with `part` spliced in: part 1 is base_path itself,
    part 2+ is `<stem>.NNNNNN<suffix>` (e.g. blocks.000002.csv)."""
    if part <= 1:
        return base_path
    return base_path.with_name(f"{base_path.stem}.{part:06d}{base_path.suffix}")


def _existing_part_numbers(base_path: Path) -> list[int]:
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


def part_path(base_path: Path, part: int) -> Path:
    """Public wrapper for _part_path - the on-disk path for a given part
    number of a logical CSV (see rpc/part_writer.py, which needs this to
    address specific parts by number rather than by disk-scanning)."""
    return _part_path(base_path, part)


def existing_part_numbers(base_path: Path) -> list[int]:
    """Public wrapper for _existing_part_numbers - every part number
    currently on disk for base_path (see rpc/part_writer.py and
    rpc/reorg_state.py, which need this to merge parts split across two
    directories - a state root and an export root)."""
    return _existing_part_numbers(base_path)


def highest_existing_part(base_path: Path) -> int:
    """The highest part number currently on disk for base_path, or 0 if none
    exists. Used only to seed a durable part-number counter the first time
    it's created (see rpc/part_writer.py) - safe to call once for that, but
    not fit to call repeatedly as a source of truth, since a downstream
    consumer (e.g. a Splunk batch input) may delete older parts, which would
    make later scans under-report and hand out already-used numbers again."""
    numbers = _existing_part_numbers(base_path)
    return numbers[-1] if numbers else 0


def all_parts(base_path: Path) -> list[Path]:
    """Every existing on-disk part of a logical CSV, oldest (part 1) first."""
    return [_part_path(base_path, n) for n in _existing_part_numbers(base_path)]


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


def _current_part_for_append(base_path: Path, max_part_bytes: int) -> Path:
    numbers = _existing_part_numbers(base_path)
    if not numbers:
        return base_path
    latest = _part_path(base_path, numbers[-1])
    if latest.stat().st_size >= max_part_bytes:
        return _part_path(base_path, numbers[-1] + 1)
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


def flush_batch_to_disk(batch_data: dict[str, list[dict[str, Any]]], out_dir: Path) -> None:
    """Append every buffered {csv_stem: rows} group to out_dir/<stem>.csv
    (or its current rotated part), then clear each list in place so the
    caller's buffer is ready to reuse."""
    for key, records in batch_data.items():
        if not records:
            continue
        write_rows_to_csv(records, out_dir / f"{key}.csv")
        records.clear()
