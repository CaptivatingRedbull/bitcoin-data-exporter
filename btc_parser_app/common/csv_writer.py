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

import re
from pathlib import Path
from typing import Any

import polars as pl

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


def all_parts(base_path: Path) -> list[Path]:
    """Every existing on-disk part of a logical CSV, oldest (part 1) first."""
    return [_part_path(base_path, n) for n in _existing_part_numbers(base_path)]


def csv_parts_exist(base_path: Path) -> bool:
    """True if any non-empty part of this logical CSV exists on disk."""
    return any(p.stat().st_size > 0 for p in all_parts(base_path))


def read_csv_parts(base_path: Path, **read_csv_kwargs: Any) -> pl.DataFrame:
    """Concatenate every on-disk part of a logical CSV into one frame, in
    part order (oldest/lowest heights first). Returns an empty DataFrame if
    no part exists or every part is empty. kwargs are forwarded to
    pl.read_csv for each part (columns=, schema_overrides=, ...)."""
    frames = [
        pl.read_csv(part, **read_csv_kwargs)
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
