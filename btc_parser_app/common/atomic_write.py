"""Atomic whole-file replacement, shared by anything that overwrites a
state/config file in place (current.csv, latest.csv, block_status.csv,
pools-v2.json) - as opposed to the append-only writing in csv_writer.py.

Writes the new contents to a temp file in the target's own directory, then
os.replace()s it over the target. os.replace() is atomic on POSIX for a
same-filesystem move, so a crash mid-write can never leave the target
truncated or partially written - a reader always sees either the complete
old file or the complete new one, never something in between. The temp
file's data and the directory entry are both fsync()ed before/after the
rename so this holds across a real OS crash or power loss, not just an
in-process exception.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Callable


def atomic_replace(path: Path, write: Callable[[Path], None]) -> None:
    """Call write(tmp_path) to produce the new contents at a temp path next
    to `path`, then atomically replace `path` with it. Cleans up the temp
    file if `write` raises."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    tmp_path = Path(tmp_name)
    os.close(fd)
    try:
        write(tmp_path)
        # Flush the new content to disk before the rename, then flush the
        # rename itself, so durability holds across a real power loss/OS
        # crash - os.replace() alone only guarantees concurrent readers never
        # see a partial file, not that either the data or the rename have
        # actually reached disk.
        tmp_fd = os.open(tmp_path, os.O_RDONLY)
        try:
            os.fsync(tmp_fd)
        finally:
            os.close(tmp_fd)
        os.replace(tmp_path, path)
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
