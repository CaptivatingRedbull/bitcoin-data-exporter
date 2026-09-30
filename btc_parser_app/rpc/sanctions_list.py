"""Sanctioned-address list(s) for mempool-watch (btc_parser_app.rpc.
mempool_watch) - loaded from one or more CSVs (mempool_watch.
sanctions_list_paths) into one address -> SanctionedAddress lookup.

Expected CSV layout (header row required, column names case-insensitive,
extra/trailing empty columns ignored):

    address,name,first_name,sanctions_programs
    12QtD5BFwRsdNsAZY76UVE1xyCGNTojH9h,YAN,Xiaobing,SDNTK

Only `address` is mandatory; the other three are copied verbatim onto every
exported event so a Splunk alert can show who an address belongs to without
a separate lookup. Multiple programs go space-separated in one field (a
comma would split it into a new column).

Addresses are normalized before comparison: bech32/bech32m (bc1.../tb1.../
bcrt1...) are case-insensitive by spec but always rendered lowercase by
Bitcoin Core, so they're lowercased here too; base58 (1.../3...) addresses
are case-sensitive and compared exactly.

Reloaded automatically whenever any list file's mtime changes (see
SanctionsList.reload_if_changed), so the list can be updated in place
without restarting the service.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_BECH32_PREFIXES = ("bc1", "tb1", "bcrt1")


def normalize_address(address: str) -> str:
    address = address.strip()
    if address.lower().startswith(_BECH32_PREFIXES):
        return address.lower()
    return address


@dataclass(frozen=True)
class SanctionedAddress:
    address: str
    name: str
    first_name: str
    sanctions_programs: str
    list_file: str


def _load_file(path: Path) -> dict[str, SanctionedAddress]:
    entries: dict[str, SanctionedAddress] = {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return entries
        reader.fieldnames = [(name or "").strip().lower() for name in reader.fieldnames]
        if "address" not in reader.fieldnames:
            raise ValueError(f"{path}: no 'address' column in header {reader.fieldnames}")

        for line_no, row in enumerate(reader, start=2):
            address = normalize_address(row.get("address") or "")
            if not address:
                continue
            if address in entries:
                logger.warning("%s:%d: duplicate address %s - keeping the first row.", path, line_no, address)
                continue
            entries[address] = SanctionedAddress(
                address=address,
                name=(row.get("name") or "").strip(),
                first_name=(row.get("first_name") or "").strip(),
                sanctions_programs=(row.get("sanctions_programs") or "").strip(),
                list_file=path.name,
            )
    return entries


class SanctionsList:
    def __init__(self, paths: tuple[Path, ...]) -> None:
        self.paths = paths
        self._entries: dict[str, SanctionedAddress] = {}
        self._mtimes: tuple[float, ...] = ()
        self.reload_if_changed(force=True)

    def _current_mtimes(self) -> tuple[float, ...]:
        return tuple(p.stat().st_mtime for p in self.paths)

    def reload_if_changed(self, force: bool = False) -> bool:
        """Re-read every list file if any of them changed on disk; True if
        a new list was loaded. A broken
        edit (unreadable file, missing address column) keeps the previous,
        still-valid list in memory instead of silently watching nothing -
        except on the very first load, where there is no previous list, so
        the error propagates and the service refuses to start."""
        try:
            mtimes = self._current_mtimes()
        except OSError as exc:
            if force:
                raise
            logger.error("Sanctions list not readable (%s) - keeping the previous list.", exc)
            return False
        if not force and mtimes == self._mtimes:
            return False

        try:
            merged: dict[str, SanctionedAddress] = {}
            for path in self.paths:
                for address, entry in _load_file(path).items():
                    merged.setdefault(address, entry)
        except (OSError, ValueError, csv.Error) as exc:
            if force:
                raise
            logger.error("Sanctions list reload failed (%s) - keeping the previous list.", exc)
            # Don't retry the same broken file every second - wait for the
            # next edit.
            self._mtimes = mtimes
            return False

        self._entries = merged
        self._mtimes = mtimes
        logger.info(
            "Loaded %d sanctioned address(es) from %s.",
            len(merged),
            ", ".join(str(p) for p in self.paths),
        )
        return True

    def lookup(self, address: str | None) -> SanctionedAddress | None:
        if not address:
            return None
        return self._entries.get(normalize_address(address))

    def __len__(self) -> int:
        return len(self._entries)
