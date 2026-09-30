"""Internal bookkeeping for mempool-watch (btc_parser_app.rpc.mempool_watch) -
NOT Splunk-facing. Splunk gets the append-only event export written by
mempool_watch.py; this is the service's own memory of which transactions
touching a sanctioned address it is currently tracking, so a restart
neither re-reports a tx that's still sitting in the mempool nor loses track
of one that confirmed/left while the service was down.

flagged.json - one entry per tracked txid, rewritten in full (atomically,
see common/atomic_write.py) on every change. Entries only exist for txs
with at least one sanctioned-address match, so this stays tiny. An entry
lives from the tx's `seen` event until either:
  - it leaves the mempool without being mined (`replaced`/`removed` event), or
  - its confirming block is mempool_watch.forget_after_confirmations deep
    (no event - by then a reorg undoing it is not a realistic concern).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from btc_parser_app.common.atomic_write import atomic_replace

IN_MEMPOOL = "mempool"
CONFIRMED = "confirmed"


@dataclass
class FlaggedTx:
    txid: str
    first_seen_at: str
    first_seen_height: int
    fee_btc: str | None
    vsize: int | None
    # [[prev_txid, prev_vout], ...] - kept so a later removal can be told
    # apart as an RBF replacement (gettxspendingprevout on these).
    inputs: list[list[Any]]
    # One dict per matched input/output - see mempool_watch.match_tx().
    matches: list[dict[str, Any]]
    status: str = IN_MEMPOOL
    block_hash: str | None = None
    block_height: int | None = None


class FlaggedTxStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._entries: dict[str, FlaggedTx] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        for txid, entry in raw.items():
            self._entries[txid] = FlaggedTx(**entry)

    def flush(self) -> None:
        payload = {txid: asdict(entry) for txid, entry in self._entries.items()}
        atomic_replace(
            self.path,
            lambda tmp: tmp.write_text(json.dumps(payload, indent=1, sort_keys=True), encoding="utf-8"),
        )

    def __contains__(self, txid: str) -> bool:
        return txid in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def get(self, txid: str) -> FlaggedTx | None:
        return self._entries.get(txid)

    def all(self) -> list[FlaggedTx]:
        return list(self._entries.values())

    def put(self, entry: FlaggedTx) -> None:
        self._entries[entry.txid] = entry
        self.flush()

    def remove(self, txid: str) -> None:
        if self._entries.pop(txid, None) is not None:
            self.flush()
