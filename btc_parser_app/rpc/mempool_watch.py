"""Sanctioned-address mempool watcher ("mempool-watch" command).

Watches this node's mempool for transactions that pay to or spend from an
address on the sanctions list (mempool_watch.sanctions_list_paths, see
rpc/sanctions_list.py) and exports one Splunk event per state change of each
such transaction - without ever exporting the mempool itself. Long-running,
same shape as run_stale_blocks_ingest: runs forever, SIGTERM/SIGINT stop it
cleanly between batches.

Detection sources:
  - ZMQ `sequence` (mempool_watch.zmq_endpoint, bitcoin.conf
    zmqpubsequence=...) - pushes A/R/C/D events as they happen (see
    rpc/zmq_sequence.py). This is the primary, near-real-time path.
  - Reconcile pass - a full getrawmempool diff at startup and every
    mempool_watch.reconcile_interval_seconds. Catches anything that
    arrived while the service was down, ZMQ messages that were dropped,
    and a sanctions list that changed on disk. With zmq_endpoint unset it's
    the only path (polling mode).

For every tx not checked before, `getrawtransaction <txid> 2` gives both
sides of the tx: each output's address (who receives) and each input's
prevout address (who pays) - the latter needs Bitcoin Core 25+, older nodes
fall back to one gettxout/getrawtransaction per input (_prevout).

Exported events (mempool_watch.output_dir/sanctioned_tx_events.csv, one row
per matched input/output of the tx, so every row is self-contained for an
alert):
  seen         first time the tx is observed in the mempool
  confirmed    mined - block_height/block_hash set
  unconfirmed  its block was reorged out (the tx is tracked again)
  replaced     left the mempool because another tx spends the same
               input(s) (RBF/conflict) - replaced_by_txid set. If the
               replacement also touches a listed address, it gets its own
               `seen` event.
  removed      left the mempool without being mined for any other reason
               (evicted, expired, conflicted by a block, or it vanished
               while ZMQ events were missed and no confirming block was
               found in the last max_confirmation_scan_blocks blocks)

Only the "exactly once" bookkeeping for flagged txs is persisted (see
rpc/mempool_watch_state.py). The set of txids already checked and found
clean is in memory only, so every restart re-checks the whole mempool once
(in parallel, mempool_watch.rpc_workers). A crash between an export row
being written and the state flush can repeat that one event after restart -
the same "rare duplicate over a lost event" tradeoff stale_blocks.py makes.

Out of scope on purpose: a tx that never passes through this node's
mempool (mined directly/privately) - that's caught on the confirmed-block
side in Splunk instead.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Any, Iterable

from btc_parser_app.common.csv_writer import write_rows_to_csv
from btc_parser_app.common.stop_signal import install_stop_signal
from btc_parser_app.config import AppConfig, MempoolWatchConfig, RpcConfig
from btc_parser_app.rpc.block_parser import btc_to_sats
from btc_parser_app.rpc.client import (
    RpcCliError,
    RpcError,
    get_block_count,
    get_block_hash,
    get_block_header,
    get_block_txids,
    get_network_info,
    get_raw_mempool_txids,
    get_raw_transaction,
    get_tx_out,
    get_tx_spending_prevout,
)
from btc_parser_app.rpc.mempool_watch_state import (
    CONFIRMED,
    IN_MEMPOOL,
    FlaggedTx,
    FlaggedTxStore,
)
from btc_parser_app.rpc.sanctions_list import SanctionsList
from btc_parser_app.rpc.stale_blocks_state import now_iso
from btc_parser_app.rpc.zmq_sequence import SequenceEvent, ZmqSequenceSubscriber

logger = logging.getLogger(__name__)

EVENTS_FILE = "sanctioned_tx_events.csv"
# Fixed column order for every exported row. Rows are appended to an
# existing file without re-writing its header, so the order must never
# depend on dict insertion order (a match dict reloaded from flagged.json
# comes back with its keys sorted).
EVENT_COLUMNS = (
    "observed_at",
    "event",
    "txid",
    "direction",
    "io_index",
    "address",
    "value_sats",
    "value_btc",
    "name",
    "first_name",
    "sanctions_programs",
    "list_file",
    "tx_fee_btc",
    "tx_vsize",
    "first_seen_at",
    "block_height",
    "block_hash",
    "replaced_by_txid",
    "source",
    "detail",
)
MIN_PREVOUT_NODE_VERSION = 250000  # Bitcoin Core 25.0 - getrawtransaction verbosity 2
RPC_ERROR_BACKOFF_SECONDS = 30
# Reconcile fetches in chunks this size so SIGTERM during a long startup
# scan is honoured within seconds, not after the whole mempool is checked.
RECONCILE_CHUNK = 500


def _btc_str(value: Any) -> str | None:
    if value is None:
        return None
    return format(Decimal(str(value)), "f")


# =============================================================================
# Matching a decoded tx against the list
# =============================================================================


def _prevout(rpc: RpcConfig, vin: dict[str, Any]) -> dict[str, Any] | None:
    """The output a tx input spends: {scriptPubKey: {address}, value}.
    Straight from `vin.prevout` on Core 25+; older nodes need a lookup -
    the chain UTXO set first (include_mempool=false, so an output this very
    tx spends still shows up), then the parent tx itself if the parent is
    unconfirmed and therefore still in the mempool."""
    if "prevout" in vin:
        return vin["prevout"]
    try:
        out = get_tx_out(rpc, vin["txid"], int(vin["vout"]), include_mempool=False)
        if out is not None:
            return out
        parent = get_raw_transaction(rpc, vin["txid"])
        return parent["vout"][int(vin["vout"])]
    except (RpcError, IndexError, KeyError):
        return None


def _script_address(script_pub_key: dict[str, Any]) -> str | None:
    """`address` on Core 22+; older nodes only have an `addresses` list
    (a single entry for every standard single-address script)."""
    if "address" in script_pub_key:
        return script_pub_key["address"]
    addresses = script_pub_key.get("addresses") or []
    return addresses[0] if len(addresses) == 1 else None


def match_tx(rpc: RpcConfig, sanctions: SanctionsList, tx: dict[str, Any]) -> list[dict[str, Any]]:
    """One dict per input/output of `tx` whose address is on the list."""
    matches: list[dict[str, Any]] = []

    for index, vin in enumerate(tx.get("vin", [])):
        if "coinbase" in vin:
            continue
        prevout = _prevout(rpc, vin)
        if prevout is None:
            continue
        address = _script_address(prevout.get("scriptPubKey", {}))
        entry = sanctions.lookup(address)
        if entry is not None:
            matches.append(_match_row("input", index, address, prevout.get("value"), entry))

    for vout in tx.get("vout", []):
        address = _script_address(vout.get("scriptPubKey", {}))
        entry = sanctions.lookup(address)
        if entry is not None:
            matches.append(_match_row("output", int(vout["n"]), address, vout.get("value"), entry))

    return matches


def _match_row(direction: str, index: int, address: str, value: Any, entry: Any) -> dict[str, Any]:
    return {
        "direction": direction,
        "io_index": index,
        "address": address,
        "value_sats": btc_to_sats(value),
        "value_btc": _btc_str(value),
        "name": entry.name,
        "first_name": entry.first_name,
        "sanctions_programs": entry.sanctions_programs,
        "list_file": entry.list_file,
    }


# =============================================================================
# Splunk-facing export
# =============================================================================


def _export(
    config: MempoolWatchConfig,
    event: str,
    tx: FlaggedTx,
    *,
    source: str,
    replaced_by_txid: str | None = None,
    detail: str | None = None,
) -> None:
    common = {
        "observed_at": now_iso(),
        "event": event,
        "txid": tx.txid,
        "tx_fee_btc": tx.fee_btc,
        "tx_vsize": tx.vsize,
        "first_seen_at": tx.first_seen_at,
        "block_height": tx.block_height,
        "block_hash": tx.block_hash,
        "replaced_by_txid": replaced_by_txid,
        "source": source,
        "detail": detail,
    }
    rows = [
        {column: {**match, **common}.get(column) for column in EVENT_COLUMNS}
        for match in tx.matches
    ]
    write_rows_to_csv(rows, config.output_dir / EVENTS_FILE)

    summary = ", ".join(
        f"{m['direction']}[{m['io_index']}] {m['address']} ({m['name']}"
        f"{', ' + m['first_name'] if m['first_name'] else ''}; {m['sanctions_programs']})"
        for m in tx.matches
    )
    log = logger.warning if event == "seen" else logger.info
    log("SANCTIONED %s txid=%s source=%s - %s", event.upper(), tx.txid, source, summary)


# =============================================================================
# Watcher
# =============================================================================


class MempoolWatcher:
    def __init__(self, config: AppConfig, stop: threading.Event | None = None) -> None:
        self.stop = stop or threading.Event()
        self.rpc = config.rpc
        self.cfg = config.mempool_watch
        self.sanctions = SanctionsList(self.cfg.sanctions_list_paths)
        self.store = FlaggedTxStore(self.cfg.state_dir / "flagged.json")
        self.pool = ThreadPoolExecutor(max_workers=self.cfg.rpc_workers, thread_name_prefix="mw-rpc")
        # txids currently in the mempool that have already been checked
        # (clean or flagged) - in memory only, see module docstring.
        self.checked: set[str] = set()
        # txids from ZMQ `A` events that were already gone again by the time
        # they were fetched. If one of them was mined, the C event for its
        # block re-fetches it by blockhash (see _on_block_connected) - without
        # -txindex that's the only way to still check it.
        self.unfetched: set[str] = set()
        self.tip_height = 0

    # --- fetching -----------------------------------------------------------

    def _fetch_and_match(
        self, txid: str, blockhash: str | None = None
    ) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
        try:
            tx = get_raw_transaction(self.rpc, txid, blockhash)
        except RpcError:
            return None  # already left the mempool again - nothing to check
        return tx, match_tx(self.rpc, self.sanctions, tx)

    def _fetch_many(self, txids: Iterable[str]) -> dict[str, tuple[dict[str, Any], list[dict[str, Any]]] | None]:
        txids = list(txids)
        return dict(zip(txids, self.pool.map(self._fetch_and_match, txids)))

    # --- per-event handlers -------------------------------------------------

    def _on_added(self, txid: str, fetched: tuple[dict[str, Any], list[dict[str, Any]]] | None, source: str) -> None:
        if fetched is None:
            return
        self.checked.add(txid)
        tx, matches = fetched
        if not matches or txid in self.store:
            return
        entry = FlaggedTx(
            txid=txid,
            first_seen_at=now_iso(),
            first_seen_height=self.tip_height,
            fee_btc=_btc_str(tx.get("fee")),
            vsize=tx.get("vsize"),
            inputs=[[vin["txid"], int(vin["vout"])] for vin in tx.get("vin", []) if "coinbase" not in vin],
            matches=matches,
        )
        _export(self.cfg, "seen", entry, source=source)
        self.store.put(entry)

    def _on_left_unmined(self, entry: FlaggedTx, source: str, detail: str) -> None:
        """A tracked tx left the mempool without being mined - replaced
        (something else in the mempool now spends one of its inputs) or
        removed (anything else)."""
        replaced_by = None
        if entry.inputs:
            try:
                spends = get_tx_spending_prevout(
                    self.rpc, [{"txid": t, "vout": v} for t, v in entry.inputs]
                )
                replaced_by = next(
                    (s["spendingtxid"] for s in spends if s.get("spendingtxid") not in (None, entry.txid)),
                    None,
                )
            except RpcError as exc:
                logger.debug("gettxspendingprevout failed for %s: %s", entry.txid, exc)

        if replaced_by:
            _export(self.cfg, "replaced", entry, source=source, replaced_by_txid=replaced_by)
        else:
            _export(self.cfg, "removed", entry, source=source, detail=detail)
        self.store.remove(entry.txid)

    def _on_removed(self, txid: str) -> None:
        self.checked.discard(txid)
        entry = self.store.get(txid)
        if entry is not None and entry.status == IN_MEMPOOL:
            self._on_left_unmined(entry, source="zmq", detail="removed_from_mempool")

    def _mark_confirmed(self, entry: FlaggedTx, block_hash: str, height: int, source: str) -> None:
        entry.status = CONFIRMED
        entry.block_hash = block_hash
        entry.block_height = height
        _export(self.cfg, "confirmed", entry, source=source)
        self.store.put(entry)

    def _on_block_connected(self, block_hash: str) -> None:
        height, _confirmations, txids = get_block_txids(self.rpc, block_hash)
        self.tip_height = max(self.tip_height, height)
        in_block = set(txids)
        for txid in self.unfetched & in_block:
            self._on_added(txid, self._fetch_and_match(txid, block_hash), source="zmq")
        self.unfetched -= in_block
        self.checked -= in_block
        for entry in self.store.all():
            if entry.status == IN_MEMPOOL and entry.txid in in_block:
                self._mark_confirmed(entry, block_hash, height, source="zmq")
        self._check_confirmed_depth()

    def _check_confirmed_depth(self) -> None:
        """For every tx tracked as confirmed: emit `unconfirmed` if its
        block has left the active chain (reorg), or stop tracking it once
        it's forget_after_confirmations deep."""
        for entry in self.store.all():
            if entry.status != CONFIRMED or entry.block_hash is None:
                continue
            confirmations = int(get_block_header(self.rpc, entry.block_hash).get("confirmations", -1))
            if confirmations == -1:
                old_hash, old_height = entry.block_hash, entry.block_height
                entry.status = IN_MEMPOOL
                _export(
                    self.cfg,
                    "unconfirmed",
                    entry,
                    source="reorg",
                    detail=f"block {old_height} {old_hash} left the active chain",
                )
                entry.block_hash = None
                entry.block_height = None
                self.store.put(entry)
            elif confirmations >= self.cfg.forget_after_confirmations:
                self.store.remove(entry.txid)

    def handle_events(self, events: list[SequenceEvent]) -> None:
        # Fetch every newly added tx of this batch in parallel up front,
        # then apply all events strictly in order.
        added = {e.hash_hex for e in events if e.label == "A" and e.hash_hex not in self.checked}
        fetched = self._fetch_many(added)

        for event in events:
            if event.label == "A":
                if event.hash_hex not in fetched:
                    continue
                if fetched[event.hash_hex] is None:
                    self.unfetched.add(event.hash_hex)
                else:
                    self._on_added(event.hash_hex, fetched[event.hash_hex], source="zmq")
            elif event.label == "R":
                self.unfetched.discard(event.hash_hex)
                self._on_removed(event.hash_hex)
            elif event.label == "C":
                self._on_block_connected(event.hash_hex)
            elif event.label == "D":
                logger.info("Block %s disconnected (reorg).", event.hash_hex)
                self._check_confirmed_depth()

    # --- reconcile ----------------------------------------------------------

    def _find_confirming_block(self, entry: FlaggedTx, cache: dict[int, tuple[str, set[str]]]) -> tuple[str, int] | None:
        lowest = max(entry.first_seen_height, self.tip_height - self.cfg.max_confirmation_scan_blocks + 1, 0)
        for height in range(self.tip_height, lowest - 1, -1):
            if height not in cache:
                block_hash = get_block_hash(self.rpc, height)
                cache[height] = (block_hash, set(get_block_txids(self.rpc, block_hash)[2]))
            block_hash, txids = cache[height]
            if entry.txid in txids:
                return block_hash, height
        return None

    def reconcile(self, source: str) -> None:
        """Full getrawmempool diff - see module docstring."""
        started = time.monotonic()
        # Mempool first, tip second: a tracked tx missing from the snapshot
        # was then mined (if at all) at or below the tip read afterwards, so
        # _find_confirming_block's scan always covers it.
        mempool = set(get_raw_mempool_txids(self.rpc))
        self.tip_height = get_block_count(self.rpc)

        new = list(mempool - self.checked)
        self.checked &= mempool
        if new:
            logger.info("%s: checking %d of %d mempool tx(s)...", source, len(new), len(mempool))
        for start in range(0, len(new), RECONCILE_CHUNK):
            if self.stop.is_set():
                return
            if start and start % 10_000 == 0:
                logger.info("%s: %d/%d checked...", source, start, len(new))
            for txid, fetched in self._fetch_many(new[start : start + RECONCILE_CHUNK]).items():
                self._on_added(txid, fetched, source=source)

        block_cache: dict[int, tuple[str, set[str]]] = {}
        for entry in self.store.all():
            if entry.status != IN_MEMPOOL or entry.txid in mempool:
                continue
            found = self._find_confirming_block(entry, block_cache)
            if found is not None:
                self._mark_confirmed(entry, found[0], found[1], source=source)
            else:
                self._on_left_unmined(entry, source=source, detail="left_mempool_unobserved")

        self._check_confirmed_depth()
        logger.info(
            "%s done in %.0fs: mempool=%d tx(s), newly checked=%d, tracking %d flagged tx(s), list=%d address(es).",
            source,
            time.monotonic() - started,
            len(mempool),
            len(new),
            len(self.store),
            len(self.sanctions),
        )

    def close(self) -> None:
        self.pool.shutdown(wait=False, cancel_futures=True)


# =============================================================================
# Main loop
# =============================================================================


def _warn_if_old_node(rpc: RpcConfig) -> None:
    version = int(get_network_info(rpc).get("version", 0))
    if version < MIN_PREVOUT_NODE_VERSION:
        logger.warning(
            "Bitcoin Core %d < 25.0: getrawtransaction has no input prevouts, "
            "falling back to one extra RPC call per input - expect a much slower "
            "startup scan.",
            version,
        )


def run_mempool_watch(config: AppConfig) -> None:
    cfg = config.mempool_watch
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    cfg.state_dir.mkdir(parents=True, exist_ok=True)

    stop = install_stop_signal()
    watcher = MempoolWatcher(config, stop)
    logger.info(
        "mempool-watch starting - mode=%s output_dir=%s state_dir=%s, tracking %d flagged tx(s) from state.",
        f"zmq ({cfg.zmq_endpoint})" if cfg.zmq_endpoint else "polling-only",
        cfg.output_dir,
        cfg.state_dir,
        len(watcher.store),
    )

    # Subscribe BEFORE the startup scan, so nothing that happens during the
    # (possibly minutes-long) scan is missed - it queues up on the socket.
    subscriber = ZmqSequenceSubscriber(cfg.zmq_endpoint) if cfg.zmq_endpoint else None

    reconcile_due = True
    reconcile_source = "startup_scan"
    next_reconcile = 0.0

    try:
        while not stop.is_set():
            try:
                if reconcile_due:
                    if reconcile_source == "startup_scan":
                        _warn_if_old_node(config.rpc)
                    watcher.reconcile(source=reconcile_source)
                    reconcile_due = False
                    reconcile_source = "reconcile"
                    next_reconcile = time.monotonic() + cfg.reconcile_interval_seconds

                if subscriber is not None:
                    events = subscriber.receive_batch(timeout_seconds=1.0)
                    if events:
                        watcher.handle_events(events)
                    if subscriber.take_gap():
                        reconcile_due = True
                else:
                    stop.wait(timeout=1.0)

                if watcher.sanctions.reload_if_changed():
                    # New/changed addresses - everything already checked
                    # has to be checked again against the new list.
                    watcher.checked.clear()
                    reconcile_due = True

                if time.monotonic() >= next_reconcile:
                    reconcile_due = True
                    if subscriber is not None and time.monotonic() - subscriber.last_message_at > cfg.reconcile_interval_seconds:
                        logger.warning(
                            "No ZMQ message from %s in %.0fs - is zmqpubsequence set in bitcoin.conf, "
                            "and is the port reachable from here? Relying on reconcile polling meanwhile.",
                            cfg.zmq_endpoint,
                            time.monotonic() - subscriber.last_message_at,
                        )
            except RpcCliError as exc:
                logger.error(
                    "RPC unreachable after retries (%s); backing off %ds, then reconciling.",
                    exc,
                    RPC_ERROR_BACKOFF_SECONDS,
                )
                reconcile_due = True
                stop.wait(timeout=RPC_ERROR_BACKOFF_SECONDS)
    finally:
        watcher.close()
        if subscriber is not None:
            subscriber.close()

    logger.info("mempool-watch stopped.")


# =============================================================================
# One-off check ("mempool-watch-check" command)
# =============================================================================


def check_single_tx(config: AppConfig, txid: str, blockhash: str | None) -> list[dict[str, Any]]:
    """Run the matcher against one tx (mempool, or confirmed with its
    blockhash) without touching state or export - for verifying the list
    and matching logic against a known historic tx."""
    sanctions = SanctionsList(config.mempool_watch.sanctions_list_paths)
    tx = get_raw_transaction(config.rpc, txid, blockhash)
    return match_tx(config.rpc, sanctions, tx)
