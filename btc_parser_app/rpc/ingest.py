"""The one and only RPC block parser - implements both "catch up" and
"steady-state tip-following" from Script_plan.md's rpc_ingest design,
including the "RPC Parser Reorg Handling" section, as a single long-running
process (meant to be started once - see ../start.sh - and left running).

There is no separate "backfill" mode: `run_rpc_ingest` behaves identically
regardless of how many blocks are already parsed.

Two directories, per rpc.output_dir/rpc.state_dir in config.py: state_dir
holds everything internal (current.csv, latest.csv, index/, block_status.csv,
reorg/, the *_part_seq.csv counters, and - while there's backlog - the
currently-still-growing blocks/transactions part); output_dir holds only
complete, Splunk-safe blocks/transactions parts (see rpc/part_writer.py) - a
part appears there the instant it's done and never before, so a Splunk
`batch` (consume-and-delete) input pointed at output_dir needs nothing
excluded, ever.

- Nothing parsed yet (no current.csv, no blocks/blocks.csv anywhere)?
  Starts at height 0 and catches up to the tip.
- Some blocks already parsed (current.csv present, or just blocks/blocks.csv
  left over under state_dir/output_dir from an earlier/interrupted run)?
  Resumes exactly where it left off.
- Caught up? Falls back to polling every `rpc.poll_interval_seconds` and
  keeps following the tip. Once a pass's backlog is within
  `rpc.reorg_confirmations`, blocks/transactions are written one already-
  complete part per block, straight into output_dir, instead of
  accumulating into a shared growing part under state_dir - so nothing
  about the Splunk input needs to change for steady-state tip-following the
  way it would with one ever-growing file.
- A reorg, however deep, is just something the normal loop detects and
  recovers from on its own (see _recover_from_reorg) - it never needs
  special-casing by the caller or a separate run mode.

Loop, per pass:

1. Resolve the node tip and set `latest = tip - rpc.reorg_confirmations`.
2. Read the stored `current` pointer (state_dir/current.csv), or discover
   one from whatever blocks/blocks.csv data exists (state_dir and/or
   output_dir), or fall back to genesis.
3. If the stored current block is no longer on the active chain, walk
   backwards via previousblockhash (state_dir/index/index.csv) until
   finding the common ancestor, mark the detached range non-canonical in
   block_status.csv, and log the event under state_dir/reorg/.
4. Parse forward from the (possibly rewound) current height up to `latest`,
   skipping any height whose exact blockhash is already in index/ (so a
   block that flips canonical -> noncanonical -> canonical is never
   exported twice), and flipping block_status.csv back to canonical=true
   wherever it has an entry.

See btc_parser_app.rpc.reorg_state for the state files this reads/writes,
rpc/part_writer.py for how state_dir/output_dir's blocks/transactions parts
are written and handed off, and Script_plan.md for the full design this
implements.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from btc_parser_app.api.mining_pools_dataset import refresh_if_stale
from btc_parser_app.config import AppConfig, RpcConfig
from btc_parser_app.rpc.block_parser import aggregate_block
from btc_parser_app.rpc.client import (
    RpcCliError,
    get_block_count,
    get_block_hash,
    get_block_header,
    get_block_verbose,
)
from btc_parser_app.rpc.mining_pools import PoolMatcher, load_pool_matcher
from btc_parser_app.rpc.part_writer import PartSequencer
from btc_parser_app.rpc.reorg_state import (
    BlockStatusStore,
    IndexStore,
    read_pointer,
    seed_index_from_blocks_csv,
    write_pointer,
    write_reorg_log,
)

logger = logging.getLogger(__name__)


def _install_stop_signal() -> threading.Event:
    """SIGTERM/SIGINT set this event instead of killing the process
    mid-batch - the follow loop checks it between blocks and after each
    pass, so a `kill`/Ctrl-C (or stop.sh) always lands on a clean
    current.csv checkpoint."""
    stop = threading.Event()

    def _handle(signum: int, _frame: Any) -> None:
        logger.info("Received signal %d - stopping after the current batch...", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    return stop


def _process_height(
    rpc_config: RpcConfig,
    pool_matcher: PoolMatcher | None,
    index: IndexStore,
    block_status: BlockStatusStore,
    batch_buffers: dict[str, list[dict[str, Any]]],
    height: int,
    prev_time: int | None,
) -> tuple[str, int]:
    """Process one active-chain height: export it if its exact blockhash
    hasn't been indexed before, then (re)confirm it canonical. Returns
    (blockhash, block_time) so the caller can chain prev_time forward."""
    block_hash = get_block_hash(rpc_config, height)

    if not index.needs_export(block_hash):
        logger.debug("Block %d (%s) already indexed - skipping export.", height, block_hash)
        header = get_block_header(rpc_config, block_hash)
        block_time = int(header["time"])
    else:
        # DEBUG, not INFO: during genesis catch-up this fires once per
        # block (hundreds of thousands of lines) - the per-batch "Progress:"
        # line below already gives INFO-level visibility.
        logger.debug("Parsing block %d (%s)...", height, block_hash)
        raw_json = get_block_verbose(rpc_config, block_hash, verbosity=3)
        # Decimal preserves exact BTC amounts until conversion to satoshis.
        block = json.loads(raw_json, parse_float=Decimal)

        block_aggregate = aggregate_block(block, rpc_config, pool_matcher)
        block_event = block_aggregate.block_event
        block_time = int(block_event["time"])
        block_event["time_since_prev_block_sec"] = (
            block_time - prev_time if prev_time is not None else None
        )

        batch_buffers["blocks"].append(block_event)
        batch_buffers["transactions"].extend(block_aggregate.tx_events)
        batch_buffers["inputs"].extend(block_aggregate.input_events)
        batch_buffers["outputs"].extend(block_aggregate.output_events)
        index.add(height, block_hash, str(block.get("previousblockhash") or ""))

    block_status.set_canonical_if_present(height, block_hash, True)
    return block_hash, block_time


def _recover_from_reorg(
    rpc_config: RpcConfig,
    index: IndexStore,
    block_status: BlockStatusStore,
    reorg_dir: Path,
    current_height: int,
    current_hash: str,
) -> tuple[int, str]:
    """Walk backwards from the stale `current` pointer via previousblockhash
    until the stored chain matches the active chain at the same height (the
    common ancestor). Marks every detached block non-canonical, writes a
    single audit CSV covering both the detached and (re-verified) attached
    range, and returns (ancestor_height, ancestor_hash) for the caller to
    resume parsing from ancestor + 1."""
    logger.warning(
        "Reorg detected: stored current block %d (%s) is no longer on the active chain.",
        current_height,
        current_hash,
    )

    detached: list[tuple[int, str]] = []
    height, blockhash = current_height, current_hash
    # Populated as the walk-back below queries each candidate height's live
    # hash, so the "attached" audit loop further down can reuse those
    # already-fetched hashes instead of re-querying bitcoin-cli for the same
    # heights a second time.
    live_hash_at_height: dict[int, str] = {}

    while True:
        detached.append((height, blockhash))
        if len(detached) > rpc_config.max_reorg_depth:
            raise RuntimeError(
                f"Reorg walk-back exceeded max_reorg_depth={rpc_config.max_reorg_depth} "
                f"without finding a common ancestor (stuck near height {height}). "
                "Stopping for manual intervention."
            )

        row = index.get(blockhash)
        if row is None:
            raise RuntimeError(
                f"Reorg walk-back hit an unindexed block {blockhash} at height {height}; "
                "cannot determine common ancestor. Stopping for manual intervention."
            )
        previousblockhash = row.previousblockhash
        if height <= 0 or not previousblockhash:
            raise RuntimeError(
                f"Reorg walk-back reached height {height} with no earlier block on record; "
                "cannot determine common ancestor. Stopping for manual intervention."
            )

        candidate_height = height - 1
        live_hash_at_candidate = get_block_hash(rpc_config, candidate_height)
        live_hash_at_height[candidate_height] = live_hash_at_candidate
        if live_hash_at_candidate == previousblockhash:
            ancestor_height, ancestor_hash = candidate_height, live_hash_at_candidate
            break

        height, blockhash = candidate_height, previousblockhash

    lowest = ancestor_height + 1
    highest = current_height
    logger.warning(
        "Common ancestor found at height %d (%s). Detached range: %d-%d.",
        ancestor_height,
        ancestor_hash,
        lowest,
        highest,
    )

    log_rows: list[dict[str, Any]] = []
    for h, bh in reversed(detached):
        block_status.set_canonical(h, bh, False)
        log_rows.append({"action": "detached", "height": h, "blockhash": bh})

    # Audit-log only: record what the live chain resolves to for the
    # reattached range. Deliberately NOT calling
    # block_status.set_canonical_if_present() here - the caller resumes the
    # normal loop from ancestor_height + 1 right after this returns, and
    # _process_height() already calls set_canonical_if_present() for every
    # height it processes with a hash it fetches fresh at that time. Mutating
    # block_status here too, from a hash fetched now, raced that later write:
    # if a second reorg landed on one of these heights in between, this loop
    # could mark a since-orphaned block canonical=True and nothing would ever
    # correct it, since set_canonical_if_present() only updates the exact
    # (height, blockhash) key it's given.
    for h in range(lowest, highest + 1):
        live_hash = live_hash_at_height.get(h) or get_block_hash(rpc_config, h)
        log_rows.append({"action": "attached", "height": h, "blockhash": live_hash})

    block_status.flush()
    write_reorg_log(reorg_dir, lowest, highest, log_rows)

    return ancestor_height, ancestor_hash


def _drain(
    batch_buffers: dict[str, list[dict[str, Any]]],
    sequencers: dict[str, PartSequencer],
    atomic: bool,
) -> None:
    """Write out whatever's buffered and clear the buffers. atomic=True
    writes each as a brand-new, complete part (see PartSequencer.write_atomic
    - used once caught up to the tip, one block at a time); atomic=False
    appends to the current rotating part (used while there's backlog)."""
    for key, seq in sequencers.items():
        rows = batch_buffers[key]
        if atomic:
            seq.write_atomic(rows)
        else:
            seq.write_batched(rows)
        rows.clear()


def _run_one_pass(
    rpc_config: RpcConfig,
    pool_matcher: PoolMatcher | None,
    index: IndexStore,
    block_status: BlockStatusStore,
    current_path: Path,
    latest_path: Path,
    reorg_dir: Path,
    sequencers: dict[str, PartSequencer],
    stop: threading.Event,
) -> bool:
    """Run one tip-check-and-catch-up pass. Returns True if the loop should
    sleep before the next pass (nothing left to do / genuinely caught up to
    latest), False if it should go again immediately (there's backlog, or a
    stop was requested mid-pass and progress should be checkpointed)."""
    blocks_seq = sequencers["blocks"]
    tip = get_block_count(rpc_config)
    latest_height = tip - rpc_config.reorg_confirmations
    if latest_height < 0:
        logger.info(
            "Node tip (%d) is shallower than reorg_confirmations (%d); nothing to do yet.",
            tip,
            rpc_config.reorg_confirmations,
        )
        return True

    latest_hash = get_block_hash(rpc_config, latest_height)
    write_pointer(latest_path, latest_height, latest_hash)

    current = read_pointer(current_path)
    if current is None:
        current = seed_index_from_blocks_csv(index, blocks_seq.state_base, blocks_seq.export_base)
        if current is not None:
            logger.info(
                "No current.csv found, but %s/%s has data - resuming from height %d.",
                blocks_seq.state_base,
                blocks_seq.export_base,
                current[0],
            )

    if current is None:
        current_height, current_hash = -1, None
        logger.info(
            "No prior state found anywhere - starting fresh from genesis (height 0), "
            "tip %d, latest %d.",
            tip,
            latest_height,
        )
    else:
        current_height, current_hash = current
        live_hash = get_block_hash(rpc_config, current_height)
        if live_hash != current_hash:
            current_height, current_hash = _recover_from_reorg(
                rpc_config, index, block_status, reorg_dir, current_height, current_hash
            )

    if current_height >= latest_height:
        write_pointer(current_path, current_height, current_hash)
        block_status.flush()
        return True

    prev_time = (
        int(get_block_header(rpc_config, current_hash)["time"])
        if current_hash is not None
        else None
    )

    # Once the backlog for this pass is within reorg_confirmations, we're
    # effectively caught up to the tip - switch to writing one atomic,
    # already-complete part per block (see rpc/part_writer.py) instead of
    # accumulating into a shared growing part. Reusing reorg_confirmations
    # rather than a separate knob: it's already the app's existing "how far
    # behind counts as caught up" threshold (latest_height itself is
    # tip - reorg_confirmations), so a backlog no bigger than that is at most
    # a block or two of ordinary tip-following jitter, not a real backlog.
    # max(..., 1): steady-state tip-following always has a backlog of at
    # least 1 (one new block since the last pass), so reorg_confirmations=0
    # must not be read as "never caught up" - it should still mean "no
    # confirmation margin", not "permanently stuck in batched mode".
    atomic_mode = (latest_height - current_height) <= max(rpc_config.reorg_confirmations, 1)

    logger.info(
        "Parsing height %d through %d (%d blocks)%s...",
        current_height + 1,
        latest_height,
        latest_height - current_height,
        " - caught up, writing one part per block" if atomic_mode else "",
    )
    batch_buffers: dict[str, list[dict[str, Any]]] = {key: [] for key in sequencers}
    start_time = time.perf_counter()
    processed = 0
    last_processed_height = current_height
    last_hash = current_hash

    for height in range(current_height + 1, latest_height + 1):
        if stop.is_set():
            break

        last_hash, block_time = _process_height(
            rpc_config, pool_matcher, index, block_status, batch_buffers, height, prev_time
        )
        prev_time = block_time
        last_processed_height = height
        processed += 1

        if atomic_mode:
            _drain(batch_buffers, sequencers, atomic=True)
            index.flush()
            block_status.flush()
            write_pointer(current_path, last_processed_height, last_hash)
            logger.info("Exported block %d (%s) as its own part.", height, last_hash)
        elif processed % rpc_config.batch_size == 0:
            _drain(batch_buffers, sequencers, atomic=False)
            index.flush()
            # block_status is flushed before the pointer is advanced: its
            # entries are idempotent to redo (set_canonical_if_present() is a
            # safe no-op/overwrite on replay), so flushing it first means a
            # crash between the two can never leave current.csv pointing past
            # a height whose reattached canonical=True flip never made it to
            # disk - the worst case is instead a harmless re-check on restart.
            block_status.flush()
            write_pointer(current_path, last_processed_height, last_hash)
            elapsed = time.perf_counter() - start_time
            bps = processed / elapsed if elapsed > 0 else 0
            logger.info(
                "Progress: height %d/%d (%d behind tip), %.2f blocks/s",
                last_processed_height,
                latest_height,
                tip - last_processed_height,
                bps,
            )

    _drain(batch_buffers, sequencers, atomic=atomic_mode)
    index.flush()
    block_status.flush()
    if last_hash is not None:
        write_pointer(current_path, last_processed_height, last_hash)
    # else: stop was requested before the first block of a fresh run was
    # processed - there's no valid pointer to checkpoint yet. Leaving
    # current.csv untouched (rather than writing height=-1/hash=None) means
    # the next run just starts fresh from genesis again, instead of trying
    # to resume from an invalid pointer and crashing on getblockhash(-1).

    if processed:
        elapsed = time.perf_counter() - start_time
        bps = processed / elapsed if elapsed > 0 else 0
        logger.info(
            "Pass done: %d blocks processed (%.2f blocks/s). Current height %d.",
            processed,
            bps,
            last_processed_height,
        )

    return last_processed_height >= latest_height


def run_rpc_ingest(config: AppConfig) -> None:
    """Runs forever until SIGTERM/SIGINT: catches up to the tip (no sleep
    between passes while there's backlog), then polls every
    rpc.poll_interval_seconds once caught up."""
    rpc_config = config.rpc
    out_dir = rpc_config.output_dir
    state_dir = rpc_config.state_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    index = IndexStore(state_dir / "index" / "index.csv")
    block_status = BlockStatusStore(state_dir / "block_status.csv")
    current_path = state_dir / "current.csv"
    latest_path = state_dir / "latest.csv"
    reorg_dir = state_dir / "reorg"
    # blocks/, transactions/, inputs/ and outputs/ are the only things ever
    # written under out_dir - a part only appears there once it's complete
    # (moved in from state_dir on rotation, or written there directly once
    # caught up to the tip - see rpc/part_writer.py) - so a Splunk `batch`
    # input pointed at out_dir alone, with nothing excluded, is always safe.
    sequencers = {
        name: PartSequencer(
            state_dir / name / f"{name}.csv",
            out_dir / name / f"{name}.csv",
            state_dir / f"{name}_part_seq.csv",
        )
        for name in ("blocks", "transactions", "inputs", "outputs")
    }

    pool_matcher = load_pool_matcher(config.mining_pools_dataset.local_path)
    stop = _install_stop_signal()

    logger.info("rpc-ingest starting - output_dir=%s state_dir=%s", out_dir, state_dir)

    while not stop.is_set():
        # Cheap no-op unless mining_pools_dataset.local_path is missing or
        # older than refresh_interval_seconds (see refresh_if_stale). Without
        # this, a long-running rpc-ingest process would keep using whatever
        # pool signatures it loaded at startup for its entire lifetime, even
        # after a newer pools-v2.json is fetched (manually or on a
        # scheduler) - reload the matcher whenever a refresh actually ran.
        if refresh_if_stale(config.mining_pools_dataset):
            pool_matcher = load_pool_matcher(config.mining_pools_dataset.local_path)

        try:
            caught_up = _run_one_pass(
                rpc_config,
                pool_matcher,
                index,
                block_status,
                current_path,
                latest_path,
                reorg_dir,
                sequencers,
                stop,
            )
        except RpcCliError as exc:
            # A sustained RPC outage (node down/restarting, network
            # partition) - run_cli already retried rpc.max_cli_retries
            # times per call. Back off and try the whole pass again next
            # cycle instead of crashing the daemon; any progress already
            # checkpointed to current.csv is unaffected.
            logger.error(
                "RPC unreachable after retries (%s); backing off %.0fs before retrying.",
                exc,
                rpc_config.poll_interval_seconds,
            )
            stop.wait(timeout=rpc_config.poll_interval_seconds)
            continue

        if stop.is_set():
            break
        if caught_up:
            stop.wait(timeout=rpc_config.poll_interval_seconds)

    logger.info("rpc-ingest stopped.")
