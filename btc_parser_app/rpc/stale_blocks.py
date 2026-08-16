"""Stale/orphaned chain-tip header pipeline - the "reorg intel" dataset
alongside the main-chain rpc-ingest pipeline. Long-running process (one
command, `stale-blocks-ingest`), same shape as run_rpc_ingest/run_poller:
runs forever, SIGTERM/SIGINT stop cleanly between passes.

Headers only, deliberately - no transaction data, no full block bodies, for
anyone. This pipeline used to also try to pull full block/tx data (via
getblockfrompeer for tips this node's own peers had, and via submitheader/
submitblock to import bitcoin-data/stale-blocks' full-block blobs), but that
turned out to produce a lumpy, inconsistent dataset in production: Bitcoin
Core's checkpoint mechanism categorically rejects any competing header or
block at or below its highest hardcoded checkpoint - confirmed here that
this covers the overwhelming majority of the GitHub dataset, right up to
within ~1000 blocks of the live tip - so only a small, essentially random
subset of entries could ever reach "complete" while the rest stayed stuck.
Header-only avoids that wall entirely (both sources already hand us raw
header bytes, parsed offline via common.block_header - no node involvement
needed to use the GitHub dataset at all) and makes every entry the same
shape, regardless of source or age.

Two passes per wake, sharing one StaleBlockRegistry:

1. Node poll (every wake, cadence = stale_blocks.node_poll_interval_seconds,
   default hourly): getchaintips, filtered to non-active/non-invalid tips,
   then getblockheader for each to get its raw header bytes.

2. GitHub pull (cadence = stale_blocks.github.poll_interval_seconds,
   default daily): pulls bitcoin-data/stale-blocks' header CSV. Every
   header is independently re-validated (double-SHA256, see
   common.block_header) before being trusted - never assumed correct just
   because it came from the dataset. Already-known hashes are skipped so a
   daily pull only costs work for genuinely new rows, not the whole
   multi-thousand-row history every time.

Every event (a hash first sighted, a header becoming available) is
exported to Splunk exactly once - StaleBlockRegistry.last_exported_status
gates this, the same "have I already exported this" pattern
reorg_state.IndexStore uses for the main chain. A later status upgrade is a
new, additional event - nothing here ever mutates or re-emits a
previously-exported row.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from pathlib import Path
from typing import Any

from btc_parser_app.common.block_header import parse_header, validate_header_hash
from btc_parser_app.common.csv_writer import write_rows_to_csv
from btc_parser_app.config import AppConfig, RpcConfig, StaleBlocksGithubConfig
from btc_parser_app.rpc.client import (
    RpcCliError,
    get_block_header_raw,
    get_chain_tips,
)
from btc_parser_app.rpc.stale_blocks_github import (
    GithubFetchError,
    fetch_stale_blocks_csv,
)
from btc_parser_app.rpc.stale_blocks_state import (
    HEADER_ONLY,
    UNUSABLE,
    StaleBlockEntry,
    StaleBlockRegistry,
    _now_iso,
)

logger = logging.getLogger(__name__)

_ACTIVE_OR_INVALID = {"active", "invalid"}
_EMPTY_HEADER_FIELDS = {
    "version": None,
    "previousblockhash": None,
    "merkleroot": None,
    "time": None,
    "bits": None,
    "nonce": None,
}


def _install_stop_signal() -> threading.Event:
    stop = threading.Event()

    def _handle(signum: int, _frame: Any) -> None:
        logger.info("Received signal %d - stopping after the current pass...", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    return stop


# =============================================================================
# Splunk-facing export (out_dir) - append-only, one row per new fact learned
# =============================================================================


def _export_header_event(out_dir: Path, entry: StaleBlockEntry) -> None:
    fields: dict[str, object] = dict(_EMPTY_HEADER_FIELDS)
    if entry.header_hex:
        try:
            fields = parse_header(entry.header_hex)
        except ValueError:
            pass

    write_rows_to_csv(
        [
            {
                "observed_at": _now_iso(),
                "height": entry.height,
                "hash": entry.blockhash,
                "status": entry.status,
                "header_hex": entry.header_hex,
                "header_valid": entry.header_valid,
                "source": entry.source,
                "chaintip_status": entry.chaintip_status,
                "branchlen": entry.branchlen,
                **fields,
            }
        ],
        out_dir / "stale_block_headers.csv",
    )


def _ingest_sighting(
    registry: StaleBlockRegistry,
    out_dir: Path,
    *,
    height: int,
    blockhash: str,
    header_hex: str | None,
    source: str,
    chaintip_status: str | None,
    branchlen: int | None,
) -> StaleBlockEntry:
    header_valid = validate_header_hash(header_hex, blockhash) if header_hex else None
    if header_hex and not header_valid:
        logger.warning(
            "Header for %s at height %d does not hash to the claimed value - "
            "discarding it (source=%s).",
            blockhash,
            height,
            source,
        )

    status = HEADER_ONLY if header_valid else UNUSABLE

    entry = registry.upsert_sighting(
        height=height,
        blockhash=blockhash,
        status=status,
        header_hex=header_hex if header_valid else None,
        header_valid=header_valid,
        source=source,
        chaintip_status=chaintip_status,
        branchlen=branchlen,
    )

    if entry.status != entry.last_exported_status:
        _export_header_event(out_dir, entry)
        registry.mark_header_status_exported(blockhash, entry.status)
        # Persist last_exported_status immediately, not just at the end of
        # the caller's loop over all tips/rows: the header event above is
        # already durable on disk, so a crash before a trailing flush() would
        # otherwise leave registry.csv unaware of it and re-export the same
        # event next pass. flush() itself is a cheap no-op unless dirty.
        registry.flush()

    return entry


# =============================================================================
# Pass 1: node poll
# =============================================================================


def _run_node_poll(rpc_config: RpcConfig, registry: StaleBlockRegistry, out_dir: Path) -> None:
    tips = get_chain_tips(rpc_config)
    stale_tips = [t for t in tips if t.get("status") not in _ACTIVE_OR_INVALID]
    logger.info("getchaintips: %d non-active, non-invalid tip(s).", len(stale_tips))

    for tip in stale_tips:
        blockhash = str(tip["hash"])
        height = int(tip["height"])
        status = str(tip["status"])
        branchlen = int(tip.get("branchlen") or 0)

        header_hex = None
        try:
            header_hex = get_block_header_raw(rpc_config, blockhash)
        except RpcCliError as exc:
            logger.warning("Could not fetch header for tip %s: %s", blockhash, exc)

        _ingest_sighting(
            registry,
            out_dir,
            height=height,
            blockhash=blockhash,
            header_hex=header_hex,
            source="own_node",
            chaintip_status=status,
            branchlen=branchlen,
        )

    registry.flush()


# =============================================================================
# Pass 2: GitHub pull
# =============================================================================


def _run_github_pull(
    github_config: StaleBlocksGithubConfig,
    timeout_seconds: float,
    registry: StaleBlockRegistry,
    out_dir: Path,
) -> bool:
    """Returns True if the pull completed (even if it found nothing new),
    False if it failed - callers should use that to decide whether to
    advance their "last pull" timer, so a transient failure gets retried on
    the next wake instead of being silently deferred a full cycle."""
    try:
        header_rows = fetch_stale_blocks_csv(github_config, timeout_seconds)
    except GithubFetchError as exc:
        logger.error("GitHub stale-blocks.csv pull failed: %s", exc)
        return False

    logger.info("GitHub dataset: %d header row(s).", len(header_rows))

    for row in header_rows:
        existing = registry.get(row.blockhash)
        if existing is not None and existing.status == HEADER_ONLY:
            continue  # already have a valid header for this hash

        if not validate_header_hash(row.header_hex, row.blockhash):
            logger.warning(
                "GitHub CSV row for %s at height %d fails hash validation - skipping.",
                row.blockhash,
                row.height,
            )
            continue

        _ingest_sighting(
            registry,
            out_dir,
            height=row.height,
            blockhash=row.blockhash,
            header_hex=row.header_hex,
            source="github",
            chaintip_status=None,
            branchlen=None,
        )

    registry.flush()
    return True


# =============================================================================
# Main loop
# =============================================================================


def run_stale_blocks_ingest(config: AppConfig) -> None:
    """Runs forever until SIGTERM/SIGINT, waking every
    stale_blocks.node_poll_interval_seconds to run the node poll, and
    additionally running the GitHub pull whenever
    stale_blocks.github.poll_interval_seconds has elapsed since the last
    one."""
    stale_config = config.stale_blocks
    out_dir = stale_config.output_dir
    state_dir = stale_config.state_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    registry = StaleBlockRegistry(state_dir / "registry.csv")
    stop = _install_stop_signal()

    last_github_pull = 0.0

    logger.info(
        "stale-blocks-ingest starting - output_dir=%s state_dir=%s", out_dir, state_dir
    )

    while not stop.is_set():
        try:
            _run_node_poll(config.rpc, registry, out_dir)
        except RpcCliError as exc:
            logger.error(
                "RPC unreachable after retries (%s); backing off %.0fs before retrying.",
                exc,
                stale_config.node_poll_interval_seconds,
            )

        # Outside the RPC try/except on purpose: the GitHub pull has no
        # bitcoin-cli/RPC dependency at all, so a node-poll outage must not
        # stall it too. last_github_pull only advances on a successful pull,
        # so a transient GitHub failure gets retried on the very next wake
        # instead of being silently deferred a full poll_interval_seconds.
        if time.time() - last_github_pull >= stale_config.github.poll_interval_seconds:
            if _run_github_pull(
                stale_config.github,
                stale_config.request_timeout_seconds,
                registry,
                out_dir,
            ):
                last_github_pull = time.time()

        if stop.is_set():
            break
        stop.wait(timeout=stale_config.node_poll_interval_seconds)

    logger.info("stale-blocks-ingest stopped.")
