"""Block fetch + parse for rpc-ingest, and the process pool that runs it
ahead of the writer while catching up (rpc.catchup_workers > 1).

Catch-up used to be strictly sequential, one block at a time: getblockhash,
getblock <hash> 3, json.loads, aggregate_block - all CPU-bound Python on one
core, with the node and disk mostly idle. iter_parsed_blocks() moves exactly
that per-block work (parse_block) into a pool of worker processes and hands
the results back strictly in height order, so everything that touches state
- the row buffers, index/, block_status.csv, current.csv, the
blocks/transactions/inputs/outputs parts and their *_part_seq.csv counters -
still happens in the main process, in the same order, with the same batch
boundaries as the sequential loop. The exported rows themselves are built by
the very same aggregate_block() call either way; only *where* it runs moves.

ingest.py only uses this for heights at least rpc.max_reorg_depth below the
tip (see _run_one_pass): a block that deep is already treated as final by
the reorg walk-back, so fetching its hash a few dozen blocks ahead of the
writer can't change what gets exported. The last stretch before the tip,
and all steady-state tip-following, stay on the sequential path unchanged.
"""

from __future__ import annotations

import logging
import multiprocessing
import signal
from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any, NamedTuple

import polars as pl

from btc_parser_app.common.logging_setup import _DATEFMT, _FORMAT
from btc_parser_app.config import RpcConfig
from btc_parser_app.rpc.block_parser import aggregate_block
from btc_parser_app.rpc.client import get_block, get_block_hash
from btc_parser_app.rpc.mining_pools import PoolMatcher

logger = logging.getLogger(__name__)

# Blocks fetched/parsed ahead of the writer, per worker. Enough to keep every
# worker busy while the writer is momentarily slower (a part rotation, a
# large block), while bounding memory: a full 2020s block is tens of MB of
# row dicts.
PREFETCH_PER_WORKER = 4


class ParsedBlock(NamedTuple):
    height: int
    block_hash: str
    previousblockhash: str
    block_time: int
    # The one blocks.csv row, still a dict: time_since_prev_block_sec is left
    # None here and filled in by the (in-order) caller, since it depends on
    # the previous block.
    block_event: dict[str, Any]
    # transactions/inputs/outputs rows of this block, already turned into
    # DataFrames. Building them from thousands of row dicts was the most
    # expensive thing the single writer process did per block; done here,
    # it runs in the catch-up workers instead. See frame_from_rows for why
    # the CSV bytes stay identical.
    frames: dict[str, pl.DataFrame]


def frame_from_rows(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """One block's rows of one event type as a DataFrame, typed exactly the
    way PartSequencer has always typed a whole batch:
    infer_schema_length=None. ingest._drain then pl.concat()s a batch of
    these with how="vertical_relaxed", which lifts each column to the
    supertype across the batch (Null + Int64 -> Int64, Int64 + Float64 ->
    Float64, ...) - the same supertype row-wise inference over the whole
    batch would have picked - so write_csv() produces the same bytes as
    building one DataFrame from all of the batch's rows (verified by
    tests/test_catchup.py's FrameEquivalenceTest)."""
    return pl.DataFrame(rows, infer_schema_length=None)


def parse_block(
    rpc_config: RpcConfig, pool_matcher: PoolMatcher | None, block_hash: str
) -> ParsedBlock:
    """getblock <hash> 3 -> exportable rows/frames."""
    return build_parsed_block(get_block(rpc_config, block_hash, verbosity=3), rpc_config, pool_matcher)


def build_parsed_block(
    block: dict[str, Any], rpc_config: RpcConfig, pool_matcher: PoolMatcher | None
) -> ParsedBlock:
    """A getblock <hash> 3 payload -> ParsedBlock (no RPC)."""
    aggregate = aggregate_block(block, rpc_config, pool_matcher)
    return ParsedBlock(
        height=int(block["height"]),
        block_hash=str(block["hash"]),
        previousblockhash=str(block.get("previousblockhash") or ""),
        block_time=int(aggregate.block_event["time"]),
        block_event=aggregate.block_event,
        frames={
            "transactions": frame_from_rows(aggregate.tx_events),
            "inputs": frame_from_rows(aggregate.input_events),
            "outputs": frame_from_rows(aggregate.output_events),
        },
    )


# Per-worker-process state, set once by _init_worker.
_worker_rpc_config: RpcConfig | None = None
_worker_pool_matcher: PoolMatcher | None = None


def _init_worker(rpc_config: RpcConfig, pool_matcher: PoolMatcher | None, log_level: int) -> None:
    global _worker_rpc_config, _worker_pool_matcher
    # SIGTERM (systemd stops the whole cgroup) and Ctrl-C (the whole process
    # group) reach the workers too. Only the main process may react - it sets
    # its stop event, finishes the block in hand, checkpoints, and shuts the
    # pool down itself; a worker dying on the signal instead would turn a
    # clean stop into a BrokenProcessPool.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    # Console only: the main process owns the rotating log file, and several
    # processes rotating the same file would clobber each other. A worker's
    # RPC retry warnings still land in the service's stdout/journal, and a
    # final failure is re-raised (and logged) in the main process.
    logging.basicConfig(level=log_level, format=_FORMAT, datefmt=_DATEFMT, force=True)
    _worker_rpc_config = rpc_config
    _worker_pool_matcher = pool_matcher


def _fetch_and_parse(height: int) -> ParsedBlock:
    assert _worker_rpc_config is not None
    block_hash = get_block_hash(_worker_rpc_config, height)
    parsed = parse_block(_worker_rpc_config, _worker_pool_matcher, block_hash)
    if parsed.height != height:
        raise RuntimeError(f"getblock {block_hash} returned height {parsed.height}, expected {height}")
    return parsed


def iter_parsed_blocks(
    rpc_config: RpcConfig,
    pool_matcher: PoolMatcher | None,
    heights: range,
    workers: int,
    fetch: Callable[[int], ParsedBlock] | None = None,
) -> Iterator[ParsedBlock]:
    """Yield ParsedBlock for every height in `heights`, in order, fetched and
    parsed by `workers` processes up to PREFETCH_PER_WORKER * workers blocks
    ahead. Any worker exception (RpcCliError after its retries, ...) is
    re-raised here, at the height it belongs to - after every earlier height
    has already been yielded, exactly as the sequential loop would have hit
    it. Closing the generator early (the caller stopping, or raising)
    cancels everything not yet started and waits for the rest.

    `fetch` (height -> ParsedBlock, run in the workers - must be a picklable
    module-level function) replaces the real getblockhash + parse_block;
    only tests pass it."""
    fetch = fetch or _fetch_and_parse
    window = PREFETCH_PER_WORKER * workers
    # spawn, not fork: the main process has polars' thread pool (and the
    # stop-signal machinery) running, and forking a multi-threaded process
    # can deadlock the child.
    pool = ProcessPoolExecutor(
        max_workers=workers,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=_init_worker,
        initargs=(rpc_config, pool_matcher, logging.getLogger().getEffectiveLevel()),
    )
    pending: deque[Future[ParsedBlock]] = deque()
    to_submit = iter(heights)
    try:
        for height in to_submit:
            pending.append(pool.submit(fetch, height))
            if len(pending) >= window:
                break
        while pending:
            parsed = pending.popleft().result()
            next_height = next(to_submit, None)
            if next_height is not None:
                pending.append(pool.submit(fetch, next_height))
            yield parsed
    finally:
        for future in pending:
            future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)
