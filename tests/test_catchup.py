"""rpc-ingest's parallel catch-up (rpc/catchup.py + ingest._run_one_pass)
must leave exactly the same bytes on disk as the sequential loop - parts,
*_part_seq.csv, current.csv/latest.csv, index/, block_status.csv - and keep
the same checkpoint/stop/error semantics.

Runs against a deterministic synthetic chain instead of a node: the
sequential path gets it through monkeypatched RPC helpers, the parallel
path through iter_parsed_blocks' `fetch` hook (module-level here, so the
spawned worker processes can import it).

    python -m unittest discover -s tests      (or: pytest tests)
"""

from __future__ import annotations

import functools
import hashlib
import random
import tempfile
import threading
import time
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

import polars as pl

from btc_parser_app.config import RpcConfig
from btc_parser_app.rpc import catchup, ingest
from btc_parser_app.rpc.block_parser import aggregate_block
from btc_parser_app.rpc.catchup import ParsedBlock, build_parsed_block, iter_parsed_blocks
from btc_parser_app.rpc.client import RpcCliError
from btc_parser_app.rpc.part_writer import PartSequencer
from btc_parser_app.rpc.reorg_state import BlockStatusStore, IndexStore

TIP = 260
GENESIS_TIME = 1231006505
SCRIPT_TYPES = ["pubkeyhash", "scripthash", "witness_v0_keyhash", "witness_v1_taproot", "nulldata"]


def make_config(**overrides) -> RpcConfig:
    values = dict(
        bitcoin_cli_path="bitcoin-cli",
        extra_args=(),
        rpcuser_env=None,
        rpcpassword_env=None,
        batch_size=7,
        output_dir=Path("unused"),
        state_dir=Path("unused"),
        reorg_confirmations=3,
        max_reorg_depth=20,
        poll_interval_seconds=1,
        max_cli_retries=0,
        cli_retry_backoff_seconds=0,
        cli_timeout_seconds=5,
        blocks_per_day=144,
        max_block_weight=4_000_000,
        halving_interval_blocks=210_000,
        initial_subsidy_sats=5_000_000_000,
    )
    values.update(overrides)
    return RpcConfig(**values)


BASE_CONFIG = make_config()


def block_hash(height: int) -> str:
    return hashlib.sha256(f"block-{height}".encode()).hexdigest()


HEIGHT_BY_HASH = {block_hash(h): h for h in range(TIP + 1)}


def make_block(height: int) -> dict:
    """getblock <hash> 3-shaped dict, deterministic per height, with
    enough variety (missing fees, OP_RETURNs, witness, None columns) that
    per-batch polars schema inference actually matters."""
    rng = random.Random(height)
    txs = [
        {
            "txid": f"cb{height:062x}",
            "hash": f"cb{height:062x}",
            "version": 1,
            "size": 120,
            "vsize": 120,
            "weight": 480,
            "locktime": 0,
            "vin": [{"coinbase": f"03{height:06x}2f74657374", "sequence": 4294967295}],
            "vout": [
                {
                    "value": Decimal("50.00000000"),
                    "scriptPubKey": {"type": "pubkeyhash", "hex": "76a9" + "00" * 20, "address": f"1miner{height % 3}"},
                }
            ],
        }
    ]
    for i in range(rng.randint(0, 4)):
        vins = []
        for j in range(rng.randint(1, 3)):
            vin = {"txid": f"{height:032x}{i:016x}{j:016x}", "vout": j, "sequence": rng.choice([4294967295, 4294967293, 10])}
            if rng.random() < 0.8:
                vin["prevout"] = {
                    "generated": rng.random() < 0.1,
                    "height": max(0, height - rng.randint(1, 50)),
                    "value": Decimal(rng.randint(1000, 10**9)) / Decimal(10**8),
                    "scriptPubKey": {"type": rng.choice(SCRIPT_TYPES), "address": f"addr{rng.randint(0, 99)}"},
                }
            if rng.random() < 0.5:
                vin["txinwitness"] = ["30" * rng.randint(1, 40), "02" * 33]
            vin["scriptSig"] = {"hex": "47" * rng.randint(0, 50)}
            vins.append(vin)
        vouts = []
        for k in range(rng.randint(1, 4)):
            stype = rng.choice(SCRIPT_TYPES)
            vouts.append(
                {
                    "value": Decimal(0) if stype == "nulldata" else Decimal(rng.randint(1, 10**8)) / Decimal(10**8),
                    "n": k,
                    "scriptPubKey": {"type": stype, "hex": "6a" * rng.randint(2, 40), **({} if stype == "nulldata" else {"address": f"addr{rng.randint(0, 99)}"})},
                }
            )
        tx = {
            "txid": f"{height:032x}{i:032x}",
            "hash": f"{height:032x}{i:032x}" if rng.random() < 0.5 else f"w{height:031x}{i:032x}",
            "version": rng.choice([1, 2]),
            "size": rng.randint(150, 900),
            "vsize": rng.randint(100, 600),
            "weight": rng.randint(400, 2400),
            "locktime": 0,
            "vin": vins,
            "vout": vouts,
        }
        if rng.random() < 0.7:
            tx["fee"] = Decimal(rng.randint(100, 50000)) / Decimal(10**8)
        txs.append(tx)
    return {
        "hash": block_hash(height),
        "height": height,
        "version": 536870912,
        "versionHex": "20000000",
        "merkleroot": "ab" * 32,
        "time": GENESIS_TIME + 600 * height + rng.randint(-300, 300),
        "mediantime": GENESIS_TIME + 600 * height - 3000,
        "nonce": rng.randint(0, 2**32 - 1),
        "bits": "1d00ffff",
        "target": "00000000ffff" + "0" * 52,
        "difficulty": Decimal("1.0") if height < 100 else Decimal("1.182899534312841"),
        "chainwork": f"{(height + 1) * 4295032833:064x}",
        "nTx": len(txs),
        "previousblockhash": block_hash(height - 1) if height > 0 else None,
        "strippedsize": 200,
        "size": 300,
        "weight": 900,
        "tx": txs,
    }


def fake_parse(height: int, prev_override: str | None = None) -> ParsedBlock:
    block = make_block(height)
    if prev_override is not None:
        block["previousblockhash"] = prev_override
    return build_parsed_block(block, BASE_CONFIG, None)


# --- worker-side fakes (must be module-level: pickled into spawned workers) ---


def fetch_ok(height: int) -> ParsedBlock:
    time.sleep(random.random() * 0.003)  # finish out of order
    return fake_parse(height)


def fetch_fail_at_40(height: int) -> ParsedBlock:
    if height == 40:
        raise RpcCliError("node went away")
    return fetch_ok(height)


def fetch_fork_at_60(height: int) -> ParsedBlock:
    if height == 60:
        return fake_parse(height, prev_override="ff" * 32)
    return fetch_ok(height)


class Harness:
    """One state_dir/output_dir pair plus the stores _run_one_pass needs,
    rebuilt from disk on each run() like a process restart would."""

    def __init__(self, root: Path, workers: int) -> None:
        self.root = root
        self.config = make_config(
            output_dir=root / "export", state_dir=root / "state", catchup_workers=workers
        )

    def run(self, tip: int = TIP, fetch=fetch_ok, stop: threading.Event | None = None, max_passes: int = 10) -> bool:
        cfg = self.config
        cfg.output_dir.mkdir(parents=True, exist_ok=True)
        cfg.state_dir.mkdir(parents=True, exist_ok=True)
        index = IndexStore(cfg.state_dir / "index" / "index.csv")
        block_status = BlockStatusStore(cfg.state_dir / "block_status.csv")
        sequencers = {
            name: PartSequencer(
                cfg.state_dir / name / f"{name}.csv",
                cfg.output_dir / name / f"{name}.csv",
                cfg.state_dir / f"{name}_part_seq.csv",
            )
            for name in ("blocks", "transactions", "inputs", "outputs")
        }
        stop = stop or threading.Event()
        if fetch is None:
            pool = mock.Mock(side_effect=AssertionError("catch-up pool must not be used"))
        else:
            pool = functools.partial(iter_parsed_blocks, fetch=fetch)
        patches = [
            mock.patch.object(ingest, "get_block_count", lambda _c: tip),
            mock.patch.object(ingest, "get_block_hash", lambda _c, h: block_hash(h)),
            mock.patch.object(ingest, "get_block_header", lambda _c, bh: {"time": make_block(HEIGHT_BY_HASH[bh])["time"]}),
            mock.patch.object(ingest, "parse_block", lambda _c, _p, bh: fake_parse(HEIGHT_BY_HASH[bh])),
            mock.patch.object(ingest, "iter_parsed_blocks", pool),
        ]
        for p in patches:
            p.start()
        try:
            for _ in range(max_passes):
                caught_up = ingest._run_one_pass(
                    cfg,
                    None,
                    index,
                    block_status,
                    cfg.state_dir / "current.csv",
                    cfg.state_dir / "latest.csv",
                    cfg.state_dir / "reorg",
                    sequencers,
                    stop,
                )
                if caught_up or stop.is_set():
                    return caught_up
            return False
        except RpcCliError:
            index.discard_pending()  # what run_rpc_ingest does before retrying
            raise
        finally:
            for p in patches:
                p.stop()

    def snapshot(self) -> dict[str, bytes]:
        return {
            str(p.relative_to(self.root)): p.read_bytes()
            for p in sorted(self.root.rglob("*"))
            if p.is_file()
        }

    def current_height(self) -> int:
        lines = (self.config.state_dir / "current.csv").read_text().splitlines()
        return int(lines[1].split(",")[0])


class ParallelCatchupTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def harness(self, name: str, workers: int) -> Harness:
        return Harness(self.tmp / name, workers)

    def assertSameFiles(self, a: Harness, b: Harness) -> None:
        snap_a, snap_b = a.snapshot(), b.snapshot()
        self.assertEqual(sorted(snap_a), sorted(snap_b))
        for name in snap_a:
            self.assertEqual(snap_a[name], snap_b[name], f"{name} differs")

    def test_parallel_output_is_byte_identical_to_sequential(self) -> None:
        seq, par = self.harness("seq", 1), self.harness("par", 3)
        self.assertTrue(seq.run())
        self.assertTrue(par.run())
        self.assertEqual(seq.current_height(), TIP - 3)
        self.assertSameFiles(seq, par)

    def test_resume_sequential_state_with_parallel_workers_and_back(self) -> None:
        # An existing (sequential) state dir resumed with workers on, then
        # tip-following sequentially again - same bytes as never switching.
        seq, mixed = self.harness("seq", 1), self.harness("mixed", 1)
        for h in (seq, mixed):
            self.assertTrue(h.run(tip=90))
        mixed.config = make_config(**{**mixed.config.__dict__, "catchup_workers": 4})
        self.assertTrue(seq.run(tip=230))
        self.assertTrue(mixed.run(tip=230))
        mixed.config = make_config(**{**mixed.config.__dict__, "catchup_workers": 1})
        # 258: batched; 259/260: within reorg_confirmations -> atomic parts.
        for tip in (258, 259, TIP):
            self.assertTrue(seq.run(tip=tip))
            self.assertTrue(mixed.run(tip=tip))
        self.assertTrue(any((seq.config.output_dir / "blocks").glob("blocks.*.csv")))
        self.assertSameFiles(seq, mixed)

    def test_stop_mid_parallel_range_checkpoints_cleanly(self) -> None:
        par = self.harness("par", 3)
        stop = threading.Event()
        original = ingest._process_prefetched

        def stop_after_50(parsed, *args, **kwargs):
            result = original(parsed, *args, **kwargs)
            if parsed.height == 50:
                stop.set()
            return result

        with mock.patch.object(ingest, "_process_prefetched", stop_after_50):
            par.run(stop=stop)
        self.assertEqual(par.current_height(), 50)
        blocks_rows = (par.config.state_dir / "blocks" / "blocks.csv").read_text().splitlines()
        self.assertEqual(len(blocks_rows), 1 + 51)  # header + heights 0..50, nothing past the stop
        index_rows = (par.config.state_dir / "index" / "index.csv").read_text().splitlines()
        self.assertEqual(len(index_rows), 1 + 51)

        # Resuming finishes the chain; the result matches a run that was
        # stopped at the same height sequentially and resumed.
        par.run()
        seq = self.harness("seq", 1)
        seq.run(tip=50 + 3)
        seq.run()
        self.assertSameFiles(seq, par)

    def test_worker_rpc_error_surfaces_in_height_order_and_retry_recovers(self) -> None:
        par = self.harness("par", 3)
        with self.assertRaises(RpcCliError):
            par.run(fetch=fetch_fail_at_40)
        # Only whole batches before the failing height were checkpointed:
        # batch_size=7 -> heights 0..34 (35 blocks), never anything from 40 on.
        self.assertEqual(par.current_height(), 34)
        blocks_rows = (par.config.state_dir / "blocks" / "blocks.csv").read_text().splitlines()
        self.assertEqual(len(blocks_rows), 1 + 35)

        par.run()
        seq = self.harness("seq", 1)
        seq.run(tip=34 + 3)
        seq.run()
        self.assertSameFiles(seq, par)

    def test_chain_mismatch_in_prefetch_window_ends_the_pass(self) -> None:
        par = self.harness("par", 3)
        self.assertFalse(par.run(fetch=fetch_fork_at_60, max_passes=1))
        self.assertEqual(par.current_height(), 59)
        index_rows = (par.config.state_dir / "index" / "index.csv").read_text().splitlines()
        self.assertEqual(len(index_rows), 1 + 60)

    def test_shallow_backlog_never_starts_the_pool(self) -> None:
        par = self.harness("par", 3)
        self.assertTrue(par.run(tip=200))
        # 10 new blocks, all within max_reorg_depth of the tip: sequential.
        self.assertTrue(par.run(tip=210, fetch=None))
        seq = self.harness("seq", 1)
        seq.run(tip=200)
        seq.run(tip=210)
        self.assertSameFiles(seq, par)


class FrameEquivalenceTest(unittest.TestCase):
    """ingest._drain writes pl.concat(per-block frames, how="vertical_relaxed")
    where the writer used to build one DataFrame from all of a batch's row
    dicts. Same bytes, for every event type, for many random batches -
    including batches where a column is all-null in some blocks (coinbase-
    only blocks, missing prevouts/fees) and populated in others."""

    def test_concat_of_block_frames_writes_same_csv_as_batch_rows(self) -> None:
        aggregates = [aggregate_block(make_block(h), BASE_CONFIG, None) for h in range(300)]
        rng = random.Random(7)
        for _ in range(300):
            n = rng.randint(1, 30)
            batch = rng.sample(aggregates, n)
            for attr in ("tx_events", "input_events", "output_events"):
                per_block = [getattr(a, attr) for a in batch]
                old = pl.DataFrame([r for rows in per_block for r in rows], infer_schema_length=None)
                new = pl.concat([catchup.frame_from_rows(rows) for rows in per_block], how="vertical_relaxed")
                self.assertEqual(old.write_csv(), new.write_csv(), f"{attr}, batch of {n}")

    def test_all_null_column_in_whole_batch(self) -> None:
        rows_a = [{"a": 1, "b": None}, {"a": 2, "b": None}]
        rows_b = [{"a": None, "b": None}]
        old = pl.DataFrame(rows_a + rows_b, infer_schema_length=None)
        new = pl.concat([catchup.frame_from_rows(rows_a), catchup.frame_from_rows(rows_b)], how="vertical_relaxed")
        self.assertEqual(old.write_csv(), new.write_csv())

    def test_int_and_float_blocks_widen_like_row_inference(self) -> None:
        rows_a = [{"x": 1}, {"x": None}]
        rows_b = [{"x": 2.5}]
        old = pl.DataFrame(rows_a + rows_b, infer_schema_length=None)
        new = pl.concat([catchup.frame_from_rows(rows_a), catchup.frame_from_rows(rows_b)], how="vertical_relaxed")
        self.assertEqual(old.write_csv(), new.write_csv())


class IterParsedBlocksTest(unittest.TestCase):
    def test_yields_in_height_order(self) -> None:
        heights = range(5, 45)
        got = [p.height for p in iter_parsed_blocks(BASE_CONFIG, None, heights, 4, fetch=fetch_ok)]
        self.assertEqual(got, list(heights))

    def test_early_close_shuts_pool_down(self) -> None:
        it = iter_parsed_blocks(BASE_CONFIG, None, range(0, 1000), 2, fetch=fetch_ok)
        self.assertEqual(next(it).height, 0)
        it.close()  # must not hang or leave work running

    def test_window_bounds_submissions(self) -> None:
        submitted: list[int] = []
        original_submit = catchup.ProcessPoolExecutor.submit

        def tracking_submit(pool, fn, height):
            submitted.append(height)
            return original_submit(pool, fn, height)

        with mock.patch.object(catchup.ProcessPoolExecutor, "submit", tracking_submit):
            it = iter_parsed_blocks(BASE_CONFIG, None, range(0, 500), 2, fetch=fetch_ok)
            next(it)
            self.assertLessEqual(len(submitted), catchup.PREFETCH_PER_WORKER * 2 + 1)
            it.close()


if __name__ == "__main__":
    unittest.main()
