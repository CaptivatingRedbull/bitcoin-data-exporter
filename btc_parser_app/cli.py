"""Command-line entrypoint for the whole app.

    python run.py --config PATH <command>

--config must come BEFORE <command> - it's a top-level argparse option, and
argparse subparsers don't inherit options placed after the subcommand name.

Commands:
    rpc-ingest              Run the RPC block parser (bitcoin-cli -> blocks.csv/transactions.csv).
                            One implementation, no separate "backfill" mode: starts from genesis
                            if nothing's parsed yet, resumes where it left off otherwise, catches
                            up to the tip, then keeps following it forever (reorg-aware throughout
                            - see "RPC Parser Reorg Handling" in ../README.md). Runs until
                            SIGTERM/SIGINT.
    stale-blocks-ingest     Run the stale/orphaned chain-tip pipeline (getchaintips + the
                            bitcoin-data/stale-blocks GitHub dataset -> out_stale_blocks/).
                            Separate sourcetype from rpc-ingest's main-chain output. Runs until
                            SIGTERM/SIGINT.
    api-poll                Run the mempool.space endpoint poller forever (until a 429 or Ctrl-C/SIGTERM),
                            including the minutely "prices" endpoint that writes
                            mempool_api.output_dir/prices.csv.
    update-pools-dataset    Force-refresh config/pools-v2.json from GitHub
    import-price-history    One-time (idempotent) bulk import of pricing.xbtusd_csv_path/
                            pricing.xbteur_csv_path (Kraken 1-minute OHLC exports) into
                            mempool_api.output_dir/prices.csv - the same minutely file api-poll
                            appends to live (see README.md's Pricing section).

For always-on production use, don't invoke this directly - use ../start.sh,
which also makes sure bitcoind is up first and runs both long-running
commands detached in the background with logs under logs/.

See full_app/README.md for details and full_app/config/config.yaml for
every setting these commands read.
"""

from __future__ import annotations

import argparse
import signal
import sys

import yaml

from btc_parser_app.api.client import FetchError, RateLimited
from btc_parser_app.api.mining_pools_dataset import refresh as refresh_pools_dataset
from btc_parser_app.api.poller import run_poller
from btc_parser_app.api.price_history_import import import_price_history
from btc_parser_app.common.logging_setup import configure_logging
from btc_parser_app.config import ConfigError, load_config
from btc_parser_app.rpc.ingest import run_rpc_ingest
from btc_parser_app.rpc.stale_blocks import run_stale_blocks_ingest


def build_parser() -> argparse.ArgumentParser:
    # --config MUST be defined only here (top-level), not duplicated onto
    # the subparsers via parents= - that looks like it'd let --config work
    # in either position, but it doesn't: argparse's subparser dispatch
    # builds its own sub-namespace using ITS OWN defaults and then copies
    # every attr from that sub-namespace onto the outer one unconditionally
    # (see _SubParsersAction.__call__), so a same-dest argument that's also
    # on the subparser silently resets --config back to None even when it
    # was correctly given before the subcommand. Keep it top-level-only,
    # which means --config must come *before* the subcommand:
    #   run.py --config PATH <command>   (correct)
    #   run.py <command> --config PATH   (argparse error: unrecognized arguments)
    parser = argparse.ArgumentParser(prog="btc_parser_app", description=__doc__)
    parser.add_argument(
        "--config", default=None, help="Path to config.yaml (default: full_app/config/config.yaml)"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "rpc-ingest",
        help="Run the RPC block parser - genesis-to-tip catch-up then continuous tip-following",
    )
    subparsers.add_parser(
        "stale-blocks-ingest",
        help="Run the stale/orphaned chain-tip pipeline (getchaintips + GitHub dataset)",
    )
    subparsers.add_parser("api-poll", help="Run the mempool.space endpoint poller")
    subparsers.add_parser(
        "update-pools-dataset", help="Force-refresh config/pools-v2.json from GitHub"
    )
    subparsers.add_parser(
        "import-price-history",
        help="One-time (idempotent) bulk import of the Kraken minute CSVs into prices.csv",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        config = load_config(args.config)
    except (ConfigError, ValueError, TypeError, yaml.YAMLError) as exc:
        # ConfigError covers our own validation (missing/out-of-range
        # values); plain ValueError/TypeError covers a non-numeric or null
        # value hitting int()/float() in config.py's loaders; yaml.YAMLError
        # covers malformed YAML syntax. All three used to produce a raw
        # traceback instead of this clean message + exit code.
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    configure_logging(config.logging, component=args.command)

    if args.command == "rpc-ingest":
        run_rpc_ingest(config)
        return 0

    if args.command == "stale-blocks-ingest":
        run_stale_blocks_ingest(config)
        return 0

    if args.command == "api-poll":
        # run_poller only breaks its loop on KeyboardInterrupt (SIGINT).
        # When started detached (e.g. by start.sh/nohup) it only ever gets
        # SIGTERM, so route that through the same handler for a clean stop
        # instead of an abrupt kill.
        signal.signal(signal.SIGTERM, signal.default_int_handler)
        return run_poller(config.mempool_api)

    if args.command == "update-pools-dataset":
        try:
            refresh_pools_dataset(config.mining_pools_dataset)
        except (RateLimited, FetchError, ValueError) as exc:
            print(f"Failed to refresh pools dataset: {exc}", file=sys.stderr)
            return 1
        return 0

    if args.command == "import-price-history":
        prices_path = config.mempool_api.output_dir / "prices.csv"
        try:
            import_price_history(config.pricing, prices_path)
        except (FileNotFoundError, ValueError) as exc:
            print(f"Failed to import price history: {exc}", file=sys.stderr)
            return 1
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
