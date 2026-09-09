"""Thin CLI entrypoint - see btc_parser_app/api/price_gap_backfill.py for
the full docstring and implementation. Standalone (no config.yaml, no
subcommand) since it's a manually-run one-off, not one of run.py's
services.

    python backfill_price_gap.py --start-timestamp UNIX --end-timestamp UNIX \
        --export-dir PATH [--rate-limit-per-minute N] [--currency EUR]
"""

from __future__ import annotations

from btc_parser_app.api.price_gap_backfill import main

if __name__ == "__main__":
    raise SystemExit(main())
