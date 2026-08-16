#!/usr/bin/env python3
"""Convenience entrypoint - makes `python run.py <command>` work from
anywhere without having to set PYTHONPATH or `cd` into full_app/ first.

Equivalent to `python -m btc_parser_app.cli` run from inside full_app/.
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from btc_parser_app.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
