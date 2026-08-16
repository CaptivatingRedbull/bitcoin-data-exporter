"""Thin bitcoin-cli wrapper.

Kept as a subprocess-over-bitcoin-cli client (matching rpc_parser_modified.py)
rather than a direct JSON-RPC/HTTP client so it transparently picks up
cookie-file auth, .bitcoin/bitcoin.conf, and any local `bitcoin-cli` alias
the host already has configured - the exact same auth path the operator
uses interactively. `rpc.extra_args` and `rpc.auth_args()` in config.yaml
extend this for remote nodes (see config/config.yaml's `rpc:` section).
"""

from __future__ import annotations

import json
import logging
import subprocess
import time
from typing import Any

from btc_parser_app.config import RpcConfig

logger = logging.getLogger(__name__)


class RpcCliError(Exception):
    """Raised when a bitcoin-cli invocation still fails after
    rpc.max_cli_retries retries. Callers that want a sustained RPC outage to
    back off and retry on the next pass rather than crash the whole ingest
    daemon should catch this specifically (see ingest.run_rpc_ingest)."""


def _describe_error(exc: Exception) -> str:
    """CalledProcessError's default str() is just the command + exit code -
    it drops stderr, which is where bitcoin-cli actually puts the RPC
    error's message. Log that instead of the useless bare exit-code summary
    whenever it's there."""
    stderr = getattr(exc, "stderr", None)
    if stderr:
        return f"{exc} | stderr: {stderr.strip()}"
    return str(exc)


def run_cli(config: RpcConfig, cmd: list[str]) -> str:
    """Execute a bitcoin-cli command and return stdout.

    Retries a failed invocation (non-zero exit, timeout, or the binary
    being briefly unreachable) up to rpc.max_cli_retries times, waiting
    rpc.cli_retry_backoff_seconds between attempts - mirroring the
    retry/timeout handling api/client.py already has for the mempool.space
    side. A bitcoind restart, cookie rotation, or a momentary network blip
    used to raise straight through every caller and kill the daemon.
    """
    full_cmd = [config.bitcoin_cli_path, *config.extra_args, *config.auth_args(), *cmd]
    last_exc: Exception | None = None

    for attempt in range(config.max_cli_retries + 1):
        try:
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=True,
                timeout=config.cli_timeout_seconds,
            )
            return result.stdout.strip()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
            last_exc = exc
            if attempt < config.max_cli_retries:
                logger.warning(
                    "bitcoin-cli %s failed (%s); retrying in %.0fs (attempt %d/%d)",
                    cmd,
                    _describe_error(exc),
                    config.cli_retry_backoff_seconds,
                    attempt + 1,
                    config.max_cli_retries,
                )
                time.sleep(config.cli_retry_backoff_seconds)

    raise RpcCliError(
        f"bitcoin-cli {cmd!r} failed after {config.max_cli_retries + 1} attempt(s): "
        f"{_describe_error(last_exc)}"
    ) from last_exc


def get_block_count(config: RpcConfig) -> int:
    return int(run_cli(config, ["getblockcount"]))


def get_block_hash(config: RpcConfig, height: int) -> str:
    return run_cli(config, ["getblockhash", str(height)])


def get_block_verbose(config: RpcConfig, block_hash: str, verbosity: int = 3) -> str:
    """Returns the raw JSON text from `getblock <hash> <verbosity>`."""
    return run_cli(config, ["getblock", block_hash, str(verbosity)])


def get_block_header(config: RpcConfig, block_hash: str) -> dict[str, Any]:
    """Header-only fetch (verbosity=1: no transaction bodies) - just the
    handful of fields the reorg-aware ingest loop needs (time,
    previousblockhash) without paying for a full verbosity=3 payload."""
    raw = run_cli(config, ["getblock", block_hash, "1"])
    return json.loads(raw)


def get_block_header_raw(config: RpcConfig, block_hash: str) -> str:
    """Raw 80-byte serialized header, hex-encoded (getblockheader
    verbose=false). Used by the stale-blocks pipeline instead of
    get_block_header's JSON form when the caller wants the raw bytes to
    independently validate/parse."""
    return run_cli(config, ["getblockheader", block_hash, "false"])


def get_chain_tips(config: RpcConfig) -> list[dict[str, Any]]:
    """All known chain tips (getchaintips): the active tip, valid-fork/
    valid-headers/headers-only forks, and invalid tips. Every entry already
    has a header known to this node - getchaintips is built from the block
    index, which only ever contains blocks whose header has been accepted."""
    raw = run_cli(config, ["getchaintips"])
    return json.loads(raw)
