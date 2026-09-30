"""Thin bitcoin-cli wrapper.

Kept as a subprocess-over-bitcoin-cli client rather than a direct JSON-RPC/HTTP client so it transparently picks up
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
from decimal import Decimal
from typing import Any

from btc_parser_app.config import RpcConfig

logger = logging.getLogger(__name__)


class RpcCliError(Exception):
    """Raised when a bitcoin-cli invocation still fails after
    rpc.max_cli_retries retries. Callers that want a sustained RPC outage to
    back off and retry on the next pass rather than crash the whole ingest
    daemon should catch this specifically (see ingest.run_rpc_ingest)."""


class RpcError(RpcCliError):
    """The node answered, but with an RPC-level error (bitcoin-cli prints
    "error code: -5 ..." to stderr) - e.g. a txid that's no longer in the
    mempool. Deterministic, so only raised straight away (without the
    rpc.max_cli_retries retry loop) when a caller opts in via
    run_cli(..., retry_rpc_errors=False)."""


def _is_rpc_error(exc: Exception) -> bool:
    stderr = getattr(exc, "stderr", None) or ""
    return isinstance(exc, subprocess.CalledProcessError) and "error code:" in stderr


def _describe_error(exc: Exception) -> str:
    """CalledProcessError's default str() is just the command + exit code -
    it drops stderr, which is where bitcoin-cli actually puts the RPC
    error's message. Log that instead of the useless bare exit-code summary
    whenever it's there."""
    stderr = getattr(exc, "stderr", None)
    if stderr:
        return f"{exc} | stderr: {stderr.strip()}"
    return str(exc)


def run_cli(config: RpcConfig, cmd: list[str], *, retry_rpc_errors: bool = True) -> str:
    """Execute a bitcoin-cli command and return stdout.

    Retries a failed invocation (non-zero exit, timeout, or the binary
    being briefly unreachable) up to rpc.max_cli_retries times, waiting
    rpc.cli_retry_backoff_seconds between attempts - mirroring the
    retry/timeout handling api/client.py already has for the mempool.space
    side. A bitcoind restart, cookie rotation, or a momentary network blip
    used to raise straight through every caller and kill the daemon.

    retry_rpc_errors=False raises RpcError immediately for an RPC-level
    error instead of retrying it - for callers (mempool-watch) where e.g.
    "no such mempool transaction" is an expected, permanent answer, and
    burning rpc.max_cli_retries * cli_retry_backoff_seconds on it would
    stall the whole event loop.
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
            if not retry_rpc_errors and _is_rpc_error(exc):
                raise RpcError(f"bitcoin-cli {cmd!r}: {_describe_error(exc)}") from exc
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
    """Header-only fetch (getblockheader, verbose) - just the handful of
    fields the reorg-aware ingest loop needs (time, previousblockhash),
    without the full txid list `getblock <hash> 1` would also return."""
    raw = run_cli(config, ["getblockheader", block_hash])
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


# =============================================================================
# mempool-watch (btc_parser_app.rpc.mempool_watch)
# =============================================================================


def get_network_info(config: RpcConfig) -> dict[str, Any]:
    return json.loads(run_cli(config, ["getnetworkinfo"]))


def get_raw_mempool_txids(config: RpcConfig) -> list[str]:
    """Every txid currently in the mempool (getrawmempool, non-verbose)."""
    return json.loads(run_cli(config, ["getrawmempool"]))


def get_raw_transaction(
    config: RpcConfig, txid: str, blockhash: str | None = None
) -> dict[str, Any]:
    """`getrawtransaction <txid> 2 [blockhash]` - decoded tx including each
    input's `prevout` (Bitcoin Core 25+). Works for any mempool tx without
    -txindex; for a confirmed tx without -txindex, pass its blockhash.
    Amounts are parsed as Decimal, never float. Raises RpcError straight
    away (no retry loop) if the node doesn't know the tx."""
    cmd = ["getrawtransaction", txid, "2"]
    if blockhash:
        cmd.append(blockhash)
    raw = run_cli(config, cmd, retry_rpc_errors=False)
    return json.loads(raw, parse_float=Decimal)


def get_tx_out(
    config: RpcConfig, txid: str, vout: int, include_mempool: bool
) -> dict[str, Any] | None:
    """`gettxout` - None if the output doesn't exist/is spent (bitcoin-cli
    prints nothing at all for a JSON null result)."""
    raw = run_cli(
        config,
        ["gettxout", txid, str(vout), "true" if include_mempool else "false"],
        retry_rpc_errors=False,
    )
    return json.loads(raw, parse_float=Decimal) if raw else None


def get_tx_spending_prevout(
    config: RpcConfig, outpoints: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """`gettxspendingprevout` (Bitcoin Core 24+) - for each {txid, vout},
    the mempool tx currently spending it, if any (`spendingtxid`)."""
    raw = run_cli(
        config,
        ["gettxspendingprevout", json.dumps(outpoints)],
        retry_rpc_errors=False,
    )
    return json.loads(raw)


def get_block_txids(config: RpcConfig, block_hash: str) -> tuple[int, int, list[str]]:
    """(height, confirmations, txids) from `getblock <hash> 1`.
    confirmations is -1 for a block that's no longer on the active chain."""
    block = json.loads(get_block_verbose(config, block_hash, verbosity=1))
    return int(block["height"]), int(block["confirmations"]), list(block["tx"])
