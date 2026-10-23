"""Thin bitcoind RPC client with two transports (rpc.transport):

- "cli" (default): a bitcoin-cli subprocess per call. Transparently picks up
  cookie-file auth, .bitcoin/bitcoin.conf, and any local `bitcoin-cli` alias
  the host already has configured - the exact same auth path the operator
  uses interactively. `rpc.extra_args` and `rpc.auth_args()` in config.yaml
  extend this for remote nodes (see config/config.yaml's `rpc:` section).
- "http": direct JSON-RPC over one kept-alive HTTP connection per
  process/thread. Skips the two process spawns plus bitcoin-cli's own
  parse-and-pretty-print round trip per call, which dominated rpc-ingest's
  per-block cost during catch-up. Endpoint and cookie file default to what
  bitcoin-cli would use given rpc.extra_args (-rpcconnect/-rpcport/
  -rpccookiefile/-datadir), unless rpc.http_url/rpc.cookie_file override
  them. The cookie is re-read on every (re)connect and on HTTP 401, so a
  bitcoind restart (which rotates it) is just another retried failure.

Both return the same values: bitcoind's JSON result, with every
non-integer number parsed as Decimal where the caller asks for it - the
HTTP body and bitcoin-cli's output carry the same UniValue number text.
Retry/timeout handling (rpc.max_cli_retries etc.) is identical too.
"""

from __future__ import annotations

import base64
import http.client
import json
import logging
import os
import subprocess
import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

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


# =============================================================================
# HTTP JSON-RPC transport (rpc.transport: http)
# =============================================================================

_CHAIN_FLAGS = {"-regtest": "regtest", "-testnet": "testnet3", "-testnet4": "testnet4", "-signet": "signet"}
_DEFAULT_PORTS = {"main": 8332, "testnet3": 18332, "testnet4": 48332, "signet": 38332, "regtest": 18443}


def _extra_arg(config: RpcConfig, name: str) -> str | None:
    """Value of the last `-name=value` in rpc.extra_args (bitcoin-cli's own
    last-one-wins rule), or None."""
    value = None
    for arg in config.extra_args:
        if arg.startswith(f"-{name}="):
            value = arg.split("=", 1)[1]
    return value


def _chain(config: RpcConfig) -> str:
    chain = "main"
    for arg in config.extra_args:
        if arg in _CHAIN_FLAGS:
            chain = _CHAIN_FLAGS[arg]
        elif arg.startswith("-chain="):
            chain = {"test": "testnet3"}.get(arg.split("=", 1)[1], arg.split("=", 1)[1])
    return chain


def http_endpoint(config: RpcConfig) -> tuple[str, int]:
    """(host, port) for rpc.transport=http: rpc.http_url if set, else
    whatever -rpcconnect/-rpcport in rpc.extra_args say, else bitcoin-cli's
    own defaults (127.0.0.1, the chain's default RPC port)."""
    if config.http_url:
        parts = urlsplit(config.http_url)
        return parts.hostname or "127.0.0.1", parts.port or _DEFAULT_PORTS[_chain(config)]
    host = _extra_arg(config, "rpcconnect") or "127.0.0.1"
    port = _extra_arg(config, "rpcport")
    if ":" in host and not host.startswith("["):
        host, _, host_port = host.rpartition(":")
        port = port or host_port
    return host.strip("[]"), int(port) if port else _DEFAULT_PORTS[_chain(config)]


def cookie_path(config: RpcConfig) -> Path:
    """Cookie file rpc.transport=http authenticates with (unless
    rpcuser_env/rpcpassword_env are set): rpc.cookie_file, else
    -rpccookiefile, else <-datadir or ~/.bitcoin>/[<chain>/].cookie - the
    same lookup bitcoin-cli does."""
    if config.cookie_file is not None:
        return config.cookie_file
    chain = _chain(config)
    datadir = Path(_extra_arg(config, "datadir") or Path.home() / ".bitcoin").expanduser()
    chain_dir = datadir if chain == "main" else datadir / chain
    explicit = _extra_arg(config, "rpccookiefile")
    if explicit:
        explicit_path = Path(explicit).expanduser()
        return explicit_path if explicit_path.is_absolute() else chain_dir / explicit_path
    return chain_dir / ".cookie"


class _HttpRpcError(Exception):
    """Transport-level failure (connect/timeout/HTTP status without a JSON-RPC
    error body). Retried like a failed bitcoin-cli invocation."""


class _HttpRpcCallError(Exception):
    """The node answered with a JSON-RPC error object - same meaning as
    bitcoin-cli's "error code: ..." on stderr."""

    def __init__(self, code: Any, message: Any) -> None:
        super().__init__(f"error code: {code} error message: {message}")


class _HttpRpcConnection:
    """One persistent HTTP/1.1 connection to bitcoind. Not thread-safe -
    _http_connection() hands out one per (process, thread)."""

    def __init__(self, config: RpcConfig) -> None:
        self._config = config
        self._host, self._port = http_endpoint(config)
        self._conn: http.client.HTTPConnection | None = None
        self._auth: str | None = None
        self._next_id = 0

    def _auth_header(self) -> str:
        args = self._config.auth_args()
        if args:
            user = args[0].split("=", 1)[1]
            password = args[1].split("=", 1)[1]
            userpass = f"{user}:{password}"
        else:
            path = cookie_path(self._config)
            try:
                userpass = path.read_text(encoding="utf-8").strip()
            except OSError as exc:
                raise _HttpRpcError(f"cannot read RPC cookie file {path}: {exc}") from exc
        return "Basic " + base64.b64encode(userpass.encode()).decode()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
        self._conn = None

    def _roundtrip(self, body: bytes) -> tuple[http.client.HTTPResponse, bytes]:
        if self._conn is None:
            self._auth = self._auth_header()
            self._conn = http.client.HTTPConnection(
                self._host, self._port, timeout=self._config.cli_timeout_seconds
            )
        assert self._auth is not None
        self._conn.request(
            "POST", "/", body, {"Authorization": self._auth, "Content-Type": "application/json"}
        )
        resp = self._conn.getresponse()
        return resp, resp.read()

    def call(self, method: str, params: list[Any], *, decimal: bool) -> Any:
        self._next_id += 1
        body = json.dumps(
            {"jsonrpc": "1.0", "id": self._next_id, "method": method, "params": params}
        ).encode()
        reused = self._conn is not None
        try:
            try:
                resp, payload = self._roundtrip(body)
            except (OSError, http.client.HTTPException):
                if not reused:
                    raise
                # bitcoind drops idle keep-alive connections after
                # -rpcservertimeout (30s by default) - about one
                # poll_interval_seconds. That's not a failure worth a retry
                # backoff: reconnect once, straight away. (Every call made
                # through here is a read, so resending is harmless.)
                self.close()
                resp, payload = self._roundtrip(body)
        except (OSError, http.client.HTTPException) as exc:
            self.close()
            raise _HttpRpcError(f"{type(exc).__name__}: {exc}") from exc

        if resp.status == 401:
            # Most likely a bitcoind restart rotated the cookie - drop the
            # connection so the retry re-reads it.
            self.close()
            raise _HttpRpcError("HTTP 401 Unauthorized (stale cookie or wrong credentials)")
        try:
            reply = json.loads(payload, parse_float=Decimal if decimal else float)
        except ValueError:
            self.close()
            raise _HttpRpcError(
                f"HTTP {resp.status} with non-JSON body: {payload[:200]!r}"
            ) from None
        if not isinstance(reply, dict) or "result" not in reply:
            self.close()
            raise _HttpRpcError(f"HTTP {resp.status} with unexpected body: {payload[:200]!r}")
        if reply.get("error") is not None:
            error = reply["error"]
            raise _HttpRpcCallError(error.get("code"), error.get("message"))
        if resp.will_close:
            self.close()
        return reply["result"]


_http_local = threading.local()


def _http_connection(config: RpcConfig) -> _HttpRpcConnection:
    """Per-thread (mempool-watch's rpc_workers pool) and per-process
    (rpc-ingest's catch-up workers) connection - an HTTPConnection must never
    be shared across either."""
    conns: dict[tuple[int, RpcConfig], _HttpRpcConnection] | None = getattr(_http_local, "conns", None)
    if conns is None:
        conns = _http_local.conns = {}
    key = (os.getpid(), config)
    conn = conns.get(key)
    if conn is None:
        conn = conns[key] = _HttpRpcConnection(config)
    return conn


def _http_call(
    config: RpcConfig, method: str, params: list[Any], *, decimal: bool, retry_rpc_errors: bool
) -> Any:
    """HTTP counterpart of run_cli: same retry count/backoff/exception types."""
    conn = _http_connection(config)
    last_exc: Exception | None = None
    for attempt in range(config.max_cli_retries + 1):
        try:
            return conn.call(method, params, decimal=decimal)
        except (_HttpRpcError, _HttpRpcCallError) as exc:
            last_exc = exc
            if not retry_rpc_errors and isinstance(exc, _HttpRpcCallError):
                raise RpcError(f"rpc {method} {params!r}: {exc}") from exc
            if attempt < config.max_cli_retries:
                logger.warning(
                    "rpc %s %r failed (%s); retrying in %.0fs (attempt %d/%d)",
                    method,
                    params,
                    exc,
                    config.cli_retry_backoff_seconds,
                    attempt + 1,
                    config.max_cli_retries,
                )
                time.sleep(config.cli_retry_backoff_seconds)
    raise RpcCliError(
        f"rpc {method} {params!r} failed after {config.max_cli_retries + 1} attempt(s): {last_exc}"
    ) from last_exc


def _cli_arg(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return str(value)


def _call(
    config: RpcConfig,
    method: str,
    params: list[Any],
    *,
    text: bool = False,
    decimal: bool = False,
    retry_rpc_errors: bool = True,
) -> Any:
    """One RPC call over rpc.transport. text=True: the result is a plain
    string (bitcoin-cli prints those unquoted, so its stdout is returned
    as-is); otherwise the JSON result, None for a JSON null (bitcoin-cli
    prints nothing at all for one). decimal=True parses non-integer numbers
    as Decimal instead of float."""
    if config.transport == "http":
        return _http_call(
            config, method, params, decimal=decimal, retry_rpc_errors=retry_rpc_errors
        )
    raw = run_cli(
        config, [method, *(_cli_arg(p) for p in params)], retry_rpc_errors=retry_rpc_errors
    )
    if text:
        return raw
    if not raw:
        return None
    return json.loads(raw, parse_float=Decimal if decimal else float)


# =============================================================================
# rpc-ingest / stale-blocks-ingest
# =============================================================================


def get_block_count(config: RpcConfig) -> int:
    return int(_call(config, "getblockcount", []))


def get_block_hash(config: RpcConfig, height: int) -> str:
    return _call(config, "getblockhash", [height], text=True)


def get_block(config: RpcConfig, block_hash: str, verbosity: int = 3) -> dict[str, Any]:
    """`getblock <hash> <verbosity>`, parsed - Decimal (never float) for every
    BTC amount, so they stay exact until conversion to satoshis."""
    return _call(config, "getblock", [block_hash, verbosity], decimal=True)


def get_block_header(config: RpcConfig, block_hash: str) -> dict[str, Any]:
    """Header-only fetch (getblockheader, verbose) - just the handful of
    fields the reorg-aware ingest loop needs (time, previousblockhash),
    without the full txid list `getblock <hash> 1` would also return."""
    return _call(config, "getblockheader", [block_hash])


def get_block_header_raw(config: RpcConfig, block_hash: str) -> str:
    """Raw 80-byte serialized header, hex-encoded (getblockheader
    verbose=false). Used by the stale-blocks pipeline instead of
    get_block_header's JSON form when the caller wants the raw bytes to
    independently validate/parse."""
    return _call(config, "getblockheader", [block_hash, False], text=True)


def get_chain_tips(config: RpcConfig) -> list[dict[str, Any]]:
    """All known chain tips (getchaintips): the active tip, valid-fork/
    valid-headers/headers-only forks, and invalid tips. Every entry already
    has a header known to this node - getchaintips is built from the block
    index, which only ever contains blocks whose header has been accepted."""
    return _call(config, "getchaintips", [])


# =============================================================================
# mempool-watch (btc_parser_app.rpc.mempool_watch)
# =============================================================================


def get_network_info(config: RpcConfig) -> dict[str, Any]:
    return _call(config, "getnetworkinfo", [])


def get_raw_mempool_txids(config: RpcConfig) -> list[str]:
    """Every txid currently in the mempool (getrawmempool, non-verbose)."""
    return _call(config, "getrawmempool", [])


def get_raw_transaction(
    config: RpcConfig, txid: str, blockhash: str | None = None
) -> dict[str, Any]:
    """`getrawtransaction <txid> 2 [blockhash]` - decoded tx including each
    input's `prevout` (Bitcoin Core 25+). Works for any mempool tx without
    -txindex; for a confirmed tx without -txindex, pass its blockhash.
    Amounts are parsed as Decimal, never float. Raises RpcError straight
    away (no retry loop) if the node doesn't know the tx."""
    params: list[Any] = [txid, 2]
    if blockhash:
        params.append(blockhash)
    return _call(config, "getrawtransaction", params, decimal=True, retry_rpc_errors=False)


def get_tx_out(
    config: RpcConfig, txid: str, vout: int, include_mempool: bool
) -> dict[str, Any] | None:
    """`gettxout` - None if the output doesn't exist/is spent."""
    return _call(
        config,
        "gettxout",
        [txid, vout, include_mempool],
        decimal=True,
        retry_rpc_errors=False,
    )


def get_tx_spending_prevout(
    config: RpcConfig, outpoints: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """`gettxspendingprevout` (Bitcoin Core 24+) - for each {txid, vout},
    the mempool tx currently spending it, if any (`spendingtxid`)."""
    return _call(config, "gettxspendingprevout", [outpoints], retry_rpc_errors=False)


def get_block_txids(config: RpcConfig, block_hash: str) -> tuple[int, int, list[str]]:
    """(height, confirmations, txids) from `getblock <hash> 1`.
    confirmations is -1 for a block that's no longer on the active chain."""
    block = get_block(config, block_hash, verbosity=1)
    return int(block["height"]), int(block["confirmations"]), list(block["tx"])
