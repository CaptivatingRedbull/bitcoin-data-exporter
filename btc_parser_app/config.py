"""
Loads config/config.yaml into typed, validated dataclasses.

Every other module in this app takes its configuration as a parameter
instead of importing module-level constants, so behaviour can be swapped
(a different config file, a config built in a test) without monkeypatching.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

# full_app/ - the directory containing config/ and this package. Every
# relative path in config.yaml is resolved against this, not the process
# working directory, so the app behaves the same no matter where it's
# launched from.
APP_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = APP_ROOT / "config" / "config.yaml"


class ConfigError(ValueError):
    """Raised for a missing/malformed config.yaml."""


def _resolve_path(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _require(section: dict[str, Any], key: str, section_name: str) -> Any:
    if key not in section:
        raise ConfigError(f"config.yaml: missing '{key}' in '{section_name}' section")
    return section[key]


# =============================================================================
# mempool_api
# =============================================================================


@dataclass(frozen=True)
class RateLimitConfig:
    requests_per_minute: float
    bucket_size: int


@dataclass(frozen=True)
class EndpointConfig:
    name: str
    path: str
    parser: str
    interval_seconds: float


@dataclass(frozen=True)
class MempoolApiConfig:
    base_url: str
    output_dir: Path
    request_timeout_seconds: float
    max_connection_retries: int
    retry_backoff_seconds: float
    rate_limit: RateLimitConfig
    endpoints: tuple[EndpointConfig, ...]


def _load_mempool_api(raw: dict[str, Any], root: Path) -> MempoolApiConfig:
    section = _require(raw, "mempool_api", "root")
    rate_limit_raw = _require(section, "rate_limit", "mempool_api")
    endpoints_raw = _require(section, "endpoints", "mempool_api")

    if not endpoints_raw:
        raise ConfigError("config.yaml: 'mempool_api.endpoints' must not be empty")

    max_connection_retries = int(
        _require(section, "max_connection_retries", "mempool_api")
    )
    if max_connection_retries < 0:
        raise ConfigError(
            "config.yaml: 'mempool_api.max_connection_retries' must be >= 0"
        )

    requests_per_minute = float(
        _require(rate_limit_raw, "requests_per_minute", "mempool_api.rate_limit")
    )
    if requests_per_minute <= 0:
        raise ConfigError(
            "config.yaml: 'mempool_api.rate_limit.requests_per_minute' must be > 0"
        )

    bucket_size = int(
        _require(rate_limit_raw, "bucket_size", "mempool_api.rate_limit")
    )
    if bucket_size < 1:
        raise ConfigError(
            "config.yaml: 'mempool_api.rate_limit.bucket_size' must be >= 1"
        )

    def _endpoint(e: dict[str, Any]) -> EndpointConfig:
        interval_seconds = float(_require(e, "interval_seconds", "mempool_api.endpoints[]"))
        if interval_seconds <= 0:
            raise ConfigError(
                "config.yaml: 'mempool_api.endpoints[].interval_seconds' must be > 0"
            )
        return EndpointConfig(
            name=_require(e, "name", "mempool_api.endpoints[]"),
            path=_require(e, "path", "mempool_api.endpoints[]"),
            parser=_require(e, "parser", "mempool_api.endpoints[]"),
            interval_seconds=interval_seconds,
        )

    endpoints = tuple(_endpoint(e) for e in endpoints_raw)

    return MempoolApiConfig(
        base_url=str(_require(section, "base_url", "mempool_api")).rstrip("/"),
        output_dir=_resolve_path(root, _require(section, "output_dir", "mempool_api")),
        request_timeout_seconds=float(
            _require(section, "request_timeout_seconds", "mempool_api")
        ),
        max_connection_retries=max_connection_retries,
        retry_backoff_seconds=float(
            _require(section, "retry_backoff_seconds", "mempool_api")
        ),
        rate_limit=RateLimitConfig(
            requests_per_minute=requests_per_minute,
            bucket_size=bucket_size,
        ),
        endpoints=endpoints,
    )


# =============================================================================
# mining_pools_dataset
# =============================================================================


@dataclass(frozen=True)
class MiningPoolsDatasetConfig:
    source_url: str
    local_path: Path
    refresh_interval_seconds: float


def _load_mining_pools_dataset(
    raw: dict[str, Any], root: Path
) -> MiningPoolsDatasetConfig:
    section = _require(raw, "mining_pools_dataset", "root")
    return MiningPoolsDatasetConfig(
        source_url=_require(section, "source_url", "mining_pools_dataset"),
        local_path=_resolve_path(
            root, _require(section, "local_path", "mining_pools_dataset")
        ),
        refresh_interval_seconds=float(
            _require(section, "refresh_interval_seconds", "mining_pools_dataset")
        ),
    )


# =============================================================================
# pricing (one-time historic minute-candle import into mempool_api's prices.csv)
# =============================================================================


@dataclass(frozen=True)
class PricingConfig:
    xbtusd_csv_path: Path
    xbteur_csv_path: Path


def _load_pricing(raw: dict[str, Any], root: Path) -> PricingConfig:
    section = _require(raw, "pricing", "root")
    return PricingConfig(
        xbtusd_csv_path=_resolve_path(
            root, _require(section, "xbtusd_csv_path", "pricing")
        ),
        xbteur_csv_path=_resolve_path(
            root, _require(section, "xbteur_csv_path", "pricing")
        ),
    )


# =============================================================================
# rpc
# =============================================================================


@dataclass(frozen=True)
class RpcConfig:
    """output_dir/state_dir mirror stale_blocks' split: output_dir is the
    Splunk-facing export (blocks/, transactions/ - safe for a `batch` input
    to consume-and-delete), state_dir is this app's own bookkeeping
    (current.csv, latest.csv, index/, block_status.csv, reorg/, the
    *_part_seq.csv counters, and the currently-still-growing blocks/
    transactions part while there's backlog - see rpc/part_writer.py) that
    Splunk should never be pointed at."""

    bitcoin_cli_path: str
    extra_args: tuple[str, ...]
    rpcuser_env: str | None
    rpcpassword_env: str | None
    batch_size: int
    output_dir: Path
    state_dir: Path
    reorg_confirmations: int
    max_reorg_depth: int
    poll_interval_seconds: float
    max_cli_retries: int
    cli_retry_backoff_seconds: float
    cli_timeout_seconds: float
    blocks_per_day: int
    max_block_weight: int
    halving_interval_blocks: int
    initial_subsidy_sats: int

    def auth_args(self) -> tuple[str, ...]:
        """-rpcuser=/-rpcpassword= flags, only if both env vars are set and
        non-empty. Otherwise bitcoin-cli falls back to cookie-file auth."""
        if not self.rpcuser_env or not self.rpcpassword_env:
            return ()
        user = os.environ.get(self.rpcuser_env)
        password = os.environ.get(self.rpcpassword_env)
        if not user or not password:
            return ()
        return (f"-rpcuser={user}", f"-rpcpassword={password}")


def _load_rpc(raw: dict[str, Any], root: Path) -> RpcConfig:
    section = _require(raw, "rpc", "root")

    reorg_confirmations = int(section.get("reorg_confirmations", 6))
    if reorg_confirmations < 0:
        raise ConfigError("config.yaml: 'rpc.reorg_confirmations' must be >= 0")

    max_reorg_depth = int(section.get("max_reorg_depth", 100))
    if max_reorg_depth < 1:
        raise ConfigError("config.yaml: 'rpc.max_reorg_depth' must be >= 1")

    poll_interval_seconds = float(section.get("poll_interval_seconds", 30))
    if poll_interval_seconds <= 0:
        raise ConfigError("config.yaml: 'rpc.poll_interval_seconds' must be > 0")

    max_cli_retries = int(section.get("max_cli_retries", 3))
    if max_cli_retries < 0:
        raise ConfigError("config.yaml: 'rpc.max_cli_retries' must be >= 0")

    cli_retry_backoff_seconds = float(section.get("cli_retry_backoff_seconds", 5))
    if cli_retry_backoff_seconds < 0:
        raise ConfigError("config.yaml: 'rpc.cli_retry_backoff_seconds' must be >= 0")

    cli_timeout_seconds = float(section.get("cli_timeout_seconds", 30))
    if cli_timeout_seconds <= 0:
        raise ConfigError("config.yaml: 'rpc.cli_timeout_seconds' must be > 0")

    batch_size = int(_require(section, "batch_size", "rpc"))
    if batch_size < 1:
        raise ConfigError("config.yaml: 'rpc.batch_size' must be >= 1")

    blocks_per_day = int(_require(section, "blocks_per_day", "rpc"))
    if blocks_per_day < 1:
        raise ConfigError("config.yaml: 'rpc.blocks_per_day' must be >= 1")

    max_block_weight = int(_require(section, "max_block_weight", "rpc"))
    if max_block_weight < 1:
        raise ConfigError("config.yaml: 'rpc.max_block_weight' must be >= 1")

    halving_interval_blocks = int(_require(section, "halving_interval_blocks", "rpc"))
    if halving_interval_blocks < 1:
        raise ConfigError("config.yaml: 'rpc.halving_interval_blocks' must be >= 1")

    return RpcConfig(
        bitcoin_cli_path=_require(section, "bitcoin_cli_path", "rpc"),
        extra_args=tuple(section.get("extra_args") or ()),
        rpcuser_env=section.get("rpcuser_env") or None,
        rpcpassword_env=section.get("rpcpassword_env") or None,
        batch_size=batch_size,
        output_dir=_resolve_path(root, _require(section, "output_dir", "rpc")),
        state_dir=_resolve_path(root, _require(section, "state_dir", "rpc")),
        reorg_confirmations=reorg_confirmations,
        max_reorg_depth=max_reorg_depth,
        poll_interval_seconds=poll_interval_seconds,
        max_cli_retries=max_cli_retries,
        cli_retry_backoff_seconds=cli_retry_backoff_seconds,
        cli_timeout_seconds=cli_timeout_seconds,
        blocks_per_day=blocks_per_day,
        max_block_weight=max_block_weight,
        halving_interval_blocks=halving_interval_blocks,
        initial_subsidy_sats=int(_require(section, "initial_subsidy_sats", "rpc")),
    )


# =============================================================================
# stale_blocks
# =============================================================================


@dataclass(frozen=True)
class StaleBlocksGithubConfig:
    csv_url: str
    poll_interval_seconds: float


@dataclass(frozen=True)
class StaleBlocksConfig:
    output_dir: Path
    state_dir: Path
    node_poll_interval_seconds: float
    request_timeout_seconds: float
    github: StaleBlocksGithubConfig


def _load_stale_blocks(raw: dict[str, Any], root: Path) -> StaleBlocksConfig:
    section = _require(raw, "stale_blocks", "root")
    github_raw = _require(section, "github", "stale_blocks")

    node_poll_interval_seconds = float(section.get("node_poll_interval_seconds", 3600))
    if node_poll_interval_seconds <= 0:
        raise ConfigError(
            "config.yaml: 'stale_blocks.node_poll_interval_seconds' must be > 0"
        )

    request_timeout_seconds = float(section.get("request_timeout_seconds", 30))
    if request_timeout_seconds <= 0:
        raise ConfigError("config.yaml: 'stale_blocks.request_timeout_seconds' must be > 0")

    github_poll_interval_seconds = float(
        _require(github_raw, "poll_interval_seconds", "stale_blocks.github")
    )
    if github_poll_interval_seconds <= 0:
        raise ConfigError(
            "config.yaml: 'stale_blocks.github.poll_interval_seconds' must be > 0"
        )

    return StaleBlocksConfig(
        output_dir=_resolve_path(root, _require(section, "output_dir", "stale_blocks")),
        state_dir=_resolve_path(root, _require(section, "state_dir", "stale_blocks")),
        node_poll_interval_seconds=node_poll_interval_seconds,
        request_timeout_seconds=request_timeout_seconds,
        github=StaleBlocksGithubConfig(
            csv_url=_require(github_raw, "csv_url", "stale_blocks.github"),
            poll_interval_seconds=github_poll_interval_seconds,
        ),
    )


# =============================================================================
# logging
# =============================================================================


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    log_dir: Path


def _load_logging(raw: dict[str, Any], root: Path) -> LoggingConfig:
    section = raw.get("logging") or {}
    return LoggingConfig(
        level=str(section.get("level", "INFO")).upper(),
        log_dir=_resolve_path(root, str(section.get("log_dir", "logs"))),
    )


# =============================================================================
# Top level
# =============================================================================


@dataclass(frozen=True)
class AppConfig:
    root: Path
    config_path: Path
    logging: LoggingConfig
    mempool_api: MempoolApiConfig
    mining_pools_dataset: MiningPoolsDatasetConfig
    pricing: PricingConfig
    rpc: RpcConfig
    stale_blocks: StaleBlocksConfig


def load_config(path: str | Path | None = None) -> AppConfig:
    config_path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise ConfigError(f"config file not found: {config_path}")

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ConfigError(
            f"config file must be a YAML mapping at the top level: {config_path}"
        )

    root = APP_ROOT

    return AppConfig(
        root=root,
        config_path=config_path,
        logging=_load_logging(raw, root),
        mempool_api=_load_mempool_api(raw, root),
        mining_pools_dataset=_load_mining_pools_dataset(raw, root),
        pricing=_load_pricing(raw, root),
        rpc=_load_rpc(raw, root),
        stale_blocks=_load_stale_blocks(raw, root),
    )
