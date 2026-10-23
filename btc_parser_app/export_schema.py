"""Column layout of every Splunk-facing export CSV, and which columns become
indexed fields in Splunk.

Single source of truth for splunk/build_field_config.py, which generates
the Splunk index-time app (TA-btc-parser-parsing, standalone/test instance),
the search-time app (TA-btc-parser-search, every environment) and the Cribl
pipeline (prod: UF -> Cribl -> Splunk) from it. tests/test_export_schema.py
checks these tuples against the rows the parsers actually emit, so a column
added to block_parser.py without updating this file fails the tests instead
of silently shifting every positional extraction by one.

Indexing concept (docs/10-splunk-integration-und-feldkonzept.md 10.6):

- Small sourcetypes (blocks, stale headers, sanctioned events, prices):
  every column indexed - cheap at their volume, and tstats over them stays
  instant over the whole chain history.
- transactions/inputs/outputs (billions of rows): only the low-cardinality
  columns that are worth grouping/filtering by in tstats and whose raw
  values ("true", "2", ...) are too ambiguous for a raw-term search. Hashes,
  txids and addresses are NOT indexed: every value is already a unique raw
  token, so `txid=...`/`address=...` resolve through the raw-term index and
  bloom filters just as fast - an indexed copy would only double the
  largest part of the tsidx.

Rule enforced by the generator: a field name indexed in one sourcetype must
be indexed in every sourcetype that has that column, because fields.conf
(INDEXED = true) is global, not per sourcetype.
"""

from __future__ import annotations

from dataclasses import dataclass

from btc_parser_app.rpc.block_parser import SCRIPT_TYPES
from btc_parser_app.rpc.mempool_watch import EVENT_COLUMNS


def _script_type_columns(prefix: str) -> tuple[str, ...]:
    return tuple(f"{prefix}_{script_type}_count" for script_type in SCRIPT_TYPES)


BLOCK_COLUMNS: tuple[str, ...] = (
    "hash",
    "height",
    "version",
    "versionHex",
    "merkleroot",
    "time",
    "time_since_prev_block_sec",
    "mediantime",
    "nonce",
    "bits",
    "target",
    "difficulty",
    "chainwork",
    "chainwork_log2",
    "nTx",
    "previousblockhash",
    "strippedsize",
    "size",
    "weight",
    "weight_utilization_pct",
    "block_subsidy_sats",
    "pool_id",
    "pool_name",
    "pool_link",
    "pool_match_method",
    "regular_tx_count",
    "total_vin_count",
    "total_vout_count",
    "coinbase_value_sats",
    "coinbase_vout_count",
    "total_fees_sats",
    "fee_known_tx_count",
    "fee_unknown_tx_count",
    "fee_avg_sats",
    "fee_median_sats",
    "fee_max_sats",
    "fee_rate_avg_sat_vb",
    "fee_rate_median_sat_vb",
    "fee_rate_max_sat_vb",
    "effective_fee_rate_sat_vb",
    "regular_input_value_sats",
    "input_value_complete_tx_count",
    "regular_output_value_sats",
    "coin_days_destroyed_btc",
    "tx_vsize_avg",
    "tx_vsize_median",
    "tx_vsize_max",
    "segwit_tx_count",
    "rbf_tx_count",
    "taproot_input_tx_count",
    "taproot_output_tx_count",
    "op_return_tx_count",
    "op_return_output_count",
    "op_return_script_bytes",
    "witness_input_count",
    "witness_item_count",
    "witness_data_bytes",
    "scriptsig_bytes",
    "scriptpubkey_bytes",
    *_script_type_columns("vin_type"),
    *_script_type_columns("vout_type"),
)

TRANSACTION_COLUMNS: tuple[str, ...] = (
    "block_hash",
    "block_height",
    "block_time",
    "tx_index",
    "txid",
    "wtxid",
    "is_coinbase",
    "version",
    "size",
    "vsize",
    "weight",
    "locktime",
    "fee_sats",
    "fee_source",
    "fee_rate_sat_vb",
    "input_value_sats",
    "output_value_sats",
    "output_value_min_sats",
    "output_value_max_sats",
    "output_value_avg_sats",
    "vin_count",
    "vout_count",
    "prevout_value_known_count",
    "prevout_height_known_count",
    "generated_input_count",
    "input_age_min_blocks",
    "input_age_max_blocks",
    "input_age_avg_blocks",
    "input_age_value_weighted_avg_blocks",
    "coin_days_destroyed_btc",
    "witness_input_count",
    "witness_item_count",
    "witness_data_bytes",
    "scriptsig_bytes",
    "scriptpubkey_bytes",
    "signals_rbf",
    "op_return_count",
    "op_return_script_bytes",
    "zero_value_output_count",
    *_script_type_columns("vin_type"),
    *_script_type_columns("vout_type"),
)

INPUT_COLUMNS: tuple[str, ...] = (
    "block_hash",
    "block_height",
    "block_time",
    "tx_index",
    "txid",
    "is_coinbase",
    "input_index",
    "prevout_txid",
    "prevout_vout",
    "value_sats",
    "prevout_height",
    "input_age_blocks",
    "prevout_generated",
    "prevout_type",
    "prevout_address",
    "scriptsig_bytes",
    "coinbase_hex",
    "witness_item_count",
    "witness_data_bytes",
    "sequence",
    "signals_rbf",
    "relative_locktime_type",
    "relative_locktime_value",
)

OUTPUT_COLUMNS: tuple[str, ...] = (
    "block_hash",
    "block_height",
    "block_time",
    "tx_index",
    "txid",
    "is_coinbase",
    "output_index",
    "value_sats",
    "script_type",
    "script_bytes",
    "address",
    "is_op_return",
)

STALE_BLOCK_HEADER_COLUMNS: tuple[str, ...] = (
    "observed_at",
    "height",
    "hash",
    "status",
    "header_hex",
    "header_valid",
    "source",
    "chaintip_status",
    "branchlen",
    "version",
    "previousblockhash",
    "merkleroot",
    "time",
    "bits",
    "nonce",
)

SANCTIONED_TX_EVENT_COLUMNS: tuple[str, ...] = tuple(EVENT_COLUMNS)

PRICE_COLUMNS: tuple[str, ...] = ("date_unix", "usd", "eur")


@dataclass(frozen=True)
class Sourcetype:
    name: str
    columns: tuple[str, ...]
    # Column that becomes _time, and its strptime format.
    time_field: str
    time_format: str
    # None = every column is an indexed field. Otherwise the indexed subset;
    # the rest is extracted at search time.
    indexed: tuple[str, ...] | None
    description: str

    @property
    def indexed_fields(self) -> tuple[str, ...]:
        return self.columns if self.indexed is None else self.indexed

    @property
    def search_time_fields(self) -> tuple[str, ...]:
        return tuple(c for c in self.columns if c not in self.indexed_fields)

    @property
    def header_prefix(self) -> str:
        """Start of the header row - no data row of this file starts with it."""
        return ",".join(self.columns[:2]) + ","


SOURCETYPES: tuple[Sourcetype, ...] = (
    Sourcetype(
        name="btc:blocks",
        columns=BLOCK_COLUMNS,
        time_field="time",
        time_format="%s",
        indexed=None,
        description="btc_parser_app blocks.csv - one row per block (docs chapter 4.9)",
    ),
    Sourcetype(
        name="btc:transactions",
        columns=TRANSACTION_COLUMNS,
        time_field="block_time",
        time_format="%s",
        indexed=("block_height", "is_coinbase", "version", "vin_count", "vout_count"),
        description="btc_parser_app transactions.csv - one row per transaction (docs chapter 4.10)",
    ),
    Sourcetype(
        name="btc:inputs",
        columns=INPUT_COLUMNS,
        time_field="block_time",
        time_format="%s",
        indexed=(
            "block_height",
            "is_coinbase",
            "prevout_type",
            "prevout_generated",
            "relative_locktime_type",
        ),
        description="btc_parser_app inputs.csv - one row per vin (docs chapter 4.11)",
    ),
    Sourcetype(
        name="btc:outputs",
        columns=OUTPUT_COLUMNS,
        time_field="block_time",
        time_format="%s",
        indexed=("block_height", "is_coinbase", "script_type"),
        description="btc_parser_app outputs.csv - one row per vout (docs chapter 4.12)",
    ),
    Sourcetype(
        name="btc:stale_block_headers",
        columns=STALE_BLOCK_HEADER_COLUMNS,
        time_field="observed_at",
        time_format="%Y-%m-%dT%H:%M:%SZ",
        indexed=None,
        description="btc_parser_app stale_block_headers.csv - non-active chain tips (docs chapter 5.6)",
    ),
    Sourcetype(
        name="btc:sanctioned_tx_events",
        columns=SANCTIONED_TX_EVENT_COLUMNS,
        time_field="observed_at",
        time_format="%Y-%m-%dT%H:%M:%SZ",
        indexed=None,
        description=(
            "btc_parser_app mempool-watch - one row per sanctioned input/output per tx "
            "state change (seen/confirmed/replaced/removed/unconfirmed, docs chapter 11)"
        ),
    ),
    Sourcetype(
        name="btc:prices",
        columns=PRICE_COLUMNS,
        time_field="date_unix",
        time_format="%s",
        indexed=None,
        description=(
            "live-polled + Kraken-imported BTC/USD/EUR minute prices (docs chapter 6.4/6.6) - "
            "date_unix is the price's own timestamp, not the poll time"
        ),
    ),
)

SOURCETYPES_BY_NAME: dict[str, Sourcetype] = {st.name: st for st in SOURCETYPES}
