"""Turns a `getblock <hash> 3` payload into flat block/transaction event
dicts ready for CSV export.

Ported from rpc_parser_modified.py, with two additions for the mining pool
extractor (btc_parser_app.rpc.mining_pools):

- aggregate_transaction now also collects the coinbase transaction's output
  addresses (coinbase_output_addresses_json), needed for address-based pool
  matching.
- aggregate_block takes an optional PoolMatcher and, when given one,
  attaches pool_id/pool_name/pool_link/pool_match_method to the block event.

vin/vout are still fully inspected here, but their individual rows are not
exported - only scalar aggregate fields make it into the CSV. The one
intentional exception is coinbase_script_sig_hex: consensus limits the
coinbase scriptSig to a small size, and it's useful for miner/BIP34/
extranonce analysis (and is exactly what the pool tag match reads).
"""

from __future__ import annotations

import json
import math
import statistics
from decimal import Decimal
from typing import Any

from btc_parser_app.common.json_utils import json_dumps_compact
from btc_parser_app.config import RpcConfig
from btc_parser_app.rpc.mining_pools import UNKNOWN_MATCH, PoolMatch, PoolMatcher

SATOSHIS_PER_BTC = Decimal(100000000)
BIP125_RBF_SEQUENCE_THRESHOLD = 0xFFFFFFFE

# Bitcoin Core scriptPubKey types worth keeping as fixed aggregate counters.
# Anything else is counted as "other", so a newly introduced script type
# does not break the CSV schema.
SCRIPT_TYPES = (
    "nonstandard",
    "pubkey",
    "pubkeyhash",
    "scripthash",
    "multisig",
    "nulldata",
    "witness_v0_keyhash",
    "witness_v0_scripthash",
    "witness_v1_taproot",
    "witness_unknown",
    "anchor",
    "other",
)


# ==============================================================================
# HELPERS
# ==============================================================================


def btc_to_sats(value: Any) -> int | None:
    """Convert a Bitcoin amount to satoshis without float rounding loss."""
    if value is None:
        return None
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return int((value * SATOSHIS_PER_BTC).to_integral_value())


def hex_data_bytes(value: Any) -> int:
    """Return the byte length of a hex string. Missing values count as zero."""
    if not isinstance(value, str):
        return 0
    return len(value) // 2


def normalized_script_type(value: Any) -> str:
    """Map unknown/new script types into the stable 'other' CSV column."""
    if value in SCRIPT_TYPES and value != "other":
        return str(value)
    return "other"


def init_script_type_counts(prefix: str) -> dict[str, int]:
    return {f"{prefix}_{script_type}_count": 0 for script_type in SCRIPT_TYPES}


def median_or_none(values: list[float | int]) -> float | None:
    return float(statistics.median(values)) if values else None


def block_subsidy_sats(height: int, config: RpcConfig) -> int:
    """Deterministic block subsidy in satoshis (mirrors Bitcoin Core's
    GetBlockSubsidy). Computed from height alone, so it stays correct even
    for transactions whose fee is 'unavailable' and doesn't depend on
    RPC-reported fee data."""
    halvings = height // config.halving_interval_blocks
    if halvings >= 64:
        return 0
    return config.initial_subsidy_sats >> halvings


# ==============================================================================
# TRANSACTION AGGREGATION
# ==============================================================================


def aggregate_transaction(
    tx: dict[str, Any],
    *,
    block_hash: str,
    block_height: int,
    block_time: int,
    tx_index: int,
    config: RpcConfig,
) -> dict[str, Any]:
    vins = tx.get("vin") or []
    vouts = tx.get("vout") or []

    is_coinbase = bool(vins and vins[0].get("coinbase") is not None)

    vin_type_counts = init_script_type_counts("vin_type")
    vout_type_counts = init_script_type_counts("vout_type")

    input_values_sats: list[int] = []
    input_ages_blocks: list[int] = []
    input_age_value_pairs: list[tuple[int, int]] = []

    prevout_value_known_count = 0
    prevout_height_known_count = 0
    generated_input_count = 0

    witness_input_count = 0
    witness_item_count = 0
    witness_data_bytes = 0
    scriptsig_bytes = 0
    signals_rbf = False

    for vin in vins:
        witness = vin.get("txinwitness") or []
        if witness:
            witness_input_count += 1
            witness_item_count += len(witness)
            witness_data_bytes += sum(hex_data_bytes(item) for item in witness)

        script_sig = vin.get("scriptSig") or {}
        scriptsig_bytes += hex_data_bytes(script_sig.get("hex"))

        sequence = vin.get("sequence")
        if (
            not is_coinbase
            and sequence is not None
            and int(sequence) < BIP125_RBF_SEQUENCE_THRESHOLD
        ):
            signals_rbf = True

        prevout = vin.get("prevout")
        if not isinstance(prevout, dict):
            continue

        if prevout.get("generated") is True:
            generated_input_count += 1

        prevout_value_sats = btc_to_sats(prevout.get("value"))
        if prevout_value_sats is not None:
            prevout_value_known_count += 1
            input_values_sats.append(prevout_value_sats)

        prevout_height = prevout.get("height")
        if prevout_height is not None:
            prevout_height_known_count += 1
            age = max(0, block_height - int(prevout_height))
            input_ages_blocks.append(age)
            if prevout_value_sats is not None:
                input_age_value_pairs.append((age, prevout_value_sats))

        prevout_script = prevout.get("scriptPubKey") or {}
        script_type = normalized_script_type(prevout_script.get("type"))
        vin_type_counts[f"vin_type_{script_type}_count"] += 1

    output_values_sats: list[int] = []
    scriptpubkey_bytes = 0
    op_return_count = 0
    op_return_script_bytes = 0
    zero_value_output_count = 0
    coinbase_output_addresses: list[str] = []

    for vout in vouts:
        value_sats = btc_to_sats(vout.get("value"))
        if value_sats is not None:
            output_values_sats.append(value_sats)
            if value_sats == 0:
                zero_value_output_count += 1

        script_pub_key = vout.get("scriptPubKey") or {}
        script_hex = script_pub_key.get("hex")
        script_bytes = hex_data_bytes(script_hex)
        scriptpubkey_bytes += script_bytes

        script_type = normalized_script_type(script_pub_key.get("type"))
        vout_type_counts[f"vout_type_{script_type}_count"] += 1

        if script_type == "nulldata":
            op_return_count += 1
            op_return_script_bytes += script_bytes

        # Payout addresses are only kept for the coinbase transaction - this
        # is the field the mining pool extractor's address-fallback match
        # reads (see btc_parser_app.rpc.mining_pools.PoolMatcher.match).
        if is_coinbase:
            address = script_pub_key.get("address")
            if address:
                coinbase_output_addresses.append(address)

    input_value_sats = sum(input_values_sats) if input_values_sats else 0
    output_value_sats = sum(output_values_sats) if output_values_sats else 0

    prevout_values_complete = is_coinbase or prevout_value_known_count == len(vins)
    prevout_heights_complete = is_coinbase or prevout_height_known_count == len(vins)

    rpc_fee = tx.get("fee")
    if is_coinbase:
        fee_sats = 0
        fee_source = "coinbase"
    elif rpc_fee is not None:
        fee_sats = btc_to_sats(rpc_fee)
        fee_source = "rpc"
    elif prevout_values_complete:
        fee_sats = input_value_sats - output_value_sats
        fee_source = "derived"
    else:
        fee_sats = None
        fee_source = "unavailable"

    vsize = int(tx.get("vsize") or 0)
    fee_rate_sat_vb = (
        float(fee_sats / vsize)
        if fee_sats is not None and vsize > 0 and not is_coinbase
        else None
    )

    input_age_avg = (
        float(sum(input_ages_blocks) / len(input_ages_blocks))
        if input_ages_blocks
        else None
    )

    weighted_age_denominator = sum(value for _, value in input_age_value_pairs)
    input_age_value_weighted_avg = (
        float(
            sum(age * value for age, value in input_age_value_pairs)
            / weighted_age_denominator
        )
        if weighted_age_denominator > 0
        else None
    )

    # Coin days destroyed: sum of (input age in days * value moved, in BTC).
    # Age is approximated as age_blocks / blocks_per_day rather than a real
    # timestamp diff, to avoid an extra RPC lookup per input for prevout time.
    coin_days_destroyed_btc = (
        float(
            sum(age * value for age, value in input_age_value_pairs)
            / config.blocks_per_day
        )
        / float(SATOSHIS_PER_BTC)
        if input_age_value_pairs
        else 0.0
    )

    coinbase_script_sig_hex = None
    if is_coinbase and vins:
        coinbase_script_sig_hex = vins[0].get("coinbase")

    event: dict[str, Any] = {
        # Identity / block relationship
        "block_hash": block_hash,
        "block_height": block_height,
        "block_time": block_time,
        "tx_index": tx_index,
        "txid": tx.get("txid"),
        "wtxid": tx.get("hash"),
        "is_coinbase": is_coinbase,
        # Native transaction metadata
        "version": tx.get("version"),
        "size": tx.get("size"),
        "vsize": tx.get("vsize"),
        "weight": tx.get("weight"),
        "locktime": tx.get("locktime"),
        # Fee/value KPIs
        "fee_sats": fee_sats,
        "fee_source": fee_source,
        "fee_rate_sat_vb": fee_rate_sat_vb,
        "input_value_sats": input_value_sats if not is_coinbase else None,
        "output_value_sats": output_value_sats,
        "output_value_min_sats": min(output_values_sats)
        if output_values_sats
        else None,
        "output_value_max_sats": max(output_values_sats)
        if output_values_sats
        else None,
        "output_value_avg_sats": (
            float(output_value_sats / len(output_values_sats))
            if output_values_sats
            else None
        ),
        # Structure
        "vin_count": len(vins),
        "vout_count": len(vouts),
        "prevout_value_known_count": prevout_value_known_count,
        "prevout_height_known_count": prevout_height_known_count,
        "prevout_values_complete": prevout_values_complete,
        "prevout_heights_complete": prevout_heights_complete,
        "generated_input_count": generated_input_count,
        # Input age / coin age
        "input_age_min_blocks": min(input_ages_blocks) if input_ages_blocks else None,
        "input_age_max_blocks": max(input_ages_blocks) if input_ages_blocks else None,
        "input_age_avg_blocks": input_age_avg,
        "input_age_value_weighted_avg_blocks": input_age_value_weighted_avg,
        "coin_days_destroyed_btc": coin_days_destroyed_btc,
        # Witness / script aggregate KPIs
        "has_witness": witness_input_count > 0,
        "witness_input_count": witness_input_count,
        "witness_item_count": witness_item_count,
        "witness_data_bytes": witness_data_bytes,
        "scriptsig_bytes": scriptsig_bytes,
        "scriptpubkey_bytes": scriptpubkey_bytes,
        "signals_rbf": signals_rbf,
        "op_return_count": op_return_count,
        "op_return_script_bytes": op_return_script_bytes,
        "zero_value_output_count": zero_value_output_count,
        # Small, bounded and useful special-case fields (coinbase only)
        "coinbase_script_sig_hex": coinbase_script_sig_hex,
        "coinbase_output_addresses_json": (
            json_dumps_compact(coinbase_output_addresses) if is_coinbase else None
        ),
    }

    event.update(vin_type_counts)
    event.update(vout_type_counts)

    event["has_taproot_input"] = event["vin_type_witness_v1_taproot_count"] > 0
    event["has_taproot_output"] = event["vout_type_witness_v1_taproot_count"] > 0
    event["has_op_return"] = op_return_count > 0

    return event


# ==============================================================================
# BLOCK AGGREGATION
# ==============================================================================


def _match_mining_pool(
    coinbase_tx: dict[str, Any] | None, pool_matcher: PoolMatcher | None
) -> PoolMatch:
    if pool_matcher is None or coinbase_tx is None:
        return UNKNOWN_MATCH

    addresses_json = coinbase_tx.get("coinbase_output_addresses_json")
    addresses: list[str] = json.loads(addresses_json) if addresses_json else []
    return pool_matcher.match(coinbase_tx.get("coinbase_script_sig_hex"), addresses)


def aggregate_block(
    block: dict[str, Any],
    config: RpcConfig,
    pool_matcher: PoolMatcher | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Produce exactly two exportable event types: one block event and one
    transaction event per transaction. Individual vin/vout events are
    intentionally not exported.

    `pool_matcher` is optional so the RPC parser degrades gracefully (pool_*
    fields come back None) if config/pools-v2.json is missing rather than
    hard-failing the whole backfill.
    """
    block_hash = str(block["hash"])
    block_height = int(block["height"])
    block_time = int(block["time"])

    tx_events = [
        aggregate_transaction(
            tx,
            block_hash=block_hash,
            block_height=block_height,
            block_time=block_time,
            tx_index=tx_index,
            config=config,
        )
        for tx_index, tx in enumerate(block.get("tx") or [])
    ]

    regular_txs = [tx for tx in tx_events if not tx["is_coinbase"]]
    coinbase_tx = tx_events[0] if tx_events else None

    pool_match = _match_mining_pool(coinbase_tx, pool_matcher)

    known_fees = [
        int(tx["fee_sats"]) for tx in regular_txs if tx["fee_sats"] is not None
    ]
    known_fee_rates = [
        float(tx["fee_rate_sat_vb"])
        for tx in regular_txs
        if tx["fee_rate_sat_vb"] is not None
    ]
    regular_vsizes = [int(tx["vsize"] or 0) for tx in regular_txs]

    total_fees_sats = sum(known_fees)
    total_regular_vsize = sum(regular_vsizes)

    input_value_known_txs = [tx for tx in regular_txs if tx["prevout_values_complete"]]

    block_event: dict[str, Any] = {
        # Original block fields
        # NOTE: confirmations and nextblockhash are intentionally omitted.
        # Both are current-chain-state snapshots, not intrinsic properties of
        # the block, and go stale/misleading once written to a historical CSV
        # (confirmations balloons on backfill, nextblockhash can be wrong
        # after a reorg). See Script_plan.md.
        "hash": block.get("hash"),
        "height": block.get("height"),
        "version": block.get("version"),
        "versionHex": block.get("versionHex"),
        "merkleroot": block.get("merkleroot"),
        "time": block.get("time"),
        # Filled in by the caller once the next-older block's time is known.
        "time_since_prev_block_sec": None,
        "mediantime": block.get("mediantime"),
        "nonce": block.get("nonce"),
        "bits": block.get("bits"),
        "target": block.get("target"),
        "difficulty": float(block["difficulty"])
        if block.get("difficulty") is not None
        else None,
        "chainwork": block.get("chainwork"),
        "chainwork_log2": (
            math.log2(int(block["chainwork"], 16)) if block.get("chainwork") else None
        ),
        "nTx": block.get("nTx"),
        "previousblockhash": block.get("previousblockhash"),
        "strippedsize": block.get("strippedsize"),
        "size": block.get("size"),
        "weight": block.get("weight"),
        "weight_utilization_pct": (
            float(block["weight"]) / config.max_block_weight * 100
            if block.get("weight") is not None
            else None
        ),
        "block_subsidy_sats": block_subsidy_sats(block_height, config),
        # Mining pool attribution (see btc_parser_app.rpc.mining_pools)
        "pool_id": pool_match.pool_id,
        "pool_name": pool_match.pool_name,
        "pool_link": pool_match.pool_link,
        "pool_match_method": pool_match.match_method,
        # Transaction/value aggregates
        "regular_tx_count": len(regular_txs),
        "total_vin_count": sum(int(tx["vin_count"]) for tx in tx_events),
        "total_vout_count": sum(int(tx["vout_count"]) for tx in tx_events),
        "coinbase_value_sats": (
            int(coinbase_tx["output_value_sats"]) if coinbase_tx else None
        ),
        "coinbase_vout_count": int(coinbase_tx["vout_count"]) if coinbase_tx else None,
        "total_fees_sats": total_fees_sats,
        "fee_known_tx_count": len(known_fees),
        "fee_unknown_tx_count": len(regular_txs) - len(known_fees),
        "fee_avg_sats": (
            float(total_fees_sats / len(known_fees)) if known_fees else None
        ),
        "fee_median_sats": median_or_none(known_fees),
        "fee_max_sats": max(known_fees) if known_fees else None,
        "fee_rate_avg_sat_vb": (
            float(sum(known_fee_rates) / len(known_fee_rates))
            if known_fee_rates
            else None
        ),
        "fee_rate_median_sat_vb": median_or_none(known_fee_rates),
        "fee_rate_max_sat_vb": max(known_fee_rates) if known_fee_rates else None,
        "effective_fee_rate_sat_vb": (
            float(total_fees_sats / total_regular_vsize)
            if known_fees
            and len(known_fees) == len(regular_txs)
            and total_regular_vsize > 0
            else None
        ),
        # Aggregate flow values. These are transaction-flow sums, not "new BTC".
        "regular_input_value_sats": sum(
            int(tx["input_value_sats"] or 0) for tx in input_value_known_txs
        ),
        "input_value_complete_tx_count": len(input_value_known_txs),
        "regular_output_value_sats": sum(
            int(tx["output_value_sats"]) for tx in regular_txs
        ),
        "coin_days_destroyed_btc": sum(
            float(tx["coin_days_destroyed_btc"]) for tx in tx_events
        ),
        # Transaction shape
        "tx_vsize_avg": (
            float(sum(int(tx["vsize"] or 0) for tx in tx_events) / len(tx_events))
            if tx_events
            else None
        ),
        "tx_vsize_median": median_or_none([int(tx["vsize"] or 0) for tx in tx_events]),
        "tx_vsize_max": (
            max(int(tx["vsize"] or 0) for tx in tx_events) if tx_events else None
        ),
        # Feature adoption / behavior
        "segwit_tx_count": sum(bool(tx["has_witness"]) for tx in tx_events),
        "rbf_tx_count": sum(bool(tx["signals_rbf"]) for tx in regular_txs),
        "taproot_input_tx_count": sum(
            bool(tx["has_taproot_input"]) for tx in regular_txs
        ),
        "taproot_output_tx_count": sum(
            bool(tx["has_taproot_output"]) for tx in tx_events
        ),
        "op_return_tx_count": sum(bool(tx["has_op_return"]) for tx in tx_events),
        "op_return_output_count": sum(int(tx["op_return_count"]) for tx in tx_events),
        "op_return_script_bytes": sum(
            int(tx["op_return_script_bytes"]) for tx in tx_events
        ),
        "witness_input_count": sum(int(tx["witness_input_count"]) for tx in tx_events),
        "witness_item_count": sum(int(tx["witness_item_count"]) for tx in tx_events),
        "witness_data_bytes": sum(int(tx["witness_data_bytes"]) for tx in tx_events),
        "scriptsig_bytes": sum(int(tx["scriptsig_bytes"]) for tx in tx_events),
        "scriptpubkey_bytes": sum(int(tx["scriptpubkey_bytes"]) for tx in tx_events),
    }

    # Block-level script type distributions - accumulated in a single pass
    # over tx_events instead of one full sum()-over-tx_events traversal per
    # script type (was 2 * len(SCRIPT_TYPES) separate passes over the same
    # list for a block with potentially thousands of transactions).
    vin_type_keys = [f"vin_type_{t}_count" for t in SCRIPT_TYPES]
    vout_type_keys = [f"vout_type_{t}_count" for t in SCRIPT_TYPES]
    vin_type_totals = dict.fromkeys(vin_type_keys, 0)
    vout_type_totals = dict.fromkeys(vout_type_keys, 0)
    for tx in tx_events:
        for key in vin_type_keys:
            vin_type_totals[key] += int(tx[key])
        for key in vout_type_keys:
            vout_type_totals[key] += int(tx[key])
    block_event.update(vin_type_totals)
    block_event.update(vout_type_totals)

    return block_event, tx_events
