#!/usr/bin/env bash
# btc-splunk-bench.sh - Splunk performance benchmark for index=btc_parser.
#
# The searches mirror the BTC Mempool Explorer dashboard and its scheduled
# searches, rewritten for the indexed-field concept of docs chapter 10.6:
# tstats only over fields that are indexed (all of btc:blocks / btc:prices /
# btc:stale_block_headers / btc:sanctioned_tx_events; on transactions/
# inputs/outputs only block_height, is_coinbase, version, vin_count,
# vout_count, prevout_type, prevout_generated, relative_locktime_type,
# script_type). Everything else - fee rates, values, addresses - is a raw
# search, bounded to a fixed time window. Every search also runs on an
# index where every column is indexed, so a run before and after a
# re-index compares like with like.
#
# Usage:
#   SPLUNK_AUTH='user:pass' btc-splunk-bench.sh <label> [GROUP...]
#       GROUP: L B P S T R C H (default: all but H)
#       -> /root/bench_<label>.txt (readable) and /root/bench_<label>.csv
#   btc-splunk-bench.sh --compare <labelA> <labelB>    side by side from the CSVs
#
# Environment:
#   SPLUNK_URL      default https://127.0.0.1:8089
#   BENCH_INDEX     default btc_parser
#   BENCH_TIMEOUT   seconds per search, default 1800 (status TIMEOUT)
#   BENCH_DIR       where results go, default /root
#
# Groups:
#   L  lookups by hash/address/indexed value (raw-term index + bloom filters)
#   B  btc:blocks - the dashboard's chain panels as tstats, plus its raw base search
#   P  btc:prices tstats
#   S  sanctioned tx events / stale headers
#   T  tstats over the indexed detail fields, all time
#   R  raw searches replacing the dashboard's former tstats on now
#      search-time fields (fees, values, addresses), fixed windows
#   C  concurrency: 4 heavy tstats at once
#   H  heavy all-time raw searches (balances, unspent, yearly percentiles) -
#      expect TIMEOUT; only when named explicitly
#
# The fixed windows are in 2024, so compare runs only once the backfill has
# passed 2025-01-01 (check: | tstats max(_time) where index=btc_parser by sourcetype).

set -uo pipefail

BENCH_DIR="${BENCH_DIR:-/root}"

if [[ "${1:-}" == --compare ]]; then
  a="$BENCH_DIR/bench_${2:?label A}.csv"; b="$BENCH_DIR/bench_${3:?label B}.csv"
  printf '%-5s %10s %10s %8s  %10s %10s\n' name "$2 s" "$3 s" ratio "$2 rows" "$3 rows"
  awk -F, 'NR==FNR { if (FNR > 1) { s[$2] = $4; r[$2] = $5 }; next }
           FNR > 1 { ratio = (s[$2] > 0 && $4 > 0) ? sprintf("%.2fx", $4 / s[$2]) : "-"
                     printf "%-5s %10s %10s %8s  %10s %10s\n", $2, s[$2], $4, ratio, r[$2], $5 }' "$a" "$b"
  exit 0
fi

: "${SPLUNK_AUTH:?export SPLUNK_AUTH=user:pass first}"
LABEL="${1:-run}"; shift || true
BENCH_GROUPS=("$@"); [[ ${#BENCH_GROUPS[@]} -gt 0 ]] || BENCH_GROUPS=(L B P S T R C)
SPLUNK_URL="${SPLUNK_URL:-https://127.0.0.1:8089}"
IDX="${BENCH_INDEX:-btc_parser}"
TIMEOUT="${BENCH_TIMEOUT:-1800}"
OUT="$BENCH_DIR/bench_${LABEL}.txt"
CSV="$BENCH_DIR/bench_${LABEL}.csv"
[[ -f "$CSV" ]] || echo "label,name,group,seconds,rows,status" >"$CSV"

want() { local g; for g in "${BENCH_GROUPS[@]}"; do [[ "$g" == "$1" ]] && return 0; done; return 1; }

run() {  # <name> <spl>
  local name="$1" spl="${2//\$IDX/$IDX}" s e res code rc rows status first tmp
  tmp="$(mktemp)"
  s=$(date +%s.%N)
  code=$(curl -sk -u "$SPLUNK_AUTH" --max-time "$TIMEOUT" -o "$tmp" -w '%{http_code}' \
         "$SPLUNK_URL/services/search/jobs/export" \
         --data-urlencode "search=$spl" -d earliest_time=0 -d output_mode=csv)
  rc=$?
  e=$(date +%s.%N)
  res="$(cat "$tmp")"; rm -f "$tmp"
  rows=$(( $(printf '%s\n' "$res" | grep -c .) - 1 )); (( rows < 0 )) && rows=0
  if [[ $rc -eq 28 ]]; then status=TIMEOUT
  elif [[ $rc -ne 0 ]]; then status="CURL$rc"
  elif [[ "$code" != 200 ]]; then status="HTTP$code"
  elif printf '%s' "$res" | grep -q '<msg type="\(ERROR\|FATAL\)"'; then status=SPLERR
  else status=ok; fi
  first="$(printf '%s\n' "$res" | sed -n 2p | cut -c1-80)"
  [[ "$status" == SPLERR || "$status" == HTTP* ]] && first="$(printf '%s' "$res" | grep -o '<msg[^<]*' | head -1 | cut -c1-80)"
  printf '%-4s %8.1fs  rows=%-7s %-7s first: %s\n' "$name" "$(awk "BEGIN{print $e - $s}")" "$rows" "$status" "$first" | tee -a "$OUT"
  printf '%s,%s,%s,%.1f,%s,%s\n' "$LABEL" "$name" "${name:0:1}" "$(awk "BEGIN{print $e - $s}")" "$rows" "$status" >>"$CSV"
}

# Fixed UTC windows (2024): halving day/week, January, the whole year.
DAY="earliest=1713571200 latest=1713657600"     # 2024-04-20 (halving block 840000)
WEEK="earliest=1713484800 latest=1714089600"    # 2024-04-19 .. 2024-04-26
MONTH="earliest=1704067200 latest=1706745600"   # 2024-01
YEAR="earliest=1704067200 latest=1735689600"    # 2024

PIZZA_TX=a1075db55d416d3ca199f55b6084e2115b9345e16c5cf302fc80e9d5fbf5d48d
OLD_L1_TX=b3794659c100f82ca4ea9ca8bd66c80e03343521a10ed87cfb936c69f42ff5f1
GENESIS_ADDR=1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
BLOCK_840000=0000000000000000000320283a032748cef8227873ff4872689bf23f1cda83a5

pipes="$( (/opt/splunk/bin/splunk btool limits list search 2>/dev/null || true) | grep -E '^batch_search_max_pipeline' || echo 'batch_search_max_pipeline = 1 (default)')"
echo "=== $(date '+%F %T')  label=$LABEL  groups=${BENCH_GROUPS[*]}  timeout=${TIMEOUT}s  $pipes" | tee -a "$OUT"
run I0 '| tstats count min(_time) as min max(_time) as max where index=$IDX by sourcetype | eval min=strftime(min,"%F"), max=strftime(max,"%F") | stats values(eval(sourcetype.":".count.":".min."..".max)) as coverage | eval coverage=mvjoin(coverage," ")'

# --- L: lookups (raw-term index + bloom filters / indexed terms) -------------
if want L; then
  run L1 "search index=\$IDX txid=$OLD_L1_TX | stats count"
  run L2 "search index=\$IDX $GENESIS_ADDR | stats count by sourcetype"
  run L3 "search index=\$IDX txid=$PIZZA_TX | stats count by sourcetype"
  run L4 "search index=\$IDX sourcetype=btc:inputs prevout_txid=$PIZZA_TX | table _time block_height txid input_index value_sats"
  run L5 "search index=\$IDX block_hash=$BLOCK_840000 | stats count by sourcetype"
  run L6 'search index=$IDX sourcetype=btc:transactions block_height=840000 | stats count sum(fee_sats) as fees'
  run L7 'search index=$IDX sourcetype=btc:outputs script_type=witness_unknown | stats count min(_time) as first'
  run L8 'search index=$IDX sourcetype=btc:blocks height=840000 | table height hash pool_name nTx total_fees_sats'
fi

# --- B: btc:blocks (every field indexed) ------------------------------------
if want B; then
  run B1 '| tstats count where index=$IDX sourcetype=btc:blocks'
  run B2 'search index=$IDX sourcetype=btc:blocks | eval _it=_indextime | sort 0 - _it | dedup height | stats count max(height) as tip'
  run B3 '| tstats sum(total_fees_sats) as f sum(block_subsidy_sats) as s sum(regular_tx_count) as n where index=$IDX sourcetype=btc:blocks by _time span=1mon | eval avg_fee=round(f/n,0), fee_share=round(f/(f+s)*100,2)'
  run B4 '| tstats sum(segwit_tx_count) as sw sum(taproot_input_tx_count) as tpi sum(taproot_output_tx_count) as tpo sum(rbf_tx_count) as rbf sum(regular_tx_count) as n where index=$IDX sourcetype=btc:blocks by _time span=1mon | eval segwit=round(sw/n*100,2), tp_out=round(tpo/n*100,2), rbf=round(rbf/n*100,2)'
  run B5 '| tstats sum(vout_type_witness_v0_keyhash_count) as p2wpkh sum(vout_type_witness_v1_taproot_count) as p2tr sum(vout_type_pubkeyhash_count) as p2pkh sum(vout_type_scripthash_count) as p2sh sum(vout_type_nulldata_count) as op_return where index=$IDX sourcetype=btc:blocks by _time span=1mon'
  run B6 '| tstats count as blocks avg(total_fees_sats) as af avg(fee_rate_median_sat_vb) as amf avg(weight_utilization_pct) as full where index=$IDX sourcetype=btc:blocks by pool_name | eventstats sum(blocks) as total | eval share=round(blocks/total*100,2) | sort - blocks'
  run B7 '| tstats sum(difficulty) as sd sum(time_since_prev_block_sec) as ss where index=$IDX sourcetype=btc:blocks by _time span=1d | eval hr=round(sd*pow(2,32)/ss/1e18,1)'
  run B8 '| tstats dc(hash) as n values(pool_name) as pools where index=$IDX sourcetype=btc:blocks by height | where n>1'
  run B9 '| tstats max(_time) as t where index=$IDX sourcetype=btc:blocks by height hash pool_name nTx total_fees_sats size time_since_prev_block_sec fee_rate_median_sat_vb | eventstats max(nTx) as a max(total_fees_sats) as b max(size) as c | where nTx==a OR total_fees_sats==b OR size==c'
  run B10 '| tstats sum(total_fees_sats) as f sum(block_subsidy_sats) as s avg(usd) as usd where index=$IDX (sourcetype=btc:blocks OR sourcetype=btc:prices) by _time span=1mon | eval fees_usd=round(f/1e8*usd,0)'
  run B11 '| tstats sum(op_return_output_count) as c sum(op_return_script_bytes) as b where index=$IDX sourcetype=btc:blocks by _time span=1mon'
fi

# --- P: prices --------------------------------------------------------------
if want P; then
  run P1 '| tstats earliest(usd) as open max(usd) as high min(usd) as low latest(usd) as close where index=$IDX sourcetype=btc:prices by _time span=1d'
  run P2 '| tstats avg(usd) as USD avg(eur) as EUR where index=$IDX sourcetype=btc:prices earliest=-24h by _time span=1h'
  run P3 '| tstats max(usd) as ath min(usd) as atl where index=$IDX sourcetype=btc:prices'
fi

# --- S: sanctioned events / stale headers -----------------------------------
if want S; then
  run S1 'search index=$IDX sourcetype=btc:sanctioned_tx_events | dedup txid direction io_index | stats dc(txid) as txs dc(address) as addrs sum(value_sats) as v'
  run S2 '| tstats count where index=$IDX sourcetype=btc:sanctioned_tx_events by event'
  run S3 '| tstats latest(status) as status max(branchlen) as branchlen min(_time) as first where index=$IDX sourcetype=btc:stale_block_headers by height hash'
fi

# --- T: tstats over the indexed detail fields, all time ----------------------
if want T; then
  run T1 '| tstats count where index=$IDX by sourcetype'
  run T2 '| tstats count where index=$IDX sourcetype=btc:outputs by _time script_type span=1mon'
  run T3 '| tstats count where index=$IDX sourcetype=btc:inputs by _time prevout_type span=1y'
  run T4 '| tstats count where index=$IDX sourcetype=btc:transactions is_coinbase=false by _time version span=1mon'
  run T5 '| tstats count where index=$IDX sourcetype=btc:transactions is_coinbase=false by vin_count vout_count | sort 20 - count'
  run T6 '| tstats count where index=$IDX sourcetype=btc:transactions vin_count>=100 by _time span=1mon'
  run T7 '| tstats count where index=$IDX sourcetype=btc:inputs relative_locktime_type=* by _time relative_locktime_type span=1mon'
  run T8 '| tstats count where index=$IDX sourcetype=btc:inputs prevout_generated=true by _time span=1y'
  run T9 '| tstats count where index=$IDX sourcetype=btc:outputs block_height>=840000 block_height<840010 by block_height'
  run T10 '| tstats count where index=$IDX sourcetype=btc:outputs is_coinbase=true by _time span=1y'
fi

# --- R: raw searches for the formerly-tstats panels (fixed windows) ----------
if want R; then
  run R1 "search index=\$IDX sourcetype=btc:transactions $WEEK | stats count avg(fee_rate_sat_vb) AS avg_rate max(fee_rate_sat_vb) AS max_rate"
  run R2 "search index=\$IDX sourcetype=btc:outputs is_op_return=true $YEAR | stats count sum(script_bytes) AS bytes"
  run R2b "search index=\$IDX sourcetype=btc:outputs script_type=nulldata $YEAR | stats count sum(script_bytes) AS bytes"
  run R3 "search index=\$IDX sourcetype=btc:transactions is_coinbase=false fee_sats>0 $WEEK | timechart span=1d min(fee_rate_sat_vb) as Min perc10(fee_rate_sat_vb) as p10 perc25(fee_rate_sat_vb) as p25 median(fee_rate_sat_vb) as Median perc75(fee_rate_sat_vb) as p75 perc90(fee_rate_sat_vb) as p90"
  run R4 "search index=\$IDX sourcetype=btc:transactions is_coinbase=false fee_rate_sat_vb=* $WEEK | eval fr=tonumber(fee_rate_sat_vb), vmb=vsize/1000000 | eval band=case(fr<2,\"<2\", fr<5,\"2-5\", fr<10,\"5-10\", fr<30,\"10-30\", fr<100,\"30-100\", true(),\"100+\") | timechart span=1d sum(vmb) by band limit=0"
  run R5 "search index=\$IDX sourcetype=btc:inputs is_coinbase=false input_age_blocks=* $WEEK | eval age=tonumber(input_age_blocks), btc=value_sats/1e8 | eval band=case(age<144,\"<1d\", age<4320,\"1d-1m\", age<52560,\"1m-1y\", age<262800,\"1-5y\", true(),\"5y+\") | timechart span=1d sum(btc) by band limit=0"
  run R6 "search index=\$IDX sourcetype=btc:transactions is_coinbase=false $MONTH | stats max(fee_sats) as max_fee_sats max(fee_rate_sat_vb) as max_fee_rate max(vsize) as max_vsize max(vin_count) as max_vin max(vout_count) as max_vout max(input_age_max_blocks) as max_age max(coin_days_destroyed_btc) as max_cdd max(output_value_sats) as max_out"
  run R7 "search index=\$IDX sourcetype=btc:transactions is_coinbase=false fee_sats>0 $MONTH | eventstats min(fee_sats) as m | where fee_sats==m | sort 25 fee_rate_sat_vb | table _time block_height txid fee_sats fee_rate_sat_vb vsize"
  run R8 "search index=\$IDX (sourcetype=btc:outputs OR (sourcetype=btc:inputs is_coinbase=false)) $MONTH | stats max(value_sats) as max_sats by sourcetype"
  run R9 "search index=\$IDX ((sourcetype=btc:outputs address=$GENESIS_ADDR) OR (sourcetype=btc:inputs prevout_address=$GENESIS_ADDR)) | eval d=if(sourcetype==\"btc:outputs\", value_sats, -value_sats) | timechart span=1mon sum(d) as d | streamstats sum(d) as bal"
  run R10 'search index=$IDX (sourcetype=btc:outputs OR sourcetype=btc:inputs) [| inputlookup sanctioned_addresses.csv | eval search="address=\"".address."\" OR prevout_address=\"".address."\"" | fields search] | eval addr=coalesce(address, prevout_address) | stats min(_time) as t sum(value_sats) as v values(addr) as addresses by txid sourcetype'
  run R11 "search index=\$IDX sourcetype=btc:outputs $MONTH | stats count sum(value_sats) AS sats by address | sort - sats | head 20"
  run R12 "search index=\$IDX sourcetype=btc:outputs $MONTH | stats dc(address) AS addresses"
  run R13 "search index=\$IDX sourcetype=btc:transactions block_height=840000 | stats count max(fee_sats) as max_fee median(fee_rate_sat_vb) as med_rate"
fi

# --- C: concurrency, 4 heavy tstats at once ---------------------------------
if want C; then
  s=$(date +%s)
  run C1 '| tstats count where index=$IDX by sourcetype' &
  run C2 '| tstats count where index=$IDX sourcetype=btc:outputs by _time script_type span=1mon' &
  run C3 '| tstats count where index=$IDX sourcetype=btc:inputs by _time prevout_type span=1y' &
  run C4 '| tstats sum(total_fees_sats) as f sum(regular_tx_count) as n where index=$IDX sourcetype=btc:blocks by _time span=1mon' &
  wait
  echo "PAR  $(( $(date +%s) - s ))s total for C1-C4 in parallel" | tee -a "$OUT"
fi

# --- H: heavy all-time raw (opt-in) ------------------------------------------
if want H; then
  run H1 "search index=\$IDX sourcetype=btc:transactions is_coinbase=false fee_sats>0 $YEAR | timechart span=1mon perc10(fee_rate_sat_vb) as p10 median(fee_rate_sat_vb) as Median perc90(fee_rate_sat_vb) as p90"
  run H2 "search index=\$IDX sourcetype=btc:inputs is_coinbase=false input_age_blocks=* $YEAR | eval age=tonumber(input_age_blocks), btc=value_sats/1e8 | eval band=case(age<52560,\"<1y\", age<262800,\"1-5y\", true(),\"5y+\") | timechart span=1mon sum(btc) by band"
  run H3 'search index=$IDX (sourcetype=btc:outputs OR (sourcetype=btc:inputs is_coinbase=false)) | eval addr=coalesce(address, prevout_address), d=if(sourcetype=="btc:outputs", value_sats, -value_sats) | stats sum(d) as bal by addr | sort 25 - bal'
  run H4 'search index=$IDX value_sats>=10000000000 (sourcetype=btc:outputs OR (sourcetype=btc:inputs is_coinbase=false)) | eval key=if(sourcetype=="btc:outputs", txid.":".output_index, prevout_txid.":".prevout_vout) | stats values(address) as address max(value_sats) as value_sats min(eval(if(sourcetype=="btc:outputs", block_height, null()))) as created max(eval(if(sourcetype=="btc:inputs", block_height, null()))) as spent by key | where isnotnull(created) AND isnull(spent) | sort 25 - value_sats'
fi

echo "=== done $(date '+%F %T') -> $OUT, $CSV" | tee -a "$OUT"
