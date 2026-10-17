#!/usr/bin/env bash
# btc-splunk-enable-backfill.sh - turn on Splunk's consume-and-delete
# (`batch`, move_policy = sinkhole) inputs for btc_parser's export/rpc/
# parts, but only after checking that Splunk will keep what it eats.
#
# A sinkhole input deletes every file right after indexing it, and
# rpc-ingest never re-exports a block it has already exported - so a
# misconfigured index (wrong timestamps, age/size-based freezing) loses
# data that can only be rebuilt with a full re-parse. Hence: every check
# below must pass before --enable edits anything (see
# docs/12-splunk-backfill-indexierung.md for what each one means).
#
# Usage (as root):
#   btc-splunk-enable-backfill.sh --check  [options]   read-only: inventory + preflight
#   btc-splunk-enable-backfill.sh --enable [options]   preflight, then remove the
#                                                     `disabled` lines and restart Splunk
# Options:
#   --splunk-home DIR   instance that READS the files (has the batch inputs).
#                       Default: /opt/splunkforwarder if it has them, else /opt/splunk.
#   --index-home DIR    instance that INDEXES (has indexes.conf for the index).
#                       Default: /opt/splunk if present, else --splunk-home.
#   --inputs-conf FILE  inputs.conf to edit. Default: the one under
#                       <splunk-home>/etc/apps/*/local/ with stanzas matching --stanza-regex.
#   --stanza-regex RE   stanzas to enable (default: export/rpc/)
#   --snapshot-dir DIR  before enabling, hard-link snapshot (cp -al) every input
#                       directory into DIR - costs no space until Splunk deletes
#                       the originals; same filesystem required. Delete it once
#                       the counts in Splunk are verified.
#   --count-rows        (with --check) also count CSV data rows per input -
#                       reads every file, takes minutes on TBs.
#   --force             --enable even if a check FAILs.
#   --log FILE          default: /var/log/btc-splunk-enable-backfill.log
#
# Schedule a one-off run (systemd transient timer - nothing left behind):
#   systemd-run --unit=btc-splunk-enable --on-calendar='2026-10-18 01:00:00 Europe/Berlin' \
#     /opt/btc_parser/app/splunk/scripts/btc-splunk-enable-backfill.sh --enable
#   systemctl list-timers 'btc-splunk-enable*'      # verify
#   systemctl stop btc-splunk-enable.timer          # cancel
#   journalctl -u btc-splunk-enable                 # output afterwards

set -uo pipefail

MODE=""
SPLUNK_HOME_ARG=""
INDEX_HOME_ARG=""
INPUTS_CONF=""
STANZA_RE="export/rpc/"
SNAPSHOT_DIR=""
COUNT_ROWS=0
FORCE=0
LOG_FILE="/var/log/btc-splunk-enable-backfill.log"

# Oldest timestamp in the data (genesis block, 2009-01-03) minus a margin.
OLDEST_EPOCH=1230768000  # 2009-01-01T00:00:00Z
MIN_MAX_DAYS_AGO=$(( ( $(date +%s) - OLDEST_EPOCH ) / 86400 + 365 ))
MIN_RETENTION_SECS=$(( $(date +%s) - OLDEST_EPOCH + 5 * 365 * 86400 ))

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) MODE=check; shift ;;
    --enable) MODE=enable; shift ;;
    --splunk-home) SPLUNK_HOME_ARG="$2"; shift 2 ;;
    --index-home) INDEX_HOME_ARG="$2"; shift 2 ;;
    --inputs-conf) INPUTS_CONF="$2"; shift 2 ;;
    --stanza-regex) STANZA_RE="$2"; shift 2 ;;
    --snapshot-dir) SNAPSHOT_DIR="$2"; shift 2 ;;
    --count-rows) COUNT_ROWS=1; shift ;;
    --force) FORCE=1; shift ;;
    --log) LOG_FILE="$2"; shift 2 ;;
    -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ -n "$MODE" ]] || { echo "Pass --check or --enable (see --help)." >&2; exit 2; }
[[ "$(id -u)" -eq 0 ]] || { echo "Run as root (it reads/edits Splunk's config and restarts it)." >&2; exit 2; }

mkdir -p "$(dirname "$LOG_FILE")"
exec > >(tee -a "$LOG_FILE") 2>&1
echo
echo "===== $(date -Is) btc-splunk-enable-backfill.sh --$MODE ====="

FAILS=0
WARNS=0
ok()   { echo "  OK    $*"; }
warn() { echo "  WARN  $*"; WARNS=$((WARNS + 1)); }
fail() { echo "  FAIL  $*"; FAILS=$((FAILS + 1)); }
human() { numfmt --to=iec --suffix=B "${1:-0}" 2>/dev/null || echo "${1}B"; }

# --- locate instances --------------------------------------------------------

has_matching_inputs() {  # <splunk_home>
  grep -lsE "^\[batch://.*(${STANZA_RE})" "$1"/etc/apps/*/local/inputs.conf >/dev/null 2>&1
}

if [[ -n "$SPLUNK_HOME_ARG" ]]; then
  READ_HOME="$SPLUNK_HOME_ARG"
elif has_matching_inputs /opt/splunkforwarder; then
  READ_HOME=/opt/splunkforwarder
else
  READ_HOME=/opt/splunk
fi
if [[ -n "$INDEX_HOME_ARG" ]]; then
  INDEX_HOME="$INDEX_HOME_ARG"
elif [[ -x /opt/splunk/bin/splunk ]]; then
  INDEX_HOME=/opt/splunk
else
  INDEX_HOME="$READ_HOME"
fi

for home in "$READ_HOME" "$INDEX_HOME"; do
  [[ -x "$home/bin/splunk" ]] || { echo "No Splunk at $home (bin/splunk missing) - pass --splunk-home/--index-home." >&2; exit 2; }
done

owner_of() { stat -c %U "$1"; }
as_owner() {  # <splunk_home> cmd... : never run splunk as root on a non-root install
  local home="$1"; shift
  local user; user="$(owner_of "$home")"
  if [[ "$user" == root ]]; then "$@"; else runuser -u "$user" -- "$@"; fi
}
btool() {  # <splunk_home> <conf> <stanza>
  as_owner "$1" "$1/bin/splunk" btool "$2" list "$3" 2>/dev/null
}
conf_get() {  # <splunk_home> <conf> <stanza> <key> -> effective value (btool merges defaults)
  btool "$1" "$2" "$3" | awk -v k="$4" 'index($0, k " = ") == 1 { print substr($0, length(k) + 4); exit }'
}

echo "Reading instance (inputs): $READ_HOME  (version $(sed -n 's/^VERSION=//p' "$READ_HOME/etc/splunk.version"), owner $(owner_of "$READ_HOME"))"
echo "Indexing instance:         $INDEX_HOME  (version $(sed -n 's/^VERSION=//p' "$INDEX_HOME/etc/splunk.version"), owner $(owner_of "$INDEX_HOME"))"

if [[ -z "$INPUTS_CONF" ]]; then
  mapfile -t candidates < <(grep -lsE "^\[batch://.*(${STANZA_RE})" "$READ_HOME"/etc/apps/*/local/inputs.conf)
  if [[ ${#candidates[@]} -ne 1 ]]; then
    echo "Expected exactly one inputs.conf with [batch://...${STANZA_RE}...] stanzas under $READ_HOME/etc/apps/*/local/, found ${#candidates[@]}: ${candidates[*]:-none}. Pass --inputs-conf." >&2
    exit 2
  fi
  INPUTS_CONF="${candidates[0]}"
fi
echo "inputs.conf:               $INPUTS_CONF"

# Stanzas to enable: "<path glob>|<sourcetype>|<index>|<disabled value>"
mapfile -t STANZAS < <(awk -v re="$STANZA_RE" '
  function flush() { if (path != "") print path "|" st "|" idx "|" dis }
  /^\[/ { flush(); path = ""; st = ""; idx = "default"; dis = "0"
          if ($0 ~ /^\[batch:\/\// && $0 ~ re) { path = substr($0, 10, length($0) - 10) } next }
  path != "" && /^[[:space:]]*sourcetype[[:space:]]*=/ { sub(/^[^=]*=[[:space:]]*/, ""); st = $0 }
  path != "" && /^[[:space:]]*index[[:space:]]*=/      { sub(/^[^=]*=[[:space:]]*/, ""); idx = $0 }
  path != "" && /^[[:space:]]*disabled[[:space:]]*=/   { sub(/^[^=]*=[[:space:]]*/, ""); dis = $0 }
  END { flush() }' "$INPUTS_CONF")
[[ ${#STANZAS[@]} -gt 0 ]] || { echo "No [batch://...] stanzas matching '$STANZA_RE' in $INPUTS_CONF." >&2; exit 2; }

# --- inventory + checks ------------------------------------------------------

echo
echo "Inputs:"
TOTAL_BYTES=0
TOTAL_FILES=0
declare -A SEEN_INDEX=()
declare -A INPUT_DIRS=()
for entry in "${STANZAS[@]}"; do
  IFS='|' read -r path st idx dis <<<"$entry"
  dir="$(dirname "$path")"; pattern="$(basename "$path")"
  INPUT_DIRS["$dir"]=1
  files=$(find "$dir" -maxdepth 1 -type f -name "$pattern" 2>/dev/null | wc -l)
  bytes=$(find "$dir" -maxdepth 1 -type f -name "$pattern" -printf '%s\n' 2>/dev/null | awk '{s += $1} END {print s + 0}')
  TOTAL_BYTES=$((TOTAL_BYTES + bytes))
  TOTAL_FILES=$((TOTAL_FILES + files))
  state="enabled"; [[ "$dis" =~ ^(1|true|t|yes|y)$ ]] && state="DISABLED"
  printf '  %-9s %-18s index=%-12s %6d files %10s  %s\n' "$state" "$st" "$idx" "$files" "$(human "$bytes")" "$path"
  if [[ "$path" == *'$'* ]]; then
    fail "stanza path still contains a placeholder/variable: $path - replace it with the real absolute path in $INPUTS_CONF"
  elif [[ ! -d "$dir" ]]; then
    fail "input directory does not exist: $dir"
  fi
  if [[ $COUNT_ROWS -eq 1 && $files -gt 0 ]]; then
    rows=$(find "$dir" -maxdepth 1 -type f -name "$pattern" -print0 | xargs -0 cat | wc -l)
    echo "            data rows: $((rows - files))  (lines minus one header per file; rpc-ingest keeps adding parts at the tip)"
  fi
  SEEN_INDEX["$idx"]=1

  # props on the READING instance - INDEXED_EXTRACTIONS is parsed by
  # whoever reads the file, so this is where timestamps get assigned.
  ie="$(conf_get "$READ_HOME" props "$st" INDEXED_EXTRACTIONS)"
  mda="$(conf_get "$READ_HOME" props "$st" MAX_DAYS_AGO)"
  tf="$(conf_get "$READ_HOME" props "$st" TIMESTAMP_FIELDS)"
  if [[ "${ie,,}" != csv ]]; then
    fail "props [$st] on $READ_HOME: INDEXED_EXTRACTIONS='${ie}' - install TA-btc-parser-parsing on the instance that READS the files"
  else
    ok "props [$st]: INDEXED_EXTRACTIONS=csv, TIMESTAMP_FIELDS=$tf"
  fi
  if [[ -z "$mda" || "$mda" -lt $MIN_MAX_DAYS_AGO ]]; then
    fail "props [$st]: MAX_DAYS_AGO=${mda:-unset} < $MIN_MAX_DAYS_AGO - pre-$(date -d "@$(( $(date +%s) - ${mda:-2000} * 86400 ))" +%Y) events would get a wrong _time (use TA-btc-parser-parsing from this branch: 10951)"
  else
    ok "props [$st]: MAX_DAYS_AGO=$mda"
  fi
done
echo "  total: $TOTAL_FILES files, $(human "$TOTAL_BYTES")"
[[ $TOTAL_FILES -gt 0 ]] || fail "no part files found under any input path - nothing to backfill, so the paths are almost certainly wrong"
# Other stanzas in the same file (the monitor inputs) aren't touched by
# --enable, but an unresolved placeholder there means they read nothing.
while IFS= read -r bad; do
  warn "other stanza with an unresolved placeholder (reads nothing): $bad"
done < <(grep -E '^\[[a-z]+://[^]]*\$' "$INPUTS_CONF" | grep -vE "^\[batch://.*(${STANZA_RE})")

echo
echo "Index settings on $INDEX_HOME:"
for idx in "${!SEEN_INDEX[@]}"; do
  if [[ -z "$(btool "$INDEX_HOME" indexes "$idx")" ]]; then
    fail "index '$idx' not defined on $INDEX_HOME (install TA-btc-parser-indexes, or pass --index-home)"
    continue
  fi
  frozen="$(conf_get "$INDEX_HOME" indexes "$idx" frozenTimePeriodInSecs)"
  quar="$(conf_get "$INDEX_HOME" indexes "$idx" quarantinePastSecs)"
  maxmb="$(conf_get "$INDEX_HOME" indexes "$idx" maxTotalDataSizeMB)"
  maxds="$(conf_get "$INDEX_HOME" indexes "$idx" maxDataSize)"
  hot="$(conf_get "$INDEX_HOME" indexes "$idx" maxHotBuckets)"
  home="$(conf_get "$INDEX_HOME" indexes "$idx" homePath)"
  echo "  [$idx] homePath=$home maxDataSize=$maxds maxHotBuckets=$hot"
  if [[ -n "$frozen" && "$frozen" -ge $MIN_RETENTION_SECS ]]; then ok "frozenTimePeriodInSecs=$frozen"
  else fail "frozenTimePeriodInSecs=${frozen:-unset} < $MIN_RETENTION_SECS - buckets with 2009+ data would be frozen (deleted) on roll"; fi
  if [[ -n "$quar" && "$quar" -ge $MIN_RETENTION_SECS ]]; then ok "quarantinePastSecs=$quar"
  else fail "quarantinePastSecs=${quar:-unset} < $MIN_RETENTION_SECS - historic events would all go to the quarantine bucket"; fi
  db_dir="$(as_owner "$INDEX_HOME" "$INDEX_HOME/bin/splunk" envvars 2>/dev/null | tr ';' '\n' | sed -n 's/^ *export SPLUNK_DB=//p' | tr -d '"')"
  db_size_mb=$(( $(df -B1 --output=size "${db_dir:-$INDEX_HOME/var/lib/splunk}" 2>/dev/null | tail -1 | tr -d ' ' || echo 0) / 1048576 ))
  if [[ -n "$maxmb" && $db_size_mb -gt 0 && "$maxmb" -gt $db_size_mb ]]; then
    warn "maxTotalDataSizeMB=$maxmb is larger than the SPLUNK_DB filesystem ($db_size_mb MB) - Splunk pauses indexing at minFreeSpace before the cap ever applies; set it to what the volume can hold (local/indexes.conf)"
  fi
  need_mb=$(( TOTAL_BYTES * 12 / 10 / 1048576 ))
  if [[ -n "$maxmb" && "$maxmb" -ge $need_mb ]]; then ok "maxTotalDataSizeMB=$maxmb (>= 1.2x current export, $need_mb)"
  else fail "maxTotalDataSizeMB=${maxmb:-unset} < $need_mb (1.2x the export) - Splunk would delete the oldest buckets to stay under it"; fi
  [[ "$maxds" == auto_high_volume ]] || warn "maxDataSize=$maxds - auto_high_volume (10 GB buckets) is better for this volume"
done

echo
echo "Throughput settings:"
for home in $(printf '%s\n' "$READ_HOME" "$INDEX_HOME" | sort -u); do
  kbps="$(conf_get "$home" limits thruput maxKBps)"
  pipes="$(conf_get "$home" server general parallelIngestionPipelines)"
  [[ "$kbps" == 0 ]] && ok "$home: limits [thruput] maxKBps=0" \
    || warn "$home: limits [thruput] maxKBps=$kbps - throttled; install TA-btc-parser-ingest-tuning"
  [[ "${pipes:-1}" -gt 1 ]] && ok "$home: parallelIngestionPipelines=$pipes" \
    || warn "$home: parallelIngestionPipelines=${pipes:-1} - one core's worth of parsing/indexing; install TA-btc-parser-ingest-tuning"
done

echo
echo "Host:"
splunk_db="$(as_owner "$INDEX_HOME" "$INDEX_HOME/bin/splunk" envvars 2>/dev/null | tr ';' '\n' | sed -n 's/^ *export SPLUNK_DB=//p' | tr -d '"')"
splunk_db="${splunk_db:-$INDEX_HOME/var/lib/splunk}"
db_free=$(df -B1 --output=avail "$splunk_db" 2>/dev/null | tail -1 | tr -d ' ')
echo "  SPLUNK_DB=$splunk_db  free: $(human "${db_free:-0}")"
first_dir="$(printf '%s\n' "${!INPUT_DIRS[@]}" | head -1)"
if [[ "$(stat -c %d "$splunk_db" 2>/dev/null)" == "$(stat -c %d "$first_dir" 2>/dev/null)" ]]; then
  echo "  (export and SPLUNK_DB share a filesystem: indexed CSVs are deleted as Splunk goes, freeing space)"
  [[ "${db_free:-0}" -ge $(( TOTAL_BYTES / 2 )) ]] && ok "free space >= 0.5x export" \
    || warn "free space < 0.5x export - indexed data can briefly outgrow what the deleted CSVs free up"
else
  [[ "${db_free:-0}" -ge $(( TOTAL_BYTES * 12 / 10 )) ]] && ok "free space >= 1.2x export" \
    || fail "free space on SPLUNK_DB < 1.2x export ($(human $(( TOTAL_BYTES * 12 / 10 ))))"
fi
thp="$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null)"
[[ "$thp" == *"[never]"* ]] && ok "transparent hugepages: never" \
  || warn "transparent hugepages: $thp - Splunk recommends 'never' (see docs chapter 12)"
pid="$(head -1 "$INDEX_HOME/var/run/splunk/splunkd.pid" 2>/dev/null)"
if [[ -n "$pid" && -r "/proc/$pid/limits" ]]; then
  nofile="$(awk '/Max open files/ {print $4}' "/proc/$pid/limits")"
  [[ "$nofile" == unlimited || "$nofile" -ge 64000 ]] && ok "splunkd open-files limit: $nofile" \
    || warn "splunkd open-files limit: $nofile (< 64000)"
fi
echo "  License: not checkable offline - make sure the license covers $(human "$TOTAL_BYTES") of ingest (see docs chapter 12)."

echo
echo "Preflight: $FAILS FAIL, $WARNS WARN"

if [[ "$MODE" == check ]]; then
  [[ $FAILS -eq 0 ]] && echo "Would enable: yes." || echo "Would enable: NO (fix the FAILs first)."
  exit $(( FAILS > 0 ))
fi

# --- enable ------------------------------------------------------------------

if [[ $FAILS -gt 0 && $FORCE -eq 0 ]]; then
  echo "Not enabling anything: $FAILS check(s) failed. Fix them (or pass --force) and re-run."
  exit 1
fi

if [[ -n "$SNAPSHOT_DIR" ]]; then
  echo "Hard-link snapshot of the input directories -> $SNAPSHOT_DIR"
  mkdir -p "$SNAPSHOT_DIR"
  for dir in "${!INPUT_DIRS[@]}"; do
    if [[ "$(stat -c %d "$dir")" != "$(stat -c %d "$SNAPSHOT_DIR")" ]]; then
      echo "  $SNAPSHOT_DIR is not on the same filesystem as $dir - a hard-link snapshot is impossible. Aborting."
      exit 1
    fi
    cp -al "$dir" "$SNAPSHOT_DIR/" || { echo "  cp -al $dir failed. Aborting."; exit 1; }
    echo "  $dir -> $SNAPSHOT_DIR/$(basename "$dir")"
  done
fi

backup="$INPUTS_CONF.before-enable.$(date +%Y%m%d%H%M%S)"
cp -p "$INPUTS_CONF" "$backup"
tmp="$(mktemp "$INPUTS_CONF.XXXXXX")"
awk -v re="$STANZA_RE" '
  /^\[/ { target = ($0 ~ /^\[batch:\/\// && $0 ~ re) }
  target && /^[[:space:]]*disabled[[:space:]]*=/ { next }
  { print }' "$INPUTS_CONF" >"$tmp"
chown --reference="$INPUTS_CONF" "$tmp"
chmod --reference="$INPUTS_CONF" "$tmp"
echo "Editing $INPUTS_CONF (backup: $backup):"
diff -u "$backup" "$tmp" | sed 's/^/  /'
mv "$tmp" "$INPUTS_CONF"

unit_for() {  # <splunk_home> -> systemd unit that runs it, if any
  local u
  for u in Splunkd SplunkForwarder splunk splunkd; do
    systemctl cat "$u.service" 2>/dev/null | grep -q "$1/bin/splunk" && { echo "$u.service"; return; }
  done
}
restart_instance() {  # <splunk_home>
  local unit; unit="$(unit_for "$1")"
  echo "Restarting $1 ${unit:+(systemd: $unit)}..."
  if [[ -n "$unit" ]]; then
    systemctl restart "$unit"
  else
    as_owner "$1" "$1/bin/splunk" restart --answer-yes --no-prompt
  fi
  for _ in $(seq 1 60); do
    as_owner "$1" "$1/bin/splunk" status >/dev/null 2>&1 && { echo "  $1 is running."; return 0; }
    sleep 10
  done
  echo "  $1 did NOT come back within 10 minutes - check $1/var/log/splunk/splunkd.log"
  return 1
}

rc=0
# Indexer first (picks up indexes.conf/server.conf), then the reader.
if [[ "$INDEX_HOME" != "$READ_HOME" ]]; then restart_instance "$INDEX_HOME" || rc=1; fi
restart_instance "$READ_HOME" || rc=1

echo "Watching the input directories for 5 minutes (sinkhole deletes each part once indexed)..."
count_files() {
  local n=0 entry path
  for entry in "${STANZAS[@]}"; do
    path="${entry%%|*}"
    n=$((n + $(find "$(dirname "$path")" -maxdepth 1 -type f -name "$(basename "$path")" | wc -l)))
  done
  echo "$n"
}
before=$(count_files)
sleep "${BTC_ENABLE_WATCH_SECS:-300}"
after=$(count_files)
echo "  part files: $before -> $after"
if [[ $after -lt $before ]]; then
  echo "  Splunk is consuming the parts."
else
  echo "  No part consumed yet - large 900 MB parts can take a while; check:"
  echo "    grep -iE 'btc|batch|sinkhole' $READ_HOME/var/log/splunk/splunkd.log | grep -iE 'warn|error' | tail"
fi
grep -iE "ERROR|WARN" "$READ_HOME/var/log/splunk/splunkd.log" 2>/dev/null | grep -iE "export/rpc|btc:|TailReader|BatchReader|DateParser" | tail -20 | sed 's/^/  /'
echo "===== done ($(date -Is)), rc=$rc ====="
exit $rc
