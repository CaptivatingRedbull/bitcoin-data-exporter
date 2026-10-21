#!/usr/bin/env bash
# btc-fresh-start.sh - wipe the btc_parser index and rpc-ingest's state, then
# start Splunk + rpc-ingest from genesis at a fixed time (one-shot cron job).
# Standalone test instance: parser, inputs and Splunk Enterprise on one host.
#
#   btc-fresh-start.sh prepare [options]   tonight, interactive - destructive, asks first
#   btc-fresh-start.sh start   [options]   what the cron job runs at --at
#   btc-fresh-start.sh status  [options]   cron entry, inputs, services, data dirs
#
# prepare, in this order:
#   1. stop btc-parser-rpc-ingest (api-poll, stale-blocks, mempool-watch keep
#      running and keep writing their monitored CSVs)
#   2. make sure the export/rpc batch stanzas carry `disabled = 1`
#   3. install the current TA-btc-parser-parsing / TA-btc-parser-search from
#      this checkout into $SPLUNK_HOME/etc/apps (default/ + metadata/ only -
#      local/ on the server is left alone)
#   4. stop Splunk, `splunk clean eventdata -index <index>`, and reset the
#      fishbucket entry of every file the monitor stanzas read (prices,
#      stale headers, sanctioned events) so Splunk re-reads them from byte 0
#   5. delete <data-root>/state/rpc and <data-root>/export/rpc (rpc-ingest
#      restarts at genesis), recreate the four empty export/rpc directories
#   6. leave Splunk STOPPED, so nothing at all is indexed before --at
#      (--keep-splunk-running: start it again now; monitors re-read at once,
#      the batch inputs stay disabled)
#   7. write /etc/cron.d/btc-fresh-start for --at
#
# start (cron, as root): removes its own cron file first (one-shot), then
#   btc-splunk-enable-backfill.sh --enable --allow-empty (preflight, removes
#   the `disabled` lines, (re)starts Splunk) and only if that succeeded
#   `systemctl start btc-parser.target`. A failed preflight leaves rpc-ingest
#   stopped on purpose: with nothing consuming export/rpc it would fill the
#   disk.
#
# Options:
#   --at 'YYYY-MM-DD HH:MM'  start time, host local time (default: next 01:00)
#   --data-root DIR          parser-data root (default /parser-data)
#   --splunk-home DIR        default /opt/splunk
#   --index NAME             default btc_parser
#   --keep-splunk-running    (prepare) restart Splunk right away
#   --yes                    (prepare) skip the confirmation prompt
#   --log FILE               default /var/log/btc-fresh-start.log

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENABLE_SCRIPT="$SCRIPT_DIR/btc-splunk-enable-backfill.sh"
CRON_FILE=/etc/cron.d/btc-fresh-start
RPC_SUBDIRS=(blocks transactions inputs outputs)
APPS_FROM_REPO=(TA-btc-parser-parsing TA-btc-parser-search)

MODE="${1:-}"; shift || true
AT=""
DATA_ROOT=/parser-data
SPLUNK_HOME=/opt/splunk
INDEX=btc_parser
KEEP_SPLUNK=0
YES=0
LOG_FILE=/var/log/btc-fresh-start.log

while [[ $# -gt 0 ]]; do
  case "$1" in
    --at) AT="$2"; shift 2 ;;
    --data-root) DATA_ROOT="$2"; shift 2 ;;
    --splunk-home) SPLUNK_HOME="$2"; shift 2 ;;
    --index) INDEX="$2"; shift 2 ;;
    --keep-splunk-running) KEEP_SPLUNK=1; shift ;;
    --yes) YES=1; shift ;;
    --log) LOG_FILE="$2"; shift 2 ;;
    -h|--help) sed -n '2,42p' "$0"; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done
case "$MODE" in prepare|start|status) ;; *) sed -n '2,10p' "$0"; exit 2 ;; esac
[[ "$(id -u)" -eq 0 ]] || { echo "Run as root." >&2; exit 2; }
[[ -x "$SPLUNK_HOME/bin/splunk" ]] || { echo "No Splunk at $SPLUNK_HOME." >&2; exit 2; }

mkdir -p "$(dirname "$LOG_FILE")"
exec > >(tee -a "$LOG_FILE") 2>&1
echo
echo "===== $(date -Is) btc-fresh-start.sh $MODE ====="

SPLUNK_USER="$(stat -c %U "$SPLUNK_HOME")"
as_splunk() { if [[ "$SPLUNK_USER" == root ]]; then "$@"; else runuser -u "$SPLUNK_USER" -- "$@"; fi; }
splunk_cmd() { as_splunk "$SPLUNK_HOME/bin/splunk" "$@"; }
splunk_unit() {
  local u
  for u in Splunkd splunk splunkd; do
    systemctl cat "$u.service" 2>/dev/null | grep -q "$SPLUNK_HOME/bin/splunk" && { echo "$u.service"; return; }
  done
}
splunk_stop() {
  local unit; unit="$(splunk_unit)"
  if [[ -n "$unit" ]]; then systemctl stop "$unit"; else splunk_cmd stop; fi
}
splunk_start() {
  local unit; unit="$(splunk_unit)"
  if [[ -n "$unit" ]]; then systemctl start "$unit"; else splunk_cmd start --answer-yes --no-prompt; fi
}
splunk_db() {
  local db
  db="$(splunk_cmd envvars 2>/dev/null | tr ';' '\n' | sed -n 's/^ *export SPLUNK_DB=//p' | tr -d '" ')"
  echo "${db:-$SPLUNK_HOME/var/lib/splunk}"
}

# inputs.conf holding the export/rpc batch stanzas (same lookup as the enable script).
find_inputs_conf() {
  local c
  mapfile -t c < <(grep -lsE '^\[batch://.*export/rpc/' "$SPLUNK_HOME"/etc/apps/*/local/inputs.conf)
  [[ ${#c[@]} -eq 1 ]] || { echo "Expected one inputs.conf with export/rpc batch stanzas under $SPLUNK_HOME/etc/apps/*/local/, found ${#c[@]}: ${c[*]:-none}" >&2; return 1; }
  echo "${c[0]}"
}

# Paths of the [monitor://...] stanzas (globs).
monitor_globs() {
  sed -n 's/^\[monitor:\/\/\(.*\)\]$/\1/p' "$1"
}

# "1" if every export/rpc batch stanza has disabled = 1/true.
batch_all_disabled() {
  awk '
    /^\[/ { if (in_st && !dis) bad = 1; in_st = ($0 ~ /^\[batch:\/\/.*export\/rpc\//); dis = 0; next }
    in_st && /^[[:space:]]*disabled[[:space:]]*=[[:space:]]*(1|true|t|yes|y)[[:space:]]*$/ { dis = 1 }
    END { if (in_st && !dis) bad = 1; print bad ? 0 : 1 }' "$1"
}

ensure_batch_disabled() {
  local conf="$1"
  if [[ "$(batch_all_disabled "$conf")" == 1 ]]; then
    echo "  export/rpc batch stanzas already disabled."
    return
  fi
  local backup="$conf.before-fresh-start.$(date +%Y%m%d%H%M%S)" tmp
  cp -p "$conf" "$backup"
  tmp="$(mktemp "$conf.XXXXXX")"
  awk '
    /^\[/ { target = ($0 ~ /^\[batch:\/\/.*export\/rpc\//) }
    target && /^[[:space:]]*disabled[[:space:]]*=/ { next }
    { print }
    /^\[/ && target { print "disabled = 1" }' "$conf" >"$tmp"
  chown --reference="$conf" "$tmp"; chmod --reference="$conf" "$tmp"
  mv "$tmp" "$conf"
  echo "  disabled the export/rpc batch stanzas (backup: $backup)."
}

parser_user() { stat -c %U "$DATA_ROOT/export" 2>/dev/null || stat -c %U "$DATA_ROOT"; }

ensure_rpc_dirs() {
  local user group d
  user="$(parser_user)"; group="$(id -gn "$user")"
  install -d -o "$user" -g "$group" "$DATA_ROOT/export/rpc"
  for d in "${RPC_SUBDIRS[@]}"; do
    install -d -o "$user" -g "$group" "$DATA_ROOT/export/rpc/$d"
  done
  echo "  $DATA_ROOT/export/rpc/{${RPC_SUBDIRS[*]}} present (owner $user)."
}

next_at() {  # next 01:00 in host local time
  if [[ "$(date +%H%M)" < "0100" ]]; then date +'%Y-%m-%d 01:00'; else date -d tomorrow +'%Y-%m-%d 01:00'; fi
}

# ----------------------------------------------------------------------------

if [[ "$MODE" == status ]]; then
  echo "cron:      $( [[ -f $CRON_FILE ]] && grep -v '^#' "$CRON_FILE" || echo "none ($CRON_FILE)")"
  conf="$(find_inputs_conf)" && echo "inputs:    $conf - export/rpc batch stanzas $( [[ "$(batch_all_disabled "$conf")" == 1 ]] && echo DISABLED || echo ENABLED)"
  echo "splunk:    $(splunk_cmd status 2>&1 | head -1)"
  for u in btc-parser-rpc-ingest btc-parser-stale-blocks-ingest btc-parser-api-poll btc-parser-mempool-watch; do
    printf '%-32s %s\n' "$u:" "$(systemctl is-active "$u.service" 2>/dev/null)"
  done
  for d in "${RPC_SUBDIRS[@]}"; do
    printf 'export/rpc/%-13s %s files\n' "$d" "$(find "$DATA_ROOT/export/rpc/$d" -maxdepth 1 -type f 2>/dev/null | wc -l)"
  done
  echo "state/rpc: $(ls -A "$DATA_ROOT/state/rpc" 2>/dev/null | wc -l) entries"
  exit 0
fi

if [[ "$MODE" == start ]]; then
  rm -f "$CRON_FILE" && echo "Removed $CRON_FILE (one-shot)."
  ensure_rpc_dirs
  echo "Enabling the export/rpc batch inputs:"
  if ! "$ENABLE_SCRIPT" --enable --allow-empty --splunk-home "$SPLUNK_HOME" --index-home "$SPLUNK_HOME"; then
    echo "Enable script failed - rpc-ingest NOT started (nothing would consume export/rpc). See /var/log/btc-splunk-enable-backfill.log, fix, then run: $0 start"
    exit 1
  fi
  echo "Starting btc-parser.target (rpc-ingest from genesis + the other services if stopped)..."
  systemctl start btc-parser.target
  sleep 30
  for u in btc-parser-rpc-ingest btc-parser-stale-blocks-ingest btc-parser-api-poll btc-parser-mempool-watch; do
    printf '  %-32s %s\n' "$u:" "$(systemctl is-active "$u.service" 2>/dev/null)"
  done
  echo "===== start done ($(date -Is)) ====="
  exit 0
fi

# --- prepare -----------------------------------------------------------------

AT="${AT:-$(next_at)}"
date -d "$AT" >/dev/null 2>&1 || { echo "--at '$AT' is not a valid date." >&2; exit 2; }
[[ "$(date -d "$AT" +%s)" -gt "$(date +%s)" ]] || { echo "--at '$AT' is in the past." >&2; exit 2; }
INPUTS_CONF="$(find_inputs_conf)" || exit 2
SPLUNK_DB="$(splunk_db)"
FISHBUCKET="$SPLUNK_DB/fishbucket/splunk_private_db"
[[ -x "$ENABLE_SCRIPT" ]] || { echo "Missing $ENABLE_SCRIPT" >&2; exit 2; }
for app in "${APPS_FROM_REPO[@]}"; do
  [[ -d "$APP_DIR/splunk/$app/default" ]] || { echo "Missing $APP_DIR/splunk/$app - wrong checkout?" >&2; exit 2; }
done
mapfile -t MONITORED < <(for g in $(monitor_globs "$INPUTS_CONF"); do compgen -G "$g"; done)

cat <<EOF

This will, on $(hostname):
  - stop btc-parser-rpc-ingest
  - make sure the export/rpc batch stanzas in $INPUTS_CONF are disabled
  - install ${APPS_FROM_REPO[*]} from $APP_DIR/splunk into $SPLUNK_HOME/etc/apps
  - stop Splunk and DELETE ALL EVENTS of index '$INDEX' (splunk clean eventdata)
  - reset the fishbucket for ${#MONITORED[@]} monitored files (re-read from the start)
  - DELETE $DATA_ROOT/state/rpc and $DATA_ROOT/export/rpc (rpc-ingest restarts at genesis)
  - $( [[ $KEEP_SPLUNK -eq 1 ]] && echo "start Splunk again right away" || echo "leave Splunk STOPPED until $AT" )
  - schedule '$0 start' at $AT ($CRON_FILE)
EOF
if [[ $YES -eq 0 ]]; then
  read -r -p "Type FRESH to continue: " answer
  [[ "$answer" == FRESH ]] || { echo "Aborted, nothing changed."; exit 1; }
fi

echo "1. Stopping rpc-ingest..."
systemctl stop btc-parser-rpc-ingest.service

echo "2. Batch inputs:"
ensure_batch_disabled "$INPUTS_CONF"

echo "3. Installing apps from $APP_DIR/splunk:"
for app in "${APPS_FROM_REPO[@]}"; do
  dest="$SPLUNK_HOME/etc/apps/$app"
  mkdir -p "$dest"
  for sub in default metadata; do
    [[ -d "$APP_DIR/splunk/$app/$sub" ]] || continue
    rm -rf "${dest:?}/$sub"
    cp -r "$APP_DIR/splunk/$app/$sub" "$dest/$sub"
  done
  chown -R "$SPLUNK_USER": "$dest"
  echo "  $dest (default/, metadata/; local/ untouched)"
done

echo "4. Stopping Splunk, cleaning index '$INDEX', resetting fishbucket entries..."
splunk_stop
for _ in $(seq 1 30); do splunk_cmd status >/dev/null 2>&1 || break; sleep 5; done
if splunk_cmd status >/dev/null 2>&1; then
  echo "  Splunk did not stop - aborting before deleting anything."
  exit 1
fi
splunk_cmd clean eventdata -index "$INDEX" -f || { echo "  clean eventdata failed - aborting."; exit 1; }
for f in "${MONITORED[@]}"; do
  splunk_cmd cmd btprobe -d "$FISHBUCKET" --file "$f" --reset >/dev/null 2>&1 \
    && echo "  fishbucket reset: $f" || echo "  fishbucket: no entry for $f (never read - fine)"
done

echo "5. Resetting rpc-ingest state and export..."
rm -rf "${DATA_ROOT:?}/state/rpc" "${DATA_ROOT:?}/export/rpc"
install -d -o "$(parser_user)" -g "$(id -gn "$(parser_user)")" "$DATA_ROOT/state/rpc"
ensure_rpc_dirs

if [[ $KEEP_SPLUNK -eq 1 ]]; then
  echo "6. Starting Splunk again (batch inputs stay disabled)..."
  splunk_start
else
  echo "6. Splunk stays stopped until $AT."
fi

echo "7. Scheduling the start:"
cat >"$CRON_FILE" <<EOF
# One-shot (the job deletes this file): btc_parser fresh start, written $(date -Is)
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
$(date -d "$AT" +'%M %H %d %m') * root $SCRIPT_DIR/btc-fresh-start.sh start --data-root $DATA_ROOT --splunk-home $SPLUNK_HOME --index $INDEX --log $LOG_FILE >/dev/null 2>&1
EOF
chmod 644 "$CRON_FILE"
echo "  $CRON_FILE:"; sed 's/^/    /' "$CRON_FILE"
echo "Done. Check with: $0 status   Output of the run: $LOG_FILE"
