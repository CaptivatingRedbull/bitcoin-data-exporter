#!/usr/bin/env bash
# Production-style startup for btc_parser_app: checks the Bitcoin node is
# already reachable, then launches the three long-running services
# (rpc-ingest, stale-blocks-ingest, api-poll) detached in the background,
# logging to logs/.
#
# This script NEVER starts a bitcoind of its own, local or otherwise - it
# only checks reachability and fails with a clear message if the node
# isn't up. Node lifecycle is entirely out of scope here; go start/fix the
# node yourself (see config.yaml's rpc.extra_args / config.production.yaml
# for how this app is told where to find it) and re-run this script.
#
# Usage:
#   ./start.sh                              # uses config/config.yaml
#   BTC_PARSER_CONFIG=/path/other.yaml ./start.sh
#
# Safe to re-run: already-running services (tracked via .pids/*.pid) are
# left alone instead of being started twice. Stop everything with ./stop.sh.
#
# No `set -u`: macOS ships bash 3.2, which treats `"${empty_array[@]}"`
# as an unbound-variable error under -u - and CONFIG_ARGS is legitimately
# empty whenever BTC_PARSER_CONFIG isn't set.
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="$SCRIPT_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  echo "No venv found at $SCRIPT_DIR/.venv - set it up first:" >&2
  echo "  cd $SCRIPT_DIR && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

RUN_PY="$SCRIPT_DIR/run.py"
LOG_DIR="$SCRIPT_DIR/logs"
PID_DIR="$SCRIPT_DIR/.pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

CONFIG_ARGS=()
if [[ -n "${BTC_PARSER_CONFIG:-}" ]]; then
  CONFIG_ARGS=(--config "$BTC_PARSER_CONFIG")
fi

# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

is_running() {
  local pid_file="$1" name="$2"
  [[ -f "$pid_file" ]] || return 1
  local pid
  pid="$(cat "$pid_file")"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  pid_matches_component "$pid" "$name"
}

start_component() {
  local name="$1"; shift
  local pid_file="$PID_DIR/$name.pid"
  if is_running "$pid_file" "$name"; then
    echo "[$name] already running (pid $(cat "$pid_file"))"
    return
  fi
  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "[$name] pid $(cat "$pid_file") is alive but isn't a tracked $name process (stale pid file, likely pid reuse) - starting fresh."
  fi
  rm -f "$pid_file"
  echo "[$name] starting: run.py ${CONFIG_ARGS[*]:-} $*"
  # --config is a top-level argparse option (btc_parser_app/cli.py) - it
  # must come *before* the subcommand, or argparse's subparser swallows it
  # and errors with "unrecognized arguments".
  nohup "$PYTHON" "$RUN_PY" "${CONFIG_ARGS[@]}" "$@" >> "$LOG_DIR/$name.out" 2>&1 &
  local pid=$!
  disown "$pid" 2>/dev/null || true
  echo "$pid" > "$pid_file"
  echo "[$name] started (pid $pid)"
}

# Reachable through the app's own config (respects rpc.extra_args,
# rpcuser_env/rpcpassword_env, cookie-file auth, a remote/Kubernetes node -
# whatever config.yaml points at).
bitcoin_rpc_reachable() {
  "$PYTHON" - "${CONFIG_ARGS[@]}" <<'PYEOF' >/dev/null 2>&1
import sys
from btc_parser_app.config import load_config
from btc_parser_app.rpc.client import get_block_count

config_path = sys.argv[2] if len(sys.argv) > 2 else None
cfg = load_config(config_path)
get_block_count(cfg.rpc)
PYEOF
}

echo "=== btc_parser_app startup ==="

# 1. Make sure the Bitcoin node is already reachable. This script never
#    starts one itself - see the file header.
if bitcoin_rpc_reachable; then
  echo "[bitcoind] RPC reachable."
else
  echo "[bitcoind] RPC not reachable via the current config." >&2
  echo "  This script will not start a bitcoind for you (local or otherwise)." >&2
  echo "  Start/fix the node yourself, check rpc.extra_args in your config" >&2
  echo "  (config.yaml / config.production.yaml), then re-run this script." >&2
  exit 1
fi

# 2. Long-running services, detached in the background.
start_component rpc-ingest rpc-ingest
start_component stale-blocks-ingest stale-blocks-ingest
start_component api-poll api-poll

echo ""
echo "All three services are running in the background."
echo "  Structured logs: $LOG_DIR/{rpc-ingest,stale-blocks-ingest,api-poll}.log (rotated)"
echo "  Raw stdout/stderr: $LOG_DIR/{rpc-ingest,stale-blocks-ingest,api-poll}.out"
echo "  PIDs: $PID_DIR/{rpc-ingest,stale-blocks-ingest,api-poll}.pid"
echo "  Stop with: $SCRIPT_DIR/stop.sh"
