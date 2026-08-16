#!/usr/bin/env bash
# Stops the background services started by start.sh (rpc-ingest,
# stale-blocks-ingest, api-poll): SIGTERM first (every command handles it
# for a clean checkpoint - see cli.py/ingest.py/stale_blocks.py), SIGKILL
# after a 30s grace period if a process doesn't exit on its own. Does not
# touch bitcoind.
#
# No `set -u` - see start.sh for why (macOS bash 3.2 + empty arrays).
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="$SCRIPT_DIR/.pids"
RUN_PY="$SCRIPT_DIR/run.py"

# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

stop_component() {
  local name="$1"
  local pid_file="$PID_DIR/$name.pid"

  if [[ ! -f "$pid_file" ]]; then
    echo "[$name] no pid file - not running (or not started by start.sh)."
    return
  fi

  local pid
  pid="$(cat "$pid_file")"

  if ! kill -0 "$pid" 2>/dev/null; then
    echo "[$name] pid $pid not running (stale pid file) - removing it."
    rm -f "$pid_file"
    return
  fi

  if ! pid_matches_component "$pid" "$name"; then
    echo "[$name] pid $pid is running but isn't a tracked $name process (stale pid file, likely pid reuse) - leaving it alone and removing the stale pid file." >&2
    rm -f "$pid_file"
    return
  fi

  echo "[$name] stopping pid $pid (SIGTERM)..."
  kill -TERM "$pid"

  for _ in $(seq 1 30); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done

  if kill -0 "$pid" 2>/dev/null; then
    echo "[$name] still running after 30s - sending SIGKILL."
    kill -KILL "$pid" 2>/dev/null || true
  else
    echo "[$name] stopped."
  fi

  rm -f "$pid_file"
}

stop_component rpc-ingest
stop_component stale-blocks-ingest
stop_component api-poll

echo ""
echo "Done. This never touches bitcoind - it's still running if it was before."
echo "Stop it yourself with 'bitcoin-cli stop' if you want it down too."
