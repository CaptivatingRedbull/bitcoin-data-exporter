#!/usr/bin/env bash
# Installs/updates/removes the systemd units for btc_parser_app's three
# long-running components (rpc-ingest, stale-blocks-ingest, api-poll) plus
# the btc-parser.target that groups them.
#
# Why this exists instead of just start.sh/stop.sh: those never restart a
# crashed process and don't survive a reboot on their own (see
# docs/02-installation-und-betrieb.md, "Gehaerteter Dauerbetrieb: systemd-
# Units"). A weekly OS reboot (routine on this host) needs the three
# components to come back up on their own - that's what `systemctl enable`
# gives us. The one wrinkle: api-poll deliberately exits on an HTTP 429
# instead of retrying (docs/09), and a naive `Restart=always` would just
# restart-loop straight back into the same rate limit. The installed
# api-poll unit uses RestartPreventExitStatus=75 (matching
# btc_parser_app/api/poller.py's EXIT_RATE_LIMITED) so a *real* crash still
# gets restarted, but a 429 stop does not - see the comments in
# btc-parser-api-poll.service.template for the full breakdown.
#
# Usage (run as root - it writes to /etc/systemd/system):
#   sudo systemd/install.sh [--user NAME] [--config PATH] [--no-start]
#   sudo systemd/install.sh --uninstall
#
# Safe to re-run: re-installing just overwrites the unit files with the
# current templates and reloads systemd; already-running services are left
# running (systemd no-ops a redundant `enable --now` on a unit that's
# already enabled+active).
#
# No `set -u` - see start.sh for why (empty-array/unset-var handling on
# older bash).
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"
UNIT_DIR="/etc/systemd/system"
UNITS=(btc-parser-rpc-ingest btc-parser-stale-blocks-ingest btc-parser-api-poll)

APP_USER="${SUDO_USER:-$(id -un)}"
CONFIG_PATH=""
START_UNITS=1
UNINSTALL=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user)
      APP_USER="$2"; shift 2 ;;
    --config)
      CONFIG_PATH="$2"; shift 2 ;;
    --no-start)
      START_UNITS=0; shift ;;
    --uninstall)
      UNINSTALL=1; shift ;;
    -h|--help)
      sed -n '2,20p' "$0"; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run this as root (or with sudo) - it writes to $UNIT_DIR and calls systemctl." >&2
  exit 1
fi

if [[ "$UNINSTALL" -eq 1 ]]; then
  echo "=== Removing btc_parser_app systemd units ==="
  systemctl stop btc-parser.target 2>/dev/null || true
  for unit in "${UNITS[@]}"; do
    systemctl disable --now "$unit.service" 2>/dev/null || true
    rm -f "$UNIT_DIR/$unit.service"
  done
  systemctl disable btc-parser.target 2>/dev/null || true
  rm -f "$UNIT_DIR/btc-parser.target"
  systemctl daemon-reload
  echo "Done. $APP_DIR itself, its venv, and $SCRIPT_DIR/env (if any) were left untouched."
  exit 0
fi

# --- sanity checks before touching anything under /etc/systemd/system ---

if [[ ! -x "$APP_DIR/.venv/bin/python" ]]; then
  echo "No venv found at $APP_DIR/.venv - set it up first:" >&2
  echo "  cd $APP_DIR && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

if ! id "$APP_USER" >/dev/null 2>&1; then
  echo "User '$APP_USER' does not exist - pass --user NAME for an existing account." >&2
  exit 1
fi

# Guard against running the same rpc-ingest/stale-blocks-ingest/api-poll
# process under both start.sh's nohup+pidfile tracking AND systemd at once -
# two copies writing the same output/state files would corrupt them.
PID_DIR="$APP_DIR/.pids"
if [[ -d "$PID_DIR" ]]; then
  for pid_file in "$PID_DIR"/*.pid; do
    [[ -f "$pid_file" ]] || continue
    pid="$(cat "$pid_file" 2>/dev/null)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "$(basename "$pid_file" .pid) looks like it's still running via start.sh (pid $pid)." >&2
      echo "Run $APP_DIR/stop.sh first, then re-run this installer." >&2
      exit 1
    fi
  done
fi

if [[ -n "$CONFIG_PATH" ]]; then
  # Resolve to an absolute path up front - systemd units don't inherit a
  # shell's notion of "current directory" the way an interactively-invoked
  # ./start.sh does.
  case "$CONFIG_PATH" in
    /*) : ;;
    *) CONFIG_PATH="$(pwd)/$CONFIG_PATH" ;;
  esac
  if [[ ! -f "$CONFIG_PATH" ]]; then
    echo "--config path does not exist: $CONFIG_PATH" >&2
    exit 1
  fi
  echo "BTC_PARSER_CONFIG=$CONFIG_PATH" > "$SCRIPT_DIR/env"
  echo "[env] wrote $SCRIPT_DIR/env (BTC_PARSER_CONFIG=$CONFIG_PATH)"
elif [[ -f "$SCRIPT_DIR/env" ]]; then
  echo "[env] keeping existing $SCRIPT_DIR/env untouched ($(cat "$SCRIPT_DIR/env"))"
fi

echo "=== Installing btc_parser_app systemd units ==="
echo "  APP_DIR:  $APP_DIR"
echo "  APP_USER: $APP_USER"

install_template() {
  local name="$1" dest="$2"
  sed -e "s#{{APP_DIR}}#$APP_DIR#g" -e "s#{{APP_USER}}#$APP_USER#g" \
    "$SCRIPT_DIR/$name" > "$UNIT_DIR/$dest"
  echo "  wrote $UNIT_DIR/$dest"
}

install_template "btc-parser.target.template" "btc-parser.target"
for unit in "${UNITS[@]}"; do
  install_template "$unit.service.template" "$unit.service"
done

systemctl daemon-reload

for unit in "${UNITS[@]}"; do
  systemctl enable "$unit.service"
done
systemctl enable btc-parser.target

if [[ "$START_UNITS" -eq 1 ]]; then
  systemctl start btc-parser.target
  echo ""
  echo "Started. Check status with:"
else
  echo ""
  echo "Enabled but not started (--no-start). Start when ready with:"
  echo "  systemctl start btc-parser.target"
  echo ""
  echo "Either way, check status with:"
fi

for unit in "${UNITS[@]}"; do
  echo "  systemctl status $unit.service"
done
echo "  journalctl -u btc-parser-api-poll.service -f   # (etc. per unit)"
echo ""
echo "All three are now enabled, so a reboot (e.g. the weekly maintenance"
echo "restart) brings them back automatically. api-poll will NOT"
echo "restart-loop after an HTTP 429 (exit code 75 is excluded via"
echo "RestartPreventExitStatus) - restart it by hand once the rate limit"
echo "has cooled off: systemctl start btc-parser-api-poll.service"
