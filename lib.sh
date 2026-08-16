#!/usr/bin/env bash
# Shared helpers for start.sh/stop.sh. Meant to be sourced (with SCRIPT_DIR
# and RUN_PY already set by the caller), not executed directly.

# A recorded pid being alive isn't enough - after a crash, the OS can reuse
# that same numeric pid for an unrelated process, which would make a naive
# alive-check falsely report "it's our process". Check the live process's
# own command line actually looks like ours (run.py, invoked with this
# component's subcommand) before trusting it.
pid_matches_component() {
  local pid="$1" name="$2"
  local cmd
  cmd="$(ps -p "$pid" -o command= 2>/dev/null)" || return 1
  [[ -n "$cmd" && "$cmd" == *"$RUN_PY"* && "$cmd" == *"$name"* ]]
}
