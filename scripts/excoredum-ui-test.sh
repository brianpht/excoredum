#!/usr/bin/env bash
#
# excoredum-ui-test.sh - end-to-end browser test of the bundled gateway UI,
# driven against a REAL multi-process stack started via excoredum-dev.sh.
#
# It brings up a fresh cluster + read replica + HTTP/WS gateway, installs the
# Playwright Chromium binaries once, runs the DevStackUiFeatureTest suite against
# that gateway, then tears the stack down (unless KEEP=1).
#
# Usage:
#   ./scripts/excoredum-ui-test.sh
#
# Env overrides (all optional):
#   EXC_NODES           number of cluster nodes           (1)
#   EXC_HTTP_PORT       gateway HTTP port                 (8080)
#   EXC_RUN_DIR         pid/log/state base dir            (/tmp/excoredum)
#   EXC_CLEAN_START     wipe cluster state on start       (true)
#   HEADLESS            run Chromium headless             (true; set 0 to watch)
#   SKIP_BROWSER_INSTALL  skip Playwright browser install (0; set 1 to skip)
#   KEEP                leave the stack running after     (0; set 1 to keep)
#
# The suite is opt-in (tag `uiStack`) and NOT wired into `check`.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROG="$(basename "$0")"
cd "$ROOT"

EXC_NODES="${EXC_NODES:-1}"
HTTP_PORT="${EXC_HTTP_PORT:-8080}"
RUN_DIR="${EXC_RUN_DIR:-/tmp/excoredum}"
CLEAN_START="${EXC_CLEAN_START:-true}"
HEADLESS="${HEADLESS:-true}"
SKIP_BROWSER_INSTALL="${SKIP_BROWSER_INSTALL:-0}"
KEEP="${KEEP:-0}"

GATEWAY_URL="http://localhost:${HTTP_PORT}"
LOG_DIR="$RUN_DIR/logs"

log() { printf '[%s] %s\n' "$PROG" "$*"; }
err() { printf '[%s] ERROR: %s\n' "$PROG" "$*" >&2; }

cleanup() {
  if [ "$KEEP" != "1" ]; then
    log "stopping dev stack ..."
    "$ROOT/scripts/excoredum-dev.sh" stop || true
  else
    log "KEEP=1; leaving the stack running (logs: $LOG_DIR)"
  fi
}
# Always tear down on exit (unless KEEP=1), even on a failed start or test step.
trap cleanup EXIT

wait_health() { # url -> wait for /api/v1/health to return 200
  local tries=90
  for _ in $(seq 1 "$tries"); do
    if curl -sf "$1/api/v1/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

dump_logs() {
  local f
  log "dumping recent logs from $LOG_DIR ..."
  for f in "$LOG_DIR"/gateway.log "$LOG_DIR"/read.log "$LOG_DIR"/cluster-*.log; do
    if [ -f "$f" ]; then
      echo "----- $f -----"
      tail -n 60 "$f" || true
    fi
  done
}

# 1. Fresh slate: tear down whatever is running so the fixed seed order ids do
#    not collide with the idempotency dedup window.
log "clearing any running stack ..."
"$ROOT/scripts/excoredum-dev.sh" stop >/dev/null 2>&1 || true

# 1b. Rebuild the application distributions so the running gateway serves the
#     latest UI (static resources are packaged into the exc-gateway jar).
log "building application distributions ..."
"$ROOT/gradlew" --quiet :exc-launcher:installDist :exc-read:installDist :exc-gateway:installDist

# 2. Bring the stack up fresh. Forward the env to the dev script.
log "starting dev stack (nodes=$EXC_NODES, clean=$CLEAN_START) ..."
EXC_NODES="$EXC_NODES" EXC_HTTP_PORT="$HTTP_PORT" EXC_RUN_DIR="$RUN_DIR" EXC_CLEAN_START="$CLEAN_START" \
  "$ROOT/scripts/excoredum-dev.sh" start

if ! wait_health "$GATEWAY_URL"; then
  err "gateway did not become healthy on $GATEWAY_URL"
  dump_logs
  exit 1
fi
log "gateway is healthy at $GATEWAY_URL"

# 3. Playwright Chromium binaries (idempotent; cached after the first run).
if [ "$SKIP_BROWSER_INSTALL" != "1" ]; then
  log "ensuring Playwright Chromium binaries ..."
  "$ROOT/gradlew" --quiet :exc-tests:installPlaywrightBrowsers
fi

# 4. Run the UI feature suite against the running gateway.
log "running DevStackUiFeatureTest ..."
GRADLE_ARGS=(-Pexc.gateway.url="$GATEWAY_URL" -Pexc.ui.headless="$HEADLESS")
"$ROOT/gradlew" "${GRADLE_ARGS[@]}" :exc-tests:devStackUiTest
RC=$?

if [ "$RC" -ne 0 ]; then
  err "UI feature suite FAILED (exit $RC)"
  dump_logs
else
  log "UI feature suite PASSED"
fi

# 5. Teardown is handled by the EXIT trap (unless KEEP=1).
exit "$RC"
