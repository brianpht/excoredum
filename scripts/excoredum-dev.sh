#!/usr/bin/env bash
#
# excoredum-dev.sh - start / stop / restart the local dev stack:
#   an N-node Aeron Raft cluster (+ archive), a CQRS read replica that follows
#   node-0's archive, and the HTTP/WS gateway.
#
# Usage:
#   ./scripts/excoredum-dev.sh {start|stop|restart|status|seed|bench}
#
# Env overrides (all optional):
#   EXC_NODES           number of cluster nodes        (3)
#   EXC_INGRESS_BASE    node-0 ingress port            (20100); node n uses +n*100
#   EXC_HOST            host advertised in members     (localhost)
#   EXC_QUERY_PORT      read query channel port        (44000)
#   EXC_HTTP_PORT       gateway HTTP port              (8080)
#   EXC_RUN_DIR         pid/log/state base dir         (/tmp/excoredum)
#   EXC_CLEAN_START     wipe cluster state on start    (true; restart uses false by default)
#   EXC_SYMBOLS         gateway.symbols                (BTC/USDT, ETH/USDT sample)
#   EXC_CURRENCIES      gateway.currencies             (BTC=10, USDT=20 sample)
#   EXC_ADMIN_UIDS      gateway.admin.uids             (1,2,811)
#   EXC_BENCH_OPS       gateway bench commands         (10000)
#   EXC_BENCH_USERS     gateway bench users            (100)
#   EXC_BENCH_SYMBOLS   gateway bench symbol count     (1)
#   EXC_ADMIN_UID       gateway bench admin uid        (811)
#   EXC_ADMIN_API_KEY   gateway.admin.apiKey           (unset = disabled)
#
# First run builds the application distributions (installDist) automatically.
# State and logs live under $EXC_RUN_DIR; PID files under $EXC_RUN_DIR/pids.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROG="$(basename "$0")"
cd "$ROOT"

# ---- overridable defaults -------------------------------------------------
EXC_NODES="${EXC_NODES:-3}"
BASE_PORT="${EXC_INGRESS_BASE:-20100}"
HOST="${EXC_HOST:-localhost}"
QUERY_PORT="${EXC_QUERY_PORT:-44000}"
HTTP_PORT="${EXC_HTTP_PORT:-8080}"
RUN_DIR="${EXC_RUN_DIR:-/tmp/excoredum}"
CLEAN_START="${EXC_CLEAN_START:-true}"
SYMBOLS="${EXC_SYMBOLS:-1|BTC/USDT|10|20|1|1|0|0,2|ETH/USDT|10|20|1|1|0|0}"
CURRENCIES="${EXC_CURRENCIES:-10|BTC|1,20|USDT|1}"
ADMIN_UIDS="${EXC_ADMIN_UIDS:-1,2,811}"
BENCH_OPS="${EXC_BENCH_OPS:-10000}"
BENCH_USERS="${EXC_BENCH_USERS:-100}"
BENCH_SYMBOLS="${EXC_BENCH_SYMBOLS:-1}"
ADMIN_UID="${EXC_ADMIN_UID:-811}"
ADMIN_API_KEY="${EXC_ADMIN_API_KEY:-}"

LOG_DIR="$RUN_DIR/logs"
PID_DIR="$RUN_DIR/pids"
CLUSTER_BASE="$RUN_DIR/cluster"
CONF_DIR="$RUN_DIR/conf"
ARCHIVE_PORT=$((BASE_PORT + 4))
ARCHIVE_CHANNEL="aeron:udp?endpoint=${HOST}:${ARCHIVE_PORT}"
QUERY_CHANNEL="aeron:udp?endpoint=${HOST}:${QUERY_PORT}"
QUERY_BIND="aeron:udp?endpoint=0.0.0.0:${QUERY_PORT}"

# The main classes / installDist classpath prefixes (matches docker/entrypoint-*.sh).
LAUNCHER_MAIN=com.exadbe.launcher.ClusterLauncher
READ_MAIN=com.exadbe.read.ReadServiceLauncher
GATEWAY_MAIN=com.exadbe.gateway.GatewayLauncher
LAUNCHER_LIB="$ROOT/exc-launcher/build/install/exc-launcher/lib"
READ_LIB="$ROOT/exc-read/build/install/exc-read/lib"
GATEWAY_LIB="$ROOT/exc-gateway/build/install/exc-gateway/lib"
BENCH_LIB="$ROOT/exc-bench/build/install/exc-bench/lib"
BENCH_MAIN=com.exadbe.bench.GatewayBenchRunner

ADD_OPENS=(
  "--add-opens" "java.base/jdk.internal.misc=ALL-UNNAMED"
  "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"
)

# ---- helpers --------------------------------------------------------------
mkdir -p "$LOG_DIR" "$PID_DIR" "$CLUSTER_BASE" "$CONF_DIR"

log()  { printf '[%s] %s\n' "$PROG" "$*"; }
err()  { printf '[%s] ERROR: %s\n' "$PROG" "$*" >&2; }

build_members() {
  local m="" p
  for ((i = 0; i < EXC_NODES; i++)); do
    p=$((BASE_PORT + i * 100))
    m+="${i},${HOST}:${p},${HOST}:$((p + 1)),${HOST}:$((p + 2)),${HOST}:$((p + 3)),${HOST}:$((p + 4))"
    if (( i + 1 < EXC_NODES )); then m+="|"; fi
  done
  printf '%s' "$m"
}

ingress_list() { # Aeron cluster-client ingress: id=host:port,...
  local id p list=""
  for ((id = 0; id < EXC_NODES; id++)); do
    p=$((BASE_PORT + id * 100))
    [ -n "$list" ] && list+=","
    list+="${id}=${HOST}:${p}"
  done
  printf '%s' "$list"
}

ensure_dists() {
  if [ ! -f "$LAUNCHER_LIB/exc-launcher.jar" ] \
      || [ ! -f "$READ_LIB/exc-read.jar" ] \
      || [ ! -f "$GATEWAY_LIB/exc-gateway.jar" ]; then
    log "building application distributions (installDist) ..."
    "$ROOT/gradlew" --quiet :exc-launcher:installDist :exc-read:installDist :exc-gateway:installDist
  fi
}

ensure_bench_dist() {
  if [ ! -f "$BENCH_LIB/exc-bench.jar" ]; then
    log "building bench distribution (installDist) ..."
    "$ROOT/gradlew" --quiet :exc-bench:installDist
  fi
}

write_confs() {
  local i
  for ((i = 0; i < EXC_NODES; i++)); do
    mkdir -p "$CLUSTER_BASE/node-$i"
    cat > "$CONF_DIR/cluster-$i.properties" <<EOF
exc.clusterMembers=$(build_members)
exc.host=${HOST}
exc.baseDir=${CLUSTER_BASE}/node-$i
EOF
  done

  cat > "$CONF_DIR/gateway.properties" <<EOF
gateway.http.host=0.0.0.0
gateway.http.port=${HTTP_PORT}
gateway.read.requestChannel=${QUERY_CHANNEL}
gateway.read.requestStreamId=300
gateway.read.responseStreamId=301
gateway.write.clientId=42
gateway.write.ingressEndpoints=$(ingress_list)
gateway.admin.uids=${ADMIN_UIDS}
gateway.symbols=${SYMBOLS}
gateway.currencies=${CURRENCIES}
gateway.marketPump.intervalMs=1000
EOF
}

ping() { # pidfile -> alive?
  local pidfile="$1" pid
  [ -f "$pidfile" ] || return 1
  pid="$(cat "$pidfile" 2>/dev/null || true)"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

stop_by_pidfile() { # pidfile mainclass
  local pidfile="$1" main="$2" pid
  if [ -f "$pidfile" ]; then
    pid="$(cat "$pidfile" 2>/dev/null || true)"
    if [ -n "$pid" ]; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.3; done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  fi
  pkill -f "$main" 2>/dev/null || true
}

wait_port() { # host port tries
  local host="$1" port="$2" tries="${3:-60}"
  for _ in $(seq 1 "$tries"); do
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
      exec 3>&- 3<&- 2>/dev/null || true
      return 0
    fi
    sleep 1
  done
  return 1
}

# ---- actions --------------------------------------------------------------
start_cluster() {
  local i pidfile
  for ((i = 0; i < EXC_NODES; i++)); do
    pidfile="$PID_DIR/cluster-$i.pid"
    if ping "$pidfile"; then
      log "cluster node $i already running (pid $(cat "$pidfile"))"
      continue
    fi
    log "starting cluster node $i (ingress ${HOST}:$((BASE_PORT + i * 100)), cleanStart=${CLEAN_START})"
    java "${ADD_OPENS[@]}" \
      -Dexc.nodeId="$i" -Dexc.cleanStart="$CLEAN_START" \
      -cp "$LAUNCHER_LIB/*" "$LAUNCHER_MAIN" --config="$CONF_DIR/cluster-$i.properties" \
      >"$LOG_DIR/cluster-$i.log" 2>&1 &
    echo $! > "$pidfile"
  done
}

start_read() {
  if ping "$PID_DIR/read.pid"; then
    log "read replica already running (pid $(cat "$PID_DIR/read.pid"))"
    return
  fi
  log "starting read replica following ${ARCHIVE_CHANNEL}, queries on ${QUERY_PORT}"
  java "${ADD_OPENS[@]}" \
    -cp "$READ_LIB/*" "$READ_MAIN" \
    --archive="$ARCHIVE_CHANNEL" --host="$HOST" --query="$QUERY_BIND" \
    >"$LOG_DIR/read.log" 2>&1 &
  echo $! > "$PID_DIR/read.pid"
}

start_gateway() {
  if ping "$PID_DIR/gateway.pid"; then
    log "gateway already running (pid $(cat "$PID_DIR/gateway.pid"))"
    return
  fi
  log "starting gateway on http://localhost:${HTTP_PORT}"
  java "${ADD_OPENS[@]}" \
    -cp "$GATEWAY_LIB/*" "$GATEWAY_MAIN" --config="$CONF_DIR/gateway.properties" \
    >"$LOG_DIR/gateway.log" 2>&1 &
  echo $! > "$PID_DIR/gateway.pid"
}

stop_all() {
  log "stopping gateway, read replica, cluster ..."
  stop_by_pidfile "$PID_DIR/gateway.pid" "$GATEWAY_MAIN"
  stop_by_pidfile "$PID_DIR/read.pid" "$READ_MAIN"
  local i
  for ((i = 0; i < EXC_NODES; i++)); do
    stop_by_pidfile "$PID_DIR/cluster-$i.pid" "$LAUNCHER_MAIN"
  done
  log "stopped."
}

status() {
  local i
  printf '%-14s %-8s %s\n' "SERVICE" "STATE" "PID"
  for ((i = 0; i < EXC_NODES; i++)); do
    if ping "$PID_DIR/cluster-$i.pid"; then printf '%-14s %-8s %s\n' "cluster-$i" "up" "$(cat "$PID_DIR/cluster-$i.pid")"; else printf '%-14s %-8s %s\n' "cluster-$i" "down" "-"; fi
  done
  if ping "$PID_DIR/read.pid"; then printf '%-14s %-8s %s\n' "read" "up" "$(cat "$PID_DIR/read.pid")"; else printf '%-14s %-8s %s\n' "read" "down" "-"; fi
  if ping "$PID_DIR/gateway.pid"; then printf '%-14s %-8s %s\n' "gateway" "up" "$(cat "$PID_DIR/gateway.pid")"; else printf '%-14s %-8s %s\n' "gateway" "down" "-"; fi
}

seed_market() { # register symbol 1 + users + a resting ask and a crossing fill
  local base="http://localhost:${HTTP_PORT}" h="Content-Type: application/json" admin="811"
  curl -sf -H "$h" -H "X-User-Id: $admin" \
    -d '{"symbolId":1,"baseCurrency":10,"quoteCurrency":20,"baseScaleK":1,"quoteScaleK":1,"takerFee":0,"makerFee":0}' \
    "$base/api/v1/symbols" >/dev/null || true
  curl -sf -H "$h" -H "X-User-Id: $admin" -d '{"uid":811}' "$base/api/v1/users" >/dev/null || true
  curl -sf -H "$h" -H "X-User-Id: $admin" -d '{"currency":10,"amount":1000}' "$base/api/v1/users/811/balance" >/dev/null || true
  curl -sf -H "$h" -H "X-User-Id: $admin" -d '{"currency":20,"amount":1000000}' "$base/api/v1/users/811/balance" >/dev/null || true
  curl -sf -H "$h" -H "X-User-Id: $admin" -d '{"uid":812}' "$base/api/v1/users" >/dev/null || true
  curl -sf -H "$h" -H "X-User-Id: $admin" -d '{"currency":20,"amount":1000000}' "$base/api/v1/users/812/balance" >/dev/null || true
  # maker rests ask 10 @ 100; a taker bid 6 @ 105 fills it, leaving 4 @ 100.
  curl -sf -H "$h" -d '{"symbolId":1,"orderId":1,"ask":true,"type":"GTC","price":100,"size":10,"reserveBidPrice":0,"uid":811,"userCookie":1}' \
    "$base/api/v1/orders" >/dev/null || true
  curl -sf -H "$h" -d '{"symbolId":1,"orderId":2,"ask":false,"type":"GTC","price":105,"size":6,"reserveBidPrice":105,"uid":812,"userCookie":2}' \
    "$base/api/v1/orders" >/dev/null || true
  log "seeded symbol 1 (BTC/USDT) + users 811/812 + resting ask and a crossing fill"
}

bench() {
  wait_port "$HOST" "$HTTP_PORT" 30 || { err "gateway is not up on :${HTTP_PORT} (run 'start' first)"; exit 1; }
  ensure_bench_dist
  log "benchmarking through gateway http://localhost:${HTTP_PORT} (ops=${BENCH_OPS} users=${BENCH_USERS} symbols=${BENCH_SYMBOLS})"
  local api_key_args=()
  if [ -n "$ADMIN_API_KEY" ]; then api_key_args=(--api-key="$ADMIN_API_KEY"); fi
  java "${ADD_OPENS[@]}" \
    -cp "$BENCH_LIB/*" "$BENCH_MAIN" \
    --base-url="http://localhost:${HTTP_PORT}" \
    --ops="$BENCH_OPS" --users="$BENCH_USERS" --symbols="$BENCH_SYMBOLS" \
    --admin-uid="$ADMIN_UID" "${api_key_args[@]}"
}

usage() {
  sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
  echo
  echo "Commands: start | stop | restart | status | seed | bench"
}

# ---- main -----------------------------------------------------------------
cmd="${1:-help}"
case "$cmd" in
  start)
    ensure_dists
    write_confs
    start_cluster
    # Give node-0's archive a moment to bind before the replica follows it.
    sleep 5
    start_read
    # The gateway HTTP port is listenable immediately; backend reads may need a few more seconds.
    sleep 3
    start_gateway
    log "stack starting; gateway http://localhost:${HTTP_PORT}  (logs: $LOG_DIR)"
    wait_port "$HOST" "$HTTP_PORT" 90 && log "gateway is up on :${HTTP_PORT}" || err "gateway did not open :${HTTP_PORT} within 90s"
    status
    ;;
  stop)
    stop_all
    ;;
  restart)
    stop_all
    # Restart preserves cluster state unless EXC_CLEAN_START is set explicitly.
    CLEAN_START="${EXC_CLEAN_START:-false}"
    sleep 2
    ensure_dists
    write_confs
    start_cluster
    sleep 5
    start_read
    sleep 3
    start_gateway
    log "stack restarted; gateway http://localhost:${HTTP_PORT}  (logs: $LOG_DIR)"
    status
    ;;
  status)
    status
    ;;
  seed)
    wait_port "$HOST" "$HTTP_PORT" 30 || { err "gateway is not up on :${HTTP_PORT} (run 'start' first)"; exit 1; }
    seed_market
    log "seeded; open http://localhost:${HTTP_PORT} to see a live book"
    ;;
  bench)
    bench
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    err "unknown command: $cmd"
    usage
    exit 1
    ;;
esac
