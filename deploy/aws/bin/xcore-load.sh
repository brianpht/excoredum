#!/bin/sh
# Runs the exchange-core TestOrdersGenerator workload against a deployed cluster
# over the write client SDK and verifies every result (all commands succeed and
# the egress fills match the matching-only reference replay). JUSTRADE_EGRESS must
# advertise this instance's private IP so the cluster can reach it; JUSTRADE_CLIENT_ID
# must be unique per concurrently running load generator.
#
# Modes (JUSTRADE_MODE): throughput (closed-loop), latency (open-loop at
# JUSTRADE_RATE ops/s), hiccups (closed-loop + pause detector). The latency mode
# measures one rate per invocation; sweep several rates on fresh clusters.
set -eu

ENDPOINTS="${JUSTRADE_ENDPOINTS:?JUSTRADE_ENDPOINTS is required}"
_ip="$(hostname -i)"
EGRESS="aeron:udp?endpoint=${_ip%% *}:0"
MODE="${JUSTRADE_MODE:-throughput}"
OPS="${JUSTRADE_OPS:-3000000}"
TARGET_ORDERS="${JUSTRADE_TARGET_ORDERS:-1000}"
USERS="${JUSTRADE_USERS:-1000}"
SEED="${JUSTRADE_SEED:-1}"
BATCH="${JUSTRADE_BATCH:-16}"
CLIENT_ID="${JUSTRADE_CLIENT_ID:-1}"
RATE="${JUSTRADE_RATE:-25000}"
RETRY_BACKOFF_MS="${JUSTRADE_RETRY_BACKOFF_MS:-2000}"
JAVA_OPTS="${JUSTRADE_JAVA_OPTS:--Xms512m -Xmx4g -XX:+UseZGC}"

RUNNER_ARGS="--endpoints=${ENDPOINTS} --egress=${EGRESS} --mode=${MODE} --commands=${OPS} --target-orders=${TARGET_ORDERS} --users=${USERS} --seed=${SEED} --batch=${BATCH} --client-id=${CLIENT_ID} --retry-backoff-ms=${RETRY_BACKOFF_MS}"
if [ "${MODE}" = "latency" ]; then
    RUNNER_ARGS="${RUNNER_ARGS} --rate=${RATE}"
fi

echo "justrade xcore-load: ${OPS} ops / ${USERS} users / ${TARGET_ORDERS} target orders / mode ${MODE} against ${ENDPOINTS} (clientId=${CLIENT_ID}, egress ${EGRESS})"
exec /opt/justrade/jre/bin/java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens java.base/java.io=ALL-UNNAMED \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    --add-opens java.base/java.util=ALL-UNNAMED \
    -cp "/opt/justrade/xcore-bench/lib/*" \
    io.justrade.xcorebench.XcoreWorkloadRunner \
    ${RUNNER_ARGS}
