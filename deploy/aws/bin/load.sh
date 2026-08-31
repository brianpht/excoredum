#!/bin/sh
# Runs the write-side load against a deployed cluster over the write client SDK
# and verifies every result. JUSTRADE_EGRESS must advertise this instance's private
# IP so the cluster can reach it; JUSTRADE_CLIENT_ID must be unique per concurrently
# running load generator.
set -eu

ENDPOINTS="${JUSTRADE_ENDPOINTS:?JUSTRADE_ENDPOINTS is required}"
_ip="$(hostname -i)"
EGRESS="aeron:udp?endpoint=${_ip%% *}:0"
OPS="${JUSTRADE_OPS:-100000}"
USERS="${JUSTRADE_USERS:-100}"
SYMBOLS="${JUSTRADE_SYMBOLS:-1}"
PROFILE="${JUSTRADE_PROFILE:-}"
BATCH="${JUSTRADE_BATCH:-}"
CLIENT_ID="${JUSTRADE_CLIENT_ID:-1}"
JAVA_OPTS="${JUSTRADE_JAVA_OPTS:--Xms512m -Xmx1g -XX:+UseZGC}"

# --profile selects a latency/throughput preset; an explicit --batch overrides
# the preset's drain depth. With neither set, the runner uses its own default.
RUNNER_ARGS="--endpoints=${ENDPOINTS} --egress=${EGRESS} --ops=${OPS} --users=${USERS} --symbols=${SYMBOLS} --client-id=${CLIENT_ID}"
if [ -n "${PROFILE}" ]; then
    RUNNER_ARGS="${RUNNER_ARGS} --profile=${PROFILE}"
fi
if [ -n "${BATCH}" ]; then
    RUNNER_ARGS="${RUNNER_ARGS} --batch=${BATCH}"
fi

echo "justrade load: ${OPS} ops / ${USERS} users / ${SYMBOLS} symbols / profile ${PROFILE:-default} against ${ENDPOINTS} (clientId=${CLIENT_ID}, egress ${EGRESS})"
exec /opt/justrade/jre/bin/java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/justrade/bench/lib/*" \
    io.justrade.bench.ExternalLoadRunner \
    ${RUNNER_ARGS}
