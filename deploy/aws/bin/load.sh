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
BATCH="${JUSTRADE_BATCH:-16}"
CLIENT_ID="${JUSTRADE_CLIENT_ID:-1}"
JAVA_OPTS="${JUSTRADE_JAVA_OPTS:--Xms512m -Xmx1g -XX:+UseZGC}"

echo "justrade load: ${OPS} ops / ${USERS} users / ${SYMBOLS} symbols / batch ${BATCH} against ${ENDPOINTS} (clientId=${CLIENT_ID}, egress ${EGRESS})"
exec /opt/justrade/jre/bin/java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/justrade/bench/lib/*" \
    io.justrade.bench.ExternalLoadRunner \
    --endpoints="${ENDPOINTS}" --egress="${EGRESS}" \
    --ops="${OPS}" --users="${USERS}" --symbols="${SYMBOLS}" --batch="${BATCH}" --client-id="${CLIENT_ID}"
