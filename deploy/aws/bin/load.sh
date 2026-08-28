#!/bin/sh
# Runs the write-side load against a deployed cluster over the write client SDK
# and verifies every result. EXC_EGRESS must advertise this instance's private
# IP so the cluster can reach it; EXC_CLIENT_ID must be unique per concurrently
# running load generator.
set -eu

ENDPOINTS="${EXC_ENDPOINTS:?EXC_ENDPOINTS is required}"
_ip="$(hostname -i)"
EGRESS="aeron:udp?endpoint=${_ip%% *}:0"
OPS="${EXC_OPS:-100000}"
USERS="${EXC_USERS:-100}"
CLIENT_ID="${EXC_CLIENT_ID:-1}"

echo "excoredum load: ${OPS} ops / ${USERS} users against ${ENDPOINTS} (clientId=${CLIENT_ID}, egress ${EGRESS})"
exec /opt/excoredum/jre/bin/java \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/excoredum/bench/lib/*" \
    com.exadbe.bench.ExternalLoadRunner \
    --endpoints="${ENDPOINTS}" --egress="${EGRESS}" \
    --ops="${OPS}" --users="${USERS}" --client-id="${CLIENT_ID}"
