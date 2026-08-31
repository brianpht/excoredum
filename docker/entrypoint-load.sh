#!/bin/sh
# Runs the write-side load: submits the deterministic LoadWorkload to the
# cluster over the write client SDK and verifies every result. The client's
# egress channel advertises this container's own address so the cluster can
# reach it.
set -eu

ENDPOINTS="${JUSTRADE_INGRESS:?JUSTRADE_INGRESS is required}"
_ip="$(hostname -i)"
EGRESS="aeron:udp?endpoint=${_ip%% *}:0"
OPS="${JUSTRADE_OPS:-100000}"
USERS="${JUSTRADE_USERS:-100}"
SYMBOLS="${JUSTRADE_SYMBOLS:-1}"
BATCH="${JUSTRADE_BATCH:-16}"
JAVA_OPTS="${JUSTRADE_JAVA_OPTS:--Xms512m -Xmx1g -XX:+UseZGC}"

echo "justrade load: ${OPS} ops / ${USERS} users / ${SYMBOLS} symbols / batch ${BATCH} against ${ENDPOINTS} (egress ${EGRESS})"
exec java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/justrade/bench/lib/*" \
    io.justrade.bench.ExternalLoadRunner \
    --endpoints="${ENDPOINTS}" --egress="${EGRESS}" --ops="${OPS}" --users="${USERS}" --symbols="${SYMBOLS}" --batch="${BATCH}"
