#!/bin/sh
# Runs the write-side load: submits the deterministic LoadWorkload to the
# cluster over the write client SDK and verifies every result. The client's
# egress channel advertises this container's own address so the cluster can
# reach it.
set -eu

ENDPOINTS="${EXC_INGRESS:?EXC_INGRESS is required}"
_ip="$(hostname -i)"
EGRESS="aeron:udp?endpoint=${_ip%% *}:0"
OPS="${EXC_OPS:-100000}"
USERS="${EXC_USERS:-100}"
SYMBOLS="${EXC_SYMBOLS:-1}"
BATCH="${EXC_BATCH:-16}"
JAVA_OPTS="${EXC_JAVA_OPTS:--Xms512m -Xmx1g -XX:+UseZGC}"

echo "excoredum load: ${OPS} ops / ${USERS} users / ${SYMBOLS} symbols / batch ${BATCH} against ${ENDPOINTS} (egress ${EGRESS})"
exec java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/excoredum/bench/lib/*" \
    com.exadbe.bench.ExternalLoadRunner \
    --endpoints="${ENDPOINTS}" --egress="${EGRESS}" --ops="${OPS}" --users="${USERS}" --symbols="${SYMBOLS}" --batch="${BATCH}"
