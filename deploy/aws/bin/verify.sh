#!/bin/sh
# Runs the read-side verification after a load run: replays the same
# deterministic workload simulation and asserts the read replica's state
# matches it exactly. EXC_EGRESS must advertise this instance's private IP so
# the read replica can reach it.
set -eu

QUERY="${EXC_QUERY:?EXC_QUERY is required}"
_ip="$(hostname -i)"
EGRESS="aeron:udp?endpoint=${_ip%% *}:0"
OPS="${EXC_OPS:-100000}"
USERS="${EXC_USERS:-100}"
SYMBOLS="${EXC_SYMBOLS:-1}"
TRADE_LIMIT="${EXC_TRADE_LIMIT:-4096}"
JAVA_OPTS="${EXC_JAVA_OPTS:--Xms512m -Xmx1g -XX:+UseZGC}"

echo "excoredum read verify: ${OPS} ops / ${USERS} users / ${SYMBOLS} symbols against ${QUERY}"
exec /opt/excoredum/jre/bin/java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/excoredum/bench/lib/*" \
    com.exadbe.bench.ReadVerifyRunner \
    --query="${QUERY}" --egress="${EGRESS}" \
    --ops="${OPS}" --users="${USERS}" --symbols="${SYMBOLS}" --trade-limit="${TRADE_LIMIT}"
