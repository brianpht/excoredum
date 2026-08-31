#!/bin/sh
# Runs the read-side verification: replays the same deterministic workload
# simulation and asserts the read replica's state matches it exactly.
set -eu

QUERY="${JUSTRADE_QUERY:?JUSTRADE_QUERY is required}"
_ip="$(hostname -i)"
EGRESS="aeron:udp?endpoint=${_ip%% *}:0"
OPS="${JUSTRADE_OPS:-100000}"
USERS="${JUSTRADE_USERS:-100}"
SYMBOLS="${JUSTRADE_SYMBOLS:-1}"
TRADE_LIMIT="${JUSTRADE_TRADE_LIMIT:-4096}"
JAVA_OPTS="${JUSTRADE_JAVA_OPTS:--Xms512m -Xmx1g -XX:+UseZGC}"

echo "justrade read verify: ${OPS} ops / ${USERS} users / ${SYMBOLS} symbols against ${QUERY}"
exec java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/justrade/bench/lib/*" \
    io.justrade.bench.ReadVerifyRunner \
    --query="${QUERY}" --egress="${EGRESS}" --ops="${OPS}" --users="${USERS}" --symbols="${SYMBOLS}" --trade-limit="${TRADE_LIMIT}"
