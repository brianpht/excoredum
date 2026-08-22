#!/bin/sh
# Runs the read-side verification: replays the same deterministic workload
# simulation and asserts the read replica's state matches it exactly.
set -eu

QUERY="${EXC_QUERY:?EXC_QUERY is required}"
_ip="$(hostname -i)"
EGRESS="aeron:udp?endpoint=${_ip%% *}:0"
OPS="${EXC_OPS:-100000}"
USERS="${EXC_USERS:-100}"

echo "excoredum read verify: ${OPS} ops / ${USERS} users against ${QUERY}"
exec java \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/excoredum/bench/lib/*" \
    com.exadbe.bench.ReadVerifyRunner \
    --query="${QUERY}" --egress="${EGRESS}" --ops="${OPS}" --users="${USERS}"
