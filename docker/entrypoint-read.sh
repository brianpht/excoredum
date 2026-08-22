#!/bin/sh
# Starts the read replica. EXC_ARCHIVE is the followed member's archive
# control channel; EXC_HOST is the replica's own container address (used for
# its archive control responses and log-replay subscription, so the followed
# member can reach it); EXC_QUERY is the channel the replica binds for
# read-side queries.
set -eu

ARCHIVE="${EXC_ARCHIVE:?EXC_ARCHIVE is required}"
# hostname -i may return several space-separated addresses; keep only the first.
_ip="$(hostname -i)"
HOST="${EXC_HOST:-${_ip%% *}}"
QUERY="${EXC_QUERY:-aeron:udp?endpoint=0.0.0.0:44000}"

# Record the PID so the healthcheck can probe liveness without pgrep.
echo $$ > /data/excoredum-read.pid

echo "excoredum read replica on ${HOST} following ${ARCHIVE}"
exec java \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/excoredum/read/lib/*" \
    com.exadbe.read.ReadServiceLauncher \
    --archive="${ARCHIVE}" --host="${HOST}" --query="${QUERY}"
