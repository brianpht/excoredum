#!/bin/sh
# Starts the CQRS read replica on EC2. EXC_ARCHIVE is the comma-separated list
# of followed member archive control channels; EXC_HOST is the replica's own
# private IP (used for its archive control responses and log-replay
# subscription); EXC_QUERY is the channel the replica binds for read queries.
set -eu

ARCHIVE="${EXC_ARCHIVE:?EXC_ARCHIVE is required}"
_ip="$(hostname -i)"
HOST="${EXC_HOST:-${_ip%% *}}"
QUERY="${EXC_QUERY:?EXC_QUERY is required}"

echo "excoredum read replica on ${HOST} following ${ARCHIVE}"
exec /opt/excoredum/jre/bin/java \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/excoredum/read/lib/*" \
    com.exadbe.read.ReadServiceLauncher \
    --archive="${ARCHIVE}" --host="${HOST}" --query="${QUERY}"
