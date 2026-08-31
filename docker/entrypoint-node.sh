#!/bin/sh
# Starts one cluster node. Writes the deployment properties file from the
# environment (each node advertises its own container address so the archive
# control and replication channels bind a reachable interface) and launches
# the ClusterLauncher with --config.
set -eu

NODE_ID="${JUSTRADE_NODE_ID:?JUSTRADE_NODE_ID is required}"
MEMBERS="${JUSTRADE_CLUSTER_MEMBERS:?JUSTRADE_CLUSTER_MEMBERS is required}"
# hostname -i may return several space-separated addresses on a multi-homed
# container; a channel URI takes exactly one, so keep only the first.
_ip="$(hostname -i)"
HOST="${JUSTRADE_HOST:-${_ip%% *}}"
CLEAN_START="${JUSTRADE_CLEAN_START:-true}"
JAVA_OPTS="${JUSTRADE_JAVA_OPTS:--Xms2g -Xmx2g -XX:+UseZGC}"

cat > /tmp/cluster.properties <<EOF
justrade.clusterMembers=${MEMBERS}
justrade.host=${HOST}
justrade.baseDir=/data
justrade.core.accountCapacity=${JUSTRADE_CORE_ACCOUNT_CAPACITY:-}
justrade.core.dedupClientCapacity=${JUSTRADE_CORE_DEDUP_CLIENT_CAPACITY:-}
justrade.core.dedupWindow=${JUSTRADE_CORE_DEDUP_WINDOW:-}
justrade.core.orderPoolCapacity=${JUSTRADE_CORE_ORDER_POOL_CAPACITY:-}
justrade.core.priceBucketCapacity=${JUSTRADE_CORE_PRICE_BUCKET_CAPACITY:-}
justrade.core.l2MaxLevels=${JUSTRADE_CORE_L2_MAX_LEVELS:-}
justrade.core.eventBufferCapacity=${JUSTRADE_CORE_EVENT_BUFFER_CAPACITY:-}
justrade.core.journalSlotCount=${JUSTRADE_CORE_JOURNAL_SLOT_COUNT:-}
justrade.core.journalSlotSize=${JUSTRADE_CORE_JOURNAL_SLOT_SIZE:-}
justrade.aeron.termLength=${JUSTRADE_AERON_TERM_LENGTH:-}
EOF

# Record the PID (the shell's own PID, which the exec below preserves) so the
# healthcheck can probe liveness without depending on pgrep being installed.
echo $$ > /data/justrade-node.pid

echo "justrade node ${NODE_ID} on ${HOST} (cleanStart=${CLEAN_START})"
exec java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -Djustrade.nodeId="${NODE_ID}" -Djustrade.cleanStart="${CLEAN_START}" \
    -cp "/opt/justrade/launcher/lib/*" \
    io.justrade.launcher.ClusterLauncher --config=/tmp/cluster.properties
