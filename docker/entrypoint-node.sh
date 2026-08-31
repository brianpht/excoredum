#!/bin/sh
# Starts one cluster node. Writes the deployment properties file from the
# environment (each node advertises its own container address so the archive
# control and replication channels bind a reachable interface) and launches
# the ClusterLauncher with --config.
set -eu

NODE_ID="${EXC_NODE_ID:?EXC_NODE_ID is required}"
MEMBERS="${EXC_CLUSTER_MEMBERS:?EXC_CLUSTER_MEMBERS is required}"
# hostname -i may return several space-separated addresses on a multi-homed
# container; a channel URI takes exactly one, so keep only the first.
_ip="$(hostname -i)"
HOST="${EXC_HOST:-${_ip%% *}}"
CLEAN_START="${EXC_CLEAN_START:-true}"
JAVA_OPTS="${EXC_JAVA_OPTS:--Xms2g -Xmx2g -XX:+UseZGC}"

cat > /tmp/cluster.properties <<EOF
exc.clusterMembers=${MEMBERS}
exc.host=${HOST}
exc.baseDir=/data
exc.core.accountCapacity=${EXC_CORE_ACCOUNT_CAPACITY:-}
exc.core.dedupClientCapacity=${EXC_CORE_DEDUP_CLIENT_CAPACITY:-}
exc.core.dedupWindow=${EXC_CORE_DEDUP_WINDOW:-}
exc.core.orderPoolCapacity=${EXC_CORE_ORDER_POOL_CAPACITY:-}
exc.core.priceBucketCapacity=${EXC_CORE_PRICE_BUCKET_CAPACITY:-}
exc.core.l2MaxLevels=${EXC_CORE_L2_MAX_LEVELS:-}
exc.core.eventBufferCapacity=${EXC_CORE_EVENT_BUFFER_CAPACITY:-}
exc.core.journalSlotCount=${EXC_CORE_JOURNAL_SLOT_COUNT:-}
exc.core.journalSlotSize=${EXC_CORE_JOURNAL_SLOT_SIZE:-}
exc.aeron.termLength=${EXC_AERON_TERM_LENGTH:-}
EOF

# Record the PID (the shell's own PID, which the exec below preserves) so the
# healthcheck can probe liveness without depending on pgrep being installed.
echo $$ > /data/excoredum-node.pid

echo "excoredum node ${NODE_ID} on ${HOST} (cleanStart=${CLEAN_START})"
exec java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -Dexc.nodeId="${NODE_ID}" -Dexc.cleanStart="${CLEAN_START}" \
    -cp "/opt/excoredum/launcher/lib/*" \
    com.exadbe.launcher.ClusterLauncher --config=/tmp/cluster.properties
