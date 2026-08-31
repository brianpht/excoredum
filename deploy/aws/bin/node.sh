#!/bin/sh
# Starts one excoredum cluster node on EC2. Writes the deployment properties
# from the environment (each node advertises its own private IP so the archive
# control and replication channels bind a reachable interface) and launches the
# ClusterLauncher with --config. The runtime JRE lives next to the launcher
# distribution at /opt/excoredum/jre.
set -eu

NODE_ID="${EXC_NODE_ID:?EXC_NODE_ID is required}"
MEMBERS="${EXC_MEMBERS:?EXC_MEMBERS is required}"
# Prefer the explicitly assigned private IP; fall back to the host's own
# address when it is not set (single-instance smoke runs).
_ip="$(hostname -i)"
HOST="${EXC_HOST:-${_ip%% *}}"
CLEAN_START="${EXC_CLEAN_START:-true}"
BASE="${EXC_BASE_DIR:-/data}"
JAVA_OPTS="${EXC_JAVA_OPTS:--Xms2g -Xmx2g -XX:+UseZGC}"

mkdir -p "$BASE"
cat > /tmp/cluster.properties <<EOF
exc.clusterMembers=${MEMBERS}
exc.host=${HOST}
exc.baseDir=${BASE}
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

echo "excoredum node ${NODE_ID} on ${HOST} (cleanStart=${CLEAN_START})"
exec /opt/excoredum/jre/bin/java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -Dexc.nodeId="${NODE_ID}" -Dexc.cleanStart="${CLEAN_START}" \
    -Dexc.metricsIntervalMs="${EXC_METRICS_INTERVAL_MS:-0}" \
    -cp "/opt/excoredum/launcher/lib/*" \
    com.exadbe.launcher.ClusterLauncher --config=/tmp/cluster.properties
