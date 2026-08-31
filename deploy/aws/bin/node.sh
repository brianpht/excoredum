#!/bin/sh
# Starts one justrade cluster node on EC2. Writes the deployment properties
# from the environment (each node advertises its own private IP so the archive
# control and replication channels bind a reachable interface) and launches the
# ClusterLauncher with --config. The runtime JRE lives next to the launcher
# distribution at /opt/justrade/jre.
set -eu

NODE_ID="${JUSTRADE_NODE_ID:?JUSTRADE_NODE_ID is required}"
MEMBERS="${JUSTRADE_MEMBERS:?JUSTRADE_MEMBERS is required}"
# Prefer the explicitly assigned private IP; fall back to the host's own
# address when it is not set (single-instance smoke runs).
_ip="$(hostname -i)"
HOST="${JUSTRADE_HOST:-${_ip%% *}}"
CLEAN_START="${JUSTRADE_CLEAN_START:-true}"
BASE="${JUSTRADE_BASE_DIR:-/data}"
JAVA_OPTS="${JUSTRADE_JAVA_OPTS:--Xms2g -Xmx2g -XX:+UseZGC}"

mkdir -p "$BASE"
cat > /tmp/cluster.properties <<EOF
justrade.clusterMembers=${MEMBERS}
justrade.host=${HOST}
justrade.baseDir=${BASE}
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

echo "justrade node ${NODE_ID} on ${HOST} (cleanStart=${CLEAN_START})"
exec /opt/justrade/jre/bin/java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -Djustrade.nodeId="${NODE_ID}" -Djustrade.cleanStart="${CLEAN_START}" \
    -Djustrade.metricsIntervalMs="${JUSTRADE_METRICS_INTERVAL_MS:-0}" \
    -cp "/opt/justrade/launcher/lib/*" \
    io.justrade.launcher.ClusterLauncher --config=/tmp/cluster.properties
