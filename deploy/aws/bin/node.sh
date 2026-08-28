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

mkdir -p "$BASE"
cat > /tmp/cluster.properties <<EOF
exc.clusterMembers=${MEMBERS}
exc.host=${HOST}
exc.baseDir=${BASE}
EOF

echo "excoredum node ${NODE_ID} on ${HOST} (cleanStart=${CLEAN_START})"
exec /opt/excoredum/jre/bin/java \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -Dexc.nodeId="${NODE_ID}" -Dexc.cleanStart="${CLEAN_START}" \
    -Dexc.metricsIntervalMs="${EXC_METRICS_INTERVAL_MS:-0}" \
    -cp "/opt/excoredum/launcher/lib/*" \
    com.exadbe.launcher.ClusterLauncher --config=/tmp/cluster.properties
