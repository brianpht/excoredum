#!/bin/sh
# Starts one cluster node. Writes the deployment properties file from the
# environment (each node advertises its own container address so the archive
# control and replication channels bind a reachable interface) and launches
# the ClusterLauncher with --config.
set -eu

NODE_ID="${EXC_NODE_ID:?EXC_NODE_ID is required}"
MEMBERS="${EXC_CLUSTER_MEMBERS:?EXC_CLUSTER_MEMBERS is required}"
HOST="${EXC_HOST:-$(hostname -i)}"
CLEAN_START="${EXC_CLEAN_START:-true}"

cat > /tmp/cluster.properties <<EOF
exc.clusterMembers=${MEMBERS}
exc.host=${HOST}
exc.baseDir=/data
EOF

echo "excoredum node ${NODE_ID} on ${HOST} (cleanStart=${CLEAN_START})"
exec java \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -Dexc.nodeId="${NODE_ID}" -Dexc.cleanStart="${CLEAN_START}" \
    -cp "/opt/excoredum/launcher/lib/*" \
    com.exadbe.launcher.ClusterLauncher --config=/tmp/cluster.properties
