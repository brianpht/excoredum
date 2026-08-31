#!/bin/sh
# Starts the HTTP/WS gateway on EC2. JUSTRADE_CONFIG points at the gateway properties
# file written by user-data; everything else (ingress endpoints, read query
# channel, symbols) lives in that file.
set -eu

CONFIG="${JUSTRADE_CONFIG:?JUSTRADE_CONFIG is required}"
JAVA_OPTS="${JUSTRADE_JAVA_OPTS:--Xms1g -Xmx1g -XX:+UseZGC}"

echo "justrade gateway with config ${CONFIG}"
exec /opt/justrade/jre/bin/java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/justrade/gateway/lib/*" \
    io.justrade.gateway.GatewayLauncher --config="${CONFIG}"
