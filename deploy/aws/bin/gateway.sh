#!/bin/sh
# Starts the HTTP/WS gateway on EC2. EXC_CONFIG points at the gateway properties
# file written by user-data; everything else (ingress endpoints, read query
# channel, symbols) lives in that file.
set -eu

CONFIG="${EXC_CONFIG:?EXC_CONFIG is required}"
JAVA_OPTS="${EXC_JAVA_OPTS:--Xms1g -Xmx1g -XX:+UseZGC}"

echo "excoredum gateway with config ${CONFIG}"
exec /opt/excoredum/jre/bin/java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/excoredum/gateway/lib/*" \
    com.exadbe.gateway.GatewayLauncher --config="${CONFIG}"
