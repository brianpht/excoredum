#!/bin/sh
# Starts the read replica. JUSTRADE_ARCHIVE is the followed member's archive
# control channel; JUSTRADE_HOST is the replica's own container address (used for
# its archive control responses and log-replay subscription, so the followed
# member can reach it); JUSTRADE_QUERY is the channel the replica binds for
# read-side queries.
set -eu

ARCHIVE="${JUSTRADE_ARCHIVE:?JUSTRADE_ARCHIVE is required}"
# hostname -i may return several space-separated addresses; keep only the first.
_ip="$(hostname -i)"
HOST="${JUSTRADE_HOST:-${_ip%% *}}"
QUERY="${JUSTRADE_QUERY:-aeron:udp?endpoint=0.0.0.0:44000}"
JAVA_OPTS="${JUSTRADE_JAVA_OPTS:--Xms1g -Xmx1g -XX:+UseZGC}"

cat > /tmp/read-core.properties <<EOF
justrade.core.accountCapacity=${JUSTRADE_CORE_ACCOUNT_CAPACITY:-}
justrade.core.dedupClientCapacity=${JUSTRADE_CORE_DEDUP_CLIENT_CAPACITY:-}
justrade.core.dedupWindow=${JUSTRADE_CORE_DEDUP_WINDOW:-}
justrade.core.orderPoolCapacity=${JUSTRADE_CORE_ORDER_POOL_CAPACITY:-}
justrade.core.priceBucketCapacity=${JUSTRADE_CORE_PRICE_BUCKET_CAPACITY:-}
justrade.core.l2MaxLevels=${JUSTRADE_CORE_L2_MAX_LEVELS:-}
justrade.core.eventBufferCapacity=${JUSTRADE_CORE_EVENT_BUFFER_CAPACITY:-}
justrade.core.journalSlotCount=${JUSTRADE_CORE_JOURNAL_SLOT_COUNT:-}
justrade.core.journalSlotSize=${JUSTRADE_CORE_JOURNAL_SLOT_SIZE:-}
EOF

LEDGER_ARGS=""
if [ -n "${JUSTRADE_LEDGER_MAX_ORDERS_PER_USER:-}" ]; then
    LEDGER_ARGS="${LEDGER_ARGS} --ledger-max-orders-per-user=${JUSTRADE_LEDGER_MAX_ORDERS_PER_USER}"
fi
if [ -n "${JUSTRADE_LEDGER_MAX_MARKET_TRADES:-}" ]; then
    LEDGER_ARGS="${LEDGER_ARGS} --ledger-max-market-trades=${JUSTRADE_LEDGER_MAX_MARKET_TRADES}"
fi

# Record the PID so the healthcheck can probe liveness without pgrep.
echo $$ > /data/justrade-read.pid

echo "justrade read replica on ${HOST} following ${ARCHIVE}"
exec java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/justrade/read/lib/*" \
    io.justrade.read.ReadServiceLauncher \
    --archive="${ARCHIVE}" --host="${HOST}" --query="${QUERY}" \
    --core-config=/tmp/read-core.properties \
    $LEDGER_ARGS
