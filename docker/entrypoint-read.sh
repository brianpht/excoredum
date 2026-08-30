#!/bin/sh
# Starts the read replica. EXC_ARCHIVE is the followed member's archive
# control channel; EXC_HOST is the replica's own container address (used for
# its archive control responses and log-replay subscription, so the followed
# member can reach it); EXC_QUERY is the channel the replica binds for
# read-side queries.
set -eu

ARCHIVE="${EXC_ARCHIVE:?EXC_ARCHIVE is required}"
# hostname -i may return several space-separated addresses; keep only the first.
_ip="$(hostname -i)"
HOST="${EXC_HOST:-${_ip%% *}}"
QUERY="${EXC_QUERY:-aeron:udp?endpoint=0.0.0.0:44000}"
JAVA_OPTS="${EXC_JAVA_OPTS:--Xms1g -Xmx1g -XX:+UseZGC}"

cat > /tmp/read-core.properties <<EOF
exc.core.symbolCapacity=${EXC_CORE_SYMBOL_CAPACITY:-}
exc.core.accountCapacity=${EXC_CORE_ACCOUNT_CAPACITY:-}
exc.core.dedupClientCapacity=${EXC_CORE_DEDUP_CLIENT_CAPACITY:-}
exc.core.dedupWindow=${EXC_CORE_DEDUP_WINDOW:-}
exc.core.orderPoolCapacity=${EXC_CORE_ORDER_POOL_CAPACITY:-}
exc.core.priceBucketCapacity=${EXC_CORE_PRICE_BUCKET_CAPACITY:-}
exc.core.l2MaxLevels=${EXC_CORE_L2_MAX_LEVELS:-}
exc.core.eventBufferCapacity=${EXC_CORE_EVENT_BUFFER_CAPACITY:-}
exc.core.journalSlotCount=${EXC_CORE_JOURNAL_SLOT_COUNT:-}
exc.core.journalSlotSize=${EXC_CORE_JOURNAL_SLOT_SIZE:-}
EOF

LEDGER_ARGS=""
if [ -n "${EXC_LEDGER_MAX_ORDERS_PER_USER:-}" ]; then
    LEDGER_ARGS="${LEDGER_ARGS} --ledger-max-orders-per-user=${EXC_LEDGER_MAX_ORDERS_PER_USER}"
fi
if [ -n "${EXC_LEDGER_MAX_MARKET_TRADES:-}" ]; then
    LEDGER_ARGS="${LEDGER_ARGS} --ledger-max-market-trades=${EXC_LEDGER_MAX_MARKET_TRADES}"
fi

# Record the PID so the healthcheck can probe liveness without pgrep.
echo $$ > /data/excoredum-read.pid

echo "excoredum read replica on ${HOST} following ${ARCHIVE}"
exec java \
    $JAVA_OPTS \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "/opt/excoredum/read/lib/*" \
    com.exadbe.read.ReadServiceLauncher \
    --archive="${ARCHIVE}" --host="${HOST}" --query="${QUERY}" \
    --core-config=/tmp/read-core.properties \
    $LEDGER_ARGS
