package io.justrade.write.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.CommandResultCode;
import io.justrade.protocol.QueryStreams;
import io.justrade.read.ReadReplica;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exactly-once across a failover with commands still in flight: a batch of
 * orders is submitted and the leader is killed immediately, without draining
 * its results. The client's idempotent retry (same clientId / clientSeq /
 * command id) plus the engine dedup must deliver every command exactly once:
 * one SUCCESS result per command id, one resting order per user, and exactly
 * one hold per balance.
 */
@Tag("fault")
class InFlightRetryAcrossFailoverFaultTest {

    private static final int NODES = 3;
    private static final long TIMEOUT_MS = 120_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final int ORDERS = 20;

    @Test
    @Timeout(300)
    void inFlightBatchSurvivesLeaderKillExactlyOnce(@TempDir final Path baseDir) {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = new ClusterNode[NODES];
        for (int i = 0; i < NODES; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults());
        }

        final Set<Long> results = new HashSet<>();
        final Map<Long, CommandResultCode> codes = new HashMap<>();
        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    results.add(idLo);
                    codes.put(idLo, code);
                };

        try {
            try (WriteClient client = new WriteClient(
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                            .build(),
                    handler)) {
                awaitLeader(client);

                // Setup: symbol plus one funded user per order, fully drained.
                submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
                for (long uid = 1L; uid <= ORDERS; uid++) {
                    final long u = uid;
                    submit(client, () -> client.addUser(u));
                    submit(client, () -> client.adjustBalance(u, BASE, 100L));
                }
                drainUntil(client, () -> results.size() >= 1 + 2 * ORDERS);

                // Submit the batch, then kill the leader immediately - some of
                // these commands have not been committed when the leader dies.
                final Set<Long> batch = new HashSet<>();
                for (long uid = 1L; uid <= ORDERS; uid++) {
                    final long u = uid;
                    batch.add(submit(client, () -> client.placeGtc(SYM, u, true, 100L + u, 1L, 0L, u, 0)));
                }
                final int leader = client.leaderMemberId();
                assertTrue(leader >= 0 && leader < NODES, "a leader must be known before the kill");
                nodes[leader].close();
                nodes[leader] = null;

                final int setupCount = 1 + 2 * ORDERS;
                drainUntil(client, () -> results.size() >= setupCount + ORDERS);

                for (final long id : batch) {
                    assertEquals(
                            CommandResultCode.SUCCESS,
                            codes.get(id),
                            "every in-flight command resolves to exactly one SUCCESS");
                }
                assertEquals(0L, client.expired(), "no command may expire during the failover");

                // Exactly-once application: one resting order per user and one
                // hold per balance (a double apply would hold twice per user).
                final String[] channels = new String[NODES];
                for (int i = 0; i < NODES; i++) {
                    channels[i] = configs[i].archiveControlChannel();
                }
                final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                        baseDir.resolve("replica").resolve("driver").toString(),
                        channels,
                        "localhost",
                        QueryStreams.QUERY_REQUEST_CHANNEL,
                        QueryStreams.QUERY_REQUEST_STREAM_ID);
                try (ReadReplica replica = new ReadReplica(replicaConfig, CoreConfig.defaults())) {
                    pollReplica(replica, () -> replica.orderCount() == ORDERS);
                    for (long uid = 1L; uid <= ORDERS; uid++) {
                        assertEquals(
                                99L, replica.balance(uid, BASE), "user " + uid + " holds exactly one order's base");
                    }
                }
            }
        } finally {
            for (final ClusterNode node : nodes) {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    private static void awaitLeader(final WriteClient client) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (client.leaderMemberId() >= 0) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no leader established within timeout");
    }

    private static long submit(final WriteClient client, final java.util.function.LongSupplier op) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                return op.getAsLong();
            } catch (final BackpressureException e) {
                client.poll();
                Thread.onSpinWait();
            }
        }
        throw new AssertionError("could not submit within timeout");
    }

    private static void drainUntil(final WriteClient client, final BooleanSupplier done) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (done.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("condition not met within timeout");
    }

    private static void pollReplica(final ReadReplica replica, final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            replica.poll();
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("replica never reached the expected state");
    }
}
