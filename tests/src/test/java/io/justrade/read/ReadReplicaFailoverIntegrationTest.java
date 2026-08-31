package io.justrade.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.QueryStreams;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.read.report.TotalCurrencyBalance;
import io.justrade.write.client.BackpressureException;
import io.justrade.write.client.ExcClient;
import io.justrade.write.client.ResultHandler;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Path;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P5 read-side failover: a read replica following node-0's archive keeps
 * replicating after node-0 dies by failing over to another member's archive.
 * Because recording positions are cluster-global, the replica resumes the
 * replay from the position already applied (no full rebuild), so
 * {@code appliedPosition} is monotonic across the switch. The read model is
 * eventually consistent, so the assertions wait for the replica to catch up to
 * the full (pre- and post-kill) state.
 */
@Tag("fault")
class ReadReplicaFailoverIntegrationTest {

    private static final int NODES = 3;
    private static final long TIMEOUT_MS = 120_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;

    @Test
    @Timeout(300)
    void replicaFailsOverToAnotherMemberWhenSourceDies(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = new ClusterNode[NODES];
        for (int i = 0; i < NODES; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults());
        }

        final String[] channels = new String[NODES];
        for (int i = 0; i < NODES; i++) {
            channels[i] = configs[i].archiveControlChannel();
        }

        try {
            final long[] lastIdLo = {-1L};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            lastIdLo[0] = idLo;
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                    .build();
            try (ExcClient client = new ExcClient(clientConfig, handler)) {
                final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                        baseDir.resolve("replica").resolve("driver").toString(),
                        channels,
                        "localhost",
                        QueryStreams.QUERY_REQUEST_CHANNEL,
                        QueryStreams.QUERY_REQUEST_STREAM_ID);
                try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults())) {
                    submitBatch1(client, lastIdLo, replica);
                    pollUntil(client, replica, () -> replica.userCount() == 5 && replica.orderCount() == 1);
                    assertEquals(0, replica.currentSource(), "the replica initially follows the primary source");

                    final long positionBeforeKill = replica.appliedPosition();
                    assertTrue(positionBeforeKill > 0L, "the replica must consume the log before the kill");

                    // Kill the replica's current source; the cluster (quorum 2
                    // of 3) keeps serving and the replica must fail over.
                    nodes[0].close();
                    nodes[0] = null;

                    submitBatch2(client, lastIdLo, replica);

                    // The order count reaches 2 as soon as the batch-2 ask rests,
                    // before the crossing bid is applied, so wait for the final
                    // maker quote as well before asserting the balances.
                    pollUntil(
                            client,
                            replica,
                            () -> replica.userCount() == 10
                                    && replica.orderCount() == 2
                                    && replica.balance(1L, QUOTE) == 1_000_600L);

                    assertNotEquals(0, replica.currentSource(), "the replica must fail over to another member archive");
                    // Positions are cluster-global: the failover resumes from the
                    // applied position instead of rebuilding from 0, so the
                    // position is strictly monotonic across the switch.
                    assertTrue(
                            replica.appliedPosition() > positionBeforeKill,
                            "appliedPosition must advance monotonically across the failover, was "
                                    + positionBeforeKill
                                    + " -> "
                                    + replica.appliedPosition());
                    assertEquals(990L, replica.balance(1L, BASE), "maker base after the 4-unit fill");
                    // FIFO within a price level: the batch-2 bid crosses the
                    // OLDER resting ask (user 1's remainder), not user 6's.
                    assertEquals(1_000_600L, replica.balance(1L, QUOTE), "maker quote: sold 4 + 2 units at price 100");
                    assertEquals(1_004L, replica.balance(2L, BASE), "taker bought 4 base units");
                    assertEquals(1_002L, replica.balance(7L, BASE), "batch-2 bidder bought 2 base units");
                    assertEquals(1_000_000L, replica.balance(6L, QUOTE), "batch-2 asker never filled (FIFO)");
                    assertEquals(995L, replica.balance(6L, BASE), "batch-2 asker still reserves its 5-unit ask");

                    final TotalCurrencyBalance totals = replica.totalCurrencyBalance();
                    assertEquals(10_000L, totals.total(BASE), "base conserved across both batches");
                    assertEquals(10_000_000L, totals.total(QUOTE), "quote conserved across both batches");
                    assertEquals(0L, totals.fees(BASE), "zero fees, zero collected");
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

    @Test
    @Timeout(300)
    void replicaRecoversWhenEverySourceComesBack(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = new ClusterNode[NODES];
        for (int i = 0; i < NODES; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults());
        }

        final String[] channels = new String[NODES];
        for (int i = 0; i < NODES; i++) {
            channels[i] = configs[i].archiveControlChannel();
        }

        try {
            final long[] lastIdLo = {-1L};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            lastIdLo[0] = idLo;
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                    .build();
            try (ExcClient client = new ExcClient(clientConfig, handler)) {
                final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                        baseDir.resolve("replica").resolve("driver").toString(),
                        channels,
                        "localhost",
                        QueryStreams.QUERY_REQUEST_CHANNEL,
                        QueryStreams.QUERY_REQUEST_STREAM_ID);
                try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults())) {
                    submitBatch1(client, lastIdLo, replica);
                    pollUntil(client, replica, () -> replica.userCount() == 5 && replica.orderCount() == 1);
                    final long positionBefore = replica.appliedPosition();
                    assertTrue(positionBefore > 0L, "the replica must consume the log before the outage");

                    // All sources die: the replica keeps serving its last state
                    // and marks itself stale without resetting its position.
                    for (int i = 0; i < NODES; i++) {
                        nodes[i].close();
                        nodes[i] = null;
                    }
                    final long staleDeadline = System.currentTimeMillis() + TIMEOUT_MS;
                    while (System.currentTimeMillis() < staleDeadline && replica.isHealthy()) {
                        replica.poll();
                        Thread.onSpinWait();
                    }
                    assertFalse(replica.isHealthy(), "the replica must report stale while every source is down");
                    // Fragments already buffered before the sources died may still
                    // be drained after the kill, so the position may advance; the
                    // invariant is that it never goes backward.
                    assertTrue(
                            replica.appliedPosition() >= positionBefore,
                            "a lost source must not reset the applied position, was "
                                    + positionBefore
                                    + " -> "
                                    + replica.appliedPosition());

                    // Warm restart every member (state preserved); the replica
                    // reconnects and resumes from its applied position.
                    for (int i = 0; i < NODES; i++) {
                        nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults(), false);
                    }
                    submitBatch2(client, lastIdLo, replica);
                    // The order count reaches 2 as soon as the batch-2 ask rests,
                    // before the crossing bid is applied, so wait for the final
                    // maker quote as well before asserting the balances.
                    pollUntil(
                            client,
                            replica,
                            () -> replica.userCount() == 10
                                    && replica.orderCount() == 2
                                    && replica.balance(1L, QUOTE) == 1_000_600L);
                    assertTrue(
                            replica.appliedPosition() > positionBefore,
                            "the replica must resume from its applied position after the cluster returns, was "
                                    + positionBefore
                                    + " -> "
                                    + replica.appliedPosition());
                    assertEquals(990L, replica.balance(1L, BASE), "maker base after the 4-unit fill");
                    assertEquals(1_004L, replica.balance(2L, BASE), "taker bought 4 base units");
                    assertEquals(1_000_600L, replica.balance(1L, QUOTE), "maker quote after both batches");
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

    private static void submitBatch1(final ExcClient client, final long[] lastIdLo, final ExcReadReplica replica) {
        // Symbol plus users 1..5 with funding; a resting ask 10 @ 100 and a
        // crossing bid fills 4 of it.
        await(client, replica, submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L)), lastIdLo);
        for (long uid = 1L; uid <= 5L; uid++) {
            final long u = uid;
            await(client, replica, submit(client, () -> client.addUser(u)), lastIdLo);
            await(client, replica, submit(client, () -> client.adjustBalance(u, BASE, 1_000L)), lastIdLo);
            await(client, replica, submit(client, () -> client.adjustBalance(u, QUOTE, 1_000_000L)), lastIdLo);
        }
        await(client, replica, submit(client, () -> client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, 1L, 0)), lastIdLo);
        await(client, replica, submit(client, () -> client.placeGtc(SYM, 2L, false, 105L, 4L, 105L, 2L, 0)), lastIdLo);
    }

    private static void submitBatch2(final ExcClient client, final long[] lastIdLo, final ExcReadReplica replica) {
        // Users 6..10 plus a resting ask 5 @ 100 crossed by a bid 2 @ 100.
        for (long uid = 6L; uid <= 10L; uid++) {
            final long u = uid;
            await(client, replica, submit(client, () -> client.addUser(u)), lastIdLo);
            await(client, replica, submit(client, () -> client.adjustBalance(u, BASE, 1_000L)), lastIdLo);
            await(client, replica, submit(client, () -> client.adjustBalance(u, QUOTE, 1_000_000L)), lastIdLo);
        }
        await(client, replica, submit(client, () -> client.placeGtc(SYM, 3L, true, 100L, 5L, 0L, 6L, 0)), lastIdLo);
        await(client, replica, submit(client, () -> client.placeGtc(SYM, 4L, false, 100L, 2L, 100L, 7L, 0)), lastIdLo);
    }

    private static long submit(final ExcClient client, final LongSupplier command) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                return command.getAsLong();
            } catch (final BackpressureException e) {
                client.poll();
                Thread.onSpinWait();
            }
        }
        throw new AssertionError("could not submit within timeout");
    }

    private static void await(
            final ExcClient client, final ExcReadReplica replica, final long commandIdLo, final long[] lastIdLo) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            replica.poll();
            if (lastIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no result for command id " + commandIdLo);
    }

    private static void pollUntil(
            final ExcClient client, final ExcReadReplica replica, final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            replica.poll();
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("replica never reached the expected state");
    }
}
