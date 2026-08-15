package com.exadbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.QueryStreams;
import com.exadbe.read.config.ReadReplicaConfig;
import com.exadbe.read.report.TotalCurrencyBalance;
import com.exadbe.write.client.BackpressureException;
import com.exadbe.write.client.ExcClient;
import com.exadbe.write.client.ResultHandler;
import com.exadbe.write.client.config.ClientConfig;
import java.nio.file.Path;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P5 read-side failover: a read replica following node-0's archive keeps
 * replicating after node-0 dies by failing over to another member's archive,
 * rebuilding its state from that member's recording and then following live.
 * The read model is eventually consistent, so the assertions wait for the
 * replica to catch up to the full (pre- and post-kill) state.
 */
@Tag("fault")
class ReadReplicaFailoverIntegrationTest {

    private static final int NODES = 3;
    private static final long TIMEOUT_MS = 120_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;

    private final long[] lastIdLo = {-1L};
    private ExcClient client;
    private ExcReadReplica replica;

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
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            lastIdLo[0] = idLo;
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                    .build();
            try (ExcClient client = new ExcClient(clientConfig, handler)) {
                this.client = client;

                final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                        baseDir.resolve("replica").resolve("driver").toString(),
                        channels,
                        "localhost",
                        QueryStreams.QUERY_REQUEST_CHANNEL,
                        QueryStreams.QUERY_REQUEST_STREAM_ID);
                try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults())) {
                    this.replica = replica;

                    submitBatch1();
                    pollUntil(() -> replica.userCount() == 5 && replica.orderCount() == 1);
                    assertEquals(0, replica.currentSource(), "the replica initially follows the primary source");

                    // Kill the replica's current source; the cluster (quorum 2
                    // of 3) keeps serving and the replica must fail over.
                    nodes[0].close();
                    nodes[0] = null;

                    submitBatch2();

                    pollUntil(() -> replica.userCount() == 10 && replica.orderCount() == 2);

                    assertNotEquals(0, replica.currentSource(), "the replica must fail over to another member archive");
                    assertTrue(replica.appliedPosition() > 0L, "the replica must consume the log after failover");
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

    private void submitBatch1() {
        // Symbol plus users 1..5 with funding; a resting ask 10 @ 100 and a
        // crossing bid fills 4 of it.
        await(submit(() -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L)));
        for (long uid = 1L; uid <= 5L; uid++) {
            final long u = uid;
            await(submit(() -> client.addUser(u)));
            await(submit(() -> client.adjustBalance(u, BASE, 1_000L)));
            await(submit(() -> client.adjustBalance(u, QUOTE, 1_000_000L)));
        }
        await(submit(() -> client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, 1L, 0)));
        await(submit(() -> client.placeGtc(SYM, 2L, false, 105L, 4L, 105L, 2L, 0)));
    }

    private void submitBatch2() {
        // Users 6..10 plus a resting ask 5 @ 100 crossed by a bid 2 @ 100.
        for (long uid = 6L; uid <= 10L; uid++) {
            final long u = uid;
            await(submit(() -> client.addUser(u)));
            await(submit(() -> client.adjustBalance(u, BASE, 1_000L)));
            await(submit(() -> client.adjustBalance(u, QUOTE, 1_000_000L)));
        }
        await(submit(() -> client.placeGtc(SYM, 3L, true, 100L, 5L, 0L, 6L, 0)));
        await(submit(() -> client.placeGtc(SYM, 4L, false, 100L, 2L, 100L, 7L, 0)));
    }

    private long submit(final LongSupplier command) {
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

    private void await(final long commandIdLo) {
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

    private void pollUntil(final java.util.function.BooleanSupplier condition) {
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
