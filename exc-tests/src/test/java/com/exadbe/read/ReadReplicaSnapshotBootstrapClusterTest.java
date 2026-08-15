package com.exadbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.QueryStreams;
import com.exadbe.read.config.ReadReplicaConfig;
import com.exadbe.write.client.BackpressureException;
import com.exadbe.write.client.ExcClient;
import com.exadbe.write.client.ResultHandler;
import com.exadbe.write.client.config.ClientConfig;
import io.aeron.cluster.ClusterTool;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P7 snapshot bootstrap: a cold read replica (no local checkpoint) loads the
 * newest service snapshot from a member's Archive to fast-forward its engine,
 * then follows the log tail. Because the {@code OrderLedger} is read-side-only
 * and not in the cluster snapshot, the replica rebuilds the full order history
 * by replaying the whole log once; the test waits for both the engine state and
 * the rebuilt ledger to converge.
 */
@Tag("cluster")
class ReadReplicaSnapshotBootstrapClusterTest {

    private static final int NODES = 3;
    private static final long TIMEOUT_MS = 120_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;

    @Test
    @Timeout(300)
    void coldStartBootstrapsFromSnapshotAndRebuildsLedger(@TempDir final Path baseDir) throws Exception {
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
            final Set<Long> results = new HashSet<>();
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            results.add(idLo);
            try (ExcClient client = new ExcClient(
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                            .build(),
                    handler)) {
                awaitLeader(client);
                submitBatch(client);
                drainUntil(client, results, 18);

                // Take a snapshot through Raft so every member holds one at the
                // same cluster-global logPosition.
                final int leader = client.leaderMemberId();
                assertTrue(
                        ClusterTool.snapshot(configs[leader].clusterDir(), System.out),
                        "the snapshot request must be accepted by the consensus module");
                awaitSnapshotTaken(nodes[leader]);
            }

            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                    baseDir.resolve("replica").resolve("driver").toString(),
                    channels,
                    "localhost",
                    QueryStreams.QUERY_REQUEST_CHANNEL,
                    QueryStreams.QUERY_REQUEST_STREAM_ID);
            try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults())) {
                // The engine converges via snapshot bootstrap + tail following.
                pollReplica(replica, () -> replica.userCount() == 5 && replica.orderCount() == 1);
                assertTrue(
                        replica.health().snapshotsLoaded() >= 1,
                        "the cold replica must bootstrap from the cluster snapshot");
                assertEquals(0, replica.health().integrityFailures(), "no snapshot may fail the integrity check");

                // The ledger (read-side-only) is rebuilt by a full-log replay.
                pollReplica(
                        replica,
                        () -> replica.orderHistory(1L).size() == 1
                                && replica.orderHistory(2L).size() == 1);

                assertEquals(990L, replica.balance(1L, BASE), "maker base after the 4-unit fill");
                assertEquals(1_004L, replica.balance(2L, BASE), "taker bought 4 base units");
                assertEquals(1_000_400L, replica.balance(1L, QUOTE), "maker quote after the 4-unit fill");
                assertEquals(1, replica.orderHistory(1L).size(), "user 1's resting ask history rebuilt");
                assertEquals(1, replica.orderHistory(2L).size(), "user 2's filled bid history rebuilt");
            }
        } finally {
            for (final ClusterNode node : nodes) {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    private static void submitBatch(final ExcClient client) {
        submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
        for (long uid = 1L; uid <= 5L; uid++) {
            final long u = uid;
            submit(client, () -> client.addUser(u));
            submit(client, () -> client.adjustBalance(u, BASE, 1_000L));
            submit(client, () -> client.adjustBalance(u, QUOTE, 1_000_000L));
        }
        submit(client, () -> client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, 1L, 0));
        submit(client, () -> client.placeGtc(SYM, 2L, false, 105L, 4L, 105L, 2L, 0));
    }

    private static void awaitLeader(final ExcClient client) {
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

    private static void awaitSnapshotTaken(final ClusterNode node) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (node.metrics().snapshotsTaken() >= 1) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("snapshot was not taken before the timeout");
    }

    private static void pollReplica(final ExcReadReplica replica, final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            replica.poll();
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("replica never reached the expected state: users=" + replica.userCount()
                + " orders=" + replica.orderCount()
                + " applied=" + replica.appliedPosition()
                + " snapshotsLoaded=" + replica.health().snapshotsLoaded()
                + " integrityFailures=" + replica.health().integrityFailures()
                + " failovers=" + replica.health().failovers()
                + " source=" + replica.currentSource());
    }

    private static void submit(final ExcClient client, final LongSupplier op) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                op.getAsLong();
                return;
            } catch (final BackpressureException e) {
                client.poll();
                Thread.onSpinWait();
            }
        }
        throw new AssertionError("could not submit command within timeout");
    }

    private static void drainUntil(final ExcClient client, final Set<Long> results, final int target) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (results.size() >= target) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("only " + results.size() + " of " + target + " results within timeout");
    }
}
