package com.exadbe.write.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.protocol.QueryStreams;
import com.exadbe.read.ExcReadReplica;
import com.exadbe.read.config.ReadReplicaConfig;
import com.exadbe.write.client.config.ClientConfig;
import io.aeron.cluster.ClusterTool;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Idempotency survives a warm restart: dedup records are part of the snapshot,
 * so after a snapshot + warm restart a re-sent {@code (clientId, clientSeq)}
 * replays the cached result instead of re-applying the command. A double apply
 * would reserve the order's hold twice, which the replica balances expose.
 */
@Tag("cluster")
class DedupSurvivesWarmRestartClusterTest {

    private static final int NODES = 3;
    private static final long TIMEOUT_MS = 120_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long UID = 1L;

    // The first client's submissions consume seqs 0-4 (symbol, user, two
    // adjustments, place); the second incarnation re-sends seq 4 verbatim.
    private static final long PLACE_SEQ = 4L;

    @Test
    @Timeout(300)
    void resubmittedCommandAfterWarmRestartReplaysCachedResult(@TempDir final Path baseDir) {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = launchAll(configs, true);

        try {
            // A duplicate replays the cached result verbatim, carrying the
            // ORIGINAL command's id, so track it across the restart.
            final long[] originalPlaceId = {-1L};
            final Map<Long, CommandResultCode> codes = new HashMap<>();
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            codes.put(idLo, code);

            try (ExcClient client = new ExcClient(
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                            .build(),
                    handler)) {
                awaitLeader(client);
                final long symbolId = submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
                final long userId = submit(client, () -> client.addUser(UID));
                final long baseId = submit(client, () -> client.adjustBalance(UID, BASE, 1_000L));
                final long quoteId = submit(client, () -> client.adjustBalance(UID, QUOTE, 1_000_000L));
                final long placeId = submit(client, () -> client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, UID, 0));
                originalPlaceId[0] = placeId;
                drainUntil(client, () -> codes.size() >= 5);
                for (final long id : new long[] {symbolId, userId, baseId, quoteId, placeId}) {
                    assertEquals(CommandResultCode.SUCCESS, codes.get(id));
                }

                // Snapshot every member so the dedup table is part of recovery.
                final int leader = client.leaderMemberId();
                assertTrue(
                        ClusterTool.snapshot(configs[leader].clusterDir(), System.out),
                        "the snapshot request must be accepted");
                awaitCondition(
                        () -> {
                            for (final ClusterNode node : nodes) {
                                if (node.metrics().snapshotsTaken() < 1) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        "every node must take the snapshot");
            }

            // Warm restart: preserve archive + cluster state on all nodes.
            for (int i = 0; i < NODES; i++) {
                nodes[i].close();
                nodes[i] = null;
            }
            launchAllInto(configs, false, nodes);

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
            try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults())) {
                pollReplica(replica, () -> replica.userCount() >= 1 && replica.orderCount() == 1);
                assertEquals(990L, replica.balance(UID, BASE), "the resting ask holds 10 base");

                // A new incarnation reusing clientId 1 re-sends the place with
                // its original clientSeq: the engine must replay the cached
                // result, not re-apply (a re-apply would hold twice: 980).
                final Map<Long, CommandResultCode> resendCodes = new HashMap<>();
                final ResultHandler resendHandler =
                        (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                                resendCodes.put(idLo, code);
                try (ExcClient resend = new ExcClient(
                        ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                                .initialClientSeq(PLACE_SEQ)
                                .build(),
                        resendHandler)) {
                    awaitLeader(resend);
                    submit(resend, () -> resend.placeGtc(SYM, 1L, true, 100L, 10L, 0L, UID, 0));
                    // The duplicate resolves under the original command's id.
                    drainUntil(resend, () -> resendCodes.containsKey(originalPlaceId[0]));
                    assertEquals(
                            CommandResultCode.SUCCESS,
                            resendCodes.get(originalPlaceId[0]),
                            "the duplicate replays the cached SUCCESS result");

                    // A genuinely fresh command still applies normally.
                    final long adjustId = submit(resend, () -> resend.adjustBalance(UID, BASE, 5L));
                    drainUntil(resend, () -> resendCodes.size() >= 2);
                    assertEquals(CommandResultCode.SUCCESS, resendCodes.get(adjustId));
                }

                pollReplica(replica, () -> replica.balance(UID, BASE) == 995L);
                assertEquals(1, replica.orderCount(), "no duplicate order may rest");
            }
        } finally {
            for (final ClusterNode node : nodes) {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    private static ClusterNode[] launchAll(final ClusterConfig[] configs, final boolean cleanStart) {
        final ClusterNode[] nodes = new ClusterNode[NODES];
        launchAllInto(configs, cleanStart, nodes);
        return nodes;
    }

    private static void launchAllInto(
            final ClusterConfig[] configs, final boolean cleanStart, final ClusterNode[] nodes) {
        for (int i = 0; i < NODES; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults(), cleanStart);
        }
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

    private static long submit(final ExcClient client, final java.util.function.LongSupplier op) {
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

    private static void drainUntil(final ExcClient client, final BooleanSupplier done) {
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

    private static void awaitCondition(final BooleanSupplier condition, final String message) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError(message);
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
        throw new AssertionError("replica never reached the expected state");
    }
}
