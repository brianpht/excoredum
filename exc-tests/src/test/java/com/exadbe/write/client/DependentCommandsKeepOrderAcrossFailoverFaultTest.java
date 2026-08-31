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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Submission order survives a failover: a batch of dependent place/cancel
 * pairs is submitted and the leader is killed immediately, without draining
 * results. A retransmit must re-offer the pairs in their original (clientSeq)
 * order - a cancel re-offered before its place would be rejected as an unknown
 * order and the place would then rest forever. Every cancel must resolve
 * SUCCESS (it only can when the place applied first) and no order may remain.
 */
@Tag("fault")
class DependentCommandsKeepOrderAcrossFailoverFaultTest {

    private static final int NODES = 3;
    private static final long TIMEOUT_MS = 120_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final int PAIRS = 10;

    @Test
    @Timeout(300)
    void dependentPairsSurviveLeaderKillInSubmissionOrder(@TempDir final Path baseDir) {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = new ClusterNode[NODES];
        for (int i = 0; i < NODES; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults());
        }

        final Map<Long, CommandResultCode> codes = new HashMap<>();
        final ResultHandler handler = (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                codes.put(idLo, code);

        try {
            try (ExcClient client = new ExcClient(
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                            .build(),
                    handler)) {
                awaitLeader(client);

                // Setup: symbol plus one funded user per pair, fully drained.
                submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
                for (long uid = 1L; uid <= PAIRS; uid++) {
                    final long u = uid;
                    submit(client, () -> client.addUser(u));
                    submit(client, () -> client.adjustBalance(u, BASE, 100L));
                }
                drainUntil(client, () -> codes.size() >= 1 + 2 * PAIRS);

                // Submit place/cancel pairs back to back, then kill the leader
                // immediately - some pairs are still uncommitted when it dies
                // and must be retransmitted in submission order.
                final long[] cancelIds = new long[PAIRS];
                for (long uid = 1L; uid <= PAIRS; uid++) {
                    final long u = uid;
                    final long orderId = 1000L + u;
                    submit(client, () -> client.placeGtc(SYM, orderId, true, 100L + u, 1L, 0L, u, 0));
                    cancelIds[(int) (u - 1)] = submit(client, () -> client.cancelOrder(SYM, orderId, u));
                }
                final int leader = client.leaderMemberId();
                assertTrue(leader >= 0 && leader < NODES, "a leader must be known before the kill");
                nodes[leader].close();
                nodes[leader] = null;

                final int setupCount = 1 + 2 * PAIRS;
                drainUntil(client, () -> codes.size() >= setupCount + 2 * PAIRS);

                for (int i = 0; i < PAIRS; i++) {
                    assertEquals(
                            CommandResultCode.SUCCESS,
                            codes.get(cancelIds[i]),
                            "cancel " + i + " resolves SUCCESS, which requires its place applied first");
                }
                assertEquals(0L, client.expired(), "no command may expire during the failover");

                // No order may survive: every place was followed by its cancel.
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
                    pollReplica(replica, () -> replica.isCaughtUp() && replica.orderCount() == 0);
                    for (long uid = 1L; uid <= PAIRS; uid++) {
                        assertEquals(100L, replica.balance(uid, BASE), "user " + uid + " balance fully released");
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
