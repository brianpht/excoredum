package io.justrade.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.QueryStreams;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.write.client.BackpressureException;
import io.justrade.write.client.ResultHandler;
import io.justrade.write.client.WriteClient;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P9 rebuild-from-zero: the replica restarts from a checkpoint whose applied
 * position is not covered by the reachable archive's consensus recording (the
 * cluster was recreated fresh with a much shorter log). No source can resume
 * the replay from that position, so the replica must discard the checkpointed
 * state and rebuild from the start of the log, converging to the NEW cluster's
 * state with the old state fully gone. This exercises the
 * {@code resetReplication} path, which no other test reaches.
 */
@Tag("cluster")
class ReplicaRebuildPathClusterTest {

    private static final long TIMEOUT_MS = 120_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final int OLD_USERS = 30;

    @Test
    @Timeout(300)
    void rebuildsFromLogStartWhenNoSourceCoversTheAppliedPosition(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig config = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final Path checkpointFile = baseDir.resolve("replica.checkpoint");

        // Run 1: a long log; the replica checkpoints at its end.
        ClusterNode node = new ClusterNode(config, CoreConfig.defaults());
        try {
            final long[] lastIdLo = {-1L};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            lastIdLo[0] = idLo;
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1))
                    .keepaliveIntervalNs(0L)
                    .build();
            try (WriteClient client = new WriteClient(clientConfig, handler)) {
                final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder(
                                baseDir.resolve("replica1").resolve("driver").toString())
                        .channels(config.archiveControlChannel())
                        .localHost("localhost")
                        .query(QueryStreams.QUERY_REQUEST_CHANNEL, QueryStreams.QUERY_REQUEST_STREAM_ID)
                        .checkpointFile(checkpointFile)
                        .checkpointIntervalMs(250L)
                        .build();
                try (ReadReplica replica = new ReadReplica(replicaConfig, CoreConfig.defaults())) {
                    submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
                    for (long uid = 1L; uid <= OLD_USERS; uid++) {
                        final long u = uid;
                        submit(client, () -> client.addUser(u));
                        submit(client, () -> client.adjustBalance(u, BASE, 1_000L));
                    }
                    drain(client, replica, () -> replica.userCount() == OLD_USERS);
                    final long position = replica.appliedPosition();
                    // Let the periodic checkpoint persist the converged position.
                    pollUntil(client, replica, () -> checkpointHolds(checkpointFile, position));
                }
            }
        } finally {
            node.close();
        }
        final long oldPosition = ReplicaCheckpoint.peek(checkpointFile).logPosition();
        assertTrue(oldPosition > 0L, "the checkpoint must store the run-1 position");

        // Run 2: a fresh single-node cluster whose recording is far shorter than
        // the run-1 checkpoint position. The node stays up: the recording is
        // active, and Aeron rejects a replay starting beyond its current end.
        node = new ClusterNode(config, CoreConfig.defaults());
        try {
            final long[] lastIdLo = {-1L};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            lastIdLo[0] = idLo;
            final ClientConfig clientConfig = ClientConfig.builder(2L, ClusterConfig.ingressEndpoints(1))
                    .keepaliveIntervalNs(0L)
                    .build();
            try (WriteClient client = new WriteClient(clientConfig, handler)) {
                long lastId = submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
                lastId = submit(client, () -> client.addUser(1L));
                lastId = submit(client, () -> client.adjustBalance(1L, BASE, 5_000L));
                lastId = submit(client, () -> client.addUser(2L));
                lastId = submit(client, () -> client.adjustBalance(2L, QUOTE, 5_000_000L));
                final long awaited = lastId;
                drain(client, () -> lastIdLo[0] == awaited);

                // Warm start from the run-1 checkpoint: the position is not
                // servable on the fresh recording, so the replica rebuilds from
                // position 0.
                final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder(
                                baseDir.resolve("replica2").resolve("driver").toString())
                        .channels(config.archiveControlChannel())
                        .localHost("localhost")
                        .query(QueryStreams.QUERY_REQUEST_CHANNEL, QueryStreams.QUERY_REQUEST_STREAM_ID)
                        .checkpointFile(checkpointFile)
                        .checkpointIntervalMs(250L)
                        .build();
                try (ReadReplica replica = new ReadReplica(replicaConfig, CoreConfig.defaults())) {
                    assertEquals(
                            OLD_USERS,
                            replica.userCount(),
                            "the checkpoint must load the run-1 state before the rebuild");
                    assertEquals(oldPosition, replica.appliedPosition(), "the checkpoint position must load");

                    drain(client, replica, () -> replica.userCount() == 2 && replica.isHealthy());

                    // The old state is fully gone and the position restarted from
                    // 0: converging below the checkpoint position proves the
                    // rebuild rather than a resume at the checkpoint boundary.
                    assertFalse(replica.userExists(3L), "run-1 users must not survive the rebuild");
                    assertFalse(replica.userExists(OLD_USERS), "run-1 users must not survive the rebuild");
                    assertTrue(
                            replica.appliedPosition() < oldPosition,
                            "the rebuilt position must restart from the log start, was "
                                    + replica.appliedPosition()
                                    + " >= checkpoint "
                                    + oldPosition);
                    assertEquals(2, replica.userCount(), "only the run-2 users must remain");
                    assertEquals(5_000L, replica.balance(1L, BASE), "run-2 user 1 balance");
                    assertEquals(5_000_000L, replica.balance(2L, QUOTE), "run-2 user 2 balance");
                }
            }
        } finally {
            node.close();
        }
    }

    private static boolean checkpointHolds(final Path file, final long position) {
        if (!Files.exists(file)) {
            return false;
        }
        try {
            // The checkpoint is written with the live appliedPosition, which can
            // land on a fragment boundary at or past the captured position; the
            // checkpoint must cover the converged state, so "at least" is the
            // contract (a stale checkpoint behind the boundary keeps the poll
            // waiting rather than masking a real gap).
            return ReplicaCheckpoint.peek(file).logPosition() >= position;
        } catch (final Exception e) {
            return false;
        }
    }

    private static void drain(final WriteClient client, final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("condition never met within timeout");
    }

    private static long submit(final WriteClient client, final LongSupplier command) {
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

    private static void pollUntil(
            final WriteClient client, final ReadReplica replica, final BooleanSupplier condition) {
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

    private static void drain(final WriteClient client, final ReadReplica replica, final BooleanSupplier condition) {
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
