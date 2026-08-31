package io.justrade.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.QueryStreams;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.write.client.BackpressureException;
import io.justrade.write.client.ExcClient;
import io.justrade.write.client.ResultHandler;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P10 crash recovery with a stale checkpoint: the replica applies a second
 * batch after its last checkpoint and then "crashes" before the tail could be
 * persisted (modeled by restoring the pre-batch checkpoint). On restart it
 * resumes from the stale checkpoint and re-applies the lost tail from the
 * cluster log exactly once - users are not duplicated, the trade tape holds
 * every trade once, and the final state matches a replica that never crashed.
 */
@Tag("cluster")
class ReplicaCrashRecoveryClusterTest {

    private static final long TIMEOUT_MS = 120_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;

    @Test
    @Timeout(300)
    void restartFromStaleCheckpointReappliesTheLostTailExactlyOnce(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig config = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final Path checkpointFile = baseDir.resolve("replica.checkpoint");
        final Path checkpointBackup = baseDir.resolve("replica.checkpoint.bak");
        final ClusterNode node = new ClusterNode(config, CoreConfig.defaults());
        try {
            // Phase 1: batch 1 converges and is checkpointed; the checkpoint is
            // backed up before the tail below is applied.
            final long[] idLo = {-1L};
            final ResultHandler handler =
                    (idHi, lo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> idLo[0] = lo;
            try (ExcClient client = new ExcClient(
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1))
                            .keepaliveIntervalNs(0L)
                            .build(),
                    handler)) {
                final ReadReplicaConfig config1 = ReadReplicaConfig.builder(
                                baseDir.resolve("replica1").resolve("driver").toString())
                        .channels(config.archiveControlChannel())
                        .localHost("localhost")
                        .query(QueryStreams.QUERY_REQUEST_CHANNEL, QueryStreams.QUERY_REQUEST_STREAM_ID)
                        .checkpointFile(checkpointFile)
                        .checkpointIntervalMs(250L)
                        .build();
                try (ExcReadReplica replica = new ExcReadReplica(config1, CoreConfig.defaults())) {
                    submitBatch1(client, idLo, replica);
                    drain(client, replica, () -> replica.userCount() == 5 && replica.orderCount() == 1);
                    final long position = replica.appliedPosition();
                    assertTrue(position > 0L, "the replica must consume batch 1");
                    pollUntil(client, replica, () -> checkpointHolds(checkpointFile, position));
                    Files.copy(checkpointFile, checkpointBackup, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            final ReplicaCheckpoint.Position stale = ReplicaCheckpoint.peek(checkpointBackup);
            assertTrue(stale.logPosition() > 0L, "the backup must hold the batch-1 position");

            // Phase 2: batch 2 is applied but only persisted by the close
            // checkpoint, which is then rolled back to the backup - the replica
            // "crashed" with its last durable checkpoint at batch 1.
            final long[] tailIdLo = {-1L};
            final ResultHandler tailHandler =
                    (idHi, lo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> tailIdLo[0] = lo;
            try (ExcClient client = new ExcClient(
                    ClientConfig.builder(2L, ClusterConfig.ingressEndpoints(1))
                            .keepaliveIntervalNs(0L)
                            .build(),
                    tailHandler)) {
                final ReadReplicaConfig config1 = ReadReplicaConfig.builder(
                                baseDir.resolve("replica1").resolve("driver").toString())
                        .channels(config.archiveControlChannel())
                        .localHost("localhost")
                        .query(QueryStreams.QUERY_REQUEST_CHANNEL, QueryStreams.QUERY_REQUEST_STREAM_ID)
                        .checkpointFile(checkpointFile)
                        .checkpointIntervalMs(250L)
                        .build();
                try (ExcReadReplica replica = new ExcReadReplica(config1, CoreConfig.defaults())) {
                    // Warm start from the phase-1 checkpoint, then apply batch 2.
                    assertEquals(5, replica.userCount(), "the phase-1 checkpoint must load");
                    submitBatch2(client, tailIdLo, replica);
                    drain(client, replica, () -> replica.userCount() == 10 && replica.orderCount() == 2);
                }
            }
            assertTrue(
                    ReplicaCheckpoint.peek(checkpointFile).logPosition() > stale.logPosition(),
                    "the close checkpoint must persist the tail");
            Files.move(checkpointBackup, checkpointFile, StandardCopyOption.REPLACE_EXISTING);
            assertEquals(
                    stale.logPosition(), ReplicaCheckpoint.peek(checkpointFile).logPosition(), "stale restored");

            // Phase 3: restart from the stale checkpoint; the lost tail is
            // re-applied from the cluster log exactly once.
            final ReadReplicaConfig config2 = ReadReplicaConfig.builder(
                            baseDir.resolve("replica2").resolve("driver").toString())
                    .channels(config.archiveControlChannel())
                    .localHost("localhost")
                    .query(QueryStreams.QUERY_REQUEST_CHANNEL, QueryStreams.QUERY_REQUEST_STREAM_ID)
                    .checkpointFile(checkpointFile)
                    .checkpointIntervalMs(250L)
                    .build();
            try (ExcReadReplica replica = new ExcReadReplica(config2, CoreConfig.defaults())) {
                assertEquals(5, replica.userCount(), "the stale checkpoint must restore batch 1");
                assertEquals(1, replica.orderCount(), "the stale checkpoint must restore the resting order");
                assertEquals(stale.logPosition(), replica.appliedPosition(), "resume from the stale position");
                assertEquals(1, replica.orderHistory(1L).size(), "user 1's order history restored");
                assertEquals(1, replica.orderHistory(2L).size(), "user 2's order history restored");

                drain(replica, () -> replica.userCount() == 10 && replica.orderCount() == 2);
                assertTrue(
                        replica.appliedPosition() > stale.logPosition(),
                        "the replica must advance past the stale checkpoint");
                assertEquals(990L, replica.balance(1L, BASE), "maker base after the 4-unit fill");
                assertEquals(1_004L, replica.balance(2L, BASE), "taker bought 4 base units");
                assertEquals(1_000_600L, replica.balance(1L, QUOTE), "maker quote after both batches");
                assertEquals(2, replica.marketTrades(SYM, 100).size(), "both trades exactly once");
                assertEquals(10, replica.userCount(), "re-applied users must not duplicate");
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
            // backup must cover batch-1, so "at least" is the contract (a stale
            // checkpoint behind the boundary still keeps the poll waiting).
            return ReplicaCheckpoint.peek(file).logPosition() >= position;
        } catch (final Exception e) {
            return false;
        }
    }

    private static void submitBatch1(final ExcClient client, final long[] lastIdLo, final ExcReadReplica replica) {
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

    private static void drain(final ExcReadReplica replica, final BooleanSupplier condition) {
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

    private static void drain(final ExcClient client, final ExcReadReplica replica, final BooleanSupplier condition) {
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
