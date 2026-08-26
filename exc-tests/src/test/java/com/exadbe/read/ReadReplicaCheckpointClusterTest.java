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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P6 read-side checkpoint: the replica persists its engine + ledger + applied
 * position locally, so a warm start loads the checkpoint and resumes the log
 * from there instead of replaying the whole history. The engine and ledger are
 * correct immediately after construction (no polling required), and live
 * following continues from the checkpoint position.
 */
@Tag("cluster")
class ReadReplicaCheckpointClusterTest {

    private static final int NODES = 3;
    private static final long TIMEOUT_MS = 300_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;

    @Test
    @Timeout(600)
    void warmStartResumesFromCheckpoint(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = new ClusterNode[NODES];
        for (int i = 0; i < NODES; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults());
        }
        final String[] channels = new String[NODES];
        for (int i = 0; i < NODES; i++) {
            channels[i] = configs[i].archiveControlChannel();
        }
        final Path checkpointFile = baseDir.resolve("replica.checkpoint");

        try {
            // Phase 1: converge a checkpointed replica and let it persist state.
            final long[] lastIdLo = {-1L};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            lastIdLo[0] = idLo;
            try (ExcClient client = new ExcClient(
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                            .build(),
                    handler)) {
                final ReadReplicaConfig coldConfig = ReadReplicaConfig.builder(
                                baseDir.resolve("cold").resolve("driver").toString())
                        .channels(channels)
                        .localHost("localhost")
                        .query(QueryStreams.QUERY_REQUEST_CHANNEL, QueryStreams.QUERY_REQUEST_STREAM_ID)
                        .checkpointFile(checkpointFile)
                        .checkpointIntervalMs(250L)
                        .build();
                try (ExcReadReplica replica = new ExcReadReplica(coldConfig, CoreConfig.defaults())) {
                    submitBatch1(client, lastIdLo, replica);
                    pollUntil(client, replica, () -> replica.userCount() == 5 && replica.orderCount() == 1);
                    final long stateBefore = replica.stateHash();
                    final long positionBefore = replica.appliedPosition();
                    assertTrue(positionBefore > 0L, "the replica must consume the log before the checkpoint");
                    final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
                    while (System.currentTimeMillis() < deadline && !Files.exists(checkpointFile)) {
                        client.poll();
                        replica.poll();
                        Thread.onSpinWait();
                    }
                    assertTrue(Files.exists(checkpointFile), "a checkpoint must be written during polling");
                    // Let the cadence roll over once more so the checkpoint holds
                    // the fully converged position.
                    pollUntil(client, replica, () -> replica.appliedPosition() == positionBefore);
                }
            }
            final ReplicaCheckpoint.Position stored = ReplicaCheckpoint.peek(checkpointFile);
            assertTrue(stored.logPosition() > 0L, "the checkpoint must store the applied position");

            // Phase 2: warm start from the checkpoint; state is correct without
            // polling, and live following resumes from the stored position.
            final long[] warmIdLo = {-1L};
            final ResultHandler warmHandler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            warmIdLo[0] = idLo;
            try (ExcClient client = new ExcClient(
                    ClientConfig.builder(2L, ClusterConfig.ingressEndpoints(NODES))
                            .build(),
                    warmHandler)) {
                final ReadReplicaConfig warmConfig = ReadReplicaConfig.builder(
                                baseDir.resolve("warm").resolve("driver").toString())
                        .channels(channels)
                        .localHost("localhost")
                        .query(QueryStreams.QUERY_REQUEST_CHANNEL, QueryStreams.QUERY_REQUEST_STREAM_ID)
                        .checkpointFile(checkpointFile)
                        .build();
                try (ExcReadReplica replica = new ExcReadReplica(warmConfig, CoreConfig.defaults())) {
                    assertEquals(5, replica.userCount(), "the checkpoint must restore the engine users");
                    assertEquals(1, replica.orderCount(), "the checkpoint must restore the resting orders");
                    assertEquals(
                            stored.logPosition(),
                            replica.appliedPosition(),
                            "the replica must resume from the checkpoint position");
                    assertEquals(
                            2,
                            replica.orderHistory(1L).size()
                                    + replica.orderHistory(2L).size(),
                            "the checkpoint must restore the ledger");
                    assertEquals(1, replica.orderHistory(1L).size(), "the checkpoint must restore user 1's history");
                    assertEquals(1, replica.orderHistory(2L).size(), "the checkpoint must restore user 2's history");

                    submitBatch2(client, warmIdLo, replica);
                    pollUntil(client, replica, () -> replica.userCount() == 10 && replica.orderCount() == 2);
                    assertTrue(
                            replica.appliedPosition() > stored.logPosition(),
                            "the warm-started replica must keep following the log");
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
