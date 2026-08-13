package com.exadbe.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.client.config.ClientConfig;
import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.CommandResultCode;
import io.aeron.cluster.ClusterTool;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P4 warm restart: a node snapshots its state, is stopped, and is restarted with
 * {@code cleanStart=false}. Recovery loads the snapshot (the pre-snapshot log is
 * not replayed), so a maker resting before the snapshot must still match a taker
 * submitted only after the restart.
 */
@Tag("cluster")
class SnapshotWarmRestartIntegrationTest {

    private static final long TIMEOUT_MS = 20_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 701L;
    private static final long TAKER = 702L;

    @Test
    @Timeout(120)
    void restartRecoversRestingOrderFromSnapshot(@TempDir final Path baseDir) {
        final ClusterConfig config = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final CoreConfig coreConfig = CoreConfig.defaults();

        // Phase 1: populate state and take a snapshot, then stop the node.
        ClusterNode node = new ClusterNode(config, coreConfig);
        try {
            final long[] lastIdLo = {-1L};
            final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                        lastIdLo[0] = idLo;
                        lastCode[0] = code;
                    };
            final ClientConfig clientConfig =
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();
            try (ExcClient client = new ExcClient(clientConfig, handler)) {
                awaitResult(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastIdLo);
                awaitResult(client, client.addUser(MAKER), lastIdLo);
                awaitResult(client, client.adjustBalance(MAKER, BASE, 1_000L), lastIdLo);
                awaitResult(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER, 0), lastIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            }

            assertTrue(
                    ClusterTool.snapshot(config.clusterDir(), System.out),
                    "snapshot request must be accepted by the consensus module");
            awaitSnapshotTaken(node);
        } finally {
            node.close();
        }

        // Phase 2: warm restart; state must come from the snapshot alone.
        node = new ClusterNode(config, coreConfig, false);
        try {
            final long[] lastIdLo = {-1L};
            final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
            final long[] lastFilled = {-1L};
            final int[] trades = {0};
            final long[] tradePrice = {-1L};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                        lastIdLo[0] = idLo;
                        lastCode[0] = code;
                        if (hasFilledSize) {
                            lastFilled[0] = filledSize;
                        }
                    };
            final ClientConfig clientConfig =
                    ClientConfig.builder(2L, ClusterConfig.ingressEndpoints(1)).build();
            try (ExcClient client = new ExcClient(clientConfig, handler)) {
                client.tradeListener(
                        (idHi,
                                idLo,
                                index,
                                symbolId,
                                makerOrderId,
                                makerUid,
                                takerUid,
                                price,
                                size,
                                makerCompleted) -> {
                            trades[0]++;
                            tradePrice[0] = price;
                        });

                awaitResult(client, client.addUser(TAKER), lastIdLo);
                awaitResult(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), lastIdLo);

                // Bid crosses the snapshot-recovered resting ask at 100.
                awaitResult(client, client.placeGtc(SYM, 2L, false, 105L, 6L, 105L, TAKER, 0), lastIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
                assertEquals(6L, lastFilled[0], "taker must fill against the recovered maker");

                final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
                while (System.currentTimeMillis() < deadline && trades[0] == 0) {
                    client.poll();
                    Thread.onSpinWait();
                }
                assertEquals(1, trades[0]);
                assertEquals(100L, tradePrice[0]);
            }
            assertTrue(node.metrics().snapshotsLoaded() >= 1, "node must have recovered from a snapshot");
        } finally {
            node.close();
        }
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

    private static void awaitResult(final ExcClient client, final long commandIdLo, final long[] lastIdLo) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no result for commandIdLo=" + commandIdLo);
    }
}
