package com.exadbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.client.ExcClient;
import com.exadbe.client.ResultHandler;
import com.exadbe.client.config.ClientConfig;
import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.protocol.MatcherEventType;
import com.exadbe.read.config.ReadReplicaConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The journal recording can be replayed from a member's Archive, and a repeated
 * replay (as after a failover) delivers each committed event exactly once.
 */
@Tag("integration")
class JournalReplayIntegrationTest {

    private static final long TIMEOUT_MS = 20_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 501L;
    private static final long TAKER = 502L;

    @Test
    @Timeout(120)
    void replayDeliversTradeAndDedupsRepeatedReplay(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {
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
                await(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastIdLo);
                await(client, client.addUser(MAKER), lastIdLo);
                await(client, client.adjustBalance(MAKER, BASE, 1_000L), lastIdLo);
                await(client, client.addUser(TAKER), lastIdLo);
                await(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), lastIdLo);
                await(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER), lastIdLo);
                await(client, client.placeGtc(SYM, 2L, false, 105L, 4L, 105L, TAKER), lastIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            }

            final int[] tradeCount = {0};
            final long[] tradePrice = {-1L};
            final long[] tradeSize = {-1L};
            final JournalConsumer.Listener listener =
                    (logPos, idx, type, symbolId, makerOrderId, makerUid, takerUid, price, size, makerCompleted) -> {
                        if (type == MatcherEventType.TRADE) {
                            tradeCount[0]++;
                            tradePrice[0] = price;
                            tradeSize[0] = size;
                        }
                    };

            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                    baseDir.resolve("journal-reader").resolve("driver").toString(),
                    clusterConfig.archiveControlChannel());

            try (JournalReplayReader reader = new JournalReplayReader(replicaConfig, listener)) {
                final long length = awaitRecordingLength(reader);
                assertTrue(length > 0L, "the trade must be recorded on the journal stream");

                assertTrue(reader.startReplay(0L, length));
                awaitUntil(reader, () -> reader.unique() >= 1L);
                assertEquals(1, tradeCount[0]);
                assertEquals(100L, tradePrice[0]);
                assertEquals(4L, tradeSize[0]);

                // Replaying the same recording again re-delivers the events; dedup drops them.
                final long uniqueAfterFirst = reader.unique();
                assertTrue(reader.startReplay(0L, length));
                awaitUntil(reader, () -> reader.duplicates() >= 1L);
                assertEquals(uniqueAfterFirst, reader.unique(), "repeated replay must not re-deliver events");
                assertEquals(1, tradeCount[0]);
            }
        }
    }

    private static long awaitRecordingLength(final JournalReplayReader reader) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            final long position = reader.recordingPosition();
            if (position > 0L) {
                return position;
            }
            Thread.onSpinWait();
        }
        return -1L;
    }

    private static void awaitUntil(final JournalReplayReader reader, final java.util.function.BooleanSupplier done) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline && !done.getAsBoolean()) {
            reader.poll(16);
            Thread.onSpinWait();
        }
    }

    private static void await(final ExcClient client, final long commandIdLo, final long[] lastIdLo) {
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
