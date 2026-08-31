package io.justrade.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.protocol.CommandResultCode;
import io.justrade.write.client.ResultHandler;
import io.justrade.write.client.WriteClient;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end domain-event journal: a committed trade flows engine -> off-heap ring
 * -> journaler agent -> recorded journal stream on the leader.
 */
@Tag("integration")
class JournalClusterIntegrationTest {

    private static final long TIMEOUT_MS = 15_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 501L;
    private static final long TAKER = 502L;

    @Test
    @Timeout(60)
    void committedTradeIsRecordedOnJournalStream(@TempDir final Path baseDir) {
        try (ClusterNode node = new ClusterNode(ClusterConfig.singleNodeLocalhost(0, baseDir), CoreConfig.defaults())) {
            final long[] lastCommandIdLo = {-1L};
            final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                        lastCommandIdLo[0] = idLo;
                        lastCode[0] = code;
                    };

            final ClientConfig config =
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

            try (WriteClient client = new WriteClient(config, handler)) {
                awaitResult(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastCommandIdLo);
                awaitResult(client, client.addUser(MAKER), lastCommandIdLo);
                awaitResult(client, client.adjustBalance(MAKER, BASE, 1_000L), lastCommandIdLo);
                awaitResult(client, client.addUser(TAKER), lastCommandIdLo);
                awaitResult(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), lastCommandIdLo);

                // Resting maker produces no domain event; the marketable taker produces a trade.
                awaitResult(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER, 0), lastCommandIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
                assertEquals(0L, node.journalPublished());

                awaitResult(client, client.placeGtc(SYM, 2L, false, 105L, 6L, 105L, TAKER, 0), lastCommandIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

                final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
                while (System.currentTimeMillis() < deadline && node.journalPublished() == 0L) {
                    client.poll();
                    Thread.onSpinWait();
                }
                assertTrue(node.journalPublished() > 0L, "the trade must reach the recorded journal stream");
            }
        }
    }

    private static void awaitResult(final WriteClient client, final long commandIdLo, final long[] lastCommandIdLo) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastCommandIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no result for commandIdLo=" + commandIdLo);
    }
}
