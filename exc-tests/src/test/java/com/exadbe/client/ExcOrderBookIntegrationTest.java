package com.exadbe.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.client.config.ClientConfig;
import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.CommandResultCode;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P2: order matching over a real single-node cluster. A resting GTC maker is hit
 * by a marketable taker; the taker result reports the fill and a trade event is
 * delivered on the egress.
 */
@Tag("integration")
class ExcOrderBookIntegrationTest {

    private static final long TIMEOUT_MS = 15_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 501L;
    private static final long TAKER = 502L;

    private ClusterNode node;

    @BeforeEach
    void startNode(@TempDir final Path baseDir) {
        node = new ClusterNode(ClusterConfig.singleNodeLocalhost(0, baseDir), CoreConfig.defaults());
    }

    @AfterEach
    void stopNode() {
        if (node != null) {
            node.close();
        }
    }

    @Test
    @Timeout(60)
    void restingMakerMatchedByTakerEmitsTrade() {
        final long[] lastCommandIdLo = {-1L};
        final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
        final long[] lastFilled = {-1L};
        final AtomicInteger trades = new AtomicInteger();
        final long[] tradePrice = {-1L};
        final long[] tradeSize = {-1L};

        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    lastCommandIdLo[0] = idLo;
                    lastCode[0] = code;
                    if (hasFilledSize) {
                        lastFilled[0] = filledSize;
                    }
                };

        final ClientConfig config =
                ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

        try (ExcClient client = new ExcClient(config, handler)) {
            client.tradeListener(
                    (idHi, idLo, index, symbolId, makerOrderId, makerUid, takerUid, price, size, makerCompleted) -> {
                        trades.incrementAndGet();
                        tradePrice[0] = price;
                        tradeSize[0] = size;
                    });

            awaitResult(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastCommandIdLo);
            awaitResult(client, client.addUser(MAKER), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(MAKER, BASE, 1_000L), lastCommandIdLo);
            awaitResult(client, client.addUser(TAKER), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), lastCommandIdLo);

            awaitResult(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER, 0), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

            awaitResult(client, client.placeGtc(SYM, 2L, false, 105L, 6L, 105L, TAKER, 0), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            assertEquals(6L, lastFilled[0]);

            // Drain the egress so the trade frame is observed.
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline && trades.get() == 0) {
                client.poll();
                Thread.onSpinWait();
            }
            assertEquals(1, trades.get());
            assertEquals(100L, tradePrice[0]);
            assertEquals(6L, tradeSize[0]);
            assertTrue(client.completed() >= 2);
        }
    }

    private static void awaitResult(final ExcClient client, final long commandIdLo, final long[] lastCommandIdLo) {
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
