package io.justrade.write.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.CommandResultCode;
import io.justrade.protocol.OrderCommandType;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P0 exit criteria: a single-node cluster starts, the client submits N commands
 * and receives N results correlated by command id, and the echo service returns
 * a deterministic result for each.
 */
@Tag("integration")
class ClientIntegrationTest {

    private static final long TIMEOUT_MS = 15_000L;

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
    void submitsCommandsAndCorrelatesResults() {
        final long[] lastCommandIdLo = {-1L};
        final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
        final long[] lastUid = {Long.MIN_VALUE};
        final AtomicInteger results = new AtomicInteger();

        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    lastCommandIdLo[0] = idLo;
                    lastCode[0] = code;
                    if (hasUid) {
                        lastUid[0] = uid;
                    }
                    results.incrementAndGet();
                };

        final ClientConfig config =
                ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

        try (WriteClient client = new WriteClient(config, handler)) {
            final int commandCount = 8;
            for (int i = 0; i < commandCount; i++) {
                final long uid = 100L + i;
                final long id = client.submit(OrderCommandType.ADD_USER, uid);
                awaitResult(client, id, lastCommandIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
                assertEquals(uid, lastUid[0]);
            }

            assertEquals(commandCount, (int) client.completed());
            assertEquals(0, client.pendingCount());
            assertTrue(client.latencyHistogram().getTotalCount() >= commandCount, "latency samples recorded");
        }
    }

    @Test
    @Timeout(120)
    void iocBidFillsAgainstRestingAsk() {
        final int sym = 1;
        final int base = 10;
        final int quote = 20;
        final long maker = 1L;
        final long taker = 2L;

        final long[] lastCommandIdLo = {-1L};
        final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
        final long[] lastFilled = {-1L};

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

        try (WriteClient client = new WriteClient(config, handler)) {
            awaitResult(client, client.addSymbol(sym, base, quote, 1L, 1L), lastCommandIdLo);
            awaitResult(client, client.addUser(maker), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(maker, base, 1_000L), lastCommandIdLo);
            awaitResult(client, client.addUser(taker), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(taker, quote, 100_000L), lastCommandIdLo);

            awaitResult(client, client.placeGtc(sym, 1L, true, 100L, 6L, 0L, maker, 0), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

            // A bid reserves quote at its limit price; before the fix the IOC
            // path sent a null reserve and this was rejected.
            awaitResult(client, client.placeIoc(sym, 2L, false, 100L, 6L, taker, 0), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            assertEquals(6L, lastFilled[0]);
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
