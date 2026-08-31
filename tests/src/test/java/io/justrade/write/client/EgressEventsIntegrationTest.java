package io.justrade.write.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.CommandResultCode;
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
 * The richer egress surface delivered end to end: one taker command sweeping
 * two makers arrives as a per-command trade group, and an ORDER_BOOK_REQUEST
 * is answered with an L2 snapshot on the same session.
 */
@Tag("integration")
class EgressEventsIntegrationTest {

    private static final long TIMEOUT_MS = 15_000L;
    private static final long CLIENT_ID = 1L;
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
    @Timeout(120)
    void takerSweepArrivesAsTradeGroupAndOrderBookRequestDeliversL2() {
        final long[] lastCommandIdLo = {-1L};
        final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
        final long[] lastFilled = {-1L};

        final AtomicInteger fills = new AtomicInteger();
        final AtomicInteger groups = new AtomicInteger();
        final long[] groupCommandIdLo = {-1L};
        final int[] groupSymbolId = {-1};
        final long[] groupTakerUid = {-1L};
        final long[] groupTotalVolume = {-1L};
        final int[] groupFillCount = {-1};
        final long[] groupMaker0 = {-1L, -1L, -1L, -1L}; // orderId, price, size, completed(1/0)
        final long[] groupMaker1 = {-1L, -1L, -1L, -1L};

        final AtomicInteger snapshots = new AtomicInteger();
        final long[] snapshotCommandIdLo = {-1L};
        final int[] snapshotSymbolId = {-1};
        final int[] askDepth = {-1};
        final long[] ask0 = {-1L, -1L, -1L}; // price, volume, orders
        final int[] bidDepth = {-1};
        final long[] bid0 = {-1L, -1L, -1L};

        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    lastCommandIdLo[0] = idLo;
                    lastCode[0] = code;
                    if (hasFilledSize) {
                        lastFilled[0] = filledSize;
                    }
                };

        final ClientConfig config = ClientConfig.builder(CLIENT_ID, ClusterConfig.ingressEndpoints(1))
                .build();

        try (WriteClient client = new WriteClient(config, handler)) {
            client.tradeListener(
                    (idHi, idLo, index, symbolId, makerOrderId, makerUid, takerUid, price, size, makerCompleted) ->
                            fills.incrementAndGet());
            client.tradeGroupListener(group -> {
                assertEquals(CLIENT_ID, group.commandIdHi());
                groups.incrementAndGet();
                groupCommandIdLo[0] = group.commandIdLo();
                groupSymbolId[0] = group.symbolId();
                groupTakerUid[0] = group.takerUid();
                groupTotalVolume[0] = group.totalVolume();
                groupFillCount[0] = group.fillCount();
                if (group.fillCount() >= 1) {
                    groupMaker0[0] = group.makerOrderId(0);
                    groupMaker0[1] = group.price(0);
                    groupMaker0[2] = group.size(0);
                    groupMaker0[3] = group.makerCompleted(0) ? 1L : 0L;
                }
                if (group.fillCount() >= 2) {
                    groupMaker1[0] = group.makerOrderId(1);
                    groupMaker1[1] = group.price(1);
                    groupMaker1[2] = group.size(1);
                    groupMaker1[3] = group.makerCompleted(1) ? 1L : 0L;
                }
            });
            client.orderBookListener(snapshot -> {
                assertEquals(CLIENT_ID, snapshot.commandIdHi());
                snapshots.incrementAndGet();
                snapshotCommandIdLo[0] = snapshot.commandIdLo();
                snapshotSymbolId[0] = snapshot.symbolId();
                askDepth[0] = snapshot.askDepth();
                if (snapshot.askDepth() >= 1) {
                    ask0[0] = snapshot.askPrice(0);
                    ask0[1] = snapshot.askVolume(0);
                    ask0[2] = snapshot.askOrders(0);
                }
                bidDepth[0] = snapshot.bidDepth();
                if (snapshot.bidDepth() >= 1) {
                    bid0[0] = snapshot.bidPrice(0);
                    bid0[1] = snapshot.bidVolume(0);
                    bid0[2] = snapshot.bidOrders(0);
                }
            });

            awaitResult(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastCommandIdLo);
            awaitResult(client, client.addUser(MAKER), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(MAKER, BASE, 1_000L), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(MAKER, QUOTE, 1_000_000L), lastCommandIdLo);
            awaitResult(client, client.addUser(TAKER), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(TAKER, BASE, 1_000L), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), lastCommandIdLo);

            // Two resting asks at distinct levels; one taker bid sweeps both.
            awaitResult(client, client.placeGtc(SYM, 10L, true, 100L, 3L, 0L, MAKER, 0), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            awaitResult(client, client.placeGtc(SYM, 11L, true, 101L, 4L, 0L, MAKER, 0), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

            final long sweepId = client.placeGtc(SYM, 12L, false, 101L, 5L, 105L, TAKER, 0);
            awaitResult(client, sweepId, lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            assertEquals(5L, lastFilled[0]);
            awaitCount(client, fills, 2);
            awaitCount(client, groups, 1);
            assertEquals(sweepId, groupCommandIdLo[0]);
            assertEquals(SYM, groupSymbolId[0]);
            assertEquals(TAKER, groupTakerUid[0]);
            assertEquals(5L, groupTotalVolume[0]);
            assertEquals(2, groupFillCount[0]);
            assertEquals(10L, groupMaker0[0]);
            assertEquals(100L, groupMaker0[1]);
            assertEquals(3L, groupMaker0[2]);
            assertEquals(1L, groupMaker0[3], "the first maker is fully filled");
            assertEquals(11L, groupMaker1[0]);
            assertEquals(101L, groupMaker1[1]);
            assertEquals(2L, groupMaker1[2]);
            assertEquals(0L, groupMaker1[3], "the second maker keeps its remainder");

            // Rest one bid, then ask for the book: the L2 frame trails the result.
            awaitResult(client, client.placeGtc(SYM, 13L, false, 99L, 7L, 99L, MAKER, 0), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

            final long l2Id = client.requestOrderBook(SYM, TAKER);
            awaitResult(client, l2Id, lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            awaitCount(client, snapshots, 1);
            assertEquals(l2Id, snapshotCommandIdLo[0]);
            assertEquals(SYM, snapshotSymbolId[0]);
            assertEquals(1, askDepth[0]);
            assertEquals(101L, ask0[0]);
            assertEquals(2L, ask0[1]);
            assertEquals(1, ask0[2]);
            assertEquals(1, bidDepth[0]);
            assertEquals(99L, bid0[0]);
            assertEquals(7L, bid0[1]);
            assertEquals(1, bid0[2]);

            // No further commands: any stale grouping state must stay silent.
            client.poll();
            assertEquals(1, groups.get());
            assertFalse(groups.get() > 1);
            assertTrue(fills.get() >= 2);
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

    private static void awaitCount(final WriteClient client, final AtomicInteger counter, final int expected) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (counter.get() >= expected) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("expected " + expected + " events but saw " + counter.get());
    }
}
