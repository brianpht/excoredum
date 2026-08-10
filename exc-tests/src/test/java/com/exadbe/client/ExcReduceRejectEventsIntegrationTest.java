package com.exadbe.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.client.config.ClientConfig;
import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.CommandResultCode;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reduce and reject events delivered end-to-end on the session egress. A
 * cancel emits a full-size reduce event, a REDUCE_ORDER a partial one, an IOC
 * with an unmatched remainder a reject event, and a FOK-BUDGET without
 * liquidity a wholesale reject.
 */
@Tag("integration")
class ExcReduceRejectEventsIntegrationTest {

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
    void cancelReduceIocAndFokEmitReduceAndRejectEvents() {
        final long[] lastCommandIdLo = {-1L};
        final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
        final long[] lastFilled = {-1L};

        final AtomicInteger reduces = new AtomicInteger();
        final AtomicLong reduceOrderId = new AtomicLong(-1L);
        final AtomicLong reduceUid = new AtomicLong(-1L);
        final AtomicLong reducedBy = new AtomicLong(-1L);
        final AtomicLong reducePrice = new AtomicLong(-1L);
        final AtomicLong reduceCommandIdLo = new AtomicLong(-1L);
        final boolean[] reduceOrderCompleted = {false};

        final AtomicInteger rejects = new AtomicInteger();
        final AtomicLong rejectOrderId = new AtomicLong(-1L);
        final AtomicLong rejectUid = new AtomicLong(-1L);
        final AtomicLong rejectedSize = new AtomicLong(-1L);
        final AtomicLong rejectPrice = new AtomicLong(-1L);
        final AtomicLong rejectCommandIdLo = new AtomicLong(-1L);

        final AtomicInteger trades = new AtomicInteger();

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

        try (ExcClient client = new ExcClient(config, handler)) {
            client.reduceListener((idHi, idLo, index, symbolId, orderId, uid, reduced, price, orderCompleted) -> {
                assertEquals(CLIENT_ID, idHi);
                assertEquals(SYM, symbolId);
                reduces.incrementAndGet();
                reduceOrderId.set(orderId);
                reduceUid.set(uid);
                reducedBy.set(reduced);
                reducePrice.set(price);
                reduceOrderCompleted[0] = orderCompleted;
                reduceCommandIdLo.set(idLo);
            });
            client.rejectListener((idHi, idLo, index, symbolId, orderId, uid, rejected, price) -> {
                assertEquals(CLIENT_ID, idHi);
                assertEquals(SYM, symbolId);
                rejects.incrementAndGet();
                rejectOrderId.set(orderId);
                rejectUid.set(uid);
                rejectedSize.set(rejected);
                rejectPrice.set(price);
                rejectCommandIdLo.set(idLo);
            });
            client.tradeListener(
                    (idHi, idLo, index, symbolId, makerOrderId, makerUid, takerUid, price, size, makerCompleted) ->
                            trades.incrementAndGet());

            awaitResult(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastCommandIdLo);
            awaitResult(client, client.addUser(MAKER), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(MAKER, BASE, 1_000L), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(MAKER, QUOTE, 1_000_000L), lastCommandIdLo);
            awaitResult(client, client.addUser(TAKER), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(TAKER, BASE, 1_000L), lastCommandIdLo);
            awaitResult(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), lastCommandIdLo);

            // Cancel of a resting order emits one reduce event for the full remainder.
            awaitResult(client, client.placeGtc(SYM, 10L, true, 100L, 10L, 0L, MAKER), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            final long cancelId = client.cancelOrder(SYM, 10L, MAKER);
            awaitResult(client, cancelId, lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            awaitCount(client, reduces, 1);
            assertEquals(10L, reduceOrderId.get());
            assertEquals(MAKER, reduceUid.get());
            assertEquals(10L, reducedBy.get());
            assertEquals(100L, reducePrice.get());
            assertTrue(reduceOrderCompleted[0], "a cancel completes the order");
            assertEquals(cancelId, reduceCommandIdLo.get());

            // REDUCE_ORDER shrinks the resting order; the remainder stays reducible.
            awaitResult(client, client.placeGtc(SYM, 11L, true, 101L, 10L, 0L, MAKER), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            final long partialReduceId = client.reduceOrder(SYM, 11L, 3L, MAKER);
            awaitResult(client, partialReduceId, lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            awaitCount(client, reduces, 2);
            assertEquals(11L, reduceOrderId.get());
            assertEquals(3L, reducedBy.get());
            assertEquals(101L, reducePrice.get());
            assertFalse(reduceOrderCompleted[0], "a partial reduce leaves the order resting");
            assertEquals(partialReduceId, reduceCommandIdLo.get());

            final long finalCancelId = client.cancelOrder(SYM, 11L, MAKER);
            awaitResult(client, finalCancelId, lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            awaitCount(client, reduces, 3);
            assertEquals(11L, reduceOrderId.get());
            assertEquals(7L, reducedBy.get());
            assertEquals(101L, reducePrice.get());
            assertTrue(reduceOrderCompleted[0], "cancelling the remainder completes the order");
            assertEquals(finalCancelId, reduceCommandIdLo.get());

            // IOC ask crossing a resting bid: the unmatched remainder is rejected.
            awaitResult(client, client.placeGtc(SYM, 12L, false, 99L, 4L, 105L, MAKER), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            final long iocId = client.placeIoc(SYM, 13L, true, 95L, 10L, TAKER);
            awaitResult(client, iocId, lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            assertEquals(4L, lastFilled[0]);
            awaitCount(client, trades, 1);
            awaitCount(client, rejects, 1);
            assertEquals(13L, rejectOrderId.get());
            assertEquals(TAKER, rejectUid.get());
            assertEquals(6L, rejectedSize.get());
            assertEquals(95L, rejectPrice.get(), "reject carries the IOC limit price");
            assertEquals(iocId, rejectCommandIdLo.get());

            // FOK-BUDGET against an empty book is rejected wholesale.
            final long fokId = client.placeFokBudget(SYM, 14L, false, 1_000L, 5L, TAKER);
            awaitResult(client, fokId, lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            assertEquals(0L, lastFilled[0]);
            awaitCount(client, rejects, 2);
            assertEquals(14L, rejectOrderId.get());
            assertEquals(TAKER, rejectUid.get());
            assertEquals(5L, rejectedSize.get());
            assertEquals(1_000L, rejectPrice.get(), "reject carries the FOK-BUDGET budget");
            assertEquals(fokId, rejectCommandIdLo.get());
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

    private static void awaitCount(final ExcClient client, final AtomicInteger counter, final int expected) {
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
