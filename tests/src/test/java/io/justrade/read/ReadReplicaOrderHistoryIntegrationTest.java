package io.justrade.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.CommandResultCode;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.read.order.MarketTrade;
import io.justrade.read.order.OrderRecord;
import io.justrade.write.client.ExcClient;
import io.justrade.write.client.ResultHandler;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration: the read replica rebuilds per-user order history, per-order
 * fills, and the market trade tape from the replicated log, covering orders
 * placed by any client, and rebuilds the same history after a replica restart
 * by replaying the log from the start.
 */
@Tag("integration")
class ReadReplicaOrderHistoryIntegrationTest {

    private static final long TIMEOUT_MS = 60_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 811L;
    private static final long TAKER = 812L;

    @Test
    @Timeout(120)
    void replicaRebuildsOrderHistoryAndTrades(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {
            submitScenario(clusterConfig, baseDir);
        }
    }

    private void submitScenario(final ClusterConfig clusterConfig, final Path baseDir) {
        final long[] lastIdLo = {Long.MIN_VALUE};
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

            // Resting maker ask 10 @ 100, then a taker bid that fills 6 of it.
            await(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER, 111), lastIdLo);
            await(client, client.placeGtc(SYM, 2L, false, 105L, 6L, 105L, TAKER, 222), lastIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

            // Cancel the maker's 4-unit remainder.
            await(client, client.cancelOrder(SYM, 1L, MAKER), lastIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

            // IOC ask against an empty book: wholesale reject.
            await(client, client.placeIoc(SYM, 4L, true, 95L, 3L, TAKER, 0), lastIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                    baseDir.resolve("replica").resolve("driver").toString(), clusterConfig.archiveControlChannel());
            try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults())) {
                pollUntil(replica, () -> replica.order(1L) != null && replica.order(2L) != null);

                final OrderRecord maker = replica.order(1L);
                assertNotNull(maker, "the maker's order must be tracked");
                assertEquals(MAKER, maker.uid());
                assertTrue(maker.ask());
                assertEquals("GTC", maker.orderType());
                assertEquals(111, maker.userCookie());
                assertEquals(10L, maker.size());
                assertEquals(6L, maker.filled());
                assertEquals(4L, maker.reduced());
                assertEquals(0L, maker.remaining());
                assertEquals(OrderRecord.STATE_CANCELLED, maker.state());
                assertEquals(1, maker.fills().size());
                assertTrue(!maker.fills().get(0).taker());
                assertEquals(100L, maker.fills().get(0).price());
                assertEquals(6L, maker.fills().get(0).size());
                assertEquals(TAKER, maker.fills().get(0).counterpartyUid());

                final OrderRecord taker = replica.order(2L);
                assertNotNull(taker, "the taker's order must be tracked");
                assertTrue(!taker.ask());
                assertEquals("GTC", taker.orderType());
                assertEquals(222, taker.userCookie());
                assertEquals(6L, taker.filled());
                assertEquals(OrderRecord.STATE_COMPLETED, taker.state());
                assertEquals(1, taker.fills().size());
                assertTrue(taker.fills().get(0).taker());
                assertEquals(MAKER, taker.fills().get(0).counterpartyUid());

                final OrderRecord ioc = replica.order(4L);
                assertNotNull(ioc, "the IOC order must be tracked");
                assertEquals("IOC", ioc.orderType());
                assertEquals(0L, ioc.filled());
                assertEquals(3L, ioc.remaining());
                assertEquals(OrderRecord.STATE_REJECTED, ioc.state());

                final List<OrderRecord> makerHistory = replica.orderHistory(MAKER);
                assertEquals(1, makerHistory.size(), "the maker has exactly one tracked order");
                assertEquals(0, replica.activeOrders(MAKER).size(), "the maker has no resting orders");
                assertEquals(0, replica.activeOrders(TAKER).size(), "both taker orders are terminal");

                final List<MarketTrade> trades = replica.marketTrades(SYM, 10);
                assertEquals(1, trades.size(), "exactly one trade executed");
                assertEquals(100L, trades.get(0).price());
                assertEquals(6L, trades.get(0).size());
                assertEquals(1L, trades.get(0).makerOrderId());
                assertEquals(MAKER, trades.get(0).makerUid());
                assertEquals(TAKER, trades.get(0).takerUid());
                assertEquals(1, replica.userTrades(MAKER, 10).size());
                assertEquals(1, replica.userTrades(TAKER, 10).size());

                // A second replica reboots and replays the log from the start:
                // the history must be rebuilt identically, fills included.
                final ReadReplicaConfig secondConfig = ReadReplicaConfig.localhost(
                        baseDir.resolve("replica2").resolve("driver").toString(),
                        clusterConfig.archiveControlChannel());
                try (ExcReadReplica second = new ExcReadReplica(secondConfig, CoreConfig.defaults())) {
                    pollUntil(second, () -> second.order(1L) != null && second.order(4L) != null);
                    assertEquals(OrderRecord.STATE_CANCELLED, second.order(1L).state());
                    assertEquals(6L, second.order(1L).filled());
                    assertEquals(1, second.order(1L).fills().size());
                    assertEquals(111, second.order(1L).userCookie());
                    assertEquals(OrderRecord.STATE_REJECTED, second.order(4L).state());
                    assertEquals(1, second.marketTrades(SYM, 10).size());
                }
            }
        }
    }

    private static void pollUntil(final ExcReadReplica replica, final java.util.function.BooleanSupplier condition) {
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
