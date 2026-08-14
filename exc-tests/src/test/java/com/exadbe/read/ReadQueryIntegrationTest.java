package com.exadbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.client.ExcClient;
import com.exadbe.client.ResultHandler;
import com.exadbe.client.config.ClientConfig;
import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.read.client.BalanceResult;
import com.exadbe.read.client.L2Snapshot;
import com.exadbe.read.client.MarketTradeResult;
import com.exadbe.read.client.OrderRecordResult;
import com.exadbe.read.client.OrderState;
import com.exadbe.read.client.QueryListener;
import com.exadbe.read.client.QueryTimeoutException;
import com.exadbe.read.client.ReadClient;
import com.exadbe.read.client.TotalBalanceResult;
import com.exadbe.read.client.UserReport;
import com.exadbe.read.client.config.ReadClientConfig;
import com.exadbe.read.config.ReadReplicaConfig;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration: a {@code ReadClient} (exc-read-client SDK) queries a read
 * replica's {@code QueryResponder} over the wire and receives the replicated
 * balances, L2 book, reports, order history, trade tape, and totals.
 */
@Tag("integration")
class ReadQueryIntegrationTest {

    private static final long TIMEOUT_MS = 60_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 811L;
    private static final long TAKER = 812L;

    @Test
    @Timeout(120)
    void querySdkReadsReplicatedState(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {
            submitScenario(clusterConfig, baseDir);
        }
    }

    private void submitScenario(final ClusterConfig clusterConfig, final Path baseDir) {
        final long[] lastIdLo = {Long.MIN_VALUE};
        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> lastIdLo[0] = idLo;
        final ClientConfig clientConfig =
                ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

        try (ExcClient client = new ExcClient(clientConfig, handler)) {
            await(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastIdLo);
            await(client, client.addUser(MAKER), lastIdLo);
            await(client, client.adjustBalance(MAKER, BASE, 1_000L), lastIdLo);
            await(client, client.addUser(TAKER), lastIdLo);
            await(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), lastIdLo);

            // Resting maker ask 10 @ 100; a taker bid fills 6 of it at 100.
            await(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER, 111), lastIdLo);
            await(client, client.placeGtc(SYM, 2L, false, 105L, 6L, 105L, TAKER, 222), lastIdLo);
            // Move the maker's 4-unit remainder to 101.
            await(client, client.moveOrder(SYM, 1L, 101L, MAKER), lastIdLo);
            // IOC ask against a book with no bids at or below 95: wholesale reject.
            await(client, client.placeIoc(SYM, 3L, true, 95L, 3L, TAKER, 0), lastIdLo);

            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                    baseDir.resolve("replica").resolve("driver").toString(), clusterConfig.archiveControlChannel());
            try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults());
                    QueryResponder responder = new QueryResponder(replica, replicaConfig);
                    ReadClient readClient = new ReadClient(ReadClientConfig.builder()
                            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                            .maxRetries(10)
                            .build())) {
                // The read service owns the replica and responder on one thread
                // (as ReadServiceLauncher does); the SDK client is a separate
                // connection that blocks on its own thread per query.
                final Thread serviceThread = startServiceLoop(replica, responder);
                try {
                    runQueries(replica, readClient, responder);
                } catch (final QueryTimeoutException e) {
                    throw new AssertionError(
                            "query timeout: lastOffer=" + readClient.lastOfferResult()
                                    + " received=" + responder.received()
                                    + " replies=" + responder.replies()
                                    + " dropped=" + responder.dropped(),
                            e);
                } finally {
                    stopServiceLoop(serviceThread);
                }
            }
        }
    }

    private void runQueries(final ExcReadReplica replica, final ReadClient readClient, final QueryResponder responder) {
        pollUntil(() -> replica.order(1L) != null && replica.order(2L) != null && replica.order(3L) != null);

        assertTrue(readClient.userExists(MAKER));
        assertFalse(readClient.userExists(9_999L));

        final BalanceResult makerBase = readClient.balance(MAKER, BASE);
        assertEquals(990L, makerBase.balance(), "the resting ask keeps its full reserve");
        assertTrue(makerBase.found());
        assertEquals(600L, readClient.balance(MAKER, QUOTE).balance());
        assertEquals(999_400L, readClient.balance(TAKER, QUOTE).balance());
        final BalanceResult unknown = readClient.balance(9_999L, BASE);
        assertEquals(0L, unknown.balance());
        assertFalse(unknown.found());
        assertTrue(makerBase.appliedPosition() > 0L);

        final L2Snapshot l2 = readClient.orderBook(SYM, 10);
        assertTrue(l2.found());
        assertEquals(1, l2.asks().size());
        assertEquals(101L, l2.asks().get(0).price());
        assertEquals(4L, l2.asks().get(0).size());
        assertEquals(1, l2.asks().get(0).orders());
        assertTrue(l2.bids().isEmpty());

        final UserReport report = readClient.singleUserReport(MAKER);
        assertTrue(report.exists());
        assertFalse(report.suspended());
        assertEquals(2, report.balances().size());
        assertEquals(
                990L,
                report.balances().stream()
                        .filter(b -> b.currency() == BASE)
                        .findFirst()
                        .orElseThrow()
                        .balance());
        assertEquals(1, report.orders().size());
        assertEquals(101L, report.orders().get(0).price());
        assertEquals(10L, report.orders().get(0).size());
        assertEquals(6L, report.orders().get(0).filled());

        final List<OrderRecordResult> makerHistory = readClient.orderHistory(MAKER);
        assertEquals(1, makerHistory.size());
        final OrderRecordResult makerOrder = makerHistory.get(0);
        assertEquals(OrderState.ACTIVE, makerOrder.state());
        assertEquals("GTC", makerOrder.orderType());
        assertEquals(111, makerOrder.userCookie());
        assertEquals(10L, makerOrder.size());
        assertEquals(6L, makerOrder.filled());
        assertEquals(4L, makerOrder.remaining());
        assertEquals(101L, makerOrder.price());
        assertEquals(1, makerOrder.fills().size());
        assertFalse(makerOrder.fills().get(0).taker());
        assertEquals(100L, makerOrder.fills().get(0).price());
        assertEquals(6L, makerOrder.fills().get(0).size());
        assertEquals(TAKER, makerOrder.fills().get(0).counterpartyUid());
        assertTrue(makerOrder.lastTimestamp() >= makerOrder.placedTimestamp());

        assertEquals(2, readClient.orderHistory(TAKER).size());
        assertEquals(OrderState.COMPLETED, readClient.order(2L).state());
        assertEquals(OrderState.REJECTED, readClient.order(3L).state());
        assertEquals(1, readClient.activeOrders(MAKER).size());
        assertEquals(0, readClient.activeOrders(TAKER).size());
        assertNull(readClient.order(777L));

        final List<MarketTradeResult> trades = readClient.marketTrades(SYM, 10);
        assertEquals(1, trades.size());
        assertEquals(100L, trades.get(0).price());
        assertEquals(6L, trades.get(0).size());
        assertEquals(1L, trades.get(0).makerOrderId());
        assertEquals(MAKER, trades.get(0).makerUid());
        assertEquals(TAKER, trades.get(0).takerUid());
        assertEquals(1, readClient.userTrades(MAKER, 10).size());

        final TotalBalanceResult totals = readClient.totalCurrencyBalance();
        final TotalBalanceResult.Total baseTotal = totals.totals().stream()
                .filter(t -> t.currency() == BASE)
                .findFirst()
                .orElseThrow();
        assertEquals(1_000L, baseTotal.total(), "base is conserved across the trade");
        assertEquals(4L, baseTotal.reserved(), "the resting 4 units stay reserved");
        final TotalBalanceResult.Total quoteTotal = totals.totals().stream()
                .filter(t -> t.currency() == QUOTE)
                .findFirst()
                .orElseThrow();
        assertEquals(1_000_000L, quoteTotal.total(), "quote is conserved across the trade");

        assertTrue(readClient.stateHash() != 0L);
        assertTrue(readClient.lastAppliedPosition() > 0L);

        runAsyncQueries(readClient);

        assertTrue(responder.replies() >= 23L, "every SDK query was answered");
    }

    /** Exercises the asynchronous path: submit without blocking, deliver via the listener. */
    private void runAsyncQueries(final ReadClient readClient) {
        final int[] delivered = {0};
        final long[] balanceValue = {0};
        final L2Snapshot[] l2Box = {null};
        final long[] hashValue = {0};
        final boolean[] missingOrderDelivered = {false};
        readClient.setListener(new QueryListener() {
            @Override
            public void onBalance(final long requestId, final BalanceResult result) {
                balanceValue[0] = result.balance();
                delivered[0]++;
            }

            @Override
            public void onL2(final long requestId, final L2Snapshot snapshot) {
                l2Box[0] = snapshot;
                delivered[0]++;
            }

            @Override
            public void onStateHash(final long requestId, final long stateHash) {
                hashValue[0] = stateHash;
                delivered[0]++;
            }

            @Override
            public void onOrder(final long requestId, final OrderRecordResult record) {
                if (record == null) {
                    missingOrderDelivered[0] = true;
                    delivered[0]++;
                }
            }
        });

        readClient.submitBalance(TAKER, QUOTE);
        readClient.submitOrderBook(SYM, 10);
        readClient.submitStateHash();
        readClient.submitOrderById(777L);

        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (delivered[0] < 4 && System.currentTimeMillis() < deadline) {
            readClient.poll();
            Thread.onSpinWait();
        }
        assertEquals(4, delivered[0], "all async queries were delivered through the listener");
        assertEquals(999_400L, balanceValue[0]);
        assertEquals(1, l2Box[0].asks().size());
        assertEquals(101L, l2Box[0].asks().get(0).price());
        assertTrue(hashValue[0] != 0L);
        assertTrue(missingOrderDelivered[0]);
    }

    /** Runs the replica and responder poll loop on its own thread, like the read service launcher. */
    private static Thread startServiceLoop(final ExcReadReplica replica, final QueryResponder responder) {
        final Thread thread = new Thread(() -> {
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (!Thread.currentThread().isInterrupted()) {
                final int work = replica.poll() + responder.poll();
                idle.idle(work);
            }
        });
        thread.setDaemon(true);
        thread.setName("read-query-service");
        thread.start();
        return thread;
    }

    private static void stopServiceLoop(final Thread thread) {
        thread.interrupt();
        try {
            thread.join(5_000L);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void pollUntil(final java.util.function.BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
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
