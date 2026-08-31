package io.justrade.write.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.CommandResultCode;
import io.justrade.read.ExcReadReplica;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P5 CQRS read side: a non-voting replica follows the cluster's committed log
 * over its archive and reproduces engine state (users, balances, resting depth)
 * without joining consensus.
 */
@Tag("integration")
class ReadReplicaIntegrationTest {

    private static final long TIMEOUT_MS = 60_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 801L;
    private static final long TAKER = 802L;

    @Test
    @Timeout(120)
    void replicaFollowsClusterLog(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {
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
                await(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER, 0), lastIdLo);
                await(client, client.placeGtc(SYM, 2L, false, 105L, 4L, 105L, TAKER, 0), lastIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            }

            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                    baseDir.resolve("replica").resolve("driver").toString(), clusterConfig.archiveControlChannel());

            try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults())) {
                final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
                while (System.currentTimeMillis() < deadline
                        && !(replica.userExists(TAKER) && replica.balance(TAKER, BASE) == 4L)) {
                    replica.poll();
                    Thread.onSpinWait();
                }

                assertTrue(replica.userExists(MAKER), "replica must replicate the maker account");
                assertTrue(replica.userExists(TAKER), "replica must replicate the taker account");
                assertEquals(2, replica.userCount());
                assertEquals(1, replica.symbolCount());
                assertEquals(1, replica.orderCount(), "the maker remainder must rest on the replica book");
                assertEquals(4L, replica.balance(TAKER, BASE), "taker bought 4 base units");
                assertEquals(400L, replica.balance(MAKER, QUOTE), "maker sold 4 base at price 100");

                final io.justrade.engine.orderbook.L2View l2 = new io.justrade.engine.orderbook.L2View(32);
                assertTrue(replica.orderBook(SYM, l2), "replica serves L2 for the replicated symbol");
                assertEquals(1, l2.askDepth(), "the maker remainder rests as one ask level");
                assertEquals(0, l2.bidDepth());
                assertEquals(100L, l2.askPrice(0));
                assertEquals(6L, l2.askVolume(0));
                assertFalse(replica.orderBook(999, l2), "unknown symbol returns false");
                assertTrue(replica.isHealthy());
                assertTrue(replica.appliedPosition() > 0L);

                final io.justrade.read.report.SingleUserReport makerReport = replica.singleUserReport(MAKER);
                assertTrue(makerReport.exists(), "the maker report must reflect the replicated account");
                assertFalse(makerReport.suspended(), "the maker is active");
                assertEquals(400L, makerReport.balance(QUOTE), "maker proceeds from the 4-unit sale");
                assertEquals(1, makerReport.orders().size(), "the maker's ask remainder still rests");
                final io.justrade.read.report.SingleUserReport.OrderLine makerOrder =
                        makerReport.orders().get(0);
                assertTrue(makerOrder.ask());
                assertEquals(10L, makerOrder.size());
                assertEquals(4L, makerOrder.filled(), "4 of the 10-unit ask filled");

                final io.justrade.read.report.TotalCurrencyBalance totals = replica.totalCurrencyBalance();
                assertEquals(1000L, totals.total(BASE), "base conserved across balances and the ask hold");
                assertEquals(1_000_000L, totals.total(QUOTE), "quote conserved across balances and holds");
                assertEquals(
                        totals.total(QUOTE),
                        totals.accountBalances(QUOTE) + totals.fees(QUOTE) + totals.ordersBalances(QUOTE),
                        "quote total is the sum of its breakdown");

                final long hash = replica.stateHash();
                assertNotEquals(0L, hash, "a populated replica has a non-trivial state hash");
                assertEquals(hash, replica.stateHash(), "state hash is stable on repeated reads");
            }
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
