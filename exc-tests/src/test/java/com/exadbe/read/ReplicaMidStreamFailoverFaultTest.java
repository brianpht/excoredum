package com.exadbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.QueryStreams;
import com.exadbe.read.config.ReadReplicaConfig;
import com.exadbe.read.report.TotalCurrencyBalance;
import com.exadbe.write.client.BackpressureException;
import com.exadbe.write.client.ExcClient;
import com.exadbe.write.client.ResultHandler;
import com.exadbe.write.client.config.ClientConfig;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P8 mid-stream kill: the replica's source is killed while a burst of trades is
 * still being consumed, so fragments remain buffered in the replica at the
 * moment of the kill. The replica drains the buffered tail, fails over to a
 * surviving member, and resumes the replay; the market trade tape must hold
 * every trade exactly once - no gap from the kill, no duplicate from the
 * failover boundary.
 */
@Tag("fault")
class ReplicaMidStreamFailoverFaultTest {

    private static final int NODES = 3;
    private static final long TIMEOUT_MS = 120_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 1L;
    private static final long TAKER = 2L;
    private static final long ASK_SIZE = 300L;
    private static final long PRICE = 100L;
    private static final int BIDS = 150;
    private static final int TAPE_LIMIT = 10_000;

    @Test
    @Timeout(300)
    void killWhileTradesAreStreamingLosesNothing(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = new ClusterNode[NODES];
        for (int i = 0; i < NODES; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults());
        }
        final String[] channels = new String[NODES];
        for (int i = 0; i < NODES; i++) {
            channels[i] = configs[i].archiveControlChannel();
        }
        final Set<Long> acks = new HashSet<>();
        final Set<Long> fills = new HashSet<>();
        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    // A retransmitted command resolves to a DUPLICATE result with
                    // the same id, so distinct ids count each submission once.
                    acks.add(idLo);
                    if (hasFilledSize && filledSize > 0L) {
                        fills.add(idLo);
                    }
                };
        try {
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                    .keepaliveIntervalNs(0L)
                    .build();
            try (ExcClient client = new ExcClient(clientConfig, handler)) {
                final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder(
                                baseDir.resolve("replica").resolve("driver").toString())
                        .channels(channels)
                        .localHost("localhost")
                        .query(QueryStreams.QUERY_REQUEST_CHANNEL, QueryStreams.QUERY_REQUEST_STREAM_ID)
                        .livenessTimeoutMs(3_000L)
                        .failoverBackoffMs(250L)
                        .build();
                try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults())) {
                    submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
                    submit(client, () -> client.addUser(MAKER));
                    submit(client, () -> client.adjustBalance(MAKER, BASE, 1_000L));
                    submit(client, () -> client.adjustBalance(MAKER, QUOTE, 1_000_000L));
                    submit(client, () -> client.addUser(TAKER));
                    submit(client, () -> client.adjustBalance(TAKER, QUOTE, 10_000_000L));
                    submit(client, () -> client.placeGtc(SYM, 1L, true, PRICE, ASK_SIZE, 0L, MAKER, 0));
                    drain(client, replica, () -> replica.userCount() == 2 && replica.orderCount() == 1);

                    // Burst of small taker bids; the replica is not polled during
                    // the burst, so its replay subscription buffers the whole
                    // batch for the mid-stream kill below.
                    for (int i = 0; i < BIDS; i++) {
                        final long orderId = 10L + i;
                        submit(client, () -> client.placeGtc(SYM, orderId, false, PRICE, 1L, PRICE, TAKER, 0));
                        if ((i & 7) == 7) {
                            client.poll();
                        }
                    }

                    // Consume part of the burst, then kill the source while the
                    // rest is still buffered inside the replica. One poll drains
                    // at most 64 fragments, so the tape cannot reach BIDS in a
                    // single poll: the kill provably lands mid-burst.
                    drain(client, replica, () -> tapeSize(replica) >= BIDS / 3);
                    assertTrue(tapeSize(replica) < BIDS, "the kill must land mid-burst, was " + tapeSize(replica));
                    final long positionBeforeKill = replica.appliedPosition();
                    nodes[0].close();
                    nodes[0] = null;

                    // The buffered tail drains, then the replica fails over and
                    // resumes the replay on a survivor; the tape must end with
                    // exactly BIDS trades and the balances must be conserved.
                    drain(
                            client,
                            replica,
                            () -> tapeSize(replica) == BIDS
                                    && replica.userCount() == 2
                                    && replica.orderCount() == 1
                                    && replica.isHealthy()
                                    && replica.currentSource() != 0);
                    final long positionAfter = replica.appliedPosition();
                    assertTrue(
                            positionAfter >= positionBeforeKill,
                            "appliedPosition must be monotonic across the mid-stream kill, was "
                                    + positionBeforeKill
                                    + " -> "
                                    + positionAfter);
                    assertNotEquals(0, replica.currentSource(), "the replica must fail over to a survivor");
                    assertEquals(BIDS, fills.size(), "the cluster must fill every burst bid");
                    assertEquals(7 + BIDS, acks.size(), "every submission must be acknowledged");

                    // The maker's ask is reserved at placement, so balance() is the
                    // available amount: 1000 - ASK_SIZE, of which BIDS units were
                    // filled. The reserved remainder is captured by the totals.
                    assertEquals(
                            1_000L - ASK_SIZE,
                            replica.balance(MAKER, BASE),
                            "maker available base after reserving the ask");
                    assertEquals(BIDS, replica.balance(TAKER, BASE), "taker bought BIDS base units");
                    final TotalCurrencyBalance totals = replica.totalCurrencyBalance();
                    assertEquals(1_000L, totals.total(BASE), "base conserved across the burst and the failover");
                    assertEquals(11_000_000L, totals.total(QUOTE), "quote conserved across the burst and the failover");
                }
            }
        } finally {
            for (final ClusterNode node : nodes) {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    private static int tapeSize(final ExcReadReplica replica) {
        return replica.marketTrades(SYM, TAPE_LIMIT).size();
    }

    private static long submit(final ExcClient client, final LongSupplier command) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                return command.getAsLong();
            } catch (final BackpressureException e) {
                client.poll();
                Thread.onSpinWait();
            }
        }
        throw new AssertionError("could not submit within timeout");
    }

    private static void drain(final ExcClient client, final ExcReadReplica replica, final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            replica.poll();
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("replica never reached the expected state");
    }
}
