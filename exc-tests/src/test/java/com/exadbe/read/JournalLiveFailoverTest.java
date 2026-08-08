package com.exadbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.exadbe.client.BackpressureException;
import com.exadbe.client.ExcClient;
import com.exadbe.client.ResultHandler;
import com.exadbe.client.config.ClientConfig;
import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.MatcherEventType;
import io.aeron.archive.client.AeronArchive;
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
 * Live multi-archive failover: a consumer following one member's journal fails
 * over to another member when its source dies, and still delivers every trade
 * exactly once across the switch.
 */
@Tag("fault")
class JournalLiveFailoverTest {

    private static final int NODES = 3;
    private static final long CONNECT_TIMEOUT_MS = 30_000L;
    private static final long DRAIN_TIMEOUT_MS = 90_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 1L;
    private static final long TAKER = 2L;
    private static final int TRADES_BEFORE = 3;
    private static final int TRADES_AFTER = 3;
    private static final int SOURCE = 0;

    @Test
    @Timeout(240)
    void followerFailsOverToAnotherMemberWithoutLoss(@TempDir final Path baseDir) {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = new ClusterNode[NODES];
        for (int i = 0; i < NODES; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults());
        }

        final Set<Long> results = new HashSet<>();
        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> results.add(idLo);

        final int[] trades = {0};
        final JournalConsumer.Listener listener =
                (logPos, idx, type, symbolId, makerOrderId, makerUid, takerUid, price, size, makerCompleted) -> {
                    if (type == MatcherEventType.TRADE) {
                        trades[0]++;
                    }
                };

        final String[] controlChannels = new String[NODES];
        for (int i = 0; i < NODES; i++) {
            controlChannels[i] = configs[i].archiveControlChannel();
        }

        HaJournalConsumer consumer = null;
        try {
            final ClientConfig config = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                    .build();
            try (ExcClient client = new ExcClient(config, handler)) {
                awaitLeader(client);

                int expected = 0;
                submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
                submit(client, () -> client.addUser(MAKER));
                submit(client, () -> client.adjustBalance(MAKER, BASE, 10_000L));
                submit(client, () -> client.addUser(TAKER));
                submit(client, () -> client.adjustBalance(TAKER, QUOTE, 10_000_000L));
                submit(client, () -> client.placeGtc(SYM, 1L, true, 100L, 100L, 0L, MAKER));
                expected += 6;
                drainUntil(client, results, expected);

                for (int i = 0; i < TRADES_BEFORE; i++) {
                    final long orderId = 10L + i;
                    submit(client, () -> client.placeGtc(SYM, orderId, false, 100L, 1L, 100L, TAKER));
                }
                expected += TRADES_BEFORE;
                drainUntil(client, results, expected);

                consumer = new HaJournalConsumer(
                        baseDir.resolve("ha-reader").resolve("driver").toString(),
                        "localhost",
                        AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT,
                        controlChannels,
                        listener);

                final HaJournalConsumer following = consumer;
                pump(
                        client,
                        following,
                        () -> following.currentSource() == SOURCE && following.unique() >= TRADES_BEFORE);
                assertEquals(SOURCE, consumer.currentSource(), "consumer must follow the intended source first");

                // Kill the member the consumer is following (also drives Raft failover if it led).
                nodes[SOURCE].close();
                nodes[SOURCE] = null;

                for (int i = 0; i < TRADES_AFTER; i++) {
                    final long orderId = 20L + i;
                    submit(client, () -> client.placeGtc(SYM, orderId, false, 100L, 1L, 100L, TAKER));
                }
                expected += TRADES_AFTER;
                drainUntil(client, results, expected);

                final int total = TRADES_BEFORE + TRADES_AFTER;
                final HaJournalConsumer followingAfter = consumer;
                pump(client, followingAfter, () -> followingAfter.unique() >= total);

                assertEquals(total, consumer.unique(), "no loss across the source failover");
                assertEquals(total, trades[0]);
                assertNotEquals(SOURCE, consumer.currentSource(), "consumer must have failed over to a survivor");
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            for (final ClusterNode node : nodes) {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    private static void pump(final ExcClient client, final HaJournalConsumer consumer, final BooleanSupplier done) {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline && !done.getAsBoolean()) {
            client.poll();
            consumer.poll(32);
            Thread.onSpinWait();
        }
    }

    private static void awaitLeader(final ExcClient client) {
        final long deadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (client.leaderMemberId() >= 0) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no leader established within timeout");
    }

    private static void submit(final ExcClient client, final LongSupplier op) {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                op.getAsLong();
                return;
            } catch (final BackpressureException e) {
                client.poll();
                Thread.onSpinWait();
            }
        }
        throw new AssertionError("could not submit command within timeout");
    }

    private static void drainUntil(final ExcClient client, final Set<Long> results, final int target) {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (results.size() >= target) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("only " + results.size() + " of " + target + " results within timeout");
    }
}
