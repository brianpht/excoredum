package io.justrade.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.MatcherEventType;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.write.client.BackpressureException;
import io.justrade.write.client.ResultHandler;
import io.justrade.write.client.WriteClient;
import io.justrade.write.client.config.ClientConfig;
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
 * Journal high availability: because every node records the committed events, the
 * domain-event journal survives losing the leader. A surviving member's Archive
 * still holds every trade from before and after the kill, delivered exactly once.
 */
@Tag("fault")
class JournalHaFailoverTest {

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

    @Test
    @Timeout(240)
    void journalSurvivesLeaderKillWithExactlyOnceDelivery(@TempDir final Path baseDir) {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = new ClusterNode[NODES];
        for (int i = 0; i < NODES; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults());
        }

        final Set<Long> results = new HashSet<>();
        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> results.add(idLo);

        try {
            final ClientConfig config = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                    .build();
            try (WriteClient client = new WriteClient(config, handler)) {
                awaitLeader(client);

                int expected = 0;
                submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
                submit(client, () -> client.addUser(MAKER));
                submit(client, () -> client.adjustBalance(MAKER, BASE, 10_000L));
                submit(client, () -> client.addUser(TAKER));
                submit(client, () -> client.adjustBalance(TAKER, QUOTE, 10_000_000L));
                submit(client, () -> client.placeGtc(SYM, 1L, true, 100L, 100L, 0L, MAKER, 0));
                expected += 6;
                drainUntil(client, results, expected);

                // Each small taker bid fully fills against the resting ask: one trade per bid.
                for (int i = 0; i < TRADES_BEFORE; i++) {
                    final long orderId = 10L + i;
                    submit(client, () -> client.placeGtc(SYM, orderId, false, 100L, 1L, 100L, TAKER, 0));
                }
                expected += TRADES_BEFORE;
                drainUntil(client, results, expected);

                final int leader = client.leaderMemberId();
                assertTrue(leader >= 0 && leader < NODES, "a leader must be known before the kill");
                nodes[leader].close();
                nodes[leader] = null;

                for (int i = 0; i < TRADES_AFTER; i++) {
                    final long orderId = 20L + i;
                    submit(client, () -> client.placeGtc(SYM, orderId, false, 100L, 1L, 100L, TAKER, 0));
                }
                expected += TRADES_AFTER;
                drainUntil(client, results, expected);

                verifyJournalOnSurvivor(baseDir, configs, nodes, leader);
            }
        } finally {
            for (final ClusterNode node : nodes) {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    private static void verifyJournalOnSurvivor(
            final Path baseDir, final ClusterConfig[] configs, final ClusterNode[] nodes, final int killed) {
        int survivor = -1;
        for (int i = 0; i < NODES; i++) {
            if (i != killed && nodes[i] != null) {
                survivor = i;
                break;
            }
        }
        assertTrue(survivor >= 0, "a survivor must remain");

        final int[] trades = {0};
        final JournalConsumer.Listener listener =
                (logPos, idx, type, symbolId, makerOrderId, makerUid, takerUid, price, size, makerCompleted) -> {
                    if (type == MatcherEventType.TRADE) {
                        trades[0]++;
                    }
                };

        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                baseDir.resolve("journal-reader").resolve("driver").toString(),
                configs[survivor].archiveControlChannel());

        final int expectedTrades = TRADES_BEFORE + TRADES_AFTER;
        try (JournalReplayReader reader = new JournalReplayReader(replicaConfig, listener)) {
            awaitRecording(reader);
            assertTrue(reader.startReplay(0L), "the survivor must expose the journal recording");
            awaitReader(reader, () -> reader.unique() >= expectedTrades);

            assertEquals(expectedTrades, reader.unique(), "no loss: every committed trade survives the leader kill");
            assertEquals(expectedTrades, trades[0]);
        }
    }

    private static void awaitRecording(final JournalReplayReader reader) {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (reader.recordingPosition() > 0L) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no journal recording on the survivor within timeout");
    }

    private static void awaitReader(final JournalReplayReader reader, final BooleanSupplier done) {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline && !done.getAsBoolean()) {
            reader.poll(32);
            Thread.onSpinWait();
        }
    }

    private static void awaitLeader(final WriteClient client) {
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

    private static void submit(final WriteClient client, final LongSupplier op) {
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

    private static void drainUntil(final WriteClient client, final Set<Long> results, final int target) {
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
