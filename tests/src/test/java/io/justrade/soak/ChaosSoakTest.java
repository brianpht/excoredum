package io.justrade.soak;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.CommandResultCode;
import io.justrade.write.client.ResultHandler;
import io.justrade.write.client.WriteClient;
import io.justrade.write.client.config.ClientConfig;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Long-running steady-state load against a single-node cluster, mixing the
 * command types the production workload touches: full and partial matching
 * (GTC maker bid hit by marketable IOC taker ask), order lifecycle (cancel,
 * reduce), and balance adjustment (credit and debit) with non-zero maker/taker
 * fees. Verifies that every command completes, no result is a rejection, and
 * end-to-end tail latency stays within budget while observing JVM
 * garbage-collection activity.
 *
 * <p>The engine hot path is allocation-free (asserted separately by JMH
 * {@code -prof gc}); this soak exercises the full client/cluster path under
 * sustained mixed load. Tagged {@code soak}: run via the opt-in
 * {@code soakTest} task, never wired into {@code check}. Scale via
 * {@code -Djustrade.soak.warmupRounds} / {@code -Djustrade.soak.steadyRounds}.
 */
@Tag("soak")
class ChaosSoakTest {

    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 501L;
    private static final long TAKER = 502L;
    private static final long PRICE = 100L;
    private static final long SIZE = 10L;
    private static final long PARTIAL_FILL_SIZE = 4L;
    private static final long TAKER_FEE = 2L;
    private static final long MAKER_FEE = 1L;
    private static final long ADJUST_CREDIT = 100L;
    private static final long ADJUST_DEBIT = 10L;
    private static final long MAKER_QUOTE_FUNDING = 10_000_000_000L;
    private static final long TAKER_BASE_FUNDING = 1_000_000_000L;

    private static final int WARMUP_ROUNDS = Integer.getInteger("justrade.soak.warmupRounds", 15_000);
    private static final int STEADY_ROUNDS = Integer.getInteger("justrade.soak.steadyRounds", 120_000);

    private static final long P99_9_BUDGET_NS = TimeUnit.MILLISECONDS.toNanos(50);

    /**
     * Upper bound on GC collections during the steady window. The deterministic
     * core hot path is allocation-free (asserted by JMH {@code -prof gc}); this
     * full client/cluster path does allocate at the edge, so the soak asserts GC
     * stays bounded rather than literally zero - a gross breach signals an
     * allocation leak.
     */
    private static final long MAX_GC_COLLECTIONS = 1_000L;

    private static final long RESULT_TIMEOUT_MS = 15_000L;

    @Test
    @Timeout(600)
    @SuppressWarnings("try")
    void sustainedMixedLoadCompletesWithinTailLatencyBudget(@TempDir final Path baseDir) {
        final ClusterConfig config = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final long[] lastCommandIdLo = {-1L};
        final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
        final AtomicLong completed = new AtomicLong();
        final AtomicLong failures = new AtomicLong();
        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    completed.incrementAndGet();
                    if (code != CommandResultCode.SUCCESS) {
                        failures.incrementAndGet();
                    }
                    lastCommandIdLo[0] = idLo;
                    lastCode[0] = code;
                };

        try (ClusterNode node = new ClusterNode(config, CoreConfig.defaults())) {
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1))
                    .maxInFlight(1024)
                    .build();

            try (WriteClient client = new WriteClient(clientConfig, handler)) {
                awaitLeader(client);
                final long setupSubmitted = setup(client, lastCommandIdLo, lastCode);

                // Warm up the JIT and the transport before measuring.
                final long warmupSubmitted = drive(client, lastCommandIdLo, lastCode, WARMUP_ROUNDS);
                assertEquals(
                        setupSubmitted + warmupSubmitted,
                        completed.get(),
                        "warmup: every submitted command must complete");

                final long gcBefore = gcCollectionCount();
                client.latencyHistogram().reset();

                final long steadySubmitted = drive(client, lastCommandIdLo, lastCode, STEADY_ROUNDS);
                final long gcDelta = gcCollectionCount() - gcBefore;
                final Histogram histogram = client.latencyHistogram();

                assertEquals(
                        setupSubmitted + warmupSubmitted + steadySubmitted,
                        completed.get(),
                        "every submitted command must complete");
                assertEquals(0, failures.get(), "no submitted command may be rejected");
                assertTrue(histogram.getTotalCount() >= steadySubmitted, "latency samples recorded for steady window");

                final long p99 = histogram.getValueAtPercentile(99.0);
                final long p999 = histogram.getValueAtPercentile(99.9);
                final long max = histogram.getMaxValue();
                System.out.printf(
                        "soak: commands=%d gcCollections=%d p99=%dus p99.9=%dus max=%dus%n",
                        steadySubmitted,
                        gcDelta,
                        TimeUnit.NANOSECONDS.toMicros(p99),
                        TimeUnit.NANOSECONDS.toMicros(p999),
                        TimeUnit.NANOSECONDS.toMicros(max));

                assertTrue(
                        p999 <= P99_9_BUDGET_NS,
                        "p99.9 latency " + TimeUnit.NANOSECONDS.toMicros(p999) + "us exceeded budget");
                assertTrue(
                        gcDelta <= MAX_GC_COLLECTIONS,
                        "GC collections " + gcDelta + " exceeded bound " + MAX_GC_COLLECTIONS
                                + " (possible allocation leak)");
            }
        }
    }

    /**
     * Registers a symbol with non-zero maker/taker fees, both users, and their
     * funding; returns the number of commands submitted.
     */
    private static long setup(
            final WriteClient client, final long[] lastCommandIdLo, final CommandResultCode[] lastCode) {
        long submitted = 0;
        awaitResult(
                client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L, TAKER_FEE, MAKER_FEE), lastCommandIdLo, lastCode);
        assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
        submitted++;
        awaitResult(client, client.addUser(MAKER), lastCommandIdLo, lastCode);
        assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
        submitted++;
        // The maker rests GTC bids, so it needs quote currency for the hold and fees.
        awaitResult(client, client.adjustBalance(MAKER, QUOTE, MAKER_QUOTE_FUNDING), lastCommandIdLo, lastCode);
        assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
        submitted++;
        awaitResult(client, client.addUser(TAKER), lastCommandIdLo, lastCode);
        assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
        submitted++;
        // The taker hits with marketable IOC asks, so it needs base currency.
        awaitResult(client, client.adjustBalance(TAKER, BASE, TAKER_BASE_FUNDING), lastCommandIdLo, lastCode);
        assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
        submitted++;
        return submitted;
    }

    /**
     * Drives {@code rounds} iterations of an 8-step workload pattern (13
     * commands per iteration):
     *
     * <ul>
     *   <li>steps 0-2: rest a fresh GTC maker bid and hit it with a marketable
     *       IOC taker ask that fills it completely;
     *   <li>step 3: rest a maker bid and credit the maker's quote balance;
     *   <li>step 4: partially fill the resting maker bid ({@link #PARTIAL_FILL_SIZE}
     *       of {@link #SIZE} lots);
     *   <li>step 5: reduce the maker bid's remaining lots, completing it;
     *   <li>step 6: rest a maker bid;
     *   <li>step 7: cancel the resting maker bid and debit the taker's base
     *       balance.
     * </ul>
     *
     * <p>Every step waits for the previous command's result, so the order book
     * is empty at the start of each iteration and the pattern is deterministic.
     *
     * @return number of commands submitted
     */
    private static long drive(
            final WriteClient client,
            final long[] lastCommandIdLo,
            final CommandResultCode[] lastCode,
            final int rounds) {
        long submitted = 0;
        long makerOrderId = 1L;
        long takerOrderId = 2L;
        long restingMaker = -1L;
        for (int round = 0; round < rounds; round++) {
            switch (round % 8) {
                case 0, 1, 2 -> {
                    awaitResult(
                            client,
                            client.placeGtc(SYM, makerOrderId, false, PRICE, SIZE, PRICE, MAKER, 0),
                            lastCommandIdLo,
                            lastCode);
                    if (round == 0) {
                        assertEquals(CommandResultCode.SUCCESS, lastCode[0], "first maker must rest");
                    }
                    submitted++;
                    awaitResult(
                            client,
                            client.placeIoc(SYM, takerOrderId, true, PRICE - 1, SIZE, TAKER, 0),
                            lastCommandIdLo,
                            lastCode);
                    submitted++;
                    makerOrderId += 2;
                    takerOrderId += 2;
                }
                case 3 -> {
                    awaitResult(
                            client,
                            client.placeGtc(SYM, makerOrderId, false, PRICE, SIZE, PRICE, MAKER, 0),
                            lastCommandIdLo,
                            lastCode);
                    submitted++;
                    restingMaker = makerOrderId;
                    makerOrderId += 2;
                    awaitResult(client, client.adjustBalance(MAKER, QUOTE, ADJUST_CREDIT), lastCommandIdLo, lastCode);
                    submitted++;
                }
                case 4 -> {
                    awaitResult(
                            client,
                            client.placeIoc(SYM, takerOrderId, true, PRICE - 1, PARTIAL_FILL_SIZE, TAKER, 0),
                            lastCommandIdLo,
                            lastCode);
                    submitted++;
                    takerOrderId += 2;
                }
                case 5 -> {
                    awaitResult(
                            client,
                            client.reduceOrder(SYM, restingMaker, SIZE - PARTIAL_FILL_SIZE, MAKER),
                            lastCommandIdLo,
                            lastCode);
                    submitted++;
                    restingMaker = -1L;
                }
                case 6 -> {
                    awaitResult(
                            client,
                            client.placeGtc(SYM, makerOrderId, false, PRICE, SIZE, PRICE, MAKER, 0),
                            lastCommandIdLo,
                            lastCode);
                    submitted++;
                    restingMaker = makerOrderId;
                    makerOrderId += 2;
                }
                default -> {
                    awaitResult(client, client.cancelOrder(SYM, restingMaker, MAKER), lastCommandIdLo, lastCode);
                    submitted++;
                    restingMaker = -1L;
                    awaitResult(client, client.adjustBalance(TAKER, BASE, -ADJUST_DEBIT), lastCommandIdLo, lastCode);
                    submitted++;
                }
            }
        }
        return submitted;
    }

    private static void awaitResult(
            final WriteClient client,
            final long commandIdLo,
            final long[] lastCommandIdLo,
            final CommandResultCode[] lastCode) {
        final long deadline = System.currentTimeMillis() + RESULT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastCommandIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no result for commandIdLo=" + commandIdLo);
    }

    private static void awaitLeader(final WriteClient client) {
        final long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (client.leaderMemberId() >= 0) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no leader established within timeout");
    }

    /** Total GC collections across all collectors (young and old) since JVM start. */
    private static long gcCollectionCount() {
        long total = 0L;
        for (final GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            final long count = bean.getCollectionCount();
            if (count > 0) {
                total += count;
            }
        }
        return total;
    }
}
