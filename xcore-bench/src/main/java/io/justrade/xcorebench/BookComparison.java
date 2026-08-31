package io.justrade.xcorebench;

import exchange.core2.collections.objpool.ObjectsPool;
import exchange.core2.core.common.config.LoggingConfiguration;
import exchange.core2.core.orderbook.OrderBookDirectImpl;
import exchange.core2.core.orderbook.OrderBookEventsHelper;
import exchange.core2.core.orderbook.OrderBookNaiveImpl;
import java.util.ArrayList;
import java.util.List;

/**
 * Matching-level comparison: replays one generated workload through justrade's
 * order book and both exchange-core implementations, cross-validates the final
 * states, and reports replay throughput. Mirrors exchange-core's ITOrderBookBase
 * shape (fresh book per iteration, whole fill + benchmark replay).
 */
public final class BookComparison {

    public static final int SYMBOL_ID = 1;

    private BookComparison() {}

    /** Runs the comparison; throws if any implementation diverges from the reference. */
    public static String run(final BookComparisonConfig config) {
        final Workload workload = WorkloadGenerator.generate(
                config.benchmarkCommands(),
                config.targetOrders(),
                config.numUsers(),
                SYMBOL_ID,
                config.slidingPrice(),
                config.avalancheIoc(),
                config.seed());

        final List<XcoreBookRunner.BookFactory> factories = new ArrayList<>();
        factories.add(new XcoreBookRunner.BookFactory() {
            @Override
            public exchange.core2.core.orderbook.IOrderBook create(final int symbolId) {
                return new OrderBookNaiveImpl(WorkloadGenerator.spotSymbol(symbolId), LoggingConfiguration.DEFAULT);
            }

            @Override
            public String name() {
                return "xcore OrderBookNaiveImpl";
            }
        });
        factories.add(new XcoreBookRunner.BookFactory() {
            @Override
            public exchange.core2.core.orderbook.IOrderBook create(final int symbolId) {
                return new OrderBookDirectImpl(
                        WorkloadGenerator.spotSymbol(symbolId),
                        ObjectsPool.createDefaultTestPool(),
                        OrderBookEventsHelper.NON_POOLED_EVENTS_HELPER,
                        LoggingConfiguration.DEFAULT);
            }

            @Override
            public String name() {
                return "xcore OrderBookDirectImpl";
            }
        });

        final ComparisonReport report = new ComparisonReport().heading("Matching-level order-book comparison");
        report.note(String.format(
                "workload: fill=%d benchmark=%d users=%d seed=%d",
                workload.fillCount(), workload.count() - workload.fillCount(), config.numUsers(), config.seed()));
        report.note(String.format(
                "mix: GTC-place=%d instant=%d cancel=%d move=%d reduce=%d",
                workload.counterPlaceLimit(),
                workload.counterPlaceMarket(),
                workload.counterCancel(),
                workload.counterMove(),
                workload.counterReduce()));

        // Reference: justrade's book.
        BookStats reference = null;
        final List<List<String>> rows = new ArrayList<>();
        long[] justradeTimes = new long[config.iterations()];
        for (int i = 0; i < config.iterations(); i++) {
            reference = JustradeBookRunner.replay(workload, SYMBOL_ID);
            justradeTimes[i] = reference.replayNanos();
        }
        rows.add(row(reference.name(), workload.count(), justradeTimes));

        for (final XcoreBookRunner.BookFactory factory : factories) {
            BookStats stats = null;
            final long[] times = new long[config.iterations()];
            for (int i = 0; i < config.iterations(); i++) {
                stats = XcoreBookRunner.replay(workload, SYMBOL_ID, factory);
                times[i] = stats.replayNanos();
            }
            rows.add(row(stats.name(), workload.count(), times));

            final List<String> diff = BookStats.diff(reference, stats);
            if (!diff.isEmpty()) {
                report.mismatches(factory.name(), diff);
                throw new IllegalStateException("cross-validation failed for " + factory.name() + ": " + diff);
            }
        }

        report.table(List.of("impl", "commands", "avg MT/s", "best MT/s", "avg us/cmd"), rows);
        report.note("cross-validation: PASS (event counters and full-depth L2 identical to core reference)");
        return report.render();
    }

    private static List<String> row(final String name, final int commands, final long[] times) {
        long sum = 0;
        long best = Long.MAX_VALUE;
        for (final long t : times) {
            sum += t;
            best = Math.min(best, t);
        }
        final long avg = sum / times.length;
        return List.of(
                name,
                String.valueOf(commands),
                ComparisonReport.mts(commands, avg),
                ComparisonReport.mts(commands, best),
                ComparisonReport.micros(avg / commands));
    }

    /** Knobs for {@link #run}. */
    public record BookComparisonConfig(
            int benchmarkCommands,
            int targetOrders,
            int numUsers,
            int iterations,
            int seed,
            boolean slidingPrice,
            boolean avalancheIoc) {

        public static BookComparisonConfig defaults() {
            return new BookComparisonConfig(100_000, 1_000, 1_000, 3, 1, false, false);
        }
    }
}
