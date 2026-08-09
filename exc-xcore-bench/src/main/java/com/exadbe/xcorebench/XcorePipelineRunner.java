package com.exadbe.xcorebench;

import com.exadbe.bench.LatencyResult;
import exchange.core2.core.ExchangeApi;
import exchange.core2.core.ExchangeCore;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.ApiAddUser;
import exchange.core2.core.common.api.ApiAdjustUserBalance;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.config.ExchangeConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/**
 * Closed-loop command latency through exchange-core's full pipeline: disruptor
 * publish, grouping, risk engine, matching engine, risk release, and result
 * completion - the closest single-node equivalent of excoredum's engine path
 * (no replication, no consensus, no durable journal on either side here).
 *
 * <p>Runs the latency-tuned configuration (2K ring, 1 ME + 1 RE, busy-spin) but
 * replaces exchange-core's affinity thread factory with plain threads: OpenHFT
 * affinity 3.x predates JDK 21 and pinning is not needed for a closed-loop
 * comparison on shared hardware.
 *
 * <p>Each measured op allocates an {@code ApiPlaceOrder} via its builder and a
 * completion future; excoredum's engine path reuses flyweights. This asymmetry
 * is inherent to the two APIs and is documented in the methodology.
 */
public final class XcorePipelineRunner {

    private static final int SYMBOL = BookComparison.SYMBOL_ID;
    // Must match WorkloadGenerator.spotSymbol so both engines trade the same pair.
    private static final int BASE = 11;
    private static final int QUOTE = 15;
    private static final long MAKER = 1L;
    private static final long TAKER = 2L;
    private static final long PRICE = 100L;

    private XcorePipelineRunner() {}

    /** Runs warmup plus measured taker fills against one deep resting maker. */
    public static LatencyResult run(final int warmupOps, final int measureOps) {
        final int totalOps = warmupOps + measureOps;
        final long makerSize = (long) totalOps + 16L;

        final ExchangeConfiguration configuration = ExchangeConfiguration.defaultBuilder()
                .performanceCfg(PerformanceConfiguration.latencyPerformanceBuilder()
                        .threadFactory(Thread::new)
                        .build())
                .build();

        final ExchangeCore core = ExchangeCore.builder()
                .resultsConsumer((cmd, seq) -> {})
                .exchangeConfiguration(configuration)
                .build();
        core.startup();

        try {
            final ExchangeApi api = core.getApi();
            final CoreSymbolSpecification symbol = WorkloadGenerator.spotSymbol(SYMBOL);

            join(api.submitBinaryDataAsync(new BatchAddSymbolsCommand(symbol)));
            join(api.submitCommandAsync(ApiAddUser.builder().uid(MAKER).build()));
            join(api.submitCommandAsync(ApiAddUser.builder().uid(TAKER).build()));
            join(api.submitCommandAsync(ApiAdjustUserBalance.builder()
                    .uid(MAKER)
                    .currency(BASE)
                    .amount(makerSize)
                    .transactionId(1L)
                    .build()));
            join(api.submitCommandAsync(ApiAdjustUserBalance.builder()
                    .uid(TAKER)
                    .currency(QUOTE)
                    .amount((long) totalOps * PRICE + 1_000L)
                    .transactionId(2L)
                    .build()));
            join(api.submitCommandAsync(place(1L, OrderAction.ASK, makerSize, MAKER)));

            for (int i = 0; i < warmupOps; i++) {
                join(api.submitCommandAsync(place(2L + i, OrderAction.BID, 1L, TAKER)));
            }

            final Histogram histogram = new Histogram(1L, 60_000_000_000L, 3);
            final long began = System.nanoTime();
            for (int i = 0; i < measureOps; i++) {
                final long t0 = System.nanoTime();
                join(api.submitCommandAsync(place(2L + warmupOps + i, OrderAction.BID, 1L, TAKER)));
                histogram.recordValue(System.nanoTime() - t0);
            }
            final long elapsedNanos = System.nanoTime() - began;

            final double throughput = measureOps / (elapsedNanos / 1_000_000_000.0);
            return new LatencyResult(
                    measureOps,
                    throughput,
                    histogram.getValueAtPercentile(50.0),
                    histogram.getValueAtPercentile(99.0),
                    histogram.getValueAtPercentile(99.9),
                    histogram.getMaxValue());
        } finally {
            core.shutdown(3L, TimeUnit.SECONDS);
        }
    }

    private static ApiPlaceOrder place(final long orderId, final OrderAction action, final long size, final long uid) {
        return ApiPlaceOrder.builder()
                .uid(uid)
                .orderId(orderId)
                .symbol(SYMBOL)
                .price(PRICE)
                .reservePrice(action == OrderAction.BID ? PRICE : 0L)
                .size(size)
                .action(action)
                .orderType(OrderType.GTC)
                .build();
    }

    private static void join(final java.util.concurrent.CompletableFuture<CommandResultCode> future) {
        final CommandResultCode code;
        try {
            code = future.get(30L, TimeUnit.SECONDS);
        } catch (final Exception e) {
            throw new IllegalStateException("command did not complete", e);
        }
        if (code != CommandResultCode.SUCCESS) {
            throw new IllegalStateException("command failed: " + code);
        }
    }
}
