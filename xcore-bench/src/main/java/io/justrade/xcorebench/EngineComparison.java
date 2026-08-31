package io.justrade.xcorebench;

import io.justrade.bench.LatencyResult;
import java.util.List;

/**
 * Engine-path comparison: justrade's single-thread dispatch (decode, dedup,
 * risk, matching) vs exchange-core's full disruptor pipeline (risk + matching
 * across thread hops). Both run closed-loop in-process with no replication and
 * no durable journal; the shape is one taker order fully filling against one
 * deep resting maker, as bench uses.
 */
public final class EngineComparison {

    private EngineComparison() {}

    public static String run(final int warmupOps, final int measureOps) {
        final ComparisonReport report = new ComparisonReport().heading("Engine dispatch / pipeline latency");
        report.note(String.format(
                "shape: closed-loop taker fill vs deep resting maker; warmup=%d ops=%d", warmupOps, measureOps));
        report.note("justrade: single-thread MatchingEngine.process (decode + dedup + risk + match), allocation-free");
        report.note("xcore: ExchangeCore disruptor pipeline (risk + match + result future), allocates per command");

        final LatencyResult justrade = ExcEngineRunner.run(warmupOps, measureOps);
        final LatencyResult xcore = XcorePipelineRunner.run(warmupOps, measureOps);

        report.table(
                List.of("impl", "ops/s", "p50 us", "p99 us", "p99.9 us", "max us"),
                List.of(
                        List.of(
                                "core engine dispatch",
                                String.format("%.0f", justrade.throughputPerSec()),
                                ComparisonReport.micros(justrade.p50Nanos()),
                                ComparisonReport.micros(justrade.p99Nanos()),
                                ComparisonReport.micros(justrade.p999Nanos()),
                                ComparisonReport.micros(justrade.maxNanos())),
                        List.of(
                                "xcore disruptor pipeline",
                                String.format("%.0f", xcore.throughputPerSec()),
                                ComparisonReport.micros(xcore.p50Nanos()),
                                ComparisonReport.micros(xcore.p99Nanos()),
                                ComparisonReport.micros(xcore.p999Nanos()),
                                ComparisonReport.micros(xcore.maxNanos()))));
        return report.render();
    }
}
