package com.exadbe.xcorebench;

import com.exadbe.bench.LatencyResult;
import java.util.List;

/**
 * Engine-path comparison: excoredum's single-thread dispatch (decode, dedup,
 * risk, matching) vs exchange-core's full disruptor pipeline (risk + matching
 * across thread hops). Both run closed-loop in-process with no replication and
 * no durable journal; the shape is one taker order fully filling against one
 * deep resting maker, as exc-bench uses.
 */
public final class EngineComparison {

    private EngineComparison() {}

    public static String run(final int warmupOps, final int measureOps) {
        final ComparisonReport report = new ComparisonReport().heading("Engine dispatch / pipeline latency");
        report.note(String.format(
                "shape: closed-loop taker fill vs deep resting maker; warmup=%d ops=%d", warmupOps, measureOps));
        report.note("exc: single-thread MatchingEngine.process (decode + dedup + risk + match), allocation-free");
        report.note("xcore: ExchangeCore disruptor pipeline (risk + match + result future), allocates per command");

        final LatencyResult exc = ExcEngineRunner.run(warmupOps, measureOps);
        final LatencyResult xcore = XcorePipelineRunner.run(warmupOps, measureOps);

        report.table(
                List.of("impl", "ops/s", "p50 us", "p99 us", "p99.9 us", "max us"),
                List.of(
                        List.of(
                                "exc-core engine dispatch",
                                String.format("%.0f", exc.throughputPerSec()),
                                ComparisonReport.micros(exc.p50Nanos()),
                                ComparisonReport.micros(exc.p99Nanos()),
                                ComparisonReport.micros(exc.p999Nanos()),
                                ComparisonReport.micros(exc.maxNanos())),
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
