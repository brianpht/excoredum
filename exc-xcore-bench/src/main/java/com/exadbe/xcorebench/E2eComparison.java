package com.exadbe.xcorebench;

import com.exadbe.bench.ExcBenchHarness;
import com.exadbe.bench.LatencyResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * System-level closed-loop round-trip comparison: excoredum's full in-process
 * Aeron Cluster node driven by the real client (consensus + replication +
 * archive included) vs exchange-core's in-process disruptor pipeline (no
 * consensus, no replication). The two stacks provide different guarantees; the
 * numbers are reported side by side, not as an apples-to-apples race.
 */
public final class E2eComparison {

    private E2eComparison() {}

    public static String run(final int warmupOps, final int measureOps) throws Exception {
        final ComparisonReport report = new ComparisonReport().heading("End-to-end closed-loop round-trip latency");
        report.note(String.format("shape: same engine shape; warmup=%d ops=%d", warmupOps, measureOps));
        report.note("exc: in-process single-node Aeron Cluster + ExcClient (consensus + archive on the path)");
        report.note("xcore: in-process ExchangeCore pipeline (no consensus / replication)");

        final LatencyResult xcore = XcorePipelineRunner.run(warmupOps, measureOps);

        final LatencyResult exc;
        final Path baseDir = Files.createTempDirectory("exc-xcore-bench-");
        try {
            exc = ExcBenchHarness.run(baseDir, warmupOps, measureOps);
        } finally {
            deleteRecursively(baseDir);
        }

        report.table(
                List.of("impl", "ops/s", "p50 us", "p99 us", "p99.9 us", "max us"),
                List.of(
                        List.of(
                                "exc-core cluster e2e",
                                String.format("%.0f", exc.throughputPerSec()),
                                ComparisonReport.micros(exc.p50Nanos()),
                                ComparisonReport.micros(exc.p99Nanos()),
                                ComparisonReport.micros(exc.p999Nanos()),
                                ComparisonReport.micros(exc.maxNanos())),
                        List.of(
                                "xcore pipeline e2e",
                                String.format("%.0f", xcore.throughputPerSec()),
                                ComparisonReport.micros(xcore.p50Nanos()),
                                ComparisonReport.micros(xcore.p99Nanos()),
                                ComparisonReport.micros(xcore.p999Nanos()),
                                ComparisonReport.micros(xcore.maxNanos()))));
        return report.render();
    }

    private static void deleteRecursively(final Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (var stream = Files.walk(root)) {
            stream.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> p.toFile().delete());
        } catch (final Exception e) {
            // Best effort: the harness directories live under the system temp dir.
        }
    }
}
