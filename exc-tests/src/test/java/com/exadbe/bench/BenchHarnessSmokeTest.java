package com.exadbe.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Smoke test that the end-to-end latency harness boots a cluster and measures. */
@Tag("integration")
class BenchHarnessSmokeTest {

    @Test
    @Timeout(120)
    void measuresRoundTripLatency(@TempDir final Path baseDir) {
        final LatencyResult result = ExcBenchHarness.run(baseDir, 50, 200);
        assertEquals(200L, result.ops());
        assertTrue(result.throughputPerSec() > 0.0, "throughput must be positive");
        assertTrue(result.p50Nanos() > 0L, "p50 latency must be recorded");
        assertTrue(result.maxNanos() >= result.p50Nanos(), "max must be at least p50");
    }
}
