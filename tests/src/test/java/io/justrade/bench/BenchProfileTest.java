package io.justrade.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Preset profiles must map names to the documented latency/throughput knobs. */
class BenchProfileTest {

    @Test
    void latencyUsesShallowPipeline() {
        assertEquals(16, BenchProfile.LATENCY.batch());
        assertEquals(2_000L, BenchProfile.LATENCY.retryBackoffMs());
    }

    @Test
    void throughputUsesDeepPipeline() {
        assertEquals(64, BenchProfile.THROUGHPUT.batch());
        assertEquals(2_000L, BenchProfile.THROUGHPUT.retryBackoffMs());
    }

    @Test
    void parsesNamesCaseInsensitively() {
        assertSame(BenchProfile.LATENCY, BenchProfile.fromName("latency"));
        assertSame(BenchProfile.LATENCY, BenchProfile.fromName("  LATENCY  "));
        assertSame(BenchProfile.THROUGHPUT, BenchProfile.fromName("Throughput"));
    }

    @Test
    void rejectsUnknownOrBlankNames() {
        assertThrows(IllegalArgumentException.class, () -> BenchProfile.fromName("bogus"));
        assertThrows(IllegalArgumentException.class, () -> BenchProfile.fromName("  "));
        assertThrows(IllegalArgumentException.class, () -> BenchProfile.fromName(null));
    }
}
