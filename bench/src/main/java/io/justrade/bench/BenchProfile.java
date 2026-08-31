package io.justrade.bench;

import java.util.Locale;

/**
 * Named presets trading tail-latency headroom against closed-loop throughput.
 *
 * <p>The lever is pipeline depth ({@code batch}): a shallow pipeline keeps the
 * in-flight set small so commands wait less in the ingress queue, while a deep
 * pipeline raises throughput roughly 3x (see deploy/aws/PERFORMANCE.md). Both
 * presets share the same re-offer deadline, which is sized above the observed
 * end-to-end tail so a slow ACK never triggers a spurious re-offer.
 *
 * <p>The complementary cluster-side knob is {@code justrade.aeron.termLength}
 * (ingress term buffer); it is configured on the nodes, not by this profile.
 */
public enum BenchProfile {
    /** Shallow pipeline: lowest tail-latency variance, ~34k ops/s at batch 16. */
    LATENCY(16, 2_000L),
    /** Deep pipeline: ~3x throughput (~106k ops/s at batch 64), more backpressure. */
    THROUGHPUT(64, 2_000L);

    private final int batch;
    private final long retryBackoffMs;

    BenchProfile(final int batch, final long retryBackoffMs) {
        this.batch = batch;
        this.retryBackoffMs = retryBackoffMs;
    }

    /** Drain batch (commands in flight before draining acknowledgements). */
    public int batch() {
        return batch;
    }

    /** Command re-offer deadline in milliseconds. */
    public long retryBackoffMs() {
        return retryBackoffMs;
    }

    /** Parses a case-insensitive profile name, e.g. {@code latency} or {@code throughput}. */
    public static BenchProfile fromName(final String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("profile is required");
        }
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }
}
