package io.justrade.bench;

/**
 * One measured window for the end-to-end latency harness: throughput plus
 * client-observed round-trip latency percentiles (all latency fields in
 * nanoseconds).
 */
public record LatencyResult(
        long ops, double throughputPerSec, long p50Nanos, long p99Nanos, long p999Nanos, long maxNanos) {}
