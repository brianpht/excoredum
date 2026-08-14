package com.exadbe.bench;

import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.write.client.ExcClient;
import com.exadbe.write.client.ResultHandler;
import com.exadbe.write.client.config.ClientConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import org.HdrHistogram.Histogram;

/**
 * End-to-end latency harness: boots an in-process single-node cluster and drives
 * it with the real client in a closed loop (one command outstanding at a time),
 * recording client-observed round-trip latency in an {@link Histogram} and
 * reporting tail percentiles. Each measured op is a taker order that fully fills
 * one unit against a deep resting maker, so the book stays bounded.
 *
 * <p>Not part of the deterministic hot path: it uses the system clock, heap
 * allocation, and a blocking client, all of which the core forbids.
 *
 * <pre>{@code
 * ./gradlew :exc-bench:run --args="--warmup=5000 --ops=20000"
 * }</pre>
 */
public final class ExcBenchHarness {

    private static final int SYMBOL = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 1L;
    private static final long TAKER = 2L;
    private static final long PRICE = 100L;
    private static final long TIMEOUT_MS = 30_000L;

    private ExcBenchHarness() {}

    public static void main(final String[] args) throws Exception {
        int warmupOps = 5_000;
        int measureOps = 20_000;
        for (final String arg : args) {
            final int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("expected --key=value, got: " + arg);
            }
            final String key = arg.substring(0, eq);
            final String value = arg.substring(eq + 1);
            switch (key) {
                case "--warmup" -> warmupOps = Integer.parseInt(value);
                case "--ops" -> measureOps = Integer.parseInt(value);
                default -> throw new IllegalArgumentException("unknown argument: " + key);
            }
        }

        final Path baseDir = Files.createTempDirectory("exc-bench-");
        final LatencyResult result = run(baseDir, warmupOps, measureOps);
        printReport(result);
    }

    /** Runs the harness against a fresh in-process cluster rooted at {@code baseDir}. */
    public static LatencyResult run(final Path baseDir, final int warmupOps, final int measureOps) {
        final long[] lastIdLo = {Long.MIN_VALUE};
        final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    lastIdLo[0] = idLo;
                    lastCode[0] = code;
                };

        final long makerSize = (long) warmupOps + measureOps + 16L;
        final ClientConfig config =
                ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

        try (ClusterNode node = new ClusterNode(ClusterConfig.singleNodeLocalhost(0, baseDir), CoreConfig.defaults());
                ExcClient client = new ExcClient(config, handler)) {

            await(client, client.addSymbol(SYMBOL, BASE, QUOTE, 1L, 1L), lastIdLo);
            await(client, client.addUser(MAKER), lastIdLo);
            await(client, client.adjustBalance(MAKER, BASE, makerSize), lastIdLo);
            await(client, client.addUser(TAKER), lastIdLo);
            await(
                    client,
                    client.adjustBalance(TAKER, QUOTE, ((long) warmupOps + measureOps) * PRICE + 1_000L),
                    lastIdLo);

            // One deep resting ask the takers nibble at without depleting.
            await(client, client.placeGtc(SYMBOL, 1L, true, PRICE, makerSize, 0L, MAKER, 0), lastIdLo);
            require(lastCode[0], "resting maker");

            long orderId = 2L;
            for (int i = 0; i < warmupOps; i++) {
                await(client, client.placeGtc(SYMBOL, orderId++, false, PRICE, 1L, PRICE, TAKER, 0), lastIdLo);
            }

            final Histogram histogram = new Histogram(1L, 60_000_000_000L, 3);
            final long began = System.nanoTime();
            for (int i = 0; i < measureOps; i++) {
                final long t0 = System.nanoTime();
                await(client, client.placeGtc(SYMBOL, orderId++, false, PRICE, 1L, PRICE, TAKER, 0), lastIdLo);
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
        }
    }

    private static void printReport(final LatencyResult r) {
        System.out.printf("%nexc end-to-end latency: ops=%d throughput=%.0f ops/s%n", r.ops(), r.throughputPerSec());
        System.out.printf(
                "  p50=%.1fus p99=%.1fus p99.9=%.1fus max=%.1fus%n",
                r.p50Nanos() / 1000.0, r.p99Nanos() / 1000.0, r.p999Nanos() / 1000.0, r.maxNanos() / 1000.0);
    }

    private static void require(final CommandResultCode code, final String what) {
        if (code != CommandResultCode.SUCCESS) {
            throw new IllegalStateException(what + " failed: " + code);
        }
    }

    private static void await(final ExcClient client, final long commandIdLo, final long[] lastIdLo) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("no result for commandIdLo=" + commandIdLo);
    }
}
