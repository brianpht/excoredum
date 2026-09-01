package io.justrade.xcorebench;

import exchange.core2.core.common.CoreSymbolSpecification;
import io.justrade.protocol.CommandResultCode;
import io.justrade.write.client.BackpressureException;
import io.justrade.write.client.ResultHandler;
import io.justrade.write.client.WriteClient;
import io.justrade.write.client.config.ClientConfig;
import org.HdrHistogram.Histogram;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Deployed-benchmark runner: replays the parity-tested exchange-core {@link Workload}
 * against a running (possibly multi-node) justrade cluster through the write client
 * SDK and verifies the whole write side. Unlike {@code ExternalLoadRunner}, this
 * drives the upstream {@code TestOrdersGenerator} mix (GTC / IOC / FOK-BUDGET / cancel
 * / move / reduce) instead of the synthetic single-price-level workload, so the
 * measured shape matches exchange-core's published single-symbol benchmark.
 *
 * <p>Three modes:
 * <ul>
 *   <li>{@code throughput} - closed-loop submit + drain; reports ops/s and latency tails.</li>
 *   <li>{@code latency} - open-loop at one target rate; reports one latency-table row.</li>
 *   <li>{@code hiccups} - closed-loop load while a detector samples the clock for pauses.</li>
 * </ul>
 *
 * <p>The measurement is end-to-end (network + consensus + replication + archive on the
 * path) and is not comparable to exchange-core's matching-only latency table. The
 * workload is stateful, so a latency sweep replays it once per target rate on a fresh
 * cluster; the runner itself measures a single rate per invocation.
 */
public final class XcoreWorkloadRunner {

    public static final int SYMBOL = 1;

    private static final long FUNDING = 1_000_000_000_000L;
    private static final long CONNECT_TIMEOUT_MS = 5 * 60_000L;
    private static final long SETUP_TIMEOUT_MS = 60_000L;
    private static final long CONNECT_RETRY_MS = 2_000L;
    private static final int DEFAULT_BATCH = 16;
    private static final long DEFAULT_RETRY_BACKOFF_MS = 2_000L;
    private static final int DEFAULT_RATE = 25_000;

    private XcoreWorkloadRunner() {}

    public static void main(final String[] args) throws Exception {
        String endpoints = "0=localhost:20100,1=localhost:20200,2=localhost:20300";
        String egress = "aeron:udp?endpoint=localhost:0";
        String mode = "throughput";
        int commands = 100_000;
        int targetOrders = 1_000;
        int users = 1_000;
        int seed = 1;
        int batch = DEFAULT_BATCH;
        long clientId = 1L;
        long retryBackoffMs = DEFAULT_RETRY_BACKOFF_MS;
        int rate = DEFAULT_RATE;

        for (final String arg : args) {
            final int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("expected --key=value, got: " + arg);
            }
            switch (arg.substring(0, eq)) {
                case "--endpoints" -> endpoints = arg.substring(eq + 1);
                case "--egress" -> egress = arg.substring(eq + 1);
                case "--mode" -> mode = arg.substring(eq + 1);
                case "--commands" -> commands = Integer.parseInt(arg.substring(eq + 1));
                case "--target-orders" -> targetOrders = Integer.parseInt(arg.substring(eq + 1));
                case "--users" -> users = Integer.parseInt(arg.substring(eq + 1));
                case "--seed" -> seed = Integer.parseInt(arg.substring(eq + 1));
                case "--batch" -> batch = Integer.parseInt(arg.substring(eq + 1));
                case "--client-id" -> clientId = Long.parseLong(arg.substring(eq + 1));
                case "--retry-backoff-ms" -> retryBackoffMs = Long.parseLong(arg.substring(eq + 1));
                case "--rate" -> rate = Integer.parseInt(arg.substring(eq + 1));
                default -> throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }

        final Workload workload = WorkloadGenerator.generate(commands, targetOrders, users, SYMBOL, false, false, seed);
        final boolean ok = run(mode, workload, users, endpoints, egress, clientId, batch, retryBackoffMs, rate);
        System.exit(ok ? 0 : 1);
    }

    /** Replays {@code workload} against the cluster at {@code endpoints}; returns whether every check passed. */
    public static boolean run(
            final String mode,
            final Workload workload,
            final int users,
            final String endpoints,
            final String egress,
            final long clientId,
            final int batch,
            final long retryBackoffMs,
            final int rate) {
        if (batch < 1 || (batch & (batch - 1)) != 0) {
            throw new IllegalArgumentException("batch must be a positive power of two, was: " + batch);
        }
        if (retryBackoffMs <= 0L) {
            throw new IllegalArgumentException("retryBackoffMs must be positive, was: " + retryBackoffMs);
        }
        if ("latency".equals(mode) && rate <= 0) {
            throw new IllegalArgumentException("latency mode requires a positive --rate");
        }

        final long[] lastIdLo = {-1L};
        final long[] results = {0L};
        final long[] success = {0L};
        final long[] nonSuccess = {0L};
        final long[] fillCount = {0L};
        final long[] fillVolume = {0L};
        final CommandResultCode[] firstFailure = {CommandResultCode.NULL_VAL};
        final String[] firstFailureCommand = {null};
        final Long2ObjectHashMap<String> submitted = new Long2ObjectHashMap<>();
        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    lastIdLo[0] = idLo;
                    results[0]++;
                    if (code == CommandResultCode.SUCCESS) {
                        success[0]++;
                    } else {
                        nonSuccess[0]++;
                        if (firstFailure[0] == CommandResultCode.NULL_VAL) {
                            firstFailure[0] = code;
                            firstFailureCommand[0] = submitted.get(idLo);
                        }
                    }
                };

        final ClientConfig config = ClientConfig.builder(clientId, endpoints)
                .egressChannel(egress)
                .retryBackoffNs(retryBackoffMs * 1_000_000L)
                .build();
        final WriteClient client = connectWithRetry(config, handler);
        try {
            client.tradeGroupListener(group -> {
                fillCount[0] += group.fillCount();
                fillVolume[0] += group.totalVolume();
            });

            setup(client, users, lastIdLo);
            final int setupCommands = (int) results[0];
            if (nonSuccess[0] != 0L) {
                throw new IllegalStateException("setup command failed with " + firstFailure[0]);
            }

            // Fill phase: build the book to its target depth one command at a time so
            // every resting order is applied before the measured phase begins.
            for (int i = 0; i < workload.fillCount(); i++) {
                final long id = submitOne(client, workload, i);
                submitted.put(id, describe(workload, i));
                await(client, id, lastIdLo);
            }

            // Measure only the benchmark phase: drop setup and fill latency from the
            // histogram so the reported tails describe the measured window alone.
            client.latencyHistogram().reset();

            final HiccupDetector detector = "hiccups".equals(mode) ? new HiccupDetector() : null;
            if (detector != null) {
                detector.start();
            }
            final long began = System.nanoTime();
            try {
                if ("latency".equals(mode)) {
                    runLatency(client, workload, rate, submitted);
                } else {
                    runClosedLoop(client, workload, batch, submitted);
                }
            } finally {
                if (detector != null) {
                    detector.close();
                }
            }
            final long elapsed = System.nanoTime() - began;

            drain(client);

            final BookStats expected = JustradeBookRunner.replay(workload, SYMBOL);
            final int benchmarkOps = workload.count() - workload.fillCount();
            final boolean ok = results[0] == setupCommands + (long) workload.count()
                    && nonSuccess[0] == 0L
                    && client.expired() == 0L
                    && fillCount[0] == expected.trades()
                    && fillVolume[0] == expected.tradeVolume();

            printReport(
                    mode,
                    workload,
                    users,
                    client,
                    setupCommands,
                    benchmarkOps,
                    elapsed,
                    results,
                    success,
                    nonSuccess,
                    fillCount,
                    fillVolume,
                    expected,
                    firstFailure,
                    firstFailureCommand,
                    detector,
                    rate,
                    ok);
            return ok;
        } finally {
            client.close();
        }
    }

    private static WriteClient connectWithRetry(final ClientConfig config, final ResultHandler handler) {
        final long deadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                System.out.println("connecting to cluster at " + config.ingressEndpoints() + " ...");
                return new WriteClient(config, handler);
            } catch (final RuntimeException e) {
                System.out.println(
                        "cluster not ready (" + e.getMessage() + "), retrying in " + CONNECT_RETRY_MS + " ms");
                sleep(CONNECT_RETRY_MS);
            }
        }
        throw new IllegalStateException("could not connect to cluster within " + CONNECT_TIMEOUT_MS + " ms");
    }

    private static void setup(final WriteClient client, final int users, final long[] lastIdLo) {
        final CoreSymbolSpecification spec = WorkloadGenerator.spotSymbol(SYMBOL);
        await(
                client,
                client.addSymbol(
                        SYMBOL,
                        spec.baseCurrency,
                        spec.quoteCurrency,
                        spec.baseScaleK,
                        spec.quoteScaleK,
                        spec.takerFee,
                        spec.makerFee),
                lastIdLo);
        for (long uid = 1L; uid <= users; uid++) {
            await(client, client.addUser(uid), lastIdLo);
            await(client, client.adjustBalance(uid, spec.baseCurrency, FUNDING), lastIdLo);
            await(client, client.adjustBalance(uid, spec.quoteCurrency, FUNDING), lastIdLo);
        }
    }

    private static void runClosedLoop(
            final WriteClient client,
            final Workload workload,
            final int batch,
            final Long2ObjectHashMap<String> submitted) {
        int inFlight = 0;
        for (int i = workload.fillCount(); i < workload.count(); i++) {
            submitted.put(submitOne(client, workload, i), describe(workload, i));
            inFlight++;
            if ((inFlight & (batch - 1)) == 0) {
                drain(client);
            }
        }
    }

    private static void runLatency(
            final WriteClient client,
            final Workload workload,
            final int rate,
            final Long2ObjectHashMap<String> submitted) {
        // Constant interval between submit instants; when the cluster falls behind
        // the schedule is not advanced to catch up, so later commands carry the
        // backlog in their measured latency (exchange-core's "no coordinated omission").
        final long intervalNs = 1_000_000_000L / rate;
        long nextSubmitNs = System.nanoTime();
        for (int i = workload.fillCount(); i < workload.count(); i++) {
            while (System.nanoTime() < nextSubmitNs) {
                Thread.onSpinWait();
            }
            submitted.put(submitOne(client, workload, i), describe(workload, i));
            nextSubmitNs += intervalNs;
            client.poll();
        }
    }

    private static long submitOne(final WriteClient client, final Workload workload, final int i) {
        for (; ; ) {
            try {
                return switch (workload.type(i)) {
                    case Workload.PLACE -> place(client, workload, i);
                    case Workload.CANCEL -> client.cancelOrder(SYMBOL, workload.orderId(i), workload.uid(i));
                    case Workload.MOVE -> client.moveOrder(
                            SYMBOL, workload.orderId(i), workload.price(i), workload.uid(i));
                    default -> client.reduceOrder(SYMBOL, workload.orderId(i), workload.size(i), workload.uid(i));
                };
            } catch (final BackpressureException e) {
                client.poll();
            }
        }
    }

    private static long place(final WriteClient client, final Workload workload, final int i) {
        return switch (workload.orderType(i)) {
            case Workload.GTC -> client.placeGtc(
                    SYMBOL,
                    workload.orderId(i),
                    workload.ask(i),
                    workload.price(i),
                    workload.size(i),
                    workload.reservePrice(i),
                    workload.uid(i),
                    0);
            case Workload.IOC -> client.placeIoc(
                    SYMBOL,
                    workload.orderId(i),
                    workload.ask(i),
                    workload.price(i),
                    workload.size(i),
                    workload.uid(i),
                    0);
            default -> client.placeFokBudget(
                    SYMBOL,
                    workload.orderId(i),
                    workload.ask(i),
                    workload.price(i),
                    workload.size(i),
                    workload.uid(i),
                    0);
        };
    }

    private static String describe(final Workload workload, final int i) {
        return switch (workload.type(i)) {
            case Workload.PLACE -> "i=" + i + " PLACE " + orderTypeName(workload.orderType(i)) + " "
                    + (workload.ask(i) ? "ask" : "bid") + " uid=" + workload.uid(i) + " orderId=" + workload.orderId(i);
            case Workload.CANCEL -> "i=" + i + " CANCEL uid=" + workload.uid(i) + " orderId=" + workload.orderId(i);
            case Workload.MOVE -> "i=" + i + " MOVE uid=" + workload.uid(i) + " orderId=" + workload.orderId(i)
                    + " price=" + workload.price(i);
            default -> "i=" + i + " REDUCE uid=" + workload.uid(i) + " orderId=" + workload.orderId(i);
        };
    }

    private static String orderTypeName(final byte orderType) {
        return switch (orderType) {
            case Workload.GTC -> "GTC";
            case Workload.IOC -> "IOC";
            default -> "FOK_BUDGET";
        };
    }

    private static void await(final WriteClient client, final long commandIdLo, final long[] lastIdLo) {
        final long deadline = System.currentTimeMillis() + SETUP_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("no result for command id " + commandIdLo);
    }

    private static void drain(final WriteClient client) {
        while (client.pendingCount() > 0) {
            client.poll();
        }
    }

    private static void printReport(
            final String mode,
            final Workload workload,
            final int users,
            final WriteClient client,
            final int setupCommands,
            final int benchmarkOps,
            final long elapsed,
            final long[] results,
            final long[] success,
            final long[] nonSuccess,
            final long[] fillCount,
            final long[] fillVolume,
            final BookStats expected,
            final CommandResultCode[] firstFailure,
            final String[] firstFailureCommand,
            final HiccupDetector detector,
            final int rate,
            final boolean ok) {
        final Histogram latency = client.latencyHistogram();
        System.out.println();
        System.out.println("== xcore workload result (mode=" + mode + ") ==");
        System.out.printf(
                "workload:  fill=%d benchmark=%d users=%d mix(placeLimit=%d placeMarket=%d cancel=%d move=%d reduce=%d)%n",
                workload.fillCount(),
                benchmarkOps,
                users,
                workload.counterPlaceLimit(),
                workload.counterPlaceMarket(),
                workload.counterCancel(),
                workload.counterMove(),
                workload.counterReduce());
        System.out.printf(
                "commands:  setup=%d fill=%d main=%d total=%d%n",
                setupCommands, workload.fillCount(), benchmarkOps, results[0]);
        System.out.printf(
                "results:   success=%d nonSuccess=%d expired=%d%n", success[0], nonSuccess[0], client.expired());
        System.out.printf("first non-success: %s %s%n", firstFailure[0], firstFailureCommand[0]);
        System.out.printf(
                "egress:    fills observed=%d expected=%d  volume observed=%d expected=%d%n",
                fillCount[0], expected.trades(), fillVolume[0], expected.tradeVolume());
        if ("latency".equals(mode)) {
            System.out.printf("target:    rate=%d ops/s%n", rate);
        }
        System.out.printf(
                "elapsed:   %.1f s  throughput=%.0f ops/s%n",
                elapsed / 1_000_000_000.0, benchmarkOps / (elapsed / 1_000_000_000.0));
        System.out.printf(
                "latency:   p50=%.1fus p90=%.1fus p95=%.1fus p99=%.1fus p99.9=%.1fus p99.99=%.1fus worst=%.1fus%n",
                latency.getValueAtPercentile(50.0) / 1000.0,
                latency.getValueAtPercentile(90.0) / 1000.0,
                latency.getValueAtPercentile(95.0) / 1000.0,
                latency.getValueAtPercentile(99.0) / 1000.0,
                latency.getValueAtPercentile(99.9) / 1000.0,
                latency.getValueAtPercentile(99.99) / 1000.0,
                latency.getMaxValue() / 1000.0);
        if (detector != null) {
            System.out.printf(
                    "hiccups:   max=%.3fms samples=%d%n", detector.maxGapNanos() / 1_000_000.0, detector.samples());
        }
        System.out.printf(
                "session:   leaderChanges=%d reconnects=%d keepalives=%d backpressure=%d retransmits=%d firstNegOffer=%d%n",
                client.leaderChanges(),
                client.reconnects(),
                client.keepalives(),
                client.backpressureEvents(),
                client.retransmits(),
                client.firstNegativeOfferResult());
        System.out.println("write-side checks: " + (ok ? "PASS" : "FAIL"));
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting", e);
        }
    }
}
