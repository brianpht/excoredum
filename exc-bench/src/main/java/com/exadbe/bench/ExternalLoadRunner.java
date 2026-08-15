package com.exadbe.bench;

import com.exadbe.protocol.CommandResultCode;
import com.exadbe.write.client.BackpressureException;
import com.exadbe.write.client.ExcClient;
import com.exadbe.write.client.ResultHandler;
import com.exadbe.write.client.config.ClientConfig;
import org.HdrHistogram.Histogram;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Drives an external (deployed, possibly multi-node) excoredum cluster through
 * the write client SDK and verifies the write side of the whole system:
 * every submitted command must be acknowledged with {@code SUCCESS}, and the
 * fills observed on egress must match the {@link LoadWorkload} simulation
 * exactly. Also prints throughput and end-to-end latency tails from the
 * client's own histogram.
 *
 * <p>Unlike {@link ExcBenchHarness}, this runner does not boot a cluster; it
 * connects to a running one over the network, so it can be containerized and
 * pointed at a dockerized cluster. The client retries the connection until the
 * cluster elects a leader, so the runner may start before the cluster is
 * ready.
 *
 * <pre>{@code
 * java -cp 'lib/*' com.exadbe.bench.ExternalLoadRunner \
 *   --endpoints=0=node-0:20100,1=node-1:20200,2=node-2:20300 \
 *   --egress=aeron:udp?endpoint=<container-ip>:0 \
 *   --ops=100000 --users=100
 * }</pre>
 *
 * <p>{@code --egress} must advertise an address the cluster nodes can reach
 * (the client's own container address); the default loopback channel only
 * works when client and cluster share a host.
 */
public final class ExternalLoadRunner {

    private static final long CONNECT_TIMEOUT_MS = 5 * 60_000L;
    private static final long SETUP_TIMEOUT_MS = 60_000L;
    private static final long CONNECT_RETRY_MS = 2_000L;
    private static final int SYMBOL = LoadWorkload.SYMBOL;
    private static final int BASE = LoadWorkload.BASE_CURRENCY;
    private static final int QUOTE = LoadWorkload.QUOTE_CURRENCY;

    private ExternalLoadRunner() {}

    public static void main(final String[] args) throws Exception {
        String endpoints = "0=localhost:20100,1=localhost:20200,2=localhost:20300";
        String egress = "aeron:udp?endpoint=localhost:0";
        int ops = 100_000;
        int users = 100;
        for (final String arg : args) {
            final int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("expected --key=value, got: " + arg);
            }
            switch (arg.substring(0, eq)) {
                case "--endpoints" -> endpoints = arg.substring(eq + 1);
                case "--egress" -> egress = arg.substring(eq + 1);
                case "--ops" -> ops = Integer.parseInt(arg.substring(eq + 1));
                case "--users" -> users = Integer.parseInt(arg.substring(eq + 1));
                default -> throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }

        final LoadWorkload workload = new LoadWorkload(ops, users);
        final boolean ok = run(workload, endpoints, egress);
        System.exit(ok ? 0 : 1);
    }

    /** Runs the workload against the external cluster; returns whether every check passed. */
    public static boolean run(final LoadWorkload workload, final String endpoints, final String egress) {
        final long[] lastIdLo = {-1L};
        final long[] results = {0L};
        final long[] success = {0L};
        final long[] nonSuccess = {0L};
        final long[] fills = {0L};
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

        final ClientConfig config =
                ClientConfig.builder(1L, endpoints).egressChannel(egress).build();
        final ExcClient client = connectWithRetry(config, handler);
        try {
            client.tradeGroupListener(group -> fills[0] += group.totalVolume());

            setup(client, workload, lastIdLo);
            final int setupCommands = (int) results[0];
            if (nonSuccess[0] != 0L) {
                throw new IllegalStateException("setup command failed with " + firstFailure[0]);
            }

            final long began = System.nanoTime();
            for (int i = 0; i < workload.ops(); i++) {
                final LoadWorkload.Command command = workload.next(i);
                submitted.put(submitOne(client, command), describe(command, i));
                // Submit in small batches and drain every batch: a burst wider
                // than the cluster's ingress buffer would backpressure an offer,
                // and a retried command lands at the end of the ingress queue,
                // reordering it behind later commands. The workload's cancel /
                // reduce commands depend on strict FIFO, so the runner must
                // never let an offer fail.
                if ((i & 15) == 15) {
                    while (client.pendingCount() > 0) {
                        client.poll();
                    }
                }
            }
            while (client.pendingCount() > 0) {
                client.poll();
            }
            final long elapsedNanos = System.nanoTime() - began;

            printReport(
                    client,
                    workload,
                    setupCommands,
                    elapsedNanos,
                    results,
                    success,
                    nonSuccess,
                    fills,
                    firstFailure,
                    firstFailureCommand,
                    submitted);
            return results[0] == setupCommands + (long) workload.ops()
                    && nonSuccess[0] == 0L
                    && client.expired() == 0L
                    && fills[0] == workload.trades();
        } finally {
            client.close();
        }
    }

    private static ExcClient connectWithRetry(final ClientConfig config, final ResultHandler handler) {
        final long deadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                System.out.println("connecting to cluster at " + config.ingressEndpoints() + " ...");
                return new ExcClient(config, handler);
            } catch (final RuntimeException e) {
                System.out.println(
                        "cluster not ready (" + e.getMessage() + "), retrying in " + CONNECT_RETRY_MS + " ms");
                sleep(CONNECT_RETRY_MS);
            }
        }
        throw new IllegalStateException("could not connect to cluster within " + CONNECT_TIMEOUT_MS + " ms");
    }

    private static void setup(final ExcClient client, final LoadWorkload workload, final long[] lastIdLo) {
        await(client, client.addSymbol(SYMBOL, BASE, QUOTE, 1L, 1L), lastIdLo);
        for (long uid = 1L; uid <= workload.users(); uid++) {
            await(client, client.addUser(uid), lastIdLo);
            await(client, client.adjustBalance(uid, BASE, LoadWorkload.BASE_FUNDING_PER_USER), lastIdLo);
            await(client, client.adjustBalance(uid, QUOTE, LoadWorkload.QUOTE_FUNDING_PER_USER), lastIdLo);
        }
    }

    private static long submitOne(final ExcClient client, final LoadWorkload.Command command) {
        for (; ; ) {
            try {
                return switch (command.type()) {
                    case PLACE -> client.placeGtc(
                            SYMBOL,
                            command.orderId(),
                            command.ask(),
                            LoadWorkload.PRICE,
                            1L,
                            command.reserveBidPrice(),
                            command.uid(),
                            0);
                    case CANCEL -> client.cancelOrder(SYMBOL, command.orderId(), command.uid());
                    case REDUCE -> client.reduceOrder(SYMBOL, command.orderId(), 1L, command.uid());
                    case ORDER_BOOK -> client.requestOrderBook(SYMBOL, command.uid());
                };
            } catch (final BackpressureException e) {
                client.poll();
            }
        }
    }

    private static String describe(final LoadWorkload.Command command, final int i) {
        return switch (command.type()) {
            case PLACE -> "i=" + i + " PLACE " + (command.ask() ? "ask" : "bid") + " uid=" + command.uid() + " orderId="
                    + command.orderId();
            case CANCEL -> "i=" + i + " CANCEL uid=" + command.uid() + " orderId=" + command.orderId();
            case REDUCE -> "i=" + i + " REDUCE uid=" + command.uid() + " orderId=" + command.orderId();
            case ORDER_BOOK -> "i=" + i + " ORDER_BOOK uid=" + command.uid();
        };
    }

    private static void await(final ExcClient client, final long commandIdLo, final long[] lastIdLo) {
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

    private static void printReport(
            final ExcClient client,
            final LoadWorkload workload,
            final int setupCommands,
            final long elapsedNanos,
            final long[] results,
            final long[] success,
            final long[] nonSuccess,
            final long[] fills,
            final CommandResultCode[] firstFailure,
            final String[] firstFailureCommand,
            final Long2ObjectHashMap<String> submitted) {
        final double opsPerSec = workload.ops() / (elapsedNanos / 1_000_000_000.0);
        final Histogram latency = client.latencyHistogram();
        System.out.println();
        System.out.println("== external load result ==");
        System.out.printf("commands:  setup=%d main=%d total=%d%n", setupCommands, workload.ops(), results[0]);
        System.out.printf(
                "results:   success=%d nonSuccess=%d expired=%d%n", success[0], nonSuccess[0], client.expired());
        System.out.printf("first non-success: %s %s%n", firstFailure[0], firstFailureCommand[0]);
        System.out.printf("egress:    fills observed=%d expected=%d%n", fills[0], workload.trades());
        System.out.printf(
                "workload:  places=%d cancels=%d reduces=%d orderBookRequests=%d%n",
                workload.totalPlaces(), workload.cancels(), workload.reduces(), workload.orderBookRequests());
        System.out.printf(
                "session:   leaderChanges=%d reconnects=%d keepalives=%d backpressure=%d retransmits=%d firstNegOffer=%d%n",
                client.leaderChanges(),
                client.reconnects(),
                client.keepalives(),
                client.backpressureEvents(),
                client.retransmits(),
                client.firstNegativeOfferResult());
        if (client.retransmits() > 0L) {
            System.out.println("first retransmitted command id: " + client.firstRetransmitIdLo() + " "
                    + submitted.get(client.firstRetransmitIdLo()));
        }
        if (client.expired() > 0L) {
            System.out.println("first expired command id: " + client.firstExpiredIdLo() + " "
                    + submitted.get(client.firstExpiredIdLo()));
        }
        System.out.printf("elapsed:   %.1f s  throughput=%.0f ops/s%n", elapsedNanos / 1_000_000_000.0, opsPerSec);
        System.out.printf(
                "latency:   p50=%.1fus p99=%.1fus p99.9=%.1fus max=%.1fus%n",
                latency.getValueAtPercentile(50.0) / 1000.0,
                latency.getValueAtPercentile(99.0) / 1000.0,
                latency.getValueAtPercentile(99.9) / 1000.0,
                latency.getMaxValue() / 1000.0);
        System.out.println("write-side checks: "
                + (results[0] == setupCommands + (long) workload.ops()
                                && nonSuccess[0] == 0L
                                && client.expired() == 0L
                                && fills[0] == workload.trades()
                        ? "PASS"
                        : "FAIL"));
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
