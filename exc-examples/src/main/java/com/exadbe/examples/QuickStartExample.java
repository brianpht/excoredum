package com.exadbe.examples;

import com.exadbe.client.ExcClient;
import com.exadbe.client.config.ClientConfig;
import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.CommandResultCode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A runnable, end-to-end tour of excoredum. Boots an in-process single-node
 * Aeron cluster in a temp directory, connects the client SDK, and walks one
 * small trading scenario that exercises every egress surface: per-fill trade
 * events, per-command trade groups, a reduce event, a reject event, and an L2
 * order-book snapshot.
 *
 * <p>Run it with: {@code ./gradlew :exc-examples:run}
 */
public final class QuickStartExample {

    private static final long TIMEOUT_MS = 15_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 1L;
    private static final long TAKER = 2L;

    private QuickStartExample() {}

    public static void main(final String[] args) throws IOException {
        final Path baseDir = Files.createTempDirectory("excoredum-example");
        System.out.println("== excoredum quick start (cluster dir: " + baseDir + ") ==");

        try (ClusterNode node = new ClusterNode(ClusterConfig.singleNodeLocalhost(0, baseDir), CoreConfig.defaults())) {
            run();
        }
        System.out.println("== done ==");
    }

    private static void run() {
        // The result handler correlates each CommandResult with the submitted
        // command by command id. Events are observed through the listeners below.
        final long[] lastCommandIdLo = {-1L};
        final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};

        final AtomicInteger trades = new AtomicInteger();
        final AtomicInteger groups = new AtomicInteger();
        final AtomicInteger reduces = new AtomicInteger();
        final AtomicInteger rejects = new AtomicInteger();
        final AtomicInteger snapshots = new AtomicInteger();

        final ClientConfig config =
                ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

        try (ExcClient client = new ExcClient(
                config, (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    lastCommandIdLo[0] = idLo;
                    lastCode[0] = code;
                    System.out.printf(
                            "  [result] command=%d %s%s%n", idLo, code, hasFilledSize ? " filled=" + filledSize : "");
                })) {

            client.tradeListener((idHi, idLo, index, symbolId, makerOrderId, makerUid, takerUid, price, size, done) -> {
                trades.incrementAndGet();
                System.out.printf(
                        "  [trade ] maker=%d(uid %d) x taker uid %d: %d @ %d%s%n",
                        makerOrderId, makerUid, takerUid, size, price, done ? " (maker completed)" : "");
            });

            // One group per taker command, delivered when the CommandResult's
            // announced eventCount frames have arrived.
            client.tradeGroupListener(group -> {
                groups.incrementAndGet();
                System.out.printf(
                        "  [group ] command=%d: %d fill(s), totalVolume=%d, taker uid %d%n",
                        group.commandIdLo(), group.fillCount(), group.totalVolume(), group.takerUid());
            });

            client.reduceListener((idHi, idLo, index, symbolId, orderId, uid, reducedBy, price, completed) -> {
                reduces.incrementAndGet();
                System.out.printf(
                        "  [reduce] order=%d uid %d reduced by %d @ %d%s%n",
                        orderId, uid, reducedBy, price, completed ? " (order completed)" : "");
            });

            client.rejectListener((idHi, idLo, index, symbolId, orderId, uid, rejectedSize, price) -> {
                rejects.incrementAndGet();
                System.out.printf("  [reject] order=%d uid %d rejected %d @ %d%n", orderId, uid, rejectedSize, price);
            });

            client.orderBookListener(snapshot -> {
                snapshots.incrementAndGet();
                final StringBuilder book = new StringBuilder("  [l2    ] asks:");
                for (int i = 0; i < snapshot.askDepth(); i++) {
                    book.append(' ').append(snapshot.askPrice(i)).append('x').append(snapshot.askVolume(i));
                }
                book.append(snapshot.askDepth() == 0 ? " empty" : "").append(" | bids:");
                for (int i = 0; i < snapshot.bidDepth(); i++) {
                    book.append(' ').append(snapshot.bidPrice(i)).append('x').append(snapshot.bidVolume(i));
                }
                book.append(snapshot.bidDepth() == 0 ? " empty" : "");
                System.out.println(book);
            });

            // -- setup: one symbol, two funded users --
            await(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastCommandIdLo);
            await(client, client.addUser(MAKER), lastCommandIdLo);
            await(client, client.adjustBalance(MAKER, BASE, 1_000L), lastCommandIdLo);
            await(client, client.adjustBalance(MAKER, QUOTE, 1_000_000L), lastCommandIdLo);
            await(client, client.addUser(TAKER), lastCommandIdLo);
            await(client, client.adjustBalance(TAKER, BASE, 1_000L), lastCommandIdLo);
            await(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), lastCommandIdLo);

            // -- match: a resting ask crossed by a taker bid --
            System.out.println("maker rests ask 10 @ 100; taker buys 6 -> one trade, one group");
            await(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER), lastCommandIdLo);
            await(client, client.placeGtc(SYM, 2L, false, 105L, 6L, 105L, TAKER), lastCommandIdLo);

            // -- sweep: one taker command crossing two price levels --
            System.out.println("maker rests ask 4 @ 101; taker buys 5 -> two fills in one group");
            await(client, client.placeGtc(SYM, 3L, true, 101L, 4L, 0L, MAKER), lastCommandIdLo);
            await(client, client.placeGtc(SYM, 4L, false, 101L, 5L, 105L, TAKER), lastCommandIdLo);

            // -- lifecycle: cancel the maker's remainder --
            System.out.println("maker cancels order 3 -> reduce event with price and completion");
            await(client, client.cancelOrder(SYM, 3L, MAKER), lastCommandIdLo);

            // -- no liquidity: a FOK-BUDGET bid against an empty book --
            System.out.println("taker FOK-BUDGET buy -> wholesale reject event");
            await(client, client.placeFokBudget(SYM, 5L, false, 100L, 5L, TAKER), lastCommandIdLo);

            // -- market data: rest a level and request the L2 snapshot --
            System.out.println("maker rests ask 2 @ 102; order-book request -> L2 snapshot");
            await(client, client.placeGtc(SYM, 6L, true, 102L, 2L, 0L, MAKER), lastCommandIdLo);
            await(client, client.requestOrderBook(SYM, TAKER), lastCommandIdLo);

            // Drain until every expected event has been observed on the egress.
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline
                    && (trades.get() < 3
                            || groups.get() < 2
                            || reduces.get() < 1
                            || rejects.get() < 1
                            || snapshots.get() < 1)) {
                client.poll();
                Thread.onSpinWait();
            }

            if (lastCode[0] != CommandResultCode.SUCCESS
                    || trades.get() < 3
                    || groups.get() < 2
                    || reduces.get() < 1
                    || rejects.get() < 1
                    || snapshots.get() < 1) {
                throw new IllegalStateException("example did not observe all events: trades=" + trades
                        + " groups=" + groups
                        + " reduces=" + reduces
                        + " rejects=" + rejects
                        + " snapshots=" + snapshots);
            }

            System.out.printf(
                    "observed: %d trades, %d groups, %d reduce, %d reject, %d l2 snapshot%n",
                    trades.get(), groups.get(), reduces.get(), rejects.get(), snapshots.get());
        }
    }

    /** Polls until the result for {@code commandIdLo} has arrived. */
    private static void await(final ExcClient client, final long commandIdLo, final long[] lastCommandIdLo) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastCommandIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("no result for commandIdLo=" + commandIdLo);
    }
}
