package com.exadbe.bench;

import com.exadbe.read.client.BalanceResult;
import com.exadbe.read.client.L2Snapshot;
import com.exadbe.read.client.MarketTradeResult;
import com.exadbe.read.client.OrderRecordResult;
import com.exadbe.read.client.QueryException;
import com.exadbe.read.client.ReadClient;
import com.exadbe.read.client.TotalBalanceResult;
import com.exadbe.read.client.UserReport;
import com.exadbe.read.client.config.ReadClientConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Verifies the read side of a deployed system after a load run: replays the
 * same deterministic {@link LoadWorkload} simulation the write-side runner
 * submitted, then queries a read replica over the read-client SDK and asserts
 * that the replicated state matches the simulation exactly - per-user free
 * balances, resting orders, order-history and trade counts, the L2 book, and
 * the value-conservation totals (zero fees, so every funded unit must still
 * exist).
 *
 * <p>The replica may still be replaying the log when this runner starts, so it
 * first waits until the replicated state settles (uid 1's base balance matches
 * the simulation) before asserting anything.
 *
 * <pre>{@code
 * java -cp 'lib/*' com.exadbe.bench.ReadVerifyRunner \
 *   --query=aeron:udp?endpoint=read-replica:44000 \
 *   --egress=aeron:udp?endpoint=<container-ip>:0 \
 *   --ops=100000 --users=100
 * }</pre>
 *
 * <p>{@code --egress} must advertise an address the read service can reach.
 */
public final class ReadVerifyRunner {

    private static final long SETTLE_TIMEOUT_MS = 3 * 60_000L;
    private static final long SETTLE_POLL_MS = 1_000L;
    private static final int MAX_LEVELS = 32;
    private static final int DEFAULT_TRADE_LIMIT = 4096;

    private ReadVerifyRunner() {}

    public static void main(final String[] args) throws Exception {
        String query = "aeron:udp?endpoint=localhost:44000";
        String egress = "aeron:udp?endpoint=localhost:0";
        int ops = 100_000;
        int users = 100;
        int symbols = 1;
        int tradeLimit = DEFAULT_TRADE_LIMIT;
        for (final String arg : args) {
            final int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("expected --key=value, got: " + arg);
            }
            switch (arg.substring(0, eq)) {
                case "--query" -> query = arg.substring(eq + 1);
                case "--egress" -> egress = arg.substring(eq + 1);
                case "--ops" -> ops = Integer.parseInt(arg.substring(eq + 1));
                case "--users" -> users = Integer.parseInt(arg.substring(eq + 1));
                case "--symbols" -> symbols = Integer.parseInt(arg.substring(eq + 1));
                case "--trade-limit" -> tradeLimit = Integer.parseInt(arg.substring(eq + 1));
                default -> throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }

        final LoadWorkload workload = new LoadWorkload(ops, users, symbols);
        for (int i = 0; i < workload.ops(); i++) {
            workload.next(i);
        }
        final boolean ok = verify(workload, query, egress, tradeLimit);
        System.exit(ok ? 0 : 1);
    }

    /** Queries the replica and returns whether the replicated state matches the simulation. */
    public static boolean verify(final LoadWorkload workload, final String query, final String egress) {
        return verify(workload, query, egress, DEFAULT_TRADE_LIMIT);
    }

    /** Variant of {@link #verify} with an explicit per-user trade-query limit. */
    public static boolean verify(
            final LoadWorkload workload, final String query, final String egress, final int tradeLimit) {
        final ReadClientConfig config = ReadClientConfig.builder()
                .requestChannel(query)
                .responseChannel(egress)
                .maxRetries(10)
                .build();
        final List<String> failures = new ArrayList<>();

        try (ReadClient client = new ReadClient(config)) {
            System.out.println("read verify: querying " + query + " ...");
            awaitSettled(client, workload, failures);

            for (long uid = 1L; uid <= workload.users(); uid++) {
                checkUser(client, workload, uid, tradeLimit, failures);
            }
            checkL2(client, workload, failures);
            checkTotals(client, workload, failures);

            System.out.println();
            System.out.println("== read-side verification ==");
            System.out.printf(
                    "stateHash=%d lastAppliedPosition=%d queries=%d expired=%d%n",
                    client.stateHash(), client.lastAppliedPosition(), client.completed(), client.expired());
            System.out.println("read-side checks: " + (failures.isEmpty() ? "PASS" : "FAIL (" + failures.size() + ")"));
            if (!failures.isEmpty()) {
                for (final String failure : failures) {
                    System.out.println("  FAIL: " + failure);
                }
            }
            return failures.isEmpty();
        }
    }

    private static void awaitSettled(
            final ReadClient client, final LoadWorkload workload, final List<String> failures) {
        final long deadline = System.currentTimeMillis() + SETTLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                final BalanceResult probe = client.balance(1L, LoadWorkload.BASE_CURRENCY);
                if (probe.found() && probe.balance() == workload.baseFree(1L)) {
                    return;
                }
                System.out.println("replica still catching up (base balance uid 1 = " + probe.balance() + ", expected "
                        + workload.baseFree(1L) + ")");
            } catch (final QueryException e) {
                System.out.println("replica not answering yet: " + e.getMessage());
            }
            sleep(SETTLE_POLL_MS);
        }
        failures.add("replica did not settle within " + SETTLE_TIMEOUT_MS + " ms");
    }

    private static void checkUser(
            final ReadClient client,
            final LoadWorkload workload,
            final long uid,
            final int tradeLimit,
            final List<String> failures) {
        final UserReport report = client.singleUserReport(uid);
        if (!report.exists()) {
            failures.add("user " + uid + " does not exist on the replica");
            return;
        }
        if (report.suspended()) {
            failures.add("user " + uid + " is suspended");
        }
        final long base = balanceOf(report, LoadWorkload.BASE_CURRENCY);
        final long quote = balanceOf(report, LoadWorkload.QUOTE_CURRENCY);
        if (base != workload.baseFree(uid)) {
            failures.add("user " + uid + " base free " + base + " != expected " + workload.baseFree(uid));
        }
        if (quote != workload.quoteFree(uid)) {
            failures.add("user " + uid + " quote free " + quote + " != expected " + workload.quoteFree(uid));
        }

        final Map<Long, UserReport.RestingOrder> resting = new HashMap<>();
        for (final UserReport.RestingOrder order : report.orders()) {
            resting.put(order.orderId(), order);
        }
        final Map<Long, LoadWorkload.Resting> expected = new HashMap<>();
        for (final LoadWorkload.Resting order : workload.restingBids()) {
            if (order.uid() == uid) {
                expected.put(order.orderId(), order);
            }
        }
        for (final LoadWorkload.Resting order : workload.restingAsks()) {
            if (order.uid() == uid) {
                expected.put(order.orderId(), order);
            }
        }
        if (resting.size() != expected.size()) {
            failures.add("user " + uid + " resting orders " + resting.size() + " != expected " + expected.size());
        } else {
            for (final Map.Entry<Long, LoadWorkload.Resting> entry : expected.entrySet()) {
                final UserReport.RestingOrder actual = resting.get(entry.getKey());
                final LoadWorkload.Resting want = entry.getValue();
                if (actual == null
                        || actual.ask() != want.ask()
                        || actual.price() != want.price()
                        || actual.size() != want.size()
                        || actual.filled() != 0L) {
                    failures.add("user " + uid + " resting order " + entry.getKey() + " does not match simulation");
                }
            }
        }

        final List<OrderRecordResult> history = client.orderHistory(uid);
        if (history.size() != workload.places(uid)) {
            failures.add(
                    "user " + uid + " order history " + history.size() + " != expected places " + workload.places(uid));
        }

        final List<MarketTradeResult> trades = client.userTrades(uid, tradeLimit);
        if (trades.size() != workload.fills(uid)) {
            failures.add("user " + uid + " trade tape " + trades.size() + " != expected fills " + workload.fills(uid));
        } else {
            for (final MarketTradeResult trade : trades) {
                if (trade.size() != 1L
                        || trade.price() < LoadWorkload.PRICE
                        || trade.price() >= LoadWorkload.PRICE + workload.symbols()) {
                    failures.add("user " + uid + " unexpected trade " + trade);
                    break;
                }
            }
        }
    }

    private static void checkL2(final ReadClient client, final LoadWorkload workload, final List<String> failures) {
        for (int symbolId = 1; symbolId <= workload.symbols(); symbolId++) {
            final L2Snapshot snapshot = client.orderBook(symbolId, MAX_LEVELS);
            if (!snapshot.found()) {
                failures.add("L2 snapshot for symbol " + symbolId + " not found");
                continue;
            }
            final List<L2Snapshot.Level> expectedAsks = aggregate(workload.restingAsks(symbolId), symbolId);
            final List<L2Snapshot.Level> expectedBids = aggregate(workload.restingBids(symbolId), symbolId);
            if (!snapshot.asks().equals(expectedAsks)) {
                failures.add("L2 asks for symbol " + symbolId + " " + snapshot.asks() + " != expected " + expectedAsks);
            }
            if (!snapshot.bids().equals(expectedBids)) {
                failures.add("L2 bids for symbol " + symbolId + " " + snapshot.bids() + " != expected " + expectedBids);
            }
        }
    }

    private static void checkTotals(final ReadClient client, final LoadWorkload workload, final List<String> failures) {
        final TotalBalanceResult totals = client.totalCurrencyBalance();
        checkTotal(totals, LoadWorkload.BASE_CURRENCY, workload.users() * LoadWorkload.BASE_FUNDING_PER_USER, failures);
        checkTotal(
                totals, LoadWorkload.QUOTE_CURRENCY, workload.users() * LoadWorkload.QUOTE_FUNDING_PER_USER, failures);
    }

    private static void checkTotal(
            final TotalBalanceResult totals, final int currency, final long expected, final List<String> failures) {
        for (final TotalBalanceResult.Total total : totals.totals()) {
            if (total.currency() == currency) {
                if (total.total() != expected) {
                    failures.add(
                            "currency " + currency + " conserved total " + total.total() + " != expected " + expected);
                }
                if (total.fees() != 0L) {
                    failures.add("currency " + currency + " fees " + total.fees() + " != 0");
                }
                return;
            }
        }
        failures.add("currency " + currency + " missing from conservation totals");
    }

    private static List<L2Snapshot.Level> aggregate(final List<LoadWorkload.Resting> resting, final int symbolId) {
        if (resting.isEmpty()) {
            return List.of();
        }
        long size = 0L;
        for (final LoadWorkload.Resting order : resting) {
            size += order.size();
        }
        return List.of(new L2Snapshot.Level(LoadWorkload.price(symbolId), size, resting.size()));
    }

    private static long balanceOf(final UserReport report, final int currency) {
        for (final UserReport.Balance balance : report.balances()) {
            if (balance.currency() == currency) {
                return balance.balance();
            }
        }
        return 0L;
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
