package io.justrade.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/**
 * System-level benchmark driven entirely through the HTTP/JSON + WebSocket
 * gateway. It submits the same deterministic {@link LoadWorkload} the
 * write/read SDK runners use, then cross-checks the replicated state through
 * every read endpoint, and finally exercises the remaining write/admin/WS
 * surfaces once each (move, IOC, FOK_BUDGET, suspend/resume, admin guard,
 * streaming).
 *
 * <p>Latency is closed-loop: one HTTP command is outstanding at a time, so the
 * histogram records the full client-observed round trip (HTTP + gateway pump +
 * Raft consensus + archive + read replica). Not the deterministic hot path:
 * this runner uses the system clock, heap allocation, and blocking HTTP, all of
 * which the core forbids.
 *
 * <pre>{@code
 * java -cp 'lib/*' io.justrade.bench.GatewayBenchRunner \
 *   --base-url=http://localhost:8080 --ops=10000 --users=100 --symbols=256 --admin-uid=811
 * }</pre>
 */
public final class GatewayBenchRunner {

    private static final int SYMBOL = LoadWorkload.SYMBOL;
    private static final int BASE = LoadWorkload.BASE_CURRENCY;
    private static final int QUOTE = LoadWorkload.QUOTE_CURRENCY;
    private static final int DEFAULT_TRADE_LIMIT = 4096;
    private static final int MAX_LEVELS = 32;
    private static final long SETTLE_TIMEOUT_MS = 3 * 60_000L;
    private static final long SETTLE_POLL_MS = 1_000L;
    private static final long HTTP_TIMEOUT_S = 30L;
    private static final long WS_FRAME_TIMEOUT_MS = 15_000L;

    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    private final String baseUrl;
    private final int ops;
    private final int users;
    private final long adminUid;
    private final String apiKey;
    private final int tradeLimit;
    private final LoadWorkload workload;

    private long loadElapsedNanos;

    private GatewayBenchRunner(
            final String baseUrl,
            final int ops,
            final int users,
            final int symbols,
            final long adminUid,
            final String apiKey,
            final int tradeLimit) {
        this.baseUrl = baseUrl;
        this.ops = ops;
        this.users = users;
        this.adminUid = adminUid;
        this.apiKey = (apiKey == null || apiKey.isBlank()) ? null : apiKey;
        this.tradeLimit = tradeLimit;
        this.workload = new LoadWorkload(ops, users, symbols);
    }

    public static void main(final String[] args) throws Exception {
        String baseUrl = "http://localhost:8080";
        int ops = 10_000;
        int users = 100;
        int symbols = 1;
        long adminUid = 811L;
        String apiKey = null;
        int tradeLimit = DEFAULT_TRADE_LIMIT;
        for (final String arg : args) {
            final int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("expected --key=value, got: " + arg);
            }
            switch (arg.substring(0, eq)) {
                case "--base-url" -> baseUrl = arg.substring(eq + 1);
                case "--ops" -> ops = Integer.parseInt(arg.substring(eq + 1));
                case "--users" -> users = Integer.parseInt(arg.substring(eq + 1));
                case "--symbols" -> symbols = Integer.parseInt(arg.substring(eq + 1));
                case "--admin-uid" -> adminUid = Long.parseLong(arg.substring(eq + 1));
                case "--api-key" -> apiKey = arg.substring(eq + 1);
                case "--trade-limit" -> tradeLimit = Integer.parseInt(arg.substring(eq + 1));
                default -> throw new IllegalArgumentException("unknown argument: " + arg.substring(0, eq));
            }
        }
        final GatewayBenchRunner runner =
                new GatewayBenchRunner(baseUrl, ops, users, symbols, adminUid, apiKey, tradeLimit);
        System.exit(runner.run() ? 0 : 1);
    }

    private boolean run() {
        final List<String> failures = new ArrayList<>();
        System.out.println("gateway bench: base=" + baseUrl + " ops=" + ops + " users=" + users + " symbols="
                + workload.symbols());

        if (!setup(failures)) {
            printReport(null, failures);
            return false;
        }

        final Histogram histogram = load(failures);

        final boolean settled = awaitSettled(failures);
        if (settled) {
            crossCheckReads(failures);
        }
        coverage(failures);

        printReport(histogram, failures);
        return failures.isEmpty();
    }

    // ---- setup (admin writes through the gateway) ----
    private boolean setup(final List<String> failures) {
        for (int symbolId = 1; symbolId <= workload.symbols(); symbolId++) {
            final JsonNode symbol = postAdmin(
                    "/api/v1/symbols",
                    String.format(
                            "{\"symbolId\":%d,\"baseCurrency\":%d,\"quoteCurrency\":%d,\"baseScaleK\":1,"
                                    + "\"quoteScaleK\":1,\"takerFee\":0,\"makerFee\":0}",
                            symbolId, BASE, QUOTE));
            if (!isSuccess(symbol)) {
                failures.add("setup addSymbol " + symbolId + " -> "
                        + symbol.path("resultCode").asText());
                return false;
            }
        }
        for (long uid = 1L; uid <= users; uid++) {
            final JsonNode added = postAdmin("/api/v1/users", String.format("{\"uid\":%d}", uid));
            if (!isSuccess(added)) {
                failures.add("setup addUser " + uid + " -> "
                        + added.path("resultCode").asText());
                return false;
            }
            final JsonNode base = postAdmin(
                    "/api/v1/users/" + uid + "/balance",
                    String.format("{\"currency\":%d,\"amount\":%d}", BASE, LoadWorkload.BASE_FUNDING_PER_USER));
            if (!isSuccess(base)) {
                failures.add("setup base balance uid " + uid + " -> "
                        + base.path("resultCode").asText());
                return false;
            }
            final JsonNode quote = postAdmin(
                    "/api/v1/users/" + uid + "/balance",
                    String.format("{\"currency\":%d,\"amount\":%d}", QUOTE, LoadWorkload.QUOTE_FUNDING_PER_USER));
            if (!isSuccess(quote)) {
                failures.add("setup quote balance uid " + uid + " -> "
                        + quote.path("resultCode").asText());
                return false;
            }
        }
        return true;
    }

    // ---- deterministic closed-loop load ----
    private Histogram load(final List<String> failures) {
        System.out.println("load: submitting " + ops + " commands closed-loop ...");
        final Histogram histogram = new Histogram(1L, 60_000_000_000L, 3);
        final long began = System.nanoTime();
        for (int i = 0; i < ops; i++) {
            final LoadWorkload.Command command = workload.next(i);
            final long t0 = System.nanoTime();
            final JsonNode result = send(buildCommand(command));
            histogram.recordValue(System.nanoTime() - t0);
            if (!isSuccess(result)) {
                failures.add("load i=" + i + " " + describe(command) + " -> "
                        + result.path("resultCode").asText());
            }
        }
        loadElapsedNanos = System.nanoTime() - began;
        return histogram;
    }

    private HttpRequest buildCommand(final LoadWorkload.Command command) {
        final int symbolId = command.symbolId();
        final long price = LoadWorkload.price(symbolId);
        return switch (command.type()) {
            case PLACE -> postReq(
                    "/api/v1/orders",
                    String.format(
                            "{\"symbolId\":%d,\"orderId\":%d,\"ask\":%b,\"type\":\"GTC\",\"price\":%d,"
                                    + "\"size\":1,\"reserveBidPrice\":%d,\"uid\":%d,\"userCookie\":0}",
                            symbolId,
                            command.orderId(),
                            command.ask(),
                            price,
                            command.reserveBidPrice(),
                            command.uid()));
            case CANCEL -> deleteReq(
                    "/api/v1/orders/" + command.orderId() + "?symbolId=" + symbolId + "&uid=" + command.uid());
            case REDUCE -> patchReq(
                    "/api/v1/orders/" + command.orderId(),
                    String.format("{\"symbolId\":%d,\"uid\":%d,\"size\":1}", symbolId, command.uid()));
            case ORDER_BOOK -> postReq(
                    "/api/v1/orderbook/" + symbolId + "/request", String.format("{\"uid\":%d}", command.uid()));
        };
    }

    private static String describe(final LoadWorkload.Command command) {
        return switch (command.type()) {
            case PLACE -> "PLACE " + (command.ask() ? "ask" : "bid") + " uid=" + command.uid() + " orderId="
                    + command.orderId();
            case CANCEL -> "CANCEL uid=" + command.uid() + " orderId=" + command.orderId();
            case REDUCE -> "REDUCE uid=" + command.uid() + " orderId=" + command.orderId();
            case ORDER_BOOK -> "ORDER_BOOK uid=" + command.uid();
        };
    }

    // ---- read-side cross-check (all read endpoints) ----
    private boolean awaitSettled(final List<String> failures) {
        final long deadline = System.currentTimeMillis() + SETTLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                final JsonNode report = get("/api/v1/users/1/balances");
                final JsonNode history = get("/api/v1/users/1/orders");
                if (report.path("exists").asBoolean()
                        && balanceOf(report, BASE) == workload.baseFree(1L)
                        && history.size() == workload.places(1L)) {
                    return true;
                }
                System.out.println("read replica still catching up ...");
            } catch (final RuntimeException e) {
                System.out.println("replica not ready: " + e.getMessage());
            }
            sleep(SETTLE_POLL_MS);
        }
        failures.add("read replica did not settle within " + SETTLE_TIMEOUT_MS + " ms");
        return false;
    }

    private void crossCheckReads(final List<String> failures) {
        final List<LoadWorkload.Resting> restingAsks = workload.restingAsks();
        final List<LoadWorkload.Resting> restingBids = workload.restingBids();

        for (long uid = 1L; uid <= users; uid++) {
            checkUser(uid, restingAsks, restingBids, failures);
        }
        checkL2(failures);
        checkTotals(failures);
        checkHealth(failures);
        checkRegistry(failures);
        checkMarketTrades(failures);
        checkOrderById(restingAsks, restingBids, failures);
    }

    private void checkUser(
            final long uid,
            final List<LoadWorkload.Resting> restingAsks,
            final List<LoadWorkload.Resting> restingBids,
            final List<String> failures) {
        final JsonNode report = get("/api/v1/users/" + uid + "/balances");
        if (!report.path("exists").asBoolean()) {
            failures.add("user " + uid + " does not exist on the replica");
            return;
        }
        if (report.path("suspended").asBoolean()) {
            failures.add("user " + uid + " is suspended");
        }
        final long base = balanceOf(report, BASE);
        final long quote = balanceOf(report, QUOTE);
        if (base != workload.baseFree(uid)) {
            failures.add("user " + uid + " base free " + base + " != expected " + workload.baseFree(uid));
        }
        if (quote != workload.quoteFree(uid)) {
            failures.add("user " + uid + " quote free " + quote + " != expected " + workload.quoteFree(uid));
        }

        final Map<Long, LoadWorkload.Resting> expected = new HashMap<>();
        for (final LoadWorkload.Resting order : restingAsks) {
            if (order.uid() == uid) {
                expected.put(order.orderId(), order);
            }
        }
        for (final LoadWorkload.Resting order : restingBids) {
            if (order.uid() == uid) {
                expected.put(order.orderId(), order);
            }
        }
        final Map<Long, JsonNode> actual = new HashMap<>();
        for (final JsonNode order : report.path("orders")) {
            actual.put(order.path("orderId").asLong(), order);
        }
        if (actual.size() != expected.size()) {
            failures.add("user " + uid + " resting orders " + actual.size() + " != expected " + expected.size());
        } else {
            for (final Map.Entry<Long, LoadWorkload.Resting> entry : expected.entrySet()) {
                final JsonNode got = actual.get(entry.getKey());
                final LoadWorkload.Resting want = entry.getValue();
                if (got == null
                        || !sideEquals(got.path("side").asText(), want.ask())
                        || got.path("price").asLong() != want.price()
                        || got.path("size").asLong() != want.size()
                        || got.path("filled").asLong() != 0L
                        || got.path("remaining").asLong() != want.size()) {
                    failures.add("user " + uid + " resting order " + entry.getKey() + " does not match simulation");
                }
            }
        }

        final JsonNode history = get("/api/v1/users/" + uid + "/orders");
        if (history.size() != workload.places(uid)) {
            failures.add(
                    "user " + uid + " order history " + history.size() + " != expected places " + workload.places(uid));
        }

        final JsonNode active = get("/api/v1/users/" + uid + "/orders/active");
        if (active.size() != expected.size()) {
            failures.add("user " + uid + " active orders " + active.size() + " != expected " + expected.size());
        }

        final JsonNode trades = get("/api/v1/users/" + uid + "/trades?limit=" + tradeLimit);
        if (trades.size() != workload.fills(uid)) {
            failures.add("user " + uid + " trade tape " + trades.size() + " != expected fills " + workload.fills(uid));
        } else {
            for (final JsonNode trade : trades) {
                final long price = trade.path("price").asLong();
                if (price < LoadWorkload.PRICE
                        || price >= LoadWorkload.PRICE + workload.symbols()
                        || trade.path("size").asLong() != 1L) {
                    failures.add("user " + uid + " unexpected trade " + trade);
                    break;
                }
            }
        }
    }

    private void checkL2(final List<String> failures) {
        for (int symbolId = 1; symbolId <= workload.symbols(); symbolId++) {
            final JsonNode book = get("/api/v1/orderbook?symbolId=" + symbolId + "&maxLevels=" + MAX_LEVELS);
            if (!book.path("found").asBoolean()) {
                failures.add("L2 snapshot for symbol " + symbolId + " not found");
                continue;
            }
            checkSide(
                    book.path("asks"), workload.restingAsks(symbolId), LoadWorkload.price(symbolId), "asks", failures);
            checkSide(
                    book.path("bids"), workload.restingBids(symbolId), LoadWorkload.price(symbolId), "bids", failures);
        }
    }

    private void checkSide(
            final JsonNode actual,
            final List<LoadWorkload.Resting> expected,
            final long price,
            final String side,
            final List<String> failures) {
        if (expected.isEmpty()) {
            if (actual.size() != 0) {
                failures.add("L2 " + side + " not empty: " + actual);
            }
            return;
        }
        if (actual.size() != 1) {
            failures.add("L2 " + side + " levels " + actual.size() + " != 1");
            return;
        }
        final JsonNode level = actual.get(0);
        long size = 0L;
        for (final LoadWorkload.Resting order : expected) {
            size += order.size();
        }
        if (level.path("price").asLong() != price
                || level.path("size").asLong() != size
                || level.path("orders").asInt() != expected.size()) {
            failures.add("L2 " + side + " mismatch: " + level);
        }
    }

    private void checkTotals(final List<String> failures) {
        final JsonNode totals = get("/api/v1/report/conservation").path("totals");
        checkTotal(totals, BASE, users * LoadWorkload.BASE_FUNDING_PER_USER, failures);
        checkTotal(totals, QUOTE, users * LoadWorkload.QUOTE_FUNDING_PER_USER, failures);
    }

    private void checkTotal(
            final JsonNode totals, final int currency, final long expected, final List<String> failures) {
        for (final JsonNode total : totals) {
            if (total.path("currency").asInt() == currency) {
                if (total.path("total").asLong() != expected) {
                    failures.add("currency " + currency + " conserved total "
                            + total.path("total").asLong() + " != expected " + expected);
                }
                if (total.path("fees").asLong() != 0L) {
                    failures.add("currency " + currency + " fees "
                            + total.path("fees").asLong() + " != 0");
                }
                return;
            }
        }
        failures.add("currency " + currency + " missing from conservation totals");
    }

    private void checkHealth(final List<String> failures) {
        final JsonNode health = get("/api/v1/health");
        if (!health.has("appliedPosition")) {
            failures.add("health missing appliedPosition");
        }
        if (!health.has("stateHash")) {
            failures.add("health missing stateHash");
        }
    }

    private void checkRegistry(final List<String> failures) {
        final JsonNode symbols = get("/api/v1/symbols");
        for (int symbolId = 1; symbolId <= workload.symbols(); symbolId++) {
            boolean found = false;
            for (final JsonNode symbol : symbols) {
                if (symbol.path("symbolId").asInt() == symbolId) {
                    found = true;
                }
            }
            if (!found) {
                failures.add("symbol " + symbolId + " missing from /symbols");
            }
        }
        boolean foundBase = false;
        boolean foundQuote = false;
        for (final JsonNode currency : get("/api/v1/currencies")) {
            final int id = currency.path("id").asInt();
            if (id == BASE) {
                foundBase = true;
            }
            if (id == QUOTE) {
                foundQuote = true;
            }
        }
        if (!foundBase || !foundQuote) {
            failures.add("currencies " + BASE + "/" + QUOTE + " missing from /currencies");
        }
    }

    private void checkMarketTrades(final List<String> failures) {
        long total = 0L;
        for (int symbolId = 1; symbolId <= workload.symbols(); symbolId++) {
            final JsonNode trades = get("/api/v1/markettrades?symbolId=" + symbolId + "&limit=" + tradeLimit);
            total += trades.size();
            for (final JsonNode trade : trades) {
                if (trade.path("price").asLong() != LoadWorkload.price(symbolId)
                        || trade.path("size").asLong() != 1L
                        || trade.path("symbolId").asInt() != symbolId) {
                    failures.add("market tape unexpected trade " + trade);
                    return;
                }
            }
        }
        if (total != workload.trades()) {
            failures.add("market tape total " + total + " != expected " + workload.trades());
        }
    }

    private void checkOrderById(
            final List<LoadWorkload.Resting> restingAsks,
            final List<LoadWorkload.Resting> restingBids,
            final List<String> failures) {
        final LoadWorkload.Resting sample =
                restingAsks.isEmpty() ? (restingBids.isEmpty() ? null : restingBids.get(0)) : restingAsks.get(0);
        if (sample == null) {
            return;
        }
        final JsonNode order = get("/api/v1/orders/" + sample.orderId());
        if (!"ACTIVE".equals(order.path("state").asText())
                || order.path("symbolId").asLong() != sample.symbolId()
                || order.path("remaining").asLong() != sample.size()
                || order.path("price").asLong() != sample.price()) {
            failures.add("order " + sample.orderId() + " by-id does not match simulation: " + order);
        }
    }

    // ---- coverage pass (remaining write/admin/WS surfaces, once each) ----
    private void coverage(final List<String> failures) {
        final long makerUid = users;
        final long takerUid = Math.max(1L, users - 1L);
        long orderId = 1_000_000L;

        // MOVE: place a resting ask above the book, move it, verify via GET.
        final JsonNode resting = post(
                "/api/v1/orders",
                String.format(
                        "{\"symbolId\":%d,\"orderId\":%d,\"ask\":true,\"type\":\"GTC\",\"price\":200,"
                                + "\"size\":1,\"reserveBidPrice\":0,\"uid\":%d,\"userCookie\":0}",
                        SYMBOL, orderId, makerUid));
        if (isSuccess(resting)) {
            final JsonNode moved = patch(
                    "/api/v1/orders/" + orderId,
                    String.format("{\"symbolId\":%d,\"uid\":%d,\"price\":201}", SYMBOL, makerUid));
            if (isSuccess(moved)) {
                if (!pollOrderPrice(orderId, 201L)) {
                    failures.add("coverage MOVE: order " + orderId + " did not settle at price 201");
                }
            } else {
                failures.add("coverage MOVE -> " + moved.path("resultCode").asText());
            }
        } else {
            failures.add("coverage MOVE place -> " + resting.path("resultCode").asText());
        }

        // IOC: cross a fresh resting ask with an IOC bid, assert filledSize.
        final long iocMaker = orderId + 1L;
        final long iocTaker = orderId + 2L;
        if (isSuccess(post(
                "/api/v1/orders",
                String.format(
                        "{\"symbolId\":%d,\"orderId\":%d,\"ask\":true,\"type\":\"GTC\",\"price\":200,"
                                + "\"size\":1,\"reserveBidPrice\":0,\"uid\":%d,\"userCookie\":0}",
                        SYMBOL, iocMaker, makerUid)))) {
            final JsonNode ioc = post(
                    "/api/v1/orders",
                    String.format(
                            "{\"symbolId\":%d,\"orderId\":%d,\"ask\":false,\"type\":\"IOC\",\"price\":200,"
                                    + "\"size\":1,\"reserveBidPrice\":200,\"uid\":%d,\"userCookie\":0}",
                            SYMBOL, iocTaker, takerUid));
            if (!isSuccess(ioc) || ioc.path("filledSize").asLong() != 1L) {
                failures.add("coverage IOC -> " + ioc.path("resultCode").asText() + " filledSize="
                        + ioc.path("filledSize").asLong());
            }
        }

        // FOK_BUDGET: cross a fresh resting ask with a sufficient budget.
        final long fokMaker = orderId + 3L;
        final long fokTaker = orderId + 4L;
        if (isSuccess(post(
                "/api/v1/orders",
                String.format(
                        "{\"symbolId\":%d,\"orderId\":%d,\"ask\":true,\"type\":\"GTC\",\"price\":400,"
                                + "\"size\":1,\"reserveBidPrice\":0,\"uid\":%d,\"userCookie\":0}",
                        SYMBOL, fokMaker, makerUid)))) {
            final JsonNode fok = post(
                    "/api/v1/orders",
                    String.format(
                            "{\"symbolId\":%d,\"orderId\":%d,\"ask\":false,\"type\":\"FOK_BUDGET\",\"price\":400,"
                                    + "\"size\":1,\"reserveBidPrice\":0,\"uid\":%d,\"userCookie\":0}",
                            SYMBOL, fokTaker, takerUid));
            if (!isSuccess(fok) || fok.path("filledSize").asLong() != 1L) {
                failures.add("coverage FOK_BUDGET -> " + fok.path("resultCode").asText() + " filledSize="
                        + fok.path("filledSize").asLong());
            }
        }

        // suspend / resume a funded user.
        final JsonNode suspend = postAdmin("/api/v1/users/" + makerUid + "/suspend", "");
        if (!isSuccess(suspend)) {
            failures.add("coverage suspend -> " + suspend.path("resultCode").asText());
        }
        final JsonNode resume = postAdmin("/api/v1/users/" + makerUid + "/resume", "");
        if (!isSuccess(resume)) {
            failures.add("coverage resume -> " + resume.path("resultCode").asText());
        }

        checkWebSocketTrade(makerUid, takerUid, failures);
        checkAdminGuard(failures);
    }

    private void checkWebSocketTrade(final long makerUid, final long takerUid, final List<String> failures) {
        final BlockingQueue<String> frames = new LinkedBlockingQueue<>();
        final WebSocket ws = client.newWebSocketBuilder()
                .buildAsync(URI.create(wsUrl()), new WebSocket.Listener() {
                    @Override
                    public void onOpen(final WebSocket webSocket) {
                        webSocket.request(1);
                    }

                    @Override
                    public CompletionStage<?> onText(
                            final WebSocket webSocket, final CharSequence data, final boolean last) {
                        frames.add(data.toString());
                        webSocket.request(1);
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .join();

        final long makerOrder = 1_000_200L;
        final long takerOrder = 1_000_201L;
        post(
                "/api/v1/orders",
                String.format(
                        "{\"symbolId\":%d,\"orderId\":%d,\"ask\":true,\"type\":\"GTC\",\"price\":500,"
                                + "\"size\":1,\"reserveBidPrice\":0,\"uid\":%d,\"userCookie\":0}",
                        SYMBOL, makerOrder, makerUid));
        post(
                "/api/v1/orders",
                String.format(
                        "{\"symbolId\":%d,\"orderId\":%d,\"ask\":false,\"type\":\"GTC\",\"price\":500,"
                                + "\"size\":1,\"reserveBidPrice\":500,\"uid\":%d,\"userCookie\":0}",
                        SYMBOL, takerOrder, takerUid));

        boolean sawTrade = false;
        final long deadline = System.currentTimeMillis() + WS_FRAME_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline && !sawTrade) {
            final String frame = poll(frames);
            if (frame != null && frame.contains("\"type\":\"TRADE\"")) {
                sawTrade = true;
            }
        }
        if (!sawTrade) {
            failures.add("coverage WS: no TRADE frame streamed over /ws");
        }
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
    }

    private void checkAdminGuard(final List<String> failures) {
        // Non-admin uid (with a valid key when one is configured) -> 403.
        final List<String> forbiddenHeaders = new ArrayList<>();
        forbiddenHeaders.add("X-User-Id");
        forbiddenHeaders.add("999999");
        if (apiKey != null) {
            forbiddenHeaders.add("X-Api-Key");
            forbiddenHeaders.add(apiKey);
        }
        final HttpResponse<String> forbidden =
                sendRaw(postReqWithHeaders("/api/v1/users", "{\"uid\":999999}", forbiddenHeaders));
        if (forbidden.statusCode() != 403) {
            failures.add("coverage admin guard non-admin uid -> " + forbidden.statusCode() + " (expected 403)");
        }

        // Malformed X-User-Id (with a valid key when one is configured) -> 400.
        final List<String> malformedHeaders = new ArrayList<>();
        malformedHeaders.add("X-User-Id");
        malformedHeaders.add("not-a-number");
        if (apiKey != null) {
            malformedHeaders.add("X-Api-Key");
            malformedHeaders.add(apiKey);
        }
        final HttpResponse<String> malformed =
                sendRaw(postReqWithHeaders("/api/v1/users", "{\"uid\":1}", malformedHeaders));
        if (malformed.statusCode() != 400) {
            failures.add("coverage admin guard malformed uid -> " + malformed.statusCode() + " (expected 400)");
        }

        // Missing X-Api-Key -> 401, only when a key is configured.
        if (apiKey != null) {
            final HttpResponse<String> missingKey = sendRaw(
                    postReqWithHeaders("/api/v1/users", "{\"uid\":1}", List.of("X-User-Id", Long.toString(adminUid))));
            if (missingKey.statusCode() != 401) {
                failures.add("coverage admin guard missing key -> " + missingKey.statusCode() + " (expected 401)");
            }
        }
    }

    private boolean pollOrderPrice(final long orderId, final long expectedPrice) {
        final long deadline = System.currentTimeMillis() + SETTLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                final JsonNode order = get("/api/v1/orders/" + orderId);
                if (order.path("price").asLong() == expectedPrice) {
                    return true;
                }
            } catch (final RuntimeException e) {
                // Replica not ready yet; keep polling.
            }
            sleep(200L);
        }
        return false;
    }

    // ---- HTTP helpers ----
    private JsonNode get(final String path) {
        return send(req(path).GET().build());
    }

    private JsonNode post(final String path, final String body) {
        return send(req(path)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build());
    }

    private JsonNode postAdmin(final String path, final String body) {
        return send(postReqWithHeaders(path, body, adminHeaders()));
    }

    private JsonNode patch(final String path, final String body) {
        return send(req(path)
                .header("Content-Type", "application/json")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(body))
                .build());
    }

    private JsonNode delete(final String path) {
        return send(req(path).DELETE().build());
    }

    private HttpRequest postReq(final String path, final String body) {
        return req(path)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private HttpRequest patchReq(final String path, final String body) {
        return req(path)
                .header("Content-Type", "application/json")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private HttpRequest deleteReq(final String path) {
        return req(path).DELETE().build();
    }

    private HttpRequest postReqWithHeaders(final String path, final String body, final List<String> headers) {
        final HttpRequest.Builder b = req(path).header("Content-Type", "application/json");
        for (int i = 0; i + 1 < headers.size(); i += 2) {
            b.header(headers.get(i), headers.get(i + 1));
        }
        return b.POST(HttpRequest.BodyPublishers.ofString(body)).build();
    }

    private List<String> adminHeaders() {
        if (apiKey == null) {
            return List.of("X-User-Id", Long.toString(adminUid));
        }
        return List.of("X-User-Id", Long.toString(adminUid), "X-Api-Key", apiKey);
    }

    private HttpRequest.Builder req(final String path) {
        return HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(HTTP_TIMEOUT_S));
    }

    private String wsUrl() {
        return baseUrl.replaceFirst("^http", "ws") + "/ws";
    }

    private JsonNode send(final HttpRequest request) {
        final HttpResponse<String> response = sendRaw(request);
        if (response.statusCode() != 200) {
            throw new IllegalStateException(
                    request.method() + " " + request.uri() + " -> " + response.statusCode() + " " + response.body());
        }
        try {
            return mapper.readTree(response.body());
        } catch (final IOException e) {
            throw new IllegalStateException("cannot parse response: " + e.getMessage(), e);
        }
    }

    private HttpResponse<String> sendRaw(final HttpRequest request) {
        try {
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (final IOException e) {
            throw new IllegalStateException("HTTP failed: " + e.getMessage(), e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    private static boolean isSuccess(final JsonNode result) {
        return "SUCCESS".equals(result.path("resultCode").asText());
    }

    private static boolean sideEquals(final String side, final boolean ask) {
        return ask ? "ASK".equals(side) : "BID".equals(side);
    }

    private static long balanceOf(final JsonNode report, final int currency) {
        for (final JsonNode balance : report.path("balances")) {
            if (balance.path("currency").asInt() == currency) {
                return balance.path("balance").asLong();
            }
        }
        return 0L;
    }

    private static String poll(final BlockingQueue<String> queue) {
        try {
            return queue.poll(1, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting", e);
        }
    }

    private void printReport(final Histogram histogram, final List<String> failures) {
        System.out.println();
        System.out.println("== gateway bench result ==");
        System.out.printf(
                "config:  base=%s ops=%d users=%d symbols=%d adminUid=%d%n",
                baseUrl, ops, users, workload.symbols(), adminUid);
        if (histogram != null) {
            final double opsPerSec = ops / (loadElapsedNanos / 1_000_000_000.0);
            System.out.printf("load:    ops=%d throughput=%.0f ops/s%n", ops, opsPerSec);
            System.out.printf(
                    "latency: p50=%.1fus p99=%.1fus p99.9=%.1fus max=%.1fus%n",
                    histogram.getValueAtPercentile(50.0) / 1000.0,
                    histogram.getValueAtPercentile(99.0) / 1000.0,
                    histogram.getValueAtPercentile(99.9) / 1000.0,
                    histogram.getMaxValue() / 1000.0);
        }
        System.out.printf("checks:  %d failure(s)%n", failures.size());
        for (final String failure : failures) {
            System.out.println("  FAIL: " + failure);
        }
        System.out.println(failures.isEmpty() ? "overall: PASS" : "overall: FAIL");
    }
}
