package com.exadbe.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Full browser feature suite for the bundled gateway UI, driven against an
 * EXTERNALLY started dev stack ({@code scripts/excoredum-dev.sh}) - a real
 * multi-process cluster + read replica + Netty gateway, seeded through the
 * gateway REST API.
 *
 * <p>This exercises the whole chain UI -> gateway REST/WS -> engine logic: the
 * order book, place (GTC / IOC / FOK_BUDGET), cancel / move / reduce, admin
 * actions (add symbol, add user, adjust balance, suspend, resume), the portfolio
 * report, and ops (health, counters, conservation, live egress events).
 *
 * <p>Tagged {@code uiStack}; orchestrated by {@code scripts/excoredum-ui-test.sh}
 * which starts/stops the stack around this suite. Opt-in: not wired into
 * {@code check}. Connect via {@code -Dexc.gateway.url=http://host:port}
 * (default {@code http://localhost:8080}).
 */
@Tag("uiStack")
class DevStackUiFeatureTest {

    private static final String BASE_URL = System.getProperty("exc.gateway.url", "http://localhost:8080");
    private static final URI BASE = URI.create(BASE_URL);
    private static final String ADMIN = "811"; // in gateway.admin.uids (script default: 1,2,811)
    private static final long MAKER = 811L;
    private static final long TAKER = 812L;
    private static final long NEW_USER = 813L;
    private static final int SYM = 1;
    private static final int BASE_CUR = 10;
    private static final int QUOTE_CUR = 20;
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final long POLL_MS = 40_000L;

    private static Playwright playwright;
    private static Browser browser;

    private Page page;
    private final ConcurrentLinkedDeque<String> promptValues = new ConcurrentLinkedDeque<>();

    // ---- lifecycle ---------------------------------------------------------
    @BeforeAll
    static void boot() throws Exception {
        assertReachable();
        seedBase();
    }

    @AfterAll
    static void shutdown() {
        if (browser != null) {
            browser.close();
        }
        if (playwright != null) {
            playwright.close();
        }
    }

    @BeforeEach
    void setUp() {
        if (playwright == null) {
            playwright = Playwright.create();
        }
        if (browser == null) {
            browser = playwright
                    .chromium()
                    .launch(new BrowserType.LaunchOptions()
                            .setHeadless(Boolean.parseBoolean(System.getProperty("exc.ui.headless", "true"))));
        }
    }

    @AfterEach
    void tearDown() {
        if (page != null) {
            page.close();
            page = null;
        }
    }

    // ---- seeding -----------------------------------------------------------
    private static void assertReachable() throws Exception {
        final long deadline = System.currentTimeMillis() + POLL_MS;
        for (; ; ) {
            try {
                final HttpResponse<String> r = HTTP.send(
                        HttpRequest.newBuilder(BASE.resolve("/api/v1/health"))
                                .timeout(Duration.ofSeconds(5))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() == 200) {
                    return;
                }
            } catch (final Exception ignored) {
                // stack still coming up
            }
            if (System.currentTimeMillis() > deadline) {
                fail("gateway not reachable at " + BASE_URL
                        + " - start it with ./scripts/excoredum-dev.sh start first");
            }
            Thread.sleep(500);
        }
    }

    private static void seedBase() throws Exception {
        // The config-driven registry names BTC/USDT (1) + ETH/USDT (2). Register
        // BOTH in the engine so their order books are non-empty and the read
        // replica reports found=true for them, then fund maker 811 and taker 812.
        // Idempotent: tolerate an already-seeded stack (DUPLICATE / USER_ALREADY_EXISTS).
        write("/api/v1/symbols", symbolJson(1, BASE_CUR, QUOTE_CUR), true, true);
        write("/api/v1/symbols", symbolJson(2, BASE_CUR, QUOTE_CUR), true, true);
        write("/api/v1/users", "{\"uid\":" + MAKER + "}", true, true);
        write("/api/v1/users/" + MAKER + "/balance", "{\"currency\":" + BASE_CUR + ",\"amount\":1000}", true);
        write("/api/v1/users/" + MAKER + "/balance", "{\"currency\":" + QUOTE_CUR + ",\"amount\":1000000}", true);
        write("/api/v1/users", "{\"uid\":" + TAKER + "}", true, true);
        write("/api/v1/users/" + TAKER + "/balance", "{\"currency\":" + QUOTE_CUR + ",\"amount\":1000000}", true);
        // Maker rests an ask 10 @ 100; a taker bid 6 @ 105 fills 6 of it, leaving
        // a 4 @ 100 ask so the Spot book and tape have content immediately.
        write("/api/v1/orders", orderJson(SYM, 900001L, true, "GTC", 100, 10, 0, MAKER, 1001), false);
        write("/api/v1/orders", orderJson(SYM, 900002L, false, "GTC", 105, 6, 105, TAKER, 1002), false);
        poll(
                () -> {
                    final String book = get("/api/v1/orderbook?symbolId=1&maxLevels=32");
                    return book.contains("\"price\":100") && book.contains("\"size\":4");
                },
                "read replica applied the seeded ask 4 @ 100");
    }

    // ---- helpers -----------------------------------------------------------
    private static String get(final String path) {
        final HttpResponse<String> r;
        try {
            r = HTTP.send(
                    HttpRequest.newBuilder(BASE.resolve(path))
                            .timeout(Duration.ofSeconds(10))
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted GET " + path, e);
        } catch (final java.io.IOException e) {
            throw new IllegalStateException("GET " + path + " failed: " + e.getMessage(), e);
        }
        assertEquals(200, r.statusCode(), () -> "GET " + path + " -> " + r.statusCode() + " " + r.body());
        return r.body();
    }

    private static String write(final String path, final String body, final boolean admin) throws Exception {
        return write(path, body, admin, false);
    }

    private static String write(final String path, final String body, final boolean admin, final boolean idempotent)
            throws Exception {
        final long deadline = System.currentTimeMillis() + POLL_MS;
        for (; ; ) {
            final HttpResponse<String> r = postRaw(path, body, admin);
            if (r.statusCode() == 200) {
                final String resp = r.body();
                if (resp.contains("\"resultCode\":\"SUCCESS\"")) {
                    return resp;
                }
                // Seeding must not fail if the stack was pre-seeded (e.g. via the
                // dev script's `seed` command or a prior non-clean run): a duplicate
                // symbol or user already carries the desired end state.
                if (idempotent
                        && (resp.contains("\"resultCode\":\"DUPLICATE\"")
                                || resp.contains("\"resultCode\":\"USER_ALREADY_EXISTS\""))) {
                    return resp;
                }
            }
            if (System.currentTimeMillis() > deadline) {
                fail("write " + path + " never succeeded: status=" + r.statusCode() + " body=" + r.body());
            }
            Thread.sleep(300);
        }
    }

    private static HttpResponse<String> postRaw(final String path, final String body, final boolean admin)
            throws Exception {
        final HttpRequest.Builder req = HttpRequest.newBuilder(BASE.resolve(path))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (admin) {
            req.header("X-User-Id", ADMIN);
        }
        return HTTP.send(req.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static void poll(final BooleanSupplier condition, final String message) throws Exception {
        final long deadline = System.currentTimeMillis() + POLL_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        fail("condition not met within " + POLL_MS + "ms: " + message);
    }

    private void pollPage(final BooleanSupplier condition, final String message) throws Exception {
        final long deadline = System.currentTimeMillis() + POLL_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (final Exception ignored) {
                // element not rendered yet
            }
            Thread.sleep(200);
        }
        fail("condition not met within " + POLL_MS + "ms on page " + page.url() + ": " + message);
    }

    private Page openPage() {
        page = browser.newPage();
        promptValues.clear();
        // Move / Reduce / Adjust-balance use window.prompt(); answer them in order.
        page.onDialog(dialog -> {
            final String value = promptValues.poll();
            dialog.accept(value != null ? value : "");
        });
        page.navigate(BASE_URL + "/");
        page.waitForSelector("#view .sec");
        return page;
    }

    private void gotoView(final String view) {
        page.dispatchEvent("button[data-view=\"" + view + "\"]", "click");
    }

    private void setUid(final long uid) {
        page.fill("#uid", String.valueOf(uid));
        page.locator("#uid").dispatchEvent("change");
    }

    private void waitForBook() {
        page.waitForSelector("#book-body .header");
    }

    // ---- JSON builders -----------------------------------------------------
    private static String symbolJson(final int id, final int base, final int quote) {
        return "{\"symbolId\":" + id
                + ",\"baseCurrency\":" + base
                + ",\"quoteCurrency\":" + quote
                + ",\"baseScaleK\":1,\"quoteScaleK\":1,\"takerFee\":0,\"makerFee\":0}";
    }

    private static String orderJson(
            final int symbolId,
            final long orderId,
            final boolean ask,
            final String type,
            final long price,
            final long size,
            final long reserve,
            final long uid,
            final long cookie) {
        return "{\"symbolId\":" + symbolId
                + ",\"orderId\":" + orderId
                + ",\"ask\":" + ask
                + ",\"type\":\"" + type + "\""
                + ",\"price\":" + price
                + ",\"size\":" + size
                + ",\"reserveBidPrice\":" + reserve
                + ",\"uid\":" + uid
                + ",\"userCookie\":" + cookie + "}";
    }

    // ---- tests -------------------------------------------------------------
    @Test
    @Timeout(90)
    void marketsViewListsSymbolsAndNavigatesToSpot() throws Exception {
        openPage();
        page.waitForSelector("#mk-body tr");
        final String text = page.locator("#mk-body").innerText();
        assertTrue(text.contains("BTC/USDT"), "markets should list BTC/USDT: " + text);
        assertTrue(text.contains("ETH/USDT"), "markets should list ETH/USDT: " + text);
        pollPage(() -> !page.locator("#mk-1-last").innerText().equals("—"), "last price should populate");

        page.locator("#mk-body [data-trade='1']").click();
        page.waitForSelector("#book-body .header");
        assertTrue(page.locator("h2:has-text('BTC/USDT')").count() > 0, "spot should open the selected symbol");
    }

    @Test
    @Timeout(90)
    void spotRendersOrderBookAndTape() throws Exception {
        openPage();
        gotoView("spot");
        waitForBook();
        // Seed a fresh ask so the rendered book is deterministic regardless of
        // what earlier tests left on the (accumulating) symbol 1 book.
        write("/api/v1/orders", orderJson(SYM, 900205L, true, "GTC", 150, 5, 0, MAKER, 1205), false);
        pollPage(() -> page.locator("#book-body").innerText().contains("150"), "rendered book shows the seeded ask");
        assertTrue(page.locator("#book-body .row.ask").count() > 0, "order book should render ask levels");
        assertTrue(page.locator("#price-field input").isVisible(), "place-order price field should be visible");
        page.waitForSelector("#tape-body tbody tr");
        assertTrue(page.locator("#tape-body tbody tr").count() > 0, "trade tape should render rows");
    }

    @Test
    @Timeout(90)
    void placeGtcViaTicketRestsAndRefreshesOrders() throws Exception {
        openPage();
        gotoView("spot");
        waitForBook();
        // A resting bid below the best ask (100) must not cross. Seed an ask far
        // above then place via the ticket.
        write("/api/v1/orders", orderJson(SYM, 900200L, true, "GTC", 200, 5, 0, MAKER, 1200), false);
        setUid(MAKER);
        final int before = page.locator("#orders-body tbody tr").count();
        page.fill("#price-field input", "90");
        page.fill("#size-field input", "2");
        page.locator("#side-buy").click();
        page.locator("#place").click();

        pollPage(() -> page.locator("#orders-body tbody tr").count() > before, "open orders gains the new order");
        assertTrue(page.locator("#orders-body").innerText().contains("90"), "open orders show the resting bid");
        pollPage(() -> page.locator("#book-body").innerText().contains("90"), "book renders the new bid level");
    }

    @Test
    @Timeout(90)
    void placeIocViaTicketCrossesAndStreamsTape() throws Exception {
        openPage();
        gotoView("spot");
        waitForBook();
        // Seed a fresh resting ask to guarantee the IOC has something to cross.
        write("/api/v1/orders", orderJson(SYM, 900210L, true, "GTC", 120, 5, 0, MAKER, 1210), false);
        setUid(TAKER);
        final int before = page.locator("#tape-body tbody tr").count();
        page.fill("#price-field input", "120");
        page.fill("#size-field input", "3");
        page.locator("#side-buy").click();
        page.locator("[data-type='IOC']").click();
        page.locator("#place").click();

        pollPage(() -> page.locator("#tape-body tbody tr").count() > before, "IOC fill streams a tape row");
        pollPage(() -> !page.locator("#toast").innerText().isEmpty(), "order submitted");
    }

    @Test
    @Timeout(90)
    void fokBudgetSelectableInTicket() throws Exception {
        openPage();
        gotoView("spot");
        waitForBook();
        // Seed a fresh ask so the FOK has depth and deterministically fills.
        write("/api/v1/orders", orderJson(SYM, 900225L, true, "GTC", 200, 5, 0, MAKER, 1225), false);
        page.locator("[data-type='FOK_BUDGET']").click();
        assertTrue(
                (Boolean) page.locator("[data-type='FOK_BUDGET']").evaluate("el => el.classList.contains('on')"),
                "FOK_BUDGET option should be selectable");
        setUid(TAKER);
        final int before = page.locator("#tape-body tbody tr").count();
        page.fill("#price-field input", "500");
        page.fill("#size-field input", "1");
        page.locator("#side-buy").click();
        page.locator("#place").click();
        // The budget (500) is large enough to fill 1 against the best ask, so a
        // fill streams a TRADE onto the tape (the FOK type goes end-to-end).
        pollPage(() -> page.locator("#tape-body tbody tr").count() > before, "FOK fill streams a tape row");
    }

    @Test
    @Timeout(90)
    void fokBudgetEngineSemanticsViaRest() throws Exception {
        // Use symbol 2 (ETH/USDT): no other test trades it, so its book is empty
        // and entirely controlled here -> deterministic fill / kill budget checks.
        final int sym2 = 2;
        write("/api/v1/orders", orderJson(sym2, 900220L, true, "GTC", 100, 5, 0, MAKER, 1220), false);
        // FOK buy 2 @ budget 200 (cost = 2 * 100 = 200) -> fills fully.
        final String fill = writeOnce(
                "/api/v1/orders", orderJson(sym2, 900221L, false, "FOK_BUDGET", 200, 2, 100, TAKER, 1221), false);
        assertTrue(fill.contains("\"filledSize\":2"), "FOK should fill when budget covers: " + fill);
        // FOK buy 10 @ budget 200; only 3 units remain (5 - 2) -> cost unavailable -> kill.
        final String kill = writeOnce(
                "/api/v1/orders", orderJson(sym2, 900222L, false, "FOK_BUDGET", 200, 10, 100, TAKER, 1222), false);
        assertTrue(kill.contains("\"filledSize\":0"), "FOK should kill when budget cannot cover: " + kill);
        poll(
                () -> !get("/api/v1/users/812/orders/active").contains("\"orderId\":900222"),
                "killed FOK must not rest in active orders");
    }

    @Test
    @Timeout(90)
    void cancelMoveReduceViaOpenOrders() throws Exception {
        openPage();
        gotoView("spot");
        waitForBook();
        // Keep bids below the best ask (100) so they rest; reserve covers a move.
        write("/api/v1/orders", orderJson(SYM, 900230L, false, "GTC", 90, 4, 90, MAKER, 1230), false);
        write("/api/v1/orders", orderJson(SYM, 900231L, false, "GTC", 85, 4, 90, MAKER, 1231), false);
        write("/api/v1/orders", orderJson(SYM, 900232L, false, "GTC", 80, 4, 90, MAKER, 1232), false);
        setUid(MAKER); // force panel refresh for 811's active orders
        pollPage(() -> page.locator("#orders-body [data-cancel='900230']").count() == 1, "orders appear in panel");

        // CANCEL 900230: the row disappears and the order leaves active orders.
        page.locator("#orders-body [data-cancel='900230']").click();
        pollPage(() -> page.locator("#orders-body [data-cancel='900230']").count() == 0, "cancelled row removed");
        poll(() -> !get("/api/v1/users/811/orders/active").contains("\"orderId\":900230"), "canceled order gone");

        // MOVE 900231 85 -> 88 (still below the best ask; reserve 90 covers it).
        promptValues.add("88");
        page.locator("#orders-body [data-move='900231']").click();
        poll(() -> get("/api/v1/orders/900231").contains("\"price\":88"), "moved order price is 88");

        // REDUCE 900232 4 -> 2 (delta reduce by 2). The read model keeps size at
        // its original value and accumulates `reduced`, so assert on that.
        promptValues.add("2");
        page.locator("#orders-body [data-reduce='900232']").click();
        poll(() -> get("/api/v1/orders/900232").contains("\"reduced\":2"), "reduced order reduced is 2");
    }

    @Test
    @Timeout(90)
    void portfolioReflectsBalancesReservationsHistoryTrades() throws Exception {
        openPage();
        setUid(MAKER);
        gotoView("portfolio");
        page.waitForSelector("#balance-panel table");
        final String balances = page.locator("#balance-panel").innerText();
        assertTrue(balances.contains("BTC") || balances.contains("USDT"), "balances render: " + balances);
        page.waitForSelector("#reserve-panel .counter-list");
        assertTrue(
                page.locator("#reserve-panel").innerText().contains("Bid holds")
                        || page.locator("#reserve-panel").innerText().contains("Ask holds"),
                "reservation breakdown renders");
        // Order history and trades tables render (may be empty -> empty placeholders).
        page.waitForSelector("#hist-panel");
        page.waitForSelector("#trades-panel");
    }

    @Test
    @Timeout(90)
    void adminAddsSymbolAndManagesUsers() throws Exception {
        openPage();
        // Admin routes require X-User-Id in the allow-list; the top-bar Admin
        // field supplies it (api.js only sends the header when store.adminUid set).
        page.fill("#adminUid", ADMIN);
        page.locator("#adminUid").dispatchEvent("change");
        gotoView("admin");
        page.waitForSelector("#symbols-panel table");
        // Add a symbol via the operator form (engine-side; not in the config list).
        page.locator("#open-add-symbol").click();
        page.fill("#sym-symbolId", "7");
        page.fill("#sym-baseCurrency", "30");
        page.fill("#sym-quoteCurrency", "40");
        page.fill("#sym-baseScaleK", "1");
        page.fill("#sym-quoteScaleK", "1");
        page.fill("#sym-makerFee", "0");
        page.fill("#sym-takerFee", "0");
        page.locator("#submit-symbol").click();
        pollPage(() -> page.locator("#toast").innerText().contains("addSymbol submitted"), "addSymbol success toast");

        // The engine creates an order book lazily on the first order, so prove the
        // new symbol is registered and tradable: fund maker in its currencies, rest
        // an ask on it, and confirm the read replica exposes its book.
        write("/api/v1/users/" + MAKER + "/balance", "{\"currency\":30,\"amount\":1000}", true);
        write("/api/v1/users/" + MAKER + "/balance", "{\"currency\":40,\"amount\":1000000}", true);
        write("/api/v1/orders", orderJson(7, 900300L, true, "GTC", 100, 5, 0, MAKER, 1300), false);
        poll(() -> get("/api/v1/orderbook?symbolId=7").contains("\"found\":true"), "new symbol book exposed");
        poll(() -> get("/api/v1/orderbook?symbolId=7").contains("\"price\":100"), "new symbol book shows the ask");

        // Admin guard: without X-User-Id the same route is rejected.
        final HttpResponse<String> denied = postRaw("/api/v1/symbols", symbolJson(8, 30, 40), false);
        assertTrue(
                denied.statusCode() == 400 || denied.statusCode() == 403,
                "admin guard should reject without header: " + denied.statusCode());

        // Users & status: create, fund, suspend, resume.
        page.fill("#target-uid", String.valueOf(NEW_USER));
        page.locator("#target-uid").dispatchEvent("change");
        pollPage(
                () -> page.locator("#user-result").innerText().contains(String.valueOf(NEW_USER)),
                "user panel loaded for new uid");
        page.dispatchEvent("[data-act='add']", "click");
        pollPage(
                () -> page.locator("#toast").innerText().contains("addUser " + NEW_USER + " submitted"),
                "addUser success toast");

        promptValues.add("20");
        promptValues.add("5000");
        page.dispatchEvent("[data-act='bal']", "click");
        pollPage(() -> page.locator("#user-result").innerText().contains("5,000"), "balance adjusted to 5,000");

        page.dispatchEvent("[data-act='suspend']", "click");
        pollPage(() -> page.locator("#user-result").innerText().contains("SUSPENDED"), "user suspended");

        page.dispatchEvent("[data-act='resume']", "click");
        pollPage(() -> page.locator("#user-result").innerText().contains("ACTIVE"), "user resumed");
    }

    @Test
    @Timeout(90)
    void opsShowsHealthCountersConservationAndLiveEvents() throws Exception {
        openPage();
        gotoView("ops");
        page.waitForSelector("#health-panel .counter-list");
        pollPage(() -> !page.locator("#health-panel").innerText().contains("—"), "health counters populate");
        pollPage(() -> page.locator("#cons-panel").innerText().contains("total"), "conservation renders");
        assertTrue(
                page.locator("#counters-panel").innerText().contains("read queries submitted"),
                "client counters render");

        // Live egress: place a crossing order while on the Ops view; the TRADE
        // event must stream over the WS into the browser's live-events table.
        write("/api/v1/orders", orderJson(SYM, 900240L, true, "GTC", 140, 5, 0, MAKER, 1240), false);
        final int before = page.locator("#events-panel tbody tr").count();
        write("/api/v1/orders", orderJson(SYM, 900241L, false, "GTC", 140, 2, 140, TAKER, 1241), false);
        pollPage(() -> page.locator("#events-panel tbody tr").count() > before, "TRADE streams into live events");
        assertTrue(page.locator("#events-panel").innerText().contains("TRADE"), "live-events table shows TRADE");
    }

    @Test
    @Timeout(90)
    void invalidTicketShowsErrorToastWithoutSubmitting() throws Exception {
        openPage();
        gotoView("spot");
        waitForBook();
        setUid(TAKER);
        final int beforeOrders = page.locator("#orders-body tbody tr").count();
        // Price left empty -> the ticket guard should reject before any request.
        page.fill("#size-field input", "1");
        page.locator("#place").click();
        pollPage(() -> page.locator("#toast").innerText().contains("required"), "missing price shows error toast");
        assertEquals(beforeOrders, page.locator("#orders-body tbody tr").count(), "no order should be placed");
    }

    // ---- writeOnce: a single write, returning the raw result ------------------
    private static String writeOnce(final String path, final String body, final boolean admin) throws Exception {
        final HttpResponse<String> r = postRaw(path, body, admin);
        assertEquals(200, r.statusCode(), () -> "POST " + path + " -> " + r.statusCode() + " " + r.body());
        return r.body();
    }
}
