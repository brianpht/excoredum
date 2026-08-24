package com.exadbe.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.gateway.config.GatewayConfig;
import com.exadbe.gateway.http.HttpServer;
import com.exadbe.gateway.http.Router;
import com.exadbe.gateway.read.ReadPump;
import com.exadbe.gateway.stream.StreamBroadcaster;
import com.exadbe.gateway.write.WritePump;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.read.ExcReadReplica;
import com.exadbe.read.QueryResponder;
import com.exadbe.read.client.config.ReadClientConfig;
import com.exadbe.read.config.ReadReplicaConfig;
import com.exadbe.write.client.ExcClient;
import com.exadbe.write.client.ResultHandler;
import com.exadbe.write.client.config.ClientConfig;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Browser smoke for the bundled gateway UI. Boots the same in-process cluster +
 * read replica + gateway as {@link GatewayEndToEndIntegrationTest}, then drives
 * headless Chromium through Playwright: the shell loads, the Spot view renders
 * the seeded order book, and a crossing order placed through the gateway's HTTP
 * write path streams back over the WebSocket into the browser's trade tape.
 *
 * <p>Opt-in (tag {@code ui}): requires browser binaries, so it is not wired into
 * the default {@code check} gate. Install them once with:
 * {@code ./gradlew :exc-tests:installPlaywrightBrowsers}.
 */
@Tag("ui")
class GatewayUiSmokeTest {

    private static final long TIMEOUT_MS = 60_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 811L;
    private static final long TAKER = 812L;

    @Test
    @Timeout(180)
    void spotViewRendersAndStreamsATrade(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);
        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {
            seed(clusterConfig);
            runGateway(clusterConfig, baseDir);
        }
    }

    private static void seed(final ClusterConfig clusterConfig) {
        final long[] lastIdLo = {Long.MIN_VALUE};
        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> lastIdLo[0] = idLo;
        final ClientConfig clientConfig =
                ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();
        try (ExcClient client = new ExcClient(clientConfig, handler)) {
            await(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastIdLo);
            await(client, client.addUser(MAKER), lastIdLo);
            await(client, client.adjustBalance(MAKER, BASE, 1_000L), lastIdLo);
            await(client, client.adjustBalance(MAKER, QUOTE, 1_000_000L), lastIdLo);
            await(client, client.addUser(TAKER), lastIdLo);
            await(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), lastIdLo);
            // A resting ask 10 @ 100; a taker bid 6 @ 105 fills 6, leaving 4 @ 100.
            await(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER, 111), lastIdLo);
            await(client, client.placeGtc(SYM, 2L, false, 105L, 6L, 105L, TAKER, 222), lastIdLo);
        }
    }

    private static void runGateway(final ClusterConfig clusterConfig, final Path baseDir) throws Exception {
        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                baseDir.resolve("replica").resolve("driver").toString(), clusterConfig.archiveControlChannel());
        try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults());
                QueryResponder responder = new QueryResponder(replica, replicaConfig)) {
            final Thread serviceThread = startServiceLoop(replica, responder);
            try {
                pollUntil(() -> replica.order(1L) != null && replica.order(2L) != null);

                final GatewayConfig gatewayConfig = GatewayConfig.builder()
                        .httpHost("127.0.0.1")
                        .httpPort(0)
                        .writeClientId(7L)
                        .writeIngressEndpoints(ClusterConfig.ingressEndpoints(1))
                        .adminUid(MAKER)
                        .symbol(new GatewayConfig.Symbol(SYM, "BTC/USDT", BASE, QUOTE, 1L, 1L, 0L, 0L))
                        .currency(new GatewayConfig.Currency(BASE, "BTC", 1L))
                        .currency(new GatewayConfig.Currency(QUOTE, "USDT", 1L))
                        .build();
                final StreamBroadcaster broadcaster = new StreamBroadcaster();
                try (ReadPump read = new ReadPump(ReadClientConfig.builder().build());
                        WritePump write = new WritePump(
                                ClientConfig.builder(7L, ClusterConfig.ingressEndpoints(1))
                                        .maxRetries(5)
                                        .build(),
                                broadcaster)) {
                    final Router router = new Router(read, write, gatewayConfig);
                    final HttpServer server =
                            new HttpServer(gatewayConfig.httpHost(), gatewayConfig.httpPort(), router, broadcaster);
                    server.start();
                    try {
                        browserSmoke(server.boundPort());
                    } finally {
                        server.stop();
                    }
                }
            } finally {
                stopServiceLoop(serviceThread);
            }
        }
    }

    private static void browserSmoke(final int port) throws Exception {
        final HttpClient http = HttpClient.newHttpClient();
        try (Playwright playwright = Playwright.create();
                Browser browser = playwright
                        .chromium()
                        .launch(new BrowserType.LaunchOptions()
                                .setHeadless(Boolean.parseBoolean(System.getProperty("exc.ui.headless", "true"))))) {
            final Page page = browser.newPage();

            // Load the shell and wait for the SPA to boot (the Markets view renders
            // a .sec section only after app.js attaches the nav handler), so the
            // nav drive below is not racing module load.
            page.navigate("http://127.0.0.1:" + port + "/");
            page.waitForSelector("#view .sec");

            // Spot view renders the seeded order book and the trade tape. Wait for
            // the mid/header row (created once the L2 snapshot arrives via REST/WS)
            // rather than the (initially empty) container, which is not "visible".
            page.dispatchEvent("button[data-view=\"spot\"]", "click");
            page.waitForSelector("#book-body .header");
            page.waitForSelector("#tape-body");
            page.waitForSelector("#price-field input");
            final String bookText = page.locator("#book-body").innerText();
            assertTrue(bookText.contains("100"), "seeded ask price should appear: " + bookText);

            // Portfolio resolves user 811's balances via the read replica.
            page.fill("#uid", String.valueOf(MAKER));
            page.locator("#uid").dispatchEvent("change");
            page.dispatchEvent("button[data-view=\"portfolio\"]", "click");
            page.waitForSelector("#balance-panel");
            final String balanceText = page.locator("#balance-panel").innerText();
            assertTrue(
                    balanceText.contains("USDT") || balanceText.contains("BTC"),
                    "balances should render: " + balanceText);

            // Back to Spot, then a crossing order through the gateway write path.
            page.dispatchEvent("button[data-view=\"spot\"]", "click");
            page.waitForSelector("#tape-body");
            final int before = page.locator("#tape-body tbody tr").count();
            final String placed = post(
                    http,
                    port,
                    "/api/v1/orders",
                    "{\"symbolId\":1,\"orderId\":999,\"ask\":false,\"type\":\"GTC\",\"price\":100,"
                            + "\"size\":1,\"reserveBidPrice\":100,\"uid\":812,\"userCookie\":0}");
            assertTrue(placed.contains("\"resultCode\":\"SUCCESS\""), placed);

            // The TRADE event must stream over the WS into the browser's tape.
            final long deadline = System.currentTimeMillis() + 15_000L;
            while (System.currentTimeMillis() < deadline
                    && page.locator("#tape-body tbody tr").count() <= before) {
                page.waitForTimeout(500);
            }
            assertTrue(
                    page.locator("#tape-body tbody tr").count() > before,
                    "WS TRADE should append a tape row (before=" + before + ")");
        }
    }

    // ---- HTTP helpers ----
    private static String post(final HttpClient client, final int port, final String path, final String body)
            throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(10))
                .build();
        final HttpResponse<String> resp = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), () -> "POST " + path + " -> " + resp.statusCode() + " " + resp.body());
        return resp.body();
    }

    // ---- replica service loop (mirrors GatewayEndToEndIntegrationTest) ----
    private static Thread startServiceLoop(final ExcReadReplica replica, final QueryResponder responder) {
        final Thread thread = new Thread(() -> {
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (!Thread.currentThread().isInterrupted()) {
                final int work = replica.poll() + responder.poll();
                idle.idle(work);
            }
        });
        thread.setDaemon(true);
        thread.setName("gateway-ui-read-service");
        thread.start();
        return thread;
    }

    private static void stopServiceLoop(final Thread thread) {
        thread.interrupt();
        try {
            thread.join(5_000L);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void pollUntil(final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("replica never reached the expected state");
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
        throw new AssertionError("no result for commandIdLo=" + commandIdLo);
    }
}
