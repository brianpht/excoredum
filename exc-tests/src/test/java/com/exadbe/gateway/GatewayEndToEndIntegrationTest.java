package com.exadbe.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end: a single-node cluster is seeded, a read replica follows it, and
 * the Netty HTTP gateway (read + write pumps) serves the UI-facing JSON API. The
 * test drives the gateway with the JDK {@link HttpClient} and asserts the value
 * conservation invariant is still visible through the HTTP boundary.
 */
@Tag("integration")
class GatewayEndToEndIntegrationTest {

    private static final long TIMEOUT_MS = 60_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 811L;
    private static final long TAKER = 812L;

    @Test
    @Timeout(180)
    void httpGatewayServesReadAndWrite(@TempDir final Path baseDir) throws Exception {
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
            // Maker rests an ask 10 @ 100; a taker bid at 105 fills 6 of it.
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
                // Wait for the replica to apply the seeded commands before querying through the gateway.
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
                        exercise(server.boundPort());
                    } finally {
                        server.stop();
                    }
                }
            } finally {
                stopServiceLoop(serviceThread);
            }
        }
    }

    private static void exercise(final int port) throws Exception {
        final HttpClient client = HttpClient.newHttpClient();

        // L2 order book: the maker's 4 remaining units still rest at price 100.
        final String book = get(client, port, "/api/v1/orderbook?symbolId=1&maxLevels=10");
        assertTrue(book.contains("\"price\":100"), book);
        assertTrue(book.contains("\"size\":4"), book);

        // Symbols are served from the config-driven registry.
        final String symbols = get(client, port, "/api/v1/symbols");
        assertTrue(symbols.contains("\"name\":\"BTC/USDT\""), symbols);

        // The config-driven currency registry names balances for the UI.
        final String currencies = get(client, port, "/api/v1/currencies");
        assertTrue(currencies.contains("\"code\":\"USDT\""), currencies);
        assertTrue(currencies.contains("\"scaleK\":1"), currencies);

        // Single-user report: the maker still holds 990 base available.
        final String report = get(client, port, "/api/v1/users/811/balances");
        assertTrue(report.contains("\"exists\":true"), report);
        assertTrue(report.contains("\"balance\":990"), report);

        // Value conservation is visible through the boundary: base total stays 1000.
        final String conservation = get(client, port, "/api/v1/report/conservation");
        assertTrue(conservation.contains("\"total\":1000"), conservation);

        // Health exposes the replica position and the deterministic state hash.
        final String health = get(client, port, "/api/v1/health");
        assertTrue(health.contains("\"appliedPosition\""), health);
        assertTrue(health.contains("\"stateHash\""), health);

        // WebSocket: open a subscriber, then place a crossing order and assert a
        // TRADE event streams back through the gateway's egress.
        final BlockingQueue<String> frames = new LinkedBlockingQueue<>();
        final WebSocket ws = openWebSocket(port, frames);
        // Write path: a taker GTC bid at 100 crosses the resting ask and fills.
        final String placed = post(
                client,
                port,
                "/api/v1/orders",
                "{\"symbolId\":1,\"orderId\":9,\"ask\":false,\"type\":\"GTC\",\"price\":100,"
                        + "\"size\":1,\"reserveBidPrice\":100,\"uid\":812,\"userCookie\":0}");
        assertTrue(placed.contains("\"resultCode\":\"SUCCESS\""), placed);

        String tradeFrame = null;
        final long deadline = System.currentTimeMillis() + 15_000L;
        while (System.currentTimeMillis() < deadline && tradeFrame == null) {
            final String frame = frames.poll(1, TimeUnit.SECONDS);
            if (frame != null && frame.contains("\"type\":\"TRADE\"")) {
                tradeFrame = frame;
            }
        }
        assertNotNull(tradeFrame, "no TRADE event streamed over the WebSocket");
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();

        // Admin write path: an admin (X-User-Id in the allow-list) adds a symbol.
        final String added = post(
                client,
                port,
                "/api/v1/symbols",
                "{\"symbolId\":5,\"baseCurrency\":30,\"quoteCurrency\":40,\"baseScaleK\":1,"
                        + "\"quoteScaleK\":1,\"takerFee\":0,\"makerFee\":0}",
                "811");
        assertTrue(added.contains("\"resultCode\":\"SUCCESS\""), added);

        // Admin guard: without the admin header the same route is rejected.
        final HttpResponse<String> denied = send(
                client,
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/v1/symbols"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .timeout(Duration.ofSeconds(10))
                        .build());
        assertTrue(
                denied.statusCode() == 400 || denied.statusCode() == 403,
                "admin guard should reject: " + denied.statusCode());
    }

    // ---- HTTP helpers ----
    private static String get(final HttpClient client, final int port, final String path) throws Exception {
        final HttpResponse<String> resp = send(
                client,
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                        .timeout(Duration.ofSeconds(10))
                        .GET()
                        .build());
        assertEquals(200, resp.statusCode(), () -> "GET " + path + " -> " + resp.statusCode() + " " + resp.body());
        return resp.body();
    }

    private static String post(final HttpClient client, final int port, final String path, final String body)
            throws Exception {
        return post(client, port, path, body, null);
    }

    private static String post(
            final HttpClient client, final int port, final String path, final String body, final String adminUid)
            throws Exception {
        final HttpRequest.Builder req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(10));
        if (adminUid != null) {
            req.header("X-User-Id", adminUid);
        }
        final HttpResponse<String> resp = send(client, req.build());
        assertEquals(200, resp.statusCode(), () -> "POST " + path + " -> " + resp.statusCode() + " " + resp.body());
        return resp.body();
    }

    private static HttpResponse<String> send(final HttpClient client, final HttpRequest request) throws Exception {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static WebSocket openWebSocket(final int port, final BlockingQueue<String> frames) {
        return HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(URI.create("ws://127.0.0.1:" + port + "/ws"), new WebSocket.Listener() {
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
    }

    // ---- replica service loop (mirrors ReadServiceLauncher) ----
    private static Thread startServiceLoop(final ExcReadReplica replica, final QueryResponder responder) {
        final Thread thread = new Thread(() -> {
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (!Thread.currentThread().isInterrupted()) {
                final int work = replica.poll() + responder.poll();
                idle.idle(work);
            }
        });
        thread.setDaemon(true);
        thread.setName("gateway-e2e-read-service");
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
