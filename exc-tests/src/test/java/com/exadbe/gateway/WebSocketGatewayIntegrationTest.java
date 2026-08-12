package com.exadbe.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.gateway.config.GatewayConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end WebSocket real-time flow against an in-process single-node
 * cluster: subscribe to ticks (journal-sourced trades), order updates for a
 * user (egress-sourced), an L2 snapshot request, unsubscribe, and error frames.
 * The wire format is plain JSON text frames over {@code /ticks-websocket} - no
 * STOMP framing.
 */
@Tag("integration")
class WebSocketGatewayIntegrationTest {

    private static final long TIMEOUT_MS = 60_000L;
    private static final String TRADE = "/syncTradeApi/v1";
    private static final String ADMIN = "/syncAdminApi/v1";

    @Test
    @Timeout(180)
    void websocketStreamsTicksOrderUpdatesAndSnapshots(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {
            final String replicaDir =
                    baseDir.resolve("gateway-replica").resolve("driver").toString();
            final GatewayConfig gatewayConfig = GatewayConfig.builder(
                            99L, ClusterConfig.ingressEndpoints(1), clusterConfig.archiveControlChannel(), replicaDir)
                    .port(0)
                    .gatewayId(7)
                    .build();

            try (RestGateway gateway = RestGateway.launch(gatewayConfig)) {
                final String base = "http://localhost:" + gateway.boundPort();
                final String wsBase = "ws://localhost:" + gateway.boundPort();
                final HttpClient http = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .build();

                adminSetup(http, base);

                try (WsClient ws = new WsClient(wsBase + "/ticks-websocket")) {
                    // -- subscribe to ticks: ack, then a resting maker crossed by a taker --
                    ws.send("{\"op\":\"subscribe\",\"channel\":\"ticks\",\"symbol\":\"BTCUSD\"}");
                    assertAck(ws.awaitFrame(TIMEOUT_MS), "subscribe", "ticks");

                    placeOrder(
                            http, base, 1L, "{\"price\":\"100\",\"size\":10,\"action\":\"ASK\",\"orderType\":\"GTC\"}");
                    placeOrder(
                            http, base, 2L, "{\"price\":\"100\",\"size\":4,\"action\":\"BID\",\"orderType\":\"GTC\"}");

                    final String tick = awaitContains(ws, "\"type\":\"tick\"");
                    assertTrue(tick.contains("\"symbol\":\"BTCUSD\""), tick);
                    assertTrue(tick.contains("\"price\":100.00"), tick);
                    assertTrue(tick.contains("\"volume\":4"), tick);

                    // -- subscribe to order updates for the taker --
                    ws.send("{\"op\":\"subscribe\",\"channel\":\"orders\",\"uid\":2}");
                    assertAck(ws.awaitFrame(TIMEOUT_MS), "subscribe", "orders");

                    placeOrder(
                            http, base, 2L, "{\"price\":\"100\",\"size\":1,\"action\":\"BID\",\"orderType\":\"GTC\"}");
                    final String orderUpdate = awaitContains(ws, "\"type\":\"orderUpdate\"");
                    assertTrue(orderUpdate.contains("\"uid\":2"), orderUpdate);
                    assertTrue(orderUpdate.contains("\"symbol\":\"BTCUSD\""), orderUpdate);
                    assertTrue(orderUpdate.contains("\"state\":\"COMPLETED\""), orderUpdate);

                    // -- L2 snapshot request --
                    ws.send("{\"op\":\"orderBook\",\"symbol\":\"BTCUSD\",\"depth\":5}");
                    final String book = awaitContains(ws, "\"type\":\"orderBook\"");
                    assertTrue(book.contains("\"askPrices\":[100.00]"), book);

                    // -- unsubscribe ticks: a later trade must not produce a tick --
                    ws.send("{\"op\":\"unsubscribe\",\"channel\":\"ticks\",\"symbol\":\"BTCUSD\"}");
                    placeOrder(
                            http, base, 2L, "{\"price\":\"100\",\"size\":1,\"action\":\"BID\",\"orderType\":\"GTC\"}");
                    assertNoTickFrames(ws, 700L);

                    // -- error frames --
                    ws.send("{\"op\":\"subscribe\",\"channel\":\"ticks\",\"symbol\":\"NOPE\"}");
                    final String unknownSymbol = awaitContains(ws, "\"type\":\"error\"");
                    assertTrue(unknownSymbol.contains("\"code\":1007"), unknownSymbol);

                    ws.send("this is not json");
                    final String malformed = awaitContains(ws, "\"type\":\"error\"");
                    assertTrue(malformed.contains("\"code\":2000"), malformed);
                }
            }
        }
    }

    private static void adminSetup(final HttpClient http, final String base) throws Exception {
        assertEquals(
                201,
                post(http, base + ADMIN + "/assets", "{\"assetCode\":\"BTC\",\"assetId\":10,\"scale\":2}")
                        .statusCode());
        assertEquals(
                201,
                post(http, base + ADMIN + "/assets", "{\"assetCode\":\"USD\",\"assetId\":20,\"scale\":2}")
                        .statusCode());
        assertEquals(
                201,
                post(
                                http,
                                base + ADMIN + "/symbols",
                                "{\"symbolCode\":\"BTCUSD\",\"symbolId\":1,\"baseAsset\":\"BTC\","
                                        + "\"quoteCurrency\":\"USD\",\"lotSize\":\"1\",\"stepSize\":\"0.01\","
                                        + "\"takerFee\":\"0\",\"makerFee\":\"0\"}")
                        .statusCode());
        assertEquals(201, post(http, base + ADMIN + "/users", "{\"uid\":1}").statusCode());
        assertEquals(201, post(http, base + ADMIN + "/users", "{\"uid\":2}").statusCode());
        for (final long uid : new long[] {1L, 2L}) {
            assertEquals(
                    201,
                    post(
                                    http,
                                    base + ADMIN + "/users/" + uid + "/accounts",
                                    "{\"transactionId\":1,\"currency\":\"BTC\",\"amount\":\"1000\"}")
                            .statusCode());
            assertEquals(
                    201,
                    post(
                                    http,
                                    base + ADMIN + "/users/" + uid + "/accounts",
                                    "{\"transactionId\":2,\"currency\":\"USD\",\"amount\":\"100000\"}")
                            .statusCode());
        }
    }

    private static void placeOrder(final HttpClient http, final String base, final long uid, final String body)
            throws Exception {
        assertEquals(
                201,
                post(http, base + TRADE + "/symbols/BTCUSD/trade/" + uid + "/orders", body)
                        .statusCode());
    }

    private static void assertAck(final String frame, final String op, final String channel) {
        assertNotNull(frame, "expected ack frame");
        assertTrue(frame.contains("\"type\":\"ack\""), frame);
        assertTrue(frame.contains("\"op\":\"" + op + "\""), frame);
        assertTrue(frame.contains("\"channel\":\"" + channel + "\""), frame);
    }

    private static String awaitContains(final WsClient ws, final String needle) throws Exception {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            final String frame = ws.awaitFrame(250L);
            if (frame != null && frame.contains(needle)) {
                return frame;
            }
        }
        throw new AssertionError("timed out waiting for frame containing '" + needle + "'");
    }

    private static void assertNoTickFrames(final WsClient ws, final long windowMs) throws Exception {
        final long deadline = System.currentTimeMillis() + windowMs;
        while (System.currentTimeMillis() < deadline) {
            final String frame = ws.awaitFrame(deadline - System.currentTimeMillis());
            if (frame != null && frame.contains("\"type\":\"tick\"")) {
                throw new AssertionError("unexpected tick frame after unsubscribe: " + frame);
            }
        }
    }

    private static HttpResponse<String> post(final HttpClient http, final String url, final String body)
            throws Exception {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /** Minimal WebSocket client collecting text frames into a blocking queue. */
    private static final class WsClient implements AutoCloseable {
        private final WebSocket socket;
        private final BlockingQueue<String> frames = new LinkedBlockingQueue<>();

        WsClient(final String uri) throws Exception {
            final java.util.concurrent.CompletableFuture<WebSocket> opened =
                    new java.util.concurrent.CompletableFuture<>();
            final WebSocket.Listener listener = new WebSocket.Listener() {
                @Override
                public void onOpen(final WebSocket webSocket) {
                    opened.complete(webSocket);
                    webSocket.request(1);
                }

                @Override
                public CompletionStage<?> onText(
                        final WebSocket webSocket, final CharSequence data, final boolean last) {
                    if (last) {
                        frames.add(data.toString());
                    }
                    webSocket.request(1);
                    return null;
                }
            };
            HttpClient.newHttpClient()
                    .newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .buildAsync(URI.create(uri), listener)
                    .get(10, TimeUnit.SECONDS);
            this.socket = opened.get(10, TimeUnit.SECONDS);
        }

        void send(final String json) {
            socket.sendText(json, true).join();
        }

        String awaitFrame(final long timeoutMs) throws InterruptedException {
            return frames.poll(timeoutMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            socket.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
        }
    }
}
