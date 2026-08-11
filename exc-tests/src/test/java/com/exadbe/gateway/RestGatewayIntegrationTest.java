package com.exadbe.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.gateway.config.GatewayConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end gateway flow against an in-process single-node cluster: admin
 * setup (assets, symbol, users, balances), order placement and lifecycle,
 * eventually-consistent reads from the embedded replica, and error paths.
 */
@Tag("integration")
class RestGatewayIntegrationTest {

    private static final long TIMEOUT_MS = 60_000L;
    private static final String TRADE = "/syncTradeApi/v1";
    private static final String ADMIN = "/syncAdminApi/v1";

    @Test
    @Timeout(180)
    void restGatewayServesTradeAndAdminApis(@TempDir final Path baseDir) throws Exception {
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
                final HttpClient http = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .build();

                // -- misc endpoints --
                final HttpResponse<String> ping = get(http, base + TRADE + "/ping");
                assertEquals(200, ping.statusCode());
                assertTrue(ping.body().contains("\"gatewayResultCode\":0"), ping.body());

                final HttpResponse<String> time = get(http, base + TRADE + "/time");
                assertEquals(200, time.statusCode());
                assertTrue(time.body().contains("isoTime"), time.body());

                // -- admin: assets --
                assertEquals(
                        201,
                        post(http, base + ADMIN + "/assets", "{\"assetCode\":\"BTC\",\"assetId\":10,\"scale\":2}")
                                .statusCode());
                assertEquals(
                        201,
                        post(http, base + ADMIN + "/assets", "{\"assetCode\":\"USD\",\"assetId\":20,\"scale\":2}")
                                .statusCode());
                final HttpResponse<String> duplicateAsset =
                        post(http, base + ADMIN + "/assets", "{\"assetCode\":\"BTC\",\"assetId\":11,\"scale\":2}");
                assertEquals(400, duplicateAsset.statusCode());
                assertTrue(duplicateAsset.body().contains("\"gatewayResultCode\":1003"), duplicateAsset.body());

                // -- admin: symbol --
                final HttpResponse<String> addSymbol = post(
                        http,
                        base + ADMIN + "/symbols",
                        "{\"symbolCode\":\"BTCUSD\",\"symbolId\":1,\"baseAsset\":\"BTC\","
                                + "\"quoteCurrency\":\"USD\",\"lotSize\":\"1\",\"stepSize\":\"0.01\","
                                + "\"takerFee\":\"0\",\"makerFee\":\"0\"}");
                assertEquals(201, addSymbol.statusCode(), addSymbol.body());
                assertTrue(addSymbol.body().contains("\"status\":\"ACTIVE\""), addSymbol.body());

                final HttpResponse<String> info = get(http, base + TRADE + "/info");
                assertEquals(200, info.statusCode());
                assertTrue(info.body().contains("BTCUSD"), info.body());
                assertTrue(info.body().contains("BTC"), info.body());

                // -- admin: users and balances --
                assertEquals(
                        201, post(http, base + ADMIN + "/users", "{\"uid\":1}").statusCode());
                assertEquals(
                        201, post(http, base + ADMIN + "/users", "{\"uid\":2}").statusCode());
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

                // -- reads are eventually consistent: wait for the replica --
                awaitContains(http, base + TRADE + "/users/1/state", 200, "\"balance\":1000.00");

                // -- place a resting maker ask --
                final HttpResponse<String> placeMaker = post(
                        http,
                        base + TRADE + "/symbols/BTCUSD/trade/1/orders",
                        "{\"price\":\"100\",\"size\":10,\"action\":\"ASK\",\"orderType\":\"GTC\"}");
                assertEquals(201, placeMaker.statusCode(), placeMaker.body());
                final long makerOrderId = extractLong(placeMaker.body(), "orderId");
                assertTrue(makerOrderId > 0, placeMaker.body());
                assertTrue(placeMaker.body().contains("\"state\":\"ACTIVE\""), placeMaker.body());

                // -- order book shows the resting ask (replica catches up) --
                awaitContains(http, base + TRADE + "/symbols/BTCUSD/orderbook?depth=10", 200, "\"askPrices\":[100.00]");
                final HttpResponse<String> book = get(http, base + TRADE + "/symbols/BTCUSD/orderbook?depth=10");
                assertTrue(book.body().contains("\"askVolumes\":[10]"), book.body());
                assertTrue(book.body().contains("\"bidPrices\":[]"), book.body());

                // -- taker crosses 4 of the 10 units --
                final HttpResponse<String> placeTaker = post(
                        http,
                        base + TRADE + "/symbols/BTCUSD/trade/2/orders",
                        "{\"price\":\"100\",\"size\":4,\"action\":\"BID\",\"orderType\":\"GTC\"}");
                assertEquals(201, placeTaker.statusCode(), placeTaker.body());

                // taker bought 4 BTC at 100: BTC 1000 -> 1004, USD 100000 -> 99600
                awaitContains(http, base + TRADE + "/users/2/state", 200, "\"balance\":1004.00");
                final HttpResponse<String> takerState = get(http, base + TRADE + "/users/2/state");
                assertTrue(takerState.body().contains("\"balance\":99600.00"), takerState.body());

                // maker remainder rests at 100 x 6
                awaitContains(http, base + TRADE + "/symbols/BTCUSD/orderbook?depth=10", 200, "\"askVolumes\":[6]");

                // -- move the remainder to 105 --
                final HttpResponse<String> move = send(
                        http,
                        base + TRADE + "/symbols/BTCUSD/trade/1/orders/" + makerOrderId,
                        "PUT",
                        "{\"price\":\"105\"}");
                assertEquals(200, move.statusCode(), move.body());
                awaitContains(http, base + TRADE + "/symbols/BTCUSD/orderbook?depth=10", 200, "\"askPrices\":[105.00]");

                // -- cancel the remainder --
                final HttpResponse<String> cancel =
                        send(http, base + TRADE + "/symbols/BTCUSD/trade/1/orders/" + makerOrderId, "DELETE", null);
                assertEquals(200, cancel.statusCode(), cancel.body());
                awaitContains(http, base + TRADE + "/symbols/BTCUSD/orderbook?depth=10", 200, "\"askPrices\":[]");

                // -- history tracks the gateway's own orders --
                final HttpResponse<String> history = get(http, base + TRADE + "/users/1/history");
                assertEquals(200, history.statusCode());
                assertTrue(history.body().contains("\"orderId\":" + makerOrderId), history.body());
                assertTrue(history.body().contains("CANCELLED"), history.body());
                assertTrue(history.body().contains("\"party\":\"MAKER\""), history.body());

                // -- error paths --
                assertEquals(
                        404,
                        get(http, base + TRADE + "/symbols/NOPE/orderbook?depth=5")
                                .statusCode());
                assertEquals(
                        404,
                        post(
                                        http,
                                        base + TRADE + "/symbols/NOPE/trade/1/orders",
                                        "{\"price\":\"1\",\"size\":1,\"action\":\"ASK\",\"orderType\":\"GTC\"}")
                                .statusCode());
                final HttpResponse<String> invalidPrice = post(
                        http,
                        base + TRADE + "/symbols/BTCUSD/trade/1/orders",
                        "{\"price\":\"1.234\",\"size\":1,\"action\":\"ASK\",\"orderType\":\"GTC\"}");
                assertEquals(400, invalidPrice.statusCode());
                assertTrue(invalidPrice.body().contains("\"gatewayResultCode\":1009"), invalidPrice.body());
                final HttpResponse<String> unknownUser = get(http, base + TRADE + "/users/777/state");
                assertEquals(404, unknownUser.statusCode());
                assertTrue(unknownUser.body().contains("\"gatewayResultCode\":1010"), unknownUser.body());
                assertEquals(404, get(http, base + TRADE + "/nonsense").statusCode());
            }
        }
    }

    private static HttpResponse<String> get(final HttpClient http, final String url) throws Exception {
        return send(http, url, "GET", null);
    }

    private static HttpResponse<String> post(final HttpClient http, final String url, final String body)
            throws Exception {
        return send(http, url, "POST", body);
    }

    private static HttpResponse<String> send(
            final HttpClient http, final String url, final String method, final String body) throws Exception {
        final HttpRequest.Builder builder =
                HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(30));
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static void awaitContains(
            final HttpClient http, final String url, final int expectedStatus, final String needle) throws Exception {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        String lastBody = "";
        int lastStatus = -1;
        while (System.currentTimeMillis() < deadline) {
            final HttpResponse<String> response = get(http, url);
            lastStatus = response.statusCode();
            lastBody = response.body();
            if (lastStatus == expectedStatus && lastBody.contains(needle)) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("timed out waiting for '" + needle + "' at " + url + "; last status " + lastStatus
                + " body " + lastBody);
    }

    private static long extractLong(final String body, final String field) {
        final Matcher matcher = Pattern.compile("\"" + field + "\":(\\d+)").matcher(body);
        if (!matcher.find()) {
            throw new AssertionError("field " + field + " not found in: " + body);
        }
        return Long.parseLong(matcher.group(1));
    }
}
