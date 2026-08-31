package com.exadbe.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.gateway.config.GatewayConfig;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Route matching and request validation that run BEFORE any pump is touched,
 * so the router is constructible with null pumps: unknown routes, per-handler
 * field validation, PATCH exclusivity, numeric path params, and the admin
 * gate wired onto the admin routes.
 */
class RouterTest {

    private static final GatewayConfig CONFIG = GatewayConfig.builder()
            .symbol(new GatewayConfig.Symbol(1, "BTC/USDT", 10, 20, 1L, 1L, 0L, 0L))
            .currency(new GatewayConfig.Currency(20, "USDT", 1L))
            .adminApiKey("secret")
            .adminUid(811L)
            .build();

    private final Router router = new Router(null, null, CONFIG);

    private static HandlerRequest request(final HttpMethod method, final String uri, final String body) {
        return new HandlerRequest(new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, method, uri, Unpooled.copiedBuffer(body, StandardCharsets.UTF_8)));
    }

    private static HandlerRequest request(final HttpMethod method, final String uri) {
        return request(method, uri, "");
    }

    @Test
    void unknownRouteIsNotFound() {
        final ApiException e = assertThrows(ApiException.class, () -> router.route(request(HttpMethod.GET, "/nope")));
        assertEquals(404, e.status());
    }

    @Test
    void wrongMethodOnKnownPathIsNotFound() {
        final ApiException e =
                assertThrows(ApiException.class, () -> router.route(request(HttpMethod.PUT, "/api/v1/orders")));
        assertEquals(404, e.status());
    }

    @Test
    void symbolsAreServedFromConfig() throws Exception {
        final Object payload =
                router.route(request(HttpMethod.GET, "/api/v1/symbols")).get();
        assertEquals(List.copyOf(CONFIG.symbols()), payload);
    }

    @Test
    void currenciesAreServedFromConfig() throws Exception {
        final Object payload =
                router.route(request(HttpMethod.GET, "/api/v1/currencies")).get();
        assertEquals(CONFIG.currencies(), payload);
    }

    @Test
    void placeOrderMissingRequiredFieldIsBadRequest() {
        final ApiException e = assertThrows(
                ApiException.class,
                () -> router.route(request(
                        HttpMethod.POST,
                        "/api/v1/orders",
                        "{\"symbolId\":1,\"orderId\":1,\"ask\":true,\"price\":100,\"size\":1}")));
        assertEquals(400, e.status());
        assertTrue(e.getMessage().contains("uid"), "the missing field is named: " + e.getMessage());
    }

    @Test
    void placeOrderUnknownTypeIsBadRequest() {
        final ApiException e = assertThrows(
                ApiException.class,
                () -> router.route(
                        request(
                                HttpMethod.POST,
                                "/api/v1/orders",
                                "{\"symbolId\":1,\"orderId\":1,\"ask\":true,\"type\":\"NOPE\",\"price\":100,\"size\":1,\"uid\":1}")));
        assertEquals(400, e.status());
    }

    @Test
    void modifyOrderWithBothFieldsIsBadRequest() {
        final ApiException e = assertThrows(
                ApiException.class,
                () -> router.route(request(
                        HttpMethod.PATCH, "/api/v1/orders/5", "{\"symbolId\":1,\"uid\":1,\"price\":100,\"size\":2}")));
        assertEquals(400, e.status());
    }

    @Test
    void modifyOrderWithNeitherFieldIsBadRequest() {
        final ApiException e = assertThrows(
                ApiException.class,
                () -> router.route(request(HttpMethod.PATCH, "/api/v1/orders/5", "{\"symbolId\":1,\"uid\":1}")));
        assertEquals(400, e.status());
    }

    @Test
    void malformedOrderIdPathIsBadRequest() {
        final ApiException e =
                assertThrows(ApiException.class, () -> router.route(request(HttpMethod.GET, "/api/v1/orders/abc")));
        assertEquals(400, e.status());
    }

    @Test
    void adminRouteWithoutCredentialsIsUnauthorized() {
        // checkAdmin runs before the (null) write pump is touched.
        final ApiException e = assertThrows(
                ApiException.class, () -> router.route(request(HttpMethod.POST, "/api/v1/users", "{\"uid\":1}")));
        assertEquals(401, e.status());
    }

    @Test
    void adminRouteWithWrongUidIsForbidden() {
        final DefaultFullHttpRequest raw = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.POST,
                "/api/v1/users",
                Unpooled.copiedBuffer("{\"uid\":1}", StandardCharsets.UTF_8));
        raw.headers().set("X-Api-Key", "secret");
        raw.headers().set("X-User-Id", "999");
        final ApiException e = assertThrows(ApiException.class, () -> router.route(new HandlerRequest(raw)));
        assertEquals(403, e.status());
    }

    @Test
    void healthNeedsTheReadPump() {
        // The health route dispatches straight into the (null) read pump, so it
        // fails synchronously in this pump-less harness.
        assertThrows(NullPointerException.class, () -> router.route(request(HttpMethod.GET, "/api/v1/health")));
    }
}
