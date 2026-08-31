package io.justrade.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.gateway.config.GatewayConfig;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * The handler's status mapping and JSON envelope, driven through an embedded
 * channel against a pump-less router (only the config-served routes and the
 * synchronous validation errors are reachable without pumps).
 */
class HttpHandlerTest {

    private static final GatewayConfig CONFIG = GatewayConfig.builder()
            .symbol(new GatewayConfig.Symbol(1, "BTC/USDT", 10, 20, 1L, 1L, 0L, 0L))
            .build();

    private static EmbeddedChannel newChannel() {
        return new EmbeddedChannel(new HttpHandler(new Router(null, null, CONFIG)));
    }

    private static FullHttpRequest request(final HttpMethod method, final String uri, final String body) {
        final DefaultFullHttpRequest req = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, method, uri, Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        HttpUtil.setKeepAlive(req, true);
        return req;
    }

    private static FullHttpResponse roundTrip(final EmbeddedChannel channel, final FullHttpRequest req) {
        channel.writeInbound(req);
        channel.runPendingTasks();
        return channel.readOutbound();
    }

    private static String body(final FullHttpResponse response) {
        return response.content().toString(StandardCharsets.UTF_8);
    }

    @Test
    void successfulRouteWritesJsonWith200() {
        final EmbeddedChannel channel = newChannel();
        try {
            final FullHttpResponse response = roundTrip(channel, request(HttpMethod.GET, "/api/v1/symbols", ""));
            assertEquals(200, response.status().code());
            assertEquals("application/json; charset=utf-8", response.headers().get("Content-Type"));
            assertTrue(body(response).contains("\"symbolId\":1"), body(response));
            assertTrue(channel.isOpen(), "a keep-alive request keeps the channel open");
        } finally {
            channel.close();
        }
    }

    @Test
    void unknownRouteWrites404ErrorEnvelope() {
        final EmbeddedChannel channel = newChannel();
        try {
            final FullHttpResponse response = roundTrip(channel, request(HttpMethod.GET, "/nope", ""));
            assertEquals(404, response.status().code());
            final String json = body(response);
            assertTrue(json.contains("\"error\":true"), json);
            assertTrue(json.contains("\"message\":"), json);
        } finally {
            channel.close();
        }
    }

    @Test
    void validationFailureWrites400ErrorEnvelope() {
        final EmbeddedChannel channel = newChannel();
        try {
            final FullHttpResponse response =
                    roundTrip(channel, request(HttpMethod.POST, "/api/v1/orders", "{\"symbolId\":1}"));
            assertEquals(400, response.status().code());
            assertTrue(body(response).contains("\"error\":true"));
        } finally {
            channel.close();
        }
    }

    @Test
    void nonKeepAliveRequestClosesTheChannel() {
        final EmbeddedChannel channel = newChannel();
        try {
            final DefaultFullHttpRequest req = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/v1/symbols", Unpooled.EMPTY_BUFFER);
            HttpUtil.setKeepAlive(req, false);
            final FullHttpResponse response = roundTrip(channel, req);
            assertEquals(200, response.status().code());
            assertFalse(channel.isOpen(), "a non-keep-alive response closes the connection");
        } finally {
            channel.close();
        }
    }
}
