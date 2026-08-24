package com.exadbe.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

/** Verifies the UI static-file handler: root -> index.html, asset content types, API passthrough. */
class StaticResourceHandlerTest {

    private static EmbeddedChannel serve(final String path) {
        final EmbeddedChannel channel = new EmbeddedChannel(new StaticResourceHandler());
        channel.writeInbound(
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path, Unpooled.EMPTY_BUFFER));
        return channel;
    }

    @Test
    void servesIndexFromRoot() {
        final EmbeddedChannel channel = serve("/");
        final FullHttpResponse response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.OK, response.status());
        assertTrue(response.headers().get(HttpHeaderNames.CONTENT_TYPE).startsWith("text/html"));
        response.release();
    }

    @Test
    void servesHtmlWithTextHtmlType() {
        final EmbeddedChannel channel = serve("/index.html");
        final FullHttpResponse response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.OK, response.status());
        assertTrue(response.headers().get(HttpHeaderNames.CONTENT_TYPE).startsWith("text/html"));
        response.release();
    }

    @Test
    void servesJsWithJavascriptType() {
        final EmbeddedChannel channel = serve("/modules/api.js");
        final FullHttpResponse response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.OK, response.status());
        assertTrue(
                response.headers().get(HttpHeaderNames.CONTENT_TYPE).startsWith("application/javascript"),
                response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        response.release();
    }

    @Test
    void passThroughForApiAndWs() {
        // The static handler must not intercept the JSON API or the WS upgrade.
        final EmbeddedChannel api = serve("/api/v1/symbols");
        assertNull(api.readOutbound(), "api request should not produce a static response");

        final EmbeddedChannel ws = serve("/ws");
        assertNull(ws.readOutbound(), "ws upgrade should not produce a static response");
    }

    @Test
    void rejectsTraversal() {
        final EmbeddedChannel channel = serve("/..%2f..%2fetc%2fpasswd");
        final FullHttpResponse response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
        response.release();
    }
}
