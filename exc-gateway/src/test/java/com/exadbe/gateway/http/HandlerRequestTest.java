package com.exadbe.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Verifies {@link HandlerRequest} query/header parsing and path-param access. */
class HandlerRequestTest {

    @Test
    void parsesQueryAndHeader() {
        final FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/api/v1/orderbook?symbolId=1&maxLevels=8",
                Unpooled.EMPTY_BUFFER);
        request.headers().set("X-User-Id", "5");
        final HandlerRequest handled = new HandlerRequest(request);

        assertEquals("/api/v1/orderbook", handled.path());
        assertEquals(1, handled.intParam("symbolId"));
        assertEquals(8, handled.intParam("maxLevels"));
        assertEquals(8, handled.intParam("maxLevels", 32));
        assertEquals(5L, Long.parseLong(handled.header("X-User-Id")));
        assertThrows(ApiException.class, () -> handled.intParam("missing"));
    }

    @Test
    void exposesPathParamsInPackage() {
        final FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/v1/users/7/balances", Unpooled.EMPTY_BUFFER);
        final HandlerRequest handled = new HandlerRequest(request);
        handled.pathParams(Map.of("uid", "7"));

        assertEquals(7, handled.pathInt("uid"));
        assertEquals(7L, handled.pathLong("uid"));
    }
}
