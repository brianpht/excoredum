package io.justrade.gateway.http;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.justrade.gateway.config.GatewayConfig;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

/** Verifies the admin gate: {@code X-Api-Key} (when configured) plus the {@code X-User-Id} allow-list. */
class AdminGuardTest {

    private static final GatewayConfig CONFIG =
            GatewayConfig.builder().adminApiKey("secret").adminUid(811L).build();

    private static HandlerRequest request(final String apiKey, final String uid) {
        final DefaultFullHttpRequest req = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/v1/users", Unpooled.EMPTY_BUFFER);
        if (apiKey != null) {
            req.headers().set("X-Api-Key", apiKey);
        }
        if (uid != null) {
            req.headers().set("X-User-Id", uid);
        }
        return new HandlerRequest(req);
    }

    @Test
    void acceptsMatchingKeyAndAllowedUid() {
        assertDoesNotThrow(() -> Router.checkAdmin(CONFIG, request("secret", "811")));
    }

    @Test
    void rejectsMissingApiKey() {
        final ApiException e = assertThrows(ApiException.class, () -> Router.checkAdmin(CONFIG, request(null, "811")));
        assertEquals(401, e.status());
    }

    @Test
    void rejectsWrongApiKey() {
        final ApiException e =
                assertThrows(ApiException.class, () -> Router.checkAdmin(CONFIG, request("nope", "811")));
        assertEquals(401, e.status());
    }

    @Test
    void rejectsMalformedUid() {
        final ApiException e =
                assertThrows(ApiException.class, () -> Router.checkAdmin(CONFIG, request("secret", "abc")));
        assertEquals(400, e.status());
    }

    @Test
    void rejectsNonAdminUid() {
        final ApiException e =
                assertThrows(ApiException.class, () -> Router.checkAdmin(CONFIG, request("secret", "999")));
        assertEquals(403, e.status());
    }

    @Test
    void skipsApiKeyWhenNotConfigured() {
        final GatewayConfig noKey = GatewayConfig.builder().adminUid(811L).build();
        assertDoesNotThrow(() -> Router.checkAdmin(noKey, request(null, "811")));
    }
}
