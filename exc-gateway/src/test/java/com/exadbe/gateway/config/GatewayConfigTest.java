package com.exadbe.gateway.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Verifies property parsing and validation of {@link GatewayConfig}. */
class GatewayConfigTest {

    @Test
    void parsesOperatorProperties() {
        final Properties p = new Properties();
        p.setProperty("gateway.http.port", "9090");
        p.setProperty("gateway.write.clientId", "42");
        p.setProperty("gateway.write.ingressEndpoints", "localhost:20100,localhost:20200");
        p.setProperty("gateway.admin.uids", "1,2");
        p.setProperty("gateway.symbols", "1|BTC|10|20|100000000|1000000,2|ETH|10|20|100000000|1000000");

        final GatewayConfig config = GatewayConfig.fromProperties(p);
        assertEquals("0.0.0.0", config.httpHost());
        assertEquals(9090, config.httpPort());
        assertEquals(42L, config.writeClientId());
        assertEquals("localhost:20100,localhost:20200", config.writeIngressEndpoints());
        assertEquals(List.of(1L, 2L), config.adminUids());
        assertEquals(2, config.symbols().size());
        assertEquals("BTC", config.symbols().get(0).name());
        assertEquals(1_000_000L, config.symbols().get(0).quoteScaleK());
    }

    @Test
    void usesConservativeDefaults() {
        final GatewayConfig config = GatewayConfig.builder().build();
        assertEquals(8080, config.httpPort());
        assertEquals(1L, config.writeClientId());
        assertEquals("localhost:20100", config.writeIngressEndpoints());
        assertEquals(List.of(), config.adminUids());
        assertEquals(List.of(), config.symbols());
    }

    @Test
    void rejectsOutOfRangePort() {
        assertThrows(
                IllegalArgumentException.class,
                () -> GatewayConfig.builder().httpPort(70_000).build());
    }

    @Test
    void rejectsMalformedSymbolToken() {
        assertThrows(IllegalArgumentException.class, () -> {
            final Properties p = new Properties();
            p.setProperty("gateway.symbols", "1|BTC");
            GatewayConfig.fromProperties(p);
        });
    }
}
