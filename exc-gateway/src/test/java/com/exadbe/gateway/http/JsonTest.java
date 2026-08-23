package com.exadbe.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.gateway.dto.OrderDto;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Verifies Jackson serializes the gateway records and reads them back. */
class JsonTest {

    @Test
    void roundTripsRecord() throws Exception {
        final OrderDto order = new OrderDto(
                1, 7L, 811L, "ASK", "GTC", 100L, 10L, 6L, 0L, 4L, 1_000L, 2_000L, 111, "ACTIVE", List.of());
        final String json = Json.write(order);

        assertTrue(json.contains("\"state\":\"ACTIVE\""));
        assertTrue(json.contains("\"remaining\":4"));
        assertTrue(json.contains("\"side\":\"ASK\""));

        final OrderDto back = Json.mapper().readValue(json, OrderDto.class);
        assertEquals(7L, back.orderId());
        assertEquals("ASK", back.side());
        assertEquals(4L, back.remaining());
    }
}
