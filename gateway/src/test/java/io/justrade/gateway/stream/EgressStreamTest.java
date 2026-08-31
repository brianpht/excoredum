package io.justrade.gateway.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.justrade.write.client.OrderBookSnapshot;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the WebSocket frame shapes the egress listeners publish. The L2 holder
 * is package-private to the write client, so the snapshot is filled through
 * reflection - the frame shape is what subscribers parse.
 */
class EgressStreamTest {

    private final List<String> frames = new ArrayList<>();
    private final StreamBroadcaster broadcaster = new StreamBroadcaster();
    private final ObjectMapper mapper = new ObjectMapper();
    private EgressStream stream;

    @BeforeEach
    void setUp() {
        broadcaster.add(frames::add);
        stream = new EgressStream(broadcaster);
    }

    private JsonNode firstFrame() throws Exception {
        assertEquals(1, frames.size(), "exactly one frame per event");
        return mapper.readTree(frames.get(0));
    }

    @Test
    void tradeFrameCarriesEveryField() throws Exception {
        stream.onTrade(1L, 42L, 3, 7, 100L, 11L, 22L, 500L, 6L, true);

        final JsonNode e = firstFrame();
        assertEquals("TRADE", e.get("type").asText());
        assertEquals(42L, e.get("commandIdLo").asLong());
        assertEquals(3, e.get("eventIndex").asInt());
        assertEquals(7, e.get("symbolId").asInt());
        assertEquals(100L, e.get("makerOrderId").asLong());
        assertEquals(11L, e.get("makerUid").asLong());
        assertEquals(22L, e.get("takerUid").asLong());
        assertEquals(500L, e.get("price").asLong());
        assertEquals(6L, e.get("size").asLong());
        assertEquals(true, e.get("makerCompleted").asBoolean());
    }

    @Test
    void reduceFrameCarriesPriceAndCompletion() throws Exception {
        stream.onReduce(1L, 43L, 0, 7, 100L, 11L, 4L, 500L, false);

        final JsonNode e = firstFrame();
        assertEquals("REDUCE", e.get("type").asText());
        assertEquals(43L, e.get("commandIdLo").asLong());
        assertEquals(7, e.get("symbolId").asInt());
        assertEquals(100L, e.get("orderId").asLong());
        assertEquals(11L, e.get("uid").asLong());
        assertEquals(4L, e.get("reducedBy").asLong());
        assertEquals(500L, e.get("price").asLong());
        assertFalse(e.get("completed").asBoolean());
    }

    @Test
    void rejectFrameCarriesBudgetAsPrice() throws Exception {
        stream.onReject(1L, 44L, 0, 7, 100L, 11L, 5L, 600L);

        final JsonNode e = firstFrame();
        assertEquals("REJECT", e.get("type").asText());
        assertEquals(44L, e.get("commandIdLo").asLong());
        assertEquals(7, e.get("symbolId").asInt());
        assertEquals(100L, e.get("orderId").asLong());
        assertEquals(5L, e.get("rejectedSize").asLong());
        assertEquals(600L, e.get("price").asLong());
    }

    @Test
    void l2FrameCopiesLevelsOutOfTheReusedHolder() throws Exception {
        final OrderBookSnapshot snapshot = new OrderBookSnapshot();
        invoke(snapshot, "begin", new Class<?>[] {long.class, long.class, int.class}, 1L, 42L, 7);
        invoke(snapshot, "addAsk", new Class<?>[] {long.class, long.class, int.class}, 101L, 5L, 2);
        invoke(snapshot, "addBid", new Class<?>[] {long.class, long.class, int.class}, 99L, 3L, 1);

        stream.onOrderBook(snapshot);
        // The holder is reused by the client; overwriting it must not corrupt
        // the already-published frame.
        invoke(snapshot, "begin", new Class<?>[] {long.class, long.class, int.class}, 2L, 99L, 8);

        final JsonNode e = firstFrame();
        assertEquals("L2", e.get("type").asText());
        assertEquals(42L, e.get("commandIdLo").asLong());
        assertEquals(7, e.get("symbolId").asInt());
        assertEquals(1, e.get("asks").size());
        assertEquals(101L, e.get("asks").get(0).get("price").asLong());
        assertEquals(5L, e.get("asks").get(0).get("size").asLong());
        assertEquals(2, e.get("asks").get(0).get("orders").asInt());
        assertEquals(1, e.get("bids").size());
        assertEquals(99L, e.get("bids").get(0).get("price").asLong());
        assertEquals(3L, e.get("bids").get(0).get("size").asLong());
        assertEquals(1, e.get("bids").get(0).get("orders").asInt());
    }

    private static void invoke(
            final OrderBookSnapshot snapshot, final String name, final Class<?>[] types, final Object... args)
            throws Exception {
        final Method method = OrderBookSnapshot.class.getDeclaredMethod(name, types);
        method.setAccessible(true);
        method.invoke(snapshot, args);
    }
}
