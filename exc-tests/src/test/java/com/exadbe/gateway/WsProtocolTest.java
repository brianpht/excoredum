package com.exadbe.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.gateway.codec.JsonWriter;
import com.exadbe.gateway.core.GatewayAssetSpec;
import com.exadbe.gateway.core.GatewayOrder;
import com.exadbe.gateway.core.GatewaySymbolSpec;
import com.exadbe.gateway.core.RealtimePublisher;
import com.exadbe.gateway.core.WsSubscriptions;
import io.netty.channel.ChannelId;
import io.netty.channel.DefaultChannelId;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the WebSocket real-time plumbing: the subscription registry
 * (idempotency, bounds, disconnect cleanup) and the outbound JSON frame shapes
 * (tick, order update) shared with the integration suite.
 */
class WsProtocolTest {

    private static final String SPOT = "CURRENCY_EXCHANGE_PAIR";

    @Test
    void subscriptionsAreIdempotentAndIndexed() {
        final WsSubscriptions subs = new WsSubscriptions(16, 8);
        final ChannelId a = DefaultChannelId.newInstance();
        final ChannelId b = DefaultChannelId.newInstance();

        assertEquals(WsSubscriptions.OK, subs.subscribeTicks(a, 1));
        assertTrue(subs.hasTicksSubscribers(1));
        // Duplicate subscribe is a no-op.
        assertEquals(WsSubscriptions.OK, subs.subscribeTicks(a, 1));

        final List<ChannelId> visited = new ArrayList<>();
        subs.forEachTickSubscriber(1, visited::add);
        assertEquals(List.of(a), visited);

        assertEquals(WsSubscriptions.OK, subs.subscribeTicks(a, 2));
        assertEquals(WsSubscriptions.OK, subs.subscribeOrders(a, 10L));
        assertEquals(WsSubscriptions.OK, subs.subscribeOrders(b, 10L));
        subs.forEachOrderSubscriber(10L, visited::add);
        assertTrue(visited.contains(a) && visited.contains(b));
        visited.clear();

        subs.unsubscribeTicks(a, 1);
        assertFalse(subs.hasTicksSubscribers(1));
        assertTrue(subs.hasTicksSubscribers(2));
        // Unknown unsubscribe is a no-op.
        subs.unsubscribeOrders(a, 99L);
        subs.unsubscribeTicks(a, 1);

        // Disconnect drops every remaining subscription.
        subs.disconnect(a);
        assertFalse(subs.hasTicksSubscribers(2));
        assertTrue(subs.hasOrderSubscribers(10L));
        subs.disconnect(b);
        assertFalse(subs.hasOrderSubscribers(10L));
    }

    @Test
    void subscriptionBoundsAreEnforced() {
        final WsSubscriptions perConnection = new WsSubscriptions(16, 2);
        final ChannelId id = DefaultChannelId.newInstance();
        assertEquals(WsSubscriptions.OK, perConnection.subscribeTicks(id, 1));
        assertEquals(WsSubscriptions.OK, perConnection.subscribeOrders(id, 1L));
        assertEquals(WsSubscriptions.SUBSCRIPTION_LIMIT, perConnection.subscribeTicks(id, 2));

        final WsSubscriptions connectionCap = new WsSubscriptions(1, 8);
        assertEquals(WsSubscriptions.OK, connectionCap.subscribeTicks(DefaultChannelId.newInstance(), 1));
        assertEquals(WsSubscriptions.CONNECTION_LIMIT, connectionCap.subscribeTicks(DefaultChannelId.newInstance(), 1));
    }

    @Test
    void tickFrameCarriesScaledPriceAndVolume() {
        final JsonWriter json = new JsonWriter(256);
        RealtimePublisher.writeTickFrame(json, symbolSpec(), 10000L, 4L, 12345L);
        final String frame = frame(json);
        assertTrue(frame.contains("\"type\":\"tick\""), frame);
        assertTrue(frame.contains("\"symbol\":\"BTCUSD\""), frame);
        assertTrue(frame.contains("\"price\":100.00"), frame);
        assertTrue(frame.contains("\"volume\":4"), frame);
        assertTrue(frame.contains("\"timestamp\":12345"), frame);
    }

    @Test
    void orderUpdateFrameCarriesOrderState() {
        final GatewayOrder order =
                new GatewayOrder(42L, "BTCUSD", 1, false, "GTC", 7L, 10000L, 10L, GatewayOrder.STATE_ACTIVE, 1L);
        order.addDeal(false, 10000L, 4L);
        final JsonWriter json = new JsonWriter(256);
        RealtimePublisher.writeOrderUpdateFrame(json, 2L, order, 2);
        final String frame = frame(json);
        assertTrue(frame.contains("\"type\":\"orderUpdate\""), frame);
        assertTrue(frame.contains("\"uid\":2"), frame);
        assertTrue(frame.contains("\"orderId\":42"), frame);
        assertTrue(frame.contains("\"symbol\":\"BTCUSD\""), frame);
        assertTrue(frame.contains("\"price\":100.00"), frame);
        assertTrue(frame.contains("\"size\":10"), frame);
        assertTrue(frame.contains("\"filled\":4"), frame);
        assertTrue(frame.contains("\"state\":\"ACTIVE\""), frame);
        assertTrue(frame.contains("\"userCookie\":7"), frame);
        assertTrue(frame.contains("\"action\":\"BID\""), frame);
        assertTrue(frame.contains("\"orderType\":\"GTC\""), frame);
    }

    private static GatewaySymbolSpec symbolSpec() {
        final GatewayAssetSpec base = new GatewayAssetSpec("BTC", 10, 2, true);
        final GatewayAssetSpec quote = new GatewayAssetSpec("USD", 20, 2, true);
        return new GatewaySymbolSpec(1, "BTCUSD", SPOT, base, quote, 1L, 1L, 0L, 0L, GatewaySymbolSpec.STATUS_ACTIVE);
    }

    private static String frame(final JsonWriter json) {
        return new String(json.buffer(), 0, json.length(), StandardCharsets.UTF_8);
    }
}
