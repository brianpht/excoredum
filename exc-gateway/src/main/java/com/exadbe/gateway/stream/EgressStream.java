package com.exadbe.gateway.stream;

import com.exadbe.gateway.http.Json;
import com.exadbe.write.client.OrderBookListener;
import com.exadbe.write.client.OrderBookSnapshot;
import com.exadbe.write.client.ReduceEventListener;
import com.exadbe.write.client.RejectEventListener;
import com.exadbe.write.client.TradeEventListener;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bridges the {@code ExcClient} egress listeners to the {@link StreamBroadcaster}.
 * Each event is published as a JSON envelope with a {@code type} discriminator
 * ({@code TRADE}, {@code REDUCE}, {@code REJECT}, {@code L2}). Runs on the write
 * pump thread; the {@code OrderBookSnapshot} holder is reused, so the L2 levels
 * are copied into a fresh JSON map before it is overwritten.
 */
public final class EgressStream
        implements TradeEventListener, ReduceEventListener, RejectEventListener, OrderBookListener {

    private final StreamBroadcaster broadcaster;

    public EgressStream(final StreamBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @Override
    public void onTrade(
            final long commandIdHi,
            final long commandIdLo,
            final int eventIndex,
            final int symbolId,
            final long makerOrderId,
            final long makerUid,
            final long takerUid,
            final long price,
            final long size,
            final boolean makerCompleted) {
        final Map<String, Object> e = new LinkedHashMap<>();
        e.put("type", "TRADE");
        e.put("commandIdLo", commandIdLo);
        e.put("eventIndex", eventIndex);
        e.put("symbolId", symbolId);
        e.put("makerOrderId", makerOrderId);
        e.put("makerUid", makerUid);
        e.put("takerUid", takerUid);
        e.put("price", price);
        e.put("size", size);
        e.put("makerCompleted", makerCompleted);
        publish(e);
    }

    @Override
    public void onReduce(
            final long commandIdHi,
            final long commandIdLo,
            final int eventIndex,
            final int symbolId,
            final long orderId,
            final long uid,
            final long reducedBy,
            final long price,
            final boolean orderCompleted) {
        final Map<String, Object> e = new LinkedHashMap<>();
        e.put("type", "REDUCE");
        e.put("commandIdLo", commandIdLo);
        e.put("eventIndex", eventIndex);
        e.put("symbolId", symbolId);
        e.put("orderId", orderId);
        e.put("uid", uid);
        e.put("reducedBy", reducedBy);
        e.put("price", price);
        e.put("completed", orderCompleted);
        publish(e);
    }

    @Override
    public void onReject(
            final long commandIdHi,
            final long commandIdLo,
            final int eventIndex,
            final int symbolId,
            final long orderId,
            final long uid,
            final long rejectedSize,
            final long price) {
        final Map<String, Object> e = new LinkedHashMap<>();
        e.put("type", "REJECT");
        e.put("commandIdLo", commandIdLo);
        e.put("eventIndex", eventIndex);
        e.put("symbolId", symbolId);
        e.put("orderId", orderId);
        e.put("uid", uid);
        e.put("rejectedSize", rejectedSize);
        e.put("price", price);
        publish(e);
    }

    @Override
    public void onOrderBook(final OrderBookSnapshot snapshot) {
        final Map<String, Object> e = new LinkedHashMap<>();
        e.put("type", "L2");
        e.put("commandIdLo", snapshot.commandIdLo());
        e.put("symbolId", snapshot.symbolId());
        e.put("asks", asks(snapshot));
        e.put("bids", bids(snapshot));
        publish(e);
    }

    private static List<Map<String, Object>> asks(final OrderBookSnapshot snapshot) {
        final List<Map<String, Object>> levels = new ArrayList<>(snapshot.askDepth());
        for (int i = 0; i < snapshot.askDepth(); i++) {
            final Map<String, Object> level = new LinkedHashMap<>();
            level.put("price", snapshot.askPrice(i));
            level.put("size", snapshot.askVolume(i));
            level.put("orders", snapshot.askOrders(i));
            levels.add(level);
        }
        return levels;
    }

    private static List<Map<String, Object>> bids(final OrderBookSnapshot snapshot) {
        final List<Map<String, Object>> levels = new ArrayList<>(snapshot.bidDepth());
        for (int i = 0; i < snapshot.bidDepth(); i++) {
            final Map<String, Object> level = new LinkedHashMap<>();
            level.put("price", snapshot.bidPrice(i));
            level.put("size", snapshot.bidVolume(i));
            level.put("orders", snapshot.bidOrders(i));
            levels.add(level);
        }
        return levels;
    }

    private void publish(final Map<String, Object> event) {
        try {
            broadcaster.publish(Json.write(event));
        } catch (final JsonProcessingException ignored) {
            // Serialization of a fixed primitive map cannot fail; drop the event.
        }
    }
}
