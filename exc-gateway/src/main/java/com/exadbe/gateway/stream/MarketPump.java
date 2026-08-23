package com.exadbe.gateway.stream;

import com.exadbe.gateway.config.GatewayConfig;
import com.exadbe.gateway.http.Json;
import com.exadbe.gateway.read.ReadPump;
import com.exadbe.read.client.L2Snapshot;
import com.exadbe.read.client.MarketTradeResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically snapshots the order book and market tape for each configured
 * symbol through the {@link ReadPump} and publishes them to the broadcaster.
 * This covers market-wide updates even when no command flows through the
 * gateway (the egress stream only reflects the gateway's own writes). A fresh
 * snapshot is requested on the read pump thread; the completion callback
 * publishes on that same thread.
 */
public final class MarketPump implements AutoCloseable {

    private final ReadPump read;
    private final StreamBroadcaster broadcaster;
    private final List<GatewayConfig.Symbol> symbols;
    private final long intervalMs;
    private final Thread thread;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean running = true;

    public MarketPump(
            final ReadPump read,
            final StreamBroadcaster broadcaster,
            final List<GatewayConfig.Symbol> symbols,
            final long intervalMs) {
        this.read = read;
        this.broadcaster = broadcaster;
        this.symbols = List.copyOf(symbols);
        this.intervalMs = intervalMs;
        this.thread = new Thread(this::loop, "exc-gateway-market-pump");
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    private void loop() {
        while (running) {
            try {
                refresh();
            } catch (final RuntimeException ignored) {
                // A transient read failure must not kill the pump.
            }
            sleep(intervalMs);
        }
    }

    private void refresh() {
        for (final GatewayConfig.Symbol symbol : symbols) {
            final int symbolId = symbol.symbolId();
            read.submitOrderBook(symbolId, 32).thenAccept(v -> publishBook((L2Snapshot) v));
            read.submitMarketTrades(symbolId, 100).thenAccept(v -> publishTape(symbolId, castTrades(v)));
        }
    }

    private void publishBook(final L2Snapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        final Map<String, Object> e = new LinkedHashMap<>();
        e.put("type", "L2");
        e.put("symbolId", snapshot.symbolId());
        e.put("appliedPosition", snapshot.appliedPosition());
        e.put("asks", levels(snapshot.asks()));
        e.put("bids", levels(snapshot.bids()));
        publish(e);
    }

    private void publishTape(final int symbolId, final List<MarketTradeResult> trades) {
        if (trades.isEmpty()) {
            return;
        }
        final Map<String, Object> e = new LinkedHashMap<>();
        e.put("type", "MARKET_TAPE");
        e.put("symbolId", symbolId);
        final List<Map<String, Object>> items = new ArrayList<>(trades.size());
        for (final MarketTradeResult t : trades) {
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("timestamp", t.timestamp());
            m.put("price", t.price());
            m.put("size", t.size());
            m.put("makerOrderId", t.makerOrderId());
            m.put("makerUid", t.makerUid());
            m.put("takerUid", t.takerUid());
            items.add(m);
        }
        e.put("trades", items);
        publish(e);
    }

    private static List<Map<String, Object>> levels(final List<L2Snapshot.Level> source) {
        final List<Map<String, Object>> levels = new ArrayList<>(source.size());
        for (final L2Snapshot.Level level : source) {
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("price", level.price());
            m.put("size", level.size());
            m.put("orders", level.orders());
            levels.add(m);
        }
        return levels;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            running = false;
            thread.interrupt();
        }
    }

    private void publish(final Map<String, Object> event) {
        try {
            broadcaster.publish(Json.write(event));
        } catch (final JsonProcessingException ignored) {
            // Serialization of a fixed primitive map cannot fail; drop the event.
        }
    }

    private void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unchecked")
    private static List<MarketTradeResult> castTrades(final Object value) {
        return (List<MarketTradeResult>) value;
    }
}
