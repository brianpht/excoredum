package com.exadbe.gateway.core;

import io.netty.channel.ChannelId;
import java.util.ArrayList;
import java.util.function.Consumer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Agent-owned WebSocket subscription registry. Keys are Netty {@link ChannelId}s
 * (unique per connection); subscribers of each tick symbol and each user are
 * indexed for O(1) fan-out on the agent thread. All methods must be called from
 * the gateway agent thread. Subscriptions are idempotent: a duplicate subscribe
 * is a no-op, an unknown unsubscribe is a no-op.
 */
public final class WsSubscriptions {

    /** Result: subscribed (or already subscribed). */
    public static final int OK = 0;

    /** Result: connection cap reached. */
    public static final int CONNECTION_LIMIT = 1;

    /** Result: per-connection subscription cap reached. */
    public static final int SUBSCRIPTION_LIMIT = 2;

    private final int maxConnections;
    private final int maxSubscriptionsPerConnection;
    private final Object2ObjectHashMap<ChannelId, WsConnection> connections = new Object2ObjectHashMap<>();
    private final Int2ObjectHashMap<ArrayList<ChannelId>> ticksBySymbol = new Int2ObjectHashMap<>();
    private final Long2ObjectHashMap<ArrayList<ChannelId>> ordersByUid = new Long2ObjectHashMap<>();

    /** One connection's subscriptions, used for unsubscribe and disconnect cleanup. */
    private static final class WsConnection {
        private final IntArrayList tickSymbols = new IntArrayList();
        private final LongArrayList orderUids = new LongArrayList();

        int count() {
            return tickSymbols.size() + orderUids.size();
        }
    }

    public WsSubscriptions(final int maxConnections, final int maxSubscriptionsPerConnection) {
        this.maxConnections = maxConnections;
        this.maxSubscriptionsPerConnection = maxSubscriptionsPerConnection;
    }

    public int subscribeTicks(final ChannelId channelId, final int symbolId) {
        final WsConnection connection = connectionFor(channelId);
        if (connection == null) {
            return CONNECTION_LIMIT;
        }
        if (connection.tickSymbols.containsInt(symbolId)) {
            return OK;
        }
        if (connection.count() >= maxSubscriptionsPerConnection) {
            return SUBSCRIPTION_LIMIT;
        }
        connection.tickSymbols.add(symbolId);
        ArrayList<ChannelId> subscribers = ticksBySymbol.get(symbolId);
        if (subscribers == null) {
            subscribers = new ArrayList<>();
            ticksBySymbol.put(symbolId, subscribers);
        }
        subscribers.add(channelId);
        return OK;
    }

    public int subscribeOrders(final ChannelId channelId, final long uid) {
        final WsConnection connection = connectionFor(channelId);
        if (connection == null) {
            return CONNECTION_LIMIT;
        }
        if (connection.orderUids.containsLong(uid)) {
            return OK;
        }
        if (connection.count() >= maxSubscriptionsPerConnection) {
            return SUBSCRIPTION_LIMIT;
        }
        connection.orderUids.add(uid);
        ArrayList<ChannelId> subscribers = ordersByUid.get(uid);
        if (subscribers == null) {
            subscribers = new ArrayList<>();
            ordersByUid.put(uid, subscribers);
        }
        subscribers.add(channelId);
        return OK;
    }

    public void unsubscribeTicks(final ChannelId channelId, final int symbolId) {
        final WsConnection connection = connections.get(channelId);
        if (connection == null || !connection.tickSymbols.removeInt(symbolId)) {
            return;
        }
        removeSubscriber(ticksBySymbol, symbolId, channelId);
        dropIfEmpty(channelId, connection);
    }

    public void unsubscribeOrders(final ChannelId channelId, final long uid) {
        final WsConnection connection = connections.get(channelId);
        if (connection == null || !connection.orderUids.removeLong(uid)) {
            return;
        }
        removeSubscriber(ordersByUid, uid, channelId);
        dropIfEmpty(channelId, connection);
    }

    /** Drops every subscription of {@code channelId}; called on disconnect. */
    public void disconnect(final ChannelId channelId) {
        final WsConnection connection = connections.remove(channelId);
        if (connection == null) {
            return;
        }
        for (int i = 0; i < connection.tickSymbols.size(); i++) {
            removeSubscriber(ticksBySymbol, connection.tickSymbols.get(i), channelId);
        }
        for (int i = 0; i < connection.orderUids.size(); i++) {
            removeSubscriber(ordersByUid, connection.orderUids.get(i), channelId);
        }
    }

    public boolean hasTicksSubscribers(final int symbolId) {
        final ArrayList<ChannelId> subscribers = ticksBySymbol.get(symbolId);
        return subscribers != null && !subscribers.isEmpty();
    }

    public boolean hasOrderSubscribers(final long uid) {
        final ArrayList<ChannelId> subscribers = ordersByUid.get(uid);
        return subscribers != null && !subscribers.isEmpty();
    }

    /** Visits each channel subscribed to tick updates for {@code symbolId}. */
    public void forEachTickSubscriber(final int symbolId, final Consumer<ChannelId> action) {
        final ArrayList<ChannelId> subscribers = ticksBySymbol.get(symbolId);
        if (subscribers == null) {
            return;
        }
        for (int i = 0; i < subscribers.size(); i++) {
            action.accept(subscribers.get(i));
        }
    }

    /** Visits each channel subscribed to order updates for {@code uid}. */
    public void forEachOrderSubscriber(final long uid, final Consumer<ChannelId> action) {
        final ArrayList<ChannelId> subscribers = ordersByUid.get(uid);
        if (subscribers == null) {
            return;
        }
        for (int i = 0; i < subscribers.size(); i++) {
            action.accept(subscribers.get(i));
        }
    }

    /** True when the connection has no subscriptions left; the registry entry is dropped. */
    private void dropIfEmpty(final ChannelId channelId, final WsConnection connection) {
        if (connection.count() == 0) {
            connections.remove(channelId);
        }
    }

    /** Returns the connection's entry, creating it when under the cap; null when the cap is reached. */
    private WsConnection connectionFor(final ChannelId channelId) {
        WsConnection connection = connections.get(channelId);
        if (connection != null) {
            return connection;
        }
        if (connections.size() >= maxConnections) {
            return null;
        }
        connection = new WsConnection();
        connections.put(channelId, connection);
        return connection;
    }

    private static void removeSubscriber(
            final Int2ObjectHashMap<ArrayList<ChannelId>> index, final int key, final ChannelId channelId) {
        final ArrayList<ChannelId> subscribers = index.get(key);
        if (subscribers == null) {
            return;
        }
        subscribers.remove(channelId);
        if (subscribers.isEmpty()) {
            index.remove(key);
        }
    }

    private static void removeSubscriber(
            final Long2ObjectHashMap<ArrayList<ChannelId>> index, final long key, final ChannelId channelId) {
        final ArrayList<ChannelId> subscribers = index.get(key);
        if (subscribers == null) {
            return;
        }
        subscribers.remove(channelId);
        if (subscribers.isEmpty()) {
            index.remove(key);
        }
    }
}
