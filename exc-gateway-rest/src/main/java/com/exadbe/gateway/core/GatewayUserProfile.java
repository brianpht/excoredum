package com.exadbe.gateway.core;

import org.agrona.collections.Long2ObjectHashMap;

/**
 * Orders placed through this gateway by one user, keyed by order id. Owned
 * exclusively by the gateway agent thread.
 */
public final class GatewayUserProfile {

    private final long uid;
    private final Long2ObjectHashMap<GatewayOrder> orders = new Long2ObjectHashMap<>(16, 0.65f);

    public GatewayUserProfile(final long uid) {
        this.uid = uid;
    }

    public long uid() {
        return uid;
    }

    public void addOrder(final GatewayOrder order) {
        orders.put(order.orderId(), order);
    }

    public GatewayOrder order(final long orderId) {
        return orders.get(orderId);
    }

    public GatewayOrder removeOrder(final long orderId) {
        return orders.remove(orderId);
    }

    /** All tracked orders, for the history endpoint. */
    public Long2ObjectHashMap<GatewayOrder> ordersMap() {
        return orders;
    }
}
