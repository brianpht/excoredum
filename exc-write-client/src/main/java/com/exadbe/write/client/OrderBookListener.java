package com.exadbe.write.client;

/**
 * Optional callback for L2 order-book snapshots delivered on the session
 * egress ({@code L2MarketData}, sent for an ORDER_BOOK_REQUEST). Invoked on
 * the client's polling thread; implementations must not block. The snapshot
 * holder is reused across callbacks.
 */
@FunctionalInterface
public interface OrderBookListener {

    OrderBookListener NONE = snapshot -> {};

    void onOrderBook(OrderBookSnapshot snapshot);
}
