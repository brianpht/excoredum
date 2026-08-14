package com.exadbe.read.client;

import com.exadbe.protocol.QueryStatusCode;
import com.exadbe.protocol.QueryType;
import java.util.List;

/**
 * Callback sink for asynchronous query delivery. Register with
 * {@link ReadClient#setListener(QueryListener)}; every callback runs on the
 * thread that calls {@link ReadClient#poll()}, matching how {@code ExcClient}
 * delivers egress events.
 *
 * <p>Each callback carries the {@code requestId} returned by the matching
 * {@code submit...} call for correlation. Only the callback relevant to the
 * submitted query type fires; {@code onTimeout} and {@code onError} fire for
 * every query type.
 */
public interface QueryListener {

    /** No-op listener; the default until one is registered. */
    QueryListener NONE = new QueryListener() {};

    default void onUserExists(final long requestId, final boolean exists) {}

    default void onBalance(final long requestId, final BalanceResult result) {}

    default void onL2(final long requestId, final L2Snapshot snapshot) {}

    default void onUserReport(final long requestId, final UserReport report) {}

    default void onOrderHistory(final long requestId, final List<OrderRecordResult> records) {}

    default void onActiveOrders(final long requestId, final List<OrderRecordResult> records) {}

    default void onOrder(final long requestId, final OrderRecordResult record) {}

    default void onUserTrades(final long requestId, final List<MarketTradeResult> trades) {}

    default void onMarketTrades(final long requestId, final List<MarketTradeResult> trades) {}

    default void onTotalCurrencyBalance(final long requestId, final TotalBalanceResult totals) {}

    default void onStateHash(final long requestId, final long stateHash) {}

    /** The query exhausted its retry budget without a response. */
    default void onTimeout(final long requestId, final QueryType type) {}

    /** The read service rejected the query (e.g. unsupported query type). */
    default void onError(final long requestId, final QueryType type, final QueryStatusCode status) {}
}
