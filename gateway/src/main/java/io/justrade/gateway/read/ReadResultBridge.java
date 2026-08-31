package io.justrade.gateway.read;

import io.justrade.gateway.http.ApiException;
import io.justrade.protocol.QueryStatusCode;
import io.justrade.protocol.QueryType;
import io.justrade.read.client.BalanceResult;
import io.justrade.read.client.L2Snapshot;
import io.justrade.read.client.MarketTradeResult;
import io.justrade.read.client.OrderRecordResult;
import io.justrade.read.client.QueryListener;
import io.justrade.read.client.TotalBalanceResult;
import io.justrade.read.client.UserReport;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Bridges {@link ReadClient} async delivery to the HTTP handlers: a single
 * {@link QueryListener} that completes a per-request {@link CompletableFuture}
 * by {@code requestId}. Runs on the read pump thread (the same thread that
 * calls {@link ReadClient#poll()}), so it must not block.
 */
public final class ReadResultBridge implements QueryListener {

    // Single-threaded: register() and the listener callbacks all run on the read pump thread.
    private final HashMap<Long, CompletableFuture<Object>> pending = new HashMap<>();

    /** Registered by the handler *after* a submit returns the request id (safe: submit and poll share one thread). */
    public CompletableFuture<Object> register(final long requestId) {
        final CompletableFuture<Object> future = new CompletableFuture<>();
        pending.put(requestId, future);
        return future;
    }

    private void complete(final long requestId, final Object value) {
        final CompletableFuture<Object> future = pending.remove(requestId);
        if (future != null) {
            future.complete(value);
        }
    }

    private void fail(final long requestId, final ApiException error) {
        final CompletableFuture<Object> future = pending.remove(requestId);
        if (future != null) {
            future.completeExceptionally(error);
        }
    }

    @Override
    public void onUserExists(final long requestId, final boolean exists) {
        complete(requestId, exists);
    }

    @Override
    public void onBalance(final long requestId, final BalanceResult result) {
        complete(requestId, result);
    }

    @Override
    public void onL2(final long requestId, final L2Snapshot snapshot) {
        complete(requestId, snapshot);
    }

    @Override
    public void onUserReport(final long requestId, final UserReport report) {
        complete(requestId, report);
    }

    @Override
    public void onOrderHistory(final long requestId, final List<OrderRecordResult> records) {
        complete(requestId, records);
    }

    @Override
    public void onActiveOrders(final long requestId, final List<OrderRecordResult> records) {
        complete(requestId, records);
    }

    @Override
    public void onOrder(final long requestId, final OrderRecordResult record) {
        complete(requestId, record);
    }

    @Override
    public void onUserTrades(final long requestId, final List<MarketTradeResult> trades) {
        complete(requestId, trades);
    }

    @Override
    public void onMarketTrades(final long requestId, final List<MarketTradeResult> trades) {
        complete(requestId, trades);
    }

    @Override
    public void onTotalCurrencyBalance(final long requestId, final TotalBalanceResult totals) {
        complete(requestId, totals);
    }

    @Override
    public void onStateHash(final long requestId, final long stateHash) {
        complete(requestId, stateHash);
    }

    @Override
    public void onTimeout(final long requestId, final QueryType type) {
        fail(requestId, ApiException.timeout("no response for query " + type));
    }

    @Override
    public void onError(final long requestId, final QueryType type, final QueryStatusCode status) {
        fail(requestId, ApiException.badRequest("read service rejected query " + type + ": " + status));
    }
}
